"""事件仓核心：WAL 重放、当前代增量维护、查询绑定、压缩整理与旧代回收。

并发模型：单进程内一把可重入锁保护全部状态；HTTP 线程与后台整理线程
通过同一把锁串行化临界区，整理的重活（遍历、落盘）在锁外做，只在
检查点与切换瞬间取锁。
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from .bitmap import Bitmap
from .filter import (
    ConflictError,
    EMPTY,
    Filter,
    StaleGenerationError,
    StoreError,
    validate_record,
)
from .fsutil import atomic_write, fsync_dir
from .generation import (
    BITMAP_DIR,
    DELETES_BITMAP,
    MASKS,
    MANIFEST,
    MAPPING,
    RECORDS,
    Generation,
    bitmap_relpath,
    build_generation_on_disk,
)
from .wal import WAL

GENS_DIRNAME = "gens"
CURRENT_NAME = "CURRENT"
REJECTED_DIRNAME = "rejected"
PINS_NAME = "query-pins.json"
COMPACT_NAME = "compaction.json"


@dataclass
class QueryHandle:
    """一次查询会话：创建时绑定数据代次、行号映射与删除掩码。"""

    query_id: str
    gen_id: int
    filter: Filter
    combinator: str
    created_lsn: int
    created_at: float
    ordinals: list[int]
    delete_snapshot: list[int]
    index_enabled: bool
    read_stat: dict[str, Any]


@dataclass
class _CompactionState:
    phase: str = "idle"          # idle/running/paused/committing/done/conflict/aborted
    target_gen: int = 0
    base_lsn: int = 0
    fence_lsn: int = 0           # 暂停时记录的 WAL 水位
    total_rows: int = 0
    done_rows: int = 0
    stage: str = ""
    request: str = ""            # 用户请求: pause/abort/""
    error: str = ""
    updated_at: float = 0.0


class EventStore:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.gens_dir = os.path.join(data_dir, GENS_DIRNAME)
        self.rejected_dir = os.path.join(data_dir, REJECTED_DIRNAME)
        os.makedirs(self.gens_dir, exist_ok=True)
        os.makedirs(self.rejected_dir, exist_ok=True)
        self.lock = threading.RLock()
        self.wal = WAL(os.path.join(data_dir, "wal.log"))
        # 内存当前状态（WAL 的物化视图）
        self.live: dict[str, dict[str, Any]] = {}
        self.current_gen: Generation | None = None
        self.gen_cache: dict[int, Generation] = {}
        self.queries: dict[str, QueryHandle] = {}
        self.pins: dict[int, int] = {}          # gen_id -> 绑定查询数（持久化）
        self.compact = _CompactionState()
        self._recover()

    # ================= 启动恢复 ========================================
    def _recover(self) -> None:
        entries = WAL.read_all(self.wal.path)
        # 1. 重放 WAL 构建内存视图
        for e in entries:
            self._apply_to_memory(e)
        # 2. 加载 CURRENT 指向的代次
        cur_path = os.path.join(self.data_dir, CURRENT_NAME)
        current_id = None
        if os.path.exists(cur_path):
            with open(cur_path, "rb") as f:
                current_id = int(f.read().decode("utf-8").strip())
        if current_id is not None and os.path.isdir(os.path.join(self.gens_dir, f"gen-{current_id:06d}")):
            gen = Generation(os.path.join(self.gens_dir, f"gen-{current_id:06d}"), current_id).load()
            if gen.rejected:
                # 当前代损坏 -> 隔离并从 WAL 全量重建，绝不带病服务
                self._quarantine(gen)
                gen = None
            else:
                self.current_gen = gen
                self.gen_cache[current_id] = gen
        # 3. 当前代缺失或被拒：从 WAL 全量构建 gen-000001
        if self.current_gen is None:
            self._rebuild_current_from_wal(1)
        # 4. WAL 完整保留历史，base_lsn 只作物化水位；一致性以内容对账为准
        elif not self._current_matches_memory():
            self._rebuild_current_from_wal(self.current_gen.gen_id + 1)
        # 5. 恢复持久化的查询绑定计数
        self._load_pins()
        # 6. 恢复整理状态；临时目录是上次中断的残骸，清理之
        self._recover_compaction()

    def _apply_to_memory(self, e) -> None:
        if e.op == "put":
            self.live[e.event_id] = validate_record(e.payload["record"])
        elif e.op == "update":
            if e.event_id not in self.live:
                return  # 更新一个已不存在的编号，空操作
            rec = self.live[e.event_id]
            changes = e.payload.get("changes", {})
            for k, v in changes.items():
                if k in ("region", "channel"):
                    from .filter import normalize_single
                    rec[k] = normalize_single(v)
                elif k == "tags":
                    from .filter import normalize_tags
                    rec[k] = normalize_tags(v)
                else:
                    rec[k] = v
        elif e.op == "delete":
            self.live.pop(e.event_id, None)

    def _current_matches_memory(self) -> bool:
        gen = self.current_gen
        deleted = set(gen.delete_mask)
        live_ids = {gen.mapping[i] for i in range(gen.row_count) if i not in deleted}
        if live_ids != set(self.live):
            return False
        for i in range(gen.row_count):
            if i in deleted:
                continue
            rec = gen.records[i]
            mem = self.live[rec["event_id"]]
            if rec["region"] != mem["region"] or rec["channel"] != mem["channel"] or rec["tags"] != mem["tags"]:
                return False
        return True

    def _rebuild_current_from_wal(self, new_gen_id: int, *, keep_gen_id: bool = False) -> None:
        """从内存视图（= WAL 重放结果）全量重建当前代磁盘文件。

        keep_gen_id=True 时复用当前代次号（更正场景，行号可能重排但代次不变）；
        否则使用 new_gen_id（启动恢复 / 损坏重建）。
        """
        if keep_gen_id and self.current_gen is not None:
            new_gen_id = self.current_gen.gen_id
        tmp = os.path.join(self.gens_dir, f".build-gen-{new_gen_id:06d}")
        if os.path.exists(tmp):
            shutil.rmtree(tmp)
        records = sorted(self.live.values(), key=lambda r: r["event_id"])
        build_generation_on_disk(tmp, new_gen_id, records, base_lsn=self.wal.next_lsn - 1)
        # 重建后恢复“哪些行经历过更正”的更新掩码（从 WAL 痕迹推导）
        tmp_gen = Generation(tmp, new_gen_id).load()
        updated_ids = {e.event_id for e in WAL.read_all(self.wal.path) if e.op == "update"}
        if updated_ids:
            tmp_gen.update_mask = Bitmap(i for i, r in enumerate(records) if r["event_id"] in updated_ids)
            self._write_masks(tmp_gen)
        self._refresh_manifest(tmp_gen)  # 掩码变化后重算全部文件校验和
        final = os.path.join(self.gens_dir, f"gen-{new_gen_id:06d}")
        if os.path.exists(final):
            shutil.rmtree(final)
        os.replace(tmp, final)
        fsync_dir(self.gens_dir)
        atomic_write(os.path.join(self.data_dir, CURRENT_NAME), f"{new_gen_id:06d}".encode())
        self.current_gen = Generation(final, new_gen_id).load()
        if self.current_gen.rejected:
            raise RuntimeError(f"重建代次自检失败: {self.current_gen.reject_reason}")
        self.gen_cache[new_gen_id] = self.current_gen

    def _quarantine(self, gen: Generation) -> None:
        dest = os.path.join(self.rejected_dir, f"gen-{gen.gen_id:06d}-{int(time.time())}")
        shutil.move(gen.gen_dir, dest)
        atomic_write(
            os.path.join(dest, "REJECTED.txt"),
            f"隔离时间: {time.ctime()}\n原因: {gen.reject_reason}\n".encode(),
        )

    # ================= 写入：插入 / 更正 / 删除 ========================
    def put_event(self, data: dict[str, Any]) -> dict[str, Any]:
        record = validate_record(data)
        with self.lock:
            if record["event_id"] in self.live:
                raise StoreError(f"业务编号 {record['event_id']} 已存在，拒绝重复插入（请使用更正接口）")
            self.wal.append("put", record["event_id"], {"record": record})
            self.live[record["event_id"]] = record
            self._materialize_insert(record)
            return {"event_id": record["event_id"], "gen_id": self.current_gen.gen_id}

    def update_event(self, event_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        event_id = str(event_id).strip()
        with self.lock:
            if event_id not in self.live:
                raise StoreError(f"业务编号 {event_id} 不存在（可能已删除），无法更正")
            clean: dict[str, Any] = {}
            from .filter import normalize_single, normalize_tags
            for k, v in changes.items():
                if k in ("region", "channel"):
                    clean[k] = normalize_single(v)
                elif k == "tags":
                    clean[k] = normalize_tags(v)
                elif k == "payload":
                    clean[k] = v
            if not clean:
                raise StoreError("没有可更正的字段")
            self.wal.append("update", event_id, {"changes": clean})
            # 类别更正涉及“撤旧位图+建新位图+重写 records”，统一走
            # 「WAL 追加 -> 从 WAL 重建当前代」，保证磁盘记录、行号映射、
            # 位图索引与内存视图严格一致（与启动恢复是同一条代码路径）
            self._apply_to_memory(type("E", (), {"op": "update", "event_id": event_id, "payload": {"changes": clean}})())
            self._rebuild_current_from_wal(0, keep_gen_id=True)
            return {"event_id": event_id, "gen_id": self.current_gen.gen_id}

    def delete_event(self, event_id: str) -> dict[str, Any]:
        event_id = str(event_id).strip()
        with self.lock:
            if event_id not in self.live:
                raise StoreError(f"业务编号 {event_id} 不存在或已删除")
            ordinal = self.current_gen.event_to_ord.get(event_id)
            self.wal.append("delete", event_id, {})
            self.live.pop(event_id, None)
            if ordinal is not None:
                self._materialize_delete(ordinal)
            return {"event_id": event_id, "deleted": True, "gen_id": self.current_gen.gen_id}

    # ---- 当前代增量物化（每次写后原子重写受影响文件）------------------
    def _materialize_insert(self, record: dict[str, Any]) -> None:
        gen = self.current_gen
        ordinal = gen.row_count
        gen.records.append(record)
        gen.mapping.append(record["event_id"])
        gen.event_to_ord[record["event_id"]] = ordinal
        gen.row_count += 1
        self._append_record_line(record)
        atomic_write(os.path.join(gen.gen_dir, MAPPING), json.dumps(gen.mapping, ensure_ascii=False).encode())
        self._add_index_value(("region", record["region"]), ordinal)
        self._add_index_value(("channel", record["channel"]), ordinal)
        if record["tags"]:
            for tag in record["tags"]:
                self._add_index_value(("tags", tag), ordinal)
        else:
            self._add_index_value(("tags", EMPTY), ordinal)
        self._refresh_manifest(gen)

    def _materialize_delete(self, ordinal: int) -> None:
        gen = self.current_gen
        gen.delete_mask.add(ordinal)
        self._write_masks(gen)
        self._refresh_manifest(gen)  # masks/deletes 位图变化后必须重算校验和

    def _add_index_value(self, key: tuple[str, str], ordinal: int) -> None:
        gen = self.current_gen
        bm = gen.bitmaps.get(key)
        if bm is None:
            bm = Bitmap()
            gen.bitmaps[key] = bm
        bm.add(ordinal)
        self._write_bitmap(gen, key, bm)

    def _remove_index_value(self, key: tuple[str, str], ordinal: int) -> None:
        gen = self.current_gen
        bm = gen.bitmaps.get(key)
        if bm is None or ordinal not in bm:
            return  # 该值索引上本来就没有这一行
        new_bm = bm - Bitmap([ordinal])
        if len(new_bm) == 0:
            # 空位图直接删文件，避免悬挂旧值；manifest 重写时会同步剔除
            path = os.path.join(gen.gen_dir, bitmap_relpath(*key))
            if os.path.exists(path):
                os.unlink(path)
            gen.bitmaps.pop(key, None)
            self._refresh_manifest(gen)
        else:
            gen.bitmaps[key] = new_bm
            self._write_bitmap(gen, key, new_bm)

    def _write_bitmap(self, gen: Generation, key: tuple[str, str], bm: Bitmap) -> None:
        rel = bitmap_relpath(*key)
        atomic_write(os.path.join(gen.gen_dir, rel), bm.serialize())

    def _append_record_line(self, record: dict[str, Any]) -> None:
        gen = self.current_gen
        with open(os.path.join(gen.gen_dir, RECORDS), "ab") as f:
            f.write(json.dumps(record, ensure_ascii=False).encode("utf-8") + b"\n")
            f.flush()
            os.fsync(f.fileno())

    def _write_masks(self, gen: Generation) -> None:
        payload = {
            "delete_mask": gen.delete_mask.serialize().hex(),
            "update_mask": gen.update_mask.serialize().hex(),
        }
        atomic_write(os.path.join(gen.gen_dir, MASKS), json.dumps(payload).encode())
        self._write_bitmap_path(gen, f"{BITMAP_DIR}/{DELETES_BITMAP}", gen.delete_mask)

    def _write_bitmap_path(self, gen: Generation, rel: str, bm: Bitmap) -> None:
        atomic_write(os.path.join(gen.gen_dir, rel), bm.serialize())

    def _refresh_manifest(self, gen: Generation) -> None:
        """重新登记当前代所有文件的 size/sha256 与 base_lsn。"""
        from .fsutil import sha256_file
        files_meta: dict[str, dict[str, Any]] = {}
        for root, _dirs, names in os.walk(gen.gen_dir):
            for name in names:
                if name == MANIFEST:
                    continue
                full = os.path.join(root, name)
                rel = os.path.relpath(full, gen.gen_dir)
                files_meta[rel] = {"size": os.path.getsize(full), "sha256": sha256_file(full)}
        manifest = {
            "gen_id": gen.gen_id,
            "base_lsn": self.wal.next_lsn - 1,
            "row_count": gen.row_count,
            "created_from": gen.manifest.get("created_from", "init"),
            "files": files_meta,
            "bitmap_format": "roaring" if gen.delete_mask.is_roaring else "pyint",
        }
        atomic_write(os.path.join(gen.gen_dir, MANIFEST), json.dumps(manifest, ensure_ascii=False, indent=2).encode())
        gen.manifest = manifest
        gen.base_lsn = self.wal.next_lsn - 1

    # ================= 查询绑定与执行 ==================================
    def _load_pins(self) -> None:
        path = os.path.join(self.data_dir, PINS_NAME)
        if os.path.exists(path):
            with open(path, "rb") as f:
                self.pins = {int(k): int(v) for k, v in json.loads(f.read().decode()).items()}

    def _save_pins(self) -> None:
        atomic_write(
            os.path.join(self.data_dir, PINS_NAME),
            json.dumps({str(k): v for k, v in self.pins.items()}).encode(),
        )

    def _load_generation(self, gen_id: int) -> Generation:
        if gen_id == self.current_gen.gen_id:
            # CURRENT 代损坏 -> 自愈：隔离旧目录并从 WAL 重建新代，新查询可继续；
            # 已绑定旧代的查询仍会因 gen_id 不匹配而在下面显式失败
            check = self._verify_current()
            if check is not None:
                return check
            return self.current_gen
        if gen_id in self.gen_cache:
            gen = self.gen_cache[gen_id]
            # 即使已缓存也要重新校验磁盘文件：旧代可能在外部被篡改/损坏
            check = Generation(gen.gen_dir, gen_id).load()
            if check.rejected:
                self.gen_cache.pop(gen_id, None)
                self._quarantine(check)
                raise StaleGenerationError(f"代次 gen-{gen_id:06d} 校验失败已隔离: {check.reject_reason}")
            return gen
        path = os.path.join(self.gens_dir, f"gen-{gen_id:06d}")
        if not os.path.isdir(path):
            raise StaleGenerationError(f"代次 gen-{gen_id:06d} 已被回收，无法套用其位图（请重新发起查询）")
        gen = Generation(path, gen_id).load()
        if gen.rejected:
            self._quarantine(gen)
            raise StaleGenerationError(f"代次 gen-{gen_id:06d} 校验失败已隔离: {gen.reject_reason}")
        self.gen_cache[gen_id] = gen
        return gen

    def _verify_current(self) -> Generation | None:
        """校验当前代磁盘；损坏则隔离并从 WAL 重建，返回新代；正常返回 None。"""
        cur = self.current_gen
        check = Generation(cur.gen_dir, cur.gen_id).load()
        if not check.rejected:
            return None
        self._quarantine(check)
        self.gen_cache.pop(cur.gen_id, None)
        self._rebuild_current_from_wal(cur.gen_id + 1)
        return self.current_gen

    def create_query(self, filter_dict: dict[str, Any], index_enabled: bool = True) -> QueryHandle:
        flt = Filter.from_dict(filter_dict)
        with self.lock:
            gen = self._load_generation(self.current_gen.gen_id)
            deleted = set(gen.delete_mask)
            stat: dict[str, Any] = {}
            if index_enabled:
                ordinals, stat = self._execute_index(gen, flt, deleted)
            else:
                ordinals, stat = self._execute_scan(gen, flt, deleted)
            qid = str(uuid.uuid4())
            handle = QueryHandle(
                query_id=qid,
                gen_id=gen.gen_id,
                filter=flt,
                combinator=flt.combinator,
                created_lsn=self.wal.next_lsn - 1,
                created_at=time.time(),
                ordinals=ordinals,
                delete_snapshot=sorted(deleted),
                index_enabled=index_enabled,
                read_stat=stat,
            )
            self.queries[qid] = handle
            self.pins[gen.gen_id] = self.pins.get(gen.gen_id, 0) + 1
            self._save_pins()
            return handle

    def _execute_index(self, gen: Generation, flt: Filter, deleted: set[int]) -> tuple[list[int], dict[str, Any]]:
        from .generation import FileStat
        stat = FileStat()
        if not flt.conditions:
            result = Bitmap(i for i in range(gen.row_count) if i not in deleted)
            stat.add_file(RECORDS, os.path.getsize(os.path.join(gen.gen_dir, RECORDS)))
        else:
            groups: list[Bitmap] = []
            for cond in flt.conditions:
                union = Bitmap()
                for value in cond.values:
                    rel = bitmap_relpath(cond.field, value)
                    path = os.path.join(gen.gen_dir, rel)
                    if os.path.exists(path):
                        with open(path, "rb") as f:
                            raw = f.read()
                        stat.add_file(rel, len(raw))
                        union = union | Bitmap.deserialize(raw)
                    # 值不存在则位图为空，记 0 字节读取
                groups.append(union)
            result = groups[0].copy()
            for g in groups[1:]:
                result = (result & g) if flt.combinator == "AND" else (result | g)
            # 删除掩码必须显式扣除
            drel = f"{BITMAP_DIR}/{DELETES_BITMAP}"
            with open(os.path.join(gen.gen_dir, drel), "rb") as f:
                draw = f.read()
            stat.add_file(drel, len(draw))
            result = result - Bitmap.deserialize(draw)
        return sorted(result), {
            "path": "index",
            "bytes_read": stat.bytes_read,
            "bitmap_files": stat.bitmap_files,
            "files": list(stat.files),
            "record_lines_read": 0,
        }

    def _execute_scan(self, gen: Generation, flt: Filter, deleted: set[int]) -> tuple[list[int], dict[str, Any]]:
        from .generation import FileStat
        stat = FileStat()
        path = os.path.join(gen.gen_dir, RECORDS)
        size = os.path.getsize(path)
        hits: list[int] = []
        with open(path, "rb") as f:
            for ordinal, line in enumerate(f):
                if ordinal in deleted:
                    continue
                rec = json.loads(line.decode("utf-8"))
                stat.record_lines_read += 1  # 只统计实际解析判定的存活行
                if flt.matches(rec):
                    hits.append(ordinal)
        stat.add_file(RECORDS, size)
        return hits, {
            "path": "scan",
            "bytes_read": stat.bytes_read,
            "bitmap_files": 0,
            "files": list(stat.files),
            "record_lines_read": stat.record_lines_read,
        }

    def preview(self, query_id: str, offset: int = 0, limit: int = 20) -> dict[str, Any]:
        with self.lock:
            handle = self.queries.get(query_id)
            if handle is None:
                raise StoreError("查询不存在或已关闭")
            gen = self._load_generation(handle.gen_id)  # 旧代被回收/拒用即显式报错
            # 删除记录后不得再从索引结果读到：同时应用绑定时刻掩码与之后的新删除
            # （当前代掩码包含全部删除；旧代为不可变快照，掩码即绑定时刻内容）
            effective_deleted = set(gen.delete_mask) | set(handle.delete_snapshot)
            visible = [o for o in handle.ordinals if o not in effective_deleted]
            page = visible[offset : offset + limit]
            rows = []
            for ordinal in page:
                rec = gen.record_at(ordinal)
                if rec is None:
                    raise StaleGenerationError(f"代次 {handle.gen_id} 行号 {ordinal} 越界，映射与数据不一致")
                rows.append({"ordinal": ordinal, "event_id": gen.mapping[ordinal], "record": rec})
            return {
                "query_id": query_id,
                "gen_id": handle.gen_id,
                "total_hits": len(visible),
                "bound_hits": len(handle.ordinals),
                "offset": offset,
                "limit": limit,
                "rows": rows,
                "read_stat": handle.read_stat,
                "created_lsn": handle.created_lsn,
                "current_lsn": self.wal.next_lsn - 1,
                "stale": self.wal.next_lsn - 1 != handle.created_lsn,
            }

    def close_query(self, query_id: str) -> dict[str, Any]:
        with self.lock:
            handle = self.queries.pop(query_id, None)
            if handle is None:
                raise StoreError("查询不存在或已关闭")
            self.pins[handle.gen_id] = max(0, self.pins.get(handle.gen_id, 1) - 1)
            self._save_pins()
            reaped = self._reap_orphan_gens()
            return {"closed": query_id, "reaped": reaped}

    def list_queries(self) -> list[dict[str, Any]]:
        with self.lock:
            return [
                {
                    "query_id": q.query_id,
                    "gen_id": q.gen_id,
                    "combinator": q.combinator,
                    "conditions": [
                        {"field": c.field, "values": list(c.values)} for c in q.filter.conditions
                    ],
                    "hits": len(q.ordinals),
                    "index_enabled": q.index_enabled,
                    "created_lsn": q.created_lsn,
                }
                for q in self.queries.values()
            ]

    # ================= 压缩整理 ========================================
    def compaction_status(self) -> dict[str, Any]:
        with self.lock:
            c = self.compact
            return {
                "phase": c.phase,
                "target_gen": c.target_gen,
                "base_lsn": c.base_lsn,
                "fence_lsn": c.fence_lsn,
                "total_rows": c.total_rows,
                "done_rows": c.done_rows,
                "stage": c.stage,
                "request": c.request,
                "error": c.error,
                "updated_at": c.updated_at,
                "current_gen": self.current_gen.gen_id,
                "current_lsn": self.wal.next_lsn - 1,
            }

    def request_compaction_control(self, action: str) -> dict[str, Any]:
        with self.lock:
            if action not in ("pause", "resume", "abort"):
                raise StoreError("action 只能是 pause/resume/abort")
            c = self.compact
            if c.phase in ("idle", "done", "aborted", "conflict") and action != "resume":
                raise StoreError(f"当前没有运行中的整理任务（phase={c.phase}）")
            # resume 必须保留显式信号，由 worker 的暂停循环消化后自行清空；
            # pause/abort 同理。不能在这里置空，否则 worker 永远等不到唤醒
            c.request = action
            self._persist_compaction()
            return self.compaction_status()

    def start_compaction(self) -> dict[str, Any]:
        with self.lock:
            c = self.compact
            if c.phase in ("running", "paused", "committing"):
                raise ConflictError(f"已有整理任务进行中（phase={c.phase}）")
            target = self.current_gen.gen_id + 1
            c.__init__()
            c.phase = "running"
            c.target_gen = target
            c.base_lsn = self.wal.next_lsn - 1
            c.total_rows = len(self.live)
            c.updated_at = time.time()
            self._persist_compaction()
        t = threading.Thread(target=self._compaction_worker, name="compaction", daemon=True)
        t.start()
        return self.compaction_status()

    def _checkpoint(self) -> None:
        """整理线程在各阶段调用：处理暂停/中止请求（协作式）。"""
        c = self.compact
        c.updated_at = time.time()
        self._persist_compaction()
        if c.request == "abort":
            c.phase = "aborted"
            c.error = "用户中止"
            c.request = ""
            self._persist_compaction()
            raise _AbortCompaction()
        if c.request == "pause":
            c.phase = "paused"
            self._persist_compaction()
            # 暂停期间不断检查：恢复 / 中止
            while c.phase == "paused":
                time.sleep(0.02)
                if c.request == "abort":
                    c.phase = "aborted"
                    c.error = "暂停后用户中止"
                    c.request = ""
                    self._persist_compaction()
                    raise _AbortCompaction()
                if c.request == "resume":
                    c.request = ""
                    c.phase = "running"
                    self._persist_compaction()
                    break
            return
        # request == ""（含 resume 已在暂停循环内消化）：直接放行
        return

    def _compaction_worker(self) -> None:
        c = self.compact
        tmp_dir = os.path.join(self.gens_dir, f".build-gen-{c.target_gen:06d}")
        try:
            with self.lock:
                snapshot = [dict(r) for r in self.live.values()]
                base_lsn = self.wal.next_lsn - 1
            self._checkpoint()

            def progress(stage: str, done: int, total: int) -> None:
                c.stage = stage
                c.done_rows = done
                c.total_rows = total
                if done % 64 == 0 or done == total:
                    self._checkpoint()

            build_generation_on_disk(tmp_dir, c.target_gen, snapshot, base_lsn, progress)
            self._checkpoint()
            with self.lock:
                # 冲突检测：构建期间若发生过写入，当前 WAL 水位已超过基线
                latest_lsn = self.wal.next_lsn - 1
                if latest_lsn != base_lsn:
                    c.phase = "conflict"
                    c.fence_lsn = latest_lsn
                    c.error = (
                        f"整理基线 LSN {base_lsn}，暂停/构建期间数据已变化到 LSN {latest_lsn}，"
                        "本次产物不自动切换；可选择 abort 后重新整理"
                    )
                    self._persist_compaction()
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                    return
                # 原子切换 CURRENT
                final_dir = os.path.join(self.gens_dir, f"gen-{c.target_gen:06d}")
                if os.path.exists(final_dir):
                    shutil.rmtree(final_dir)
                os.replace(tmp_dir, final_dir)
                fsync_dir(self.gens_dir)
                atomic_write(os.path.join(self.data_dir, CURRENT_NAME), f"{c.target_gen:06d}".encode())
                new_gen = Generation(final_dir, c.target_gen).load()
                if new_gen.rejected:
                    raise RuntimeError(new_gen.reject_reason)
                self.current_gen = new_gen
                self.gen_cache[c.target_gen] = new_gen
                # WAL 完整保留：它是唯一事实来源，当前代日后损坏时仍可全量重放重建。
                # 新代 manifest 的 base_lsn 标记物化水位，但不截断日志。
                c.phase = "done"
                c.done_rows = c.total_rows
                self._persist_compaction()
            with self.lock:
                self._reap_orphan_gens()
        except _AbortCompaction:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception as exc:  # 中断/异常：清理半成品，状态落盘
            with self.lock:
                c.phase = "aborted"
                c.error = f"整理中断: {exc!r}"
                self._persist_compaction()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _persist_compaction(self) -> None:
        c = self.compact
        atomic_write(
            os.path.join(self.data_dir, COMPACT_NAME),
            json.dumps(c.__dict__, ensure_ascii=False).encode(),
        )

    def _recover_compaction(self) -> None:
        path = os.path.join(self.data_dir, COMPACT_NAME)
        if os.path.exists(path):
            with open(path, "rb") as f:
                saved = json.loads(f.read().decode())
            phase = saved.get("phase", "idle")
            if phase in ("running", "paused", "committing"):
                # 进程中断时任务未完成：标记 aborted，等待用户重新发起
                saved["phase"] = "aborted"
                saved["error"] = "进程重启，上次整理未完成（任务中断），临时构建目录已清理"
                atomic_write(path, json.dumps(saved, ensure_ascii=False).encode())
            self.compact = _CompactionState(**{k: saved.get(k, v) for k, v in _CompactionState().__dict__.items()})
        # 清理所有 .build-* 残骸
        for name in os.listdir(self.gens_dir):
            if name.startswith(".build-"):
                shutil.rmtree(os.path.join(self.gens_dir, name), ignore_errors=True)

    def _reap_orphan_gens(self) -> list[str]:
        """回收没有任何查询绑定、且不是 CURRENT 的旧代文件。"""
        current_id = self.current_gen.gen_id
        reaped: list[str] = []
        for name in sorted(os.listdir(self.gens_dir)):
            if not name.startswith("gen-"):
                continue
            gen_id = int(name.split("-")[1])
            if gen_id == current_id:
                continue
            if self.pins.get(gen_id, 0) > 0:
                continue  # 旧查询仍绑定，旧代文件必须保留
            path = os.path.join(self.gens_dir, name)
            shutil.rmtree(path, ignore_errors=True)
            self.gen_cache.pop(gen_id, None)
            reaped.append(name)
        return reaped

    # ================= 观测 ============================================
    def overview(self) -> dict[str, Any]:
        with self.lock:
            gen = self.current_gen
            gens = []
            for name in sorted(os.listdir(self.gens_dir)):
                if not name.startswith("gen-"):
                    continue
                gid = int(name.split("-")[1])
                gens.append(
                    {
                        "gen_id": gid,
                        "is_current": gid == gen.gen_id,
                        "pins": self.pins.get(gid, 0),
                        "rows": self.gen_cache[gid].row_count if gid in self.gen_cache else None,
                        "size_bytes": _dir_size(os.path.join(self.gens_dir, name)),
                    }
                )
            return {
                "current_gen": gen.gen_id,
                "current_lsn": self.wal.next_lsn - 1,
                "live_records": len(self.live),
                "deleted_rows": len(gen.delete_mask),
                "updated_rows": len(gen.update_mask),
                "total_rows": gen.row_count,
                "bitmap_backend": "roaring" if gen.delete_mask.is_roaring else "pyint",
                "generations": gens,
                "rejected": sorted(os.listdir(self.rejected_dir)),
                "distinct": gen.distinct_values(),
            }


class _AbortCompaction(Exception):
    pass


def _dir_size(path: str) -> int:
    total = 0
    for root, _d, files in os.walk(path):
        for name in files:
            total += os.path.getsize(os.path.join(root, name))
    return total
