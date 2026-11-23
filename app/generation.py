"""一个数据代次（generation）的磁盘布局与内存视图。

目录结构（``data/gens/gen-000007/``）::

    manifest.json   代次元数据 + 每个文件的 sha256 校验和
    records.jsonl   行号顺序的业务记录，行号 = 该文件中的行序（0 起）
    mapping.json    行号 -> 业务编号映射（真实持久化，非内存临时表）
    masks.json      删除掩码与更新掩码（仅当前代会变化，原子重写）
    bitmaps/
        region__华东.bitm   单值字段：该值出现在哪些行
        region__%40EMPTY.bitm        空值行
        tags__vip.bitm              多值字段：持有该标签的行
        tags__%40EMPTY.bitm          空集合行（与单值空值分开存储）
        deletes.bitm      已删除行（与 masks.json 双写，便于核对）

所有位图文件带魔数（见 app/bitmap.py），manifest 中登记 sha256 与字节数。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from .bitmap import Bitmap
from .filter import EMPTY, ALL_FIELDS
from .fsutil import atomic_write, sha256_file

MANIFEST = "manifest.json"
RECORDS = "records.jsonl"
MAPPING = "mapping.json"
MASKS = "masks.json"
BITMAP_DIR = "bitmaps"
DELETES_BITMAP = "deletes.bitm"


def bitmap_relpath(field: str, value: str) -> str:
    # 文件名做百分号编码，避免 "/" 等字符破坏路径；EMPTY 也只是一个普通值
    safe = value.replace("%", "%25").replace("/", "%2F")
    return f"{BITMAP_DIR}/{field}__{safe}.bitm"


def bitmap_key_from_relpath(rel: str) -> tuple[str, str]:
    name = rel[len(BITMAP_DIR) + 1 : -len(".bitm")]
    field, value = name.split("__", 1)
    value = value.replace("%2F", "/").replace("%25", "%")
    return field, value


@dataclass
class FileStat:
    bytes_read: int = 0
    bitmap_files: int = 0
    record_lines_read: int = 0
    files: tuple[str, ...] = ()

    def add_file(self, relpath: str, nbytes: int) -> None:
        self.bytes_read += nbytes
        self.files = self.files + (relpath,)
        if relpath.endswith(".bitm"):
            self.bitmap_files += 1
        # 注意：record_lines_read 由扫描循环自行累加，这里不重复计数


class Generation:
    """不可变代次视图（旧查询绑定的就是它）。

    当前代也以同一格式落盘，并额外允许重写 masks/位图。
    """

    def __init__(self, gen_dir: str, gen_id: int) -> None:
        self.gen_dir = gen_dir
        self.gen_id = gen_id
        self.manifest_path = os.path.join(gen_dir, MANIFEST)
        self.rejected = False
        self.reject_reason = ""
        self.manifest: dict[str, Any] = {}
        self.records: list[dict[str, Any]] = []
        self.mapping: list[str] = []          # 行号 -> event_id
        self.event_to_ord: dict[str, int] = {}
        self.bitmaps: dict[tuple[str, str], Bitmap] = {}
        self.delete_mask: Bitmap = Bitmap()
        self.update_mask: Bitmap = Bitmap()
        self.base_lsn: int = 0
        self.row_count: int = 0

    # ---- 加载与校验 -----------------------------------------------------
    def load(self, verify: bool = True) -> "Generation":
        try:
            with open(self.manifest_path, "rb") as f:
                self.manifest = json.loads(f.read().decode("utf-8"))
        except FileNotFoundError:
            return self._reject(f"代次 {self.gen_id} 缺少 manifest.json")
        except (ValueError, UnicodeDecodeError) as exc:
            return self._reject(f"代次 {self.gen_id} manifest 解析失败: {exc}")

        if verify:
            self._verify_files()
            if self.rejected:
                return self

        self._load_records_and_mapping()
        self._load_bitmaps()
        self._load_masks()
        self.base_lsn = int(self.manifest.get("base_lsn", 0))
        self.row_count = len(self.records)
        return self

    def _reject(self, reason: str) -> "Generation":
        self.rejected = True
        self.reject_reason = reason
        return self

    def _verify_files(self) -> None:
        files_meta = self.manifest.get("files", {})
        for rel, meta in files_meta.items():
            path = os.path.join(self.gen_dir, rel)
            if not os.path.exists(path):
                self._reject(f"代次 {self.gen_id} 缺少文件 {rel}")
                return
            actual_size = os.path.getsize(path)
            if actual_size != meta["size"]:
                self._reject(f"代次 {self.gen_id} 文件 {rel} 字节数不符（可能被截断/替换）")
                return
            if sha256_file(path) != meta["sha256"]:
                self._reject(f"代次 {self.gen_id} 文件 {rel} 校验和不匹配（疑似损坏或旧代伪造文件）")
                return

    def _load_records_and_mapping(self) -> None:
        mapping_path = os.path.join(self.gen_dir, MAPPING)
        with open(mapping_path, "rb") as f:
            self.mapping = json.loads(f.read().decode("utf-8"))
        with open(os.path.join(self.gen_dir, RECORDS), "rb") as f:
            for line in f:
                line = line.strip()
                if line:
                    self.records.append(json.loads(line.decode("utf-8")))
        if len(self.records) != len(self.mapping):
            self._reject(f"代次 {self.gen_id} 行数与映射长度不一致")
            return
        self.event_to_ord = {eid: i for i, eid in enumerate(self.mapping)}
        if len(self.event_to_ord) != len(self.mapping):
            self._reject(f"代次 {self.gen_id} 行号映射中存在重复业务编号")

    def _load_bitmaps(self) -> None:
        for rel in self.manifest.get("files", {}):
            if not rel.startswith(BITMAP_DIR + "/") or not rel.endswith(".bitm"):
                continue
            if rel == f"{BITMAP_DIR}/{DELETES_BITMAP}":
                continue
            try:
                with open(os.path.join(self.gen_dir, rel), "rb") as f:
                    bm = Bitmap.deserialize(f.read())
            except ValueError as exc:
                self._reject(f"代次 {self.gen_id} 位图 {rel} 无法反序列化: {exc}")
                return
            self.bitmaps[bitmap_key_from_relpath(rel)] = bm

    def _load_masks(self) -> None:
        path = os.path.join(self.gen_dir, MASKS)
        if os.path.exists(path):
            with open(path, "rb") as f:
                data = json.loads(f.read().decode("utf-8"))
            self.delete_mask = Bitmap.deserialize(bytes.fromhex(data["delete_mask"]))
            self.update_mask = Bitmap.deserialize(bytes.fromhex(data.get("update_mask", ""))) if data.get("update_mask") else Bitmap()
        dpath = os.path.join(self.gen_dir, BITMAP_DIR, DELETES_BITMAP)
        if os.path.exists(dpath):
            with open(dpath, "rb") as f:
                disk_deletes = Bitmap.deserialize(f.read())
            if [i for i in disk_deletes] != [i for i in self.delete_mask]:
                self._reject(f"代次 {self.gen_id} 删除掩码与 deletes.bitm 不一致")

    # ---- 查询用 ---------------------------------------------------------
    def get_bitmap(self, field: str, value: str) -> Bitmap:
        return self.bitmaps.get((field, value), Bitmap())

    def record_at(self, ordinal: int) -> dict[str, Any] | None:
        if 0 <= ordinal < len(self.records):
            return self.records[ordinal]
        return None

    def live_ordinals(self) -> list[int]:
        deleted = set(self.delete_mask)
        return [i for i in range(self.row_count) if i not in deleted]

    def distinct_values(self) -> dict[str, list[dict[str, Any]]]:
        """供前端构建筛选面板：每个字段的可选值与命中行数（未删除）。"""
        out: dict[str, list[dict[str, Any]]] = {}
        deleted = set(self.delete_mask)
        for field in ALL_FIELDS:
            rows: list[dict[str, Any]] = []
            for (f, value), bm in self.bitmaps.items():
                if f != field:
                    continue
                count = sum(1 for i in bm if i not in deleted)
                rows.append({"value": value, "count": count, "is_empty": value == EMPTY})
            rows.sort(key=lambda r: (r["is_empty"], r["value"]))
            out[field] = rows
        return out


# ----------------------------------------------------------------------
# 构建：给定全量记录，落盘一个新代次
# ----------------------------------------------------------------------
def build_generation_on_disk(
    gen_dir: str,
    gen_id: int,
    records: list[dict[str, Any]],
    base_lsn: int,
    progress_cb=None,
) -> Generation:
    """把全量记录整理成一个新代次并原子落盘（目录先写临时名由调用方负责）。

    records 必须已去重、已排除删除；行号按 event_id 排序，保证整理确定。
    progress_cb(stage, done, total) 用于暂停/中断测试观察进度。
    """
    os.makedirs(os.path.join(gen_dir, BITMAP_DIR), exist_ok=True)
    records = sorted(records, key=lambda r: r["event_id"])
    mapping = [r["event_id"] for r in records]

    index: dict[tuple[str, str], Bitmap] = {}
    for ordinal, rec in enumerate(records):
        if progress_cb:
            progress_cb("index", ordinal, len(records))
        for field in ("region", "channel"):
            key = (field, rec[field])  # 单值列，空值已归一为 EMPTY
            index.setdefault(key, Bitmap()).add(ordinal)
        if rec["tags"]:
            for tag in rec["tags"]:  # 多值列：一条记录可命中多个标签
                index.setdefault(("tags", tag), Bitmap()).add(ordinal)
        else:
            index.setdefault(("tags", EMPTY), Bitmap()).add(ordinal)  # 空集合单独保存

    files_meta: dict[str, dict[str, Any]] = {}

    def write_file(rel: str, data: bytes) -> None:
        path = os.path.join(gen_dir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        files_meta[rel] = {"size": len(data), "sha256": _sha(data)}

    # records.jsonl
    lines = [json.dumps(r, ensure_ascii=False).encode("utf-8") + b"\n" for r in records]
    write_file(RECORDS, b"".join(lines))
    # mapping.json —— 行号到业务编号的真实持久化映射
    write_file(MAPPING, json.dumps(mapping, ensure_ascii=False).encode("utf-8"))
    # 位图
    if progress_cb:
        progress_cb("flush", 0, len(index))
    for k, (key, bm) in enumerate(index.items()):
        if progress_cb:
            progress_cb("flush", k, len(index))
        write_file(bitmap_relpath(*key), bm.serialize())
    # 新代次初始无删除
    empty = Bitmap()
    write_file(f"{BITMAP_DIR}/{DELETES_BITMAP}", empty.serialize())
    write_file(MASKS, json.dumps({"delete_mask": empty.serialize().hex(), "update_mask": ""}).encode("utf-8"))

    manifest = {
        "gen_id": gen_id,
        "base_lsn": base_lsn,
        "row_count": len(records),
        "created_from": "compaction",
        "files": files_meta,
        "bitmap_format": "roaring" if empty.is_roaring else "pyint",
    }
    atomic_write(os.path.join(gen_dir, MANIFEST), json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
    gen = Generation(gen_dir, gen_id).load()
    if gen.rejected:
        raise RuntimeError(f"新代次自检失败: {gen.reject_reason}")
    return gen


def _sha(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()
