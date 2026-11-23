"""预写日志（WAL）。

每条记录是一行 JSON，以 ``\\n`` 结尾，字段：

- ``lsn``: 单调递增日志序号，从 1 开始
- ``op``: ``put``（插入或全量替换一条事件）/ ``update``（按 event_id 更正单字段）/ ``delete``
- ``event_id``: 业务编号（字符串）
- ``record`` / ``changes``: 事件内容或字段更正
- ``ts``: 写入时间戳

WAL 是所有变更的唯一事实来源（source of truth）。内存状态、当前代的
删除掩码、压缩产生的新代快照都从它重放构建。
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

from .fsutil import append_fsync

WAL_NAME = "wal.log"


@dataclass(frozen=True)
class WalEntry:
    lsn: int
    op: str
    event_id: str
    payload: dict[str, Any]
    ts: float


class WAL:
    def __init__(self, path: str) -> None:
        self.path = path
        self._next_lsn = 1
        self._fh = None
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        if os.path.exists(path):
            self._next_lsn = self._scan_next_lsn()

    def _scan_next_lsn(self) -> int:
        """扫描已有 WAL，返回下一个可用 lsn；尾部残缺行直接截断。"""
        max_lsn = 0
        valid_bytes = 0
        with open(self.path, "rb") as f:
            while True:
                line = f.readline()
                if not line:
                    break
                if not line.endswith(b"\n"):
                    # 最后一行不完整（崩溃于 write 中途），截断丢弃
                    break
                try:
                    obj = json.loads(line.decode("utf-8"))
                    max_lsn = max(max_lsn, int(obj["lsn"]))
                    valid_bytes += len(line)
                except (ValueError, KeyError, UnicodeDecodeError):
                    break
        cur_size = os.path.getsize(self.path)
        if cur_size != valid_bytes:
            with open(self.path, "r+b") as f:
                f.truncate(valid_bytes)
                f.flush()
                os.fsync(f.fileno())
        return max_lsn + 1

    @property
    def next_lsn(self) -> int:
        return self._next_lsn

    def append(self, op: str, event_id: str, payload: dict[str, Any]) -> WalEntry:
        entry = WalEntry(
            lsn=self._next_lsn,
            op=op,
            event_id=event_id,
            ts=time.time(),
            payload=payload,
        )
        line = json.dumps(
            {
                "lsn": entry.lsn,
                "op": entry.op,
                "event_id": entry.event_id,
                "ts": entry.ts,
                **entry.payload,
            },
            ensure_ascii=False,
        ).encode("utf-8") + b"\n"
        append_fsync(self.path, line)
        self._next_lsn += 1
        return entry

    @staticmethod
    def read_all(path: str) -> list[WalEntry]:
        if not os.path.exists(path):
            return []
        out: list[WalEntry] = []
        with open(path, "rb") as f:
            for line in f:
                if not line.endswith(b"\n"):
                    break
                obj = json.loads(line.decode("utf-8"))
                lsn = int(obj.pop("lsn"))
                op = obj.pop("op")
                event_id = obj.pop("event_id")
                ts = obj.pop("ts", 0.0)
                out.append(WalEntry(lsn=lsn, op=op, event_id=event_id, payload=obj, ts=ts))
        return out
