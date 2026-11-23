"""事件模式与查询过滤器。

事件记录逻辑字段::

    event_id : str        业务编号（唯一）
    region   : str | None 地区，单值类别列
    channel  : str | None 渠道，单值类别列
    tags     : list[str]  标签，多值字段（可为空集合）

单值字段的空值（None / 空串）与多值字段的空集合是两种不同语义，
分别建位图，统一用哨兵 ``EMPTY`` 表示，但落在不同字段的索引里。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

EMPTY = "__EMPTY__"  # 空值 / 空集合哨兵，业务侧真实值不允许等于它

SINGLE_FIELDS = ("region", "channel")
MULTI_FIELDS = ("tags")
ALL_FIELDS = ("region", "channel", "tags")


class StoreError(Exception):
    """业务可处理错误（重复编号、字段非法等），HTTP 层映射为 4xx。"""


class StaleGenerationError(StoreError):
    """查询绑定的代次已被拒用（文件损坏或显式隔离）。"""


class ConflictError(StoreError):
    """整理暂停期间数据被改动等冲突场景。"""


def normalize_single(value: Any) -> str:
    if value is None:
        return EMPTY
    value = str(value).strip()
    return value if value else EMPTY


def normalize_tags(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)):
        raise StoreError("tags 必须是字符串数组")
    out: list[str] = []
    for t in value:
        t = str(t).strip()
        if not t or t == EMPTY:
            continue
        if t not in out:
            out.append(t)
    return out


def validate_record(data: dict[str, Any]) -> dict[str, Any]:
    event_id = str(data.get("event_id", "")).strip()
    if not event_id:
        raise StoreError("event_id 不能为空")
    if event_id == EMPTY:
        raise StoreError(f"event_id 不能等于保留值 {EMPTY}")
    region = normalize_single(data.get("region"))
    channel = normalize_single(data.get("channel"))
    tags = normalize_tags(data.get("tags"))
    return {
        "event_id": event_id,
        "region": region,
        "channel": channel,
        "tags": tags,
        "payload": data.get("payload", {}),
    }


@dataclass(frozen=True)
class Condition:
    """一个字段上的取值条件；values 之间为 OR。"""

    field: str
    values: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.field not in ALL_FIELDS:
            raise StoreError(f"不支持的筛选字段: {self.field}")
        if not self.values:
            raise StoreError("条件取值不能为空")

    @property
    def multi(self) -> bool:
        return self.field == MULTI_FIELDS


@dataclass
class Filter:
    """组合过滤器。

    combinator: 条件组之间 AND（交集）还是 OR（并集）；
    每个条件内部多值恒为 OR。
    """

    conditions: list[Condition] = field(default_factory=list)
    combinator: str = "AND"

    def __post_init__(self) -> None:
        if self.combinator not in ("AND", "OR"):
            raise StoreError("combinator 只能是 AND 或 OR")

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "Filter":
        data = data or {}
        conds: list[Condition] = []
        for raw in data.get("conditions", []):
            values = tuple(str(v) for v in raw.get("values", []) if str(v) != "")
            if values:
                conds.append(Condition(field=str(raw["field"]), values=values))
        return cls(conditions=conds, combinator=str(data.get("combinator", "AND")).upper())

    def matches(self, record: dict[str, Any]) -> bool:
        """扫描路径的真值判定，作为位图路径的正确性基准。"""
        if not self.conditions:
            return True
        results = [self._match_one(c, record) for c in self.conditions]
        return all(results) if self.combinator == "AND" else any(results)

    @staticmethod
    def _match_one(cond: Condition, record: dict[str, Any]) -> bool:
        if cond.multi:
            tags = record.get("tags", [])
            wanted = set(cond.values)
            if EMPTY in wanted and not tags:
                return True
            return bool(wanted.intersection(tags))
        actual = record.get(cond.field) or EMPTY
        return actual in set(cond.values)
