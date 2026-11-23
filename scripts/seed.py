"""生成确定性的演示事件数据。

覆盖场景：
- 一条记录命中多个标签（多值字段）
- 标签为空集合（与单值字段的空值分开）
- 地区/渠道为空值
- 足够多的行数，让「索引读取 vs 全表扫描」的字节数差异肉眼可见
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request

REGIONS = ["华东", "华北", "华南", "西南", None]          # None = 空值
CHANNELS = ["app", "web", "mini-program", "partner", None]
TAG_POOL = ["vip", "enterprise", "beta", "risk", "promo", "churn-risk"]


def gen_records(n: int) -> list[dict]:
    records = []
    for i in range(n):
        rid = f"EVT-{i:06d}"
        region = REGIONS[(i * 7 + 1) % len(REGIONS)]
        channel = CHANNELS[(i * 3 + 2) % len(CHANNELS)]
        # 确定性的多标签：不同行命中 0~3 个标签
        tags = [t for j, t in enumerate(TAG_POOL) if (i >> j) & 1]
        if i % 11 == 0:
            tags = []  # 空集合
        records.append(
            {
                "event_id": rid,
                "region": region,
                "channel": channel,
                "tags": tags,
                "payload": {"amount": (i * 137) % 10000, "seq": i},
            }
        )
    return records


def post(base: str, path: str, body: dict) -> dict:
    req = urllib.request.Request(
        base.rstrip("/") + path,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--count", type=int, default=400)
    args = ap.parse_args()
    records = gen_records(args.count)
    ok = 0
    for rec in records:
        try:
            post(args.base, "/api/events", rec)
            ok += 1
        except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
            detail = exc.read().decode("utf-8")
            print(f"跳过 {rec['event_id']}: {detail}", file=sys.stderr)
    print(f"已写入 {ok}/{len(records)} 条事件")


if __name__ == "__main__":
    main()
