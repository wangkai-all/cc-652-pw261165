"""测试用确定性样本构造（与 scripts/seed.py 同分布、零网络依赖）。"""
from app.filter import EMPTY

REGIONS = ["华东", "华北", "华南", "西南", EMPTY]
CHANNELS = ["app", "web", "mini-program", "partner", EMPTY]
TAG_POOL = ["vip", "enterprise", "beta", "risk", "promo", "churn-risk"]


def build_sample(n):
    out = []
    for i in range(n):
        tags = [t for j, t in enumerate(TAG_POOL) if (i >> j) & 1]
        if i % 11 == 0:
            tags = []
        out.append({
            "event_id": f"EVT-{i:06d}",
            "region": REGIONS[(i * 7 + 1) % len(REGIONS)],
            "channel": CHANNELS[(i * 3 + 2) % len(CHANNELS)],
            "tags": tags,
            "payload": {"seq": i},
        })
    return out
