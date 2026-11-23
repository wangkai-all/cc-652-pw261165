import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.store import EventStore
from app.filter import EMPTY


@pytest.fixture()
def store(tmp_path):
    s = EventStore(str(tmp_path / "data"))
    yield s


def put(store, eid, region=EMPTY, channel=EMPTY, tags=None, payload=None):
    return store.put_event({
        "event_id": eid,
        "region": None if region == EMPTY else region,
        "channel": None if channel == EMPTY else channel,
        "tags": [] if tags is None else tags,
        "payload": payload or {},
    })


def make_filter(groups, combinator="AND"):
    return {"combinator": combinator, "conditions": [
        {"field": f, "values": list(v)} for f, v in groups if v
    ]}


def ids_of(store, qid):
    """返回查询命中的业务编号集合。"""
    pv = store.preview(qid, 0, 10000)
    return {r["event_id"] for r in pv["rows"]}, pv["total_hits"]
