"""核心功能：插入、多值标签、空值/空集合、扫描基准对账。"""
import pytest

from conftest import put, make_filter, ids_of, EMPTY
from app.filter import StoreError
from app.store import EventStore


def test_empty_store_query(store):
    q = store.create_query({"combinator": "AND", "conditions": []}, True)
    assert len(q.ordinals) == 0
    pv = store.preview(q.query_id, 0, 10)
    assert pv["rows"] == [] and pv["total_hits"] == 0


def test_single_value_index_and_scan_agree(store):
    put(store, "E1", "华东", "app", ["vip"])
    put(store, "E2", "华东", "web", ["beta"])
    put(store, "E3", "华北", "app", ["vip", "enterprise"])  # 一条记录命中多个标签
    put(store, "E4", "西南", "partner", [])                  # 标签空集合
    put(store, "E5", EMPTY, EMPTY, ["risk"])                 # 单值字段空值

    flt = make_filter([("region", {"华东"})])
    qi = store.create_query(flt, True)
    qs = store.create_query(flt, False)
    assert qi.ordinals == qs.ordinals
    got, total = ids_of(store, qi.query_id)
    assert got == {"E1", "E2"} and total == 2


def test_multivalue_record_hits_multiple_tag_bitmaps(store):
    put(store, "E1", tags=["vip", "enterprise", "beta"])
    put(store, "E2", tags=["vip"])
    for tag in ("vip", "enterprise", "beta"):
        flt = make_filter([("tags", {tag})])
        q = store.create_query(flt, True)
        s = store.create_query(flt, False)
        assert q.ordinals == s.ordinals
        got, _ = ids_of(store, q.query_id)
        assert "E1" in got
    # vip 应同时命中 E1/E2
    got, total = ids_of(store, store.create_query(make_filter([("tags", {"vip"})]), True).query_id)
    assert got == {"E1", "E2"} and total == 2


def test_empty_tag_set_distinct_from_null_single_values(store):
    put(store, "E1", region="华东", tags=[])          # 空集合
    put(store, "E2", region=EMPTY, tags=["vip"])      # region 空值
    put(store, "E3", region="华东", tags=["vip"])

    # 空集合位图只命中 E1
    q = store.create_query(make_filter([("tags", {EMPTY})]), True)
    got, _ = ids_of(store, q.query_id)
    assert got == {"E1"}
    # region 空值位图只命中 E2 —— 两种“空”分开存储
    q = store.create_query(make_filter([("region", {EMPTY})]), True)
    got, _ = ids_of(store, q.query_id)
    assert got == {"E2"}
    # 磁盘上确实是两个不同的位图文件
    gen = store.current_gen
    assert ("tags", EMPTY) in gen.bitmaps
    assert ("region", EMPTY) in gen.bitmaps


def test_and_or_combinators(store):
    put(store, "E1", "华东", "app", ["vip"])
    put(store, "E2", "华东", "web", ["risk"])
    put(store, "E3", "华北", "app", ["risk"])

    and_f = make_filter([("region", {"华东"}), ("channel", {"app"})], "AND")
    or_f = make_filter([("region", {"华东"}), ("channel", {"app"})], "OR")
    assert ids_of(store, store.create_query(and_f, True).query_id)[0] == {"E1"}
    assert ids_of(store, store.create_query(or_f, True).query_id)[0] == {"E1", "E2", "E3"}
    # 多值字段内部恒为 OR
    tag_f = make_filter([("tags", {"vip", "risk"})])
    assert ids_of(store, store.create_query(tag_f, True).query_id)[0] == {"E1", "E2", "E3"}


def test_scan_is_correctness_baseline_across_all_filters(store):
    import itertools
    from scripts_seed_helper import build_sample

    for rec in build_sample(60):
        store.put_event(rec)
    values = {f: {v for (ff, v) in store.current_gen.bitmaps if ff == f}
              for f in ("region", "channel", "tags")}
    # 随机（确定性）挑一批过滤器，两条路径必须完全一致
    combos = list(itertools.product(
        [("region", x) for x in itertools.combinations(sorted(values["region"]), 2)],
        [("tags", x) for x in itertools.combinations(sorted(values["tags"]), 2)],
    ))[::7]
    for (f1, v1), (f2, v2) in combos:
        for combinator in ("AND", "OR"):
            flt = make_filter([(f1, set(v1)), (f2, set(v2))], combinator)
            qi = store.create_query(flt, True)
            qs = store.create_query(flt, False)
            assert qi.ordinals == qs.ordinals, (flt, combinator)


def test_duplicate_business_id_rejected(store):
    put(store, "DUP-1", "华东", "app", ["vip"])
    with pytest.raises(StoreError, match="已存在"):
        put(store, "DUP-1", "华北", "web", [])
    # 被拒后原记录不变
    q = store.create_query(make_filter([("region", {"华东"})]), True)
    assert ids_of(store, q.query_id)[0] == {"DUP-1"}


def test_persistence_mapping_is_real_and_reloadable(store, tmp_path):
    put(store, "E1", "华东", "app", ["vip"])
    put(store, "E2", "华北", "web", [])
    data_dir = store.data_dir
    del store
    reloaded = EventStore(data_dir)
    q = reloaded.create_query(make_filter([("tags", {"vip"})]), True)
    got, _ = ids_of(reloaded, q.query_id)
    assert got == {"E1"}
    # mapping.json 是真实磁盘文件，行号与业务编号一一对应
    import json, os
    mapping_path = os.path.join(reloaded.current_gen.gen_dir, "mapping.json")
    assert os.path.isfile(mapping_path)
    mapping = json.loads(open(mapping_path, encoding="utf-8").read())
    assert mapping == ["E1", "E2"]
