"""更正、删除、压缩整理、代次绑定与冲突处理。"""
import os
import time

import pytest

from conftest import put, make_filter, ids_of, EMPTY
from app.filter import StoreError, StaleGenerationError
from app.store import EventStore


def test_category_correction_withdraws_old_bitmap_position(store):
    put(store, "E1", "华东", "app", ["vip"])
    put(store, "E2", "华北", "web", ["beta"])
    store.update_event("E1", {"region": "华北"})

    # 旧位置（华东）必须撤下
    q_old = store.create_query(make_filter([("region", {"华东"})]), True)
    assert ids_of(store, q_old.query_id)[0] == set()
    # 新位置（华北）必须可命中，且与扫描基准一致
    flt = make_filter([("region", {"华北"})])
    q_idx = store.create_query(flt, True)
    q_scan = store.create_query(flt, False)
    assert q_idx.ordinals == q_scan.ordinals
    assert ids_of(store, q_idx.query_id)[0] == {"E1", "E2"}
    # records.jsonl 中该行内容已同步更正，行号映射不变
    gen = store.current_gen
    ordinal = gen.event_to_ord["E1"]
    assert gen.records[ordinal]["region"] == "华北"
    assert gen.mapping[ordinal] == "E1"
    assert ordinal in gen.update_mask


def test_tag_correction_add_and_remove_positions(store):
    put(store, "E1", tags=["vip"])
    store.update_event("E1", {"tags": ["enterprise", "beta"]})
    flt_vip = make_filter([("tags", {"vip"})])
    flt_ent = make_filter([("tags", {"enterprise"})])
    assert ids_of(store, store.create_query(flt_vip, True).query_id)[0] == set()
    assert ids_of(store, store.create_query(flt_ent, True).query_id)[0] == {"E1"}
    # 变成空集合：空集合位图应重新出现
    store.update_event("E1", {"tags": []})
    q = store.create_query(make_filter([("tags", {EMPTY})]), True)
    assert ids_of(store, q.query_id)[0] == {"E1"}


def test_deleted_record_never_readable_from_index(store):
    put(store, "E1", "华东", "app", ["vip"])
    put(store, "E2", "华东", "web", ["beta"])
    q_before = store.create_query(make_filter([("region", {"华东"})]), True)
    assert ids_of(store, q_before.query_id)[0] == {"E1", "E2"}

    store.delete_event("E1")
    # 新查询读不到
    q_after = store.create_query(make_filter([("region", {"华东"})]), True)
    got, total = ids_of(store, q_after.query_id)
    assert got == {"E2"} and total == 1
    # 删除掩码落盘，重启后依然读不到
    data_dir = store.data_dir
    del store
    reloaded = EventStore(data_dir)
    q = reloaded.create_query(make_filter([("region", {"华东"})]), True)
    assert ids_of(reloaded, q.query_id)[0] == {"E2"}
    # 再删已删除编号应报错
    with pytest.raises(StoreError):
        reloaded.delete_event("E1")


def test_query_pins_generation_and_survives_compaction(store):
    put(store, "E1", "华东", "app", ["vip"])
    put(store, "E2", "华北", "web", ["beta"])
    put(store, "E3", "华南", "app", ["risk", "vip"])

    # 旧查询绑定 gen-1
    old_q = store.create_query(make_filter([("tags", {"vip"})]), True)
    old_gen = old_q.gen_id
    old_hits = ids_of(store, old_q.query_id)[0]
    assert old_hits == {"E1", "E3"}

    store.start_compaction()
    _wait_phase(store, {"done", "conflict", "aborted"})
    assert store.compaction_status()["phase"] == "done"
    assert store.current_gen.gen_id == old_gen + 1

    # 旧代目录仍在（被 pin），旧查询仍按旧代映射读到同样的真实行
    assert os.path.isdir(os.path.join(store.gens_dir, f"gen-{old_gen:06d}"))
    assert ids_of(store, old_q.query_id)[0] == old_hits
    assert store.pins[old_gen] == 1

    # 新查询走新代
    new_q = store.create_query(make_filter([("tags", {"vip"})]), True)
    assert new_q.gen_id == store.current_gen.gen_id
    assert ids_of(store, new_q.query_id)[0] == old_hits

    # 关闭旧查询后旧代被回收
    result = store.close_query(old_q.query_id)
    assert f"gen-{old_gen:06d}" in result["reaped"]
    assert not os.path.exists(os.path.join(store.gens_dir, f"gen-{old_gen:06d}"))
    # 关闭后句柄已失效，再访问即显式报错（不会静默套用新代位图）
    with pytest.raises(StoreError):
        store.preview(old_q.query_id)


def test_compaction_pause_modify_then_conflict(store):
    for rec in _sample(40):
        store.put_event(rec)
    # 暂停整理 -> 在另一处修改事件 -> 恢复 -> 必须检出冲突且不切换
    store.start_compaction()
    _wait_phase(store, {"paused", "running"})
    store.request_compaction_control("pause")
    _wait_phase(store, {"paused"})
    base_lsn = store.compaction_status()["base_lsn"]

    # “另一处”修改：直接通过同一个 store 的写入接口模拟并发客户端
    put(store, "EVT-900001", "华东", "app", ["vip"])
    store.update_event("EVT-000001", {"region": "西南"})

    store.request_compaction_control("resume")
    _wait_phase(store, {"done", "conflict", "aborted"})
    c = store.compaction_status()
    assert c["phase"] == "conflict"
    assert c["fence_lsn"] > base_lsn
    # CURRENT 未切换，数据仍然完整可查（扫描基准对账）
    q = store.create_query({"combinator": "AND", "conditions": []}, False)
    assert q.ordinals == sorted(range(len(store.live)))
    # 临时构建目录被清理
    leftovers = [n for n in os.listdir(store.gens_dir) if n.startswith(".build-")]
    assert leftovers == []
    # 中止后可以重新整理成功
    store.start_compaction()
    _wait_phase(store, {"done", "conflict", "aborted"})
    assert store.compaction_status()["phase"] == "done"


def test_compaction_abort_cleans_partial_generation(store):
    for rec in _sample(30):
        store.put_event(rec)
    store.start_compaction()
    _wait_phase(store, {"paused", "running"})
    store.request_compaction_control("abort")
    _wait_phase(store, {"aborted"})
    gens = [n for n in os.listdir(store.gens_dir) if n.startswith("gen-")]
    builds = [n for n in os.listdir(store.gens_dir) if n.startswith(".build-")]
    assert builds == []
    assert gens == [f"gen-{store.current_gen.gen_id:06d}"]


def test_interrupted_compaction_marked_after_restart(store):
    for rec in _sample(20):
        store.put_event(rec)
    store.start_compaction()
    _wait_phase(store, {"paused", "running"})
    store.request_compaction_control("pause")
    _wait_phase(store, {"paused"})
    data_dir = store.data_dir
    del store  # 模拟进程在暂停状态被杀掉

    reloaded = EventStore(data_dir)
    assert reloaded.compaction_status()["phase"] == "aborted"
    assert "中断" in reloaded.compaction_status()["error"]
    # 残骸目录已清理，服务可正常查询、可重新整理
    assert [n for n in os.listdir(reloaded.gens_dir) if n.startswith(".build-")] == []
    reloaded.start_compaction()
    _wait_phase(reloaded, {"done", "conflict", "aborted"})
    assert reloaded.compaction_status()["phase"] == "done"


def test_tampered_old_generation_is_quarantined_and_rejected(store):
    for rec in _sample(20):
        store.put_event(rec)
    old_q = store.create_query(make_filter([("tags", {"vip"})]), True)
    old_gen_id = old_q.gen_id
    store.start_compaction()
    _wait_phase(store, {"done"})
    old_dir = os.path.join(store.gens_dir, f"gen-{old_gen_id:06d}")
    assert os.path.isdir(old_dir)

    # 篡改旧代的一个位图文件（模拟伪造/损坏的旧代索引）
    target = None
    for name in os.listdir(os.path.join(old_dir, "bitmaps")):
        if name != "deletes.bitm":
            target = os.path.join(old_dir, "bitmaps", name)
            break
    with open(target, "r+b") as f:
        f.seek(2)
        f.write(b"\xff\xff\xff\xff")
    with pytest.raises(StaleGenerationError):
        store.preview(old_q.query_id)
    # 被移到 rejected 隔离区并附带原因说明
    rejected = os.listdir(store.rejected_dir)
    assert any(n.startswith(f"gen-{old_gen_id:06d}") for n in rejected)
    reason = open(os.path.join(store.rejected_dir, rejected[0], "REJECTED.txt"), encoding="utf-8").read()
    assert "校验和" in reason


def test_index_reads_far_less_than_scan(store):
    for rec in _sample(400):
        store.put_event(rec)
    flt = make_filter([("region", {"华东"}), ("tags", {"vip"})], "AND")
    qi = store.create_query(flt, True)
    qs = store.create_query(flt, False)
    assert qi.ordinals == qs.ordinals and len(qi.ordinals) > 0
    assert qi.read_stat["path"] == "index" and qs.read_stat["path"] == "scan"
    # 扫描必须读完全部记录行；索引只读几个位图文件
    assert qs.read_stat["record_lines_read"] == store.current_gen.row_count
    assert qi.read_stat["bitmap_files"] >= 2
    assert qi.read_stat["bytes_read"] < qs.read_stat["bytes_read"] / 3


def test_bound_query_isolation_against_later_delete(store):
    put(store, "E1", "华东", "app", ["vip"])
    put(store, "E2", "华东", "web", ["beta"])
    q = store.create_query(make_filter([("region", {"华东"})]), True)
    store.delete_event("E2")
    # 绑定时刻的结果不受之后删除影响的是“命中集合”，但 preview 必须扣除删除快照之外的新删除
    pv = store.preview(q.query_id, 0, 100)
    assert {r["event_id"] for r in pv["rows"]} == {"E1"}
    assert pv["stale"] is True


# ----------------------------------------------------------------------
def _sample(n):
    from scripts_seed_helper import build_sample
    return build_sample(n)


def _wait_phase(store, targets, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        phase = store.compaction_status()["phase"]
        if phase in targets:
            return phase
        time.sleep(0.02)
    raise AssertionError(f"等待整理阶段 {targets} 超时，当前: {store.compaction_status()}")
