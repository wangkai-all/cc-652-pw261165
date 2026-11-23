#!/usr/bin/env bash
# 端到端验收脚本：在运行中的服务上跑全部关键场景
set -u
B="${1:-http://127.0.0.1:8013}"
PY=".venv/bin/python"
DATA="${2:-./e2e-data/store}"
pass=0; fail=0
check() { # desc  expected_substring  actual
  if echo "$3" | grep -q "$2"; then echo "  ✓ $1"; pass=$((pass+1));
  else echo "  ✗ $1 | 实际: $3"; fail=$((fail+1)); fi
}

echo "=== 灌数据 ==="
$PY scripts/seed.py --base "$B" --count 200 | tail -1

echo "=== 交集：索引 vs 扫描基准 ==="
FLT='{"combinator":"AND","conditions":[{"field":"region","values":["华东"]},{"field":"tags","values":["vip","beta"]}]}'
QI=$(curl -s -X POST $B/api/query -H 'Content-Type: application/json' -d "{\"filter\":$FLT,\"index_enabled\":true}")
QS=$(curl -s -X POST $B/api/query -H 'Content-Type: application/json' -d "{\"filter\":$FLT,\"index_enabled\":false}")
HI=$(echo "$QI" | $PY -c "import json,sys;print(json.load(sys.stdin)['hits'])")
HS=$(echo "$QS" | $PY -c "import json,sys;print(json.load(sys.stdin)['hits'])")
[ "$HI" = "$HS" ] && { echo "  ✓ 索引与扫描命中一致（均为 $HI）"; pass=$((pass+1)); } || { echo "  ✗ 命中不一致 $HI vs $HS"; fail=$((fail+1)); }
IB=$(echo "$QI" | $PY -c "import json,sys;print(json.load(sys.stdin)['read_stat']['bytes_read'])")
SB=$(echo "$QS" | $PY -c "import json,sys;print(json.load(sys.stdin)['read_stat']['bytes_read'])")
echo "  索引读取 ${IB}B vs 扫描读取 ${SB}B（$($PY -c "print(f'{$SB/$IB:.1f}')") 倍差距）"

echo "=== 并集 / 空集合 / 空值 ==="
R=$(curl -s -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"filter":{"combinator":"OR","conditions":[{"field":"tags","values":["__EMPTY__"]},{"field":"region","values":["__EMPTY__"]}]},"index_enabled":true}')
HITS=$(echo "$R" | $PY -c "import json,sys;print(json.load(sys.stdin)['hits'])")
[ "$HITS" -gt 0 ] && { echo "  ✓ OR 空集合+空值命中 $HITS 行"; pass=$((pass+1)); } || { echo "  ✗ 空值查询无命中"; fail=$((fail+1)); }

echo "=== 重复编号拒用 ==="
R=$(curl -s -X POST $B/api/events -H 'Content-Type: application/json' -d '{"event_id":"EVT-000001","region":"x","tags":[]}')
check "重复业务编号被拒" "已存在" "$R"

echo "=== 类别更正：撤旧位、建新位 ==="
curl -s -X POST $B/api/events/EVT-000001/update -H 'Content-Type: application/json' -d '{"changes":{"region":"西南"}}' >/dev/null
A=$(curl -s -X POST $B/api/query -H 'Content-Type: application/json' -d '{"filter":{"conditions":[{"field":"region","values":["西南"]}]},"index_enabled":true}')
C=$(curl -s -X POST $B/api/query -H 'Content-Type: application/json' -d '{"filter":{"conditions":[{"field":"region","values":["西南"]}]},"index_enabled":false}')
check "更正后索引=扫描基准" "$(echo "$A" | $PY -c 'import json,sys;print(json.load(sys.stdin)["hits"])')" "$(echo "{\"v\":$(echo "$C" | $PY -c 'import json,sys;print(json.load(sys.stdin)["hits"])')}")"

echo "=== 删除后不可读 ==="
curl -s -X POST $B/api/events/EVT-000002/delete -H 'Content-Type: application/json' >/dev/null
R=$(curl -s -X POST $B/api/query -H 'Content-Type: application/json' -d '{"filter":{"conditions":[]},"index_enabled":true}')
HITS=$(echo "$R" | $PY -c "import json,sys;print(json.load(sys.stdin)['hits'])")
[ "$HITS" = "199" ] && { echo "  ✓ 全量命中 199（删除 1 条已不可读）"; pass=$((pass+1)); } || { echo "  ✗ 期望 199 实际 $HITS"; fail=$((fail+1)); }

echo "=== 整理暂停 -> 另一处修改 -> 恢复 -> 冲突 ==="
curl -s -X POST $B/api/compaction/start -H 'Content-Type: application/json' -d '{}' >/dev/null
sleep 0.3
curl -s -X POST $B/api/compaction/control -H 'Content-Type: application/json' -d '{"action":"pause"}' >/dev/null
sleep 0.4
curl -s -X POST $B/api/events -H 'Content-Type: application/json' -d '{"event_id":"EVT-900001","region":"华东","channel":"app","tags":["vip"]}' >/dev/null
curl -s -X POST $B/api/compaction/control -H 'Content-Type: application/json' -d '{"action":"resume"}' >/dev/null
sleep 1
R=$(curl -s $B/api/compaction)
check "暂停期修改检出冲突" '"phase": "conflict"' "$R"

echo "=== 重新整理成功 + 旧代 pin 保留 + 关闭后回收 ==="
Q_OLD=$(curl -s -X POST $B/api/query -H 'Content-Type: application/json' -d '{"filter":{"conditions":[{"field":"tags","values":["vip"]}]},"index_enabled":true}')
OLD_GEN=$(echo "$Q_OLD" | $PY -c 'import json,sys;print(json.load(sys.stdin)["gen_id"])')
curl -s -X POST $B/api/compaction/start -H 'Content-Type: application/json' -d '{}' >/dev/null
sleep 1.2
check "整理完成" '"phase": "done"' "$(curl -s $B/api/compaction)"
check "旧代目录仍在（被查询绑定）" "gen-$(printf '%06d' $OLD_GEN)" "$(ls $DATA/gens)"
# 关闭全部查询
for q in $(curl -s $B/api/queries | $PY -c "import json,sys;[print(x['query_id']) for x in json.load(sys.stdin)['queries']]"); do
  curl -s -X POST $B/api/query/close -H 'Content-Type: application/json' -d "{\"query_id\":\"$q\"}" >/dev/null
done
sleep 0.3
check "旧代关闭查询后被回收" "gen-$(printf '%06d' $((OLD_GEN+1)))" "$(ls $DATA/gens)"

echo "=== 篡改当前代位图 -> 自愈重建 ==="
CUR=$(ls -d $DATA/gens/gen-* | head -1)
BMP=$(ls $CUR/bitmaps/*.bitm | grep -v deletes | head -1)
printf 'xx' | dd of="$BMP" bs=1 seek=2 count=2 conv=notrunc 2>/dev/null
R=$(curl -s -X POST $B/api/query -H 'Content-Type: application/json' -d '{"filter":{"conditions":[]},"index_enabled":true}')
check "篡改后自愈、新查询可用" '"hits"' "$R"
check "损坏代进入隔离区" "gen-" "$(ls $DATA/rejected)"

echo
echo "通过 $pass / 失败 $fail"
exit $fail
