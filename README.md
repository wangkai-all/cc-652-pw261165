# 事件仓位图筛选服务

面向带**地区、渠道（单值类别列）和标签（多值字段）**的事件仓，提供基于位图索引的
组合筛选服务。支持交集/并集、真实行预览、索引与全表扫描的读取对比、数据代次管理、
压缩整理（compaction）、查询与代次的绑定隔离，以及损坏/伪造索引文件的显式拒用。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 启动服务（默认 127.0.0.1:8000）
.venv/bin/python -m scripts.run_server --data data/store --port 8000

# 另开终端灌入演示数据（含多标签、空标签、空类别）
.venv/bin/python scripts/seed.py --base http://127.0.0.1:8000 --count 400
```

浏览器打开 <http://127.0.0.1:8000/>。

## 核心设计

### 1. 字段语义与位图

- `region` / `channel`：单值类别列，每个取值一张位图；空值归一为哨兵 `__EMPTY__`。
- `tags`：多值字段，一条记录可同时落入多个标签位图；**空集合**单独存
  `tags__%40EMPTY.bitm`，与单值字段的空值位图物理分开。
- 位图后端优先用 `pyroaring`（Roaring 压缩位图）；无该库时自动退化为 Python 大整数
  位图。磁盘文件带魔数（`R` / `P`）与版本，按魔数分派反序列化，损坏魔数显式报错。

### 2. 磁盘布局（全部真实持久化，非内存演示）

```
data/store/
├── wal.log                  WAL：所有变更（put/update/delete）的唯一事实来源，fsync 追加
├── CURRENT                  当前代次指针（原子写）
├── query-pins.json          每个代次被多少查询绑定（持久化，跨重启保留）
├── compaction.json          整理任务状态（暂停/中止/冲突，支持中断恢复）
├── gens/
│   └── gen-000007/
│       ├── manifest.json    每个文件的 size + sha256，加载时逐一校验
│       ├── records.jsonl    行号顺序的业务记录（行号 = 行序，0 起）
│       ├── mapping.json     行号 -> 业务编号的真实持久化映射
│       ├── masks.json       删除掩码 / 更新掩码
│       └── bitmaps/         region__*.bitm / channel__*.bitm / tags__*.bitm / deletes.bitm
└── rejected/                校验失败被隔离的代次（附 REJECTED.txt 原因）
```

所有写操作走「同目录临时文件 → fsync → 原子 rename → 目录 fsync」。

### 3. 查询绑定（一次查询绑定数据代次、行号映射与删除掩码）

`POST /api/query` 创建查询会话，返回 `query_id`，会话创建时刻固定：

- 绑定的代次 ID 与该代不可变的行号映射；
- 绑定时刻的删除掩码快照；
- 命中行号集合（位运算结果）。

旧查询**永远不会套用新代的位图**：旧代被回收后再访问显式返回 409
`StaleGenerationError`。绑定之后被删除的行，preview 时结合当前删除掩码二次扣除，
保证「删除记录后不得再从索引结果读到」。页面会标记 `stale`（绑定后 WAL 已前进）。

### 4. 写入与更正

- 插入：先写 WAL 再物化；重复业务编号显式拒用（400）。
- 类别更正（update）：WAL 追加后从 WAL 重放结果**整代重建**（与启动恢复同一条代码
  路径），天然实现「撤下旧位置、加入新位置」，并保证 records / mapping / 位图严格一致；
  经历过更正的行号记入更新掩码。
- 删除：写入删除掩码（masks.json + deletes.bitm 双写），不物理移除行号。

### 5. 压缩整理与冲突处理

`POST /api/compaction/start` 后台线程执行，支持 `pause / resume / abort` 协作式控制：

1. 锁内取全量快照与基线 LSN，构建工作在锁外完成（带进度回调）；
2. 新代先写临时目录 `.build-gen-*`，全部文件落盘并自检通过；
3. 提交前再次取锁比对 WAL 水位：**暂停/构建期间若有任何写入 → 判冲突，拒绝切换**，
   清理半成品并把水位记录到 `fence_lsn`；
4. 无冲突才原子 rename + 切换 CURRENT；
5. 旧代只要仍被查询绑定（pin > 0）就保留；全部查询关闭后才回收旧代文件；
6. 进程在整理中被杀：重启后任务标记 `aborted`，临时目录残骸自动清理，可重新发起。

### 6. 损坏拒用与自愈

加载任何代次都按 manifest 校验 sha256/字节数（已缓存的代次也重新校验磁盘）：

- 旧代损坏：移动到 `rejected/` 并附原因，绑定它的查询得到 409；
- 当前代损坏：隔离后从完整保留的 WAL 全量重放重建新代，新查询继续可用
  （WAL 不做截断，始终是可独立重建数据的唯一事实来源）。

### 7. 索引 vs 扫描的读取对比

同一过滤器可分别走两条路径，响应中带真实读取统计：

- 索引路径：只读取相关位图文件 + 删除掩码，返回 `bytes_read / bitmap_files`；
- 扫描路径：逐行读取 `records.jsonl` 做真值判定，返回 `bytes_read / record_lines_read`。

扫描路径同时是正确性基准，测试中断言两条路径的命中集合完全一致。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/overview` | 代次、LSN、各字段可选值与计数、隔离区 |
| POST | `/api/events` | 插入事件（重复编号拒用） |
| POST | `/api/events/{id}/update` | 更正类别/标签（撤旧位图+建新位图） |
| POST | `/api/events/{id}/delete` | 删除（加入删除掩码） |
| POST | `/api/query` | 创建查询会话，`index_enabled` 切换索引/扫描 |
| GET | `/api/query/{id}/preview` | 分页预览命中的真实业务行 |
| POST | `/api/query/close` | 关闭查询，释放代次绑定 |
| GET | `/api/queries` | 查询会话列表 |
| POST | `/api/compaction/start` | 开始整理 |
| POST | `/api/compaction/control` | `pause` / `resume` / `abort` |
| GET | `/api/compaction` | 整理状态与进度 |

## 测试

```bash
.venv/bin/pytest tests/ -q          # 22 个单元/集成测试
bash scripts/e2e_check.sh           # 对运行中的服务做端到端验收（11 项）
```

覆盖场景（均以扫描结果为基准对账）：

- 一条记录命中多个标签、标签空集合、单值字段空值、两种“空”分开存储；
- 交集 AND / 并集 OR、多值字段内部 OR；
- 重复业务编号拒用、行号映射真实落盘且可重启恢复；
- 类别更正撤旧建新、标签更正、转为空集合；
- 删除后索引与重启后均不可读、绑定查询对后续删除的隔离；
- 整理 pin 旧代、关闭后回收、暂停期修改的冲突检测、中止清理、进程中断恢复；
- 篡改旧代/当前代位图的隔离拒用与自愈重建；
- Roaring 与纯 Python 回退两种位图格式的往返与坏文件拒用。
