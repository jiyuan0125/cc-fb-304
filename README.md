# 会员行为日志接入环境

这个目录是一套已在运行的接入环境，此前由同事维护，现在交给你继续。

已有的东西：
- data/warehouse.db：SQLite 库。表 events 是已接入的明细（注意表上没有任何唯一约束，历史导入工具没建）；
  表 ingested_batches 是已接入批次的登记，水位即该表中最大的 batch_date。
- inbox/：新到的批次文件（CSV，UTF-8，带表头），共 8 个。上游的文件到达顺序不保证，偶有重投。

运行约定：脚本从仓库根目录以 python3 ingest.py 方式运行，数据用相对路径 inbox/ 与 data/warehouse.db。

## 接入管道 ingest.py

### 运行

```bash
python3 ingest.py                 # 默认 inbox/、data/warehouse.db、report.txt
python3 ingest.py --log-level DEBUG
python3 ingest.py --inbox /path/to/inbox --db /path/to/w.db --report /path/to/report.txt
```

依赖：Python 3.10+、pandas。退出码：`0` 全部成功；`1` 运行级错误（如 inbox 目录不存在）；
`2` 存在接入失败的文件（其余正常批次仍会提交，失败明细见报告与日志）。

### 去重口径（重要）

**去重键是 `batch_date`（批次），不是文件名，也不是 `event_id`。**

- 文件到达顺序不保证、晚到批次的 `batch_date` 可能低于当前水位，因此**水位只用于进度展示，
  不用于过滤文件**。每个文件以其内的 `batch_date` 对照 `ingested_batches` 登记表判断是否接入，
  晚到批次不会被漏接。
- 重名重传、改名重传（如 `batch_20251109_r1.csv`）、整目录重跑，只要批次已登记就整体跳过，
  重复执行不在库里产生任何重复数据。
- `event_id` 实测存在**跨批次碰撞**（同一 id 出现在不同批次且内容不同），因此不能作为唯一键；
  批次才是天然的接入单元。管道只校验「单个文件内 event_id 不重复」。
- 数据库级兜底：首次运行会给遗留库补建 `ux_ingested_batches_batch_date` 唯一索引，
  即使绕过管道直接写库，重复批次登记也会被拒绝。

### 事务性

每个批次在一个 `BEGIN IMMEDIATE` 事务内完成「明细写入 events + 登记写入 ingested_batches」，
两步要么都成功要么整体回滚，不会留下半个批次。各批次事务相互独立，一个坏文件不阻塞其他批次。

### 数据校验

接入前逐文件校验，任一不通过则该文件标记失败、不写库：必需列齐全、无空值、金额可解析且不为负、
文件内 `batch_date` 唯一、文件内 `event_id` 不重复。所有失败都通过 logging 记录原因，不静默吞掉。

### 对账报告 report.txt

每次运行结束覆盖生成，供财务核对，包含：本次接入批次/行数/金额合计（amount_cny）、
跳过与失败文件清单、运行后库内累计行数、累计金额合计与当前水位。
金额用 Decimal 求和，报告统一保留两位小数，避免浮点累计误差。

## 测试

```bash
python3 -m pytest tests/ -v
```

测试在 tmp_path 隔离环境运行，不污染仓库内的 data/warehouse.db。覆盖：首次接入、重复执行幂等、
重名/改名重传、晚到批次、event_id 跨批次碰撞、事务回滚、坏文件隔离、遗留库兼容、真实 8 文件端到端等。
