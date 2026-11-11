# 会员行为日志接入环境

这个目录是一套已在运行的接入环境，此前由同事维护，现在交给你继续。

已有的东西：
- data/warehouse.db：SQLite 库。表 events 是已接入的明细（注意表上没有任何唯一约束，历史导入工具没建）；
  表 ingested_batches 是已接入批次的登记，水位即该表中最大的 batch_date。
- inbox/：新到的批次文件（CSV，UTF-8，带表头），共 8 个。上游的文件到达顺序不保证，偶有重投。

运行约定：脚本从仓库根目录以 python3 ingest.py 方式运行，数据用相对路径 inbox/ 与 data/warehouse.db。
