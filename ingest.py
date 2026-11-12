#!/usr/bin/env python3
"""会员行为日志接入管道。

职责：
- 扫描 inbox 目录下的批次 CSV（UTF-8、带表头），只接入尚未登记的新批次；
- 以 batch_date 为批次去重键，重名重传、改名重传（如 _r1 后缀）、整目录重跑
  均不会产生重复数据；
- 每个批次在一个 SQLite 事务内完成明细写入与批次登记，中途失败整体回滚；
- 每次运行结束输出对账报告 report.txt。

运行约定：从仓库根目录执行 `python3 ingest.py`，默认使用相对路径
inbox/ 与 data/warehouse.db，也可通过命令行参数覆盖（供测试使用）。
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger("ingest")

# CSV 必须具备的列（顺序以表头为准，这里只校验集合）
REQUIRED_COLUMNS = [
    "event_id",
    "member_id",
    "event_time",
    "event_type",
    "amount_orig",
    "amount_cny",
    "batch_date",
]
NUMERIC_COLUMNS = ["amount_orig", "amount_cny"]


def local_now_text() -> str:
    """返回本地时区的当前时间字符串（YYYY-MM-DD HH:MM:SS）。

    历史 ingested_batches.ingested_at 是无时区后缀的本地时间字符串，
    这里保持同一口径；通过带 tz 的 now() 取值以避免裸用 datetime.now()。
    """
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


class IngestError(Exception):
    """接入过程中的可预期错误（文件格式、数据校验、数据库写入失败等）。"""


@dataclass
class BatchResult:
    """单个批次文件的处理结果。"""

    file_name: str
    batch_date: str
    status: str  # ingested | skipped_duplicate | failed
    row_count: int = 0
    amount_cny_sum: Decimal = Decimal(0)
    reason: str = ""


@dataclass
class RunReport:
    """一次运行的汇总信息，用于生成 report.txt。"""

    started_at: str
    inbox_dir: str
    db_path: str
    results: list[BatchResult] = field(default_factory=list)
    # 运行后库内累计口径
    cumulative_rows: int = 0
    cumulative_amount: Decimal = Decimal(0)
    watermark: str | None = None

    @property
    def ingested(self) -> list[BatchResult]:
        return [r for r in self.results if r.status == "ingested"]

    @property
    def skipped(self) -> list[BatchResult]:
        return [r for r in self.results if r.status == "skipped_duplicate"]

    @property
    def failed(self) -> list[BatchResult]:
        return [r for r in self.results if r.status == "failed"]

    @property
    def ingested_rows(self) -> int:
        return sum(r.row_count for r in self.ingested)

    @property
    def ingested_amount(self) -> Decimal:
        return sum((r.amount_cny_sum for r in self.ingested), Decimal(0))


def init_database(db_path: Path) -> None:
    """初始化/修补库结构。

    历史库的两张表都没有唯一约束，这里在保持表结构兼容的前提下补齐：
    - ingested_batches.batch_date 加 UNIQUE，作为批次去重的数据库级兜底；
    - events 上加 (event_id, batch_date) 复合索引，加速对账与查询。
      注意 event_id 实测存在跨批次碰撞，不能对 event_id 单列加唯一约束。
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT,
                member_id TEXT,
                event_time TEXT,
                event_type TEXT,
                amount_orig REAL,
                amount_cny REAL,
                batch_date TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ingested_batches (
                batch_file TEXT,
                batch_date TEXT,
                row_count INTEGER,
                ingested_at TEXT
            )
            """
        )
        # 旧库可能已存在同名非唯一索引，先查 sql 主表定义再决定是否补唯一索引
        existing = {
            row[0]: row[1]
            for row in con.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='index'"
            )
        }
        if "ux_ingested_batches_batch_date" not in existing:
            con.execute(
                "CREATE UNIQUE INDEX ux_ingested_batches_batch_date "
                "ON ingested_batches(batch_date)"
            )
        if "ix_events_event_batch" not in existing:
            con.execute(
                "CREATE INDEX ix_events_event_batch "
                "ON events(event_id, batch_date)"
            )
        con.commit()


def load_and_validate(csv_path: Path) -> tuple[str, pd.DataFrame]:
    """读取并校验一个批次文件，返回 (batch_date, DataFrame)。

    全部列按字符串读入，金额再单独转数值，避免 pandas 自动类型推断
    把 event_id 之类的文本列改写格式。
    """
    try:
        frame = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8")
    except (OSError, UnicodeDecodeError, pd.errors.ParserError) as exc:
        raise IngestError(f"读取 CSV 失败: {exc}") from exc

    missing = [c for c in REQUIRED_COLUMNS if c not in frame.columns]
    if missing:
        raise IngestError(f"缺少必需列: {', '.join(missing)}")
    frame = frame[REQUIRED_COLUMNS].copy()

    if frame.empty:
        raise IngestError("文件没有任何数据行")

    # 空值/纯空白校验
    for column in REQUIRED_COLUMNS:
        blank_mask = frame[column].map(lambda v: str(v).strip() == "")
        if blank_mask.any():
            bad = int(blank_mask.idxmax()) + 2  # +2: 表头占第 1 行，DataFrame 从 0 起
            raise IngestError(f"列 {column} 存在空值（首个空值约在 CSV 第 {bad} 行）")

    # 金额必须能转成数值，且业务金额不允许为负
    for column in NUMERIC_COLUMNS:
        try:
            numeric = pd.to_numeric(frame[column], errors="raise")
        except (ValueError, TypeError) as exc:
            raise IngestError(f"列 {column} 存在无法解析的数值: {exc}") from exc
        if (numeric < 0).any():
            raise IngestError(f"列 {column} 存在负数金额，业务上不允许")
        frame[column] = numeric

    # 一个文件只能属于一个批次
    batch_dates = frame["batch_date"].str.strip().unique()
    if len(batch_dates) != 1:
        raise IngestError(
            f"文件内 batch_date 不唯一，发现: {', '.join(sorted(batch_dates))}"
        )
    batch_date = str(batch_dates[0])

    # 文件内 event_id 重复意味着上游文件本身有问题，拒绝接入而非静默去重
    if frame["event_id"].duplicated().any():
        dupes = frame.loc[frame["event_id"].duplicated(), "event_id"].tolist()[:5]
        raise IngestError(f"文件内 event_id 重复，样例: {', '.join(dupes)}")

    return batch_date, frame


def _sum_amount_cny(frame: pd.DataFrame) -> Decimal:
    """用 Decimal 对金额字符串求和，避免浮点累计误差，供对账报告使用。"""
    total = Decimal(0)
    for raw in frame["amount_cny"]:
        try:
            total += Decimal(str(raw))
        except InvalidOperation as exc:
            raise IngestError(f"amount_cny 存在无法精确解析的值: {raw!r}") from exc
    return total


def ingest_batch(
    con: sqlite3.Connection,
    csv_path: Path,
    already_ingested: set[str],
) -> BatchResult:
    """接入单个批次文件。

    批次登记与明细插入在同一个事务内：任何一步失败都整体回滚，
    库里不会留下半个批次。
    """
    file_name = csv_path.name
    try:
        batch_date, frame = load_and_validate(csv_path)
    except IngestError as exc:
        LOGGER.error("批次文件 %s 校验失败: %s", file_name, exc)
        return BatchResult(file_name, batch_date="?", status="failed", reason=str(exc))

    if batch_date in already_ingested:
        LOGGER.info("跳过已接入批次 %s（文件 %s）", batch_date, file_name)
        return BatchResult(
            file_name=file_name,
            batch_date=batch_date,
            status="skipped_duplicate",
            reason="批次已登记（重传/重复文件）",
        )

    row_count = len(frame)
    amount_sum = _sum_amount_cny(frame)

    # 立即占位，保证同一次运行内多个文件指向同一新批次时只接入第一个
    already_ingested.add(batch_date)

    rows = [tuple(row[c] for c in REQUIRED_COLUMNS) for row in frame.to_dict("records")]
    ingested_at = local_now_text()

    try:
        # IMMEDIATE 立即拿写锁，配合 batch_date 唯一索引防并发/崩溃后的重复写入
        con.execute("BEGIN IMMEDIATE")
        con.executemany(
            "INSERT INTO events "
            "(event_id, member_id, event_time, event_type, amount_orig, "
            "amount_cny, batch_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        con.execute(
            "INSERT INTO ingested_batches "
            "(batch_file, batch_date, row_count, ingested_at) "
            "VALUES (?, ?, ?, ?)",
            (file_name, batch_date, row_count, ingested_at),
        )
        con.commit()
    except sqlite3.Error as exc:
        con.rollback()
        already_ingested.discard(batch_date)
        LOGGER.error("批次 %s 写入失败，已回滚: %s", batch_date, exc)
        return BatchResult(
            file_name=file_name,
            batch_date=batch_date,
            status="failed",
            reason=f"数据库写入失败已回滚: {exc}",
        )

    LOGGER.info(
        "接入批次 %s（文件 %s）: %s 行, amount_cny 合计 %s",
        batch_date,
        file_name,
        row_count,
        amount_sum.quantize(Decimal("0.01")),
    )
    return BatchResult(
        file_name=file_name,
        batch_date=batch_date,
        status="ingested",
        row_count=row_count,
        amount_cny_sum=amount_sum,
    )


def fetch_cumulative_stats(con: sqlite3.Connection) -> tuple[int, Decimal, str | None]:
    """读取库内累计行数、累计金额与当前水位（最大已登记 batch_date）。"""
    row_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    amount_raw = con.execute("SELECT COALESCE(SUM(amount_cny), 0) FROM events").fetchone()[0]
    watermark = con.execute(
        "SELECT MAX(batch_date) FROM ingested_batches"
    ).fetchone()[0]
    return row_count, Decimal(str(amount_raw)), watermark


def render_report(report: RunReport) -> str:
    """把运行结果渲染成给财务核对的纯文本对账报告。"""
    lines: list[str] = []
    lines.append("会员行为日志接入对账报告")
    lines.append("=" * 40)
    lines.append(f"运行时间: {report.started_at}")
    lines.append(f"inbox 目录: {report.inbox_dir}")
    lines.append(f"数据库: {report.db_path}")
    lines.append("")

    lines.append(f"本次接入批次数: {len(report.ingested)}")
    lines.append(f"本次接入行数: {report.ingested_rows}")
    lines.append(
        f"本次接入金额合计(amount_cny): "
        f"{report.ingested_amount.quantize(Decimal('0.01'))}"
    )
    lines.append("")

    lines.append("本次接入批次明细:")
    if report.ingested:
        lines.append("  批次日期      行数    金额合计(CNY)      来源文件")
        for result in sorted(report.ingested, key=lambda r: r.batch_date):
            lines.append(
                f"  {result.batch_date}  "
                f"{result.row_count:>6}  "
                f"{result.amount_cny_sum.quantize(Decimal('0.01')):>16}  "
                f"{result.file_name}"
            )
    else:
        lines.append("  （无新批次）")
    lines.append("")

    lines.append(f"跳过的重复文件数: {len(report.skipped)}")
    for result in sorted(report.skipped, key=lambda r: r.file_name):
        lines.append(
            f"  {result.file_name} -> 批次 {result.batch_date}（{result.reason}）"
        )
    lines.append("")

    lines.append(f"失败文件数: {len(report.failed)}")
    for result in report.failed:
        lines.append(
            f"  {result.file_name} -> 批次 {result.batch_date}（{result.reason}）"
        )
    lines.append("")

    lines.append("库内累计口径（运行结束后）:")
    lines.append(f"  累计行数: {report.cumulative_rows}")
    lines.append(
        f"  累计金额合计(amount_cny): "
        f"{report.cumulative_amount.quantize(Decimal('0.01'))}"
    )
    lines.append(f"  水位（最大已登记 batch_date）: {report.watermark or '（空）'}")
    lines.append("")
    return "\n".join(lines)


def run_ingest(inbox_dir: Path, db_path: Path, report_path: Path) -> RunReport:
    """管道主流程：扫描、接入、统计、写报告。返回 RunReport。"""
    if not inbox_dir.is_dir():
        raise IngestError(f"inbox 目录不存在: {inbox_dir}")

    init_database(db_path)
    report = RunReport(
        started_at=local_now_text(),
        inbox_dir=str(inbox_dir),
        db_path=str(db_path),
    )

    csv_files = sorted(inbox_dir.glob("*.csv"), key=lambda p: p.name)
    if not csv_files:
        LOGGER.warning("inbox 目录 %s 下没有 CSV 文件", inbox_dir)

    with sqlite3.connect(db_path) as con:
        already_ingested = {
            row[0] for row in con.execute("SELECT batch_date FROM ingested_batches")
        }
        for csv_path in csv_files:
            report.results.append(ingest_batch(con, csv_path, already_ingested))

        cumulative_rows, cumulative_amount, watermark = fetch_cumulative_stats(con)
        report.cumulative_rows = cumulative_rows
        report.cumulative_amount = cumulative_amount
        report.watermark = watermark

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(report), encoding="utf-8")
    LOGGER.info("对账报告已写入 %s", report_path)
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="会员行为日志接入管道")
    parser.add_argument("--inbox", default="inbox", help="批次 CSV 目录（默认 inbox）")
    parser.add_argument(
        "--db", default="data/warehouse.db", help="SQLite 库路径（默认 data/warehouse.db）"
    )
    parser.add_argument(
        "--report", default="report.txt", help="对账报告输出路径（默认 report.txt）"
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        report = run_ingest(Path(args.inbox), Path(args.db), Path(args.report))
    except IngestError as exc:
        LOGGER.error("接入运行失败: %s", exc)
        return 1
    if report.failed:
        LOGGER.error("本次运行有 %s 个文件接入失败，详见报告", len(report.failed))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
