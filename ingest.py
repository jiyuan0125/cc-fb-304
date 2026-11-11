"""会员行为日志 CSV 接入管道。

职责：
- 扫描 inbox 目录下的批次 CSV，只接入新批次，已接入过的内容（重名重传、
  改名重传、整个目录重跑）自动跳过，库内绝不产生重复数据。
- 批次身份由文件内容指纹（SHA-256）决定，与文件名、batch_date 均无关，
  因此 batch_date 早于当前水位的晚到批次也能正常接入。
- 每个批次在单个事务内完成 events 明细写入与 ingested_batches 登记，
  中途失败整体回滚，不留半个批次。
- 每次运行结束输出对账报告 report.txt。

运行约定（见 README）：
    python3 ingest.py [--inbox inbox] [--db data/warehouse.db] [--report report.txt]
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger("ingest")


def _now() -> datetime:
    """当前本地时间（naive，与历史 ingested_at 的文本口径一致）。

    先取带 UTC 时区的时间再转到本地时区并去掉 tzinfo，
    避免使用不带时区参数的 datetime.now()。
    """
    return datetime.now(tz=timezone.utc).astimezone().replace(tzinfo=None)


COLUMNS = [
    "event_id",
    "member_id",
    "event_time",
    "event_type",
    "amount_orig",
    "amount_cny",
    "batch_date",
]
# 单元/记录分隔符，保证字段内含逗号或换行时指纹也无歧义
_UNIT_SEP = "\x1f"
_RECORD_SEP = "\x1e"
_TWO_PLACES = Decimal("0.01")


class BatchError(Exception):
    """批次文件本身不合法（缺列、空文件、金额不可解析等），该批次必须拒绝。"""


@dataclass
class BatchOutcome:
    """单个批次文件的处理结果。"""

    file_name: str
    batch_date: str
    row_count: int
    amount_sum: Decimal
    fingerprint: str
    status: str  # ingested / skipped / failed
    note: str = ""


@dataclass
class RunSummary:
    """一次运行的完整结果，用于生成对账报告。"""

    db_path: Path
    inbox_path: Path
    report_path: Path
    started_at: datetime
    finished_at: datetime | None = None
    outcomes: list[BatchOutcome] = field(default_factory=list)

    @property
    def ingested(self) -> list[BatchOutcome]:
        return [o for o in self.outcomes if o.status == "ingested"]

    @property
    def skipped(self) -> list[BatchOutcome]:
        return [o for o in self.outcomes if o.status == "skipped"]

    @property
    def failed(self) -> list[BatchOutcome]:
        return [o for o in self.outcomes if o.status == "failed"]


def read_batch(path: Path) -> pd.DataFrame:
    """按原始字符串读取批次 CSV 并校验表头与字段。

    全部列按字符串读取（keep_default_na=False），避免 pandas 把空值转成 NaN、
    把金额解析成浮点，保证指纹基于上游原始文本、金额求和走 Decimal。
    """
    try:
        frame = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )
    except Exception as exc:
        raise BatchError(f"CSV 解析失败: {exc}") from exc

    missing = [c for c in COLUMNS if c not in frame.columns]
    if missing:
        raise BatchError(f"缺少必需列: {', '.join(missing)}")
    extra = [c for c in frame.columns if c not in COLUMNS]
    if extra:
        raise BatchError(f"存在未声明的多余列: {', '.join(extra)}")

    frame = frame[COLUMNS]
    if frame.empty:
        raise BatchError("文件没有任何数据行")
    return frame


def compute_fingerprint(frame: pd.DataFrame) -> str:
    """对数据行内容计算规范指纹，与文件名、行序之外的写法差异无关。

    行按全部字段排序后再哈希：同一批数据即使行顺序被上游打乱，指纹仍一致；
    任意一行内容变化（如金额修正后重投）指纹即变化，会被视为新批次。
    """
    canonical_lines = [
        _UNIT_SEP.join(str(value) for value in row)
        for row in frame.sort_values(COLUMNS, kind="mergesort").itertuples(
            index=False, name=None
        )
    ]
    digest = hashlib.sha256(
        _RECORD_SEP.join(canonical_lines).encode("utf-8")
    ).hexdigest()
    return digest


def validate_batch(frame: pd.DataFrame) -> str:
    """校验批次内容，返回该批次唯一的 batch_date。"""
    distinct_dates = frame["batch_date"].unique().tolist()
    if len(distinct_dates) != 1:
        raise BatchError(
            f"同一文件内 batch_date 不唯一，发现 {len(distinct_dates)} 个值: "
            f"{', '.join(sorted(distinct_dates))}"
        )
    batch_date = distinct_dates[0]
    if not batch_date:
        raise BatchError("batch_date 不能为空")

    empty_ids = frame["event_id"].eq("").sum()
    if empty_ids:
        raise BatchError(f"存在 {empty_ids} 行 event_id 为空")

    for column in ("amount_orig", "amount_cny"):
        for raw in frame[column]:
            try:
                Decimal(raw)
            except (InvalidOperation, ValueError) as exc:
                raise BatchError(f"列 {column} 存在不可解析的金额: {raw!r}") from exc
    return batch_date


def sum_amount(frame: pd.DataFrame, column: str = "amount_cny") -> Decimal:
    """按原始字符串以 Decimal 累加金额，避免浮点误差。"""
    total = Decimal(0)
    for raw in frame[column]:
        total += Decimal(raw)
    return total.quantize(_TWO_PLACES)


def ensure_schema(con: sqlite3.Connection) -> None:
    """初始化/兼容历史库：缺表则建表，老版本库补齐 content_hash 列与唯一索引。

    历史库的 events 没有唯一约束（event_id 跨批次可能重复），建表语句保持
    不加约束；老版本 ingested_batches 没有 content_hash，ALTER TABLE 加列后
    历史行该列为 NULL，SQLite 唯一索引允许 NULL，不影响新数据去重。
    """
    con.execute(
        "CREATE TABLE IF NOT EXISTS events ("
        "event_id TEXT, member_id TEXT, event_time TEXT, event_type TEXT, "
        "amount_orig REAL, amount_cny REAL, batch_date TEXT)"
    )
    table_exists = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ingested_batches'"
    ).fetchone()
    if not table_exists:
        con.execute(
            "CREATE TABLE ingested_batches ("
            "batch_file TEXT, batch_date TEXT, row_count INTEGER, "
            "ingested_at TEXT, content_hash TEXT)"
        )
    else:
        existing = {
            row[1] for row in con.execute("PRAGMA table_info(ingested_batches)")
        }
        if "content_hash" not in existing:
            LOGGER.info("迁移历史库：为 ingested_batches 增加 content_hash 列")
            con.execute("ALTER TABLE ingested_batches ADD COLUMN content_hash TEXT")
    con.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "idx_ingested_batches_content_hash "
        "ON ingested_batches(content_hash)"
    )
    con.commit()


def known_fingerprints(con: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in con.execute(
            "SELECT content_hash FROM ingested_batches WHERE content_hash IS NOT NULL"
        )
    }


def insert_batch(
    con: sqlite3.Connection,
    file_name: str,
    batch_date: str,
    frame: pd.DataFrame,
    fingerprint: str,
) -> None:
    """在单个事务内写入明细并登记批次；任何失败都整体回滚。"""
    row_count = len(frame)
    amount_sum = sum_amount(frame)
    records = [
        (
            event_id,
            member_id,
            event_time,
            event_type,
            float(Decimal(amount_orig)),
            float(Decimal(amount_cny)),
            batch_date,
        )
        for (
            event_id,
            member_id,
            event_time,
            event_type,
            amount_orig,
            amount_cny,
            batch_date,
        ) in frame.itertuples(index=False, name=None)
    ]
    try:
        con.execute("BEGIN IMMEDIATE")
        con.executemany(
            "INSERT INTO events "
            "(event_id, member_id, event_time, event_type, "
            "amount_orig, amount_cny, batch_date) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            records,
        )
        con.execute(
            "INSERT INTO ingested_batches "
            "(batch_file, batch_date, row_count, ingested_at, content_hash) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                file_name,
                batch_date,
                row_count,
                _now().strftime("%Y-%m-%d %H:%M:%S"),
                fingerprint,
            ),
        )
        con.commit()
    except Exception:
        con.rollback()
        LOGGER.exception(
            "批次 %s（%s）接入失败，事务已回滚，库内无该批次残留数据",
            file_name,
            batch_date,
        )
        raise
    LOGGER.info(
        "已接入批次 %s（%s）：%d 行，金额合计 %s",
        file_name,
        batch_date,
        row_count,
        amount_sum,
    )


def cumulative_totals(con: sqlite3.Connection) -> tuple[int, Decimal, int, str | None]:
    """返回 events 累计行数/金额、登记行数合计、最新批次日期。"""
    event_count, event_sum = con.execute(
        "SELECT COUNT(*), COALESCE(SUM(amount_cny), 0) FROM events"
    ).fetchone()
    registered_count = con.execute(
        "SELECT COALESCE(SUM(row_count), 0) FROM ingested_batches"
    ).fetchone()[0]
    watermark = con.execute("SELECT MAX(batch_date) FROM ingested_batches").fetchone()[
        0
    ]
    return (
        event_count,
        Decimal(str(event_sum)).quantize(_TWO_PLACES),
        registered_count,
        watermark,
    )


def _money(value: Decimal) -> str:
    return f"{value.quantize(_TWO_PLACES):,.2f}"


def write_report(con: sqlite3.Connection, summary: RunSummary) -> None:
    """生成中文对账报告，供财务核对。"""
    event_count, event_sum, registered_count, watermark = cumulative_totals(con)
    new_rows = sum(o.row_count for o in summary.ingested)
    new_amount = sum((o.amount_sum for o in summary.ingested), Decimal(0)).quantize(
        _TWO_PLACES
    )
    batch_count = con.execute("SELECT COUNT(*) FROM ingested_batches").fetchone()[0]

    lines: list[str] = []
    lines.append("会员行为日志接入对账报告")
    lines.append("=" * 60)
    lines.append(f"生成时间: {summary.finished_at:%Y-%m-%d %H:%M:%S}")
    lines.append(f"数据库: {summary.db_path}")
    lines.append(f"收件目录: {summary.inbox_path}")
    lines.append("")

    lines.append("一、本次新接入批次")
    lines.append(
        f"共 {len(summary.ingested)} 个，新增 {new_rows} 行，"
        f"新增金额合计 {_money(new_amount)} 元（CNY）"
    )
    if summary.ingested:
        lines.append("")
        lines.append(
            f"{'批次日期':<12}{'来源文件':<28}{'行数':>8}  {'金额合计(CNY)':>18}"
        )
        for outcome in sorted(
            summary.ingested, key=lambda o: (o.batch_date, o.file_name)
        ):
            lines.append(
                f"{outcome.batch_date:<12}{outcome.file_name:<28}"
                f"{outcome.row_count:>8}  {_money(outcome.amount_sum):>18}"
            )
    lines.append("")

    lines.append("二、跳过的已接入/重传文件")
    lines.append(f"共 {len(summary.skipped)} 个（内容指纹与已接入批次一致）")
    for outcome in sorted(summary.skipped, key=lambda o: o.file_name):
        lines.append(
            f"  - {outcome.file_name}（批次 {outcome.batch_date}，{outcome.note}）"
        )
    lines.append("")

    lines.append("三、失败批次")
    if summary.failed:
        for outcome in summary.failed:
            lines.append(f"  - {outcome.file_name}: {outcome.note}")
    else:
        lines.append("无")
    lines.append("")

    lines.append("四、累计水位")
    lines.append(f"已登记批次数: {batch_count}")
    lines.append(f"events 表累计行数: {event_count}")
    lines.append(f"ingested_batches 登记行数合计: {registered_count}")
    lines.append(f"累计金额合计: {_money(event_sum)} 元（CNY）")
    lines.append(f"最新批次日期（可读水位）: {watermark or '无'}")
    if event_count != registered_count:
        lines.append(
            f"[告警] events 行数({event_count}) 与登记行数({registered_count}) 不一致，"
            "请核查历史数据"
        )
    lines.append("")
    lines.append(
        "说明: 去重以批次内容指纹为准，与文件名无关；晚到批次按内容正常接入，"
        "不受最新批次日期限制。"
    )

    summary.report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    LOGGER.info("对账报告已写入 %s", summary.report_path)


def run(inbox: Path, db_path: Path, report_path: Path) -> int:
    """执行一次接入，返回进程退出码（0 成功，1 存在失败批次）。"""
    summary = RunSummary(
        db_path=db_path,
        inbox_path=inbox,
        report_path=report_path,
        started_at=_now(),
    )

    if not inbox.is_dir():
        LOGGER.error("收件目录不存在: %s", inbox)
        return 1
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    try:
        ensure_schema(con)
        seen = known_fingerprints(con)

        csv_files = sorted(inbox.glob("*.csv"))
        if not csv_files:
            LOGGER.warning("收件目录 %s 下没有 CSV 文件", inbox)

        for path in csv_files:
            try:
                frame = read_batch(path)
                fingerprint = compute_fingerprint(frame)
                batch_date = validate_batch(frame)
            except BatchError as exc:
                LOGGER.error("跳过非法批次 %s: %s", path.name, exc)
                summary.outcomes.append(
                    BatchOutcome(
                        file_name=path.name,
                        batch_date="-",
                        row_count=0,
                        amount_sum=Decimal(0),
                        fingerprint="",
                        status="failed",
                        note=str(exc),
                    )
                )
                break

            if fingerprint in seen:
                LOGGER.info("跳过已接入批次（内容指纹命中）: %s", path.name)
                summary.outcomes.append(
                    BatchOutcome(
                        file_name=path.name,
                        batch_date=batch_date,
                        row_count=len(frame),
                        amount_sum=sum_amount(frame),
                        fingerprint=fingerprint,
                        status="skipped",
                        note="重传/重复文件，未重复入库",
                    )
                )
                continue

            try:
                insert_batch(con, path.name, batch_date, frame, fingerprint)
            except sqlite3.Error:
                summary.outcomes.append(
                    BatchOutcome(
                        file_name=path.name,
                        batch_date=batch_date,
                        row_count=len(frame),
                        amount_sum=sum_amount(frame),
                        fingerprint=fingerprint,
                        status="failed",
                        note="数据库写入失败，事务已回滚",
                    )
                )
                break
            seen.add(fingerprint)
            summary.outcomes.append(
                BatchOutcome(
                    file_name=path.name,
                    batch_date=batch_date,
                    row_count=len(frame),
                    amount_sum=sum_amount(frame),
                    fingerprint=fingerprint,
                    status="ingested",
                )
            )

        summary.finished_at = _now()
        write_report(con, summary)
    finally:
        con.close()

    if summary.failed:
        LOGGER.error("本次运行存在失败批次，退出码 1；详见 %s", report_path)
        return 1
    LOGGER.info(
        "本次运行结束：新接入 %d 个，跳过 %d 个，失败 0 个",
        len(summary.ingested),
        len(summary.skipped),
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="会员行为日志 CSV 接入管道")
    parser.add_argument(
        "--inbox", type=Path, default=Path("inbox"), help="批次 CSV 目录"
    )
    parser.add_argument(
        "--db", type=Path, default=Path("data/warehouse.db"), help="SQLite 数据库路径"
    )
    parser.add_argument(
        "--report", type=Path, default=Path("report.txt"), help="对账报告输出路径"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = parse_args(argv)
    return run(args.inbox, args.db, args.report)


if __name__ == "__main__":
    sys.exit(main())
