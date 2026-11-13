#!/usr/bin/env python3
"""会员行为日志接入管道。

从 ``inbox/`` 读取上游按批次导出的 CSV，幂等写入 ``data/warehouse.db``，
每次运行结束生成对账报告 ``report.txt``。

幂等分三层（重名重传、改名重传、整个目录重跑都不会产生重复数据）：

1. **批次内容指纹**：把一批数据规范化（逐行规整、排序）后取 sha256，成功接入后
   登记在 ``ingested_batches.content_sha256``（带唯一索引）。内容相同即同一批次，
   文件怎么改名都整批跳过。
2. **事件主键去重**：为 ``events(event_id)`` 建唯一索引；每批写入前再与库内已有行
   逐条比对，已存在的 event_id 不会二次写入。历史库没有唯一约束，这一层是兜底。
   若新投文件与库内同 event_id 的载荷不一致，保留先到版本并把冲突写进报告。
3. **单批事务**：每批一个 ``BEGIN IMMEDIATE`` ... ``COMMIT``，任何一步失败整批回滚，
   不会留下半个批次的数据。

批次是否"新到"只看内容指纹，不看 batch_date：上游晚到的批次其 batch_date 可能
早于当前水位（例如 ``batch_20251104_late.csv`` 在 ``2025-11-05`` 之后到达），
按水位过滤会永久漏数。水位（``pipeline_state.batch_date_watermark``）只记录
"已接入的最大 batch_date"，用于对账展示与进度追踪。
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import logging
import os
import sqlite3
import sys
import tempfile
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

LOGGER = logging.getLogger("ingest")

#: CSV 约定字段，缺任何一个都视为坏批次。
REQUIRED_COLUMNS: tuple[str, ...] = (
    "event_id",
    "member_id",
    "event_time",
    "event_type",
    "amount_orig",
    "amount_cny",
    "batch_date",
)

DEFAULT_INBOX = Path("inbox")
DEFAULT_DB = Path("data/warehouse.db")
DEFAULT_REPORT = Path("report.txt")

#: SQLite 旧版本单语句参数上限为 999，分块查询保守取 500。
CHUNK_SIZE = 500
#: 单个文件最多收集的坏行，避免烂文件把错误列表撑爆。
MAX_ROW_ERRORS = 20
#: 报告里最多列出的冲突行数。
MAX_CONFLICTS_IN_REPORT = 50
#: 金额比较容差：库内 REAL 往返的末位误差不应判为冲突。
AMOUNT_TOLERANCE = Decimal("0.005")

WATERMARK_KEY = "batch_date_watermark"

OUTCOME_DUPLICATE = "duplicate"
OUTCOME_INVALID = "invalid"
OUTCOME_FAILED = "failed"

CREATE_EVENTS_SQL = """
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

CREATE_BATCHES_SQL = """
CREATE TABLE IF NOT EXISTS ingested_batches (
    batch_file TEXT,
    batch_date TEXT,
    row_count INTEGER,
    ingested_at TEXT
)
"""

CREATE_STATE_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT
)
"""

CREATE_ATTEMPTS_SQL = """
CREATE TABLE IF NOT EXISTS batch_attempts (
    batch_file TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    outcome TEXT NOT NULL,
    detail TEXT,
    duplicate_of TEXT,
    attempts INTEGER NOT NULL DEFAULT 1,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (batch_file, content_sha256)
)
"""

#: 为历史表补的列（老库迁移用）。
BATCH_EXTRA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("content_sha256", "TEXT"),
    ("rows_inserted", "INTEGER"),
    ("rows_duplicate", "INTEGER"),
    ("rows_conflict", "INTEGER"),
    ("amount_cny", "REAL"),
)

INSERT_EVENT_SQL = """
INSERT OR IGNORE INTO events
    (event_id, member_id, event_time, event_type, amount_orig, amount_cny, batch_date)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

REGISTER_BATCH_SQL = """
INSERT INTO ingested_batches
    (batch_file, batch_date, row_count, ingested_at, content_sha256,
     rows_inserted, rows_duplicate, rows_conflict, amount_cny)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_ATTEMPT_SQL = """
INSERT INTO batch_attempts
    (batch_file, content_sha256, outcome, detail, duplicate_of,
     attempts, first_seen_at, last_seen_at)
VALUES (?, ?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(batch_file, content_sha256) DO UPDATE SET
    outcome = excluded.outcome,
    detail = excluded.detail,
    duplicate_of = excluded.duplicate_of,
    attempts = batch_attempts.attempts + 1,
    last_seen_at = excluded.last_seen_at
"""


class IngestError(Exception):
    """批次不可接入（数据格式或内容问题），整批拒绝。"""


@dataclass(frozen=True)
class Event:
    """一条已规整的行为日志。金额用 Decimal，避免入库前的浮点漂移。"""

    event_id: str
    member_id: str
    event_time: str
    event_type: str
    amount_orig: Decimal
    amount_cny: Decimal
    batch_date: str

    def as_db_tuple(self) -> tuple[str, str, str, str, float, float, str]:
        return (
            self.event_id,
            self.member_id,
            self.event_time,
            self.event_type,
            float(self.amount_orig),
            float(self.amount_cny),
            self.batch_date,
        )

    def fingerprint(self) -> str:
        """单行指纹，用于判定"是不是同一行数据"。"""
        return "|".join(
            (
                self.event_id,
                self.member_id,
                self.event_time,
                self.event_type,
                str(self.amount_orig),
                str(self.amount_cny),
                self.batch_date,
            )
        )


@dataclass
class Batch:
    """一个已解析、已校验的批次文件。"""

    path: Path
    rows: list[Event]
    batch_date: str
    sha256: str
    duplicate_rows: list[Event] = field(default_factory=list)

    @property
    def amount_total(self) -> Decimal:
        return sum((row.amount_cny for row in self.rows), Decimal(0))


@dataclass
class Conflict:
    """库内已有同 event_id 但载荷不同的行。"""

    event_id: str
    existing: dict[str, object]
    incoming: Event


@dataclass
class BatchResult:
    """单批接入结果，也是对账报告里的一行。"""

    path: Path
    batch_date: str = ""
    rows_read: int = 0
    rows_inserted: int = 0
    rows_duplicate: int = 0
    rows_conflict: int = 0
    file_amount: Decimal = Decimal(0)
    inserted_amount: Decimal = Decimal(0)
    status: str = "ok"
    reason: str = ""
    sha256: str = ""
    conflicts: list[Conflict] = field(default_factory=list)
    duplicate_of: str = ""
    late: bool = False


@dataclass
class RunSummary:
    """一次运行的完整结果。"""

    started_at: datetime
    results: list[BatchResult] = field(default_factory=list)
    watermark_before: str | None = None
    watermark_after: str | None = None
    total_rows: int = 0
    total_amount: Decimal = Decimal(0)

    @property
    def ingested(self) -> list[BatchResult]:
        return [r for r in self.results if r.rows_inserted > 0]

    @property
    def redelivered(self) -> list[BatchResult]:
        return [r for r in self.results if r.duplicate_of]

    @property
    def failed(self) -> list[BatchResult]:
        return [r for r in self.results if r.status != "ok"]

    @property
    def inserted_rows(self) -> int:
        return sum(r.rows_inserted for r in self.results)

    @property
    def inserted_amount(self) -> Decimal:
        return sum((r.inserted_amount for r in self.results), Decimal(0))


# ---------------------------------------------------------------------------
# 数据库
# ---------------------------------------------------------------------------


def now_text() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def open_database(path: Path) -> sqlite3.Connection:
    """打开库并确保表结构就绪（老库就地补列，不改动已有数据）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA synchronous=FULL")
    ensure_schema(conn)
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_EVENTS_SQL)
    conn.execute(CREATE_BATCHES_SQL)
    conn.execute(CREATE_STATE_SQL)
    conn.execute(CREATE_ATTEMPTS_SQL)
    migrate_batch_columns(conn)
    ensure_event_id_index(conn)
    ensure_fingerprint_index(conn)
    seed_watermark(conn)


def migrate_batch_columns(conn: sqlite3.Connection) -> None:
    """给历史库的 ingested_batches 补齐本次需要的列。"""
    existing = {
        row["name"] for row in conn.execute("PRAGMA table_info(ingested_batches)")
    }
    for name, sql_type in BATCH_EXTRA_COLUMNS:
        if name in existing:
            continue
        LOGGER.info("数据库迁移：ingested_batches 增加列 %s", name)
        conn.execute(f"ALTER TABLE ingested_batches ADD COLUMN {name} {sql_type}")


def ensure_event_id_index(conn: sqlite3.Connection) -> None:
    """为 event_id 建唯一索引，作为库内不出现重复事件的物理保证。

    历史库可能已含重复 event_id（老工具没建约束）。建索引失败时降级为应用层
    去重（写入前逐行比对），并明确告警，不静默跳过。
    """
    try:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_events_event_id ON events(event_id)"
        )
    except sqlite3.IntegrityError:
        duplicates = conn.execute(
            "SELECT event_id, COUNT(*) AS n FROM events GROUP BY event_id "
            "HAVING n > 1 ORDER BY n DESC LIMIT 5"
        ).fetchall()
        LOGGER.error(
            "events 已存在重复 event_id，无法建立唯一索引，降级为应用层去重。"
            "重复样本（event_id, 行数）：%s",
            [(row["event_id"], row["n"]) for row in duplicates],
        )


def ensure_fingerprint_index(conn: sqlite3.Connection) -> None:
    """内容指纹唯一索引：保证同一批数据在账本里只登记一次。"""
    try:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_batches_sha "
            "ON ingested_batches(content_sha256)"
        )
    except sqlite3.IntegrityError:
        LOGGER.error(
            "ingested_batches 已存在重复 content_sha256，无法建立唯一索引；"
            "批次级去重仍由应用层按指纹比对保证"
        )


def stored_watermark(conn: sqlite3.Connection) -> str | None:
    """只读 pipeline_state 里已落盘的水位，不做回退推断。"""
    row = conn.execute(
        "SELECT value FROM pipeline_state WHERE key = ?", (WATERMARK_KEY,)
    ).fetchone()
    return str(row["value"]) if row and row["value"] else None


def read_watermark(conn: sqlite3.Connection) -> str | None:
    """读取水位。以 pipeline_state 为准，缺失时退回 ingested_batches 最大 batch_date。"""
    return stored_watermark(conn) or legacy_watermark(conn)


def legacy_watermark(conn: sqlite3.Connection) -> str | None:
    """老库的接入进度：ingested_batches 中最大的 batch_date。"""
    row = conn.execute("SELECT MAX(batch_date) AS wm FROM ingested_batches").fetchone()
    return row["wm"] if row and row["wm"] else None


def seed_watermark(conn: sqlite3.Connection) -> str | None:
    """把水位显式落库。

    老库只有 ingested_batches 可推断水位，这一推断必须**先落盘**：否则每批登记后
    ``read_watermark`` 的回退值就会立刻等于刚登记的 batch_date，``raise_watermark``
    会误判"无需更新"，水位永远写不进 pipeline_state。
    """
    stored = stored_watermark(conn)
    if stored:
        return stored
    legacy = legacy_watermark(conn)
    if legacy is None:
        return None
    conn.execute(
        "INSERT INTO pipeline_state(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
        "updated_at = excluded.updated_at",
        (WATERMARK_KEY, legacy, now_text()),
    )
    LOGGER.info("水位初始化（自历史批次表）：%s", legacy)
    return legacy


def raise_watermark(conn: sqlite3.Connection, candidate: str | None) -> str | None:
    """把水位抬到 max(已落盘水位, candidate)，只升不降；返回当前水位。"""
    current = stored_watermark(conn)
    if not candidate:
        return current
    new = candidate if current is None else max(current, candidate)
    if new != current:
        conn.execute(
            "INSERT INTO pipeline_state(key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
            "updated_at = excluded.updated_at",
            (WATERMARK_KEY, new, now_text()),
        )
        LOGGER.info("水位更新：%s -> %s", current, new)
    return new


def known_fingerprints(conn: sqlite3.Connection) -> dict[str, str]:
    """已成功接入的内容指纹 -> 首个登记该内容的文件名。"""
    rows = conn.execute(
        "SELECT content_sha256, batch_file FROM ingested_batches "
        "WHERE content_sha256 IS NOT NULL ORDER BY rowid"
    )
    known: dict[str, str] = {}
    for row in rows:
        known.setdefault(row["content_sha256"], row["batch_file"])
    return known


def record_attempt(
    conn: sqlite3.Connection,
    path: Path,
    sha256: str,
    outcome: str,
    detail: str = "",
    duplicate_of: str = "",
) -> None:
    """登记未接入的尝试（重投 / 损坏 / 失败）。按 (文件名, 指纹) 累计次数，不重复膨胀。"""
    stamp = now_text()
    conn.execute(
        UPSERT_ATTEMPT_SQL,
        (path.name, sha256, outcome, detail[:500], duplicate_of, stamp, stamp),
    )


def clear_attempt(conn: sqlite3.Connection, path: Path, sha256: str) -> None:
    """批次成功接入后，清掉它此前留下的失败/重投记录。"""
    conn.execute(
        "DELETE FROM batch_attempts WHERE batch_file = ? AND content_sha256 = ?",
        (path.name, sha256),
    )


# ---------------------------------------------------------------------------
# 文件解析与校验
# ---------------------------------------------------------------------------


def compute_fingerprint(rows: list[Event]) -> str:
    """批次内容指纹：逐行规整、排序后取 sha256。

    排序使指纹与文件内行序、行尾空白、换行符差异无关；只按数据本身判定
    "是不是同一批"。
    """
    digest = hashlib.sha256()
    for line in sorted(row.fingerprint() for row in rows):
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def parse_amount(
    raw: str, field_name: str, line_no: int, errors: list[str]
) -> Decimal | None:
    text = (raw or "").strip()
    if not text:
        errors.append(f"第 {line_no} 行 {field_name} 为空")
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        errors.append(f"第 {line_no} 行 {field_name} 不是合法数字：{raw!r}")
        return None
    if not value.is_finite():
        errors.append(f"第 {line_no} 行 {field_name} 不是有限数字：{raw!r}")
        return None
    return value


def validate_batch_date(raw: str, line_no: int, errors: list[str]) -> str | None:
    """校验 batch_date。格式是封闭字面量 YYYY-MM-DD，按固定位置检查后交给标准库解析。"""
    text = (raw or "").strip()
    if not text:
        errors.append(f"第 {line_no} 行 batch_date 为空")
        return None
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        errors.append(f"第 {line_no} 行 batch_date 不是 YYYY-MM-DD 格式：{raw!r}")
        return None
    try:
        date.fromisoformat(text)
    except ValueError:
        errors.append(f"第 {line_no} 行 batch_date 不是合法日期：{raw!r}")
        return None
    return text


def load_batch(path: Path) -> Batch:
    """读取并校验一个批次文件；任何数据问题都抛出 IngestError（整批拒绝）。"""
    errors: list[str] = []
    rows: list[Event] = []
    batch_dates: set[str] = set()

    try:
        handle = path.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise IngestError(f"无法读取文件：{exc}") from exc

    with handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        missing = [name for name in REQUIRED_COLUMNS if name not in header]
        if missing:
            raise IngestError(
                f"表头缺少字段：{', '.join(missing)}（实际表头：{header}）"
            )

        for line_no, raw_row in enumerate(reader, start=2):
            if len(errors) >= MAX_ROW_ERRORS:
                break
            event_id = (raw_row.get("event_id") or "").strip()
            member_id = (raw_row.get("member_id") or "").strip()
            event_time = (raw_row.get("event_time") or "").strip()
            event_type = (raw_row.get("event_type") or "").strip()
            if not event_id:
                errors.append(f"第 {line_no} 行 event_id 为空")
                continue
            if not member_id:
                errors.append(f"第 {line_no} 行（{event_id}）member_id 为空")
                continue
            if not event_time:
                errors.append(f"第 {line_no} 行（{event_id}）event_time 为空")
                continue
            if not event_type:
                errors.append(f"第 {line_no} 行（{event_id}）event_type 为空")
                continue
            amount_orig = parse_amount(
                raw_row.get("amount_orig") or "", "amount_orig", line_no, errors
            )
            amount_cny = parse_amount(
                raw_row.get("amount_cny") or "", "amount_cny", line_no, errors
            )
            batch_date = validate_batch_date(
                raw_row.get("batch_date") or "", line_no, errors
            )
            if amount_orig is None or amount_cny is None or batch_date is None:
                continue
            batch_dates.add(batch_date)
            rows.append(
                Event(
                    event_id=event_id,
                    member_id=member_id,
                    event_time=event_time,
                    event_type=event_type,
                    amount_orig=amount_orig,
                    amount_cny=amount_cny,
                    batch_date=batch_date,
                )
            )

    if errors:
        raise IngestError("；".join(errors))
    if not rows:
        raise IngestError("文件中没有可接入的数据行")
    if len(batch_dates) > 1:
        raise IngestError(
            "文件内 batch_date 不一致（应为单一批次）："
            + ", ".join(sorted(batch_dates))
        )

    # 文件内按 event_id 去重，保留首次出现的行；同 id 不同载荷属于上游数据问题，告警。
    seen: dict[str, Event] = {}
    deduped: list[Event] = []
    duplicates: list[Event] = []
    for row in rows:
        prior = seen.get(row.event_id)
        if prior is None:
            seen[row.event_id] = row
            deduped.append(row)
            continue
        duplicates.append(row)
        if prior.fingerprint() != row.fingerprint():
            LOGGER.warning(
                "%s 内 event_id %s 出现多次且内容不同，保留首行",
                path.name,
                row.event_id,
            )

    return Batch(
        path=path,
        rows=deduped,
        batch_date=next(iter(batch_dates)),
        sha256=compute_fingerprint(rows),
        duplicate_rows=duplicates,
    )


# ---------------------------------------------------------------------------
# 写入
# ---------------------------------------------------------------------------


def fetch_existing(
    conn: sqlite3.Connection, event_ids: list[str]
) -> dict[str, sqlite3.Row]:
    """按 event_id 批量取库内已有行。"""
    existing: dict[str, sqlite3.Row] = {}
    for start in range(0, len(event_ids), CHUNK_SIZE):
        chunk = event_ids[start : start + CHUNK_SIZE]
        placeholders = ",".join("?" for _ in chunk)
        query = (
            "SELECT event_id, member_id, event_time, event_type, amount_orig, "
            "amount_cny, batch_date FROM events "
            f"WHERE event_id IN ({placeholders})"
        )
        for row in conn.execute(query, chunk):
            existing.setdefault(row["event_id"], row)
    return existing


def amounts_equal(existing_value: float | None, incoming: Decimal) -> bool:
    if existing_value is None:
        return False
    return abs(Decimal(str(existing_value)) - incoming) <= AMOUNT_TOLERANCE


def payload_matches(row: sqlite3.Row, event: Event) -> bool:
    return (
        row["member_id"] == event.member_id
        and row["event_time"] == event.event_time
        and row["event_type"] == event.event_type
        and row["batch_date"] == event.batch_date
        and amounts_equal(row["amount_orig"], event.amount_orig)
        and amounts_equal(row["amount_cny"], event.amount_cny)
    )


def rollback(conn: sqlite3.Connection, batch_name: str) -> None:
    """尽力回滚；本身失败只记日志，不掩盖原始异常。"""
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        LOGGER.warning("批次 %s 的 ROLLBACK 未生效（事务可能已结束）", batch_name)


def ingest_batch(
    conn: sqlite3.Connection, batch: Batch, watermark: str | None
) -> BatchResult:
    """在单个事务内接入一个批次；任何一步失败都整批回滚并上抛。"""
    result = BatchResult(
        path=batch.path,
        batch_date=batch.batch_date,
        rows_read=len(batch.rows),
        rows_duplicate=len(batch.duplicate_rows),
        file_amount=batch.amount_total,
        sha256=batch.sha256,
        late=watermark is not None and batch.batch_date < watermark,
    )

    event_ids = [row.event_id for row in batch.rows]
    to_insert: list[Event] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = fetch_existing(conn, event_ids)
        for row in batch.rows:
            current = existing.get(row.event_id)
            if current is None:
                to_insert.append(row)
                continue
            result.rows_duplicate += 1
            if not payload_matches(current, row):
                result.conflicts.append(
                    Conflict(
                        event_id=row.event_id,
                        existing=dict(current),
                        incoming=row,
                    )
                )

        if to_insert:
            conn.executemany(INSERT_EVENT_SQL, [row.as_db_tuple() for row in to_insert])
        result.rows_inserted = len(to_insert)
        result.rows_conflict = len(result.conflicts)
        result.inserted_amount = sum((row.amount_cny for row in to_insert), Decimal(0))

        conn.execute(
            REGISTER_BATCH_SQL,
            (
                batch.path.name,
                batch.batch_date,
                result.rows_read,
                now_text(),
                batch.sha256,
                result.rows_inserted,
                result.rows_duplicate,
                result.rows_conflict,
                float(result.inserted_amount),
            ),
        )
        clear_attempt(conn, batch.path, batch.sha256)
        raise_watermark(conn, batch.batch_date)
        conn.execute("COMMIT")
    except Exception as exc:
        rollback(conn, batch.path.name)
        LOGGER.exception("批次 %s 接入失败，已整批回滚", batch.path.name)
        result.status = "failed"
        result.reason = str(exc)
        result.rows_inserted = 0
        result.inserted_amount = Decimal(0)
        result.conflicts = []
        raise

    LOGGER.info(
        "批次 %s：读取 %d 行，新接入 %d 行，重复跳过 %d 行，冲突 %d 行，金额 %s",
        batch.path.name,
        result.rows_read,
        result.rows_inserted,
        result.rows_duplicate,
        result.rows_conflict,
        format_amount(result.inserted_amount),
    )
    return result


# ---------------------------------------------------------------------------
# 管道
# ---------------------------------------------------------------------------


def batch_files(inbox: Path) -> list[Path]:
    if not inbox.is_dir():
        raise IngestError(f"批次目录不存在：{inbox}")
    return sorted(
        (p for p in inbox.iterdir() if p.is_file() and p.suffix.lower() == ".csv"),
        key=lambda p: p.name,
    )


def run_pipeline(inbox: Path, db_path: Path, report_path: Path) -> RunSummary:
    """处理 inbox 中尚未接入的批次，写库并生成对账报告。"""
    summary = RunSummary(started_at=datetime.now().astimezone())
    conn = open_database(db_path)
    try:
        summary.watermark_before = read_watermark(conn)
        known = known_fingerprints(conn)

        for path in batch_files(inbox):
            try:
                batch = load_batch(path)
            except IngestError as exc:
                LOGGER.error("批次 %s 校验不通过：%s", path.name, exc)
                record_attempt(
                    conn, path, compute_file_digest(path), OUTCOME_INVALID, str(exc)
                )
                summary.results.append(
                    BatchResult(path=path, status="failed", reason=str(exc))
                )
                continue

            if batch.sha256 in known:
                original = known[batch.sha256]
                LOGGER.info("批次 %s 内容与 %s 相同，整批跳过", path.name, original)
                record_attempt(
                    conn, path, batch.sha256, OUTCOME_DUPLICATE, duplicate_of=original
                )
                summary.results.append(
                    BatchResult(
                        path=path,
                        batch_date=batch.batch_date,
                        rows_read=len(batch.rows),
                        rows_duplicate=len(batch.rows),
                        file_amount=batch.amount_total,
                        sha256=batch.sha256,
                        duplicate_of=original,
                    )
                )
                continue

            try:
                result = ingest_batch(conn, batch, summary.watermark_before)
            except Exception as exc:  # noqa: BLE001 - 单批失败不阻断其余批次
                LOGGER.error("批次 %s 处理失败，本批未接入：%s", path.name, exc)
                record_attempt(conn, path, batch.sha256, OUTCOME_FAILED, str(exc))
                summary.results.append(
                    BatchResult(
                        path=path,
                        batch_date=batch.batch_date,
                        rows_read=len(batch.rows),
                        file_amount=batch.amount_total,
                        sha256=batch.sha256,
                        status="failed",
                        reason=str(exc),
                    )
                )
                continue

            known[batch.sha256] = path.name
            summary.results.append(result)

        summary.watermark_after = read_watermark(conn)
        summary.total_rows, summary.total_amount = cumulative_totals(conn)
    finally:
        conn.close()

    write_report(report_path, summary, db_path, inbox)
    return summary


def compute_file_digest(path: Path) -> str:
    """原始文件字节的 sha256，仅用于给无法解析的文件做去重登记。"""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        LOGGER.warning("无法读取 %s 计算文件指纹", path)
        return ""


def cumulative_totals(conn: sqlite3.Connection) -> tuple[int, Decimal]:
    """全库累计行数与金额。金额用 Decimal 逐行累加，避免 REAL 求和的末位漂移。"""
    count = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
    total = Decimal(0)
    for row in conn.execute("SELECT amount_cny FROM events"):
        if row["amount_cny"] is not None:
            total += Decimal(str(row["amount_cny"]))
    return int(count), total


# ---------------------------------------------------------------------------
# 报告
# ---------------------------------------------------------------------------


def display_width(text: str) -> int:
    """终端显示宽度：中文等全角字符占 2 列。"""
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)


def pad(text: str, width: int) -> str:
    return text + " " * max(0, width - display_width(text))


def format_amount(value: Decimal) -> str:
    return f"{value:,.2f}"


def format_int(value: int) -> str:
    return f"{value:,}"


def render_table(header: tuple[str, ...], rows: list[tuple[str, ...]]) -> list[str]:
    widths = [
        max(display_width(header[i]), *(display_width(r[i]) for r in rows))
        if rows
        else display_width(header[i])
        for i in range(len(header))
    ]

    def join(cells: tuple[str, ...]) -> str:
        return "  ".join(pad(cell, widths[i]) for i, cell in enumerate(cells)).rstrip()

    lines = [join(header)]
    lines.append("  ".join("-" * w for w in widths))
    for row in rows:
        lines.append(join(row))
    return lines


def status_text(result: BatchResult) -> str:
    if result.status != "ok":
        return "失败"
    if result.duplicate_of:
        if result.duplicate_of == result.path.name:
            return "跳过（本文件此前已接入）"
        return f"跳过（重复于 {result.duplicate_of}）"
    if result.rows_inserted == 0:
        return "跳过（行均已存在）"
    return "接入（晚到）" if result.late else "接入"


def render_report(summary: RunSummary, db_path: Path, inbox: Path) -> str:
    lines: list[str] = []
    lines.append("会员行为日志接入对账报告")
    lines.append(f"生成时间：{summary.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"库文件：{db_path}")
    lines.append(f"批次目录：{inbox}")
    lines.append("")

    lines.append("== 本次运行 ==")
    if not summary.results:
        lines.append("批次目录下没有 CSV 文件，本次未接入任何批次。")
    else:
        lines.append(
            f"扫描批次文件 {len(summary.results)} 个："
            f"新接入 {len(summary.ingested)} 个，"
            f"重投跳过 {len(summary.redelivered)} 个，"
            f"失败 {len(summary.failed)} 个。"
        )
        lines.append("")
        lines.append("本次接入批次：")
        table_rows = [
            (
                r.path.name,
                r.batch_date or "-",
                format_int(r.rows_read),
                format_int(r.rows_inserted),
                format_amount(r.inserted_amount),
                format_int(r.rows_duplicate),
                format_int(r.rows_conflict),
                status_text(r),
            )
            for r in summary.results
        ]
        lines.extend(
            render_table(
                (
                    "批次文件",
                    "批次日期",
                    "读取行数",
                    "新接入行数",
                    "新接入金额(CNY)",
                    "跳过行数",
                    "冲突行数",
                    "状态",
                ),
                table_rows,
            )
        )
        lines.append("")
        lines.append(
            f"本次合计：新接入 {format_int(summary.inserted_rows)} 行，"
            f"金额 {format_amount(summary.inserted_amount)} CNY。"
        )

    late = [r for r in summary.results if r.late and r.rows_inserted > 0]
    if late:
        lines.append("")
        lines.append(
            "== 晚到批次（batch_date 早于当时水位，按内容指纹判定为新批次接入）=="
        )
        for r in late:
            lines.append(
                f"{r.path.name}：batch_date {r.batch_date}，"
                f"新接入 {format_int(r.rows_inserted)} 行，"
                f"金额 {format_amount(r.inserted_amount)} CNY"
            )

    conflicts = [(r, c) for r in summary.results for c in r.conflicts]
    if conflicts:
        lines.append("")
        lines.append("== 事件冲突（库内保留先到版本，本批同 event_id 的行未写入）==")
        lines.append(
            f"冲突行共 {format_int(len(conflicts))} 条，"
            f"下列最多 {MAX_CONFLICTS_IN_REPORT} 条："
        )
        for result, conflict in conflicts[:MAX_CONFLICTS_IN_REPORT]:
            incoming = conflict.incoming
            lines.append(
                f"{conflict.event_id}（{result.path.name}）："
                f"库内 batch_date {conflict.existing['batch_date']} / "
                f"amount_cny {conflict.existing['amount_cny']}，"
                f"本批 batch_date {incoming.batch_date} / "
                f"amount_cny {incoming.amount_cny}"
            )

    if summary.failed:
        lines.append("")
        lines.append("== 失败批次（整批未接入，修正后可直接重跑）==")
        for r in summary.failed:
            lines.append(f"{r.path.name}：{r.reason}")

    lines.append("")
    lines.append("== 累计对账 ==")
    lines.append(
        f"水位（已接入最大 batch_date）："
        f"{summary.watermark_before or '（无）'} -> {summary.watermark_after or '（无）'}"
    )
    lines.append(f"累计接入行数：{format_int(summary.total_rows)}")
    lines.append(f"累计金额合计：{format_amount(summary.total_amount)} CNY")
    lines.append("")
    lines.append(
        "说明：金额按 events.amount_cny 逐行精确累加（保留 2 位小数）；"
        "重投的批次文件、已存在的 event_id 均不会二次入库。"
    )
    return "\n".join(lines) + "\n"


def write_report(
    report_path: Path, summary: RunSummary, db_path: Path, inbox: Path
) -> None:
    """原子写报告，避免财务读到半截文件。"""
    content = render_report(summary, db_path, inbox)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".report-", dir=str(report_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_name, report_path)
    except Exception:
        LOGGER.exception("报告写入失败：%s", report_path)
        Path(tmp_name).unlink(missing_ok=True)
        raise
    LOGGER.info("对账报告已写入 %s", report_path)


# ---------------------------------------------------------------------------
# 命令行
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="会员行为日志接入管道：把 inbox/ 下新到的批次幂等写入 "
        "warehouse.db，并输出 report.txt。",
    )
    parser.add_argument(
        "--inbox", type=Path, default=DEFAULT_INBOX, help="批次目录（默认 inbox/）"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="SQLite 库路径（默认 data/warehouse.db）",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT,
        help="对账报告路径（默认 report.txt）",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="输出调试日志")
    return parser.parse_args(argv)


def acquire_lock(db_path: Path):
    """同一库上的并发运行直接拒绝，避免两个进程交错写同一批。"""
    lock_path = db_path.with_suffix(db_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise IngestError(f"另一个接入进程正持有锁 {lock_path}，本次退出") from None
    return handle


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    try:
        lock = acquire_lock(args.db)
    except IngestError as exc:
        LOGGER.error("%s", exc)
        return 2
    try:
        summary = run_pipeline(args.inbox, args.db, args.report)
    except IngestError as exc:
        LOGGER.error("管道中止：%s", exc)
        return 2
    finally:
        lock.close()

    print(
        f"本次接入 {len(summary.ingested)} 个批次、"
        f"{format_int(summary.inserted_rows)} 行，"
        f"金额 {format_amount(summary.inserted_amount)} CNY；"
        f"累计 {format_int(summary.total_rows)} 行、"
        f"{format_amount(summary.total_amount)} CNY。详见 {args.report}"
    )
    return 0 if not summary.failed else 1


if __name__ == "__main__":
    sys.exit(main())
