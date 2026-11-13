"""ingest.py 的测试：幂等、事务性、水位、对账报告。

每个用例都在 tmp_path 里造一份独立的库和 inbox，不碰仓库里的真实数据。
"""

from __future__ import annotations

import csv
import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import ingest

HEADER = [
    "event_id",
    "member_id",
    "event_time",
    "event_type",
    "amount_orig",
    "amount_cny",
    "batch_date",
]


def make_row(
    event_id: str, batch_date: str, amount_cny: str, *, hours: int = 0
) -> dict:
    return {
        "event_id": event_id,
        "member_id": "M000001",
        "event_time": f"{batch_date} 1{hours:02d}:00:00",
        "event_type": "click",
        "amount_orig": amount_cny,
        "amount_cny": amount_cny,
        "batch_date": batch_date,
    }


def write_csv(path: Path, rows: list[dict], header: list[str] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header or HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def make_legacy_db(path: Path, rows: list[dict], batches: list[tuple]) -> Path:
    """造一个"历史库"：events 无唯一约束，ingested_batches 是旧四列表结构。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE events (event_id TEXT, member_id TEXT, event_time TEXT, "
        "event_type TEXT, amount_orig REAL, amount_cny REAL, batch_date TEXT)"
    )
    conn.execute(
        "CREATE TABLE ingested_batches (batch_file TEXT, batch_date TEXT, "
        "row_count INTEGER, ingested_at TEXT)"
    )
    for row in rows:
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row["event_id"],
                row["member_id"],
                row["event_time"],
                row["event_type"],
                float(row["amount_orig"]),
                float(row["amount_cny"]),
                row["batch_date"],
            ),
        )
    conn.executemany("INSERT INTO ingested_batches VALUES (?, ?, ?, ?)", batches)
    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def env(tmp_path: Path) -> dict:
    """一套干净环境：空库 + 空 inbox + 报告路径。"""
    return {
        "db": tmp_path / "data" / "warehouse.db",
        "inbox": tmp_path / "inbox",
        "report": tmp_path / "report.txt",
    }


def run(env: dict) -> ingest.RunSummary:
    return ingest.run_pipeline(env["inbox"], env["db"], env["report"])


def count_events(db: Path) -> int:
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        conn.close()


def event_ids(db: Path) -> list[str]:
    conn = sqlite3.connect(str(db))
    try:
        return [row[0] for row in conn.execute("SELECT event_id FROM events")]
    finally:
        conn.close()


def total_amount(db: Path) -> Decimal:
    conn = sqlite3.connect(str(db))
    try:
        total = Decimal(0)
        for (value,) in conn.execute("SELECT amount_cny FROM events"):
            total += Decimal(str(value))
        return total
    finally:
        conn.close()


def read_report(env: dict) -> str:
    return env["report"].read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 基本接入
# ---------------------------------------------------------------------------


def test_ingests_new_batch_and_writes_report(env: dict) -> None:
    rows = [make_row(f"E{i}", "2025-11-07", f"{i}.50") for i in range(1, 6)]
    write_csv(env["inbox"] / "batch_20251107.csv", rows)

    summary = run(env)

    assert len(summary.ingested) == 1
    assert summary.inserted_rows == 5
    assert count_events(env["db"]) == 5
    assert total_amount(env["db"]) == Decimal("17.50")

    report = read_report(env)
    assert "batch_20251107.csv" in report
    assert "累计接入行数：5" in report
    assert "17.50" in report


def test_creates_schema_on_empty_database(env: dict) -> None:
    write_csv(env["inbox"] / "b.csv", [make_row("E1", "2025-11-01", "1.00")])
    run(env)

    conn = sqlite3.connect(str(env["db"]))
    try:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        indexes = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        }
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(ingested_batches)")
        }
    finally:
        conn.close()

    assert {"events", "ingested_batches", "pipeline_state", "batch_attempts"} <= tables
    assert "ux_events_event_id" in indexes
    assert {"content_sha256", "rows_inserted", "rows_duplicate"} <= columns


# ---------------------------------------------------------------------------
# 幂等：同名重传 / 改名重传 / 整个目录重跑 / _r1 后缀
# ---------------------------------------------------------------------------


def test_rerun_same_directory_is_idempotent(env: dict) -> None:
    write_csv(
        env["inbox"] / "batch_20251107.csv",
        [make_row(f"E{i}", "2025-11-07", "10.00") for i in range(1, 4)],
    )

    first = run(env)
    second = run(env)
    third = run(env)

    assert first.inserted_rows == 3
    assert second.inserted_rows == 0
    assert third.inserted_rows == 0
    assert count_events(env["db"]) == 3
    assert total_amount(env["db"]) == Decimal("30.00")


def test_renamed_duplicate_is_skipped_by_content_fingerprint(env: dict) -> None:
    rows = [make_row(f"E{i}", "2025-11-09", "5.00") for i in range(1, 5)]
    write_csv(env["inbox"] / "batch_20251109.csv", rows)

    first = run(env)
    assert first.inserted_rows == 4

    # 同一批数据改名重投（内容一致，文件名不同）。
    write_csv(env["inbox"] / "batch_20251109_r1.csv", rows)
    second = run(env)

    assert second.inserted_rows == 0
    # 原文件与 _r1 副本都按指纹判定为已接入内容，指向首次登记的文件名。
    renamed = [r for r in second.redelivered if r.path.name == "batch_20251109_r1.csv"]
    assert len(renamed) == 1
    assert renamed[0].duplicate_of == "batch_20251109.csv"
    assert count_events(env["db"]) == 4
    assert total_amount(env["db"]) == Decimal("20.00")


def test_duplicate_with_reordered_rows_still_matches(env: dict) -> None:
    """上游重投时行序可能不同，指纹按内容排序，仍应判定为同一批。"""
    rows = [make_row(f"E{i}", "2025-11-09", "5.00") for i in range(1, 5)]
    write_csv(env["inbox"] / "batch_20251109.csv", rows)
    run(env)

    write_csv(env["inbox"] / "batch_20251109_r1.csv", list(reversed(rows)))
    summary = run(env)

    assert summary.inserted_rows == 0
    assert count_events(env["db"]) == 4


def test_same_name_redelivery_after_success_is_skipped(env: dict) -> None:
    write_csv(env["inbox"] / "b.csv", [make_row("E1", "2025-11-03", "2.00")])
    run(env)
    write_csv(env["inbox"] / "b.csv", [make_row("E1", "2025-11-03", "2.00")])
    summary = run(env)

    assert summary.inserted_rows == 0
    assert summary.redelivered[0].duplicate_of == "b.csv"


def test_duplicate_row_inside_one_file_is_collapsed(env: dict) -> None:
    rows = [make_row("E1", "2025-11-03", "2.00"), make_row("E1", "2025-11-03", "2.00")]
    write_csv(env["inbox"] / "b.csv", rows)
    summary = run(env)

    assert summary.inserted_rows == 1
    assert count_events(env["db"]) == 1


def test_partially_overlapping_redelivery_inserts_only_new_rows(env: dict) -> None:
    """名字变了、内容也变了（多出几行）：靠 event_id 去重只补新行。"""
    write_csv(
        env["inbox"] / "a.csv",
        [make_row(f"E{i}", "2025-11-09", "1.00") for i in range(1, 4)],
    )
    run(env)

    write_csv(
        env["inbox"] / "a_r2.csv",
        [make_row(f"E{i}", "2025-11-09", "1.00") for i in range(1, 6)],
    )
    summary = run(env)

    assert summary.inserted_rows == 2
    assert count_events(env["db"]) == 5
    assert len(event_ids(env["db"])) == len(set(event_ids(env["db"])))


def test_two_different_files_with_same_event_ids_do_not_duplicate(env: dict) -> None:
    write_csv(env["inbox"] / "a.csv", [make_row("E1", "2025-11-09", "1.00")])
    write_csv(env["inbox"] / "b.csv", [make_row("E1", "2025-11-10", "2.00")])

    summary = run(env)

    assert summary.inserted_rows == 1
    assert count_events(env["db"]) == 1
    conflicts = [c for r in summary.results for c in r.conflicts]
    assert len(conflicts) == 1
    assert conflicts[0].event_id == "E1"
    assert "事件冲突" in read_report(env)


def test_conflict_keeps_first_version(env: dict) -> None:
    """同一 event_id 在两批里载荷不同：保留先到的，后到的不写、只记账。"""
    write_csv(env["inbox"] / "a.csv", [make_row("E1", "2025-11-09", "1.00")])
    run(env)
    write_csv(env["inbox"] / "b.csv", [make_row("E1", "2025-11-10", "99.00")])
    run(env)

    conn = sqlite3.connect(str(env["db"]))
    try:
        value, batch_date = conn.execute(
            "SELECT amount_cny, batch_date FROM events WHERE event_id='E1'"
        ).fetchone()
    finally:
        conn.close()
    assert value == 1.0
    assert batch_date == "2025-11-09"


# ---------------------------------------------------------------------------
# 水位与晚到批次
# ---------------------------------------------------------------------------


def test_late_batch_before_watermark_is_still_ingested(env: dict) -> None:
    """batch_date 早于水位的晚到批次不能被水位过滤掉。"""
    write_csv(
        env["inbox"] / "batch_20251107.csv", [make_row("E2", "2025-11-07", "4.00")]
    )
    run(env)

    # 晚到的 11-04 批次在水位已经到 11-07 之后才到达。
    write_csv(
        env["inbox"] / "batch_20251104_late.csv", [make_row("E1", "2025-11-04", "3.00")]
    )
    summary = run(env)

    assert summary.inserted_rows == 1
    assert count_events(env["db"]) == 2
    late = [r for r in summary.results if r.late]
    assert [r.path.name for r in late] == ["batch_20251104_late.csv"]
    assert "晚到批次" in read_report(env)


def test_watermark_only_moves_forward(env: dict) -> None:
    write_csv(
        env["inbox"] / "batch_20251107.csv", [make_row("E1", "2025-11-07", "1.00")]
    )
    run(env)
    write_csv(
        env["inbox"] / "batch_20251104_late.csv", [make_row("E2", "2025-11-04", "1.00")]
    )
    summary = run(env)

    assert summary.watermark_before == "2025-11-07"
    assert summary.watermark_after == "2025-11-07"


def test_watermark_is_persisted_to_pipeline_state(env: dict) -> None:
    """水位必须真写进 pipeline_state，不能靠回退推断"看起来对"。"""
    write_csv(
        env["inbox"] / "batch_20251109.csv", [make_row("E1", "2025-11-09", "1.00")]
    )
    run(env)

    conn = sqlite3.connect(str(env["db"]))
    try:
        stored = dict(conn.execute("SELECT key, value FROM pipeline_state"))
    finally:
        conn.close()
    assert stored == {ingest.WATERMARK_KEY: "2025-11-09"}


def test_watermark_resumes_from_legacy_batches_table(env: dict) -> None:
    """历史库只有旧结构的 ingested_batches：水位要能从那里接上。"""
    make_legacy_db(
        env["db"],
        [make_row("EOLD", "2025-11-05", "1.00")],
        [("batch_20251105.csv", "2025-11-05", 1, "2025-11-05 23:55:00")],
    )
    write_csv(
        env["inbox"] / "batch_20251109.csv", [make_row("E1", "2025-11-09", "1.00")]
    )
    summary = run(env)

    assert summary.inserted_rows == 1
    assert summary.watermark_before == "2025-11-05"
    assert summary.watermark_after == "2025-11-09"
    assert count_events(env["db"]) == 2


def test_legacy_batches_are_not_reingested_without_fingerprint(env: dict) -> None:
    """历史账本没有指纹时，靠 event_id 唯一索引兜底：数据不会重复，并记为冲突。"""
    legacy = [make_row("E1", "2025-11-05", "1.00")]
    make_legacy_db(
        env["db"],
        legacy,
        [("batch_20251105.csv", "2025-11-05", 1, "2025-11-05 23:55:00")],
    )
    write_csv(env["inbox"] / "batch_20251105.csv", legacy)
    summary = run(env)

    assert summary.inserted_rows == 0
    assert count_events(env["db"]) == 1


# ---------------------------------------------------------------------------
# 事务性
# ---------------------------------------------------------------------------


def test_failed_batch_leaves_no_partial_rows(
    env: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """写入中途炸掉：整批回滚，库里该批一行都不留。"""
    write_csv(
        env["inbox"] / "b.csv",
        [make_row(f"E{i}", "2025-11-09", "1.00") for i in range(1, 4)],
    )

    class FlakyConnection:
        """让 events 插入只写进去两行就炸，验证事务回滚。"""

        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def __getattr__(self, name: str):
            return getattr(self._conn, name)

        def executemany(self, sql: str, params):
            if "INSERT OR IGNORE INTO events" in sql:
                self._conn.executemany(sql, list(params)[:2])
                raise sqlite3.OperationalError("磁盘满（测试注入）")
            return self._conn.executemany(sql, params)

    original_open = ingest.open_database
    monkeypatch.setattr(
        ingest, "open_database", lambda path: FlakyConnection(original_open(path))
    )
    summary = run(env)

    assert len(summary.failed) == 1
    assert summary.inserted_rows == 0
    assert count_events(env["db"]) == 0
    assert "磁盘满（测试注入）" in read_report(env)


def test_failed_batch_can_be_retried_after_fix(env: dict) -> None:
    """坏批次不入库；修好文件后重跑能正常接入，且不会重复。"""
    bad = [make_row("E1", "2025-11-09", "abc")]
    write_csv(env["inbox"] / "b.csv", bad)
    summary = run(env)

    assert len(summary.failed) == 1
    assert count_events(env["db"]) == 0

    write_csv(env["inbox"] / "b.csv", [make_row("E1", "2025-11-09", "1.00")])
    fixed = run(env)

    assert len(fixed.failed) == 0
    assert fixed.inserted_rows == 1
    assert count_events(env["db"]) == 1


def test_other_batches_still_ingest_when_one_fails(env: dict) -> None:
    write_csv(env["inbox"] / "bad.csv", [make_row("E9", "2025-11-08", "x")])
    write_csv(env["inbox"] / "good.csv", [make_row("E1", "2025-11-08", "1.00")])
    summary = run(env)

    assert summary.inserted_rows == 1
    assert [r.path.name for r in summary.failed] == ["bad.csv"]


# ---------------------------------------------------------------------------
# 校验
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rows", "needle"),
    [
        ([make_row("E1", "2025-11-08", "1.00") | {"event_id": ""}], "event_id 为空"),
        (
            [make_row("E1", "2025-11-08", "1.00") | {"amount_cny": ""}],
            "amount_cny 为空",
        ),
        (
            [make_row("E1", "2025-11-08", "1.00") | {"amount_cny": "abc"}],
            "不是合法数字",
        ),
        (
            [make_row("E1", "2025-11-08", "1.00") | {"amount_cny": "nan"}],
            "不是有限数字",
        ),
        (
            [make_row("E1", "2025-11-08", "1.00") | {"amount_cny": "inf"}],
            "不是有限数字",
        ),
        ([make_row("E1", "20251108", "1.00")], "不是 YYYY-MM-DD 格式"),
        ([make_row("E1", "2025-02-30", "1.00")], "不是合法日期"),
    ],
)
def test_invalid_rows_reject_whole_batch(
    env: dict, rows: list[dict], needle: str
) -> None:
    write_csv(env["inbox"] / "b.csv", rows)
    summary = run(env)

    assert len(summary.failed) == 1
    assert needle in summary.failed[0].reason
    assert count_events(env["db"]) == 0


def test_mixed_batch_dates_reject_whole_batch(env: dict) -> None:
    rows = [make_row("E1", "2025-11-08", "1.00"), make_row("E2", "2025-11-09", "1.00")]
    write_csv(env["inbox"] / "b.csv", rows)
    summary = run(env)

    assert len(summary.failed) == 1
    assert "batch_date 不一致" in summary.failed[0].reason
    assert count_events(env["db"]) == 0


def test_missing_header_column_rejects_batch(env: dict) -> None:
    header = HEADER[:-1]
    write_csv(
        env["inbox"] / "b.csv",
        [
            {
                k: v
                for k, v in make_row("E1", "2025-11-08", "1.00").items()
                if k in header
            }
        ],
        header=header,
    )
    summary = run(env)

    assert len(summary.failed) == 1
    assert "表头缺少字段：batch_date" in summary.failed[0].reason


def test_empty_file_rejects_batch(env: dict) -> None:
    write_csv(env["inbox"] / "b.csv", [])
    summary = run(env)

    assert len(summary.failed) == 1
    assert "没有可接入的数据行" in summary.failed[0].reason


def test_utf8_bom_file_is_accepted(env: dict) -> None:
    """上游偶尔带 BOM，utf-8-sig 应能正常读。"""
    path = env["inbox"] / "b.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADER)
        writer.writeheader()
        writer.writerow(make_row("E1", "2025-11-08", "1.00"))
    summary = run(env)

    assert summary.inserted_rows == 1


# ---------------------------------------------------------------------------
# 报告
# ---------------------------------------------------------------------------


def test_report_reports_amounts_for_finance(env: dict) -> None:
    write_csv(
        env["inbox"] / "batch_20251107.csv",
        [make_row(f"E{i}", "2025-11-07", "1000.10") for i in range(1, 4)],
    )
    run(env)
    write_csv(
        env["inbox"] / "batch_20251108.csv", [make_row("E9", "2025-11-08", "0.99")]
    )
    summary = run(env)

    report = read_report(env)
    assert "batch_20251108.csv" in report
    assert "本次合计：新接入 1 行，金额 0.99 CNY" in report
    assert "累计接入行数：4" in report
    assert "累计金额合计：3,001.29 CNY" in report
    assert summary.total_amount == Decimal("3001.29")


def test_report_is_written_even_when_nothing_is_new(env: dict) -> None:
    write_csv(env["inbox"] / "b.csv", [make_row("E1", "2025-11-08", "1.00")])
    run(env)
    run(env)

    report = read_report(env)
    assert "新接入 0 个" in report
    assert "累计接入行数：1" in report


def test_amount_total_is_exact_with_decimal_accumulation(env: dict) -> None:
    """浮点累加会出现 0.30000000000000004，报告必须用 Decimal 精确累加。"""
    rows = [make_row(f"E{i}", "2025-11-08", "0.10") for i in range(1, 4)]
    write_csv(env["inbox"] / "b.csv", rows)
    run(env)

    assert total_amount(env["db"]) == Decimal("0.30")
    assert "0.30" in read_report(env)


# ---------------------------------------------------------------------------
# 并发
# ---------------------------------------------------------------------------


def test_concurrent_run_is_rejected(env: dict) -> None:
    """两个进程同时跑同一库：后来者拿不到锁，直接退出（返回码 2）。"""
    import fcntl

    env["db"].parent.mkdir(parents=True, exist_ok=True)
    lock_path = env["db"].with_suffix(env["db"].suffix + ".lock")
    holder = lock_path.open("w")
    fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(ingest.IngestError):
            ingest.acquire_lock(env["db"])
    finally:
        holder.close()


def test_main_returns_nonzero_on_failure(
    env: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_csv(env["inbox"] / "bad.csv", [make_row("E1", "2025-11-08", "oops")])
    argv = [
        "--db",
        str(env["db"]),
        "--inbox",
        str(env["inbox"]),
        "--report",
        str(env["report"]),
    ]
    code = ingest.main(argv)
    assert code == 1


def test_main_returns_zero_when_successful(env: dict) -> None:
    write_csv(env["inbox"] / "b.csv", [make_row("E1", "2025-11-08", "1.00")])
    argv = [
        "--db",
        str(env["db"]),
        "--inbox",
        str(env["inbox"]),
        "--report",
        str(env["report"]),
    ]
    assert ingest.main(argv) == 0


# ---------------------------------------------------------------------------
# 真实样例
# ---------------------------------------------------------------------------


REAL_INBOX = REPO_ROOT / "inbox"
#: 用仓库初始提交里的历史库快照，而不是 data/warehouse.db —— 后者会被真实运行改写，
#: 用它当夹具会让测试随运行次数漂移。
LEGACY_DB_SNAPSHOT = (
    Path(__file__).resolve().parent / "fixtures" / "legacy_warehouse.db"
)

#: 初始快照里的历史行数与 8 个样例批次的期望接入量。
LEGACY_ROWS = 2005
SAMPLE_ROWS_INSERTED = 2769
SAMPLE_TOTAL_ROWS = 4774
SAMPLE_TOTAL_AMOUNT = Decimal("15358324.96")


@pytest.fixture()
def sample_copy(tmp_path: Path) -> dict:
    """把历史库快照 + 真实 inbox 复制到 tmp_path，避免测试污染仓库。"""
    import shutil

    if not REAL_INBOX.is_dir():
        pytest.skip("仓库里没有真实样例数据")
    if not LEGACY_DB_SNAPSHOT.is_file():
        pytest.skip("缺少历史库快照 tests/fixtures/legacy_warehouse.db")
    db = tmp_path / "data" / "warehouse.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(LEGACY_DB_SNAPSHOT, db)
    inbox = tmp_path / "inbox"
    shutil.copytree(REAL_INBOX, inbox)
    return {"db": db, "inbox": inbox, "report": tmp_path / "report.txt"}


def test_real_samples_full_run(sample_copy: dict) -> None:
    """8 个样例批次：晚到批次接入、_r1 重投跳过、累计值可对账。"""
    assert count_events(sample_copy["db"]) == LEGACY_ROWS
    summary = run(sample_copy)

    assert count_events(sample_copy["db"]) == SAMPLE_TOTAL_ROWS
    assert summary.inserted_rows == SAMPLE_ROWS_INSERTED
    assert summary.watermark_before == "2025-11-05"
    assert summary.watermark_after == "2025-11-12"
    assert [r.path.name for r in summary.redelivered] == ["batch_20251109_r1.csv"]
    assert len([c for r in summary.results for c in r.conflicts]) == 2
    assert summary.total_amount == SAMPLE_TOTAL_AMOUNT


def test_real_samples_rerun_produces_no_duplicates(sample_copy: dict) -> None:
    run(sample_copy)
    run(sample_copy)

    ids = event_ids(sample_copy["db"])
    assert len(ids) == len(set(ids))
    report = read_report(sample_copy)
    assert "新接入 0 行" in report
