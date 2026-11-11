"""ingest.py 的 pytest 测试。

测试数据全部在 tmp_path 下现造，不接触仓库真实的 inbox/ 与 data/warehouse.db。
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal

import pandas as pd
import pytest

import ingest

HEADER = "event_id,member_id,event_time,event_type,amount_orig,amount_cny,batch_date"


def write_csv(path, rows, *, batch_date=None):
    """rows 为不含 batch_date 的 6 字段元组列表；batch_date 自动补末列。"""
    lines = [HEADER]
    for event_id, member, event_time, event_type, amount_orig, amount_cny in rows:
        date = batch_date or event_time[:10]
        lines.append(
            f"{event_id},{member},{event_time},{event_type},{amount_orig},{amount_cny},{date}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_batch_rows(prefix, start, count, batch_date):
    rows = []
    for i in range(count):
        n = start + i
        rows.append(
            (
                f"E{prefix}{n:06d}",
                f"M{n:06d}",
                f"{batch_date} 10:00:{i % 60:02d}",
                "click",
                f"{100 + n}.01",
                f"{700 + n}.07",
            )
        )
    return rows


@pytest.fixture
def env(tmp_path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    db = tmp_path / "data" / "warehouse.db"
    report = tmp_path / "report.txt"
    return inbox, db, report


def init_legacy_db(db):
    """模拟题目交付时的历史库：无 content_hash 列、events 无唯一约束。"""
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE events (event_id TEXT, member_id TEXT, event_time TEXT, "
        "event_type TEXT, amount_orig REAL, amount_cny REAL, batch_date TEXT)"
    )
    con.execute(
        "CREATE TABLE ingested_batches (batch_file TEXT, batch_date TEXT, "
        "row_count INTEGER, ingested_at TEXT)"
    )
    con.commit()
    con.close()


def db_rows(db):
    con = sqlite3.connect(db)
    rows = con.execute(
        "SELECT event_id, member_id, event_time, event_type, amount_orig, "
        "amount_cny, batch_date FROM events ORDER BY event_id"
    ).fetchall()
    con.close()
    return rows


def db_registry(db):
    con = sqlite3.connect(db)
    rows = con.execute(
        "SELECT batch_file, batch_date, row_count, content_hash "
        "FROM ingested_batches ORDER BY batch_file"
    ).fetchall()
    con.close()
    return rows


def test_ingest_new_batches_and_idempotent_rerun(env):
    inbox, db, report = env
    write_csv(
        inbox / "batch_20251107.csv",
        make_batch_rows("A", 1, 3, "2025-11-07"),
    )
    write_csv(
        inbox / "batch_20251108.csv",
        make_batch_rows("B", 10, 2, "2025-11-08"),
    )

    assert ingest.run(inbox, db, report) == 0
    assert len(db_rows(db)) == 5
    assert len(db_registry(db)) == 2
    first_report = report.read_text(encoding="utf-8")
    assert "新增 5 行" in first_report

    # 整个目录重跑：不产生任何重复数据
    assert ingest.run(inbox, db, report) == 0
    assert len(db_rows(db)) == 5
    assert len(db_registry(db)) == 2

    text = report.read_text(encoding="utf-8")
    assert "共 0 个，新增 0 行" in text
    assert "共 2 个（内容指纹与已接入批次一致）" in text
    assert "无" in text  # 失败批次为无


def test_renamed_and_identically_named_redelivery(env):
    inbox, db, report = env
    rows = make_batch_rows("A", 1, 4, "2025-11-09")
    write_csv(inbox / "batch_20251109.csv", rows)
    assert ingest.run(inbox, db, report) == 0
    assert len(db_rows(db)) == 4

    # 改名重传（_r1 后缀），内容字节级一致
    write_csv(inbox / "batch_20251109_r1.csv", rows)
    # 重名重传：同文件名再放一份同样内容
    write_csv(inbox / "batch_20251109.csv", rows)
    assert ingest.run(inbox, db, report) == 0

    assert len(db_rows(db)) == 4
    assert len(db_registry(db)) == 1
    report_text = report.read_text(encoding="utf-8")
    assert "batch_20251109_r1.csv" in report_text
    assert "重传/重复文件" in report_text


def test_reshuffled_redelivery_still_detected(env):
    """同一批数据行顺序被上游打乱，内容指纹仍应命中。"""
    inbox, db, report = env
    rows = make_batch_rows("A", 1, 5, "2025-11-09")
    write_csv(inbox / "batch_20251109.csv", rows)
    ingest.run(inbox, db, report)

    write_csv(inbox / "batch_20251109_shuf.csv", list(reversed(rows)))
    assert ingest.run(inbox, db, report) == 0
    assert len(db_rows(db)) == 5
    assert len(db_registry(db)) == 1


def test_late_batch_before_watermark_is_ingested(env):
    """晚到批次：batch_date 早于已接入水位，只要内容不同就必须接入。"""
    inbox, db, report = env
    write_csv(
        inbox / "batch_20251108.csv",
        make_batch_rows("N", 1, 3, "2025-11-08"),
    )
    ingest.run(inbox, db, report)

    # 晚到的 11-04 批次，event_id 与已有数据完全不重叠
    write_csv(
        inbox / "batch_20251104_late.csv",
        make_batch_rows("L", 100, 2, "2025-11-04"),
    )
    assert ingest.run(inbox, db, report) == 0

    assert len(db_rows(db)) == 5
    registry = db_registry(db)
    assert {r[1] for r in registry} == {"2025-11-04", "2025-11-08"}
    text = report.read_text(encoding="utf-8")
    assert "2025-11-04" in text
    assert "最新批次日期（可读水位）: 2025-11-08" in text


def test_corrected_redelivery_is_new_batch(env):
    """内容被修正（金额变化）后重投：指纹变化，作为新批次接入。"""
    inbox, db, report = env
    rows = make_batch_rows("A", 1, 3, "2025-11-09")
    write_csv(inbox / "batch_20251109.csv", rows)
    ingest.run(inbox, db, report)

    corrected = [
        (eid, member, ts, etype, orig, "999.99")
        for (eid, member, ts, etype, orig, _cny) in rows
    ]
    write_csv(inbox / "batch_20251109_fix.csv", corrected)
    assert ingest.run(inbox, db, report) == 0

    # 两个不同内容的批次都登记，明细各 3 行
    assert len(db_rows(db)) == 6
    assert len(db_registry(db)) == 2


def test_transaction_rollback_on_failure(env, monkeypatch):
    """写入中途失败：events 与登记都不留该批次的半个数据。"""
    inbox, db, report = env
    write_csv(
        inbox / "batch_20251107.csv",
        make_batch_rows("A", 1, 3, "2025-11-07"),
    )
    write_csv(
        inbox / "batch_20251108.csv",
        make_batch_rows("B", 1, 2, "2025-11-08"),
    )

    # sqlite3.Connection 是 C 层不可变类型，无法 monkeypatch，
    # 改用 connect(factory=...) 子类在第二次写 events 明细时注入故障
    real_connect = sqlite3.connect
    events_inserts = {"n": 0}

    class FlakyConnection(sqlite3.Connection):
        def executemany(self, sql, parameters):
            if "INSERT INTO events" in sql:
                events_inserts["n"] += 1
                if events_inserts["n"] == 2:
                    raise sqlite3.OperationalError("simulated disk failure")
            return super().executemany(sql, parameters)

    def flaky_connect(path, *args, **kwargs):
        kwargs["factory"] = FlakyConnection
        return real_connect(path, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", flaky_connect)

    code = ingest.run(inbox, db, report)
    assert code == 1

    # 第一批次已提交；第二批次完全回滚
    assert len(db_rows(db)) == 3
    registry = db_registry(db)
    assert len(registry) == 1
    assert registry[0][1] == "2025-11-07"

    # 报告记录失败批次
    text = report.read_text(encoding="utf-8")
    assert "失败批次" in text
    assert "batch_20251108.csv" in text
    assert "事务已回滚" in text

    # 修复后重跑：失败批次可以正常补入，不重复不遗漏
    monkeypatch.undo()
    assert ingest.run(inbox, db, report) == 0
    assert len(db_rows(db)) == 5
    assert len(db_registry(db)) == 2


def test_invalid_batch_rejected_and_not_registered(env):
    inbox, db, report = env
    write_csv(
        inbox / "batch_20251107.csv",
        make_batch_rows("A", 1, 2, "2025-11-07"),
    )
    # 缺列的坏文件
    bad = inbox / "batch_20251108.csv"
    bad.write_text(
        "event_id,member_id\nX1,M1\n",
        encoding="utf-8",
    )
    code = ingest.run(inbox, db, report)
    assert code == 1
    assert len(db_rows(db)) == 2
    assert {r[0] for r in db_registry(db)} == {"batch_20251107.csv"}
    assert "缺少必需列" in report.read_text(encoding="utf-8")


def test_mixed_batch_date_in_one_file_rejected(env):
    inbox, db, report = env
    path = inbox / "batch_mix.csv"
    path.write_text(
        HEADER + "\n"
        "E1,M1,2025-11-07 10:00:00,click,1.00,7.00,2025-11-07\n"
        "E2,M2,2025-11-07 10:01:00,click,1.00,7.00,2025-11-08\n",
        encoding="utf-8",
    )
    assert ingest.run(inbox, db, report) == 1
    assert db_rows(db) == []
    assert "batch_date 不唯一" in report.read_text(encoding="utf-8")


def test_bad_amount_rejected(env):
    inbox, db, report = env
    path = inbox / "batch_bad.csv"
    path.write_text(
        HEADER + "\nE1,M1,2025-11-07 10:00:00,click,not-a-number,7.00,2025-11-07\n",
        encoding="utf-8",
    )
    assert ingest.run(inbox, db, report) == 1
    assert db_rows(db) == []


def test_legacy_schema_migration_and_totals(env):
    """对接题目交付时的老库：自动加列、累计口径正确、金额 Decimal 求和。"""
    inbox, db, report = env
    init_legacy_db(db)
    con = sqlite3.connect(db)
    con.execute(
        "INSERT INTO events VALUES "
        "('E0','M0','2025-11-05 09:00:00','click',1.10,7.70,'2025-11-05')"
    )
    con.execute(
        "INSERT INTO ingested_batches VALUES "
        "('batch_20251105.csv','2025-11-05',1,'2025-11-05 23:55:00')"
    )
    con.commit()
    con.close()

    write_csv(
        inbox / "batch_20251107.csv",
        make_batch_rows("A", 1, 2, "2025-11-07"),
    )
    assert ingest.run(inbox, db, report) == 0

    # 历史行 content_hash 为 NULL，新行有哈希且唯一索引生效
    registry = db_registry(db)
    hashes = [r[3] for r in registry]
    assert hashes[0] is None
    assert len(hashes[1]) == 64

    text = report.read_text(encoding="utf-8")
    assert "events 表累计行数: 3" in text
    # 701.07 + 702.07 + 7.70 = 1410.84
    assert "1,410.84" in text
    assert "最新批次日期（可读水位）: 2025-11-07" in text


def test_decimal_amounts_no_float_drift(env):
    """金额走 Decimal：0.1+0.2 这类场景报告与库内口径不出现浮点尾巴。"""
    inbox, db, report = env
    path = inbox / "batch_d.csv"
    path.write_text(
        HEADER + "\n"
        "E1,M1,2025-11-07 10:00:00,click,0.10,0.10,2025-11-07\n"
        "E2,M2,2025-11-07 10:01:00,click,0.20,0.20,2025-11-07\n",
        encoding="utf-8",
    )
    ingest.run(inbox, db, report)
    text = report.read_text(encoding="utf-8")
    assert "0.30" in text
    assert "0.300000" not in text


def test_fingerprint_logic_directly(env):
    inbox, _db, _report = env
    rows = make_batch_rows("A", 1, 3, "2025-11-07")
    p1 = inbox / "a.csv"
    p2 = inbox / "b.csv"
    write_csv(p1, rows)
    write_csv(p2, list(reversed(rows)))
    f1 = ingest.compute_fingerprint(ingest.read_batch(p1))
    f2 = ingest.compute_fingerprint(ingest.read_batch(p2))
    assert f1 == f2

    changed = [
        r if i else (r[0], r[1], r[2], r[3], r[4], "1.00") for i, r in enumerate(rows)
    ]
    p3 = inbox / "c.csv"
    write_csv(p3, changed)
    f3 = ingest.compute_fingerprint(ingest.read_batch(p3))
    assert f3 != f1


def test_empty_inbox_still_writes_report(env):
    inbox, db, report = env
    assert ingest.run(inbox, db, report) == 0
    text = report.read_text(encoding="utf-8")
    assert "共 0 个，新增 0 行" in text
    assert "events 表累计行数: 0" in text
    assert db_rows(db) == []


def test_missing_inbox_returns_error(tmp_path):
    code = ingest.run(
        tmp_path / "nope",
        tmp_path / "db.sqlite",
        tmp_path / "report.txt",
    )
    assert code == 1


def test_pandas_not_used_for_summing(env, monkeypatch):
    """防御性约束：金额求和必须经 Decimal，而非 pandas 浮点聚合。"""
    assert ingest.sum_amount(pd.DataFrame({"amount_cny": ["0.10", "0.20"]})) == Decimal(
        "0.30"
    )
