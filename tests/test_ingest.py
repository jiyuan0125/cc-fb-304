"""ingest.py 的 pytest 测试。

覆盖需求点：
- 只接入新批次，已接入批次不重复处理（重名/改名重传、整目录重跑）；
- 晚到批次（batch_date 低于水位）仍能正确接入；
- event_id 跨批次碰撞不会被误判为重复，批次是去重单元；
- 单批事务性：写入失败整体回滚，不留半个批次；
- 对账报告内容完整；
- 与仓库自带的遗留库（无唯一约束）兼容；
- 数据校验拒绝脏文件。
"""

from __future__ import annotations

import shutil
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

import ingest

REPO_ROOT = Path(__file__).resolve().parent.parent
LEGACY_DB = REPO_ROOT / "data" / "warehouse.db"
SAMPLE_INBOX = REPO_ROOT / "inbox"

COLUMNS = [
    "event_id",
    "member_id",
    "event_time",
    "event_type",
    "amount_orig",
    "amount_cny",
    "batch_date",
]


def write_batch_csv(
    path: Path,
    batch_date: str,
    rows: int = 5,
    *,
    start_id: int = 1,
    amount: str = "100.00",
) -> None:
    """生成一个合法的批次 CSV。"""
    lines = [",".join(COLUMNS)]
    for i in range(rows):
        eid = f"E{start_id + i:06d}"
        lines.append(
            f"{eid},M100{i:03d},{batch_date} 10:00:0{i},click,{amount},{amount},{batch_date}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture()
def workspace(tmp_path: Path) -> dict[str, Path]:
    inbox = tmp_path / "inbox"
    data = tmp_path / "data"
    inbox.mkdir()
    data.mkdir()
    return {
        "root": tmp_path,
        "inbox": inbox,
        "db": data / "warehouse.db",
        "report": tmp_path / "report.txt",
    }


def run(workspace: dict[str, Path]):
    return ingest.run_ingest(
        workspace["inbox"], workspace["db"], workspace["report"]
    )


def connect(workspace: dict[str, Path]) -> sqlite3.Connection:
    return sqlite3.connect(workspace["db"])


# ---------------------------------------------------------------------------
# 基础接入
# ---------------------------------------------------------------------------

def test_first_run_ingests_all_new_batches(workspace):
    write_batch_csv(workspace["inbox"] / "batch_20251107.csv", "2025-11-07", rows=5)
    write_batch_csv(workspace["inbox"] / "batch_20251108.csv", "2025-11-08", rows=3)

    report = run(workspace)

    assert len(report.ingested) == 2
    assert report.ingested_rows == 8
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 8
        assert con.execute("SELECT COUNT(*) FROM ingested_batches").fetchone()[0] == 2


def test_second_run_is_idempotent(workspace):
    write_batch_csv(workspace["inbox"] / "batch_20251107.csv", "2025-11-07", rows=5)

    first = run(workspace)
    second = run(workspace)

    assert len(first.ingested) == 1
    assert len(second.ingested) == 0
    assert len(second.skipped) == 1
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 5
        # 登记表同批次只有一条
        assert (
            con.execute(
                "SELECT COUNT(*) FROM ingested_batches WHERE batch_date='2025-11-07'"
            ).fetchone()[0]
            == 1
        )


def test_renamed_redelivery_with_r1_suffix_is_skipped(workspace):
    """改名重传：原文件接入后，_r1 文件同批次必须跳过。"""
    write_batch_csv(workspace["inbox"] / "batch_20251109.csv", "2025-11-09", rows=4)
    run(workspace)

    shutil.copy(
        workspace["inbox"] / "batch_20251109.csv",
        workspace["inbox"] / "batch_20251109_r1.csv",
    )
    report = run(workspace)

    assert len(report.ingested) == 0
    # 原文件与改名重传文件都应被识别为重复
    assert {r.file_name for r in report.skipped} == {
        "batch_20251109.csv",
        "batch_20251109_r1.csv",
    }
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 4


def test_redelivery_present_on_first_run_only_one_ingested(workspace):
    """同一批次的原文件与重传文件同时存在时，只接入一个（按文件名排序先到者）。"""
    write_batch_csv(workspace["inbox"] / "batch_20251109.csv", "2025-11-09", rows=4)
    shutil.copy(
        workspace["inbox"] / "batch_20251109.csv",
        workspace["inbox"] / "batch_20251109_r1.csv",
    )

    report = run(workspace)

    assert len(report.ingested) == 1
    assert len(report.skipped) == 1
    assert report.ingested[0].file_name == "batch_20251109.csv"
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 4


def test_same_filename_redelivery_overwrites_then_rerun(workspace):
    """重名重传：同名文件被新内容覆盖后重跑，库中数据不翻倍。"""
    write_batch_csv(workspace["inbox"] / "batch_20251109.csv", "2025-11-09", rows=4)
    run(workspace)

    # 上游重投同名文件（内容行数不同也无所谓，批次已登记）
    write_batch_csv(
        workspace["inbox"] / "batch_20251109.csv",
        "2025-11-09",
        rows=7,
        start_id=100,
    )
    report = run(workspace)

    assert report.ingested == []
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 4


def test_late_batch_below_watermark_is_ingested(workspace):
    """晚到批次：先接入 11-05，再到达 11-04，后者不能被水位挡掉。"""
    write_batch_csv(workspace["inbox"] / "batch_20251105.csv", "2025-11-05", rows=3)
    run(workspace)

    write_batch_csv(
        workspace["inbox"] / "batch_20251104_late.csv",
        "2025-11-04",
        rows=2,
        start_id=50,
    )
    report = run(workspace)

    assert len(report.ingested) == 1
    assert report.ingested[0].batch_date == "2025-11-04"
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 5
        # 水位取最大 batch_date，仍是 11-05
        assert report.watermark == "2025-11-05"


def test_cross_batch_event_id_collision_both_ingested(workspace):
    """event_id 跨批次碰撞（同 id 不同内容）时，两个批次都应接入。

    样例数据中 E102375 同时出现在 11-08 和 11-10，证明 event_id 不能做去重键。
    """
    write_batch_csv(
        workspace["inbox"] / "batch_20251108.csv",
        "2025-11-08",
        rows=2,
        start_id=1,
    )
    write_batch_csv(
        workspace["inbox"] / "batch_20251110.csv",
        "2025-11-10",
        rows=2,
        start_id=1,  # 故意复用同样的 event_id
    )
    report = run(workspace)

    assert len(report.ingested) == 2
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 4
        assert con.execute(
            "SELECT COUNT(DISTINCT batch_date) FROM events"
        ).fetchone()[0] == 2


# ---------------------------------------------------------------------------
# 事务性
# ---------------------------------------------------------------------------

class FailingConnection:
    """包装真实 sqlite3 连接：批次登记 INSERT 时确定性抛异常，其余语句透传。

    用于验证事务回滚；Python 3.12 的内置 sqlite3 类型属性只读，
    无法 monkeypatch，因此用包装对象模拟故障。
    """

    def __init__(self, real: sqlite3.Connection):
        self._real = real

    def execute(self, sql, parameters=()):
        if sql.lstrip().upper().startswith("INSERT INTO INGESTED_BATCHES"):
            raise sqlite3.OperationalError("simulated registry failure")
        return self._real.execute(sql, parameters)

    def executemany(self, sql, parameters):
        return self._real.executemany(sql, parameters)

    def commit(self):
        return self._real.commit()

    def rollback(self):
        return self._real.rollback()


def test_failed_batch_rolls_back_completely(workspace):
    """批次登记写入失败时，已插入的明细必须随事务整体回滚。"""
    write_batch_csv(workspace["inbox"] / "batch_20251107.csv", "2025-11-07", rows=5)
    ingest.init_database(workspace["db"])

    with sqlite3.connect(workspace["db"]) as raw:
        failing = FailingConnection(raw)
        result = ingest.ingest_batch(
            failing,
            workspace["inbox"] / "batch_20251107.csv",
            already_ingested=set(),
        )

    assert result.status == "failed"
    assert "回滚" in result.reason
    with connect(workspace) as con:
        # 半个批次都不能留下
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM ingested_batches").fetchone()[0] == 0

    # 修复后重跑，同批次仍能正常接入（失败未污染水位登记）
    report = run(workspace)
    assert len(report.ingested) == 1
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 5


def test_one_bad_batch_does_not_block_others(workspace):
    """单个文件校验失败不影响同次运行的其他批次。"""
    write_batch_csv(workspace["inbox"] / "batch_20251107.csv", "2025-11-07", rows=2)
    bad = workspace["inbox"] / "batch_20251108.csv"
    bad.write_text(
        "event_id,member_id,event_time,event_type,amount_orig,amount_cny,batch_date\n"
        "E1,M1,2025-11-08 10:00:00,click,not_a_number,10.00,2025-11-08\n",
        encoding="utf-8",
    )
    write_batch_csv(workspace["inbox"] / "batch_20251109.csv", "2025-11-09", rows=3)

    report = run(workspace)

    assert {r.batch_date for r in report.ingested} == {"2025-11-07", "2025-11-09"}
    assert len(report.failed) == 1
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 5


# ---------------------------------------------------------------------------
# 报告
# ---------------------------------------------------------------------------

def test_report_contains_required_sections(workspace):
    write_batch_csv(
        workspace["inbox"] / "batch_20251107.csv",
        "2025-11-07",
        rows=2,
        amount="123.45",
    )
    run(workspace)

    text = workspace["report"].read_text(encoding="utf-8")
    assert "会员行为日志接入对账报告" in text
    assert "2025-11-07" in text
    assert "本次接入行数: 2" in text
    assert "本次接入金额合计(amount_cny): 246.90" in text
    assert "累计行数: 2" in text
    assert "累计金额合计(amount_cny): 246.90" in text
    assert "水位" in text


def test_report_amounts_use_decimal_not_float_artifact(workspace):
    """金额合计不能出现浮点尾巴（如 0.30000000000000004）。"""
    write_batch_csv(
        workspace["inbox"] / "batch_20251107.csv",
        "2025-11-07",
        rows=3,
        amount="0.10",
    )
    report = run(workspace)
    assert report.ingested_amount == Decimal("0.30")
    text = workspace["report"].read_text(encoding="utf-8")
    assert "0.30" in text
    assert "0.300000" not in text


def test_empty_inbox_produces_zero_report(workspace):
    report = run(workspace)
    assert report.ingested == []
    text = workspace["report"].read_text(encoding="utf-8")
    assert "本次接入行数: 0" in text
    assert "（无新批次）" in text


# ---------------------------------------------------------------------------
# 遗留库兼容
# ---------------------------------------------------------------------------

def test_works_with_legacy_warehouse_db(workspace):
    """仓库自带 warehouse.db（无唯一约束、含 5 个历史批次）必须可直接使用。"""
    shutil.copy(LEGACY_DB, workspace["db"])
    write_batch_csv(
        workspace["inbox"] / "batch_20251107.csv", "2025-11-07", rows=2
    )

    report = run(workspace)

    with connect(workspace) as con:
        # 历史 2005 行 + 新 2 行
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2007
        assert con.execute("SELECT COUNT(*) FROM ingested_batches").fetchone()[0] == 6
        # 唯一索引已补上
        indexes = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        assert "ux_ingested_batches_batch_date" in indexes
    assert report.watermark == "2025-11-07"
    assert report.cumulative_rows == 2007


def test_legacy_db_rerun_skips_history(workspace):
    shutil.copy(LEGACY_DB, workspace["db"])
    # 历史批次 11-01 重传，必须跳过
    write_batch_csv(
        workspace["inbox"] / "batch_20251101.csv", "2025-11-01", rows=9
    )
    report = run(workspace)

    assert report.ingested == []
    assert len(report.skipped) == 1
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2005


def test_unique_index_blocks_duplicate_registration(workspace):
    """数据库级兜底：绕过管道直接插重复 batch_date 会被唯一索引拒绝。"""
    write_batch_csv(workspace["inbox"] / "batch_20251107.csv", "2025-11-07", rows=1)
    run(workspace)

    with connect(workspace) as con, pytest.raises(sqlite3.IntegrityError):
        con.execute(
            "INSERT INTO ingested_batches "
            "(batch_file, batch_date, row_count, ingested_at) "
            "VALUES ('x.csv', '2025-11-07', 1, '2025-11-07 23:00:00')"
        )


# ---------------------------------------------------------------------------
# 样例数据端到端（8 个文件，含晚到与重传）
# ---------------------------------------------------------------------------

def test_end_to_end_with_real_sample_inbox(workspace):
    """用仓库 inbox 的 8 个真实文件跑完整场景（基于遗留库）。

    预期：
    - 新接入 6 个批次（11-07..11-12）；
    - 跳过 2 个：batch_20251109_r1.csv 是改名重传，
      batch_20251104_late.csv 的批次已在遗留库登记（重投，不是新批次）；
    - 净新增行数 = 365+439+416+480+485+406 = 2591。
    """
    shutil.copy(LEGACY_DB, workspace["db"])
    for csv_file in SAMPLE_INBOX.glob("*.csv"):
        shutil.copy(csv_file, workspace["inbox"] / csv_file.name)

    report = run(workspace)

    ingested_dates = {r.batch_date for r in report.ingested}
    assert ingested_dates == {
        "2025-11-07",
        "2025-11-08",
        "2025-11-09",
        "2025-11-10",
        "2025-11-11",
        "2025-11-12",
    }
    assert {r.file_name for r in report.skipped} == {
        "batch_20251104_late.csv",
        "batch_20251109_r1.csv",
    }
    assert report.ingested_rows == 2591

    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2005 + 2591
        assert con.execute("SELECT COUNT(*) FROM ingested_batches").fetchone()[0] == 11

    # 再跑一次：全部跳过，行数纹丝不动
    report2 = run(workspace)
    assert report2.ingested == []
    assert len(report2.skipped) == 8
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2005 + 2591


def test_late_batch_is_ingested_on_fresh_db(workspace):
    """晚到批次在空库（无该批次登记）场景下必须接入，不被文件名/水位挡住。

    与遗留库场景互补：真实 inbox 的 11-04 文件在遗留库下被跳过，
    这里验证同文件在全新库中会作为新批次接入。
    """
    shutil.copy(
        SAMPLE_INBOX / "batch_20251104_late.csv",
        workspace["inbox"] / "batch_20251104_late.csv",
    )
    report = run(workspace)

    assert len(report.ingested) == 1
    assert report.ingested[0].batch_date == "2025-11-04"
    assert report.ingested_rows == 180
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 180


# ---------------------------------------------------------------------------
# 数据校验
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("header", "data_line", "expected_fragment"),
    [
        (
            "event_id,member_id,event_time,event_type,amount_orig,amount_cny,batch_date",
            "E1,M1,2025-11-07 10:00:00,click,,10.00,2025-11-07",
            "空值",
        ),
        (
            "event_id,member_id,event_time,event_type,amount_orig,amount_cny,batch_date",
            "E1,M1,2025-11-07 10:00:00,click,10.00,-1.00,2025-11-07",
            "负数",
        ),
        (
            "event_id,member_id,event_time,event_type,amount_orig,amount_cny",
            "E1,M1,2025-11-07 10:00:00,click,10.00,10.00",
            "缺少必需列",
        ),
    ],
)
def test_invalid_files_are_rejected(workspace, header, data_line, expected_fragment):
    (workspace["inbox"] / "bad.csv").write_text(
        header + "\n" + data_line + "\n", encoding="utf-8"
    )
    report = run(workspace)

    assert len(report.failed) == 1
    assert expected_fragment in report.failed[0].reason
    with connect(workspace) as con:
        assert con.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM ingested_batches").fetchone()[0] == 0


def test_mixed_batch_dates_in_one_file_rejected(workspace):
    (workspace["inbox"] / "mixed.csv").write_text(
        ",".join(COLUMNS) + "\n"
        "E1,M1,2025-11-07 10:00:00,click,10.00,10.00,2025-11-07\n"
        "E2,M2,2025-11-08 10:00:00,click,10.00,10.00,2025-11-08\n",
        encoding="utf-8",
    )
    report = run(workspace)
    assert len(report.failed) == 1
    assert "batch_date 不唯一" in report.failed[0].reason


def test_duplicate_event_id_within_file_rejected(workspace):
    (workspace["inbox"] / "dup.csv").write_text(
        ",".join(COLUMNS) + "\n"
        "E1,M1,2025-11-07 10:00:00,click,10.00,10.00,2025-11-07\n"
        "E1,M2,2025-11-07 11:00:00,click,20.00,20.00,2025-11-07\n",
        encoding="utf-8",
    )
    report = run(workspace)
    assert len(report.failed) == 1
    assert "event_id 重复" in report.failed[0].reason


def test_missing_inbox_dir_raises(tmp_path):
    with pytest.raises(ingest.IngestError):
        ingest.run_ingest(
            tmp_path / "no_such_inbox",
            tmp_path / "db.sqlite",
            tmp_path / "report.txt",
        )
