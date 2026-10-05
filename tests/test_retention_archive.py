"""Archive before delete (phase 0 W3): rows leave the database only once on the NAS."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from bifrost_market_data.retention_archive import (
    RetentionArchive,
    _chunk_end,
    resolve_archive,
)


def test_archiving_is_off_unless_tables_are_named() -> None:
    assert resolve_archive({}) is None
    assert resolve_archive({"retention_archive": {"dir": "/archive"}}) is None
    assert resolve_archive({"retention_archive": {"tables": ["raw_market.stock_daily"]}}) is None


def test_archive_config_keeps_known_tables_and_requires_a_mount_by_default() -> None:
    a = resolve_archive(
        {"retention_archive": {"tables": ["raw_market.option_daily", "raw_market.nope"]}}
    )
    assert a is not None
    assert a.tables == {"raw_market.option_daily"}
    assert a.root == Path("/archive")
    assert a.require_mount is True


def test_a_directory_that_is_not_a_mount_is_refused(tmp_path: Path) -> None:
    """An empty directory in the container layer would take the files and lose them."""
    a = RetentionArchive(root=tmp_path, tables=frozenset({"raw_market.option_daily"}))
    assert "not a mount point" in (a.unavailable_reason() or "")
    missing = RetentionArchive(root=tmp_path / "nope", tables=a.tables, require_mount=False)
    assert "does not exist" in (missing.unavailable_reason() or "")
    ok = RetentionArchive(root=tmp_path, tables=a.tables, require_mount=False)
    assert ok.unavailable_reason() is None


def test_a_chunk_never_crosses_the_cutoff_or_a_month() -> None:
    assert _chunk_end(date(2024, 10, 1), date(2024, 11, 1), 31) == date(2024, 11, 1)
    assert _chunk_end(date(2024, 10, 20), date(2025, 3, 1), 31) == date(2024, 11, 1)
    assert _chunk_end(date(2024, 10, 1), date(2024, 10, 15), 31) == date(2024, 10, 15)
    assert _chunk_end(date(2026, 8, 5), date(2026, 9, 20), 3) == date(2026, 8, 8)
    assert _chunk_end(date(2026, 8, 30), date(2026, 9, 20), 3) == date(2026, 9, 1)


# ── Live: a real Postgres, a temporary directory standing in for the NAS ──

DSN = (os.environ.get("MARKET_DATA_ARCHIVE_TEST_DSN") or "").strip()
live = pytest.mark.skipif(
    not DSN, reason="Set MARKET_DATA_ARCHIVE_TEST_DSN to a scratch Postgres to run"
)


@pytest.fixture
def conn() -> Any:
    psycopg = pytest.importorskip("psycopg")
    pytest.importorskip("pyarrow")
    c = psycopg.connect(DSN)
    with c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS raw_market CASCADE")
        cur.execute("CREATE SCHEMA raw_market")
        cur.execute(
            """
            CREATE TABLE raw_market.option_daily (
                option_ticker text NOT NULL, underlying text NOT NULL,
                bar_date date NOT NULL, close double precision, volume bigint,
                vwap numeric(18, 6), PRIMARY KEY (option_ticker, bar_date)
            ) PARTITION BY RANGE (bar_date)
            """
        )
        cur.execute(
            "CREATE TABLE raw_market.option_daily_default "
            "PARTITION OF raw_market.option_daily DEFAULT"
        )
        cur.execute(
            """
            INSERT INTO raw_market.option_daily
            SELECT 'O:T' || i, 'T', d::date, i * 1.5, i, 1.123456
            FROM generate_series(date '2024-09-25', date '2024-11-05', interval '1 day') d,
                 generate_series(1, 7) i
            """
        )
        cur.execute(
            """
            CREATE TABLE raw_market.short_volume (
                symbol text NOT NULL, period_date date NOT NULL, period_type text NOT NULL,
                payload jsonb, PRIMARY KEY (symbol, period_date, period_type)
            )
            """
        )
        cur.execute(
            """
            INSERT INTO raw_market.short_volume
            SELECT 'S' || i, d::date, 'daily', jsonb_build_object('v', i)
            FROM generate_series(date '2024-09-28', date '2024-10-03', interval '1 day') d,
                 generate_series(1, 3) i
            """
        )
        cur.execute(
            """
            CREATE TABLE raw_market.option_snapshot (
                option_ticker text NOT NULL, underlying text NOT NULL,
                snapshot_ts timestamptz NOT NULL, iv double precision,
                open_interest integer, PRIMARY KEY (option_ticker, snapshot_ts)
            )
            """
        )
        # Two sessions: 14:30 and 20:00 UTC (10:30 intraday, 16:00 EOD in New York).
        cur.execute(
            """
            INSERT INTO raw_market.option_snapshot
            SELECT 'O:S' || i, 'S', ts, 0.2 + i / 100.0, i
            FROM unnest(ARRAY[
                timestamptz '2026-08-05 14:30Z', timestamptz '2026-08-05 20:00Z',
                timestamptz '2026-08-06 14:30Z', timestamptz '2026-08-06 20:00Z'
            ]) ts, generate_series(1, 5) i
            """
        )
    c.commit()
    yield c
    c.close()


def _count(conn: Any, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        n = int(cur.fetchone()[0])
    conn.commit()
    return n


def _days_since(d: date) -> int:
    return (datetime.now(UTC).date() - d).days


@live
def test_option_daily_rows_past_the_window_are_on_disk_before_they_leave(
    conn: Any, tmp_path: Path
) -> None:
    import pyarrow.parquet as pq

    from bifrost_market_data.retention_archive import archive_past_window

    archive = RetentionArchive(
        root=tmp_path, tables=frozenset({"raw_market.option_daily"}), require_mount=False
    )
    # A window whose month-floored cutoff is 2024-11-01: Sept and Oct 2024 expire.
    keep = _days_since(date(2024, 11, 10))
    before = _count(conn, "SELECT count(*) FROM raw_market.option_daily")
    expired = _count(
        conn, "SELECT count(*) FROM raw_market.option_daily WHERE bar_date < '2024-11-01'"
    )
    run = archive_past_window(conn, archive, "raw_market.option_daily", keep_days=keep)

    assert run.error is None, run.error
    assert run.cutoff == "2024-11-01"
    assert run.rows == expired == 37 * 7
    assert len(run.files) == 2, "one file per calendar month"
    left = _count(conn, "SELECT count(*) FROM raw_market.option_daily")
    assert left == before - expired
    assert (
        _count(conn, "SELECT count(*) FROM raw_market.option_daily WHERE bar_date < '2024-11-01'")
        == 0
    )

    total = 0
    for rel in run.files:
        path = tmp_path / rel
        table = pq.read_table(path)
        total += table.num_rows
        manifest = json.loads(
            path.with_name(path.name.removesuffix(".parquet") + ".json").read_text()
        )
        assert manifest["rows"] == table.num_rows
        assert manifest["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        assert sum(manifest["rows_per_day"].values()) == table.num_rows
        # numeric is kept as text, every digit.
        assert set(table.column("vwap").to_pylist()) == {"1.123456"}
        assert table.schema.field("bar_date").type.__str__() == "date32[day]"
    assert total == expired
    ledger = (tmp_path / "raw_market" / "_ledger.jsonl").read_text().splitlines()
    assert len(ledger) == 2

    again = archive_past_window(conn, archive, "raw_market.option_daily", keep_days=keep)
    assert again.rows == 0 and again.files == [], "nothing left past the window"


@live
def test_nothing_is_deleted_when_the_file_cannot_be_written(conn: Any, tmp_path: Path) -> None:
    from bifrost_market_data.retention_archive import archive_past_window

    root = tmp_path / "ro"
    (root / "raw_market").mkdir(parents=True)
    os.chmod(root / "raw_market", 0o500)
    try:
        archive = RetentionArchive(
            root=root, tables=frozenset({"raw_market.short_volume"}), require_mount=False
        )
        before = _count(conn, "SELECT count(*) FROM raw_market.short_volume")
        run = archive_past_window(
            conn, archive, "raw_market.short_volume", keep_days=_days_since(date(2024, 10, 10))
        )
        assert run.error is not None
        assert _count(conn, "SELECT count(*) FROM raw_market.short_volume") == before
        assert not list(root.rglob("*.partial"))
    finally:
        os.chmod(root / "raw_market", 0o700)


@live
def test_jsonb_is_archived_as_its_text(conn: Any, tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    from bifrost_market_data.retention_archive import archive_past_window

    archive = RetentionArchive(
        root=tmp_path, tables=frozenset({"raw_market.short_volume"}), require_mount=False
    )
    run = archive_past_window(
        conn, archive, "raw_market.short_volume", keep_days=_days_since(date(2024, 10, 10))
    )
    assert run.error is None and run.rows == 9, "the three September days"
    table = pq.read_table(tmp_path / run.files[0])
    assert json.loads(table.column("payload")[0].as_py()) in ({"v": 1}, {"v": 2}, {"v": 3})
    assert _count(conn, "SELECT count(*) FROM raw_market.short_volume") == 9


@live
def test_snapshot_intraday_goes_first_and_the_eod_anchor_stays(conn: Any, tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    from bifrost_market_data.retention_archive import INTRADAY_ONLY, archive_past_window

    archive = RetentionArchive(
        root=tmp_path, tables=frozenset({"raw_market.option_snapshot"}), require_mount=False
    )
    keep = _days_since(date(2026, 8, 7))
    run = archive_past_window(
        conn,
        archive,
        "raw_market.option_snapshot",
        keep_days=keep,
        kind="intraday",
        extra=f"AND {INTRADAY_ONLY}",
        month_floor=False,
        span_days=3,
    )
    assert run.error is None, run.error
    assert run.rows == 10
    left = _count(
        conn,
        "SELECT count(*) FROM raw_market.option_snapshot "
        "WHERE (snapshot_ts AT TIME ZONE 'America/New_York')::time = time '16:00'",
    )
    assert left == 10 == _count(conn, "SELECT count(*) FROM raw_market.option_snapshot")
    table = pq.read_table(tmp_path / run.files[0])
    assert str(table.schema.field("snapshot_ts").type) == "timestamp[us, tz=UTC]"
    assert "intraday" in run.files[0]

    run = archive_past_window(
        conn,
        archive,
        "raw_market.option_snapshot",
        keep_days=keep,
        kind="all",
        month_floor=False,
        span_days=3,
    )
    assert run.rows == 10
    assert _count(conn, "SELECT count(*) FROM raw_market.option_snapshot") == 0
