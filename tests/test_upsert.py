"""Unit tests for ingest/_upsert helpers."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    daily_snapshot_anchor,
    epoch_ms_to_date,
    epoch_ms_to_datetime,
    epoch_ns_to_datetime,
    parse_date,
    parse_option_right,
    parse_option_ticker,
    period_label,
)


def test_epoch_ms_to_date_and_datetime() -> None:
    # 2024-01-02 00:00:00 UTC
    ms = 1_704_153_600_000
    assert epoch_ms_to_date(ms) == date(2024, 1, 2)
    dt = epoch_ms_to_datetime(ms)
    assert dt.tzinfo is not None
    assert dt == datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)


def test_epoch_ns_to_datetime() -> None:
    ns = 1_704_153_600_000_000_000
    assert epoch_ns_to_datetime(ns) == datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)


def test_parse_option_ticker() -> None:
    parsed = parse_option_ticker("O:AAPL250620C00150000")
    assert parsed["option_ticker"] == "O:AAPL250620C00150000"
    assert parsed["underlying"] == "AAPL"
    assert parsed["expiry"] == date(2025, 6, 20)
    assert parsed["strike"] == 150.0
    assert parsed["option_right"] == "C"

    put = parse_option_ticker("o:spy241220p00450000")
    assert put["underlying"] == "SPY"
    assert put["option_right"] == "P"
    assert put["strike"] == 450.0


def test_parse_option_ticker_invalid() -> None:
    with pytest.raises(ValueError):
        parse_option_ticker("AAPL")
    with pytest.raises(ValueError):
        parse_option_ticker("O:BAD")


def test_parse_option_right() -> None:
    assert parse_option_right("call") == "C"
    assert parse_option_right("PUT") == "P"
    with pytest.raises(ValueError):
        parse_option_right("x")


def test_period_label_and_parsers() -> None:
    assert period_label(5, "Minute") == "5 minute"
    assert parse_date("2024-06-20") == date(2024, 6, 20)
    assert parse_date(None) is None
    assert as_float("1.5") == 1.5
    assert as_float(None) is None
    assert as_int("10") == 10
    assert as_int("") is None


def test_batch_upsert_empty() -> None:
    conn = MagicMock()
    assert batch_upsert(conn, "market.stock_daily", ["a"], [], conflict_keys=["a"]) == 0
    conn.cursor.assert_not_called()


def test_batch_upsert_executemany() -> None:
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None

    n = batch_upsert(
        conn,
        "market.stock_daily",
        ("symbol", "bar_date", "close"),
        [("AAPL", date(2024, 1, 2), 100.0)],
        conflict_keys=("symbol", "bar_date"),
        update_cols=("close",),
        set_fetched_at=True,
    )
    assert n == 1
    sql = cur.executemany.call_args[0][0]
    assert "INSERT INTO market.stock_daily" in sql
    assert "ON CONFLICT (symbol, bar_date)" in sql
    assert "fetched_at = now()" in sql
    conn.commit.assert_called_once()


def test_batch_upsert_do_nothing_when_no_updates() -> None:
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None

    batch_upsert(
        conn,
        "market.option_expiration",
        ("underlying", "expiry"),
        [("AAPL", date(2025, 6, 20))],
        conflict_keys=("underlying", "expiry"),
        update_cols=(),
        set_fetched_at=False,
    )
    sql = cur.executemany.call_args[0][0]
    assert "DO NOTHING" in sql


def test_batch_upsert_auto_commit_false() -> None:
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None

    n = batch_upsert(
        conn,
        "market.stock_daily",
        ("symbol", "bar_date", "close"),
        [("AAPL", date(2024, 1, 2), 100.0)],
        conflict_keys=("symbol", "bar_date"),
        update_cols=("close",),
        set_fetched_at=False,
        auto_commit=False,
    )
    assert n == 1
    conn.commit.assert_not_called()
    cur.executemany.assert_called_once()


def test_daily_snapshot_anchor_ny() -> None:
    from zoneinfo import ZoneInfo

    ny = ZoneInfo("America/New_York")
    # Late evening UTC still previous NY calendar day
    anchor = daily_snapshot_anchor(datetime(2024, 6, 20, 3, 0, tzinfo=timezone.utc))
    assert anchor.astimezone(ny).date() == date(2024, 6, 19)
    assert anchor.astimezone(ny).hour == 16
