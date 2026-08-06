"""Tests for scripts/backfill date chunking and enqueue."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data.scheduler.backfill import date_chunks, enqueue_backfill


class _BFCursor:
    def __init__(self, parent: _BFConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        if "returning id" in query.lower():
            kind = params[0] if params else None
            ph = params[2] if params and len(params) > 2 else None
            key = (kind, ph)
            if key in self.parent.seen_keys:
                self.parent._fetchone = None
            else:
                self.parent.seen_keys.add(key)
                self.parent.next_id += 1
                self.parent._fetchone = (self.parent.next_id,)
        else:
            self.parent._fetchone = None

    def fetchone(self) -> Any:
        return self.parent._fetchone

    def __enter__(self) -> _BFCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _BFConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.seen_keys: set[tuple[Any, Any]] = set()
        self.next_id = 0
        self._fetchone: Any = None

    def cursor(self) -> _BFCursor:
        return _BFCursor(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_date_chunks_basic() -> None:
    chunks = date_chunks(date(2024, 1, 1), date(2024, 1, 10), 5)
    assert chunks == [
        (date(2024, 1, 1), date(2024, 1, 5)),
        (date(2024, 1, 6), date(2024, 1, 10)),
    ]


def test_date_chunks_single() -> None:
    chunks = date_chunks(date(2024, 6, 20), date(2024, 6, 20), 365)
    assert chunks == [(date(2024, 6, 20), date(2024, 6, 20))]


def test_date_chunks_invalid() -> None:
    with pytest.raises(ValueError):
        date_chunks(date(2024, 6, 21), date(2024, 6, 20), 1)
    with pytest.raises(ValueError):
        date_chunks(date(2024, 1, 1), date(2024, 1, 2), 0)


def test_enqueue_backfill_stock_daily() -> None:
    conn = _BFConn()
    result = enqueue_backfill(
        conn,
        kind="stock_daily",
        symbols=["AAPL", "MSFT"],
        from_date=date(2023, 1, 1),
        to_date=date(2024, 12, 31),
        chunk_days=365,
    )
    # 2 symbols × N chunks spanning 2023-01-01 .. 2024-12-31
    assert result["chunks"] >= 2
    assert result["enqueued"] == 2 * result["chunks"]
    assert result["deduped"] == 0
    assert all(j["kind"] == "stock_daily" for j in result["jobs"])


def test_enqueue_backfill_dedup() -> None:
    conn = _BFConn()
    kwargs = dict(
        kind="stock_daily",
        symbols=["AAPL"],
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 31),
        chunk_days=365,
    )
    r1 = enqueue_backfill(conn, **kwargs)
    r2 = enqueue_backfill(conn, **kwargs)
    assert r1["enqueued"] == 1
    assert r2["enqueued"] == 0
    assert r2["deduped"] == 1


def test_enqueue_backfill_option_daily() -> None:
    conn = _BFConn()
    result = enqueue_backfill(
        conn,
        kind="option_daily",
        symbols=["O:AAPL250620C00150000"],
        from_date=date(2024, 1, 1),
        to_date=date(2024, 3, 31),
        chunk_days=90,
    )
    assert result["enqueued"] >= 1
    assert result["jobs"][0]["payload"]["option_ticker"] == "O:AAPL250620C00150000"


def test_enqueue_backfill_stock_daily_grouped() -> None:
    conn = _BFConn()
    # 2024-06-20 Thu .. 2024-06-23 Sun → weekdays Thu+Fri only (2 of 4 calendar days)
    result = enqueue_backfill(
        conn,
        kind="stock_daily_grouped",
        from_date=date(2024, 6, 20),
        to_date=date(2024, 6, 23),
    )
    assert result["chunks"] == 2
    assert result["enqueued"] == 2
    assert result["chunks"] < 4  # weekends skipped
    assert result["jobs"][0]["payload"]["from"] == "2024-06-20"
    assert result["jobs"][1]["payload"]["from"] == "2024-06-21"
    assert result["jobs"][0]["payload"]["market"] == "stocks"


def test_enqueue_backfill_financials() -> None:
    conn = _BFConn()
    result = enqueue_backfill(
        conn,
        kind="financials",
        symbols=["AAPL", "MSFT"],
    )
    assert result["enqueued"] == 2
    assert result["chunks"] == 0
    assert result["jobs"][0]["payload"] == {"symbol": "AAPL"}


def test_enqueue_backfill_option_snapshot() -> None:
    conn = _BFConn()
    result = enqueue_backfill(conn, kind="option_snapshot", symbols=["AAPL"])
    assert result["enqueued"] == 1
    assert result["jobs"][0]["payload"] == {"underlying": "AAPL"}


def test_enqueue_backfill_option_contract() -> None:
    conn = _BFConn()
    result = enqueue_backfill(conn, kind="option_contract", symbols=["AAPL"])
    assert result["enqueued"] == 1
    assert result["jobs"][0]["payload"]["underlying"] == "AAPL"
    assert result["jobs"][0]["payload"]["expired"] is False


def test_enqueue_backfill_option_open_interest() -> None:
    """API backfill enqueues current-snapshot OI jobs (not historical extract)."""
    conn = _BFConn()
    result = enqueue_backfill(conn, kind="option_open_interest", symbols=["AAPL", "MSFT"])
    assert result["enqueued"] == 2
    assert result["chunks"] == 0
    assert result["jobs"][0]["payload"] == {"underlying": "AAPL"}
    assert result["jobs"][1]["payload"] == {"underlying": "MSFT"}


def test_enqueue_backfill_option_open_interest_with_trade_date() -> None:
    conn = _BFConn()
    result = enqueue_backfill(
        conn,
        kind="option_open_interest",
        symbols=["AAPL"],
        from_date=date(2024, 6, 20),
    )
    assert result["enqueued"] == 1
    assert result["jobs"][0]["payload"] == {
        "underlying": "AAPL",
        "trade_date": "2024-06-20",
    }


def test_enqueue_backfill_ticker_sync() -> None:
    conn = _BFConn()
    result = enqueue_backfill(conn, kind="ticker_sync")
    assert result["enqueued"] == 1
    assert result["jobs"][0]["payload"] == {}


def test_enqueue_backfill_stock_minute() -> None:
    conn = _BFConn()
    result = enqueue_backfill(
        conn,
        kind="stock_minute",
        symbols=["AAPL"],
        from_date=date(2024, 6, 1),
        to_date=date(2024, 6, 30),
        chunk_days=7,
    )
    assert result["enqueued"] == result["chunks"]
    assert result["chunks"] >= 4
    assert result["jobs"][0]["payload"]["symbol"] == "AAPL"
    assert result["jobs"][0]["payload"]["timespan"] == "minute"
    assert result["jobs"][0]["payload"]["multiplier"] == 1


def test_enqueue_backfill_stock_minute_default_chunk_30() -> None:
    """Minute kinds default chunk_days=30 (not 365)."""
    conn = _BFConn()
    # 90 calendar days → 3 chunks of 30
    result = enqueue_backfill(
        conn,
        kind="stock_minute",
        symbols=["AAPL"],
        from_date=date(2024, 1, 1),
        to_date=date(2024, 3, 30),
    )
    assert result["chunks"] == 3


def test_enqueue_backfill_option_minute() -> None:
    conn = _BFConn()
    result = enqueue_backfill(
        conn,
        kind="option_minute",
        symbols=["O:AAPL250620C00150000"],
        from_date=date(2024, 6, 1),
        to_date=date(2024, 6, 7),
        chunk_days=7,
    )
    assert result["enqueued"] >= 1
    assert result["jobs"][0]["payload"]["option_ticker"] == "O:AAPL250620C00150000"
    assert result["jobs"][0]["payload"]["timespan"] == "minute"


def test_enqueue_backfill_unsupported() -> None:
    with pytest.raises(ValueError, match="unsupported kind"):
        enqueue_backfill(_BFConn(), kind="not_a_real_kind", symbols=["AAPL"])
