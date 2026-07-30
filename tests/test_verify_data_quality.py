"""Tests for P7 data quality check helpers."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from bifrost_market_data.quality import (
    check_freshness,
    check_option_snapshot_coverage,
    check_stock_daily_coverage,
    run_all_checks,
)


class _QCur:
    def __init__(self, parent: _QConn) -> None:
        self.parent = parent
        self._rows: list[Any] = []
        self._one: Any = None

    def execute(self, query: str, params: Any = None) -> None:
        q = query.lower()
        self.parent.statements.append((query, params))
        if "count(distinct symbol)" in q:
            self._one = (self.parent.symbol_count,)
            self._rows = []
        elif "us_trading_calendar" in q:
            self._rows = [(d,) for d in self.parent.trading_days]
            self._one = None
        elif "from market.stock_daily" in q and "bar_date" in q:
            self._rows = list(self.parent.stock_daily_rows)
            self._one = None
        elif "from market.option_snapshot" in q:
            self._rows = [(u,) for u in self.parent.snapshot_underlyings]
            self._one = None
        elif "from data_ops.ingest_freshness" in q:
            self._rows = list(self.parent.freshness_rows)
            self._one = None
        elif "from public.watchlist" in q or "watchlist" in q:
            self._rows = [(s,) for s in self.parent.watchlist]
            self._one = None
        else:
            self._rows = []
            self._one = None

    def fetchone(self) -> Any:
        return self._one

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def __enter__(self) -> _QCur:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _QConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.symbol_count = 4500
        self.watchlist = ["AAPL", "MSFT"]
        self.trading_days = [
            date(2024, 6, 18),
            date(2024, 6, 19),
            date(2024, 6, 20),
        ]
        self.stock_daily_rows: list[tuple[str, date]] = [
            ("AAPL", date(2024, 6, 18)),
            ("AAPL", date(2024, 6, 19)),
            ("AAPL", date(2024, 6, 20)),
            ("MSFT", date(2024, 6, 18)),
            ("MSFT", date(2024, 6, 19)),
            ("MSFT", date(2024, 6, 20)),
        ]
        self.snapshot_underlyings = ["AAPL", "MSFT"]
        now = datetime.now(timezone.utc)
        self.freshness_rows = [
            ("stock_daily", now - timedelta(hours=1), 10, "ok", now),
            ("option_snapshot", now - timedelta(hours=2), 5, "ok", now),
            ("option_open_interest", now - timedelta(hours=2), 5, "ok", now),
            ("calendar", now - timedelta(hours=3), 1, "ok", now),
        ]

    def cursor(self) -> _QCur:
        return _QCur(self)


def test_stock_daily_coverage_pass() -> None:
    conn = _QConn()
    result = check_stock_daily_coverage(
        conn,
        watchlist_symbols=["AAPL", "MSFT"],
        lookback_days=3,
        min_symbols=4000,
    )
    assert result["ok"] is True
    assert result["gap_count"] == 0


def test_stock_daily_coverage_gaps() -> None:
    conn = _QConn()
    conn.stock_daily_rows = [
        ("AAPL", date(2024, 6, 18)),
        ("AAPL", date(2024, 6, 19)),
        # missing AAPL 2024-06-20 and all MSFT
    ]
    result = check_stock_daily_coverage(
        conn,
        watchlist_symbols=["AAPL", "MSFT"],
        lookback_days=3,
        min_symbols=4000,
    )
    assert result["ok"] is False
    assert result["gap_count"] > 0


def test_stock_daily_coverage_low_symbols() -> None:
    conn = _QConn()
    conn.symbol_count = 100
    result = check_stock_daily_coverage(
        conn,
        watchlist_symbols=["AAPL"],
        lookback_days=1,
        min_symbols=4000,
    )
    assert result["ok"] is False


def test_option_snapshot_coverage_pass() -> None:
    conn = _QConn()
    result = check_option_snapshot_coverage(
        conn,
        watchlist_symbols=["AAPL", "MSFT"],
        as_of=date(2024, 6, 20),
    )
    assert result["ok"] is True
    assert result["missing_count"] == 0


def test_option_snapshot_coverage_missing() -> None:
    conn = _QConn()
    conn.snapshot_underlyings = ["AAPL"]
    result = check_option_snapshot_coverage(
        conn,
        watchlist_symbols=["AAPL", "MSFT"],
        as_of=date(2024, 6, 20),
    )
    assert result["ok"] is False
    assert "MSFT" in result["missing_sample"]


def test_freshness_pass() -> None:
    conn = _QConn()
    result = check_freshness(conn)
    assert result["ok"] is True


def test_freshness_stale() -> None:
    conn = _QConn()
    old = datetime.now(timezone.utc) - timedelta(hours=48)
    conn.freshness_rows = [
        ("stock_daily", old, 10, "ok", old),
        ("option_snapshot", old, 5, "ok", old),
        ("option_open_interest", old, 5, "ok", old),
        ("calendar", old, 1, "ok", old),
    ]
    result = check_freshness(conn, max_age_hours=24)
    assert result["ok"] is False
    assert len(result["failures"]) > 0


def test_run_all_checks() -> None:
    conn = _QConn()
    report = run_all_checks(
        conn,
        watchlist_symbols=["AAPL", "MSFT"],
        lookback_days=3,
        min_symbols=4000,
    )
    assert report["ok"] is True
    assert report["summary"] == "PASS"
    assert len(report["checks"]) == 3
