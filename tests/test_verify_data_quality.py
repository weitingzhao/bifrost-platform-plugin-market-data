"""Tests for P7 data quality check helpers."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from bifrost_market_data.quality import (
    check_freshness,
    check_option_oi_coverage,
    check_option_snapshot_coverage,
    check_stock_daily_coverage,
    fetch_completed_trading_days,
    run_all_checks,
)
from bifrost_market_data.trading_calendar import iter_weekdays_excluding


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
        elif "us_market_holiday" in q:
            self._rows = [(d,) for d in self.parent.closed_holidays]
            self._one = None
        elif "from market.stock_daily" in q and "bar_date" in q:
            self._rows = list(self.parent.stock_daily_rows)
            self._one = None
        elif "from market.option_contract" in q:
            self._rows = [(u,) for u in self.parent.optionable_underlyings]
            self._one = None
        elif "from market.option_snapshot" in q:
            self._rows = [(u,) for u in self.parent.snapshot_underlyings]
            self._one = None
        elif "from market.option_open_interest" in q:
            self._rows = list(self.parent.oi_rows)
            self._one = None
        elif "from ops_jobs.ingest_freshness" in q:
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
        self.closed_holidays: list[date] = []
        # Default fixture window: three completed sessions ending 2024-06-20.
        self.fixture_as_of = date(2024, 6, 20)
        self.trading_days = fetch_completed_trading_days(
            self, 3, as_of=self.fixture_as_of
        )
        self.stock_daily_rows: list[tuple[str, date]] = [
            (sym, d) for sym in ("AAPL", "MSFT") for d in self.trading_days
        ]
        self.optionable_underlyings = ["AAPL", "MSFT"]
        self.snapshot_underlyings = ["AAPL", "MSFT"]
        self.oi_rows: list[tuple[str, date]] = [
            (sym, d) for sym in ("AAPL", "MSFT") for d in self.trading_days
        ]
        now = datetime.now(timezone.utc)
        self.freshness_rows = [
            ("stock_daily", now - timedelta(hours=1), 10, "ok", now),
            ("option_snapshot", now - timedelta(hours=2), 5, "ok", now),
            ("option_open_interest", now - timedelta(hours=2), 5, "ok", now),
            ("calendar", now - timedelta(hours=3), 1, "ok", now),
        ]

    def cursor(self) -> _QCur:
        return _QCur(self)


def test_iter_weekdays_skips_closed() -> None:
    days = iter_weekdays_excluding(
        end=date(2024, 7, 5),
        n=3,
        closed={date(2024, 7, 4)},
    )
    assert days == [date(2024, 7, 2), date(2024, 7, 3), date(2024, 7, 5)]


def test_stock_daily_coverage_pass() -> None:
    conn = _QConn()
    # Coverage uses live completed days — seed rows to match.
    live_days = fetch_completed_trading_days(conn, 3)
    conn.stock_daily_rows = [(sym, d) for sym in ("AAPL", "MSFT") for d in live_days]
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
    live_days = fetch_completed_trading_days(conn, 3)
    conn.stock_daily_rows = [
        ("AAPL", live_days[0]),
        ("AAPL", live_days[1]),
        # missing last AAPL day and all MSFT
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


def test_option_snapshot_skips_equity_only() -> None:
    """Symbols with zero option_contract rows must not fail snapshot acceptance."""
    conn = _QConn()
    conn.optionable_underlyings = ["AAPL"]
    conn.snapshot_underlyings = ["AAPL"]
    result = check_option_snapshot_coverage(
        conn,
        watchlist_symbols=["AAPL", "SATS"],
        as_of=date(2024, 6, 20),
    )
    assert result["ok"] is True
    assert result["skipped_non_optionable"] == 1
    assert result["missing_count"] == 0


def test_option_oi_coverage_pass() -> None:
    conn = _QConn()
    result = check_option_oi_coverage(
        conn,
        watchlist_symbols=["AAPL", "MSFT"],
        lookback_days=3,
        as_of=date(2024, 6, 20),
    )
    assert result["ok"] is True
    assert result["gap_count"] == 0


def test_option_oi_coverage_gaps() -> None:
    conn = _QConn()
    conn.oi_rows = [
        ("AAPL", date(2024, 6, 18)),
        ("AAPL", date(2024, 6, 19)),
        # missing AAPL 2024-06-20 and all MSFT
    ]
    result = check_option_oi_coverage(
        conn,
        watchlist_symbols=["AAPL", "MSFT"],
        lookback_days=3,
        as_of=date(2024, 6, 20),
    )
    assert result["ok"] is False
    assert result["gap_count"] > 0


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
    live_days = fetch_completed_trading_days(conn, 3)
    conn.stock_daily_rows = [(sym, d) for sym in ("AAPL", "MSFT") for d in live_days]
    conn.oi_rows = [(sym, d) for sym in ("AAPL", "MSFT") for d in live_days]
    report = run_all_checks(
        conn,
        watchlist_symbols=["AAPL", "MSFT"],
        lookback_days=3,
        min_symbols=4000,
    )
    assert report["ok"] is True
    assert report["summary"] == "PASS"
    assert len(report["checks"]) == 4
    assert {c["check"] for c in report["checks"]} == {
        "stock_daily_coverage",
        "option_snapshot_coverage",
        "option_oi_coverage",
        "freshness",
    }
