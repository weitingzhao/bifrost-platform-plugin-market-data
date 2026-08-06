"""Tests for scheduler daily slot enqueue."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data.scheduler.daily import (
    SLOT_NAMES,
    enqueue_slot,
    is_trading_day,
    resolve_target_date,
)
from bifrost_market_data.scheduler.enqueue import payload_hash


class _DailyCursor:
    def __init__(self, parent: _DailyConn) -> None:
        self.parent = parent
        self.rowcount = 0

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "us_trading_calendar" in q:
            d = params[0] if params else None
            if d in self.parent.calendar:
                self.parent._fetchone = (self.parent.calendar[d],)
            else:
                self.parent._fetchone = None
            # fetch_recent_trading_days uses SELECT cal_date ... fetchall
            if "cal_date" in q:
                days = sorted(self.parent.calendar.keys(), reverse=True)
                trading = [d for d in days if self.parent.calendar.get(d)]
                self.parent._fetchall = [(d,) for d in trading]
            else:
                self.parent._fetchall = []
        elif "option_snapshot" in q:
            # oi-gap-heal extract SELECT (may also JOIN option_contract)
            self.parent._fetchall = list(self.parent.extract_rows)
            self.parent._fetchone = None
        elif "from market.option_contract" in q:
            underlyings = set(params[0]) if params else set()
            as_of = params[1] if params and len(params) > 1 else None
            end = params[2] if params and len(params) > 2 else None
            max_per = int(params[3]) if params and len(params) > 3 else 40
            counts: dict[str, int] = {}
            rows: list[tuple[str]] = []
            for ticker, und, expiry in self.parent.option_contracts:
                if und not in underlyings:
                    continue
                if as_of is not None and expiry < as_of:
                    continue
                if end is not None and expiry > end:
                    continue
                n = counts.get(und, 0)
                if n >= max_per:
                    continue
                counts[und] = n + 1
                rows.append((ticker,))
            self.parent._fetchall = rows
            self.parent._fetchone = None
        elif "from watchlist" in q or "from public.watchlist" in q or "select distinct symbol" in q:
            self.parent._fetchall = [(s,) for s in self.parent.watchlist]
            self.parent._fetchone = None
        elif "returning id" in q:
            kind = params[0] if params else None
            ph = params[2] if params and len(params) > 2 else None
            key = (kind, ph)
            if key in self.parent.seen_keys:
                self.parent._fetchone = None
            else:
                self.parent.seen_keys.add(key)
                self.parent.next_id += 1
                self.parent._fetchone = (self.parent.next_id,)
        elif "delete from" in q:
            self.rowcount = 2
            self.parent._fetchone = None
        elif "stock_readiness_daily" in q:
            self.rowcount = 2
            self.parent._fetchone = None
        else:
            self.parent._fetchone = None
            self.parent._fetchall = []

    def fetchone(self) -> Any:
        return self.parent._fetchone

    def fetchall(self) -> list[Any]:
        return list(self.parent._fetchall)

    def executemany(self, query: str, params_seq: Any) -> None:
        self.parent.statements.append((query, list(params_seq)))
        self.parent.extract_inserts.extend(list(params_seq))

    def __enter__(self) -> _DailyCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _DailyConn:
    def __init__(
        self,
        watchlist: list[str] | None = None,
        calendar: dict[date, bool] | None = None,
        option_contracts: list[tuple[str, str, date]] | None = None,
        extract_rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.watchlist = watchlist or ["AAPL", "MSFT", "TSLA"]
        self.calendar = calendar or {}
        # (option_ticker, underlying, expiry)
        self.option_contracts = option_contracts or []
        # JOIN-shaped rows for oi-gap-heal extract
        self.extract_rows = extract_rows or []
        self.extract_inserts: list[tuple[Any, ...]] = []
        self.statements: list[tuple[str, Any]] = []
        self.seen_keys: set[tuple[Any, Any]] = set()
        self.next_id = 0
        self._fetchone: Any = None
        self._fetchall: list[Any] = []
        self.committed = 0

    def cursor(self) -> _DailyCursor:
        return _DailyCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        return None

def test_resolve_target_date_explicit() -> None:
    assert resolve_target_date("2024-06-20") == date(2024, 6, 20)
    assert resolve_target_date(date(2024, 1, 2)) == date(2024, 1, 2)


def test_is_trading_day_from_calendar() -> None:
    holiday = date(2024, 7, 4)
    conn = _DailyConn(calendar={holiday: False})
    assert is_trading_day(conn, holiday) is False
    assert is_trading_day(conn, date(2024, 7, 5)) is True  # missing → weekday fallback


def test_enqueue_stock_eod() -> None:
    conn = _DailyConn(["AAPL", "MSFT", "TSLA"])
    result = enqueue_slot(
        conn,
        "stock-eod",
        target_date=date(2024, 6, 20),
        scheduler_cfg={"slots": {"stock-eod": {"priority": 5}}},
    )
    assert result["enqueued"] == 3
    assert result["deduped"] == 0
    kinds = [j["kind"] for j in result["jobs"]]
    assert kinds == ["stock_daily", "stock_daily", "stock_daily"]
    assert all(j["payload"]["from"] == "2024-06-20" for j in result["jobs"])
    assert {j["payload"]["symbol"] for j in result["jobs"]} == {"AAPL", "MSFT", "TSLA"}


def test_enqueue_stock_eod_dedup() -> None:
    conn = _DailyConn(["AAPL"])
    cfg = {"slots": {"stock-eod": {"priority": 5}}, "watchlist_symbols": ["AAPL"]}
    r1 = enqueue_slot(conn, "stock-eod", target_date=date(2024, 6, 20), scheduler_cfg=cfg)
    r2 = enqueue_slot(conn, "stock-eod", target_date=date(2024, 6, 20), scheduler_cfg=cfg)
    assert r1["enqueued"] == 1
    assert r2["enqueued"] == 0
    assert r2["deduped"] == 1


def test_enqueue_eod_pipeline() -> None:
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "eod-pipeline",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"eod-pipeline": {"priority": 5}}},
    )
    kinds = [j["kind"] for j in result["jobs"]]
    assert kinds == ["option_snapshot", "option_open_interest"]
    assert result["jobs"][1]["payload"]["trade_date"] == "2024-06-20"


def test_enqueue_universe_daily() -> None:
    conn = _DailyConn([])
    result = enqueue_slot(
        conn,
        "universe-daily",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={"slots": {"universe-daily": {"priority": 3}}},
    )
    kinds = [j["kind"] for j in result["jobs"]]
    assert kinds == ["stock_daily_grouped"]
    assert "calendar" not in kinds
    stock = result["jobs"][0]
    assert stock["payload"]["from"] == "2024-06-20"
    assert stock["payload"]["market"] == "stocks"
    assert "mode" not in stock["payload"]


def test_enqueue_corporate() -> None:
    conn = _DailyConn(["MSFT"])
    result = enqueue_slot(
        conn,
        "corporate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["MSFT"],
        scheduler_cfg={"slots": {"corporate": {"priority": 2}}},
    )
    kinds = [j["kind"] for j in result["jobs"]]
    assert kinds == ["splits", "dividends"]


def test_enqueue_option_refresh_batch() -> None:
    conn = _DailyConn()
    symbols = ["AAPL", "MSFT", "TSLA", "NVDA"]
    result = enqueue_slot(
        conn,
        "option-refresh",
        target_date=date(2024, 6, 20),
        watchlist_symbols=symbols,
        scheduler_cfg={"slots": {"option-refresh": {"priority": 4, "batch_size": 2}}},
    )
    # 2 symbols × (contract + expiration)
    assert result["enqueued"] == 4
    underlyings = {j["payload"]["underlying"] for j in result["jobs"]}
    assert len(underlyings) == 2
    assert underlyings.issubset(set(symbols))


def test_enqueue_option_refresh_rotates_by_date() -> None:
    symbols = ["AAPL", "MSFT", "TSLA", "NVDA", "AMD", "META"]
    r1 = enqueue_slot(
        _DailyConn(),
        "option-refresh",
        target_date=date(2024, 6, 20),
        watchlist_symbols=symbols,
        scheduler_cfg={"slots": {"option-refresh": {"batch_size": 2}}},
    )
    r2 = enqueue_slot(
        _DailyConn(),
        "option-refresh",
        target_date=date(2024, 6, 21),
        watchlist_symbols=symbols,
        scheduler_cfg={"slots": {"option-refresh": {"batch_size": 2}}},
    )
    u1 = {j["payload"]["underlying"] for j in r1["jobs"]}
    u2 = {j["payload"]["underlying"] for j in r2["jobs"]}
    # Different dates should generally pick different batches (stable sha256 rotation).
    assert u1 != u2 or len(symbols) <= 2


def test_enqueue_option_bars() -> None:
    contracts = [
        ("O:AAPL240719C00200000", "AAPL", date(2024, 7, 19)),
        ("O:AAPL240719P00200000", "AAPL", date(2024, 7, 19)),
        ("O:MSFT240719C00400000", "MSFT", date(2024, 7, 19)),
    ]
    conn = _DailyConn(option_contracts=contracts)
    result = enqueue_slot(
        conn,
        "option-bars",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL", "MSFT"],
        scheduler_cfg={"slots": {"option-bars": {"priority": 4, "max_per_underlying": 40}}},
    )
    assert result["enqueued"] == 3
    assert all(j["kind"] == "option_daily" for j in result["jobs"])
    assert {j["payload"]["option_ticker"] for j in result["jobs"]} == {
        "O:AAPL240719C00200000",
        "O:AAPL240719P00200000",
        "O:MSFT240719C00400000",
    }
    assert all(j["payload"]["from"] == "2024-06-20" for j in result["jobs"])


def test_enqueue_minute_bars() -> None:
    contracts = [
        ("O:AAPL240719C00200000", "AAPL", date(2024, 7, 19)),
        ("O:AAPL240719P00200000", "AAPL", date(2024, 7, 19)),
    ]
    conn = _DailyConn(option_contracts=contracts)
    result = enqueue_slot(
        conn,
        "minute-bars",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={
            "slots": {
                "minute-bars": {
                    "priority": 3,
                    "batch_size": 80,
                    "max_per_underlying": 10,
                }
            }
        },
    )
    kinds = [j["kind"] for j in result["jobs"]]
    assert kinds.count("stock_minute") == 1
    assert kinds.count("option_minute") == 2
    stock = next(j for j in result["jobs"] if j["kind"] == "stock_minute")
    assert stock["payload"]["symbol"] == "AAPL"
    assert stock["payload"]["from"] == "2024-06-20"
    assert stock["payload"]["timespan"] == "minute"


def test_skip_non_trading_day() -> None:
    holiday = date(2024, 7, 4)  # Thursday holiday
    conn = _DailyConn(calendar={holiday: False})
    result = enqueue_slot(
        conn,
        "stock-eod",
        target_date=holiday,
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"stock-eod": {"priority": 5}}},
    )
    assert result.get("skipped") is True
    assert result["enqueued"] == 0
    assert result["jobs"] == []


def test_calendar_slot_not_skipped_on_holiday() -> None:
    holiday = date(2024, 7, 4)
    conn = _DailyConn(calendar={holiday: False})
    result = enqueue_slot(conn, "calendar", target_date=holiday, watchlist_symbols=[])
    assert result.get("skipped") is not True
    assert result["enqueued"] == 1


def test_enqueue_reference_ticker_sync() -> None:
    conn = _DailyConn([])
    result = enqueue_slot(
        conn,
        "reference",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={"slots": {"reference": {"priority": 2}}},
    )
    assert result["enqueued"] == 1
    assert result["jobs"][0]["kind"] == "ticker_sync"
    assert result["jobs"][0]["payload"] == {"mode": "universe"}


def test_reference_slot_not_skipped_on_holiday() -> None:
    holiday = date(2024, 7, 4)
    conn = _DailyConn(calendar={holiday: False})
    result = enqueue_slot(
        conn,
        "reference",
        target_date=holiday,
        watchlist_symbols=[],
        scheduler_cfg={"slots": {"reference": {"priority": 2}}},
    )
    assert result.get("skipped") is not True
    assert result["enqueued"] == 1
    assert result["jobs"][0]["kind"] == "ticker_sync"


def test_enqueue_fundamentals_rotate_batch() -> None:
    symbols = ["AAPL", "MSFT", "TSLA", "NVDA", "AMD"]
    result = enqueue_slot(
        _DailyConn(),
        "fundamentals-rotate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=symbols,
        scheduler_cfg={"slots": {"fundamentals-rotate": {"priority": 1, "batch_size": 2}}},
    )
    assert result["enqueued"] == 2
    assert all(j["kind"] == "financials" for j in result["jobs"])
    batch = {j["payload"]["symbol"] for j in result["jobs"]}
    assert len(batch) == 2
    assert batch.issubset(set(symbols))


def test_enqueue_fundamentals_rotate_by_date() -> None:
    symbols = ["AAPL", "MSFT", "TSLA", "NVDA", "AMD", "META"]
    r1 = enqueue_slot(
        _DailyConn(),
        "fundamentals-rotate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=symbols,
        scheduler_cfg={"slots": {"fundamentals-rotate": {"batch_size": 2}}},
    )
    r2 = enqueue_slot(
        _DailyConn(),
        "fundamentals-rotate",
        target_date=date(2024, 6, 21),
        watchlist_symbols=symbols,
        scheduler_cfg={"slots": {"fundamentals-rotate": {"batch_size": 2}}},
    )
    s1 = {j["payload"]["symbol"] for j in r1["jobs"]}
    s2 = {j["payload"]["symbol"] for j in r2["jobs"]}
    assert s1 != s2 or len(symbols) <= 2


def test_fundamentals_rotate_skipped_on_holiday() -> None:
    holiday = date(2024, 7, 4)
    conn = _DailyConn(calendar={holiday: False})
    result = enqueue_slot(
        conn,
        "fundamentals-rotate",
        target_date=holiday,
        watchlist_symbols=["AAPL", "MSFT"],
        scheduler_cfg={"slots": {"fundamentals-rotate": {"batch_size": 40}}},
    )
    assert result.get("skipped") is True
    assert result["enqueued"] == 0
    assert result["jobs"] == []


def test_enqueue_calendar_and_trim() -> None:
    conn = _DailyConn([])
    cal = enqueue_slot(conn, "calendar", watchlist_symbols=[], scheduler_cfg={})
    assert cal["enqueued"] == 1
    assert cal["jobs"][0]["kind"] == "calendar"

    trim = enqueue_slot(
        conn,
        "trim",
        scheduler_cfg={"slots": {"trim": {"keep_days": 7, "keep_max": 100}}},
    )
    assert trim["trimmed"] == 4  # two DELETEs × rowcount 2
    assert trim["enqueued"] == 0


def test_unknown_slot() -> None:
    with pytest.raises(ValueError, match="unknown slot"):
        enqueue_slot(_DailyConn(), "nope")


def test_all_slot_names_covered() -> None:
    assert "stock-eod" in SLOT_NAMES
    assert "option-bars" in SLOT_NAMES
    assert "minute-bars" in SLOT_NAMES
    assert "reference" in SLOT_NAMES
    assert "fundamentals-rotate" in SLOT_NAMES
    assert "readiness-refresh" in SLOT_NAMES
    assert "trim" in SLOT_NAMES
    assert "stock-snapshot" in SLOT_NAMES
    assert "stock-movers" in SLOT_NAMES
    assert "oi-gap-heal" in SLOT_NAMES
    # payload_hash stable for slot payloads
    assert payload_hash({"symbol": "AAPL"}) == payload_hash({"symbol": "AAPL"})


def test_enqueue_stock_snapshot_slot() -> None:
    conn = _DailyConn([])
    result = enqueue_slot(
        conn,
        "stock-snapshot",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={"slots": {"stock-snapshot": {"priority": 4}}},
    )
    assert result["enqueued"] == 1
    assert result["jobs"][0]["kind"] == "stock_snapshot"
    assert result["jobs"][0]["payload"] == {"mode": "all", "session_date": "2024-06-20"}


def test_enqueue_stock_movers_slot() -> None:
    conn = _DailyConn([])
    result = enqueue_slot(
        conn,
        "stock-movers",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={"slots": {"stock-movers": {"priority": 4}}},
    )
    assert result["enqueued"] == 1
    assert result["jobs"][0]["kind"] == "stock_movers"
    assert result["jobs"][0]["payload"] == {
        "direction": "both",
        "session_date": "2024-06-20",
    }


def test_stock_snapshot_skipped_on_holiday() -> None:
    holiday = date(2024, 7, 4)
    conn = _DailyConn(calendar={holiday: False})
    result = enqueue_slot(
        conn,
        "stock-snapshot",
        target_date=holiday,
        watchlist_symbols=[],
        scheduler_cfg={"slots": {"stock-snapshot": {"priority": 4}}},
    )
    assert result.get("skipped") is True
    assert result["enqueued"] == 0

def test_enqueue_readiness_refresh() -> None:
    conn = _DailyConn([])
    result = enqueue_slot(
        conn,
        "readiness-refresh",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={"slots": {"readiness-refresh": {"priority": 0}}},
    )
    assert result["slot"] == "readiness-refresh"
    assert result["rows_updated"] == 2  # _DailyCursor sets rowcount=2 for UPDATE
    assert result["enqueued"] == 0


def test_readiness_refresh_not_skipped_on_holiday() -> None:
    holiday = date(2024, 7, 4)
    conn = _DailyConn(calendar={holiday: False})
    result = enqueue_slot(
        conn,
        "readiness-refresh",
        target_date=holiday,
        watchlist_symbols=[],
        scheduler_cfg={"slots": {"readiness-refresh": {"priority": 0}}},
    )
    assert result.get("skipped") is not True
    assert result["rows_updated"] == 2


def test_readiness_refresh_commits() -> None:
    conn = _DailyConn([])
    enqueue_slot(
        conn,
        "readiness-refresh",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={},
    )
    assert conn.committed >= 1


def test_enqueue_oi_gap_heal() -> None:
    """D6=B: weekly slot runs extract inline (no Polygon jobs)."""
    conn = _DailyConn(
        watchlist=["AAPL"],
        calendar={
            date(2024, 6, 18): True,
            date(2024, 6, 19): True,
            date(2024, 6, 20): True,
        },
        extract_rows=[
            (
                "O:AAPL250620C00150000",
                "AAPL",
                100,
                date(2024, 6, 20),
                date(2025, 6, 20),
                150.0,
                "C",
                "AAPL",
            ),
        ],
    )
    result = enqueue_slot(
        conn,
        "oi-gap-heal",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"oi-gap-heal": {"lookback_days": 3}}},
    )
    assert result["slot"] == "oi-gap-heal"
    assert result["enqueued"] == 0
    assert result["candidates"] == 1
    assert result["from_date"] == "2024-06-18"
    assert result["to_date"] == "2024-06-20"
    assert any("DO NOTHING" in s[0] for s in conn.statements if "INSERT INTO" in s[0])


def test_oi_gap_heal_runs_on_weekend() -> None:
    """oi-gap-heal is not holiday-skipped (Saturday CronJob)."""
    saturday = date(2024, 6, 22)
    conn = _DailyConn(
        watchlist=["AAPL"],
        calendar={
            date(2024, 6, 18): True,
            date(2024, 6, 19): True,
            date(2024, 6, 20): True,
            saturday: False,
        },
        extract_rows=[],
    )
    result = enqueue_slot(
        conn,
        "oi-gap-heal",
        target_date=saturday,
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"oi-gap-heal": {"lookback_days": 3}}},
    )
    assert result.get("skipped") is not True or result.get("reason") == "no trading days"
    assert result["enqueued"] == 0
