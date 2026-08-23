"""Tests for scheduler daily slot enqueue."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data.scheduler.daily import (
    DEFAULT_IV_RADAR_BENCHMARKS,
    MIGRATED_ANALYTICS_SLOTS,
    SLOT_NAMES,
    enqueue_slot,
    is_trading_day,
    option_trades_universe,
    resolve_target_date,
    union_iv_radar_benchmarks,
)
from bifrost_market_data.scheduler.enqueue import payload_hash


class _DailyCursor:
    def __init__(self, parent: _DailyConn) -> None:
        self.parent = parent
        self.rowcount = 0

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "us_market_holiday" in q:
            # is_trading_day: SELECT 1 ... holiday_date = %s AND status = 'closed'
            if "holiday_date = %s" in q or "holiday_date=%s" in q:
                d = params[0] if params else None
                # calendar[d] False → closed holiday present
                if d in self.parent.calendar and self.parent.calendar[d] is False:
                    self.parent._fetchone = (1,)
                else:
                    self.parent._fetchone = None
                self.parent._fetchall = []
            else:
                # fetch_closed_holiday_dates → list of closed dates
                closed = [d for d, trading in self.parent.calendar.items() if not trading]
                self.parent._fetchall = [(d,) for d in closed]
                self.parent._fetchone = None
        elif "v_option_snapshot_with_stock" in q:
            trade_date = params[0] if params else None
            underlyings = set(params[1]) if params and len(params) > 1 else None
            rows = []
            for r in self.parent.atm_snap_rows:
                if r.get("trade_date") != trade_date:
                    continue
                if underlyings is not None and r.get("underlying") not in underlyings:
                    continue
                rows.append(
                    (
                        r["option_ticker"],
                        r["underlying"],
                        r["iv"],
                        r["underlying_price"],
                        r["expiry"],
                        r["strike"],
                        r["option_right"],
                    )
                )
            self.parent._fetchall = rows
            self.parent._fetchone = None
        elif "from market.option_snapshot" in q and "day_volume" in q:
            trade_date = params[0] if params else None
            underlyings = set(params[1]) if params and len(params) > 1 else None
            rows = []
            for r in self.parent.vol_rows:
                if r.get("trade_date") != trade_date:
                    continue
                if underlyings is not None and r.get("underlying") not in underlyings:
                    continue
                rows.append((r["underlying"], r["option_right"], r["day_volume"]))
            self.parent._fetchall = rows
            self.parent._fetchone = None
        elif "option_snapshot" in q:
            # oi-gap-heal extract SELECT (may also JOIN option_contract)
            self.parent._fetchall = list(self.parent.extract_rows)
            self.parent._fetchone = None
        elif "from market.option_open_interest" in q:
            trade_date = params[0] if params else None
            underlyings = None
            if params and len(params) > 1:
                underlyings = set(params[1])
            # PCR path: SUM(...) GROUP BY underlying, option_right
            if "sum(open_interest)" in q:
                buckets: dict[tuple[str, str], int] = {}
                for r in self.parent.oi_rows:
                    if r.get("trade_date") != trade_date:
                        continue
                    if underlyings is not None and r.get("underlying") not in underlyings:
                        continue
                    key = (r["underlying"], r["option_right"])
                    buckets[key] = buckets.get(key, 0) + int(r.get("open_interest") or 0)
                self.parent._fetchall = [
                    (und, right, total) for (und, right), total in sorted(buckets.items())
                ]
            else:
                rows = []
                for r in self.parent.oi_rows:
                    if r.get("trade_date") != trade_date:
                        continue
                    if underlyings is not None and r.get("underlying") not in underlyings:
                        continue
                    rows.append(
                        (
                            r["underlying"],
                            r["expiry"],
                            r["strike"],
                            r["option_right"],
                            r["open_interest"],
                        )
                    )
                self.parent._fetchall = rows
            self.parent._fetchone = None
        elif "from features_daily.atm_iv_daily" in q:
            from_d = params[0] if params else None
            to_d = params[1] if params and len(params) > 1 else None
            underlyings = set(params[2]) if params and len(params) > 2 else None
            rows = []
            for r in self.parent.atm_iv_hist:
                td = r["trade_date"]
                if from_d is not None and td < from_d:
                    continue
                if to_d is not None and td > to_d:
                    continue
                if underlyings is not None and r["symbol"] not in underlyings:
                    continue
                rows.append((r["symbol"], r["trade_date"], r["expiry"], r["atm_iv"]))
            self.parent._fetchall = rows
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
        elif "from market.ticker" in q and "instrument_type" in q:
            self.parent._fetchall = [(s,) for s in self.parent.cs_universe]
            self.parent._fetchone = None
        elif "from market.stock_financials" in q and "income_statement" in q:
            self.parent._fetchall = [(s,) for s in self.parent.income_covered]
            self.parent._fetchone = None
        elif "from watchlist" in q or "from public.watchlist" in q or "select distinct symbol" in q:
            if self.parent.raise_on_watchlist:
                raise RuntimeError('relation "public.watchlist" does not exist')
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
            if self.parent.raise_on_readiness:
                raise RuntimeError('relation "public.stock_readiness_daily" does not exist')
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
        oi_rows: list[dict[str, Any]] | None = None,
        atm_snap_rows: list[dict[str, Any]] | None = None,
        vol_rows: list[dict[str, Any]] | None = None,
        atm_iv_hist: list[dict[str, Any]] | None = None,
        raise_on_watchlist: bool = False,
        raise_on_readiness: bool = False,
        cs_universe: list[str] | None = None,
        income_covered: list[str] | None = None,
    ) -> None:
        self.watchlist = watchlist or ["AAPL", "MSFT", "TSLA"]
        self.cs_universe = cs_universe or []
        self.income_covered = income_covered or []
        self.raise_on_watchlist = raise_on_watchlist
        self.raise_on_readiness = raise_on_readiness
        self.calendar = calendar or {}
        # (option_ticker, underlying, expiry)
        self.option_contracts = option_contracts or []
        # JOIN-shaped rows for oi-gap-heal extract
        self.extract_rows = extract_rows or []
        self.oi_rows = oi_rows or []
        self.atm_snap_rows = atm_snap_rows or []
        self.vol_rows = vol_rows or []
        self.atm_iv_hist = atm_iv_hist or []
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
    # early-close stored as trading=True is ignored; only closed holidays matter
    early = date(2024, 7, 3)
    conn2 = _DailyConn(calendar={early: True})
    assert is_trading_day(conn2, early) is True
    assert is_trading_day(conn2, date(2024, 7, 6)) is False  # Saturday


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
    assert kinds.count("option_snapshot") == 4  # AAPL ∪ SPY/QQQ/IWM
    assert kinds.count("option_open_interest") == 4
    assert {j["payload"]["underlying"] for j in result["jobs"]} == {
        "AAPL",
        "SPY",
        "QQQ",
        "IWM",
    }
    oi = next(j for j in result["jobs"] if j["kind"] == "option_open_interest")
    assert oi["payload"]["trade_date"] == "2024-06-20"


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
    # 3 benchmarks always + 2 rotated watchlist × (contract + expiration)
    assert result["enqueued"] == 10
    underlyings = {j["payload"]["underlying"] for j in result["jobs"]}
    assert {"SPY", "QQQ", "IWM"}.issubset(underlyings)
    assert len(underlyings) == 5
    assert underlyings - {"SPY", "QQQ", "IWM"} <= set(symbols)


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


def test_option_trades_universe_always_includes_spx() -> None:
    # Truncate to 50 while always keeping SPX even if alphabetically late.
    many = [f"SYM{i:03d}" for i in range(60)]
    out = option_trades_universe(many, limit=50)
    assert len(out) == 50
    assert "SPX" in out
    assert out == sorted(out)


def test_enqueue_option_trades() -> None:
    contracts = [
        ("O:AAPL240719C00200000", "AAPL", date(2024, 7, 19)),
        ("O:SPX240719C05000000", "SPX", date(2024, 7, 19)),
        ("O:MSFT240719C00400000", "MSFT", date(2024, 7, 19)),
    ]
    conn = _DailyConn(option_contracts=contracts)
    result = enqueue_slot(
        conn,
        "option-trades",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL", "MSFT"],
        scheduler_cfg={
            "slots": {
                "option-trades": {
                    "priority": 3,
                    "max_per_underlying": 40,
                    "universe_limit": 50,
                }
            }
        },
    )
    assert result["enqueued"] == 3
    assert all(j["kind"] == "option_trades" for j in result["jobs"])
    assert {j["payload"]["option_ticker"] for j in result["jobs"]} == {
        "O:AAPL240719C00200000",
        "O:SPX240719C05000000",
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
    assert kinds.count("stock_minute") == 3
    assert kinds.count("option_minute") == 2
    stock_jobs = [j for j in result["jobs"] if j["kind"] == "stock_minute"]
    assert {j["payload"]["symbol"] for j in stock_jobs} == {"AAPL"}
    assert {j["payload"]["from"] for j in stock_jobs} == {"2024-06-20"}
    stock_timespans = {(j["payload"]["multiplier"], j["payload"]["timespan"]) for j in stock_jobs}
    assert stock_timespans == {(1, "minute"), (5, "minute"), (1, "hour")}


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


def test_enqueue_fundamentals_rotate_cs_missing_first() -> None:
    conn = _DailyConn(
        cs_universe=["AAA", "BBB", "CCC", "DDD"],
        income_covered=["AAA", "BBB"],
    )
    result = enqueue_slot(
        conn,
        "fundamentals-rotate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["WATCH"],
        scheduler_cfg={
            "slots": {
                "fundamentals-rotate": {
                    "priority": 1,
                    "batch_size": 2,
                    "universe": "cs",
                    "prioritize_missing": True,
                    "include_ratios": False,
                    "include_short_interest": False,
                    "include_short_volume": False,
                }
            },
            "iv_radar_benchmarks": [],
        },
    )
    assert result["enqueued"] == 2
    batch = [j["payload"]["symbol"] for j in result["jobs"]]
    assert set(batch) <= {"CCC", "DDD"}
    assert all(j["kind"] == "financials" for j in result["jobs"])


def test_enqueue_fundamentals_rotate_force_on_holiday() -> None:
    holiday = date(2024, 7, 4)
    conn = _DailyConn(calendar={holiday: False})
    skipped = enqueue_slot(
        conn,
        "fundamentals-rotate",
        target_date=holiday,
        watchlist_symbols=["AAPL"],
        scheduler_cfg={
            "slots": {"fundamentals-rotate": {"batch_size": 1}},
            "iv_radar_benchmarks": [],
        },
    )
    assert skipped.get("skipped") is True
    forced = enqueue_slot(
        conn,
        "fundamentals-rotate",
        target_date=holiday,
        watchlist_symbols=["AAPL"],
        scheduler_cfg={
            "slots": {
                "fundamentals-rotate": {
                    "batch_size": 1,
                    "include_ratios": False,
                    "include_short_interest": False,
                    "include_short_volume": False,
                }
            },
            "iv_radar_benchmarks": [],
        },
        force=True,
    )
    assert forced.get("skipped") is not True
    assert forced["enqueued"] == 1


def test_enqueue_fundamentals_rotate_batch() -> None:
    # Universe = watchlist ∪ iv_radar_benchmarks. Each batched symbol enqueues
    # 4 SEPA-supporting kinds: financials, ratios, short_interest, short_volume.
    symbols = ["AAPL", "MSFT", "TSLA", "NVDA", "AMD"]
    result = enqueue_slot(
        _DailyConn(),
        "fundamentals-rotate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=symbols,
        scheduler_cfg={
            "slots": {"fundamentals-rotate": {"priority": 1, "batch_size": 2}},
            # Pin benchmarks so the union is deterministic in the test.
            "iv_radar_benchmarks": ["SPY", "QQQ", "IWM"],
        },
    )
    # 2 symbols × 4 kinds = 8 jobs
    assert result["enqueued"] == 8
    kinds = {j["kind"] for j in result["jobs"]}
    assert kinds == {"financials", "ratios", "short_interest", "short_volume"}
    batch = {j["payload"]["symbol"] for j in result["jobs"]}
    assert len(batch) == 2
    expected_universe = set(symbols) | {"SPY", "QQQ", "IWM"}
    assert batch.issubset(expected_universe)


def test_enqueue_fundamentals_rotate_toggle_kinds() -> None:
    # include_ratios/short_interest/short_volume flags let ops fine-tune what
    # each rotation batch enqueues (e.g. disable short-interest during backfill).
    result = enqueue_slot(
        _DailyConn(),
        "fundamentals-rotate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={
            "slots": {
                "fundamentals-rotate": {
                    "priority": 1,
                    "batch_size": 1,
                    "include_ratios": False,
                    "include_short_interest": False,
                    "include_short_volume": True,
                }
            },
            "iv_radar_benchmarks": [],
        },
    )
    assert result["enqueued"] == 2  # financials + short_volume only
    kinds = {j["kind"] for j in result["jobs"]}
    assert kinds == {"financials", "short_volume"}


def test_enqueue_related_rotate_batch() -> None:
    symbols = ["AAPL", "MSFT", "TSLA", "NVDA", "AMD"]
    result = enqueue_slot(
        _DailyConn(),
        "related-rotate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=symbols,
        scheduler_cfg={"slots": {"related-rotate": {"priority": 1, "batch_size": 2}}},
    )
    assert result["enqueued"] == 2
    assert all(j["kind"] == "ticker_related" for j in result["jobs"])
    batch = {j["payload"]["symbol"] for j in result["jobs"]}
    assert len(batch) == 2
    assert batch.issubset(set(symbols))


def test_enqueue_related_rotate_skipped_on_holiday() -> None:
    holiday = date(2024, 7, 4)
    result = enqueue_slot(
        _DailyConn(calendar={holiday: False}),
        "related-rotate",
        target_date=holiday,
        watchlist_symbols=["AAPL", "MSFT"],
        scheduler_cfg={"slots": {"related-rotate": {"batch_size": 2}}},
    )
    assert result.get("skipped") is True
    assert result["enqueued"] == 0


def test_enqueue_fundamentals_rotate_by_date() -> None:
    symbols = ["AAPL", "MSFT", "TSLA", "NVDA", "AMD", "META"]
    cfg_base = {
        "slots": {"fundamentals-rotate": {"batch_size": 2}},
        "iv_radar_benchmarks": [],
    }
    r1 = enqueue_slot(
        _DailyConn(),
        "fundamentals-rotate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=symbols,
        scheduler_cfg=cfg_base,
    )
    r2 = enqueue_slot(
        _DailyConn(),
        "fundamentals-rotate",
        target_date=date(2024, 6, 21),
        watchlist_symbols=symbols,
        scheduler_cfg=cfg_base,
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
    assert "option-trades" in SLOT_NAMES
    assert "minute-bars" in SLOT_NAMES
    assert "reference" in SLOT_NAMES
    assert "fundamentals-rotate" in SLOT_NAMES
    assert "related-rotate" in SLOT_NAMES
    assert "readiness-refresh" in SLOT_NAMES
    assert "trim" in SLOT_NAMES
    assert "stock-snapshot" in SLOT_NAMES
    assert "stock-movers" in SLOT_NAMES
    assert "oi-gap-heal" in SLOT_NAMES
    assert "max-pain" not in SLOT_NAMES
    assert "atm-iv-pcr" not in SLOT_NAMES
    assert "iv-percentile" in MIGRATED_ANALYTICS_SLOTS
    assert MIGRATED_ANALYTICS_SLOTS == frozenset(
        {"max-pain", "atm-iv-pcr", "iv-percentile"}
    )
    # payload_hash stable for slot payloads
    assert payload_hash({"symbol": "AAPL"}) == payload_hash({"symbol": "AAPL"})


def test_union_iv_radar_benchmarks() -> None:
    merged = union_iv_radar_benchmarks(["AAPL"])
    assert merged == sorted({"AAPL", *DEFAULT_IV_RADAR_BENCHMARKS})
    custom = union_iv_radar_benchmarks(
        ["NVDA"],
        {"iv_radar_benchmarks": ["SPY"]},
    )
    assert custom == ["NVDA", "SPY"]


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


def test_watchlist_db_fallback_missing_table_returns_empty() -> None:
    from bifrost_market_data.scheduler.daily import load_watchlist_symbols

    conn = _DailyConn(raise_on_watchlist=True)
    symbols = load_watchlist_symbols(conn, {"watchlist_source": "db"})
    assert symbols == []


def test_resolve_watchlist_option_contract_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """When platform-api/DB watchlist is empty, fall back to option underlyings."""
    from bifrost_market_data.scheduler import daily as daily_mod

    monkeypatch.setattr(
        daily_mod,
        "load_watchlist_symbols",
        lambda _conn, _cfg: [],
    )

    class _Conn:
        def cursor(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, q, params=None):
            self._q = q

        def fetchall(self):
            return [("SPY",), ("QQQ",)]

        def rollback(self):
            pass

    symbols, source = daily_mod.resolve_watchlist_with_source(_Conn(), limit=10)
    assert source == "option_contract_underlyings"
    assert symbols == ["QQQ", "SPY"]


def test_resolve_watchlist_prefers_loaded_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.scheduler import daily as daily_mod

    monkeypatch.setattr(
        daily_mod,
        "load_watchlist_symbols",
        lambda _conn, _cfg: ["MU", "TSLA"],
    )
    symbols, source = daily_mod.resolve_watchlist_with_source(object(), limit=10)
    assert source == "watchlist"
    assert symbols == ["MU", "TSLA"]


def test_readiness_refresh_missing_table_skips() -> None:
    conn = _DailyConn(raise_on_readiness=True)
    result = enqueue_slot(
        conn,
        "readiness-refresh",
        target_date=date(2024, 6, 20),
        scheduler_cfg={},
    )
    assert result["slot"] == "readiness-refresh"
    assert result["rows_updated"] == 0
    assert result["enqueued"] == 0


def test_reference_and_universe_skip_watchlist_lookup() -> None:
    """ticker_sync / grouped EOD must succeed even if public.watchlist is gone."""
    conn = _DailyConn(raise_on_watchlist=True)
    ref = enqueue_slot(conn, "reference", target_date=date(2024, 6, 20), scheduler_cfg={})
    assert ref["enqueued"] == 1
    assert ref["jobs"][0]["kind"] == "ticker_sync"
    uni = enqueue_slot(conn, "universe-daily", target_date=date(2024, 6, 20), scheduler_cfg={})
    assert uni["enqueued"] == 1
    assert uni["jobs"][0]["kind"] == "stock_daily_grouped"


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


def test_migrated_analytics_slots_rejected() -> None:
    """Wave 2.1: max-pain / atm-iv-pcr / iv-percentile moved to Research."""
    conn = _DailyConn([])
    for slot in ("max-pain", "atm-iv-pcr", "iv-percentile"):
        with pytest.raises(ValueError, match="moved to bifrost_research"):
            enqueue_slot(conn, slot, target_date=date(2024, 6, 20))
