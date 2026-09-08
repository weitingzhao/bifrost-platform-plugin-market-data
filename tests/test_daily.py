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
        elif "from features.option_metric_atm_iv_daily" in q:
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
        elif "/* near-spot */" in q:
            # (syms, as_of, syms, as_of, n_exp, per_right)
            syms = set(params[0])
            as_of = params[1]
            n_exp = int(params[4])
            per_right = int(params[5])
            picked: list[tuple[str]] = []
            for und in sorted(syms):
                spot = self.parent.spots.get(und)
                if spot is None:
                    continue
                mine = [c for c in self.parent.option_contracts if c[1] == und and c[2] >= as_of]
                expiries = sorted({c[2] for c in mine})[:n_exp]
                for exp in expiries:
                    for right in ("C", "P"):
                        cands = [c for c in mine if c[2] == exp and c[0][-9] == right]
                        cands.sort(key=lambda c: (abs((c[3] if len(c) > 3 else 0) - spot), c[3] if len(c) > 3 else 0))
                        picked.extend((c[0],) for c in cands[:per_right])
            self.parent._fetchall = sorted(picked)
            self.parent._fetchone = None
        elif "from research.option_universe" in q:
            self.parent._fetchall = list(self.parent.research_universe)
            self.parent._fetchone = None
        elif "group by upper(trim(underlying))" in q:
            # _option_contract_underlyings: every underlying with any contract row.
            seen = sorted({und for _t, und, *_r in self.parent.option_contracts})
            self.parent._fetchall = [(u,) for u in seen]
            self.parent._fetchone = None
        elif "from market.option_contract" in q or "from raw_market.option_contract" in q:
            underlyings = set(params[0]) if params else set()
            as_of = params[1] if params and len(params) > 1 else None
            end = params[2] if params and len(params) > 2 else None
            max_per = int(params[3]) if params and len(params) > 3 else 40
            counts: dict[str, int] = {}
            rows: list[tuple[str]] = []
            for ticker, und, expiry, *_rest in self.parent.option_contracts:
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
        elif ("from market.ticker" in q or "from raw_market.ticker" in q) and "instrument_type" in q:
            self.parent._fetchall = [(s,) for s in self.parent.cs_universe]
            self.parent._fetchone = None
        elif "raw_market.income_statement" in q or (
            "stock_financials" in q and "income_statement" in q
        ):
            self.parent._fetchall = [(s,) for s in self.parent.income_covered]
            self.parent._fetchone = None
        elif "from watchlist" in q or "from public.watchlist" in q or "select distinct symbol" in q:
            if self.parent.raise_on_watchlist:
                raise RuntimeError('relation "public.watchlist" does not exist')
            self.parent._fetchall = [(s,) for s in self.parent.watchlist]
            self.parent._fetchone = None
        elif "returning id" in q and "unnest(" not in q:
            kind = params[0] if params else None
            ph = params[2] if params and len(params) > 2 else None
            key = (kind, ph)
            if key in self.parent.seen_keys:
                self.parent._fetchone = None
            else:
                self.parent.seen_keys.add(key)
                self.parent.next_id += 1
                self.parent._fetchone = (self.parent.next_id,)
        elif "unnest(" in q and "insert into ops_jobs.job_ingest" in q:
            # bulk enqueue: (kinds[], payloads[], hashes[], priorities[], max_attempts[])
            rows = []
            for kind, ph in zip(params[0], params[2]):
                key = (kind, ph)
                if key in self.parent.seen_keys:
                    continue
                self.parent.seen_keys.add(key)
                self.parent.next_id += 1
                rows.append((self.parent.next_id, kind, ph))
            self.parent._fetchall = rows
            self.parent._fetchone = None
        elif "symbol_source_void" in q:
            if q.lstrip().startswith("select"):
                self.parent._fetchall = [(sym,) for sym in self.parent.voided]
            self.parent._fetchone = None
        elif "watchlist_cache" in q:
            if q.lstrip().startswith("select"):
                self.parent._fetchall = [(sym,) for sym in self.parent.watchlist_cache]
            self.parent._fetchone = None
        elif "from ops_jobs.job_ingest" in q and "payload ->>" in q:
            # session-once evidence probe: keyed by the session, not a time window
            self.parent.evidence_probes.append(params)
            self.parent._fetchone = (1,) if self.parent.session_evidence else None
            self.parent._fetchall = []
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
        session_evidence: bool = False,
        spots: dict[str, float] | None = None,
        voided: list[str] | None = None,
        watchlist_cache: list[str] | None = None,
        research_universe: list[tuple[str, str, int]] | None = None,
    ) -> None:
        self.session_evidence = session_evidence
        self.evidence_probes: list[Any] = []
        self.spots = spots or {}
        self.voided = voided or []
        self.watchlist_cache = watchlist_cache or []
        self.research_universe = research_universe or []
        self.watchlist = watchlist or ["AAPL", "MSFT", "TSLA"]
        self.cs_universe = cs_universe or []
        self.income_covered = income_covered or []
        self.raise_on_watchlist = raise_on_watchlist
        self.raise_on_readiness = raise_on_readiness
        self.calendar = calendar or {}
        # (option_ticker, underlying, expiry[, strike])
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
    # One snapshot per underlying (AAPL ∪ SPY/QQQ/IWM); OI comes out of the
    # same download, so no option_open_interest job and no I:SPX spot job.
    assert kinds == ["option_snapshot"] * 4
    assert {j["payload"]["underlying"] for j in result["jobs"]} == {
        "AAPL",
        "SPY",
        "QQQ",
        "IWM",
    }
    assert all(j["payload"]["trade_date"] == "2024-06-20" for j in result["jobs"])


def test_eod_pipeline_skipped_when_fire_date_is_weekend() -> None:
    """A Saturday cron fire rolls the target back to Friday; it must still skip."""
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "eod-pipeline",
        target_date=date(2024, 6, 21),  # Friday (rolled back)
        fire_date=date(2024, 6, 22),  # Saturday (when the cron fired)
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"eod-pipeline": {"priority": 5}}},
    )
    assert result["skipped"] is True
    assert result["reason"] == "non_trading_day"
    assert result["jobs"] == []
    # An explicit date (no fire_date) still runs Friday: that is the catch-up path.
    result2 = enqueue_slot(
        _DailyConn(["AAPL"]),
        "eod-pipeline",
        target_date=date(2024, 6, 21),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"eod-pipeline": {"priority": 5}}},
    )
    assert result2["enqueued"] == 4


def test_eod_pipeline_session_once_dedup() -> None:
    """The 22:30 ET catch-up must not re-fetch what the 22:00 UTC fire already enqueued."""
    conn = _DailyConn(["AAPL"], session_evidence=True)
    result = enqueue_slot(
        conn,
        "eod-pipeline",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"eod-pipeline": {"priority": 5}}},
    )
    assert result["skipped"] is True
    assert result["reason"] == "evidence_exists"
    # The probe asks about this session's date, not "anything in the last 12h".
    assert conn.evidence_probes == [(["option_snapshot"], "trade_date", "2024-06-20")]
    forced = enqueue_slot(
        _DailyConn(["AAPL"], session_evidence=True),
        "eod-pipeline",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"eod-pipeline": {"priority": 5}}},
        force=True,
    )
    assert forced["enqueued"] == 4
    # option-refresh is not session-once: it legitimately fires every 6 hours.
    refresh = enqueue_slot(
        _DailyConn(["AAPL"], session_evidence=True),
        "option-refresh",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"option-refresh": {"batch_size": 2}}},
    )
    assert refresh["enqueued"] > 0


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
    # Whole market by ex-date window — two jobs, not two per symbol.
    assert kinds == ["splits_market", "dividends_market"]
    assert result["jobs"][0]["payload"] == {"from": "2024-06-13", "to": "2024-08-19"}


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
    # 3 benchmarks always + 2 rotated watchlist; expirations come out of the
    # contract page walk, so one job per underlying.
    assert result["enqueued"] == 5
    assert all(j["kind"] == "option_contract" for j in result["jobs"])
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


def test_enqueue_option_bars_targets_the_money() -> None:
    """Next expiries, strikes nearest the close — not the lowest strikes of the nearest expiry."""
    exp1, exp2, exp3, exp4 = date(2024, 6, 21), date(2024, 6, 28), date(2024, 7, 5), date(2024, 7, 12)
    contracts = []
    for exp in (exp1, exp2, exp3, exp4):
        for strike in (50, 100, 150, 190, 200, 210, 250, 300):
            ymd = exp.strftime("%y%m%d")
            contracts.append((f"O:AAPL{ymd}C{int(strike * 1000):08d}", "AAPL", exp, float(strike)))
            contracts.append((f"O:AAPL{ymd}P{int(strike * 1000):08d}", "AAPL", exp, float(strike)))
    conn = _DailyConn(option_contracts=contracts, spots={"AAPL": 201.0})
    result = enqueue_slot(
        conn,
        "option-bars",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={
            "iv_radar_benchmarks": [],
            "slots": {"option-bars": {"priority": 4, "expiries": 2, "strikes_each_side": 1}},
        },
    )
    tickers = sorted(j["payload"]["option_ticker"] for j in result["jobs"])
    # 2 expiries × 2 rights × 3 strikes (190/200/210) = 12; nothing from exp3/exp4, nothing at 50 or 300.
    assert len(tickers) == 12
    assert all(t[6:12] in ("240621", "240628") for t in tickers)
    assert {t[-8:] for t in tickers} == {"00190000", "00200000", "00210000"}
    assert all(j["kind"] == "option_daily" and j["payload"]["from"] == "2024-06-20" for j in result["jobs"])


def test_enqueue_option_bars_skips_underlyings_without_a_close() -> None:
    contracts = [("O:SPX240621C05000000", "SPX", date(2024, 6, 21), 5000.0)]
    conn = _DailyConn(option_contracts=contracts, spots={})  # no index level on this plan
    result = enqueue_slot(
        conn,
        "option-bars",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["SPX"],
        scheduler_cfg={"iv_radar_benchmarks": [], "slots": {"option-bars": {}}},
    )
    assert result["enqueued"] == 0


def test_option_trades_universe_always_includes_spx() -> None:
    # Truncate to 50 while always keeping SPX even if alphabetically late.
    many = [f"SYM{i:03d}" for i in range(60)]
    out = option_trades_universe(many, limit=50)
    assert len(out) == 50
    assert "SPX" in out
    assert out == sorted(out)


def test_option_trades_slot_retired() -> None:
    """Options Starter has no trades entitlement: the slot is a no-op, not a 400."""
    assert "option-trades" in SLOT_NAMES
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "option-trades",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"option-trades": {"priority": 3}}},
    )
    assert result["skipped"] is True
    assert result["reason"] == "unentitled"
    assert result["enqueued"] == 0
    assert not any("returning id" in st[0].lower() for st in conn.statements)

def test_enqueue_minute_bars() -> None:
    contracts = [
        ("O:AAPL240719C00200000", "AAPL", date(2024, 7, 19), 200.0),
        ("O:AAPL240719P00200000", "AAPL", date(2024, 7, 19), 200.0),
    ]
    conn = _DailyConn(option_contracts=contracts, spots={"AAPL": 199.5})
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
    assert "oi-gap-heal" not in SLOT_NAMES  # retired: OI comes from the chain snapshot
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
    assert result.get("skipped") is True
    assert result.get("reason") == "retired"
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
    # Retired slot skips for reason=retired, not holiday gate.
    assert result.get("skipped") is True
    assert result.get("reason") == "retired"
    assert result["enqueued"] == 0


def test_readiness_refresh_commits() -> None:
    conn = _DailyConn([])
    enqueue_slot(
        conn,
        "readiness-refresh",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={},
    )
    assert conn.committed == 0  # retired slot — no SQL executed


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
    """Retired slot never touches stock_readiness_daily (even if mock would raise)."""
    conn = _DailyConn(raise_on_readiness=True)
    result = enqueue_slot(
        conn,
        "readiness-refresh",
        target_date=date(2024, 6, 20),
        scheduler_cfg={},
    )
    assert result["slot"] == "readiness-refresh"
    assert result.get("skipped") is True
    assert result.get("reason") == "retired"
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


def test_migrated_analytics_slots_rejected() -> None:
    """Wave 2.1: max-pain / atm-iv-pcr / iv-percentile moved to Research."""
    conn = _DailyConn([])
    for slot in ("max-pain", "atm-iv-pcr", "iv-percentile"):
        with pytest.raises(ValueError, match="moved to bifrost_research"):
            enqueue_slot(conn, slot, target_date=date(2024, 6, 20))


def test_enqueue_fundamentals_market() -> None:
    """Whole-market ratios + short data for the last completed session, three jobs."""
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "fundamentals-market",
        target_date=date(2024, 6, 21),  # Friday
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"fundamentals-market": {"priority": 2, "short_interest_lookback_days": 45}}},
    )
    by_kind = {j["kind"]: j["payload"] for j in result["jobs"]}
    assert set(by_kind) == {"ratios_market", "short_volume_market", "short_interest_market"}
    assert by_kind["ratios_market"] == {"date": "2024-06-21"}
    assert by_kind["short_volume_market"] == {"date": "2024-06-21"}
    assert by_kind["short_interest_market"] == {"settlement_date_gte": "2024-05-07"}
    # Not holiday-gated: it fires the morning after a session, which may be a Saturday.
    weekend = enqueue_slot(
        _DailyConn(["AAPL"]),
        "fundamentals-market",
        target_date=date(2024, 6, 21),
        fire_date=date(2024, 6, 22),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"fundamentals-market": {}}},
    )
    assert weekend["enqueued"] == 3


def test_fundamentals_rotate_skips_vendor_voids() -> None:
    """A name the vendor answered nothing for waits a month instead of leading the queue daily."""
    conn = _DailyConn(
        cs_universe=["AAA", "BBB", "CCC", "DDD"],
        income_covered=["AAA"],
        voided=["CCC"],
    )
    result = enqueue_slot(
        conn,
        "fundamentals-rotate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={
            "iv_radar_benchmarks": [],
            "slots": {
                "fundamentals-rotate": {
                    "batch_size": 2,
                    "universe": "cs",
                    "prioritize_missing": True,
                    "include_ratios": False,
                    "include_short_interest": False,
                    "include_short_volume": False,
                }
            },
        },
    )
    batch = {j["payload"]["symbol"] for j in result["jobs"]}
    assert batch == {"BBB", "DDD"}  # the two missing names that are not voids


def test_watchlist_falls_back_to_cache_not_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.scheduler import daily as mod

    cfg = {"watchlist_source": "platform-api", "platform_api_url": "http://platform.test"}
    # Reachable: the union is returned and cached.
    monkeypatch.setattr(mod, "load_watchlist_from_platform", lambda url, **kw: ["NVDA", "TSLA"])
    conn = _DailyConn(watchlist=["AAPL"])
    assert mod.load_watchlist_symbols(conn, cfg) == ["NVDA", "TSLA"]
    assert any("watchlist_cache" in st[0].lower() and "insert" in st[0].lower() for st in conn.statements)
    # Unreachable: the cached union wins over the (Trade-owned, usually absent) DB query.
    monkeypatch.setattr(mod, "load_watchlist_from_platform", lambda url, **kw: None)
    conn = _DailyConn(watchlist=["AAPL"], watchlist_cache=["NVDA", "TSLA"])
    assert mod.load_watchlist_symbols(conn, cfg) == ["NVDA", "TSLA"]
    # Unreachable and no cache: the old DB fallback still applies.
    conn = _DailyConn(watchlist=["AAPL"])
    assert mod.load_watchlist_symbols(conn, cfg) == ["AAPL"]


def test_trim_counts_snapshot_retention_in_sessions() -> None:
    """90 sessions is a longer window than 90 days; holidays must not shorten it."""
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "trim",
        target_date=date(2024, 6, 20),
        scheduler_cfg={"slots": {"trim": {"option_snapshot_keep_sessions": 5}}},
    )
    assert result["option_snapshot_keep_sessions"] == 5
    # 5 sessions back from Thursday 2024-06-20 is Friday 2024-06-14 → 6 days.
    assert result["option_snapshot_keep_days"] == 6
    drops = [st for st in conn.statements if "drop_month_partitions_older_than" in st[0]]
    assert any("option_snapshot" in st[0] and st[1] == (6,) for st in drops)


def test_enqueue_intraday_chain_marks_rows_as_intraday() -> None:
    """Intraday jobs carry their own instant so they sit beside the EOD row."""
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "intraday-chain",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"intraday-chain": {"priority": 3}}, "iv_radar_benchmarks": []},
    )
    assert result["enqueued"] >= 1
    payloads = [j["payload"] for j in result["jobs"]]
    assert all(p["intraday"] is True for p in payloads)
    assert all(p["trade_date"] == "2024-06-20" for p in payloads)
    assert all(p["observed_at"] for p in payloads)


def test_enqueue_treasury() -> None:
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "treasury",
        target_date=date(2024, 6, 20),
        scheduler_cfg={"slots": {"treasury": {"lookback_days": 45}}},
    )
    assert result["enqueued"] == 1
    assert result["jobs"][0]["kind"] == "treasury_yields"
    assert result["jobs"][0]["payload"] == {"lookback_days": 45}


def test_enqueue_option_backfill_plans_one_job_per_underlying_month() -> None:
    """The planner is split by expiry month so no job walks 50,000 contracts."""
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "option-backfill",
        target_date=date(2026, 9, 8),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={
            "slots": {"option-backfill": {"months": 3, "strike_pct": 0.25, "dte": 60}},
            "iv_radar_benchmarks": [],
        },
    )
    assert result["enqueued"] == 3
    windows = sorted((j["payload"]["expiry_gte"], j["payload"]["expiry_lte"]) for j in result["jobs"])
    assert windows == [("2026-07-01", "2026-07-31"), ("2026-08-01", "2026-08-31"), ("2026-09-01", "2026-09-30")]
    assert all(j["payload"]["strike_pct"] == 0.25 and j["payload"]["dte"] == 60 for j in result["jobs"])


# ── Research option universe (blueprint C-F5) ──────────────────────────────


def _universe_rows() -> list[tuple[str, str, int]]:
    return [("SPY", "resident", 24), ("AAPL", "core", 24), ("MSFT", "core", 24), ("HALO", "edge", 12)]


def test_option_refresh_enumerates_the_research_universe_by_tier() -> None:
    conn = _DailyConn(research_universe=_universe_rows(), option_contracts=[])
    r = enqueue_slot(
        conn,
        "option-refresh",
        target_date=date(2026, 9, 9),
        watchlist_symbols=["TSLA"],  # ignored: the universe is the rule
        scheduler_cfg={"slots": {"option-refresh": {"priority": 4, "universe": "research", "max_new_per_run": 10}}},
    )
    by = {j["payload"]["underlying"]: j for j in r["jobs"]}
    assert set(by) == {"SPY", "QQQ", "IWM", "AAPL", "MSFT", "HALO"}, "universe plus benchmarks, not the watchlist"
    assert "TSLA" not in by
    pri = {u: j["priority"] for u, j in by.items()}
    assert pri["SPY"] == 4 + 3 and pri["AAPL"] == 4 + 2 and pri["HALO"] == 4 + 1
    assert pri["QQQ"] == 4, "a benchmark not in the table keeps the slot priority"


def test_option_refresh_ramps_new_names_first_within_the_daily_cap() -> None:
    rows = [(f"N{i:03d}", "core", 24) for i in range(30)] + [("AAPL", "core", 24)]
    # AAPL already has contracts; the thirty N-names do not.
    conn = _DailyConn(research_universe=rows, option_contracts=[("O:AAPL260117C00200000", "AAPL", date(2026, 1, 17))])
    r = enqueue_slot(
        conn,
        "option-refresh",
        target_date=date(2026, 9, 9),
        scheduler_cfg={"slots": {"option-refresh": {"universe": "research", "max_new_per_run": 12, "batch_size": 1}}},
    )
    unds = [j["payload"]["underlying"] for j in r["jobs"]]
    # Order is the contract: benchmarks, then the capped newcomers, then the
    # rotation. (batch_size 0 reads as unset and falls back to the default.)
    assert len(unds) == 3 + 12 + 1, "benchmarks + capped newcomers + one rotated name"
    newcomers = unds[3:15]
    assert all(u.startswith("N") for u in newcomers), "the cap bounds the ramp"
    assert "AAPL" not in newcomers, "a name that already has contracts is not a newcomer"


def test_option_backfill_takes_history_months_from_the_row() -> None:
    conn = _DailyConn(research_universe=_universe_rows())
    r = enqueue_slot(
        conn,
        "option-backfill",
        target_date=date(2026, 9, 9),
        scheduler_cfg={"slots": {"option-backfill": {"universe": "research", "months": 24}}},
    )
    per = {}
    for j in r["jobs"]:
        per[j["payload"]["underlying"]] = per.get(j["payload"]["underlying"], 0) + 1
    assert per["HALO"] == 12, "an edge name carries a year"
    assert per["AAPL"] == 24 and per["SPY"] == 24, "core and resident carry two"
    assert per["QQQ"] == 24, "a benchmark outside the table gets the slot default"


def test_option_refresh_falls_back_to_the_watchlist_when_the_universe_is_empty() -> None:
    conn = _DailyConn(research_universe=[])
    r = enqueue_slot(
        conn,
        "option-refresh",
        target_date=date(2026, 9, 9),
        watchlist_symbols=["TSLA", "NVDA"],
        scheduler_cfg={"slots": {"option-refresh": {"universe": "research", "batch_size": 5}}},
    )
    unds = {j["payload"]["underlying"] for j in r["jobs"]}
    assert {"TSLA", "NVDA", "SPY", "QQQ", "IWM"} == unds


def test_option_refresh_ignores_the_universe_unless_configured() -> None:
    conn = _DailyConn(research_universe=_universe_rows())
    r = enqueue_slot(
        conn,
        "option-refresh",
        target_date=date(2026, 9, 9),
        watchlist_symbols=["TSLA"],
        scheduler_cfg={"slots": {"option-refresh": {"batch_size": 5}}},
    )
    unds = {j["payload"]["underlying"] for j in r["jobs"]}
    assert "HALO" not in unds and "TSLA" in unds
