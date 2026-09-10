"""Three axes over the dataset contract table — GET /market/coverage/dimensions.

Breadth, depth and freshness for every dataset, each against the denominator
its contract declares rather than one the panel invented. This is the endpoint
the blueprint exists for: before it, freshness was answered by seven panels
with four thresholds, breadth had four denominators (two of them structurally
always 100%), and depth was answered by nothing at all — 47 of 575 names had
any option history and no surface could say so.

Read-only.
"""

from __future__ import annotations

import logging
import statistics
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from typing import Any, Sequence

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import connect_db
from bifrost_market_data.continuity import measure as measure_continuity
from bifrost_market_data.contracts import CONTRACTS, UNIVERSE_MONTHS, DatasetContract
from bifrost_market_data.scheduler.daily import load_research_universe
from bifrost_market_data.scheduler.daily import resolve_scheduler_cfg
from bifrost_market_data.api.slow_cache import DEFAULT_TTL_SEC, BackgroundCache
from bifrost_market_data.scopes import (
    TIER_DEFINITIONS,
    active_tickers,
    benchmark_scope,
    universe_symbols,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/market/coverage", tags=["market-coverage"])

# Per-dataset budget. The widest scan measured 24s (stock_daily, 20,695 symbols
# over 13.6M rows); the rest are far cheaper.
STATEMENT_TIMEOUT = "120s"
MAX_WORKERS = 4
# The numbers move once a day. Several viewers polling must not each pay for a
# 13.6M-row scan, and `age_sec` says how old the answer is rather than dressing
# a cached number as live. The mechanism is shared with the inventory and the
# readiness summary, which have the same problem for the same reason.
TTL_SEC = DEFAULT_TTL_SEC
CACHE = BackgroundCache("dimensions", ttl_sec=TTL_SEC)

#: Depth kinds that name a plan boundary rather than a target to reach (C-D3).
BOUNDARY_KINDS = frozenset({"current_only", "catalogue", "forward_only"})


def _today() -> date:
    """Seam: depth is measured against a day, and a test needs to fix which one."""
    return date.today()


def _ok(data: Any) -> dict[str, Any]:
    return {"ok": True, "data": data}


def _close_quietly(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _date_expr(column: str) -> str:
    """Observation dates are stored as date or timestamptz; compare as dates."""
    return f"({column})::date"


def _rows(conn: Any, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
        cur.execute(sql, tuple(params or ()))
        return list(cur.fetchall() or [])


def _held_symbols(conn: Any, c: DatasetContract, session: date | None = None) -> set[str]:
    """The instruments this dataset holds, over the window its contract declares.

    A numerator has to cover the same span as its denominator. Counting every
    symbol ever seen against today's active tickers reported 389% for
    stock_daily — five years of listings, delisted ones included, over a
    denominator of what is listed now.

    For a session window that means the most recent *complete* delivery, not
    simply the newest date present. Two slots write option_snapshot: the EOD
    pipeline covers all 575 underlyings at 22:00 UTC, and the intraday chain
    covers the 26-name benchmark union at 14:30. Reading max(date) therefore
    made breadth swing 570 → 26 → 570 every weekday, and between 14:30 and
    22:00 it divided the intraday numerator by the universe denominator —
    exactly the mismatch C-B1 exists to prevent. Measured 2026-09-10: 99.1% at
    05:22 and 07:55, 4.5% at 16:14 and 16:33, on unchanged data.

    Bounded by the session rather than filtered to it. A dataset that is behind
    — treasury_yield was two days back — still holds a most-recent delivery, and
    lateness is freshness's answer to give, not breadth's.
    """
    sym = c.symbol_column
    if sym is None:
        return set()
    where = ""
    params: tuple[Any, ...] = ()
    if c.breadth_window == "session" and c.date_column:
        d = _date_expr(c.date_column)
        if session is None:
            where = f"WHERE {d} = (SELECT max({d}) FROM {c.dataset})"
        else:
            where = f"WHERE {d} = (SELECT max({d}) FROM {c.dataset} WHERE {d} <= %s)"
            params = (session,)
    rows = _rows(conn, f"SELECT DISTINCT {sym} FROM {c.dataset} {where}", params)
    return {str(r[0]) for r in rows if r and r[0] is not None}


def _per_symbol_oldest(conn: Any, c: DatasetContract) -> list[tuple[str, date | None]]:
    """(symbol, oldest observation) for every instrument the dataset holds.

    The shape follows the cardinality, not a preference. A skip scan probes once
    per distinct value, which wins only when those are few: measured 2026-09-09,
    option_daily (60 underlyings) 0.85s by skip scan against 152s for
    stock_daily's 20,695 symbols, where a plain grouped scan is 24s.
    """
    sym, dt = c.symbol_column, c.date_column
    if sym is None or dt is None:
        return []
    if c.low_cardinality:
        sql = f"""
            WITH RECURSIVE walk AS (
                (SELECT min({sym}) AS s FROM {c.dataset})
                UNION ALL
                SELECT (SELECT min({sym}) FROM {c.dataset} WHERE {sym} > walk.s)
                FROM walk WHERE walk.s IS NOT NULL
            )
            SELECT s, (SELECT min({_date_expr(dt)}) FROM {c.dataset} d WHERE d.{sym} = walk.s)
            FROM walk WHERE s IS NOT NULL
        """
    else:
        sql = f"SELECT {sym}, min({_date_expr(dt)}) FROM {c.dataset} GROUP BY 1"
    return [(str(r[0]), r[1]) for r in _rows(conn, sql) if r and r[0] is not None]


def _breadth_only(conn: Any, c: DatasetContract) -> int:
    """Instrument count for a catalogue — there is no observation date to group by."""
    if c.symbol_column is None:
        return 1
    return int(_rows(conn, f"SELECT count(DISTINCT {c.symbol_column}) FROM {c.dataset}")[0][0] or 0)


def _newest(conn: Any, c: DatasetContract) -> date | None:
    if c.date_column is None:
        return None
    rows = _rows(conn, f"SELECT max({_date_expr(c.date_column)}) FROM {c.dataset}")
    return rows[0][0] if rows else None


#: Depth targets that describe a distribution rather than a bar to clear. An
#: absolute start cannot be judged per symbol without knowing when each
#: instrument began: a company that listed in 2020 can never reach 2009, and
#: measured 2026-09-10 every one of the 4,467 symbols in income_statement
#: "failed" a 2009 target while the median held 9.7 years. The spread is
#: reported; the pass count is not, because it would be a count of nothing.
UNJUDGED_DEPTH_KINDS = frozenset({"since"})


#: How many sessions a forward-only dataset has managed to accrue, and when it
#: started. A boundary said "this cannot be bought" and then showed nothing at
#: all, which is a quarter of the matrix rendered inert — but a chain snapshot
#: climbing toward the ninety sessions trim keeps is doing something, and the
#: number is worth reading.
#:
#: One index probe per distinct day, the same loose scan `enumerated_underlyings`
#: uses. Bounded, because a table with years of daily rows would otherwise walk
#: every one of them for a figure nobody needs to that precision.
ACCRUAL_WALK_LIMIT = 2000

_ACCRUAL_SQL = """
/* accrual */
WITH RECURSIVE walk AS (
    (SELECT min({col}) AS t FROM {table})
    UNION ALL
    SELECT (SELECT min({col}) FROM {table} WHERE {col} >= ((walk.t)::date + 1))
    FROM walk WHERE walk.t IS NOT NULL
)
SELECT count(*)::bigint, min(t)::date, max(t)::date
FROM (SELECT t FROM walk WHERE t IS NOT NULL LIMIT {limit}) d
"""


def _rollback_quietly(conn: Any) -> None:
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001
        pass


def _accrual(conn: Any, c: DatasetContract) -> dict[str, Any] | None:
    """Sessions held and the first of them, or None when it cannot be read."""
    if c.date_column is None:
        return None
    sql = _ACCRUAL_SQL.format(
        col=c.date_column, table=c.dataset, limit=ACCRUAL_WALK_LIMIT
    )
    try:
        rows = _rows(conn, sql)
        # Parsing belongs inside the guard too — an unexpected row shape is as
        # much a failed read as a timeout, and neither may escape. Written once
        # for per_day_counts in 0.20.1 and worth writing again: left outside,
        # this exact line took breadth, depth and freshness down with it.
        if not rows or rows[0][0] is None:
            return None
        held = int(rows[0][0] or 0)
        first, last = rows[0][1], rows[0][2]
    except Exception as exc:  # noqa: BLE001 — the boundary still stands without it
        logger.warning("accrual read failed for %s: %s", c.dataset, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    out: dict[str, Any] = {
        "sessions_held": held,
        "since": first.isoformat() if first else None,
        "newest": last.isoformat() if last else None,
        "capped": held >= ACCRUAL_WALK_LIMIT,
    }
    if c.depth.accrues_to_sessions:
        out["accrues_to"] = c.depth.accrues_to_sessions
        out["pct"] = round(100.0 * held / c.depth.accrues_to_sessions, 1)
    return out


def _depth(
    c: DatasetContract,
    per_symbol: list[tuple[str, date | None]],
    today: date,
    scope: set[str] | None = None,
    accrual: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Depth as the contract defines it, or the plan boundary that replaces it."""
    target = {
        "kind": c.depth.kind,
        "value": c.depth.value,
        "why": c.depth.why,
        "accrues_to_sessions": c.depth.accrues_to_sessions,
    }
    if c.depth.kind in BOUNDARY_KINDS or not per_symbol:
        out: dict[str, Any] = {
            "target": target,
            "measured": False,
            "at_target": None,
            "of": len(per_symbol) or None,
        }
        # A boundary says the depth cannot be bought. It does not say nothing is
        # happening: a chain snapshot is climbing toward the ninety sessions
        # trim keeps, and that climb is the only thing this axis can report for
        # it. Without this the square was inert.
        if accrual is not None:
            out["accrual"] = accrual
        return out

    # The same population breadth divides by. Depth counted every symbol the
    # table had ever held — 20,703 for stock_daily against a breadth denominator
    # of 5,317 — so one dataset was graded on two different populations, which
    # is the mismatch C-B1 exists to end. Most of that tail is delisted or never
    # in scope, and a five-year window ending today was never asked of it.
    if scope and c.symbol_column:
        in_scope = [(sym, d) for sym, d in per_symbol if sym in scope]
        if in_scope:
            per_symbol = in_scope

    spans = [(s, (today - d).days) for s, d in per_symbol if d is not None]
    if not spans:
        return {"target": target, "measured": False, "at_target": None, "of": 0}

    if c.depth.kind == "rolling_days":
        need = int(c.depth.value or 0)
    elif c.depth.kind == "sessions":
        # Sessions are not calendar days; ~1.45 calendar days per session.
        need = int(round(int(c.depth.value or 0) * 1.45))
    elif c.depth.kind == "since":
        need = (today - date.fromisoformat(str(c.depth.value))).days
    else:
        need = 0

    days = sorted(d for _s, d in spans)
    shallowest = min(spans, key=lambda kv: kv[1])
    if c.depth.kind in UNJUDGED_DEPTH_KINDS:
        return {
            "target": target,
            "measured": True,
            "judged": False,
            "why": (
                "an absolute start cannot be judged per symbol without knowing when "
                "each instrument began; the spread is the answer, not a pass count"
            ),
            "need_days": need,
            "at_target": None,
            "of": len(days),
            "median_days": int(statistics.median(days)),
            "shallowest": {"symbol": shallowest[0], "days": shallowest[1]},
            "oldest_days": days[-1],
        }
    return {
        "target": target,
        "measured": True,
        "judged": True,
        "need_days": need,
        "at_target": sum(1 for d in days if d >= need),
        "of": len(days),
        "median_days": int(statistics.median(days)),
        "shallowest": {"symbol": shallowest[0], "days": shallowest[1]},
        "oldest_days": days[-1],
    }


def _session(conn: Any) -> date | None:
    """The session the tables should hold, or None when it cannot be resolved.

    None means "do not bound", which restores the old max(date) reading rather
    than silently reporting nothing held.
    """
    from bifrost_market_data.session import resolve_session

    try:
        return resolve_session(conn, datetime.now(timezone.utc))[0]
    except Exception as exc:  # noqa: BLE001 — breadth still has an answer without it
        logger.warning("dimensions: session unresolved, breadth reads max(date): %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def _expected_days(conn: Any, today: date) -> list[date]:
    """Trading days in the continuity window, or [] when the calendar is unreadable.

    Without it a completely blank session is invisible: it contributes no row to
    a per-day count, so nothing notices it is not there.
    """
    from bifrost_market_data.continuity import WINDOW_DAYS, window_start
    from bifrost_market_data.trading_calendar import expected_trading_days

    try:
        return expected_trading_days(conn, start=window_start(WINDOW_DAYS, today), end=today)
    except Exception as exc:  # noqa: BLE001 — absent days go unnamed, the rest still reports
        logger.warning("continuity calendar unavailable: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []


def _one(
    c: DatasetContract,
    denominators: dict[str, Any],
    today: date,
    conn: Any = None,
    expected_days: Sequence[date] | None = None,
    session: date | None = None,
) -> dict[str, Any]:
    own = conn is None
    if own:
        conn = connect_db(statement_timeout=STATEMENT_TIMEOUT)
    try:
        try:
            per_symbol: list[tuple[str, date | None]] = []
            if c.symbol_column is None:
                held_symbols: set[str] = set()
                held_total = 1
            else:
                held_symbols = _held_symbols(conn, c, session)
                held_total = len(held_symbols)
            # A dataset whose depth is a plan boundary has no target to measure
            # against, so the per-symbol scan would be work done only to discard.
            # That is most of the cost: short_interest alone is 22,932 symbols.
            if c.symbol_column and c.date_column and c.depth.kind not in BOUNDARY_KINDS:
                per_symbol = _per_symbol_oldest(conn, c)
            newest = _newest(conn, c)
            error = None
        except Exception as exc:  # noqa: BLE001 — one unreadable dataset must not sink the page
            logger.warning("coverage dimensions failed for %s: %s", c.dataset, exc)
            try:
                conn.rollback()
            except Exception:
                pass
            per_symbol, held_symbols, held_total, newest = [], set(), 0, None
            error = str(exc)[:160]

        # In a guard of its own, and only for a forward-only depth. A catalogue
        # holds what is listed now and a point-in-time snapshot holds today;
        # neither climbs toward anything. Sharing the guard above is how 0.20.0
        # let one axis blank the three that had already succeeded.
        accrual = None
        if c.depth.kind == "forward_only":
            try:
                accrual = _accrual(conn, c)
            except Exception as exc:  # noqa: BLE001
                logger.warning("accrual failed for %s: %s", c.dataset, exc)
                _rollback_quietly(conn)

        # The fourth axis, in a guard of its own. Breadth, depth and freshness
        # all read healthy over seven blank days in stock_daily and only this
        # one looks at the middle — but it is the newest of the four, and a
        # fault in it must not erase three measurements that already succeeded.
        try:
            continuity = measure_continuity(
                conn, c, expected_days=expected_days, statement_timeout=STATEMENT_TIMEOUT
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("continuity failed for %s: %s", c.dataset, exc)
            try:
                conn.rollback()
            except Exception:
                pass
            continuity = {"measured": False, "why": "read failed"}
    finally:
        if own:
            _close_quietly(conn)

    scope: set[str] = denominators["scopes"].get(c.tier) or set()
    of_n = 1 if c.symbol_column is None else len(scope)
    # The numerator is what we hold *of what this tier asked for*. Held rows
    # outside that scope are reported separately rather than inflating a ratio.
    in_scope = of_n if c.symbol_column is None else len(held_symbols & scope)
    return {
        "dataset": c.dataset,
        "tier": c.tier,
        # Tier says which instruments, grain says what one row is. Two axes, and
        # the console arranges the estate by both.
        "grain": c.grain,
        "slots": list(c.slots),
        "breadth_window": c.breadth_window,
        # How a missed session is repaired, or why it needs no repairing. The
        # console's agent brief turns this into the exact call, and three
        # different reasons for "nothing to prescribe" must not collapse into
        # one: gone for good, repairs itself, and not a session series.
        "refill": {
            "how": c.refill.how,
            "target": c.refill.target,
            "lookback_days": c.refill.lookback_days,
            "why": c.refill.why,
        },
        "error": error,
        "breadth": {
            # False where dividing by the tier's scope is the wrong question,
            # not where the read failed.
            "judged": c.breadth_unjudged is None,
            "why": c.breadth_unjudged,
            "held": in_scope,
            "held_total": held_total,
            "outside_scope": max(0, held_total - in_scope),
            "of": of_n or None,
            # Intent fulfilment: how much of what this tier asked for we hold.
            "pct": round(100.0 * in_scope / of_n, 1) if of_n else None,
            # Entitlement utilisation: how much of what the plan allows the tier
            # even asks for. Only meaningful where the tier narrows the market.
            "entitlement_pct": (
                round(100.0 * of_n / int(denominators["whole-market"]), 1)
                if c.tier == "universe" and denominators.get("whole-market")
                else None
            ),
        },
        "depth": _depth(c, per_symbol, today, scope, accrual),
        "freshness": _freshness(
            c, newest, today, interval_days=continuity.get("median_interval_days")
        ),
        "continuity": continuity,
    }


#: How many of a dataset's own publication intervals may pass before the
#: newest row counts as late. One interval means "the next one is not due yet";
#: two means one was skipped. Blunt on purpose — the vendor's publication lag
#: is not observable from here, so the line is drawn where a *missed* release
#: is unambiguous rather than where a late one might be.
OVERDUE_INTERVALS = 2


def _freshness(
    c: DatasetContract,
    newest: date | None,
    today: date,
    *,
    interval_days: int | None = None,
) -> dict[str, Any]:
    """How late the newest row is, judged on the dataset's own cadence.

    A deadline in hours only means something for a feed that publishes every
    session. short_interest settles twice a month and FINRA publishes about ten
    days later, so measured 2026-09-10 it read 27 days behind a 30-hour
    deadline while holding every settlement the vendor had released — the same
    mistake the fourth axis made before cadence was declared, repeated on this
    axis because it never learned to read the field.
    """
    if c.date_column is None:
        return {"newest": None, "deadline_hours": c.freshness_hours, "measured": False}
    behind = (today - newest).days if newest else None
    out: dict[str, Any] = {
        "newest": newest.isoformat() if newest else None,
        "deadline_hours": c.freshness_hours,
        "measured": True,
        "days_behind": behind,
        "cadence": c.cadence,
        "expected_interval_days": None,
        "overdue": None,
        # False where a clock is the wrong instrument, not where it failed.
        "judged": True,
    }
    if c.cadence == "session":
        # A session feed keeps its hour deadline; the doctor owns that verdict
        # and this axis must not answer it differently.
        return out
    if c.cadence == "filing":
        # There is nothing here for a clock to judge. A company files when it
        # files, and period_date follows each company's own fiscal calendar, so
        # the dataset's newest row only says who filed most recently — measured
        # 2026-09-10 the three statements read 39 days behind a 48-hour deadline
        # with nothing wrong. Whether the collector still runs is the
        # ingest_freshness question, and it is asked elsewhere.
        out["judged"] = False
        out["why"] = (
            "a filing arrives when the company files; the newest period_date is "
            "not a clock. Watch the slot's own liveness instead."
        )
        return out
    out["expected_interval_days"] = interval_days
    if behind is not None and interval_days:
        out["overdue"] = behind > interval_days * OVERDUE_INTERVALS
    return out


def _denominators(conn: Any) -> dict[str, Any]:
    """The four denominators, each from ``scopes`` — never from a panel.

    Each carries its symbol set, because a percentage is only honest when the
    numerator is drawn from the same population: five years of stock symbols
    over today's active tickers reads as 389% coverage.
    """
    active = active_tickers(conn, statement_timeout=STATEMENT_TIMEOUT)
    universe_syms = universe_symbols(conn, statement_timeout=STATEMENT_TIMEOUT)
    by_tier: dict[str, int] = {}
    for row in load_research_universe(conn) or []:
        by_tier[str(row.get("tier") or "?")] = by_tier.get(str(row.get("tier") or "?"), 0) + 1
    sched = resolve_scheduler_cfg()
    bench = benchmark_scope(
        conn,
        _benchmarks(sched),
        scheduler_cfg=sched,
        statement_timeout=STATEMENT_TIMEOUT,
    )
    return {
        "whole-market": len(active),
        "universe": {"total": len(universe_syms), "by_tier": by_tier, "months": UNIVERSE_MONTHS},
        "benchmark-only": len(bench),
        "global": 1,
        # What each of those numbers *is*. A denominator with no definition is
        # how "Whole market 5,317" came to read as the market when it is the
        # vendor's active common-stock list.
        "definitions": TIER_DEFINITIONS,
        "scopes": {
            "whole-market": active,
            "universe": universe_syms,
            "benchmark-only": bench,
            "global": set(),
        },
    }


def _benchmarks(scheduler_cfg: dict[str, Any]) -> list[str]:
    names = scheduler_cfg.get("iv_radar_benchmarks") or []
    if isinstance(names, str):
        names = [s for s in names.split(",") if s.strip()]
    return [str(s) for s in names]


def _compute(key: str, wanted: list[DatasetContract]) -> dict[str, Any]:
    """The expensive part, shared by the synchronous and background paths."""
    probe = connect_db(statement_timeout=STATEMENT_TIMEOUT)
    try:
        denominators = _denominators(probe)
        today = _today()
        # One calendar read for the whole page: an absent session can only be
        # named against the days the market was actually open.
        expected_days = _expected_days(probe, today)
        # The one definition of "the session the tables should hold" (C-F1).
        # Breadth is bounded by it so a half-written session — the intraday
        # chain's 26 names at 14:30, before the EOD pipeline's 575 at 22:00 —
        # cannot be divided by the universe denominator.
        session = _session(probe)
        workers = min(MAX_WORKERS, len(wanted))
        if workers == 1:
            rows = [_one(wanted[0], denominators, today, probe, expected_days, session)]
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dimensions") as pool:
                futures = [
                    pool.submit(
                        _one, wanted[0], denominators, today, probe, expected_days, session
                    )
                ]
                futures += [
                    pool.submit(_one, c, denominators, today, None, expected_days, session)
                    for c in wanted[1:]
                ]
                rows = [f.result() for f in futures]
    finally:
        _close_quietly(probe)
    public_denominators = {k: v for k, v in denominators.items() if k != "scopes"}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "denominators": public_denominators,
        "datasets": rows,
    }


@router.get("/dimensions")
def get_dimensions(
    tier: str | None = Query(None, description="whole-market | universe | benchmark-only | global"),
    refresh: bool = Query(False, description="recompute instead of reading the cached answer"),
) -> dict[str, Any]:
    """Breadth, depth and freshness for every dataset that has a contract."""
    key = tier or "all"
    wanted = [c for c in CONTRACTS if tier is None or c.tier == tier]
    if not wanted:
        raise HTTPException(status_code=400, detail=f"no datasets in tier {tier!r}")

    # Scanning every dataset takes minutes; the API gateway gives up at 60s and
    # the reader should never wait that long for numbers that move once a day.
    def _work() -> dict[str, Any]:
        return _compute(key, wanted)

    if not refresh:
        return _ok(CACHE.read(key, _work, empty={"datasets": []}))

    try:
        return _ok(CACHE.compute_now(key, _work))
    except Exception as exc:
        logger.exception("coverage dimensions failed")
        raise HTTPException(status_code=503, detail=str(exc)) from exc


__all__ = ["router", "get_dimensions", "CACHE", "TTL_SEC"]
