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
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from time import monotonic
from typing import Any, Sequence

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import connect_db
from bifrost_market_data.contracts import CONTRACTS, UNIVERSE_MONTHS, DatasetContract
from bifrost_market_data.scheduler.daily import load_research_universe
from bifrost_market_data.scopes import active_tickers, benchmark_scope, universe_symbols

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/market/coverage", tags=["market-coverage"])

# Per-dataset budget. The widest scan measured 24s (stock_daily, 20,695 symbols
# over 13.6M rows); the rest are far cheaper.
STATEMENT_TIMEOUT = "120s"
MAX_WORKERS = 4
# The numbers move once a day. Several viewers polling must not each pay for a
# 13.6M-row scan, and `age_sec` says how old the answer is rather than dressing
# a cached number as live.
TTL_SEC = 600.0
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_REFRESHING: dict[str, bool] = {}
_REFRESH_LOCK = threading.Lock()

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


def _held_symbols(conn: Any, c: DatasetContract) -> set[str]:
    """The instruments this dataset holds, over the window its contract declares.

    A numerator has to cover the same span as its denominator. Counting every
    symbol ever seen against today's active tickers reported 389% for
    stock_daily — five years of listings, delisted ones included, over a
    denominator of what is listed now.
    """
    sym = c.symbol_column
    if sym is None:
        return set()
    where = ""
    if c.breadth_window == "session" and c.date_column:
        d = _date_expr(c.date_column)
        where = f"WHERE {d} = (SELECT max({d}) FROM {c.dataset})"
    rows = _rows(conn, f"SELECT DISTINCT {sym} FROM {c.dataset} {where}")
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


def _depth(
    c: DatasetContract, per_symbol: list[tuple[str, date | None]], today: date
) -> dict[str, Any]:
    """Depth as the contract defines it, or the plan boundary that replaces it."""
    target = {"kind": c.depth.kind, "value": c.depth.value, "why": c.depth.why}
    if c.depth.kind in BOUNDARY_KINDS or not per_symbol:
        return {
            "target": target,
            "measured": False,
            "at_target": None,
            "of": len(per_symbol) or None,
        }

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
    return {
        "target": target,
        "measured": True,
        "need_days": need,
        "at_target": sum(1 for d in days if d >= need),
        "of": len(days),
        "median_days": int(statistics.median(days)),
        "shallowest": {"symbol": shallowest[0], "days": shallowest[1]},
        "oldest_days": days[-1],
    }


def _one(
    c: DatasetContract, denominators: dict[str, Any], today: date, conn: Any = None
) -> dict[str, Any]:
    own = conn is None
    if own:
        conn = connect_db(statement_timeout=STATEMENT_TIMEOUT)
    try:
        per_symbol: list[tuple[str, date | None]] = []
        if c.symbol_column is None:
            held_symbols: set[str] = set()
            held_total = 1
        else:
            held_symbols = _held_symbols(conn, c)
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
        per_symbol, held_symbols, held_total, newest, error = [], set(), 0, None, str(exc)[:160]
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
        "slots": list(c.slots),
        "breadth_window": c.breadth_window,
        "error": error,
        "breadth": {
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
        "depth": _depth(c, per_symbol, today),
        "freshness": _freshness(c, newest, today),
    }


def _freshness(c: DatasetContract, newest: date | None, today: date) -> dict[str, Any]:
    if c.date_column is None:
        return {"newest": None, "deadline_hours": c.freshness_hours, "measured": False}
    behind = (today - newest).days if newest else None
    return {
        "newest": newest.isoformat() if newest else None,
        "deadline_hours": c.freshness_hours,
        "measured": True,
        "days_behind": behind,
    }


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
    bench = benchmark_scope(conn, _benchmarks(), statement_timeout=STATEMENT_TIMEOUT)
    return {
        "whole-market": len(active),
        "universe": {"total": len(universe_syms), "by_tier": by_tier, "months": UNIVERSE_MONTHS},
        "benchmark-only": len(bench),
        "global": 1,
        "scopes": {
            "whole-market": active,
            "universe": universe_syms,
            "benchmark-only": bench,
            "global": set(),
        },
    }


def _benchmarks() -> list[str]:
    from bifrost_market_data.scheduler.daily import load_schedule

    raw = load_schedule() or {}
    sched = raw.get("scheduler") if isinstance(raw, dict) else {}
    names = (sched or {}).get("iv_radar_benchmarks") or []
    return [str(s) for s in names]


def _compute(key: str, wanted: list[DatasetContract]) -> dict[str, Any]:
    """The expensive part, shared by the synchronous and background paths."""
    probe = connect_db(statement_timeout=STATEMENT_TIMEOUT)
    try:
        denominators = _denominators(probe)
        today = _today()
        started = monotonic()
        workers = min(MAX_WORKERS, len(wanted))
        if workers == 1:
            rows = [_one(wanted[0], denominators, today, probe)]
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dimensions") as pool:
                futures = [pool.submit(_one, wanted[0], denominators, today, probe)]
                futures += [pool.submit(_one, c, denominators, today) for c in wanted[1:]]
                rows = [f.result() for f in futures]
    finally:
        _close_quietly(probe)
    public_denominators = {k: v for k, v in denominators.items() if k != "scopes"}
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "age_sec": 0.0,
        "computing": False,
        "computed_ms": int((monotonic() - started) * 1000),
        "denominators": public_denominators,
        "datasets": rows,
    }
    _CACHE[key] = (monotonic(), payload)
    return payload


def _start_refresh(key: str, wanted: list[DatasetContract]) -> bool:
    """Ensure a recompute is in flight for this key; True when one is.

    Answers "is a refresh running", not "did I start one" — a caller arriving
    while another is already scanning was told computing=false and shown an
    empty page it had no reason to poll again.
    """
    with _REFRESH_LOCK:
        if _REFRESHING.get(key):
            return True
        _REFRESHING[key] = True

    def run() -> None:
        try:
            _compute(key, wanted)
        except Exception:  # noqa: BLE001 — a failed refresh leaves the last good answer
            logger.exception("coverage dimensions refresh failed for %s", key)
        finally:
            with _REFRESH_LOCK:
                _REFRESHING[key] = False

    threading.Thread(target=run, name=f"dimensions-{key}", daemon=True).start()
    return True


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

    now = monotonic()
    hit = _CACHE.get(key)
    if not refresh:
        # Scanning every dataset takes minutes; the API gateway gives up at 60s
        # and the reader should never wait that long for numbers that move once
        # a day. Answer from the cache and refresh behind it, saying plainly how
        # old the answer is and whether a fresher one is on its way.
        fresh_enough = hit is not None and now - hit[0] <= TTL_SEC
        if not fresh_enough:
            started_refresh = _start_refresh(key, wanted)
            if hit is None:
                return _ok({"computing": started_refresh, "age_sec": None, "datasets": []})
        out = dict(hit[1])
        out["age_sec"] = round(now - hit[0], 1)
        out["computing"] = not fresh_enough
        return _ok(out)

    try:
        return _ok(_compute(key, wanted))
    except Exception as exc:
        logger.exception("coverage dimensions failed")
        raise HTTPException(status_code=503, detail=str(exc)) from exc


__all__ = ["router", "get_dimensions", "TTL_SEC"]
