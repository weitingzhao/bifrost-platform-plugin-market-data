"""Who a dataset is supposed to cover — one answer per tier, read from here.

There were four answers to "how many symbols should we have": a watchlist
sample capped at 80, the doctor's watchlist ∪ benchmarks, a constant 4,000, and
``v_us_equity_universe``. A denominator that lives beside the number it divides
cannot disagree with itself, which is why the coverage meters read 100% however
little was collected.

Each tier's scope is the *set*, not just its size: a percentage is only honest
when its numerator is drawn from the same population — counting five years of
symbols, delisted ones included, against the tickers listed today reported 389%
coverage before this.

See ``docs/MASSIVE_BLUEPRINT.md`` §3.1 and contracts C-B1, C-G1.
"""

from __future__ import annotations

import logging
from typing import Any

from bifrost_market_data.contracts import Tier

logger = logging.getLogger(__name__)


def active_tickers(conn: Any, *, statement_timeout: str = "60s") -> set[str]:
    """The whole-market scope: what the vendor lists as active today.

    ``raw_market.ticker`` is the reference slot's own output, so this and the
    ``v_us_equity_universe`` view are two readings of one population; this is
    the one the contracts divide by.
    """
    return _symbol_set(
        conn,
        "SELECT symbol FROM raw_market.ticker WHERE active",
        statement_timeout=statement_timeout,
        what="active tickers",
    )


def universe_symbols(conn: Any, *, statement_timeout: str = "60s") -> set[str]:
    """The universe scope: the names Research's rule asks the collector for."""
    return _symbol_set(
        conn,
        "SELECT symbol FROM research.option_universe",
        statement_timeout=statement_timeout,
        what="option universe",
    )


def common_stock_scope(conn: Any, *, statement_timeout: str = "60s") -> set[str]:
    """Active USD common stock — the population the financials slots address.

    Judged against every active ticker, the three statements read ~83% held and
    rendered red, but an ETF or a trust files nothing: the shortfall was the
    denominator counting instruments the dataset was never going to hold. The
    ``fundamentals-rotate`` slot already walks exactly this list.
    """
    return _symbol_set(
        conn,
        """
        SELECT symbol FROM raw_market.ticker
        WHERE instrument_type = 'CS'
          AND market = 'stocks'
          AND COALESCE(active, true) = true
          AND lower(COALESCE(currency, 'usd')) = 'usd'
          AND symbol IS NOT NULL AND trim(symbol) <> ''
        """,
        statement_timeout=statement_timeout,
        what="common stock universe",
    )


def benchmark_scope(
    conn: Any,
    benchmarks: list[str],
    *,
    scheduler_cfg: Any = None,
    statement_timeout: str = "60s",
) -> set[str]:
    """The benchmark scope: the benchmarks plus the watchlist the slots rotate.

    The minute slots target that union, so dividing by the eleven benchmarks
    alone reported 164% coverage. The watchlist half needs the real scheduler
    block: with an empty one the loader takes the DB path to ``public.watchlist``,
    which Golden Source does not have, and the union quietly shrinks back to the
    benchmarks — measured 3/11 = 27% for stock_minute where the honest reading
    against the 29-name union is lower.
    """
    from bifrost_market_data.scheduler.daily import load_watchlist_symbols, resolve_scheduler_cfg

    out = {str(b).strip().upper() for b in benchmarks if str(b).strip()}
    try:
        cfg = scheduler_cfg if scheduler_cfg is not None else resolve_scheduler_cfg()
        out |= {str(s).strip().upper() for s in (load_watchlist_symbols(conn, cfg) or [])}
    except Exception as exc:  # noqa: BLE001 — a missing watchlist narrows the scope, it does not break it
        logger.warning("watchlist unavailable for the benchmark scope: %s", exc)
    return out


def scope_for(
    conn: Any,
    tier: Tier,
    *,
    benchmarks: list[str] | None = None,
    scheduler_cfg: Any = None,
) -> set[str]:
    """The population a tier's datasets are measured against."""
    if tier == "whole-market":
        return active_tickers(conn)
    if tier == "universe":
        return universe_symbols(conn)
    if tier == "benchmark-only":
        return benchmark_scope(conn, benchmarks or [], scheduler_cfg=scheduler_cfg)
    return set()


def _symbol_set(conn: Any, sql: str, *, statement_timeout: str, what: str) -> set[str]:
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            cur.execute(sql)
            rows = cur.fetchall() or []
    except Exception as exc:  # noqa: BLE001 — an unreadable scope is empty, never a crash
        logger.warning("%s scope unavailable: %s", what, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return set()
    return {str(r[0]).strip().upper() for r in rows if r and r[0]}


__all__ = ["active_tickers", "universe_symbols", "benchmark_scope", "scope_for"]
