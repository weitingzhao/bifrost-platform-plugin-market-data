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


def benchmark_scope(
    conn: Any, benchmarks: list[str], *, statement_timeout: str = "60s"
) -> set[str]:
    """The benchmark scope: the benchmarks plus the watchlist the slots rotate.

    The minute slots target that union, so dividing by the eleven benchmarks
    alone reported 164% coverage.
    """
    from bifrost_market_data.scheduler.daily import load_watchlist_symbols

    out = {str(b).strip().upper() for b in benchmarks if str(b).strip()}
    try:
        out |= {str(s).strip().upper() for s in (load_watchlist_symbols(conn, {}) or [])}
    except Exception as exc:  # noqa: BLE001 — a missing watchlist narrows the scope, it does not break it
        logger.warning("watchlist unavailable for the benchmark scope: %s", exc)
    return out


def scope_for(conn: Any, tier: Tier, *, benchmarks: list[str] | None = None) -> set[str]:
    """The population a tier's datasets are measured against."""
    if tier == "whole-market":
        return active_tickers(conn)
    if tier == "universe":
        return universe_symbols(conn)
    if tier == "benchmark-only":
        return benchmark_scope(conn, benchmarks or [])
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
