"""Per-expiry option chain OI/volume breakdown endpoint.

Provides aggregated put/call OI and volume grouped by expiry date,
used by Trade API's stock_option_pcr.py for the Stock Inspector.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Query

from bifrost_market_data.api.deps import (
    normalize_symbol,
    require_db,
    table_exists,
)

router = APIRouter(prefix="/options/analytics", tags=["options-analytics"])


def _iso_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()[:10]
    return s if s else None


def query_chain_by_expiry(
    conn: Any,
    *,
    symbol: str,
    fallback_date: str | None = None,
) -> dict[str, Any]:
    """Aggregate OI + volume by expiry from option_contract + option_snapshot.

    The latest snapshot per contract, read by LATERAL on option_snapshot's
    (option_ticker, snapshot_ts) key; option_open_interest as last resort.
    """
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "option_contract"):
        return {"ok": True, "symbol": sym, "chain": [], "basis": None}

    chain: list[dict[str, Any]] = []

    # One contract at a time through the (option_ticker, snapshot_ts) key, not a
    # join to ``v_option_chain_latest``. That view is a DISTINCT ON over the whole
    # of option_snapshot and a join condition cannot be pushed into it, so every
    # call sorted all 8M rows and spilled ~70k pages to disk: 8.9 s for AAPL on
    # 2026-09-26 (24.6 s through the API) against 44 ms here. Both read the same
    # latest snapshot per contract — AAPL's 38 expiries and VRSN's 5 matched row
    # for row — so ``basis`` keeps its name.
    basis: str | None = "option_snapshots_latest"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT oc.expiry,
                   MAX(DATE(timezone('America/New_York', os.snapshot_ts))) AS snap_day,
                   SUM(CASE WHEN oc.option_right = 'P'
                       THEN COALESCE(os.open_interest, 0) ELSE 0 END)::bigint AS put_oi,
                   SUM(CASE WHEN oc.option_right = 'C'
                       THEN COALESCE(os.open_interest, 0) ELSE 0 END)::bigint AS call_oi,
                   SUM(CASE WHEN oc.option_right = 'P'
                       THEN COALESCE(os.day_volume, 0) ELSE 0 END)::bigint AS put_vol,
                   SUM(CASE WHEN oc.option_right = 'C'
                       THEN COALESCE(os.day_volume, 0) ELSE 0 END)::bigint AS call_vol
            FROM raw_market.option_contract oc
            LEFT JOIN LATERAL (
              SELECT open_interest, day_volume, snapshot_ts
              FROM raw_market.option_snapshot s
              WHERE s.option_ticker = oc.option_ticker
              ORDER BY s.snapshot_ts DESC
              LIMIT 1
            ) os ON TRUE
            WHERE oc.underlying = %s
            GROUP BY oc.expiry
            ORDER BY oc.expiry ASC
            """,
            (sym,),
        )

        raw = cur.fetchall() or []
        for r in raw:
            if hasattr(r, "keys"):
                d = dict(r)
            else:
                d = {
                    "expiry": r[0], "snap_day": r[1],
                    "put_oi": r[2], "call_oi": r[3],
                    "put_vol": r[4], "call_vol": r[5],
                }
            d["expiry"] = _iso_date(d.get("expiry"))
            d["snap_day"] = _iso_date(d.get("snap_day"))
            chain.append(d)

        if not chain and fallback_date and table_exists(conn, "market", "option_open_interest"):
            basis = "option_open_interest"
            cur.execute(
                """
                SELECT expiry,
                       SUM(CASE WHEN option_right = 'P'
                           THEN COALESCE(open_interest, 0) ELSE 0 END)::bigint AS put_oi,
                       SUM(CASE WHEN option_right = 'C'
                           THEN COALESCE(open_interest, 0) ELSE 0 END)::bigint AS call_oi
                FROM raw_market.option_open_interest
                WHERE underlying = %s AND trade_date = %s
                GROUP BY expiry
                ORDER BY expiry ASC
                """,
                (sym, fallback_date),
            )
            for r in cur.fetchall() or []:
                if hasattr(r, "keys"):
                    d = dict(r)
                else:
                    d = {"expiry": r[0], "put_oi": r[1], "call_oi": r[2]}
                d["expiry"] = _iso_date(d.get("expiry"))
                d["snap_day"] = fallback_date
                d["put_vol"] = 0
                d["call_vol"] = 0
                chain.append(d)

    return {"ok": True, "symbol": sym, "chain": chain, "basis": basis}


@router.get("/chain-by-expiry")
def options_chain_by_expiry(
    symbol: str = Query(..., description="Underlying symbol (e.g. NVDA)"),
    fallback_date: str | None = Query(None, description="OI fallback date YYYY-MM-DD"),
) -> dict[str, Any]:
    """Per-expiry OI and volume breakdown from option_contract + option_snapshot."""
    conn = require_db()
    try:
        return query_chain_by_expiry(conn, symbol=symbol, fallback_date=fallback_date)
    finally:
        conn.close()
