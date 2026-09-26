"""Corporate actions and daily checklist DB routes (Wave 5-B)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import (
    connect_db,
    iso_value,
    normalize_symbol,
    require_db,
    row_dict,
    table_exists,
)

router = APIRouter(tags=["corp-actions"])


def query_corporate_actions(
    conn: Any,
    *,
    symbol: str,
    action_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not table_exists(conn, "market", "corporate_action"):
        return []
    sym = normalize_symbol(symbol)
    clauses = ["symbol = %s"]
    params: list[Any] = [sym]
    if action_type:
        clauses.append("LOWER(action_type) = LOWER(%s)")
        params.append(action_type.strip())
    where = " AND ".join(clauses)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                id, symbol, action_type, ex_date, record_date, payment_date,
                ratio_from, ratio_to, amount, currency, description, fetched_at,
                distribution_type, frequency
            FROM raw_market.corporate_action
            WHERE {where}
            ORDER BY ex_date DESC NULLS LAST, distribution_type NULLS LAST, currency, amount DESC, id DESC
            LIMIT %s
            """,
            (*params, limit),
        )
        raw = cur.fetchall() or []
    cols = (
        "id",
        "symbol",
        "action_type",
        "ex_date",
        "record_date",
        "payment_date",
        "ratio_from",
        "ratio_to",
        "amount",
        "currency",
        "description",
        "fetched_at",
        # One ex-date can carry several rows — a special beside the regular, the
        # same dividend in CAD and in USD. Sum within one currency, and read
        # `special` apart from what the regular schedule pays.
        "distribution_type",
        "frequency",
    )
    return [row_dict(r, cols) for r in raw]


def query_daily_checklist(
    conn: Any,
    *,
    symbols: list[str],
    trade_date: str,
) -> dict[str, Any]:
    """Simplified readiness checklist from local tables + ingest freshness."""
    syms = [normalize_symbol(s) for s in symbols if normalize_symbol(s)]
    checklist: dict[str, dict[str, Any]] = {}
    freshness_by_dim: dict[str, Any] = {}
    if table_exists(conn, "ops_jobs", "ingest_freshness"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dimension, last_run_at, status, rows_written
                FROM ops_jobs.ingest_freshness
                """
            )
            for row in cur.fetchall() or []:
                freshness_by_dim[str(row[0])] = {
                    "last_run_at": iso_value(row[1]),
                    "status": row[2],
                    "rows_written": row[3],
                }

    # Three point probes per symbol, against three tables that each carry an
    # index leading with exactly this column: stock_daily (symbol, bar_date),
    # option_open_interest (underlying, trade_date) and corporate_action
    # (symbol, ex_date). Wrapping the column in ``UPPER(TRIM())`` made an
    # equality probe unusable by all three, so the panel above — 40 watchlist
    # symbols, refetching every 60 seconds — put 120 sequential full scans a
    # minute on the shared database. Research measured a batch of them beside
    # the snapshot-coverage join on 2026-09-25.
    #
    # Normalising the *input* is enough because the columns already hold
    # normalised values: ``underlying <> UPPER(TRIM(underlying))`` returned 0
    # on 2026-09-08 and the ingest path has upper-cased and stripped since.
    # ``normalize_symbol`` above does the same to what the caller asked for.
    #
    # This is not a general rule about the wrapper. On the whole-table
    # aggregates it is harmless and removing it measured *slower* (401s against
    # 151s on 2026-09-09) because either way every row is read. It only costs
    # something where an index could otherwise have been probed.
    for sym in syms:
        item: dict[str, Any] = {"symbol": sym, "trade_date": trade_date}
        if table_exists(conn, "market", "stock_daily"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint FROM raw_market.stock_daily
                    WHERE symbol = %s AND bar_date = %s::date
                    """,
                    (sym, trade_date),
                )
                item["stock_daily_rows"] = int((cur.fetchone() or [0])[0])
        if table_exists(conn, "market", "option_open_interest"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint FROM raw_market.option_open_interest
                    WHERE underlying = %s AND trade_date = %s::date
                    """,
                    (sym, trade_date),
                )
                item["option_oi_rows"] = int((cur.fetchone() or [0])[0])
        if table_exists(conn, "market", "corporate_action"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint FROM raw_market.corporate_action
                    WHERE symbol = %s AND ex_date = %s::date
                    """,
                    (sym, trade_date),
                )
                item["corporate_action_rows"] = int((cur.fetchone() or [0])[0])
        checklist[sym] = item

    return {
        "ok": True,
        "trade_date": trade_date,
        "symbols": checklist,
        "freshness": freshness_by_dim,
        "note": "Simplified checklist from market.* row presence and ops_jobs.ingest_freshness.",
    }


@router.get("/corporate-actions")
def get_corporate_actions(
    symbol: str = Query(..., description="Stock ticker"),
    action_type: str | None = Query(None, description="dividend | split | ..."),
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    conn = require_db()
    try:
        rows = query_corporate_actions(conn, symbol=sym, action_type=action_type, limit=limit)
        return {"ok": True, "symbol": sym, "rows": rows, "count": len(rows)}
    finally:
        conn.close()


@router.get("/daily-checklist")
def get_daily_checklist(
    symbols: str = Query(..., description="Comma-separated underlying symbols"),
    trade_date: str | None = Query(None, description="Session calendar date YYYY-MM-DD (US)"),
) -> dict[str, Any]:
    sym_list = [s.strip() for s in (symbols or "").split(",") if s.strip()][:80]
    if not sym_list:
        raise HTTPException(status_code=400, detail="symbols is required")
    td = (trade_date or "").strip()
    if not td:
        td = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    conn = require_db()
    try:
        return query_daily_checklist(conn, symbols=sym_list, trade_date=td)
    finally:
        conn.close()


_connect = connect_db
