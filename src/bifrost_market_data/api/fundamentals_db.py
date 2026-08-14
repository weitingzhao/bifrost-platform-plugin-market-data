"""DB-read stock financials routes under ``/market/stocks/fundamentals/db/*`` (W0-P3).

Reads from ``market.stock_financials`` — the persisted Polygon financial data.
Separate from ``fundamentals.py`` which is Polygon REST pass-through.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import (
    iso_value,
    normalize_symbol,
    require_db,
    table_exists,
)

router = APIRouter(prefix="/stocks/fundamentals/db", tags=["fundamentals-db"])

_VALID_REPORT_TYPES = frozenset(
    {"income_statement", "balance_sheet", "cash_flow", "short_interest", "short_volume"}
)
_VALID_TIMEFRAMES = frozenset({"quarterly", "annual"})


def _parse_symbols(raw: str) -> list[str]:
    """Split comma-separated symbols, normalize, deduplicate preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for part in raw.split(","):
        sym = normalize_symbol(part)
        if sym and sym not in seen:
            seen.add(sym)
            result.append(sym)
    return result


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def query_short_interest(
    conn: Any, *, symbols: list[str], settlements: int
) -> dict[str, list[dict[str, Any]]]:
    """Recent short interest from market.stock_financials grouped by symbol.

    Field names match ``market_pg.get_short_interest_recent`` consumer contract.
    """
    if not table_exists(conn, "market", "stock_financials"):
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              UPPER(TRIM(symbol)) AS symbol,
              period_date AS settlement_date,
              COALESCE(
                (data->>'short_interest')::bigint,
                (data->>'short_interest_shares')::bigint,
                (data->>'short_shares')::bigint
              ) AS short_interest,
              COALESCE(
                (data->>'avg_daily_volume')::bigint,
                (data->>'avg_daily_volume_consolidated')::bigint
              ) AS avg_daily_volume,
              (data->>'days_to_cover')::double precision AS days_to_cover
            FROM (
              SELECT *,
                ROW_NUMBER() OVER (
                  PARTITION BY UPPER(TRIM(symbol)) ORDER BY period_date DESC
                ) AS rn
              FROM market.stock_financials
              WHERE UPPER(TRIM(symbol)) = ANY(%s)
                AND report_type = 'short_interest'
            ) sub
            WHERE rn <= %s
            ORDER BY symbol, settlement_date DESC
            """,
            (symbols, settlements),
        )
        rows = cur.fetchall() or []

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        rec = _short_interest_row(r)
        grouped[rec["symbol"]].append(rec)
    return dict(grouped)


def _short_interest_row(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {
            "symbol": str(row.get("symbol") or ""),
            "settlement_date": _date_str(row.get("settlement_date")),
            "short_interest": row.get("short_interest"),
            "avg_daily_volume": row.get("avg_daily_volume"),
            "days_to_cover": row.get("days_to_cover"),
        }
    return {
        "symbol": str(row[0] or ""),
        "settlement_date": _date_str(row[1]),
        "short_interest": row[2],
        "avg_daily_volume": row[3],
        "days_to_cover": row[4],
    }


def query_short_volume(
    conn: Any, *, symbols: list[str], trade_days: int
) -> dict[str, list[dict[str, Any]]]:
    """Recent short volume from market.stock_financials grouped by symbol.

    Field names match ``market_pg.get_short_volume_recent`` consumer contract.
    """
    if not table_exists(conn, "market", "stock_financials"):
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              UPPER(TRIM(symbol)) AS symbol,
              period_date AS trade_date,
              (data->>'short_volume')::bigint AS short_volume,
              (data->>'short_volume_ratio')::double precision AS short_volume_ratio,
              (data->>'total_volume')::bigint AS total_volume
            FROM (
              SELECT *,
                ROW_NUMBER() OVER (
                  PARTITION BY UPPER(TRIM(symbol)) ORDER BY period_date DESC
                ) AS rn
              FROM market.stock_financials
              WHERE UPPER(TRIM(symbol)) = ANY(%s)
                AND report_type = 'short_volume'
            ) sub
            WHERE rn <= %s
            ORDER BY symbol, trade_date DESC
            """,
            (symbols, trade_days),
        )
        rows = cur.fetchall() or []

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        rec = _short_volume_row(r)
        grouped[rec["symbol"]].append(rec)
    return dict(grouped)


def _short_volume_row(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {
            "symbol": str(row.get("symbol") or ""),
            "trade_date": _date_str(row.get("trade_date")),
            "short_volume": row.get("short_volume"),
            "short_volume_ratio": row.get("short_volume_ratio"),
            "total_volume": row.get("total_volume"),
        }
    return {
        "symbol": str(row[0] or ""),
        "trade_date": _date_str(row[1]),
        "short_volume": row[2],
        "short_volume_ratio": row[3],
        "total_volume": row[4],
    }


def query_financials(
    conn: Any,
    *,
    symbol: str,
    report_type: str | None,
    timeframe: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Generic financials rows from market.stock_financials for a single symbol."""
    if not table_exists(conn, "market", "stock_financials"):
        return []

    clauses = ["UPPER(TRIM(symbol)) = %s"]
    params: list[Any] = [symbol]

    if report_type:
        clauses.append("report_type = %s")
        params.append(report_type)
    if timeframe:
        clauses.append("timeframe = %s")
        params.append(timeframe)

    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT symbol, report_type, period_date, timeframe, data, fetched_at
            FROM market.stock_financials
            WHERE {" AND ".join(clauses)}
            ORDER BY period_date DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall() or []

    return [_financials_row(r) for r in rows]


def _financials_row(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {
            "symbol": str(row.get("symbol") or ""),
            "report_type": str(row.get("report_type") or ""),
            "period_date": _date_str(row.get("period_date")),
            "timeframe": row.get("timeframe"),
            "data": row.get("data"),
            "fetched_at": iso_value(row.get("fetched_at")),
        }
    return {
        "symbol": str(row[0] or ""),
        "report_type": str(row[1] or ""),
        "period_date": _date_str(row[2]),
        "timeframe": row[3],
        "data": row[4],
        "fetched_at": iso_value(row[5]),
    }


def _date_str(val: Any) -> str | None:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------


@router.get("/short-interest")
def fundamentals_db_short_interest(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    settlements: int = Query(6, ge=1, le=200, description="Number of settlement periods per symbol"),
) -> dict[str, Any]:
    """Batch short interest from local DB (market.stock_financials)."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_short_interest(conn, symbols=parsed, settlements=settlements)
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/short-volume")
def fundamentals_db_short_volume(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    trade_days: int = Query(60, ge=1, le=500, description="Number of trade days per symbol"),
) -> dict[str, Any]:
    """Batch short volume from local DB (market.stock_financials)."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_short_volume(conn, symbols=parsed, trade_days=trade_days)
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/financials")
def fundamentals_db_financials(
    symbol: str = Query(..., description="Single stock symbol"),
    report_type: str | None = Query(None, description="Filter by report_type"),
    timeframe: str | None = Query(None, description="Filter by timeframe (quarterly/annual)"),
    limit: int = Query(20, ge=1, le=500, description="Max rows to return"),
) -> dict[str, Any]:
    """Generic financials rows from local DB (market.stock_financials)."""
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="No valid symbol provided")
    if report_type and report_type not in _VALID_REPORT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid report_type: {report_type}. Valid: {sorted(_VALID_REPORT_TYPES)}",
        )
    if timeframe and timeframe not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe: {timeframe}. Valid: {sorted(_VALID_TIMEFRAMES)}",
        )
    conn = require_db()
    try:
        rows = query_financials(
            conn, symbol=sym, report_type=report_type, timeframe=timeframe, limit=limit
        )
        return {"ok": True, "rows": rows, "count": len(rows)}
    finally:
        conn.close()
