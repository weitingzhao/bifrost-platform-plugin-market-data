"""PCR (Put/Call Ratio) aggregate endpoint — reads from option_snapshot + option_open_interest.

Provides the same data as bifrost-trade-api's stock_option_pcr.py but via Plugin API.
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


def _safe_ratio(num: float, den: float) -> float | None:
    if den <= 0 or num < 0:
        return None
    return round(num / den, 3)


def _serialize_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()
    return s[:10] if len(s) >= 10 else (s or None)


def query_pcr_aggregate(
    conn: Any,
    *,
    symbol: str,
    pcr_type: str = "oi",
    lookback_days: int = 365,
) -> dict[str, Any]:
    """Aggregate OI or volume PCR trend from option_open_interest + option_snapshot."""
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}

    lb = max(30, min(int(lookback_days), 400))

    trend: list[dict[str, Any]] = []

    if pcr_type == "oi":
        if table_exists(conn, "market", "option_open_interest"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_date::date AS trade_date,
                           SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('P', 'PUT')
                               THEN COALESCE(open_interest, 0) ELSE 0 END)::bigint AS put_oi,
                           SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('C', 'CALL')
                               THEN COALESCE(open_interest, 0) ELSE 0 END)::bigint AS call_oi
                    FROM market.option_open_interest
                    WHERE underlying = %s
                      AND trade_date >= (CURRENT_DATE - %s)
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                    """,
                    (sym, lb),
                )
                for row in cur.fetchall() or []:
                    td_key = _serialize_date(row[0])
                    if not td_key:
                        continue
                    put_oi = int(row[1] or 0)
                    call_oi = int(row[2] or 0)
                    trend.append({
                        "trade_date": td_key,
                        "put_value": put_oi,
                        "call_value": call_oi,
                        "ratio": _safe_ratio(float(put_oi), float(call_oi)),
                    })

        if not trend and table_exists(conn, "market", "option_snapshot"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH snap AS (
                      SELECT DISTINCT ON (oc.option_ticker, DATE(timezone('America/New_York', os.snapshot_ts)))
                        oc.option_right,
                        COALESCE(os.open_interest, 0)::bigint AS open_interest,
                        DATE(timezone('America/New_York', os.snapshot_ts)) AS trade_date
                      FROM market.option_contract oc
                      INNER JOIN market.option_snapshot os
                        ON os.option_ticker = oc.option_ticker
                      WHERE UPPER(TRIM(oc.underlying)) = %s
                        AND os.snapshot_ts >= (CURRENT_DATE - %s)
                      ORDER BY oc.option_ticker,
                               DATE(timezone('America/New_York', os.snapshot_ts)),
                               os.snapshot_ts DESC
                    )
                    SELECT trade_date,
                           SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('P', 'PUT')
                               THEN open_interest ELSE 0 END)::bigint AS put_oi,
                           SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('C', 'CALL')
                               THEN open_interest ELSE 0 END)::bigint AS call_oi
                    FROM snap
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                    """,
                    (sym, lb),
                )
                for row in cur.fetchall() or []:
                    td_key = _serialize_date(row[0])
                    if not td_key:
                        continue
                    put_oi = int(row[1] or 0)
                    call_oi = int(row[2] or 0)
                    trend.append({
                        "trade_date": td_key,
                        "put_value": put_oi,
                        "call_value": call_oi,
                        "ratio": _safe_ratio(float(put_oi), float(call_oi)),
                    })

    elif pcr_type == "volume":
        if table_exists(conn, "market", "option_snapshot"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH snap AS (
                      SELECT DISTINCT ON (oc.option_ticker, DATE(timezone('America/New_York', os.snapshot_ts)))
                        oc.option_right,
                        COALESCE(os.day_volume, 0)::bigint AS day_volume,
                        DATE(timezone('America/New_York', os.snapshot_ts)) AS trade_date
                      FROM market.option_contract oc
                      INNER JOIN market.option_snapshot os
                        ON os.option_ticker = oc.option_ticker
                      WHERE UPPER(TRIM(oc.underlying)) = %s
                        AND os.snapshot_ts >= (CURRENT_DATE - %s)
                      ORDER BY oc.option_ticker,
                               DATE(timezone('America/New_York', os.snapshot_ts)),
                               os.snapshot_ts DESC
                    )
                    SELECT trade_date,
                           SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('P', 'PUT')
                               THEN day_volume ELSE 0 END)::bigint AS put_vol,
                           SUM(CASE WHEN UPPER(TRIM(option_right)) IN ('C', 'CALL')
                               THEN day_volume ELSE 0 END)::bigint AS call_vol
                    FROM snap
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                    """,
                    (sym, lb),
                )
                for row in cur.fetchall() or []:
                    td_key = _serialize_date(row[0])
                    if not td_key:
                        continue
                    put_vol = int(row[1] or 0)
                    call_vol = int(row[2] or 0)
                    trend.append({
                        "trade_date": td_key,
                        "put_value": put_vol,
                        "call_value": call_vol,
                        "ratio": _safe_ratio(float(put_vol), float(call_vol)),
                    })

    latest_ratio = trend[-1]["ratio"] if trend else None
    return {
        "ok": True,
        "symbol": sym,
        "type": pcr_type,
        "lookback_days": lb,
        "count": len(trend),
        "latest_ratio": latest_ratio,
        "trend": trend,
    }


@router.get("/pcr")
def options_analytics_pcr(
    symbol: str = Query(..., description="Underlying symbol (e.g. NVDA)"),
    type: str = Query("oi", description="PCR type: 'oi' or 'volume'"),
    lookback_days: int = Query(365, ge=30, le=400),
) -> dict[str, Any]:
    """Put/Call Ratio aggregate trend by OI or volume."""
    pcr_type = type.strip().lower()
    if pcr_type not in ("oi", "volume"):
        return {"ok": False, "error": "type must be 'oi' or 'volume'"}
    conn = require_db()
    try:
        return query_pcr_aggregate(conn, symbol=symbol, pcr_type=pcr_type, lookback_days=lookback_days)
    finally:
        conn.close()
