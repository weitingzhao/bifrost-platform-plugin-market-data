"""Stock OHLC bars batch write & delete (W0-P1).

POST /market/stocks/bars/ingest — upsert into market.stock_daily / stock_minute.
DELETE /market/stocks/bars — remove bars by symbol (+ optional period filter).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from bifrost_market_data.api.deps import require_db, require_write_token

router = APIRouter(prefix="/stocks/bars", tags=["stocks-ingest"])

DAILY_PERIOD = "1 D"
MINUTE_PERIODS = frozenset({"1 min", "5 mins", "1 hour"})
ALL_VALID_PERIODS = {DAILY_PERIOD} | MINUTE_PERIODS


class BarRow(BaseModel):
    symbol: str
    period: str
    bar_time: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class IngestRequest(BaseModel):
    rows: list[BarRow] = Field(..., min_length=1, max_length=50_000)


@router.post("/ingest", dependencies=[Depends(require_write_token)])
def ingest_bars(body: IngestRequest) -> dict[str, Any]:
    """Batch upsert OHLC bars into market.stock_daily / market.stock_minute."""
    daily_rows: list[tuple[Any, ...]] = []
    minute_rows: list[tuple[Any, ...]] = []

    for row in body.rows:
        period = row.period.strip()
        symbol = row.symbol.strip().upper()
        if not symbol:
            raise HTTPException(status_code=400, detail="empty symbol")
        if period not in ALL_VALID_PERIODS:
            raise HTTPException(
                status_code=400,
                detail=f"invalid period: {period!r}; allowed: {sorted(ALL_VALID_PERIODS)}",
            )
        if period == DAILY_PERIOD:
            daily_rows.append((symbol, row.bar_time, row.open, row.high, row.low, row.close, row.volume))
        else:
            minute_rows.append((symbol, period, row.bar_time, row.open, row.high, row.low, row.close, row.volume))

    conn = require_db()
    try:
        written = 0
        if daily_rows:
            written += _upsert_daily(conn, daily_rows)
        if minute_rows:
            written += _upsert_minute(conn, minute_rows)
        return {"ok": True, "written": written}
    finally:
        conn.close()


@router.delete("", dependencies=[Depends(require_write_token)])
def delete_bars(
    symbol: str = Query(..., description="Stock symbol"),
    delete_daily: bool = Query(False, description="Delete daily bars"),
    periods: str | None = Query(None, description="Comma-separated minute periods to delete"),
) -> dict[str, Any]:
    """Delete bars for a symbol from market.stock_daily and/or market.stock_minute."""
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="empty symbol")

    period_list: list[str] = []
    if periods:
        for p in periods.split(","):
            p = p.strip()
            if p and p in MINUTE_PERIODS:
                period_list.append(p)

    if not delete_daily and not period_list:
        raise HTTPException(
            status_code=400,
            detail="nothing to delete: set delete_daily=true or provide periods",
        )

    conn = require_db()
    try:
        deleted_daily = 0
        deleted_minute = 0
        with conn.cursor() as cur:
            if delete_daily:
                cur.execute("DELETE FROM raw_market.stock_daily WHERE symbol = %s", (sym,))
                deleted_daily = cur.rowcount
            if period_list:
                cur.execute(
                    "DELETE FROM raw_market.stock_minute WHERE symbol = %s AND period = ANY(%s)",
                    (sym, period_list),
                )
                deleted_minute = cur.rowcount
        conn.commit()
        return {"ok": True, "deleted_daily": deleted_daily, "deleted_minute": deleted_minute}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _upsert_daily(conn: Any, rows: list[tuple[Any, ...]]) -> int:
    """UPSERT into market.stock_daily with ON CONFLICT (symbol, bar_date) DO UPDATE."""
    sql = (
        "INSERT INTO raw_market.stock_daily (symbol, bar_date, open, high, low, close, volume, fetched_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, now()) "
        "ON CONFLICT (symbol, bar_date) DO UPDATE SET "
        "open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, "
        "close = EXCLUDED.close, volume = EXCLUDED.volume, fetched_at = now()"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def _upsert_minute(conn: Any, rows: list[tuple[Any, ...]]) -> int:
    """UPSERT into market.stock_minute with ON CONFLICT (symbol, period, bar_time) DO UPDATE."""
    sql = (
        "INSERT INTO raw_market.stock_minute (symbol, period, bar_time, open, high, low, close, volume, fetched_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now()) "
        "ON CONFLICT (symbol, period, bar_time) DO UPDATE SET "
        "open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, "
        "close = EXCLUDED.close, volume = EXCLUDED.volume, fetched_at = now()"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)
