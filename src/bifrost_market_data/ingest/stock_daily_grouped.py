"""kind=stock_daily_grouped → market.stock_daily (Polygon Grouped Daily)."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    epoch_ms_to_date,
)
from bifrost_market_data.worker.claim import JobRow

_COLS = (
    "symbol",
    "bar_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
)


async def handle_stock_daily_grouped(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Ingest full-market daily bars via Grouped Daily API.

    Payload::
        {"from": "2024-06-20", "to": "2024-06-20", "market": "stocks"}

    ``from`` is the trade date used for the Polygon path. ``to`` is accepted for
    symmetry with ``stock_daily`` but ignored (grouped endpoint is single-day).
    """
    payload = job.payload or {}
    date_str = str(payload.get("from") or payload.get("date") or "").strip()
    if not date_str:
        raise ValueError("stock_daily_grouped payload requires from (trade date)")
    market = str(payload.get("market") or "stocks").strip().lower() or "stocks"
    locale = str(payload.get("locale") or "us").strip().lower() or "us"

    data = await client.fetch_grouped_daily(date_str, locale=locale, market=market)
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for bar in results:
        if not isinstance(bar, dict) or bar.get("t") is None:
            continue
        symbol = str(bar.get("T") or "").strip().upper()
        if not symbol:
            continue
        rows.append(
            (
                symbol,
                epoch_ms_to_date(bar["t"]),
                as_float(bar.get("o")),
                as_float(bar.get("h")),
                as_float(bar.get("l")),
                as_float(bar.get("c")),
                as_int(bar.get("v")),
                as_float(bar.get("vw")),
                as_int(bar.get("n")),
            )
        )

    n = batch_upsert(
        conn,
        "market.stock_daily",
        _COLS,
        rows,
        conflict_keys=("symbol", "bar_date"),
        update_cols=("open", "high", "low", "close", "volume", "vwap", "trade_count"),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "date": date_str,
        "market": market,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages") or 1,
    }
