"""kind=stock_daily_grouped → market.stock_daily (Polygon Grouped Daily).

kind=stock_daily_unadjusted → market.stock_daily.close_unadjusted only.

``close`` is the vendor's adjusted close, restated whenever a later corporate
action reaches back over the bar. ``close_unadjusted`` is the close the session
actually printed, which is what an option listed that day was struck against:
the adjusted series folds in spin-offs as well as splits (HON 2025-10-29 reads
200.65 adjusted and 212.89 as traded; put-call parity on that day's chain puts
the underlying at 212.10), and the vendor reports splits only, so a consumer
cannot undo the adjustment from the corporate-action feed. Measured 2026-10-02.
"""

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
    "close_unadjusted",
)


def _unadjusted_closes(data: Mapping[str, Any]) -> dict[str, float | None]:
    """Symbol → the close the session printed, from an ``adjusted=false`` grouped page."""
    out: dict[str, float | None] = {}
    for bar in data.get("results") or []:
        if not isinstance(bar, dict):
            continue
        symbol = str(bar.get("T") or "").strip().upper()
        if symbol:
            out[symbol] = as_float(bar.get("c"))
    return out


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
    # Same day, as traded. A failure here fails the job and it retries: writing
    # the adjusted bars without it would null a close_unadjusted already stored.
    as_traded = _unadjusted_closes(
        await client.fetch_grouped_daily(date_str, locale=locale, market=market, adjusted=False)
    )
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
                as_traded.get(symbol),
            )
        )

    n = batch_upsert(
        conn,
        "market.stock_daily",
        _COLS,
        rows,
        conflict_keys=("symbol", "bar_date"),
        update_cols=("open", "high", "low", "close", "volume", "vwap", "trade_count", "close_unadjusted"),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "date": date_str,
        "market": market,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages") or 1,
    }


#: One statement per job: the session's rows already in the table get the close
#: they printed. Rows the table does not hold are not created — an as-traded
#: close with no bar beside it would read as a bar. ``bar_date`` is a constant so
#: only that year's partition is touched.
_SET_UNADJUSTED_SQL = """
    UPDATE raw_market.stock_daily AS s
    SET close_unadjusted = v.close_unadjusted
    FROM unnest(%s::text[], %s::double precision[]) AS v(symbol, close_unadjusted)
    WHERE s.bar_date = %s::date
      AND s.symbol = v.symbol
      AND s.close_unadjusted IS DISTINCT FROM v.close_unadjusted
"""


async def handle_stock_daily_unadjusted(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Backfill ``close_unadjusted`` for one session from the grouped ``adjusted=false`` page.

    Payload::
        {"from": "2025-10-29", "market": "stocks"}

    Leaves ``close``, the other bar columns and ``fetched_at`` as they are, so it
    can walk history without restating the adjusted series.
    """
    payload = job.payload or {}
    date_str = str(payload.get("from") or payload.get("date") or "").strip()
    if not date_str:
        raise ValueError("stock_daily_unadjusted payload requires from (trade date)")
    market = str(payload.get("market") or "stocks").strip().lower() or "stocks"
    locale = str(payload.get("locale") or "us").strip().lower() or "us"

    data = await client.fetch_grouped_daily(date_str, locale=locale, market=market, adjusted=False)
    closes = {sym: c for sym, c in _unadjusted_closes(data).items() if c is not None}
    symbols = sorted(closes)
    updated = 0
    if symbols:
        try:
            with conn.cursor() as cur:
                cur.execute(_SET_UNADJUSTED_SQL, (symbols, [closes[s] for s in symbols], date_str))
                updated = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "rows_written": updated,
        "vendor_rows": len(symbols),
        "date": date_str,
        "market": market,
        "truncated": bool(data.get("truncated")),
    }
