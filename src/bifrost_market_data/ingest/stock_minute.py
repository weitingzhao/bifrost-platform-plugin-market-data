"""kind=stock_minute → market.stock_minute."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    epoch_ms_to_datetime,
    period_label,
)
from bifrost_market_data.worker.claim import JobRow

_COLS = (
    "symbol",
    "period",
    "bar_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
)


async def handle_stock_minute(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("stock_minute payload requires symbol")
    from_value = payload.get("from") or payload.get("from_value")
    to_value = payload.get("to") or payload.get("to_value")
    if from_value is None or to_value is None:
        raise ValueError("stock_minute payload requires from and to")
    multiplier = int(payload.get("multiplier") or 1)
    timespan = str(payload.get("timespan") or "minute").strip().lower()
    period = period_label(multiplier, timespan)

    data = await client.fetch_stock_aggs(
        symbol,
        from_value=from_value,
        to_value=to_value,
        multiplier=multiplier,
        timespan=timespan,
    )
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for bar in results:
        if not isinstance(bar, dict) or bar.get("t") is None:
            continue
        rows.append(
            (
                symbol,
                period,
                epoch_ms_to_datetime(bar["t"]),
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
        "market.stock_minute",
        _COLS,
        rows,
        conflict_keys=("symbol", "period", "bar_time"),
        update_cols=("open", "high", "low", "close", "volume", "vwap", "trade_count"),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "symbol": symbol,
        "period": period,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
