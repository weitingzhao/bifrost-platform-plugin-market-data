"""kind=option_minute → market.option_minute."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    epoch_ms_to_datetime,
    parse_option_ticker,
    period_label,
)
from bifrost_market_data.worker.claim import JobRow

_COLS = (
    "option_ticker",
    "underlying",
    "expiry",
    "strike",
    "option_right",
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


async def handle_option_minute(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    option_ticker = str(payload.get("option_ticker") or "").strip().upper()
    if not option_ticker:
        raise ValueError("option_minute payload requires option_ticker")
    # The payload's underlying wins where the enqueuer supplied it. An adjusted
    # contract's root is not its underlying — O:BDX1260918C00085000 belongs to
    # BDX, and option_contract says so — while parse_option_ticker can only
    # report what the ticker spells. option_snapshot has always stored the
    # request's underlying for the same reason.
    parsed = dict(parse_option_ticker(option_ticker))
    payload_und = str(payload.get("underlying") or "").strip().upper()
    if payload_und:
        parsed["underlying"] = payload_und
    from_value = payload.get("from") or payload.get("from_value")
    to_value = payload.get("to") or payload.get("to_value")
    if from_value is None or to_value is None:
        raise ValueError("option_minute payload requires from and to")
    multiplier = int(payload.get("multiplier") or 1)
    timespan = str(payload.get("timespan") or "minute").strip().lower()
    period = period_label(multiplier, timespan)

    data = await client.fetch_stock_aggs(
        option_ticker,
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
                parsed["option_ticker"],
                parsed["underlying"],
                parsed["expiry"],
                parsed["strike"],
                parsed["option_right"],
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
        "market.option_minute",
        _COLS,
        rows,
        conflict_keys=("option_ticker", "period", "bar_time"),
        update_cols=(
            "underlying",
            "expiry",
            "strike",
            "option_right",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
        ),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "option_ticker": parsed["option_ticker"],
        "underlying": parsed["underlying"],
        "period": period,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
