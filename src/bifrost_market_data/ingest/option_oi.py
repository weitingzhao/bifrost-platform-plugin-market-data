"""kind=option_open_interest → market.option_open_interest (from snapshot)."""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    daily_snapshot_anchor,
    parse_date,
    parse_option_right,
    parse_option_ticker,
)
from bifrost_market_data.worker.claim import JobRow

_COLS = (
    "option_ticker",
    "underlying",
    "expiry",
    "strike",
    "option_right",
    "trade_date",
    "open_interest",
)


async def handle_option_open_interest(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    underlying = str(payload.get("underlying") or "").strip().upper()
    if not underlying:
        raise ValueError("option_open_interest payload requires underlying")
    trade_date = parse_date(payload.get("trade_date"))
    if trade_date is None:
        trade_date = daily_snapshot_anchor().date()  # NY calendar date (tz-aware .date())

    data = await client.fetch_options_snapshot(
        underlying,
        expiration_date=payload.get("expiration_date"),
        contract_type=payload.get("contract_type"),
    )
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []

    for item in results:
        if not isinstance(item, dict):
            continue
        oi = as_int(item.get("open_interest"))
        if oi is None:
            continue
        details = item.get("details") if isinstance(item.get("details"), dict) else {}
        ticker = str(details.get("ticker") or item.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        expiry = parse_date(details.get("expiration_date"))
        strike = as_float(details.get("strike_price"))
        try:
            right = parse_option_right(details.get("contract_type"))
        except ValueError:
            right = None
        und = underlying
        ua = item.get("underlying_asset")
        if isinstance(ua, dict) and ua.get("ticker"):
            und = str(ua["ticker"]).strip().upper()

        if expiry is None or strike is None or right is None:
            try:
                parsed = parse_option_ticker(ticker)
                expiry = expiry or parsed["expiry"]
                strike = strike if strike is not None else parsed["strike"]
                right = right or parsed["option_right"]
                und = und or parsed["underlying"]
            except ValueError:
                continue
        if expiry is None or strike is None or right is None:
            continue

        rows.append((ticker, und, expiry, strike, right, trade_date, oi))

    n = batch_upsert(
        conn,
        "market.option_open_interest",
        _COLS,
        rows,
        conflict_keys=("option_ticker", "trade_date"),
        update_cols=("underlying", "expiry", "strike", "option_right", "open_interest"),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "underlying": underlying,
        "trade_date": trade_date.isoformat() if isinstance(trade_date, date) else str(trade_date),
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
