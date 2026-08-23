"""kind=option_trades → market.option_trades (Polygon v3 trades REST, paginated)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    epoch_ns_to_datetime,
    parse_option_ticker,
)
from bifrost_market_data.worker.claim import JobRow

_NY = ZoneInfo("America/New_York")

_COLS = (
    "option_ticker",
    "underlying",
    "expiry",
    "strike",
    "option_right",
    "trade_date",
    "sip_ts",
    "sequence_number",
    "price",
    "size",
    "exchange",
    "conditions",
    "correction",
    "participant_ts",
)


def _as_conditions(value: Any) -> list[int] | None:
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        out: list[int] = []
        for item in value:
            n = as_int(item)
            if n is not None:
                out.append(n)
        return out
    return None


def _ny_trade_date(sip_ts: datetime) -> date:
    if sip_ts.tzinfo is None:
        sip_ts = sip_ts.replace(tzinfo=timezone.utc)
    return sip_ts.astimezone(_NY).date()


async def handle_option_trades(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    option_ticker = str(payload.get("option_ticker") or "").strip().upper()
    if not option_ticker:
        raise ValueError("option_trades payload requires option_ticker")
    parsed = parse_option_ticker(option_ticker)
    from_value = payload.get("from") or payload.get("from_value") or payload.get("trade_date")
    to_value = payload.get("to") or payload.get("to_value") or from_value
    if from_value is None or to_value is None:
        raise ValueError("option_trades payload requires from and to (or trade_date)")

    day_from = str(from_value).strip()[:10]
    day_to = str(to_value).strip()[:10]
    limit = int(payload.get("limit") or 50_000)
    max_pages = int(payload.get("max_pages") or 50)

    data = await client.fetch_option_trades(
        option_ticker,
        timestamp_gte=f"{day_from}T00:00:00-04:00",
        timestamp_lte=f"{day_to}T23:59:59.999999999-04:00",
        limit=limit,
        sort="timestamp",
        order="asc",
        max_pages=max_pages,
    )
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for trade in results:
        if not isinstance(trade, dict):
            continue
        sip_raw = trade.get("sip_timestamp")
        seq = as_int(trade.get("sequence_number"))
        if sip_raw is None or seq is None:
            continue
        try:
            sip_ts = epoch_ns_to_datetime(sip_raw)
        except (TypeError, ValueError, OSError):
            continue
        trade_date = _ny_trade_date(sip_ts)
        participant_ts = None
        pt_raw = trade.get("participant_timestamp")
        if pt_raw is not None:
            try:
                participant_ts = epoch_ns_to_datetime(pt_raw)
            except (TypeError, ValueError, OSError):
                participant_ts = None
        rows.append(
            (
                parsed["option_ticker"],
                parsed["underlying"],
                parsed["expiry"],
                parsed["strike"],
                parsed["option_right"],
                trade_date,
                sip_ts,
                seq,
                as_float(trade.get("price")),
                as_int(trade.get("size")),
                as_int(trade.get("exchange")),
                _as_conditions(trade.get("conditions")),
                as_int(trade.get("correction")),
                participant_ts,
            )
        )

    n = batch_upsert(
        conn,
        "market.option_trades",
        _COLS,
        rows,
        conflict_keys=("option_ticker", "trade_date", "sip_ts", "sequence_number"),
        update_cols=(
            "underlying",
            "expiry",
            "strike",
            "option_right",
            "price",
            "size",
            "exchange",
            "conditions",
            "correction",
            "participant_ts",
        ),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "option_ticker": parsed["option_ticker"],
        "underlying": parsed["underlying"],
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
        "results_fetched": len(results),
    }
