"""kind=stock_snapshot → market.stock_snapshot (Polygon stock snapshot)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from bifrost_market_data.ingest._upsert import as_float, as_int, batch_upsert
from bifrost_market_data.worker.claim import JobRow

_NY = ZoneInfo("America/New_York")

_COLS = (
    "symbol",
    "session_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "prev_close",
    "change",
    "change_pct",
)


def _session_date(now: datetime | None = None) -> date:
    """NY calendar date for the snapshot session day."""
    base = now if now is not None else datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    return base.astimezone(_NY).date()


def _ticker_items(data: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Normalize all / single snapshot payloads into a list of ticker dicts."""
    tickers = data.get("tickers")
    if isinstance(tickers, list):
        return [t for t in tickers if isinstance(t, dict)]
    single = data.get("ticker")
    if isinstance(single, dict):
        return [single]
    return []


def _row_from_ticker(item: Mapping[str, Any], session_date: date) -> tuple[Any, ...] | None:
    symbol = str(item.get("ticker") or item.get("symbol") or "").strip().upper()
    if not symbol:
        return None
    day = item.get("day") if isinstance(item.get("day"), dict) else {}
    prev = item.get("prevDay") if isinstance(item.get("prevDay"), dict) else {}
    # Polygon uses short keys on day/prevDay (o/h/l/c/v/vw); also accept long names.
    open_ = as_float(day.get("o") if "o" in day else day.get("open"))
    high = as_float(day.get("h") if "h" in day else day.get("high"))
    low = as_float(day.get("l") if "l" in day else day.get("low"))
    close = as_float(day.get("c") if "c" in day else day.get("close"))
    volume = as_int(day.get("v") if "v" in day else day.get("volume"))
    vwap = as_float(day.get("vw") if "vw" in day else day.get("vwap"))
    prev_close = as_float(prev.get("c") if "c" in prev else prev.get("close"))
    change = as_float(item.get("todaysChange"))
    change_pct = as_float(item.get("todaysChangePerc"))
    return (
        symbol,
        session_date,
        open_,
        high,
        low,
        close,
        volume,
        vwap,
        prev_close,
        change,
        change_pct,
    )


async def handle_stock_snapshot(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Fetch full-market or single-ticker snapshot and upsert daily rows.

    Payload:
    - ``{}`` / ``{"mode": "all"}`` — All Tickers Snapshot (scheduled default)
    - ``{"symbol": "AAPL"}`` or ``{"mode": "single", "symbol": "AAPL"}`` — single ticker
    """
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    mode = str(payload.get("mode") or "").strip().lower()
    include_otc = bool(payload.get("include_otc"))
    session_date = _session_date()
    if payload.get("session_date"):
        session_date = date.fromisoformat(str(payload["session_date"]).strip()[:10])

    if symbol and mode != "all":
        data = await client.fetch_stock_snapshot_single(symbol)
        mode_used = "single"
    else:
        data = await client.fetch_stock_snapshot_all(include_otc=include_otc)
        mode_used = "all"

    rows: list[tuple[Any, ...]] = []
    for item in _ticker_items(data):
        row = _row_from_ticker(item, session_date)
        if row is not None:
            rows.append(row)

    n = batch_upsert(
        conn,
        "market.stock_snapshot",
        _COLS,
        rows,
        conflict_keys=("symbol", "session_date"),
        update_cols=(
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "prev_close",
            "change",
            "change_pct",
        ),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "mode": mode_used,
        "session_date": session_date.isoformat(),
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
