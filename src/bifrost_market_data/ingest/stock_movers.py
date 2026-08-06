"""kind=stock_movers → market.stock_movers (Polygon gainers / losers snapshot)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from bifrost_market_data.ingest._upsert import as_float, as_int, batch_upsert
from bifrost_market_data.worker.claim import JobRow

_NY = ZoneInfo("America/New_York")
_VALID_DIRECTIONS = frozenset({"gainers", "losers"})

_COLS = (
    "direction",
    "symbol",
    "session_date",
    "change_pct",
    "price",
    "volume",
)


def _session_date(now: datetime | None = None) -> date:
    base = now if now is not None else datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    return base.astimezone(_NY).date()


def _normalize_directions(payload: Mapping[str, Any]) -> list[str]:
    raw = payload.get("direction") or payload.get("directions")
    if raw is None:
        return ["gainers", "losers"]
    if isinstance(raw, (list, tuple)):
        dirs = [str(d).strip().lower() for d in raw if str(d).strip()]
    else:
        s = str(raw).strip().lower()
        if s in ("both", "all"):
            dirs = ["gainers", "losers"]
        else:
            dirs = [s]
    out: list[str] = []
    for d in dirs:
        if d not in _VALID_DIRECTIONS:
            raise ValueError(f"stock_movers direction must be gainers|losers, got {d!r}")
        if d not in out:
            out.append(d)
    if not out:
        raise ValueError("stock_movers payload requires direction gainers|losers")
    return out


def _row_from_ticker(
    direction: str,
    item: Mapping[str, Any],
    session_date: date,
) -> tuple[Any, ...] | None:
    symbol = str(item.get("ticker") or item.get("symbol") or "").strip().upper()
    if not symbol:
        return None
    day = item.get("day") if isinstance(item.get("day"), dict) else {}
    price = as_float(day.get("c") if "c" in day else day.get("close"))
    if price is None:
        price = as_float(item.get("value") or item.get("price"))
    volume = as_int(day.get("v") if "v" in day else day.get("volume"))
    change_pct = as_float(item.get("todaysChangePerc"))
    return (direction, symbol, session_date, change_pct, price, volume)


async def handle_stock_movers(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Fetch gainers and/or losers and upsert into ``market.stock_movers``.

    Payload:
    - ``{}`` / ``{"direction": "both"}`` — both gainers and losers
    - ``{"direction": "gainers"}`` or ``{"direction": "losers"}``
    - ``{"directions": ["gainers", "losers"]}``
    """
    payload = job.payload or {}
    directions = _normalize_directions(payload)
    include_otc = bool(payload.get("include_otc"))
    session_date = _session_date()
    if payload.get("session_date"):
        session_date = date.fromisoformat(str(payload["session_date"]).strip()[:10])

    rows: list[tuple[Any, ...]] = []
    pages = 0
    truncated = False
    for direction in directions:
        data = await client.fetch_stock_gainers_losers(
            direction, include_otc=include_otc
        )
        pages += int(data.get("pages") or 1)
        truncated = truncated or bool(data.get("truncated"))
        tickers = data.get("tickers") or data.get("results") or []
        for item in tickers:
            if not isinstance(item, dict):
                continue
            row = _row_from_ticker(direction, item, session_date)
            if row is not None:
                rows.append(row)

    n = batch_upsert(
        conn,
        "market.stock_movers",
        _COLS,
        rows,
        conflict_keys=("direction", "symbol", "session_date"),
        update_cols=("change_pct", "price", "volume"),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "directions": directions,
        "session_date": session_date.isoformat(),
        "truncated": truncated,
        "pages": pages,
    }
