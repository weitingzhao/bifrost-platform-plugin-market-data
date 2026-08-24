"""kind=ticker_sync → market.ticker (universe or detail mode)."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import as_float, as_int, batch_upsert, parse_date, physical_table_name
from bifrost_market_data.worker.claim import JobRow

_COLS = (
    "symbol",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "instrument_type",
    "active",
    "currency",
    "cik",
    "composite_figi",
    "sic_code",
    "sector",
    "industry",
    "market_cap",
    "list_date",
    "homepage_url",
    "total_employees",
    "description",
)

# List/universe API does not return overview fields; never overwrite them on conflict.
_UNIVERSE_UPDATE_COLS = (
    "name",
    "market",
    "locale",
    "primary_exchange",
    "instrument_type",
    "active",
    "currency",
    "cik",
    "composite_figi",
)


def _row_from_list_item(item: Mapping[str, Any]) -> tuple[Any, ...] | None:
    symbol = str(item.get("ticker") or item.get("symbol") or "").strip().upper()
    if not symbol:
        return None
    currency = item.get("currency_name") or item.get("currency_symbol") or item.get("currency")
    return (
        symbol,
        item.get("name"),
        item.get("market"),
        item.get("locale"),
        item.get("primary_exchange"),
        item.get("type") or item.get("instrument_type"),
        item.get("active") if item.get("active") is not None else True,
        str(currency).strip() if currency else None,
        item.get("cik"),
        item.get("composite_figi"),
        item.get("sic_code"),
        item.get("sector") if item.get("sector") is not None else None,
        item.get("industry") if item.get("industry") is not None else None,
        as_float(item.get("market_cap")),
        parse_date(item.get("list_date")),
        item.get("homepage_url"),
        as_int(item.get("total_employees")),
        item.get("description"),
    )


def _row_from_detail(data: Mapping[str, Any]) -> tuple[Any, ...] | None:
    # Polygon ticker details wrap payload in results
    item = data.get("results") if isinstance(data.get("results"), dict) else data
    if not isinstance(item, dict):
        return None
    return _row_from_list_item(item)


async def handle_ticker_sync(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    mode = str(payload.get("mode") or "universe").strip().lower()

    if mode == "detail":
        symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
        if not symbol:
            raise ValueError("ticker_sync detail mode requires symbol")
        data = await client.fetch_ticker_details(symbol)
        row = _row_from_detail(data)
        rows = [row] if row else []
        try:
            n = batch_upsert(
                conn,
                "market.ticker",
                _COLS,
                rows,
                conflict_keys=("symbol",),
                update_cols=tuple(c for c in _COLS if c != "symbol"),
                set_fetched_at=False,
                auto_commit=False,
            )
            if rows:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {physical_table_name('market.ticker')} SET updated_at = now() WHERE symbol = %s",
                        (symbol,),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {"rows_written": n, "mode": "detail", "symbol": symbol}

    # universe mode — paginate via client (caller can pass max_pages)
    max_pages = int(payload.get("max_pages") or 100)
    market = str(payload.get("market") or "stocks")
    active = payload.get("active")
    if active is None:
        active = True
    locale = str(payload.get("locale") or "us")
    ticker_type = payload.get("ticker_type", "CS")
    if ticker_type is not None:
        ticker_type = str(ticker_type)

    data = await client.fetch_reference_tickers(
        market=market,
        active=bool(active),
        locale=locale,
        ticker_type=ticker_type,
        max_pages=max_pages,
    )
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        row = _row_from_list_item(item)
        if row:
            rows.append(row)

    try:
        n = batch_upsert(
            conn,
            "market.ticker",
            _COLS,
            rows,
            conflict_keys=("symbol",),
            update_cols=_UNIVERSE_UPDATE_COLS,
            set_fetched_at=False,
            auto_commit=False,
        )
        if rows:
            symbols = [r[0] for r in rows]
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {physical_table_name('market.ticker')} SET updated_at = now() WHERE symbol = ANY(%s)",
                    (symbols,),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "rows_written": n,
        "mode": "universe",
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
