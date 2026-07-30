"""kind=splits / dividends → market.corporate_action."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import as_float, batch_upsert, parse_date
from bifrost_market_data.worker.claim import JobRow

_COLS = (
    "symbol",
    "action_type",
    "ex_date",
    "record_date",
    "payment_date",
    "ratio_from",
    "ratio_to",
    "amount",
    "currency",
    "description",
)


async def handle_splits(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("splits payload requires symbol")

    data = await client.fetch_splits(ticker=symbol)
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        ex_date = parse_date(item.get("execution_date") or item.get("ex_date"))
        if ex_date is None:
            continue
        ratio_from = as_float(item.get("split_from") or item.get("ratio_from"))
        ratio_to = as_float(item.get("split_to") or item.get("ratio_to"))
        adj = item.get("adjustment_type")
        desc = f"adjustment_type={adj}" if adj else None
        rows.append(
            (
                symbol,
                "split",
                ex_date,
                None,
                None,
                ratio_from,
                ratio_to,
                None,
                None,
                desc,
            )
        )

    n = batch_upsert(
        conn,
        "market.corporate_action",
        _COLS,
        rows,
        conflict_keys=("symbol", "action_type", "ex_date"),
        update_cols=(
            "record_date",
            "payment_date",
            "ratio_from",
            "ratio_to",
            "amount",
            "currency",
            "description",
        ),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "symbol": symbol,
        "action_type": "split",
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }


async def handle_dividends(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("dividends payload requires symbol")

    data = await client.fetch_dividends(ticker=symbol)
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        ex_date = parse_date(item.get("ex_dividend_date") or item.get("ex_date"))
        if ex_date is None:
            continue
        record_date = parse_date(item.get("record_date"))
        payment_date = parse_date(item.get("pay_date") or item.get("payment_date"))
        amount = as_float(item.get("cash_amount") or item.get("amount"))
        currency = item.get("currency")
        currency_s = str(currency).strip() if currency else None
        dtype = item.get("dividend_type") or item.get("frequency")
        desc = str(dtype) if dtype else None
        rows.append(
            (
                symbol,
                "dividend",
                ex_date,
                record_date,
                payment_date,
                None,
                None,
                amount,
                currency_s,
                desc,
            )
        )

    n = batch_upsert(
        conn,
        "market.corporate_action",
        _COLS,
        rows,
        conflict_keys=("symbol", "action_type", "ex_date"),
        update_cols=(
            "record_date",
            "payment_date",
            "ratio_from",
            "ratio_to",
            "amount",
            "currency",
            "description",
        ),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "symbol": symbol,
        "action_type": "dividend",
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
