"""kind=splits / dividends (per symbol) and splits_market / dividends_market
(whole market by date window) → market.corporate_action.

A row is one distribution as the vendor classifies it, not one ex-date: the key is
``schema.corporate_action_identity.IDENTITY``, which lets a special dividend sit
beside the regular one it shares a day with and folds only the vendor's duplicate
records. Because a corrected amount or a relabelled type is a *new* key, the two
dividend handlers finish by deleting the rows a complete fetch no longer lists.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from bifrost_market_data.ingest._upsert import as_float, as_int, batch_upsert, parse_date
from bifrost_market_data.schema.corporate_action_identity import IDENTITY
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
    "distribution_type",
    "frequency",
)

_UPDATE_COLS = tuple(c for c in _COLS if c not in IDENTITY)

# ``now()`` is the transaction's start, and the upsert stamps every row it wrote or
# matched with it — so inside the same transaction, an older ``fetched_at`` is
# exactly "this fetch did not list it".
_DROP_UNLISTED_FOR_SYMBOL = """
    DELETE FROM raw_market.corporate_action
    WHERE symbol = %s AND action_type = 'dividend'
      AND (fetched_at IS NULL OR fetched_at < now())
"""
_DROP_UNLISTED_IN_WINDOW = """
    DELETE FROM raw_market.corporate_action
    WHERE action_type = 'dividend' AND ex_date BETWEEN %s AND %s
      AND (fetched_at IS NULL OR fetched_at < now())
"""


def _split_row(symbol: str, item: Mapping[str, Any]) -> tuple[Any, ...] | None:
    ex_date = parse_date(item.get("execution_date") or item.get("ex_date"))
    if ex_date is None:
        return None
    adj = item.get("adjustment_type")
    return (
        symbol,
        "split",
        ex_date,
        None,
        None,
        as_float(item.get("split_from") or item.get("ratio_from")),
        as_float(item.get("split_to") or item.get("ratio_to")),
        None,
        None,
        f"adjustment_type={adj}" if adj else None,
        None,
        None,
    )


def _dividend_row(symbol: str, item: Mapping[str, Any]) -> tuple[Any, ...] | None:
    ex_date = parse_date(item.get("ex_dividend_date") or item.get("ex_date"))
    if ex_date is None:
        return None
    currency = item.get("currency")
    # ``description`` has carried the frequency since the v1 endpoint dropped
    # ``dividend_type``; kept as it was for readers that already show it.
    dtype = item.get("dividend_type") or item.get("frequency")
    distribution = str(item.get("distribution_type") or "").strip().lower()
    return (
        symbol,
        "dividend",
        ex_date,
        parse_date(item.get("record_date")),
        parse_date(item.get("pay_date") or item.get("payment_date")),
        None,
        None,
        as_float(item.get("cash_amount") or item.get("amount")),
        str(currency).strip() if currency else None,
        str(dtype) if dtype else None,
        distribution or None,
        as_int(item.get("frequency")),
    )


def _upsert(conn: Any, rows: Sequence[tuple[Any, ...]], *, auto_commit: bool = True) -> int:
    return batch_upsert(
        conn,
        "market.corporate_action",
        _COLS,
        rows,
        conflict_keys=IDENTITY,
        update_cols=_UPDATE_COLS,
        set_fetched_at=True,
        auto_commit=auto_commit,
    )


def _replace_dividends(
    conn: Any,
    rows: Sequence[tuple[Any, ...]],
    data: Mapping[str, Any],
    drop_sql: str,
    drop_params: tuple[Any, ...],
) -> tuple[int, int]:
    """Upsert, then delete what the fetch did not list — one transaction.

    Only a complete, non-empty answer is allowed to delete: a truncated page walk
    lists part of the truth, and an empty one is more likely a vendor hiccup than a
    symbol whose whole dividend history was withdrawn.
    """
    if not rows:
        return 0, 0
    try:
        written = _upsert(conn, rows, auto_commit=False)
        removed = 0
        if not data.get("truncated"):
            with conn.cursor() as cur:
                cur.execute(drop_sql, drop_params)
                removed = max(int(getattr(cur, "rowcount", 0) or 0), 0)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return written, removed


async def handle_splits(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("splits payload requires symbol")

    data = await client.fetch_splits(ticker=symbol)
    rows: list[tuple[Any, ...]] = []
    for item in data.get("results") or []:
        row = _split_row(symbol, item) if isinstance(item, dict) else None
        if row is not None:
            rows.append(row)
    n = _upsert(conn, rows)
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
    rows: list[tuple[Any, ...]] = []
    for item in data.get("results") or []:
        row = _dividend_row(symbol, item) if isinstance(item, dict) else None
        if row is not None:
            rows.append(row)
    n, removed = _replace_dividends(conn, rows, data, _DROP_UNLISTED_FOR_SYMBOL, (symbol,))
    return {
        "rows_written": n,
        "rows_removed": removed,
        "symbol": symbol,
        "action_type": "dividend",
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }


def _market_window(payload: Mapping[str, Any]) -> tuple[str, str]:
    start = str(payload.get("from") or "").strip()
    end = str(payload.get("to") or "").strip()
    if not start or not end:
        raise ValueError("market corporate-action payload requires from and to")
    return start, end


async def handle_splits_market(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Every split with an execution date in ``[from, to]`` — the whole market in a few pages."""
    start, end = _market_window(job.payload or {})
    data = await client.fetch_splits_market(start, end)
    rows: list[tuple[Any, ...]] = []
    for item in data.get("results") or []:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("ticker") or item.get("symbol") or "").strip().upper()
        row = _split_row(symbol, item) if symbol else None
        if row is not None:
            rows.append(row)
    n = _upsert(conn, rows)
    return {"rows_written": n, "action_type": "split", "from": start, "to": end, "pages": data.get("pages")}


async def handle_dividends_market(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Every dividend with an ex-date in ``[from, to]``."""
    start, end = _market_window(job.payload or {})
    data = await client.fetch_dividends_market(start, end)
    rows: list[tuple[Any, ...]] = []
    for item in data.get("results") or []:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("ticker") or item.get("symbol") or "").strip().upper()
        row = _dividend_row(symbol, item) if symbol else None
        if row is not None:
            rows.append(row)
    window = (date.fromisoformat(start[:10]), date.fromisoformat(end[:10]))
    n, removed = _replace_dividends(conn, rows, data, _DROP_UNLISTED_IN_WINDOW, window)
    return {
        "rows_written": n,
        "rows_removed": removed,
        "action_type": "dividend",
        "from": start,
        "to": end,
        "pages": data.get("pages"),
    }
