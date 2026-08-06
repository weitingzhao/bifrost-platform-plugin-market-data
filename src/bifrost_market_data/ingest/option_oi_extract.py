"""Extract daily OI from market.option_snapshot → market.option_open_interest.

D4=B: last snapshot of each NY calendar day (MAX snapshot_ts per option_ticker + NY date).
D5=A: ON CONFLICT DO NOTHING — never overwrite live ingest rows; extract only fills gaps.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping, Sequence

from bifrost_market_data.ingest._upsert import batch_upsert, parse_option_ticker

_COLS = (
    "option_ticker",
    "underlying",
    "expiry",
    "strike",
    "option_right",
    "trade_date",
    "open_interest",
)

_SELECT_SQL = """
SELECT
  s.option_ticker,
  s.underlying,
  s.open_interest,
  s.trade_date,
  c.expiry AS contract_expiry,
  c.strike AS contract_strike,
  c.option_right AS contract_right,
  c.underlying AS contract_underlying
FROM (
  SELECT DISTINCT ON (
    option_ticker,
    (snapshot_ts AT TIME ZONE 'America/New_York')::date
  )
    option_ticker,
    underlying,
    open_interest,
    (snapshot_ts AT TIME ZONE 'America/New_York')::date AS trade_date
  FROM market.option_snapshot
  WHERE open_interest IS NOT NULL
    AND (snapshot_ts AT TIME ZONE 'America/New_York')::date >= %s
    AND (snapshot_ts AT TIME ZONE 'America/New_York')::date <= %s
  ORDER BY
    option_ticker,
    (snapshot_ts AT TIME ZONE 'America/New_York')::date,
    snapshot_ts DESC
) s
LEFT JOIN market.option_contract c ON c.option_ticker = s.option_ticker
""".strip()

_SELECT_SQL_FILTERED = """
SELECT
  s.option_ticker,
  s.underlying,
  s.open_interest,
  s.trade_date,
  c.expiry AS contract_expiry,
  c.strike AS contract_strike,
  c.option_right AS contract_right,
  c.underlying AS contract_underlying
FROM (
  SELECT DISTINCT ON (
    option_ticker,
    (snapshot_ts AT TIME ZONE 'America/New_York')::date
  )
    option_ticker,
    underlying,
    open_interest,
    (snapshot_ts AT TIME ZONE 'America/New_York')::date AS trade_date
  FROM market.option_snapshot
  WHERE open_interest IS NOT NULL
    AND (snapshot_ts AT TIME ZONE 'America/New_York')::date >= %s
    AND (snapshot_ts AT TIME ZONE 'America/New_York')::date <= %s
    AND underlying = ANY(%s)
  ORDER BY
    option_ticker,
    (snapshot_ts AT TIME ZONE 'America/New_York')::date,
    snapshot_ts DESC
) s
LEFT JOIN market.option_contract c ON c.option_ticker = s.option_ticker
""".strip()


def _row_value(row: Any, key: str, idx: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[idx] if row and len(row) > idx else None


def extract_oi_from_snapshots(
    conn: Any,
    *,
    underlyings: Sequence[str] | None,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    """Gap-fill ``market.option_open_interest`` from snapshot history (DB-to-DB).

    For each ``(option_ticker, NY session date)`` in ``[from_date, to_date]``,
    takes the row with ``MAX(snapshot_ts)`` where ``open_interest IS NOT NULL``.
    Upserts with ``ON CONFLICT DO NOTHING`` so live ingest rows are never overwritten.
    """
    if to_date < from_date:
        raise ValueError(f"to_date {to_date} < from_date {from_date}")

    syms = sorted({str(s).strip().upper() for s in (underlyings or []) if str(s).strip()})
    if syms:
        sql = _SELECT_SQL_FILTERED
        params: tuple[Any, ...] = (from_date, to_date, syms)
    else:
        sql = _SELECT_SQL
        params = (from_date, to_date)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        raw_rows = cur.fetchall() if hasattr(cur, "fetchall") else []

    rows: list[tuple[Any, ...]] = []
    skipped = 0
    for raw in raw_rows or []:
        ticker = str(_row_value(raw, "option_ticker", 0) or "").strip().upper()
        und = str(_row_value(raw, "underlying", 1) or "").strip().upper()
        oi = _row_value(raw, "open_interest", 2)
        trade_date = _row_value(raw, "trade_date", 3)
        expiry = _row_value(raw, "contract_expiry", 4)
        strike = _row_value(raw, "contract_strike", 5)
        right = _row_value(raw, "contract_right", 6)
        contract_und = _row_value(raw, "contract_underlying", 7)

        if not ticker or oi is None or trade_date is None:
            skipped += 1
            continue
        if isinstance(trade_date, datetime):
            trade_date = trade_date.date()
        elif not isinstance(trade_date, date):
            trade_date = date.fromisoformat(str(trade_date)[:10])

        if contract_und:
            und = str(contract_und).strip().upper() or und

        if expiry is None or strike is None or right is None:
            try:
                parsed = parse_option_ticker(ticker)
                expiry = expiry or parsed["expiry"]
                strike = float(strike) if strike is not None else parsed["strike"]
                right = right or parsed["option_right"]
                und = und or parsed["underlying"]
            except ValueError:
                skipped += 1
                continue

        right_s = str(right).strip().upper()[:1]
        if right_s not in ("C", "P"):
            skipped += 1
            continue

        rows.append(
            (
                ticker,
                und,
                expiry if isinstance(expiry, date) else date.fromisoformat(str(expiry)[:10]),
                float(strike),
                right_s,
                trade_date,
                int(oi),
            )
        )

    # D5=A: empty update_cols + set_fetched_at=False → ON CONFLICT DO NOTHING
    n = batch_upsert(
        conn,
        "market.option_open_interest",
        _COLS,
        rows,
        conflict_keys=("option_ticker", "trade_date"),
        update_cols=(),
        set_fetched_at=False,
    )
    return {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "underlyings": len(syms),
        "candidates": len(rows),
        "rows_attempted": n,
        "skipped": skipped,
    }
