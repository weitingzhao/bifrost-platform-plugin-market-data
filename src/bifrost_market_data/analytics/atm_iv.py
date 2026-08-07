"""ATM IV daily compute: market.v_option_snapshot_with_stock → market_analytics.atm_iv_daily.

Algorithm independently reimplemented from bifrost_api.research.iv_atm
(no bifrost-core / trade-api pip dependency).

D10=A: use ``market.v_option_snapshot_with_stock`` (underlying_price JOIN already).
Black-box: ``iv`` is Polygon precomputed — we select ATM strike but cannot verify the IV model.

Per (symbol, expiry): nearest strikes to spot; avg call+put IV when both exist; iv in (0, 10).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from statistics import median
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from bifrost_market_data.ingest._upsert import batch_upsert

_COLS = (
    "symbol",
    "trade_date",
    "expiry",
    "atm_strike",
    "atm_iv",
    "underlying_price",
    "iv_source",
    "computed_at",
)

IV_SOURCE = "snapshot"


def _row_to_dict(row: Any, columns: Sequence[str]) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return {columns[i]: row[i] for i in range(min(len(columns), len(row)))}


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()[:10]
    if not s:
        return None
    return date.fromisoformat(s)


def _valid_iv(value: Any) -> float | None:
    if value is None:
        return None
    try:
        iv_f = float(value)
    except (TypeError, ValueError):
        return None
    if not (0.0 < iv_f < 10.0):
        return None
    return iv_f


def atm_iv_from_side_items(
    items: List[Tuple[float, Optional[float], Optional[float], float]],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Return (atm_iv, iv_call, iv_put, best_strike) using nearest strikes with IV.

    items: (distance_to_spot, iv_call, iv_put, strike) — one side populated per entry.
    """
    if not items:
        return None, None, None, None
    items_sorted = sorted(items, key=lambda x: x[0])
    best_call: Optional[float] = None
    best_put: Optional[float] = None
    best_strike: Optional[float] = None
    for _dist, iv_c, iv_p, st in items_sorted:
        if iv_c is not None and best_call is None:
            best_call = iv_c
            if best_strike is None:
                best_strike = st
        if iv_p is not None and best_put is None:
            best_put = iv_p
            if best_strike is None:
                best_strike = st
        if best_call is not None and best_put is not None:
            break

    atm_iv: Optional[float] = None
    if best_call is not None and best_put is not None:
        atm_iv = (best_call + best_put) / 2.0
    elif best_call is not None:
        atm_iv = best_call
    elif best_put is not None:
        atm_iv = best_put
    return atm_iv, best_call, best_put, best_strike


def build_expiry_side_items(
    rows: Sequence[Mapping[str, Any]],
    spot: float,
) -> List[Tuple[float, Optional[float], Optional[float], float]]:
    """Build (dist, iv_call, iv_put, strike) list from snapshot+contract rows for one expiry."""
    items: List[Tuple[float, Optional[float], Optional[float], float]] = []
    for r in rows:
        try:
            strike = float(r.get("strike"))
        except (TypeError, ValueError):
            continue
        if strike <= 0:
            continue
        iv_f = _valid_iv(r.get("iv"))
        if iv_f is None:
            continue
        right = str(r.get("option_right") or "").strip().upper()
        dist = abs(strike - spot)
        if right in ("C", "CALL"):
            items.append((dist, iv_f, None, strike))
        elif right in ("P", "PUT"):
            items.append((dist, None, iv_f, strike))
    return items


def representative_spot(rows: Sequence[Mapping[str, Any]]) -> float | None:
    """Median positive underlying_price across rows."""
    spots: list[float] = []
    for r in rows:
        up = r.get("underlying_price")
        if up is None:
            continue
        try:
            v = float(up)
        except (TypeError, ValueError):
            continue
        if v > 0:
            spots.append(v)
    if not spots:
        return None
    return float(median(spots))


def fetch_snapshot_iv_rows_for_date(
    conn: Any,
    trade_date: date,
    *,
    underlyings: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Last snapshot of NY day per option_ticker with contract strike/expiry/right (D10=A)."""
    cols = (
        "option_ticker",
        "underlying",
        "iv",
        "underlying_price",
        "expiry",
        "strike",
        "option_right",
    )
    syms = [str(s).strip().upper() for s in (underlyings or []) if str(s).strip()]
    base_sql = """
        SELECT DISTINCT ON (v.option_ticker)
          v.option_ticker,
          v.underlying,
          v.iv,
          v.underlying_price,
          oc.expiry,
          oc.strike,
          oc.option_right
        FROM market.v_option_snapshot_with_stock v
        INNER JOIN market.option_contract oc
          ON oc.option_ticker = v.option_ticker
        WHERE DATE(timezone('America/New_York', v.snapshot_ts)) = %s
          AND v.iv IS NOT NULL
          AND v.underlying_price IS NOT NULL
    """
    with conn.cursor() as cur:
        if syms:
            cur.execute(
                base_sql
                + """
                  AND UPPER(TRIM(v.underlying)) = ANY(%s)
                ORDER BY v.option_ticker, v.snapshot_ts DESC
                """,
                (trade_date, syms),
            )
        else:
            cur.execute(
                base_sql
                + """
                ORDER BY v.option_ticker, v.snapshot_ts DESC
                """,
                (trade_date,),
            )
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [_row_to_dict(r, cols) for r in (raw or [])]


def compute_atm_iv_for_date(
    conn: Any,
    *,
    trade_date: date,
    underlyings: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Compute ATM IV for all (underlying, expiry) with snapshots on ``trade_date`` and upsert."""
    snap_rows = fetch_snapshot_iv_rows_for_date(conn, trade_date, underlyings=underlyings)
    if not snap_rows:
        return {
            "trade_date": trade_date.isoformat(),
            "groups": 0,
            "rows_written": 0,
            "symbols": 0,
        }

    groups: dict[tuple[str, date], list[dict[str, Any]]] = {}
    for r in snap_rows:
        und = str(r.get("underlying") or "").strip().upper()
        exp = _as_date(r.get("expiry"))
        if not und or exp is None:
            continue
        groups.setdefault((und, exp), []).append(r)

    now = datetime.now(timezone.utc)
    upsert_rows: list[tuple[Any, ...]] = []
    for (symbol, expiry), rows in sorted(groups.items()):
        spot = representative_spot(rows)
        if spot is None:
            continue
        items = build_expiry_side_items(rows, spot)
        atm_iv, _iv_c, _iv_p, best_strike = atm_iv_from_side_items(items)
        if atm_iv is None or best_strike is None:
            continue
        upsert_rows.append(
            (
                symbol,
                trade_date,
                expiry,
                float(best_strike),
                float(atm_iv),
                float(spot),
                IV_SOURCE,
                now,
            )
        )

    n = batch_upsert(
        conn,
        "market_analytics.atm_iv_daily",
        _COLS,
        upsert_rows,
        conflict_keys=("symbol", "trade_date", "expiry"),
        update_cols=(
            "atm_strike",
            "atm_iv",
            "underlying_price",
            "iv_source",
            "computed_at",
        ),
        set_fetched_at=False,
    )
    symbols = sorted({r[0] for r in upsert_rows})
    return {
        "trade_date": trade_date.isoformat(),
        "groups": len(upsert_rows),
        "rows_written": n,
        "symbols": len(symbols),
    }
