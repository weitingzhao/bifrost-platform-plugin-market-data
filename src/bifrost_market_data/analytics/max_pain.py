"""Max Pain daily compute: market.option_open_interest → market_analytics.max_pain_daily.

Algorithm independently reimplemented from bifrost_core.monitor.reader.max_pain_math
(D7=A — no bifrost-core pip dependency).

pain(K) = Σ [ OI_call(s) * max(0, K-s)*100 + OI_put(s) * max(0, s-K)*100 ]
max_pain_strike = argmin_K(pain(K))
Per (symbol, expiry) independently.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from bifrost_market_data.ingest._upsert import batch_upsert

_COLS = (
    "symbol",
    "trade_date",
    "expiry",
    "max_pain_strike",
    "total_oi",
    "total_pain_at_strike",
    "computed_at",
)


def normalize_expiry_for_oi(expiry: str | date) -> str:
    """Normalize expiry to YYYYMMDD for strike-map matching."""
    if isinstance(expiry, date):
        return expiry.strftime("%Y%m%d")
    s = (expiry or "").strip()
    if len(s) >= 10 and s[4] == "-":
        return s[:4] + s[5:7] + s[8:10]
    return s.replace("-", "")


def strike_map_for_expiry(
    rows: List[Dict[str, Any]],
    target_expiry: str | date,
) -> Dict[float, Tuple[int, int]]:
    """Build strike -> (call_oi, put_oi) for one expiry from OI rows."""
    ne = normalize_expiry_for_oi(target_expiry)
    skmap: Dict[float, Tuple[int, int]] = {}
    for r in rows:
        exp = r.get("expiry")
        if normalize_expiry_for_oi(exp if isinstance(exp, date) else str(exp or "")) != ne:
            continue
        try:
            sk = float(r.get("strike"))
        except (TypeError, ValueError):
            continue
        oi = int(r.get("open_interest") or 0)
        right = (r.get("option_right") or "").strip().upper()
        c_oi, p_oi = skmap.get(sk, (0, 0))
        if right == "C":
            skmap[sk] = (c_oi + oi, p_oi)
        elif right == "P":
            skmap[sk] = (c_oi, p_oi + oi)
    return skmap


def compute_max_pain_curve(
    skmap: Dict[float, Tuple[int, int]],
) -> Tuple[float, float, List[Dict[str, Any]], int]:
    """Return (max_pain_strike, min_pain_value, points, total_oi).

    points: one entry per candidate strike K (sorted), with pain at K and OI at that strike.
    """
    if not skmap:
        return 0.0, 0.0, [], 0
    total_oi = sum(int(c) + int(p) for c, p in skmap.values())
    strikes_sorted = sorted(skmap.keys())
    points: List[Dict[str, Any]] = []
    best_x = strikes_sorted[0]
    best_pain: float | None = None
    for x in strikes_sorted:
        pain_call = 0.0
        pain_put = 0.0
        for s, (coi, poi) in skmap.items():
            pain_call += float(coi) * max(0.0, x - s) * 100.0
            pain_put += float(poi) * max(0.0, s - x) * 100.0
        pain = pain_call + pain_put
        c_at, p_at = skmap.get(x, (0, 0))
        points.append(
            {
                "strike": x,
                "pain": pain,
                "pain_call": pain_call,
                "pain_put": pain_put,
                "call_oi": int(c_at),
                "put_oi": int(p_at),
            }
        )
        if best_pain is None or pain < best_pain:
            best_pain = pain
            best_x = x
    return best_x, float(best_pain if best_pain is not None else 0.0), points, int(total_oi)


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


def fetch_oi_rows_for_date(
    conn: Any,
    trade_date: date,
    *,
    underlyings: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Load OI rows for one trade_date (optional underlying filter)."""
    cols = ("underlying", "expiry", "strike", "option_right", "open_interest")
    syms = [str(s).strip().upper() for s in (underlyings or []) if str(s).strip()]
    with conn.cursor() as cur:
        if syms:
            cur.execute(
                """
                SELECT underlying, expiry, strike, option_right, open_interest
                FROM market.option_open_interest
                WHERE trade_date = %s AND underlying = ANY(%s)
                """,
                (trade_date, syms),
            )
        else:
            cur.execute(
                """
                SELECT underlying, expiry, strike, option_right, open_interest
                FROM market.option_open_interest
                WHERE trade_date = %s
                """,
                (trade_date,),
            )
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [_row_to_dict(r, cols) for r in (raw or [])]


def compute_max_pain_for_date(
    conn: Any,
    *,
    trade_date: date,
    underlyings: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Compute max pain for all (underlying, expiry) with OI on ``trade_date`` and upsert.

    Uses ON CONFLICT DO UPDATE so recomputes refresh ``computed_at`` and values.
    """
    oi_rows = fetch_oi_rows_for_date(conn, trade_date, underlyings=underlyings)
    if not oi_rows:
        return {
            "trade_date": trade_date.isoformat(),
            "groups": 0,
            "rows_written": 0,
            "symbols": 0,
        }

    # Group OI by (underlying, expiry)
    groups: dict[tuple[str, date], list[dict[str, Any]]] = {}
    for r in oi_rows:
        und = str(r.get("underlying") or "").strip().upper()
        exp = _as_date(r.get("expiry"))
        if not und or exp is None:
            continue
        groups.setdefault((und, exp), []).append(r)

    now = datetime.now(timezone.utc)
    upsert_rows: list[tuple[Any, ...]] = []
    for (symbol, expiry), rows in sorted(groups.items()):
        skmap = strike_map_for_expiry(rows, expiry)
        if not skmap:
            continue
        max_pain_strike, min_pain, _points, total_oi = compute_max_pain_curve(skmap)
        upsert_rows.append(
            (
                symbol,
                trade_date,
                expiry,
                float(max_pain_strike),
                int(total_oi),
                float(min_pain),
                now,
            )
        )

    n = batch_upsert(
        conn,
        "market_analytics.max_pain_daily",
        _COLS,
        upsert_rows,
        conflict_keys=("symbol", "trade_date", "expiry"),
        update_cols=(
            "max_pain_strike",
            "total_oi",
            "total_pain_at_strike",
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
