"""Put/Call Ratio daily: OI + volume → market_analytics.pcr_daily.

Independently reimplemented (no bifrost-core / trade-api pip dependency).

D11=A: PCR volume from ``option_snapshot.day_volume`` (last snap per ticker on NY day).
OI from ``market.option_open_interest`` (prefer EOD OI table).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Mapping, Sequence

from bifrost_market_data.ingest._upsert import batch_upsert

_COLS = (
    "symbol",
    "trade_date",
    "pcr_oi",
    "pcr_volume",
    "total_put_oi",
    "total_call_oi",
    "total_put_volume",
    "total_call_volume",
    "computed_at",
)


def _row_to_dict(row: Any, columns: Sequence[str]) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return {columns[i]: row[i] for i in range(min(len(columns), len(row)))}


def safe_pcr(put: float | int, call: float | int) -> float | None:
    """put/call ratio; None when call side is zero or put negative."""
    c = float(call)
    p = float(put)
    if c <= 0 or p < 0:
        return None
    return round(p / c, 6)


def _is_put(right: str) -> bool:
    return right.strip().upper() in ("P", "PUT")


def _is_call(right: str) -> bool:
    return right.strip().upper() in ("C", "CALL")


def fetch_oi_totals_for_date(
    conn: Any,
    trade_date: date,
    *,
    underlyings: Sequence[str] | None = None,
) -> dict[str, tuple[int, int]]:
    """Return symbol → (put_oi, call_oi) from market.option_open_interest."""
    cols = ("underlying", "option_right", "total_oi")
    syms = [str(s).strip().upper() for s in (underlyings or []) if str(s).strip()]
    with conn.cursor() as cur:
        if syms:
            cur.execute(
                """
                SELECT underlying, option_right, SUM(open_interest)::bigint AS total_oi
                FROM market.option_open_interest
                WHERE trade_date = %s AND underlying = ANY(%s)
                GROUP BY underlying, option_right
                """,
                (trade_date, syms),
            )
        else:
            cur.execute(
                """
                SELECT underlying, option_right, SUM(open_interest)::bigint AS total_oi
                FROM market.option_open_interest
                WHERE trade_date = %s
                GROUP BY underlying, option_right
                """,
                (trade_date,),
            )
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []

    out: dict[str, tuple[int, int]] = {}
    for r in raw or []:
        d = _row_to_dict(r, cols)
        und = str(d.get("underlying") or "").strip().upper()
        if not und:
            continue
        right = str(d.get("option_right") or "")
        oi = int(d.get("total_oi") or 0)
        put_oi, call_oi = out.get(und, (0, 0))
        if _is_put(right):
            put_oi += oi
        elif _is_call(right):
            call_oi += oi
        out[und] = (put_oi, call_oi)
    return out


def fetch_volume_totals_for_date(
    conn: Any,
    trade_date: date,
    *,
    underlyings: Sequence[str] | None = None,
) -> dict[str, tuple[int, int]]:
    """Return symbol → (put_volume, call_volume) from last snapshot day_volume (D11=A)."""
    cols = ("underlying", "option_right", "day_volume")
    syms = [str(s).strip().upper() for s in (underlyings or []) if str(s).strip()]
    base_sql = """
        SELECT DISTINCT ON (os.option_ticker)
          oc.underlying,
          oc.option_right,
          COALESCE(os.day_volume, 0)::bigint AS day_volume
        FROM market.option_snapshot os
        INNER JOIN market.option_contract oc
          ON oc.option_ticker = os.option_ticker
        WHERE DATE(timezone('America/New_York', os.snapshot_ts)) = %s
    """
    with conn.cursor() as cur:
        if syms:
            cur.execute(
                base_sql
                + """
                  AND UPPER(TRIM(oc.underlying)) = ANY(%s)
                ORDER BY os.option_ticker, os.snapshot_ts DESC
                """,
                (trade_date, syms),
            )
        else:
            cur.execute(
                base_sql
                + """
                ORDER BY os.option_ticker, os.snapshot_ts DESC
                """,
                (trade_date,),
            )
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []

    out: dict[str, tuple[int, int]] = {}
    for r in raw or []:
        d = _row_to_dict(r, cols)
        und = str(d.get("underlying") or "").strip().upper()
        if not und:
            continue
        right = str(d.get("option_right") or "")
        vol = int(d.get("day_volume") or 0)
        put_v, call_v = out.get(und, (0, 0))
        if _is_put(right):
            put_v += vol
        elif _is_call(right):
            call_v += vol
        out[und] = (put_v, call_v)
    return out


def compute_pcr_for_date(
    conn: Any,
    *,
    trade_date: date,
    underlyings: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Compute PCR for symbols with OI and/or volume on ``trade_date`` and upsert."""
    oi_map = fetch_oi_totals_for_date(conn, trade_date, underlyings=underlyings)
    vol_map = fetch_volume_totals_for_date(conn, trade_date, underlyings=underlyings)
    symbols = sorted(set(oi_map) | set(vol_map))
    if not symbols:
        return {
            "trade_date": trade_date.isoformat(),
            "groups": 0,
            "rows_written": 0,
            "symbols": 0,
        }

    now = datetime.now(timezone.utc)
    upsert_rows: list[tuple[Any, ...]] = []
    for symbol in symbols:
        put_oi, call_oi = oi_map.get(symbol, (0, 0))
        put_vol, call_vol = vol_map.get(symbol, (0, 0))
        if put_oi == 0 and call_oi == 0 and put_vol == 0 and call_vol == 0:
            continue
        upsert_rows.append(
            (
                symbol,
                trade_date,
                safe_pcr(put_oi, call_oi),
                safe_pcr(put_vol, call_vol),
                int(put_oi),
                int(call_oi),
                int(put_vol),
                int(call_vol),
                now,
            )
        )

    n = batch_upsert(
        conn,
        "market_analytics.pcr_daily",
        _COLS,
        upsert_rows,
        conflict_keys=("symbol", "trade_date"),
        update_cols=(
            "pcr_oi",
            "pcr_volume",
            "total_put_oi",
            "total_call_oi",
            "total_put_volume",
            "total_call_volume",
            "computed_at",
        ),
        set_fetched_at=False,
    )
    return {
        "trade_date": trade_date.isoformat(),
        "groups": len(upsert_rows),
        "rows_written": n,
        "symbols": len(upsert_rows),
    }
