"""IV Percentile / Rank daily: market_analytics.atm_iv_daily → iv_percentile_daily.

Current IV for a symbol+trade_date = **median** of ``atm_iv`` across expiries that day
(documented choice: median is robust when near-term expiries are noisy or sparse).

IV Percentile: fraction of historical daily IVs (inclusive lookback window) <= current × 100.
IV Rank: (current − min) / (max − min) × 100; when max == min → 50.0.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any, Dict, Mapping, Sequence

from bifrost_market_data.ingest._upsert import batch_upsert

_COLS = (
    "symbol",
    "trade_date",
    "iv_current",
    "iv_percentile_1y",
    "iv_rank_1y",
    "lookback_days",
    "computed_at",
)

DEFAULT_PERCENTILE_WINDOW = 252


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


def daily_representative_iv(atm_ivs: Sequence[float]) -> float | None:
    """Median of per-expiry ATM IVs for one symbol+trade_date."""
    vals = [float(v) for v in atm_ivs if v is not None]
    if not vals:
        return None
    return float(median(vals))


def iv_percentile(current: float, history: Sequence[float]) -> float | None:
    """Fraction of history values <= current, as 0–100. Empty history → None."""
    if not history:
        return None
    n = len(history)
    le = sum(1 for v in history if float(v) <= current)
    return round(100.0 * le / n, 4)


def iv_rank(current: float, history: Sequence[float]) -> float | None:
    """(current-min)/(max-min)*100; max==min → 50. Empty history → None."""
    if not history:
        return None
    lo = min(float(v) for v in history)
    hi = max(float(v) for v in history)
    if hi == lo:
        return 50.0
    return round(100.0 * (current - lo) / (hi - lo), 4)


def fetch_atm_iv_rows(
    conn: Any,
    *,
    from_date: date,
    to_date: date,
    underlyings: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Load atm_iv_daily rows in [from_date, to_date]."""
    cols = ("symbol", "trade_date", "expiry", "atm_iv")
    syms = [str(s).strip().upper() for s in (underlyings or []) if str(s).strip()]
    with conn.cursor() as cur:
        if syms:
            cur.execute(
                """
                SELECT symbol, trade_date, expiry, atm_iv
                FROM market_analytics.atm_iv_daily
                WHERE trade_date >= %s AND trade_date <= %s
                  AND symbol = ANY(%s)
                ORDER BY symbol, trade_date, expiry
                """,
                (from_date, to_date, syms),
            )
        else:
            cur.execute(
                """
                SELECT symbol, trade_date, expiry, atm_iv
                FROM market_analytics.atm_iv_daily
                WHERE trade_date >= %s AND trade_date <= %s
                ORDER BY symbol, trade_date, expiry
                """,
                (from_date, to_date),
            )
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [_row_to_dict(r, cols) for r in (raw or [])]


def rollup_daily_iv_by_symbol(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[date, float]]:
    """symbol → {trade_date → median atm_iv across expiries}."""
    buckets: dict[str, dict[date, list[float]]] = {}
    for r in rows:
        sym = str(r.get("symbol") or "").strip().upper()
        td = _as_date(r.get("trade_date"))
        if not sym or td is None:
            continue
        try:
            iv = float(r.get("atm_iv"))
        except (TypeError, ValueError):
            continue
        buckets.setdefault(sym, {}).setdefault(td, []).append(iv)

    out: dict[str, dict[date, float]] = {}
    for sym, by_day in buckets.items():
        day_map: dict[date, float] = {}
        for td, ivs in by_day.items():
            rep = daily_representative_iv(ivs)
            if rep is not None:
                day_map[td] = rep
        if day_map:
            out[sym] = day_map
    return out


def compute_iv_percentile_for_date(
    conn: Any,
    *,
    trade_date: date,
    underlyings: Sequence[str] | None = None,
    percentile_window: int = DEFAULT_PERCENTILE_WINDOW,
) -> dict[str, Any]:
    """Compute IV percentile/rank for symbols with ATM IV on ``trade_date`` and upsert.

    History window: calendar days covering ~``percentile_window`` trading days
    (fetch from ``trade_date - percentile_window * 2`` to include weekends/holidays,
    then use at most ``percentile_window`` prior daily IVs + current day).
    """
    window = max(1, int(percentile_window))
    # ~2 calendar days per trading day covers holidays; clamp floor
    from_date = trade_date - timedelta(days=window * 2 + 30)
    rows = fetch_atm_iv_rows(
        conn,
        from_date=from_date,
        to_date=trade_date,
        underlyings=underlyings,
    )
    by_sym = rollup_daily_iv_by_symbol(rows)
    if not by_sym:
        return {
            "trade_date": trade_date.isoformat(),
            "groups": 0,
            "rows_written": 0,
            "symbols": 0,
            "percentile_window": window,
        }

    now = datetime.now(timezone.utc)
    upsert_rows: list[tuple[Any, ...]] = []
    for symbol, day_map in sorted(by_sym.items()):
        current = day_map.get(trade_date)
        if current is None:
            continue
        # Prior days + current (inclusive), oldest first; take last `window` points ending at trade_date
        hist_pairs = sorted(
            ((d, v) for d, v in day_map.items() if d <= trade_date),
            key=lambda x: x[0],
        )
        hist_vals = [v for _d, v in hist_pairs[-window:]]
        lookback_used = len(hist_vals)
        pct = iv_percentile(current, hist_vals)
        rank = iv_rank(current, hist_vals)
        if pct is None or rank is None:
            continue
        upsert_rows.append(
            (
                symbol,
                trade_date,
                float(current),
                float(pct),
                float(rank),
                int(lookback_used),
                now,
            )
        )

    n = batch_upsert(
        conn,
        "market_analytics.iv_percentile_daily",
        _COLS,
        upsert_rows,
        conflict_keys=("symbol", "trade_date"),
        update_cols=(
            "iv_current",
            "iv_percentile_1y",
            "iv_rank_1y",
            "lookback_days",
            "computed_at",
        ),
        set_fetched_at=False,
    )
    return {
        "trade_date": trade_date.isoformat(),
        "groups": len(upsert_rows),
        "rows_written": n,
        "symbols": len(upsert_rows),
        "percentile_window": window,
    }
