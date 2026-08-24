"""Analytics read routes — Max Pain, ATM IV, PCR, IV Percentile.

Wave 7: daily upsert ownership is Research API ``:8795`` (``features.*``).
Plugin keeps DB **read** endpoints on canonical ``features.option_metric_*`` tables
for FE/Plugin consumers during transition, plus live max-pain compute from OI.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

from fastapi import APIRouter, HTTPException, Query, Response

router = APIRouter(prefix="/analytics", tags=["analytics"])

# Deprecation notice: Research owns scheduled compute + canonical write path.
_RESEARCH_OWNER_HEADER = (
    "Research API :8795 /analytics/options/* "
    "(bifrost_research) owns features.* upserts; "
    "Plugin keeps Golden Source read + live max-pain compute"
)


def _deprecation_headers(response: Response) -> None:
    response.headers["X-Bifrost-Analytics-Owner"] = "bifrost-research"
    response.headers["Deprecation"] = "true"
    response.headers["X-Bifrost-Analytics-Note"] = _RESEARCH_OWNER_HEADER


def _connect():
    import psycopg

    from bifrost_market_data.config import load_config, postgres_connect_kwargs

    return psycopg.connect(**postgres_connect_kwargs(load_config()), connect_timeout=10)


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


def _row_dict(row: Any, columns: Sequence[str]) -> dict[str, Any]:
    if isinstance(row, Mapping):
        out = dict(row)
    else:
        out = {columns[i]: row[i] for i in range(min(len(columns), len(row)))}
    for key in ("trade_date", "expiry"):
        if key in out and out[key] is not None:
            d = _as_date(out[key])
            out[key] = d.isoformat() if d else out[key]
    if "computed_at" in out and isinstance(out["computed_at"], datetime):
        out["computed_at"] = out["computed_at"].isoformat()
    return out


def _apply_date_filters(
    clauses: list[str],
    params: list[Any],
    *,
    table: str,
    symbol_col: str,
    sym: str | None,
    trade_date: date | None,
    lookback_days: int | None,
    conn: Any,
) -> date | None:
    """Append symbol/trade_date/lookback clauses. Returns resolved trade_date."""
    if sym:
        clauses.append(f"{symbol_col} = %s")
        params.append(sym)

    resolved_td = trade_date
    if resolved_td is None and sym and lookback_days is None:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(trade_date) FROM {table} WHERE {symbol_col} = %s",
                (sym,),
            )
            row = cur.fetchone()
        if row is not None:
            if isinstance(row, Mapping):
                resolved_td = _as_date(next(iter(row.values()), None))
            else:
                resolved_td = _as_date(row[0])

    if lookback_days is not None and lookback_days > 0:
        end = resolved_td or date.today()
        start = end - timedelta(days=int(lookback_days))
        clauses.append("trade_date >= %s")
        params.append(start)
        clauses.append("trade_date <= %s")
        params.append(end)
    elif resolved_td is not None:
        clauses.append("trade_date = %s")
        params.append(resolved_td)
    return resolved_td


def query_max_pain(
    conn: Any,
    *,
    symbol: str | None = None,
    expiry: date | None = None,
    trade_date: date | None = None,
    lookback_days: int | None = None,
) -> list[dict[str, Any]]:
    """Read rows from ``features.option_metric_max_pain_daily``."""
    cols = (
        "symbol",
        "trade_date",
        "expiry",
        "max_pain_strike",
        "total_oi",
        "total_pain_at_strike",
        "computed_at",
    )
    clauses: list[str] = []
    params: list[Any] = []
    sym = str(symbol).strip().upper() if symbol else None
    _apply_date_filters(
        clauses,
        params,
        table="features.option_metric_max_pain_daily",
        symbol_col="symbol",
        sym=sym,
        trade_date=trade_date,
        lookback_days=lookback_days,
        conn=conn,
    )
    if expiry is not None:
        clauses.append("expiry = %s")
        params.append(expiry)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT symbol, trade_date, expiry, max_pain_strike,
               total_oi, total_pain_at_strike, computed_at
        FROM features.option_metric_max_pain_daily
        {where}
        ORDER BY trade_date DESC, symbol ASC, expiry ASC
        LIMIT 500
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [_row_dict(r, cols) for r in (raw or [])]


def query_atm_iv(
    conn: Any,
    *,
    symbol: str | None = None,
    expiry: date | None = None,
    trade_date: date | None = None,
    lookback_days: int | None = None,
) -> list[dict[str, Any]]:
    """Read rows from ``features.option_metric_atm_iv_daily``."""
    cols = (
        "symbol",
        "trade_date",
        "expiry",
        "atm_strike",
        "atm_iv",
        "underlying_price",
        "iv_source",
        "computed_at",
    )
    clauses: list[str] = []
    params: list[Any] = []
    sym = str(symbol).strip().upper() if symbol else None
    _apply_date_filters(
        clauses,
        params,
        table="features.option_metric_atm_iv_daily",
        symbol_col="symbol",
        sym=sym,
        trade_date=trade_date,
        lookback_days=lookback_days,
        conn=conn,
    )
    if expiry is not None:
        clauses.append("expiry = %s")
        params.append(expiry)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT symbol, trade_date, expiry, atm_strike, atm_iv,
               underlying_price, iv_source, computed_at
        FROM features.option_metric_atm_iv_daily
        {where}
        ORDER BY trade_date DESC, symbol ASC, expiry ASC
        LIMIT 500
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [_row_dict(r, cols) for r in (raw or [])]


def query_pcr(
    conn: Any,
    *,
    symbol: str | None = None,
    trade_date: date | None = None,
    lookback_days: int | None = None,
) -> list[dict[str, Any]]:
    """Read rows from ``features.option_metric_pcr_daily``."""
    cols = (
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
    clauses: list[str] = []
    params: list[Any] = []
    sym = str(symbol).strip().upper() if symbol else None
    _apply_date_filters(
        clauses,
        params,
        table="features.option_metric_pcr_daily",
        symbol_col="symbol",
        sym=sym,
        trade_date=trade_date,
        lookback_days=lookback_days,
        conn=conn,
    )
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT symbol, trade_date, pcr_oi, pcr_volume,
               total_put_oi, total_call_oi,
               total_put_volume, total_call_volume, computed_at
        FROM features.option_metric_pcr_daily
        {where}
        ORDER BY trade_date DESC, symbol ASC
        LIMIT 500
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [_row_dict(r, cols) for r in (raw or [])]


def query_iv_percentile(
    conn: Any,
    *,
    symbol: str | None = None,
    trade_date: date | None = None,
    lookback_days: int | None = None,
) -> list[dict[str, Any]]:
    """Read rows from ``features.option_metric_iv_percentile_daily``."""
    cols = (
        "symbol",
        "trade_date",
        "iv_current",
        "iv_percentile_1y",
        "iv_rank_1y",
        "lookback_days",
        "computed_at",
    )
    clauses: list[str] = []
    params: list[Any] = []
    sym = str(symbol).strip().upper() if symbol else None
    _apply_date_filters(
        clauses,
        params,
        table="features.option_metric_iv_percentile_daily",
        symbol_col="symbol",
        sym=sym,
        trade_date=trade_date,
        lookback_days=lookback_days,
        conn=conn,
    )
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT symbol, trade_date, iv_current, iv_percentile_1y,
               iv_rank_1y, lookback_days, computed_at
        FROM features.option_metric_iv_percentile_daily
        {where}
        ORDER BY trade_date DESC, symbol ASC
        LIMIT 500
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [_row_dict(r, cols) for r in (raw or [])]


@router.get("/max-pain")
def max_pain(
    response: Response,
    symbol: str | None = Query(None, description="Underlying symbol filter"),
    expiry: date | None = Query(None, description="Option expiry YYYY-MM-DD"),
    trade_date: date | None = Query(None, description="Trade date (default: latest for symbol)"),
    lookback_days: int | None = Query(
        None,
        ge=1,
        le=365,
        description="When set, return rows in [end-lookback, end] window",
    ),
) -> dict[str, Any]:
    """Read persisted Max Pain from ``features.option_metric_max_pain_daily``.

    Upserts owned by Research API ``:8795``; Plugin keeps Golden Source reads.
    """
    _deprecation_headers(response)
    try:
        conn = _connect()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    try:
        rows = query_max_pain(
            conn,
            symbol=symbol,
            expiry=expiry,
            trade_date=trade_date,
            lookback_days=lookback_days,
        )
    finally:
        conn.close()

    if not rows and symbol:
        raise HTTPException(status_code=404, detail="No max-pain rows for symbol")
    return {
        "rows": rows,
        "count": len(rows),
        "symbol": str(symbol).strip().upper() if symbol else None,
        "trade_date": trade_date.isoformat() if trade_date else None,
        "expiry": expiry.isoformat() if expiry else None,
        "lookback_days": lookback_days,
    }


@router.get("/atm-iv")
def atm_iv(
    response: Response,
    symbol: str | None = Query(None, description="Underlying symbol filter"),
    expiry: date | None = Query(None, description="Option expiry YYYY-MM-DD"),
    trade_date: date | None = Query(None, description="Trade date (default: latest for symbol)"),
    lookback_days: int | None = Query(
        None,
        ge=1,
        le=365,
        description="When set, return rows in [end-lookback, end] window",
    ),
) -> dict[str, Any]:
    """Read persisted ATM IV from ``features.option_metric_atm_iv_daily``.

    Upserts owned by Research API ``:8795``; Plugin keeps Golden Source reads.
    """
    _deprecation_headers(response)
    try:
        conn = _connect()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    try:
        rows = query_atm_iv(
            conn,
            symbol=symbol,
            expiry=expiry,
            trade_date=trade_date,
            lookback_days=lookback_days,
        )
    finally:
        conn.close()

    if not rows and symbol:
        raise HTTPException(status_code=404, detail="No atm-iv rows for symbol")
    return {
        "rows": rows,
        "count": len(rows),
        "symbol": str(symbol).strip().upper() if symbol else None,
        "trade_date": trade_date.isoformat() if trade_date else None,
        "expiry": expiry.isoformat() if expiry else None,
        "lookback_days": lookback_days,
    }


@router.get("/pcr")
def pcr(
    response: Response,
    symbol: str | None = Query(None, description="Underlying symbol filter"),
    trade_date: date | None = Query(None, description="Trade date (default: latest for symbol)"),
    lookback_days: int | None = Query(
        None,
        ge=1,
        le=365,
        description="When set, return rows in [end-lookback, end] window",
    ),
) -> dict[str, Any]:
    """Read persisted Put/Call Ratio from ``features.option_metric_pcr_daily``.

    Upserts owned by Research API ``:8795``; Plugin keeps Golden Source reads.
    """
    _deprecation_headers(response)
    try:
        conn = _connect()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    try:
        rows = query_pcr(
            conn,
            symbol=symbol,
            trade_date=trade_date,
            lookback_days=lookback_days,
        )
    finally:
        conn.close()

    if not rows and symbol:
        raise HTTPException(status_code=404, detail="No pcr rows for symbol")
    return {
        "rows": rows,
        "count": len(rows),
        "symbol": str(symbol).strip().upper() if symbol else None,
        "trade_date": trade_date.isoformat() if trade_date else None,
        "lookback_days": lookback_days,
    }


@router.get("/iv-percentile")
def iv_percentile(
    response: Response,
    symbol: str | None = Query(None, description="Underlying symbol filter"),
    trade_date: date | None = Query(None, description="Trade date (default: latest for symbol)"),
    lookback_days: int | None = Query(
        None,
        ge=1,
        le=365,
        description="When set, return rows in [end-lookback, end] window",
    ),
) -> dict[str, Any]:
    """Read persisted IV percentile/rank from ``features.option_metric_iv_percentile_daily``.

    Upserts owned by Research API ``:8795``; Plugin keeps Golden Source reads.
    """
    _deprecation_headers(response)
    try:
        conn = _connect()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    try:
        rows = query_iv_percentile(
            conn,
            symbol=symbol,
            trade_date=trade_date,
            lookback_days=lookback_days,
        )
    finally:
        conn.close()

    if not rows and symbol:
        raise HTTPException(status_code=404, detail="No iv-percentile rows for symbol")
    return {
        "rows": rows,
        "count": len(rows),
        "symbol": str(symbol).strip().upper() if symbol else None,
        "trade_date": trade_date.isoformat() if trade_date else None,
        "lookback_days": lookback_days,
    }


def _parse_expiry_param(expiry: str) -> date:
    s = str(expiry).strip()
    if len(s) == 8 and s.isdigit():
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    d = _as_date(s)
    if d is None:
        raise HTTPException(status_code=400, detail="Invalid expiry; use YYYY-MM-DD or YYYYMMDD")
    return d


def _fetch_oi_rows_for_date(
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
                FROM raw_market.option_open_interest
                WHERE trade_date = %s AND underlying = ANY(%s)
                """,
                (trade_date, syms),
            )
        else:
            cur.execute(
                """
                SELECT underlying, expiry, strike, option_right, open_interest
                FROM raw_market.option_open_interest
                WHERE trade_date = %s
                """,
                (trade_date,),
            )
        raw = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [_row_dict(r, cols) for r in (raw or [])]


def compute_max_pain_live(
    conn: Any,
    *,
    symbol: str,
    expiry: date,
    trade_date: date | None = None,
) -> dict[str, Any]:
    """Live max-pain from OI (does not require persisted analytics row)."""
    from bifrost_market_data.analytics.max_pain_math import (
        compute_max_pain_curve,
        strike_map_for_expiry,
    )

    sym = str(symbol).strip().upper()
    td = trade_date
    if td is None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(trade_date) FROM raw_market.option_open_interest
                WHERE underlying = %s AND expiry = %s
                """,
                (sym, expiry),
            )
            row = cur.fetchone()
        if row is None or (isinstance(row, Mapping) and next(iter(row.values()), None) is None):
            return {"ok": False, "error": "No OI rows for symbol/expiry", "symbol": sym}
        td = _as_date(row[0] if not isinstance(row, Mapping) else next(iter(row.values())))
        if td is None:
            return {"ok": False, "error": "No OI rows for symbol/expiry", "symbol": sym}

    oi_rows = _fetch_oi_rows_for_date(conn, td, underlyings=[sym])
    skmap = strike_map_for_expiry(oi_rows, expiry)
    if not skmap:
        return {
            "ok": False,
            "error": "No strike OI for symbol/expiry/trade_date",
            "symbol": sym,
            "expiry": expiry.isoformat(),
            "trade_date": td.isoformat(),
        }
    max_pain_strike, min_pain, points, total_oi = compute_max_pain_curve(skmap)
    return {
        "ok": True,
        "symbol": sym,
        "expiry": expiry.isoformat(),
        "trade_date": td.isoformat(),
        "max_pain_strike": float(max_pain_strike),
        "total_pain_at_strike": float(min_pain),
        "total_oi": int(total_oi),
        "points": points,
        "source": "live_oi",
    }


def compute_max_pain_history(
    conn: Any,
    *,
    symbol: str,
    expiry: date,
    lookback_days: int = 90,
) -> dict[str, Any]:
    """Per-trade-date max pain series recomputed from OI."""
    from bifrost_market_data.analytics.max_pain_math import (
        compute_max_pain_curve,
        strike_map_for_expiry,
    )

    sym = str(symbol).strip().upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(trade_date) FROM raw_market.option_open_interest
            WHERE underlying = %s AND expiry = %s
            """,
            (sym, expiry),
        )
        row = cur.fetchone()
    end = None
    if row is not None:
        end = _as_date(row[0] if not isinstance(row, Mapping) else next(iter(row.values()), None))
    if end is None:
        return {
            "ok": False,
            "error": "No OI rows for symbol/expiry",
            "symbol": sym,
            "expiry": expiry.isoformat(),
            "series": [],
        }
    start = end - timedelta(days=int(lookback_days))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, strike, option_right, open_interest
            FROM raw_market.option_open_interest
            WHERE underlying = %s AND expiry = %s
              AND trade_date >= %s AND trade_date <= %s
            ORDER BY trade_date ASC
            """,
            (sym, expiry, start, end),
        )
        raw = cur.fetchall() or []

    by_date: dict[date, list[dict[str, Any]]] = {}
    for r in raw:
        if isinstance(r, Mapping):
            td = _as_date(r.get("trade_date"))
            strike = r.get("strike")
            right = r.get("option_right")
            oi = r.get("open_interest")
        else:
            td = _as_date(r[0])
            strike = r[1]
            right = r[2]
            oi = r[3]
        if td is None:
            continue
        by_date.setdefault(td, []).append(
            {
                "underlying": sym,
                "expiry": expiry,
                "strike": strike,
                "option_right": right,
                "open_interest": oi,
            }
        )

    series: list[dict[str, Any]] = []
    for td in sorted(by_date.keys()):
        skmap = strike_map_for_expiry(by_date[td], expiry)
        if not skmap:
            continue
        max_pain_strike, min_pain, _pts, total_oi = compute_max_pain_curve(skmap)
        series.append(
            {
                "trade_date": td.isoformat(),
                "max_pain_strike": float(max_pain_strike),
                "total_pain_at_strike": float(min_pain),
                "total_oi": int(total_oi),
            }
        )
    return {
        "ok": True,
        "symbol": sym,
        "expiry": expiry.isoformat(),
        "lookback_days": lookback_days,
        "series": series,
        "count": len(series),
        "source": "live_oi",
    }


@router.get("/max-pain/compute")
def max_pain_compute(
    response: Response,
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    trade_date: date | None = Query(None, description="OI as-of date (default: latest)"),
) -> dict[str, Any]:
    """Live Max Pain from ``market.option_open_interest`` (not table-only read)."""
    _deprecation_headers(response)
    exp = _parse_expiry_param(expiry)
    try:
        conn = _connect()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    try:
        result = compute_max_pain_live(
            conn, symbol=symbol, expiry=exp, trade_date=trade_date
        )
    finally:
        conn.close()
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error") or "compute failed")
    return result


@router.get("/max-pain/compute/history")
def max_pain_compute_history(
    response: Response,
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str = Query(..., description="Expiration YYYYMMDD or YYYY-MM-DD"),
    lookback_days: int = Query(90, ge=7, le=365),
) -> dict[str, Any]:
    """Per-trade-date max pain series recomputed from OI."""
    _deprecation_headers(response)
    exp = _parse_expiry_param(expiry)
    try:
        conn = _connect()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    try:
        result = compute_max_pain_history(
            conn, symbol=symbol, expiry=exp, lookback_days=lookback_days
        )
    finally:
        conn.close()
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error") or "compute failed")
    return result


@router.get("/atm-iv/term")
def atm_iv_term(
    response: Response,
    symbol: str = Query(..., description="Underlying symbol"),
    trade_date: date | None = Query(None, description="Trade date (default: latest)"),
) -> dict[str, Any]:
    """ATM IV term structure from persisted ``features.option_metric_atm_iv_daily``."""
    _deprecation_headers(response)
    try:
        conn = _connect()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
    try:
        rows = query_atm_iv(conn, symbol=symbol, trade_date=trade_date)
    finally:
        conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="No atm-iv rows for symbol")
    # One trade_date cohort (latest if mixed)
    td = rows[0].get("trade_date")
    term = [r for r in rows if r.get("trade_date") == td]
    term_sorted = sorted(term, key=lambda r: str(r.get("expiry") or ""))
    return {
        "symbol": str(symbol).strip().upper(),
        "trade_date": td,
        "term": term_sorted,
        "count": len(term_sorted),
        "source": "features.option_metric_atm_iv_daily",
    }
