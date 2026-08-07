"""Option discovery DB-read routes (expirations, snapshots, OI, lite analytics)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping

from fastapi import APIRouter, Query

from bifrost_market_data.api.deps import (
    as_date,
    normalize_symbol,
    require_db,
    row_dict,
    table_exists,
)

router = APIRouter(prefix="/options", tags=["options"])


def _norm_expiry(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    if len(s) == 8 and s.isdigit():
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    return as_date(s)


def query_expirations(conn: Any, symbol: str) -> dict[str, Any]:
    """Read expirations + strikes from market.option_expiration / option_contract."""
    sym = normalize_symbol(symbol)
    expirations: list[str] = []
    strikes: list[float] = []
    source = "none"

    if table_exists(conn, "market", "option_expiration"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT expiry FROM market.option_expiration
                WHERE underlying = %s
                ORDER BY expiry ASC
                """,
                (sym,),
            )
            rows = cur.fetchall() or []
        for r in rows:
            d = as_date(r[0] if not isinstance(r, Mapping) else r.get("expiry"))
            if d is not None:
                expirations.append(d.isoformat())
        if expirations:
            source = "option_expiration"

    if not expirations and table_exists(conn, "market", "option_contract"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT expiry FROM market.option_contract
                WHERE underlying = %s
                ORDER BY expiry ASC
                """,
                (sym,),
            )
            rows = cur.fetchall() or []
        for r in rows:
            d = as_date(r[0] if not isinstance(r, Mapping) else r.get("expiry"))
            if d is not None:
                expirations.append(d.isoformat())
        if expirations:
            source = "option_contract"

    # Strikes for nearest upcoming expiry (or first)
    target_exp: date | None = None
    today = date.today()
    for e in expirations:
        d = as_date(e)
        if d is not None and d >= today:
            target_exp = d
            break
    if target_exp is None and expirations:
        target_exp = as_date(expirations[0])

    if target_exp is not None and table_exists(conn, "market", "option_contract"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT strike FROM market.option_contract
                WHERE underlying = %s AND expiry = %s
                ORDER BY strike ASC
                """,
                (sym, target_exp),
            )
            rows = cur.fetchall() or []
        for r in rows:
            try:
                strikes.append(float(r[0] if not isinstance(r, Mapping) else r["strike"]))
            except (TypeError, ValueError, KeyError):
                continue

    return {
        "symbol": sym,
        "expirations": expirations,
        "strikes": strikes,
        "provider": "db",
        "source": source,
        "expiration_for_strikes": target_exp.isoformat() if target_exp else None,
    }


def query_snapshots(
    conn: Any,
    *,
    symbol: str,
    expiration: date | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Latest snapshot per option_ticker for an underlying (optional expiry filter)."""
    sym = normalize_symbol(symbol)
    if not table_exists(conn, "market", "option_snapshot"):
        return []

    params: list[Any] = [sym]
    join_sql = ""
    where_extra = ""
    if expiration is not None:
        if table_exists(conn, "market", "option_contract"):
            join_sql = """
                JOIN market.option_contract c
                  ON c.option_ticker = s.option_ticker
            """
            where_extra = " AND c.expiry = %s"
            params.append(expiration)
        else:
            # Cannot filter by expiry without contracts table
            pass

    params.append(int(limit))
    sql = f"""
        SELECT s.option_ticker, s.underlying, s.snapshot_ts,
               s.iv, s.delta, s.gamma, s.theta, s.vega,
               s.open_interest, s.day_volume, s.day_close, s.day_vwap,
               s.fetched_at
        FROM (
            SELECT DISTINCT ON (option_ticker) *
            FROM market.option_snapshot
            WHERE underlying = %s
            ORDER BY option_ticker, snapshot_ts DESC
        ) s
        {join_sql}
        WHERE 1=1 {where_extra}
        ORDER BY s.option_ticker
        LIMIT %s
    """
    cols = (
        "option_ticker",
        "underlying",
        "snapshot_ts",
        "iv",
        "delta",
        "gamma",
        "theta",
        "vega",
        "open_interest",
        "day_volume",
        "day_close",
        "day_vwap",
        "fetched_at",
    )
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        raw = cur.fetchall() or []
    return [row_dict(r, cols) for r in raw]


def query_oi(
    conn: Any,
    *,
    symbol: str,
    expiry: date | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    sym = normalize_symbol(symbol)
    if not table_exists(conn, "market", "option_open_interest"):
        return []
    clauses = ["underlying = %s"]
    params: list[Any] = [sym]
    if expiry is not None:
        clauses.append("expiry = %s")
        params.append(expiry)
    if date_from is not None:
        clauses.append("trade_date >= %s")
        params.append(date_from)
    if date_to is not None:
        clauses.append("trade_date <= %s")
        params.append(date_to)
    params.append(int(limit))
    cols = (
        "option_ticker",
        "underlying",
        "expiry",
        "strike",
        "option_right",
        "trade_date",
        "open_interest",
        "fetched_at",
    )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT option_ticker, underlying, expiry, strike, option_right,
                   trade_date, open_interest, fetched_at
            FROM market.option_open_interest
            WHERE {' AND '.join(clauses)}
            ORDER BY trade_date DESC, expiry ASC, strike ASC
            LIMIT %s
            """,
            tuple(params),
        )
        raw = cur.fetchall() or []
    return [row_dict(r, cols) for r in raw]


@router.get("/expirations")
def option_expirations(
    symbol: str = Query(..., description="Underlying symbol"),
) -> dict[str, Any]:
    """DB-first option expirations + strikes (IB path deferred to P7)."""
    conn = require_db()
    try:
        return query_expirations(conn, symbol)
    finally:
        conn.close()


@router.get("/snapshots")
def option_snapshots(
    symbol: str = Query(..., description="Underlying symbol"),
    expiration: str | None = Query(None, description="Expiry YYYY-MM-DD or YYYYMMDD"),
    limit: int = Query(500, ge=1, le=5000),
) -> dict[str, Any]:
    """Latest option chain snapshots from ``market.option_snapshot``."""
    exp = _norm_expiry(expiration)
    conn = require_db()
    try:
        rows = query_snapshots(conn, symbol=symbol, expiration=exp, limit=limit)
    finally:
        conn.close()
    return {
        "symbol": normalize_symbol(symbol),
        "expiration": exp.isoformat() if exp else None,
        "rows": rows,
        "count": len(rows),
        "source": "market.option_snapshot",
    }


@router.get("/oi")
def option_oi(
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str | None = Query(None, description="Expiry YYYY-MM-DD or YYYYMMDD"),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    """Open interest rows from ``market.option_open_interest``."""
    exp = _norm_expiry(expiry)
    conn = require_db()
    try:
        rows = query_oi(
            conn,
            symbol=symbol,
            expiry=exp,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
        )
    finally:
        conn.close()
    return {
        "symbol": normalize_symbol(symbol),
        "expiry": exp.isoformat() if exp else None,
        "rows": rows,
        "count": len(rows),
    }


@router.get("/liquidity-summary")
def liquidity_summary(
    symbol: str = Query(..., description="Underlying symbol"),
    expiration: str | None = Query(None),
    limit: int = Query(200, ge=1, le=2000),
) -> dict[str, Any]:
    """Minimal liquidity stats from latest snapshots (OI + volume percentiles).

    Scope: Plugin market schema only — no Trade IB bid/ask spreads.
    """
    exp = _norm_expiry(expiration)
    conn = require_db()
    try:
        rows = query_snapshots(conn, symbol=symbol, expiration=exp, limit=limit)
    finally:
        conn.close()

    ois = [int(r["open_interest"]) for r in rows if r.get("open_interest") is not None]
    vols = [int(r["day_volume"]) for r in rows if r.get("day_volume") is not None]

    def _pct(vals: list[int], p: float) -> float | None:
        if not vals:
            return None
        s = sorted(vals)
        idx = min(len(s) - 1, max(0, int(round((len(s) - 1) * p))))
        return float(s[idx])

    return {
        "symbol": normalize_symbol(symbol),
        "expiration": exp.isoformat() if exp else None,
        "contracts": len(rows),
        "oi": {
            "count": len(ois),
            "sum": sum(ois) if ois else 0,
            "p50": _pct(ois, 0.5),
            "p90": _pct(ois, 0.9),
        },
        "volume": {
            "count": len(vols),
            "sum": sum(vols) if vols else 0,
            "p50": _pct(vols, 0.5),
            "p90": _pct(vols, 0.9),
        },
        "scope": "snapshot_oi_volume_only",
        "note": "Bid/ask spread metrics deferred (no NBBO persistence in Plugin schema)",
    }


@router.get("/relative-value")
def relative_value(
    symbol: str = Query(..., description="Underlying symbol"),
    expiration: str | None = Query(None),
    limit: int = Query(200, ge=1, le=2000),
) -> dict[str, Any]:
    """Minimal IV curve from latest snapshots (strike vs IV).

    Full relative-value scoring remains in Trade Research until P7.
    """
    exp = _norm_expiry(expiration)
    conn = require_db()
    try:
        rows = query_snapshots(conn, symbol=symbol, expiration=exp, limit=limit)
        # Enrich with strike from contracts when available
        tickers = [r["option_ticker"] for r in rows if r.get("option_ticker")]
        strike_map: dict[str, float] = {}
        if tickers and table_exists(conn, "market", "option_contract"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT option_ticker, strike, option_right
                    FROM market.option_contract
                    WHERE option_ticker = ANY(%s)
                    """,
                    (tickers,),
                )
                for r in cur.fetchall() or []:
                    if isinstance(r, Mapping):
                        strike_map[str(r["option_ticker"])] = float(r["strike"])
                    else:
                        strike_map[str(r[0])] = float(r[1])
    finally:
        conn.close()

    iv_curve: list[dict[str, Any]] = []
    for r in rows:
        iv = r.get("iv")
        if iv is None:
            continue
        ot = str(r.get("option_ticker") or "")
        strike = strike_map.get(ot)
        if strike is None:
            continue
        try:
            iv_f = float(iv)
        except (TypeError, ValueError):
            continue
        if not (0 < iv_f < 10):
            continue
        iv_curve.append({"strike": strike, "iv": iv_f, "option_ticker": ot})

    return {
        "symbol": normalize_symbol(symbol),
        "expiration": exp.isoformat() if exp else None,
        "contracts_compared": len(iv_curve),
        "iv_curve": sorted(iv_curve, key=lambda x: x["strike"]),
        "scope": "iv_curve_from_snapshot",
        "note": "Full relative-value scoring deferred to P7 Trade cleanup",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
