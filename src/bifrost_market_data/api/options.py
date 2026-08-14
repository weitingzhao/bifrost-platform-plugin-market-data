"""Option discovery DB-read routes (expirations, snapshots, OI, lite analytics).

W0-P2 additions: chain/latest, chain/eod, contracts, strikes, expirations/yyyymmdd
— all with IB ↔ Polygon contract_key bridging.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import (
    as_date,
    iso_value,
    normalize_symbol,
    require_db,
    row_dict,
    table_exists,
    view_exists,
)
from bifrost_market_data.api.options_bridge import (
    ib_contract_key_from_parts,
    identity_key,
    split_contract_keys,
)

logger = logging.getLogger(__name__)

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


# ──────────────────────────────────────────────────────────────────
# W0-P2: Bridged option chain endpoints (IB ↔ Polygon)
# ──────────────────────────────────────────────────────────────────

_CHAIN_SNAPSHOT_COLS = (
    "snapshot_ts",
    "iv",
    "delta",
    "gamma",
    "theta",
    "vega",
    "open_interest",
    "underlying_ticker",
    "day_open",
    "day_high",
    "day_low",
    "day_close",
    "day_previous_close",
    "day_change_percent",
    "day_volume",
    "day_vwap",
    "fetched_at",
)

_CHAIN_SELECT = """
    s.snapshot_ts,
    s.iv, s.delta, s.gamma, s.theta, s.vega, s.open_interest,
    s.underlying AS underlying_ticker,
    s.day_open, s.day_high, s.day_low, s.day_close,
    s.day_previous_close,
    s.day_change_percent,
    s.day_volume, s.day_vwap,
    s.fetched_at,
    oc.option_ticker AS _option_ticker,
    oc.underlying AS _underlying,
    oc.expiry AS _expiry,
    oc.strike AS _strike,
    oc.option_right AS _option_right
"""

_IB_CONTRACT_JOIN = """
    FROM unnest(%s::text[], %s::date[], %s::text[], %s::float8[], %s::text[])
      AS req(underlying, expiry, option_right, strike, ib_key)
    JOIN market.option_contract oc
      ON oc.underlying = req.underlying
     AND oc.expiry = req.expiry
     AND oc.option_right = req.option_right
     AND abs(oc.strike - req.strike) < 1e-4
"""


def _map_row_to_ib_key(
    row: Dict[str, Any],
    *,
    ib_by_identity: Dict[Tuple, str],
    poly_requested: set,
) -> Optional[Dict[str, Any]]:
    """Attach IB ``contract_key``; drop internal bridge columns."""
    out = dict(row)
    req_ib_key = out.pop("_req_ib_key", None)
    underlying = out.pop("_underlying", None) or out.get("underlying_ticker")
    expiry = out.pop("_expiry", None)
    strike = out.pop("_strike", None)
    option_right = out.pop("_option_right", None)
    option_ticker = out.pop("_option_ticker", None)

    ck: Optional[str] = None
    if req_ib_key:
        ck = str(req_ib_key)
    elif underlying is not None and expiry is not None and strike is not None and option_right:
        try:
            exp_d = expiry if isinstance(expiry, date) else date.fromisoformat(str(expiry)[:10])
            ident = identity_key(str(underlying), exp_d, float(strike), str(option_right))
            ck = ib_by_identity.get(ident)
            if ck is None and option_ticker in poly_requested:
                ck = ib_contract_key_from_parts(
                    str(underlying), exp_d, float(strike), str(option_right)
                )
        except (TypeError, ValueError):
            ck = None
    if ck is None:
        return None

    for key in ("snapshot_ts", "fetched_at"):
        if key in out and out[key] is not None:
            out[key] = iso_value(out[key])
    for key in ("expiry",):
        if key in out and out[key] is not None:
            d = as_date(out[key])
            if d is not None:
                out[key] = d.isoformat()

    out["contract_key"] = ck
    return out


def _fetch_chain_latest(conn: Any, keys: List[str]) -> List[Dict[str, Any]]:
    """Bridged latest snapshot per contract key (IB + Polygon mixed)."""
    polygon, ib_parts = split_contract_keys(keys)
    if not polygon and not ib_parts:
        return []

    ib_by_ident = {
        identity_key(p.underlying, p.expiry, p.strike, p.option_right): p.original_key
        for p in ib_parts
    }
    poly_requested = set(polygon)
    rows: List[Dict[str, Any]] = []

    use_view = view_exists(conn, "market", "v_option_chain_latest")

    with conn.cursor() as cur:
        if polygon:
            if use_view:
                cur.execute(
                    f"""
                    SELECT {_CHAIN_SELECT}
                    FROM market.v_option_chain_latest s
                    JOIN market.option_contract oc ON oc.option_ticker = s.option_ticker
                    WHERE s.option_ticker = ANY(%s)
                    """,
                    (polygon,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (s.option_ticker)
                        {_CHAIN_SELECT}
                    FROM market.option_snapshot s
                    JOIN market.option_contract oc ON oc.option_ticker = s.option_ticker
                    WHERE s.option_ticker = ANY(%s)
                    ORDER BY s.option_ticker, s.snapshot_ts DESC
                    """,
                    (polygon,),
                )
            for r in cur.fetchall() or []:
                d = dict(r) if isinstance(r, Mapping) else _tuple_to_chain_dict(r)
                rows.append(d)

        if ib_parts:
            underlyings = [p.underlying for p in ib_parts]
            expiries = [p.expiry for p in ib_parts]
            strikes = [p.strike for p in ib_parts]
            rights = [p.option_right for p in ib_parts]
            ib_keys = [p.original_key for p in ib_parts]
            ib_params = (underlyings, expiries, rights, strikes, ib_keys)
            select_ib = _CHAIN_SELECT + ",\n    req.ib_key AS _req_ib_key"

            if use_view:
                cur.execute(
                    f"""
                    SELECT {select_ib}
                    {_IB_CONTRACT_JOIN}
                    JOIN market.v_option_chain_latest s ON s.option_ticker = oc.option_ticker
                    """,
                    ib_params,
                )
            else:
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (oc.option_ticker)
                        {select_ib}
                    {_IB_CONTRACT_JOIN}
                    JOIN market.option_snapshot s ON s.option_ticker = oc.option_ticker
                    ORDER BY oc.option_ticker, s.snapshot_ts DESC
                    """,
                    ib_params,
                )
            for r in cur.fetchall() or []:
                d = dict(r) if isinstance(r, Mapping) else _tuple_to_chain_dict(r, has_ib_key=True)
                rows.append(d)

    out: List[Dict[str, Any]] = []
    seen_ck: set = set()
    for row in rows:
        mapped = _map_row_to_ib_key(row, ib_by_identity=ib_by_ident, poly_requested=poly_requested)
        if mapped is None:
            continue
        ck = mapped["contract_key"]
        if ck in seen_ck:
            continue
        seen_ck.add(ck)
        out.append(mapped)
    return out


def _fetch_chain_eod(
    conn: Any,
    keys: List[str],
    since_ts: datetime,
) -> List[Dict[str, Any]]:
    """One snapshot per calendar day (America/New_York) per contract, bridged."""
    polygon, ib_parts = split_contract_keys(keys)
    if not polygon and not ib_parts:
        return []

    ib_by_ident = {
        identity_key(p.underlying, p.expiry, p.strike, p.option_right): p.original_key
        for p in ib_parts
    }
    poly_requested = set(polygon)

    use_view = (
        table_exists(conn, "market", "v_option_snapshot_with_stock")
        or view_exists(conn, "market", "v_option_snapshot_with_stock")
    )
    snapshot_table = "market.v_option_snapshot_with_stock" if use_view else "market.option_snapshot"
    price_col = "v.underlying_price" if use_view else "NULL::double precision AS underlying_price"

    out: List[Dict[str, Any]] = []

    def _append_mapped(raw_rows: list) -> None:
        for row in raw_rows:
            d = dict(row) if isinstance(row, Mapping) else {}
            req_ib_key = d.pop("_req_ib_key", None)
            underlying = d.pop("_underlying", None)
            expiry = d.pop("_expiry", None)
            strike = d.pop("_strike", None)
            option_right = d.pop("_option_right", None)
            option_ticker = d.pop("_option_ticker", None)

            ck: Optional[str] = None
            if req_ib_key:
                ck = str(req_ib_key)
            elif underlying and expiry is not None and strike is not None and option_right:
                try:
                    exp_d = expiry if isinstance(expiry, date) else date.fromisoformat(str(expiry)[:10])
                    ident = identity_key(str(underlying), exp_d, float(strike), str(option_right))
                    ck = ib_by_ident.get(ident)
                    if ck is None and option_ticker in poly_requested:
                        ck = ib_contract_key_from_parts(str(underlying), exp_d, float(strike), str(option_right))
                except (TypeError, ValueError):
                    ck = None
            if ck is None:
                continue
            d["contract_key"] = ck
            if "snap_day" in d and d["snap_day"] is not None:
                sd = d["snap_day"]
                d["snap_day"] = sd.isoformat() if hasattr(sd, "isoformat") else str(sd)
            if "snapshot_ts" in d and d["snapshot_ts"] is not None:
                d["snapshot_ts"] = iso_value(d["snapshot_ts"])
            out.append(d)

    with conn.cursor() as cur:
        if polygon:
            cur.execute(
                f"""
                SELECT DISTINCT ON (
                  DATE(timezone('America/New_York', v.snapshot_ts)),
                  oc.option_ticker
                )
                  DATE(timezone('America/New_York', v.snapshot_ts)) AS snap_day,
                  v.iv,
                  {price_col},
                  v.snapshot_ts,
                  oc.option_ticker AS _option_ticker,
                  oc.underlying AS _underlying,
                  oc.expiry AS _expiry,
                  oc.strike AS _strike,
                  oc.option_right AS _option_right
                FROM {snapshot_table} v
                JOIN market.option_contract oc ON oc.option_ticker = v.option_ticker
                WHERE v.option_ticker = ANY(%s)
                  AND v.snapshot_ts >= %s
                ORDER BY
                  DATE(timezone('America/New_York', v.snapshot_ts)),
                  oc.option_ticker,
                  v.snapshot_ts DESC
                """,
                (polygon, since_ts),
            )
            _append_mapped(cur.fetchall() or [])

        if ib_parts:
            underlyings = [p.underlying for p in ib_parts]
            expiries = [p.expiry for p in ib_parts]
            strikes = [p.strike for p in ib_parts]
            rights = [p.option_right for p in ib_parts]
            ib_keys = [p.original_key for p in ib_parts]
            cur.execute(
                f"""
                SELECT DISTINCT ON (
                  DATE(timezone('America/New_York', v.snapshot_ts)),
                  oc.option_ticker
                )
                  DATE(timezone('America/New_York', v.snapshot_ts)) AS snap_day,
                  v.iv,
                  {price_col},
                  v.snapshot_ts,
                  oc.option_ticker AS _option_ticker,
                  oc.underlying AS _underlying,
                  oc.expiry AS _expiry,
                  oc.strike AS _strike,
                  oc.option_right AS _option_right,
                  req.ib_key AS _req_ib_key
                {_IB_CONTRACT_JOIN}
                JOIN {snapshot_table} v ON v.option_ticker = oc.option_ticker
                WHERE v.snapshot_ts >= %s
                ORDER BY
                  DATE(timezone('America/New_York', v.snapshot_ts)),
                  oc.option_ticker,
                  v.snapshot_ts DESC
                """,
                (underlyings, expiries, rights, strikes, ib_keys, since_ts),
            )
            _append_mapped(cur.fetchall() or [])

    return out


def _tuple_to_chain_dict(row: Any, *, has_ib_key: bool = False) -> Dict[str, Any]:
    """Build dict from positional tuple matching ``_CHAIN_SELECT`` column order."""
    cols = list(_CHAIN_SNAPSHOT_COLS) + [
        "_option_ticker", "_underlying", "_expiry", "_strike", "_option_right",
    ]
    if has_ib_key:
        cols.append("_req_ib_key")
    return {cols[i]: row[i] for i in range(min(len(cols), len(row)))}


@router.get("/chain/latest")
def chain_latest(
    keys: str = Query(..., description="Comma-separated contract keys (IB or Polygon)"),
) -> Dict[str, Any]:
    """Latest snapshot per contract, accepting mixed IB + Polygon keys.

    Returns IB-shaped ``contract_key`` in every row.
    """
    raw_keys = [k.strip() for k in keys.split(",") if k.strip()]
    if not raw_keys:
        raise HTTPException(status_code=400, detail="No valid keys provided")
    if len(raw_keys) > 120:
        raise HTTPException(status_code=400, detail="Max 120 keys per request")

    conn = require_db()
    try:
        if not table_exists(conn, "market", "option_snapshot") or not table_exists(conn, "market", "option_contract"):
            return {"ok": True, "rows": [], "count": 0, "note": "required tables missing"}
        rows = _fetch_chain_latest(conn, raw_keys)
    finally:
        conn.close()
    return {"ok": True, "rows": rows, "count": len(rows)}


@router.get("/chain/eod")
def chain_eod(
    keys: str = Query(..., description="Comma-separated contract keys (IB or Polygon)"),
    since: str | None = Query(None, description="Since datetime (ISO or YYYY-MM-DD)"),
) -> Dict[str, Any]:
    """One snapshot per calendar day per contract (America/New_York)."""
    raw_keys = [k.strip() for k in keys.split(",") if k.strip()]
    if not raw_keys:
        raise HTTPException(status_code=400, detail="No valid keys provided")
    if len(raw_keys) > 120:
        raise HTTPException(status_code=400, detail="Max 120 keys per request")

    since_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
    if since:
        try:
            since_ts = datetime.fromisoformat(since)
            if since_ts.tzinfo is None:
                since_ts = since_ts.replace(tzinfo=timezone.utc)
        except ValueError:
            d = as_date(since)
            if d is not None:
                since_ts = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

    conn = require_db()
    try:
        if not table_exists(conn, "market", "option_snapshot") or not table_exists(conn, "market", "option_contract"):
            return {"ok": True, "rows": [], "count": 0, "note": "required tables missing"}
        rows = _fetch_chain_eod(conn, raw_keys, since_ts)
    finally:
        conn.close()
    return {"ok": True, "rows": rows, "count": len(rows)}


@router.get("/contracts")
def option_contracts(
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str | None = Query(None, description="Expiry YYYYMMDD or YYYY-MM-DD"),
) -> Dict[str, Any]:
    """List option contracts with IB contract_key for each row."""
    sym = normalize_symbol(symbol)
    exp = _norm_expiry(expiry)
    conn = require_db()
    try:
        if not table_exists(conn, "market", "option_contract"):
            return {"ok": True, "contracts": [], "count": 0, "note": "table missing"}
        clauses = ["underlying = %s"]
        params: list = [sym]
        if exp is not None:
            clauses.append("expiry = %s")
            params.append(exp)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT option_ticker, underlying, expiry, strike, option_right
                FROM market.option_contract
                WHERE {' AND '.join(clauses)}
                ORDER BY expiry, strike, option_right
                """,
                tuple(params),
            )
            raw = cur.fetchall() or []
    finally:
        conn.close()

    contracts: List[Dict[str, Any]] = []
    for r in raw:
        if isinstance(r, Mapping):
            ot, und, ex, st, oright = r["option_ticker"], r["underlying"], r["expiry"], r["strike"], r["option_right"]
        else:
            ot, und, ex, st, oright = r[0], r[1], r[2], r[3], r[4]
        exp_d = ex if isinstance(ex, date) else as_date(ex)
        ib_ck = ib_contract_key_from_parts(str(und), exp_d, float(st), str(oright)) if exp_d else None
        contracts.append({
            "option_ticker": str(ot),
            "underlying": str(und),
            "expiry": exp_d.isoformat() if exp_d else str(ex),
            "strike": float(st),
            "option_right": str(oright),
            "ib_contract_key": ib_ck,
        })
    return {"ok": True, "contracts": contracts, "count": len(contracts)}


@router.get("/strikes")
def option_strikes(
    symbol: str = Query(..., description="Underlying symbol"),
    expiry: str = Query(..., description="Expiry YYYYMMDD or YYYY-MM-DD"),
) -> Dict[str, Any]:
    """Sorted distinct strikes for a symbol + expiry."""
    sym = normalize_symbol(symbol)
    exp = _norm_expiry(expiry)
    if exp is None:
        raise HTTPException(status_code=400, detail="Invalid expiry format")
    conn = require_db()
    try:
        if not table_exists(conn, "market", "option_contract"):
            return {"ok": True, "strikes": [], "count": 0, "note": "table missing"}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT strike FROM market.option_contract
                WHERE underlying = %s AND expiry = %s
                ORDER BY strike
                """,
                (sym, exp),
            )
            raw = cur.fetchall() or []
    finally:
        conn.close()
    strikes: List[float] = []
    for r in raw:
        try:
            strikes.append(float(r[0] if not isinstance(r, Mapping) else r["strike"]))
        except (TypeError, ValueError, KeyError):
            continue
    return {"ok": True, "strikes": strikes, "count": len(strikes)}


@router.get("/expirations/yyyymmdd")
def option_expirations_yyyymmdd(
    symbol: str = Query(..., description="Underlying symbol"),
) -> Dict[str, Any]:
    """Expirations in YYYYMMDD format from ``market.option_contract``."""
    sym = normalize_symbol(symbol)
    conn = require_db()
    try:
        if not table_exists(conn, "market", "option_contract"):
            return {"ok": True, "expirations": [], "count": 0, "note": "table missing"}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT expiry FROM market.option_contract
                WHERE underlying = %s
                ORDER BY expiry
                """,
                (sym,),
            )
            raw = cur.fetchall() or []
    finally:
        conn.close()
    expirations: List[str] = []
    for r in raw:
        v = r[0] if not isinstance(r, Mapping) else r["expiry"]
        if v is None:
            continue
        if hasattr(v, "strftime"):
            expirations.append(v.strftime("%Y%m%d"))
        else:
            s = str(v).strip()
            if len(s) >= 10 and s[4] == "-":
                expirations.append(s[:4] + s[5:7] + s[8:10])
            else:
                expirations.append(s)
    return {"ok": True, "expirations": expirations, "count": len(expirations)}
