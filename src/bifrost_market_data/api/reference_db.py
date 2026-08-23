"""DB-read reference ticker routes under ``/market/reference/*`` (Wave 5-B)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import (
    connect_db,
    normalize_symbol,
    require_db,
    row_dict,
    safe_count,
    table_exists,
)

router = APIRouter(prefix="/reference", tags=["reference-db"])

_HAS_RELATED = "market.ticker_related"


def _related_table_exists(conn: Any) -> bool:
    return table_exists(conn, "market", "ticker_related")


def _overview_missing_predicate() -> str:
    """Merged ``market.ticker`` row lacks core overview fields."""
    return """
        name IS NULL OR TRIM(name) = ''
        OR description IS NULL OR TRIM(description) = ''
        OR market_cap IS NULL
    """


def query_ticker_search(conn: Any, *, q: str, limit: int) -> list[dict[str, Any]]:
    if not table_exists(conn, "market", "ticker"):
        return []
    needle = str(q or "").strip()
    if not needle:
        return []
    pattern = f"{needle.upper()}%"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, name, market, locale, primary_exchange, instrument_type, active
            FROM raw_market.ticker
            WHERE UPPER(symbol) LIKE %s
               OR name ILIKE %s
            ORDER BY
                CASE WHEN UPPER(symbol) = UPPER(%s) THEN 0
                     WHEN UPPER(symbol) LIKE %s THEN 1
                     ELSE 2 END,
                symbol ASC
            LIMIT %s
            """,
            (pattern, f"%{needle}%", needle, pattern, limit),
        )
        raw = cur.fetchall() or []
    cols = ("symbol", "name", "market", "locale", "primary_exchange", "instrument_type", "active")
    return [row_dict(r, cols) for r in raw]


def query_overview_coverage(conn: Any) -> dict[str, Any]:
    if not table_exists(conn, "market", "ticker"):
        return {"ok": True, "total": 0, "filled": 0, "missing": 0}
    pred = _overview_missing_predicate()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*)::bigint FROM raw_market.ticker")
        total = int((cur.fetchone() or [0])[0])
        cur.execute(f"SELECT COUNT(*)::bigint FROM raw_market.ticker WHERE NOT ({pred})")
        filled = int((cur.fetchone() or [0])[0])
    missing = max(0, total - filled)
    return {"ok": True, "total": total, "filled": filled, "missing": missing, "source": "market.ticker"}


def query_missing_overview(conn: Any, *, limit: int, offset: int) -> dict[str, Any]:
    counts = query_overview_coverage(conn)
    if not table_exists(conn, "market", "ticker"):
        return {"ok": True, "tickers": [], "limit": limit, "offset": offset, **counts}
    pred = _overview_missing_predicate()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT symbol
            FROM raw_market.ticker
            WHERE {pred}
            ORDER BY symbol ASC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        tickers = [str(r[0]).upper() for r in (cur.fetchall() or []) if r and r[0]]
    total_missing = int(counts.get("missing") or 0)
    loaded = offset + len(tickers)
    return {
        "ok": True,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "total_missing": total_missing,
        "has_more": total_missing > 0 and loaded < total_missing,
    }


def query_related_coverage(conn: Any) -> dict[str, Any]:
    if not _related_table_exists(conn):
        total = safe_count(conn, "market.ticker") or 0
        return {
            "ok": True,
            "total": total,
            "filled": 0,
            "missing": total,
            "source": "db",
            "note": "market.ticker_related not present; related coverage unavailable.",
        }
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*)::bigint FROM raw_market.ticker")
        total = int((cur.fetchone() or [0])[0])
        cur.execute(
            """
            SELECT COUNT(DISTINCT UPPER(TRIM(from_symbol)))::bigint
            FROM raw_market.ticker_related
            WHERE TRIM(COALESCE(from_symbol, '')) <> ''
            """
        )
        filled = int((cur.fetchone() or [0])[0])
    missing = max(0, total - filled)
    return {"ok": True, "total": total, "filled": filled, "missing": missing, "source": _HAS_RELATED}


def query_missing_related(conn: Any, *, limit: int, offset: int) -> dict[str, Any]:
    counts = query_related_coverage(conn)
    if not _related_table_exists(conn) or not table_exists(conn, "market", "ticker"):
        return {
            "ok": True,
            "tickers": [],
            "limit": limit,
            "offset": offset,
            "total_missing": int(counts.get("missing") or 0),
            "has_more": False,
        }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.symbol
            FROM raw_market.ticker t
            LEFT JOIN (
                SELECT DISTINCT UPPER(TRIM(from_symbol)) AS sym
                FROM raw_market.ticker_related
            ) r ON r.sym = UPPER(TRIM(t.symbol))
            WHERE r.sym IS NULL
            ORDER BY t.symbol ASC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        tickers = [str(r[0]).upper() for r in (cur.fetchall() or []) if r and r[0]]
    total_missing = int(counts.get("missing") or 0)
    loaded = offset + len(tickers)
    return {
        "ok": True,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "total_missing": total_missing,
        "has_more": total_missing > 0 and loaded < total_missing,
    }


def query_filled_related(conn: Any, *, limit: int, offset: int) -> dict[str, Any]:
    counts = query_related_coverage(conn)
    if not _related_table_exists(conn):
        return {
            "ok": True,
            "tickers": [],
            "limit": limit,
            "offset": offset,
            "total_filled": 0,
            "has_more": False,
        }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT UPPER(TRIM(from_symbol)) AS sym
            FROM raw_market.ticker_related
            WHERE TRIM(COALESCE(from_symbol, '')) <> ''
            ORDER BY sym ASC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        tickers = [str(r[0]).upper() for r in (cur.fetchall() or []) if r and r[0]]
    total_filled = int(counts.get("filled") or 0)
    loaded = offset + len(tickers)
    return {
        "ok": True,
        "tickers": tickers,
        "limit": limit,
        "offset": offset,
        "total_filled": total_filled,
        "has_more": total_filled > 0 and loaded < total_filled,
    }


def query_universe_count(conn: Any) -> dict[str, Any]:
    n = safe_count(conn, "market.ticker")
    return {"ok": True, "total_tickers": n or 0, "source": "market.ticker"}


def query_us_equity_universe(conn: Any) -> dict[str, Any]:
    """US common-stock universe matching former public.v_us_equity_universe.

    Filters: active, locale=us, market=stocks, instrument_type=cs.
    tickers_id is hashtext(upper(trim(symbol)))::bigint for SEPA compatibility.
    """
    if not table_exists(conn, "market", "ticker"):
        return {"ok": True, "rows": [], "count": 0}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                hashtext(upper(trim(t.symbol)))::bigint AS tickers_id,
                upper(trim(t.symbol)) AS symbol,
                t.name,
                t.market,
                t.locale,
                t.primary_exchange,
                t.instrument_type,
                t.active,
                t.list_date::text AS list_date,
                t.sector,
                t.industry
            FROM raw_market.ticker t
            WHERE COALESCE(t.active, false) = true
              AND lower(COALESCE(t.locale, '')) = 'us'
              AND lower(COALESCE(t.market, '')) = 'stocks'
              AND lower(COALESCE(t.instrument_type, '')) = 'cs'
            ORDER BY 2
            """
        )
        raw = cur.fetchall() or []
    rows: list[dict[str, Any]] = []
    for r in raw:
        if hasattr(r, "keys"):
            rows.append(
                {
                    "tickers_id": int(r["tickers_id"]) if r["tickers_id"] is not None else None,
                    "symbol": str(r["symbol"]),
                    "name": r["name"],
                    "market": r["market"],
                    "locale": r["locale"],
                    "primary_exchange": r["primary_exchange"],
                    "instrument_type": r["instrument_type"],
                    "active": bool(r["active"]) if r["active"] is not None else None,
                    "delisted_utc": None,
                    "list_date": str(r["list_date"])[:10] if r["list_date"] else None,
                    "sector": r["sector"],
                    "industry": r["industry"],
                }
            )
        else:
            rows.append(
                {
                    "tickers_id": int(r[0]) if r[0] is not None else None,
                    "symbol": str(r[1]),
                    "name": r[2],
                    "market": r[3],
                    "locale": r[4],
                    "primary_exchange": r[5],
                    "instrument_type": r[6],
                    "active": bool(r[7]) if r[7] is not None else None,
                    "delisted_utc": None,
                    "list_date": str(r[8])[:10] if r[8] else None,
                    "sector": r[9],
                    "industry": r[10],
                }
            )
    return {"ok": True, "rows": rows, "count": len(rows)}


def query_ticker_types(conn: Any, *, asset_class: str = "*", locale: str = "*") -> dict[str, Any]:
    if table_exists(conn, "market", "ticker_type"):
        clauses: list[str] = []
        params: list[Any] = []
        if asset_class and asset_class != "*":
            clauses.append("asset_class = %s")
            params.append(asset_class)
        if locale and locale != "*":
            clauses.append("locale = %s")
            params.append(locale)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT asset_class, locale, code AS ticker_type, description
                FROM raw_market.ticker_type
                {where}
                ORDER BY asset_class, locale, code
                LIMIT 500
                """,
                tuple(params),
            )
            raw = cur.fetchall() or []
        cols = ("asset_class", "locale", "ticker_type", "description")
        rows = [row_dict(r, cols) for r in raw]
        return {"ok": True, "source": "market.ticker_type", "rows": rows, "count": len(rows)}

    if not table_exists(conn, "market", "ticker"):
        return {"ok": True, "source": "market.ticker.distinct", "rows": [], "count": 0}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(instrument_type), ''), 'unknown') AS ticker_type,
                COALESCE(NULLIF(TRIM(market), ''), '*') AS asset_class,
                COALESCE(NULLIF(TRIM(locale), ''), '*') AS locale,
                COUNT(*)::bigint AS symbol_count
            FROM raw_market.ticker
            GROUP BY 1, 2, 3
            ORDER BY symbol_count DESC, ticker_type ASC
            """
        )
        raw = cur.fetchall() or []
    rows = [
        {
            "ticker_type": r[0],
            "asset_class": r[1],
            "locale": r[2],
            "symbol_count": int(r[3] or 0),
        }
        for r in raw
    ]
    return {"ok": True, "source": "market.ticker.distinct", "rows": rows, "count": len(rows)}


def query_ticker_types_count(conn: Any) -> dict[str, Any]:
    if table_exists(conn, "market", "ticker_type"):
        n = safe_count(conn, "market.ticker_type") or 0
        return {"ok": True, "total_ticker_types": n, "source": "market.ticker_type"}
    data = query_ticker_types(conn)
    return {
        "ok": True,
        "total_ticker_types": data.get("count", 0),
        "source": data.get("source"),
    }


def query_ticker_related(conn: Any, *, ticker: str) -> dict[str, Any]:
    sym = normalize_symbol(ticker)
    if not sym:
        return {"ok": False, "error": "Invalid symbol"}
    if not _related_table_exists(conn):
        return {
            "ok": True,
            "source": "db",
            "symbol": sym,
            "related": [],
            "note": "market.ticker_related not present.",
        }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                UPPER(TRIM(r.to_symbol)) AS symbol,
                r.rank,
                t.name
            FROM raw_market.ticker_related r
            LEFT JOIN raw_market.ticker t ON t.symbol = UPPER(TRIM(r.to_symbol))
            WHERE UPPER(TRIM(r.from_symbol)) = %s
            ORDER BY r.rank ASC, r.to_symbol ASC
            LIMIT 200
            """,
            (sym,),
        )
        raw = cur.fetchall() or []
    related = [
        {"symbol": str(r[0]), "rank": r[1], "name": r[2]}
        for r in raw
        if r and r[0]
    ]
    return {"ok": True, "source": "db", "symbol": sym, "related": related}


_TICKER_COLS = (
    "symbol", "name", "market", "locale", "primary_exchange",
    "instrument_type", "active", "currency", "cik", "composite_figi",
    "sic_code", "sector", "industry", "market_cap", "list_date",
    "homepage_url", "total_employees", "description", "updated_at",
)


def query_ticker_single(conn: Any, *, symbol: str) -> dict[str, Any] | None:
    """Full row from market.ticker for a single symbol."""
    if not table_exists(conn, "market", "ticker"):
        return None
    sym = normalize_symbol(symbol)
    if not sym:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, name, market, locale, primary_exchange,
                   instrument_type, active, currency, cik, composite_figi,
                   sic_code, sector, industry, market_cap, list_date,
                   homepage_url, total_employees, description, updated_at
            FROM raw_market.ticker
            WHERE symbol = %s
            LIMIT 1
            """,
            (sym,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return row_dict(row, _TICKER_COLS)


def query_ticker_batch(conn: Any, *, symbols: list[str]) -> list[dict[str, Any]]:
    """Full rows from market.ticker for multiple symbols."""
    if not table_exists(conn, "market", "ticker"):
        return []
    if not symbols:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, name, market, locale, primary_exchange,
                   instrument_type, active, currency, cik, composite_figi,
                   sic_code, sector, industry, market_cap, list_date,
                   homepage_url, total_employees, description, updated_at
            FROM raw_market.ticker
            WHERE symbol = ANY(%s)
            ORDER BY symbol ASC
            """,
            (symbols,),
        )
        rows = cur.fetchall() or []
    return [row_dict(r, _TICKER_COLS) for r in rows]


@router.get("/ticker")
def reference_ticker_single(
    symbol: str = Query(..., max_length=20),
) -> dict[str, Any]:
    """Full ticker row for a single symbol from market.ticker."""
    conn = require_db()
    try:
        result = query_ticker_single(conn, symbol=symbol)
        if result is None:
            raise HTTPException(status_code=404, detail="Ticker not found")
        return {"ok": True, "ticker": result}
    finally:
        conn.close()


@router.get("/tickers/batch")
def reference_tickers_batch(
    symbols: str = Query(..., description="Comma-separated symbols"),
) -> dict[str, Any]:
    """Batch ticker lookup from market.ticker."""
    parsed = [normalize_symbol(s) for s in symbols.split(",") if normalize_symbol(s)]
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        tickers = query_ticker_batch(conn, symbols=parsed)
        return {"ok": True, "tickers": tickers, "count": len(tickers)}
    finally:
        conn.close()


@router.get("/tickers/search")
def reference_tickers_search(
    q: str = Query("", max_length=128),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    conn = require_db()
    try:
        results = query_ticker_search(conn, q=q, limit=limit)
        return {"ok": True, "cached": False, "results": results, "count": len(results)}
    finally:
        conn.close()


@router.get("/tickers/overview-coverage")
def reference_overview_coverage() -> dict[str, Any]:
    conn = require_db()
    try:
        return query_overview_coverage(conn)
    finally:
        conn.close()


@router.get("/tickers/missing-overview")
def reference_missing_overview(
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    conn = require_db()
    try:
        return query_missing_overview(conn, limit=limit, offset=offset)
    finally:
        conn.close()


@router.get("/tickers/related-coverage")
def reference_related_coverage() -> dict[str, Any]:
    conn = require_db()
    try:
        return query_related_coverage(conn)
    finally:
        conn.close()


@router.get("/tickers/missing-related")
def reference_missing_related(
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    conn = require_db()
    try:
        return query_missing_related(conn, limit=limit, offset=offset)
    finally:
        conn.close()


@router.get("/tickers/filled-related")
def reference_filled_related(
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    conn = require_db()
    try:
        return query_filled_related(conn, limit=limit, offset=offset)
    finally:
        conn.close()


@router.get("/tickers/universe-count")
def reference_universe_count() -> dict[str, Any]:
    conn = require_db()
    try:
        return query_universe_count(conn)
    finally:
        conn.close()


@router.get("/universe")
def reference_us_equity_universe() -> dict[str, Any]:
    """US CS universe rows matching former public.v_us_equity_universe."""
    conn = require_db()
    try:
        return query_us_equity_universe(conn)
    finally:
        conn.close()


@router.get("/ticker-types/count")
def reference_ticker_types_count() -> dict[str, Any]:
    conn = require_db()
    try:
        return query_ticker_types_count(conn)
    finally:
        conn.close()


@router.get("/ticker-types")
def reference_ticker_types(
    asset_class: str = Query("*"),
    locale: str = Query("*"),
) -> dict[str, Any]:
    conn = require_db()
    try:
        return query_ticker_types(conn, asset_class=asset_class, locale=locale)
    finally:
        conn.close()


@router.get("/tickers/{ticker}/related")
def reference_ticker_related(ticker: str) -> dict[str, Any]:
    conn = require_db()
    try:
        result = query_ticker_related(conn, ticker=ticker)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "bad request"))
        return result
    finally:
        conn.close()


_connect = connect_db
