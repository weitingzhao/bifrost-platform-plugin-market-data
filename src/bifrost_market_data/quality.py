"""Data quality checks for market.* + data_ops.ingest_freshness (P7)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from bifrost_market_data.scheduler.daily import resolve_watchlist_symbols_for_coverage

# Acceptance thresholds (program YAML)
STOCK_DAILY_MIN_SYMBOLS = 4000
STOCK_DAILY_GAP_LOOKBACK_DAYS = 30
FRESHNESS_MAX_AGE_HOURS = 24.0

# Dimensions expected to be actively refreshed by daily CronJobs.
EXPECTED_FRESHNESS_DIMENSIONS = (
    "stock_daily",
    "option_snapshot",
    "option_open_interest",
    "calendar",
)


def fetch_stock_daily_symbol_count(conn: Any) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT symbol) FROM market.stock_daily")
        row = cur.fetchone()
    if row is None:
        return 0
    if isinstance(row, Mapping):
        return int(next(iter(row.values())))
    return int(row[0] or 0)


def fetch_recent_trading_days(conn: Any, n: int, *, as_of: date | None = None) -> list[date]:
    """Return up to ``n`` most recent NYSE trading days (weekday − closed holidays)."""
    from bifrost_market_data.trading_calendar import fetch_recent_trading_days as _fetch

    return _fetch(conn, n, as_of=as_of)


def fetch_completed_trading_days(
    conn: Any,
    n: int,
    *,
    as_of: date | None = None,
) -> list[date]:
    """Return ``n`` most recent *completed* sessions for gap acceptance.

    When ``as_of`` is omitted (live probe), the calendar ``today`` is still an
    open session — EOD bars/OI are not expected yet — so it is excluded.
    Explicit historical ``as_of`` keeps that date in the window (tests / backfill).
    """
    today = datetime.now(timezone.utc).date()
    end = as_of or today
    live_probe = as_of is None or as_of >= today
    days = fetch_recent_trading_days(conn, n + (1 if live_probe else 0), as_of=end)
    if live_probe and days and days[-1] == end:
        days = days[:-1]
    if len(days) > n:
        days = days[-n:]
    return days


def filter_optionable_underlyings(conn: Any, symbols: Sequence[str]) -> list[str]:
    """Watchlist symbols that have ≥1 row in ``market.option_contract``.

    Equity-only names (e.g. SATS with zero contracts) must not fail option
    snapshot / OI acceptance.
    """
    syms = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
    if not syms:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT UPPER(TRIM(underlying)) AS und
                FROM market.option_contract
                WHERE UPPER(TRIM(underlying)) = ANY(%s)
                """,
                (syms,),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception:
        return syms
    found: set[str] = set()
    for row in rows or []:
        if isinstance(row, Mapping):
            und = row.get("und") or next(iter(row.values()), None)
        else:
            und = row[0] if row else None
        if und:
            found.add(str(und).strip().upper())
    return [s for s in syms if s in found]


def check_stock_daily_coverage(
    conn: Any,
    *,
    min_symbols: int = STOCK_DAILY_MIN_SYMBOLS,
    lookback_days: int = STOCK_DAILY_GAP_LOOKBACK_DAYS,
    watchlist_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Check symbol count and date gaps for watchlist over recent trading days."""
    symbol_count = fetch_stock_daily_symbol_count(conn)
    symbols = (
        list(watchlist_symbols)
        if watchlist_symbols is not None
        else resolve_watchlist_symbols_for_coverage(conn)
    )
    trading_days = fetch_completed_trading_days(conn, lookback_days)
    gaps: list[dict[str, Any]] = []

    if symbols and trading_days:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, bar_date
                FROM market.stock_daily
                WHERE symbol = ANY(%s)
                  AND bar_date >= %s
                  AND bar_date <= %s
                """,
                (list(symbols), trading_days[0], trading_days[-1]),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
        present: set[tuple[str, date]] = set()
        for row in rows or []:
            if isinstance(row, Mapping):
                sym = str(row.get("symbol") or "")
                bd = row.get("bar_date")
            else:
                sym = str(row[0] or "")
                bd = row[1]
            if isinstance(bd, datetime):
                bd = bd.date()
            if sym and isinstance(bd, date):
                present.add((sym.upper(), bd))
        for sym in symbols:
            for d in trading_days:
                if (sym, d) not in present:
                    gaps.append({"symbol": sym, "bar_date": d.isoformat()})

    ok = symbol_count > min_symbols and len(gaps) == 0
    return {
        "check": "stock_daily_coverage",
        "ok": ok,
        "symbol_count": symbol_count,
        "min_symbols": min_symbols,
        "watchlist_symbols": len(symbols),
        "trading_days": [d.isoformat() for d in trading_days],
        "gap_count": len(gaps),
        "gaps_sample": gaps[:20],
        "detail": (
            f"symbols={symbol_count} (need >{min_symbols}); "
            f"gaps={len(gaps)} over {len(trading_days)} trading days × {len(symbols)} watchlist"
        ),
    }


def check_option_snapshot_coverage(
    conn: Any,
    *,
    watchlist_symbols: Sequence[str] | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Optionable watchlist underlyings should have a snapshot on the last completed session."""
    raw_symbols = (
        list(watchlist_symbols)
        if watchlist_symbols is not None
        else resolve_watchlist_symbols_for_coverage(conn)
    )
    symbols = filter_optionable_underlyings(conn, raw_symbols)
    trading_days = fetch_completed_trading_days(conn, 1, as_of=as_of)
    if not trading_days:
        return {
            "check": "option_snapshot_coverage",
            "ok": False,
            "missing": list(symbols),
            "detail": "no completed trading day available",
        }
    target = trading_days[-1]
    # Snapshots use timestamptz; match NY calendar day via date(snapshot_ts AT TIME ZONE 'America/New_York')
    missing: list[str] = []
    if symbols:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT underlying
                FROM market.option_snapshot
                WHERE underlying = ANY(%s)
                  AND (snapshot_ts AT TIME ZONE 'America/New_York')::date = %s
                """,
                (list(symbols), target),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
        found: set[str] = set()
        for row in rows or []:
            if isinstance(row, Mapping):
                found.add(str(row.get("underlying") or next(iter(row.values()), "")).upper())
            else:
                found.add(str(row[0] or "").upper())
        missing = [s for s in symbols if s not in found]

    skipped = len(raw_symbols) - len(symbols)
    if len(raw_symbols) > 0 and len(symbols) == 0:
        ok = True  # equity-only watchlist: option checks N/A
    else:
        ok = len(missing) == 0 and (len(symbols) > 0 or len(raw_symbols) == 0)
    return {
        "check": "option_snapshot_coverage",
        "ok": ok,
        "target_date": target.isoformat(),
        "watchlist_symbols": len(raw_symbols),
        "optionable_symbols": len(symbols),
        "skipped_non_optionable": skipped,
        "missing_count": len(missing),
        "missing_sample": missing[:20],
        "detail": (
            f"target={target.isoformat()}; "
            f"missing={len(missing)}/{len(symbols)} optionable "
            f"(skipped {skipped} equity-only)"
        ),
    }


def check_option_oi_coverage(
    conn: Any,
    *,
    watchlist_symbols: Sequence[str] | None = None,
    lookback_days: int = 14,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Optionable underlyings should have ≥1 OI row per completed trading day."""
    raw_symbols = (
        list(watchlist_symbols)
        if watchlist_symbols is not None
        else resolve_watchlist_symbols_for_coverage(conn)
    )
    symbols = filter_optionable_underlyings(conn, raw_symbols)
    trading_days = fetch_completed_trading_days(conn, lookback_days, as_of=as_of)
    gaps: list[dict[str, Any]] = []

    if symbols and trading_days:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT underlying, trade_date
                FROM market.option_open_interest
                WHERE underlying = ANY(%s)
                  AND trade_date >= %s
                  AND trade_date <= %s
                GROUP BY underlying, trade_date
                """,
                (list(symbols), trading_days[0], trading_days[-1]),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
        present: set[tuple[str, date]] = set()
        for row in rows or []:
            if isinstance(row, Mapping):
                und = str(row.get("underlying") or "")
                td = row.get("trade_date")
            else:
                und = str(row[0] or "")
                td = row[1]
            if isinstance(td, datetime):
                td = td.date()
            if und and isinstance(td, date):
                present.add((und.upper(), td))
        for sym in symbols:
            for d in trading_days:
                if (sym, d) not in present:
                    gaps.append({"underlying": sym, "trade_date": d.isoformat()})

    skipped = len(raw_symbols) - len(symbols)
    if len(raw_symbols) > 0 and len(symbols) == 0:
        ok = True
    else:
        ok = len(symbols) > 0 and len(gaps) == 0
    return {
        "check": "option_oi_coverage",
        "ok": ok,
        "watchlist_symbols": len(raw_symbols),
        "optionable_symbols": len(symbols),
        "skipped_non_optionable": skipped,
        "trading_days": [d.isoformat() for d in trading_days],
        "gap_count": len(gaps),
        "gaps_sample": gaps[:20],
        "detail": (
            f"gaps={len(gaps)} over {len(trading_days)} trading days × "
            f"{len(symbols)} optionable (skipped {skipped} equity-only)"
        ),
    }


def check_freshness(
    conn: Any,
    *,
    max_age_hours: float = FRESHNESS_MAX_AGE_HOURS,
    expected_dimensions: Sequence[str] = EXPECTED_FRESHNESS_DIMENSIONS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """All expected dimensions must be present, status=ok, and age < max_age_hours."""
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dimension, last_run_at, rows_written, status, updated_at
            FROM data_ops.ingest_freshness
            """
        )
        rows = cur.fetchall() if hasattr(cur, "fetchall") else []

    by_dim: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if isinstance(row, Mapping):
            dim = str(row.get("dimension") or "")
            last = row.get("last_run_at")
            status = str(row.get("status") or "unknown")
            rows_w = int(row.get("rows_written") or 0)
        else:
            dim = str(row[0] or "")
            last = row[1]
            rows_w = int(row[2] or 0)
            status = str(row[3] or "unknown")
        if not dim:
            continue
        age_hours: float | None = None
        if isinstance(last, datetime):
            last_utc = last if last.tzinfo else last.replace(tzinfo=timezone.utc)
            age_hours = max(0.0, (now_utc - last_utc.astimezone(timezone.utc)).total_seconds() / 3600.0)
        by_dim[dim] = {
            "dimension": dim,
            "last_run_at": last.isoformat() if isinstance(last, datetime) else None,
            "rows_written": rows_w,
            "status": status,
            "age_hours": age_hours,
        }

    failures: list[str] = []
    details: list[dict[str, Any]] = []
    for dim in expected_dimensions:
        info = by_dim.get(dim)
        if info is None:
            failures.append(f"{dim}: missing")
            details.append({"dimension": dim, "ok": False, "detail": "missing"})
            continue
        age = info.get("age_hours")
        status = str(info.get("status") or "")
        ok_dim = status == "ok" and age is not None and float(age) < float(max_age_hours)
        if not ok_dim:
            failures.append(
                f"{dim}: status={status} age_hours={age}"
            )
        details.append(
            {
                **info,
                "ok": ok_dim,
                "max_age_hours": max_age_hours,
            }
        )

    ok = len(failures) == 0
    return {
        "check": "freshness",
        "ok": ok,
        "max_age_hours": max_age_hours,
        "dimensions": details,
        "failures": failures,
        "detail": "ok" if ok else "; ".join(failures),
    }


def run_all_checks(
    conn: Any,
    *,
    watchlist_symbols: Sequence[str] | None = None,
    min_symbols: int = STOCK_DAILY_MIN_SYMBOLS,
    lookback_days: int = STOCK_DAILY_GAP_LOOKBACK_DAYS,
    max_age_hours: float = FRESHNESS_MAX_AGE_HOURS,
) -> dict[str, Any]:
    """Run all P7 quality checks. Returns report with ``ok`` aggregate flag."""
    symbols = (
        list(watchlist_symbols)
        if watchlist_symbols is not None
        else resolve_watchlist_symbols_for_coverage(conn)
    )
    stock = check_stock_daily_coverage(
        conn,
        min_symbols=min_symbols,
        lookback_days=lookback_days,
        watchlist_symbols=symbols,
    )
    snaps = check_option_snapshot_coverage(conn, watchlist_symbols=symbols)
    oi = check_option_oi_coverage(
        conn,
        watchlist_symbols=symbols,
        lookback_days=lookback_days,
    )
    fresh = check_freshness(conn, max_age_hours=max_age_hours)
    checks = [stock, snaps, oi, fresh]
    ok = all(bool(c.get("ok")) for c in checks)
    return {
        "ok": ok,
        "checks": checks,
        "summary": "PASS" if ok else "FAIL",
        "watchlist_source_count": len(symbols),
    }
