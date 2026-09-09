"""Data quality checks for market.* + ops_jobs.ingest_freshness (P7)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from bifrost_market_data.contracts import deadline_for_dimension
from bifrost_market_data.session import is_late, resolve_session
from bifrost_market_data.scheduler.daily import resolve_watchlist_symbols_for_coverage

# Acceptance thresholds (program YAML)
STOCK_DAILY_MIN_SYMBOLS = 4000
STOCK_DAILY_GAP_LOOKBACK_DAYS = 30
# Kept for callers that still pass an explicit override; the gate itself now
# measures against the session and each dataset's own contract deadline.
FRESHNESS_MAX_AGE_HOURS = 24.0
FRESHNESS_WEEKEND_MAX_AGE_HOURS = 72.0

# Dimensions expected to be actively refreshed by daily CronJobs.
EXPECTED_FRESHNESS_DIMENSIONS = (
    "stock_daily",
    "option_snapshot",
    "option_open_interest",
    "calendar",
)


def freshness_age_limit_hours(now: datetime) -> float:
    """Deprecated: a weekday rule standing in for a trading calendar.

    It answered "is this stale" with 24 hours, or 72 across a weekend so a
    Friday night snapshot would not fail on Monday morning — an approximation
    of the session that ``session.resolve_session`` knows exactly. Kept only
    for callers that have not moved; ``check_freshness`` no longer uses it.
    """
    wd = now.weekday()  # Mon=0 … Sun=6
    if wd >= 5 or (wd == 0 and now.hour < 22):
        return FRESHNESS_WEEKEND_MAX_AGE_HOURS
    return FRESHNESS_MAX_AGE_HOURS


def fetch_stock_daily_symbol_count(conn: Any) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT symbol) FROM raw_market.stock_daily")
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
                FROM raw_market.option_contract
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
                FROM raw_market.stock_daily
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
    """Optionable watchlist underlyings should have a recent session snapshot.

    Polygon option snapshots are point-in-time: a catch-up job today cannot
    rewrite yesterday's ``snapshot_ts``. For live probes we therefore accept
    any NY session day from the last completed target through today (inclusive)
    so a same-day heal clears a prior miss. Historical ``as_of`` keeps the
    exact-day contract for tests / backfill audits.
    """
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
    if as_of is None:
        try:
            from zoneinfo import ZoneInfo

            today_ny = datetime.now(ZoneInfo("America/New_York")).date()
        except Exception:
            today_ny = datetime.now(timezone.utc).date()
        window_end = max(target, today_ny)
    else:
        window_end = target

    missing: list[str] = []
    if symbols:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT underlying
                FROM raw_market.option_snapshot
                WHERE underlying = ANY(%s)
                  AND (snapshot_ts AT TIME ZONE 'America/New_York')::date >= %s
                  AND (snapshot_ts AT TIME ZONE 'America/New_York')::date <= %s
                """,
                (list(symbols), target, window_end),
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
    window_note = (
        f"window={target.isoformat()}…{window_end.isoformat()}"
        if window_end != target
        else f"target={target.isoformat()}"
    )
    return {
        "check": "option_snapshot_coverage",
        "ok": ok,
        "target_date": target.isoformat(),
        "window_end": window_end.isoformat(),
        "watchlist_symbols": len(raw_symbols),
        "optionable_symbols": len(symbols),
        "skipped_non_optionable": skipped,
        "missing_count": len(missing),
        "missing_sample": missing[:20],
        "detail": (
            f"{window_note}; "
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
                FROM raw_market.option_open_interest
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
    max_age_hours: float | None = None,
    expected_dimensions: Sequence[str] = EXPECTED_FRESHNESS_DIMENSIONS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """All expected dimensions must be present, status=ok, and not late.

    Late is measured against the session the tables should hold and each
    dataset's own contract deadline, not against a flat age: a feed published
    the morning after its session is not missing on the night of it, which is
    why the flat rule needed a weekend exception to stop failing every Friday.
    ``max_age_hours`` still forces the old behaviour for callers that pass it.
    """
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    session_day, _is_today = resolve_session(conn, now_utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dimension, last_run_at, rows_written, status, updated_at
            FROM ops_jobs.ingest_freshness
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
            age_hours = max(
                0.0, (now_utc - last_utc.astimezone(timezone.utc)).total_seconds() / 3600.0
            )
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
        hours = (
            float(max_age_hours)
            if max_age_hours is not None
            else (deadline_for_dimension(dim) or FRESHNESS_MAX_AGE_HOURS)
        )
        last_run = info.get("last_run_at")
        last_dt = datetime.fromisoformat(last_run) if isinstance(last_run, str) else None
        ok_dim = status == "ok" and not is_late(last_dt, session_day, hours, now_utc)
        if not ok_dim:
            failures.append(
                f"{dim}: status={status} age_hours={age} deadline={hours}h session={session_day}"
            )
        details.append(
            {
                **info,
                "ok": ok_dim,
                "deadline_hours": hours,
            }
        )

    ok = len(failures) == 0
    return {
        "check": "freshness",
        "ok": ok,
        "session": session_day.isoformat(),
        # Per dimension now — one flat number was the thing being removed.
        "max_age_hours": float(max_age_hours) if max_age_hours is not None else None,
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
    max_age_hours: float | None = None,
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
