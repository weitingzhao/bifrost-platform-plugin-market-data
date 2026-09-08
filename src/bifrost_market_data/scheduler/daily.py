"""Daily / EOD job generation — CronJob-driven enqueue into ops_jobs.job_ingest."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import yaml

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.ingest.index_options import storage_underlying
from bifrost_market_data.freshness import update_freshness
from bifrost_market_data.scheduler.enqueue import insert_jobs_bulk, trim_old_jobs
from bifrost_market_data.subscription import SLOT_REQUIREMENTS
from bifrost_market_data.symbol_void import load_voided_symbols

logger = logging.getLogger(__name__)

_NY = ZoneInfo("America/New_York")

SLOT_NAMES = (
    "stock-eod",
    "eod-pipeline",
    "universe-daily",
    "corporate",
    "option-refresh",
    "option-bars",
    "option-trades",
    "minute-bars",
    "calendar",
    "reference",
    "fundamentals-rotate",
    "related-rotate",
    "readiness-refresh",
    "trim",
    "stock-snapshot",
    "stock-movers",
    "fundamentals-market",
)

# Wave 2.1: analytics upserts moved to bifrost_research.scheduler.volatility
MIGRATED_ANALYTICS_SLOTS = frozenset({"max-pain", "atm-iv-pcr", "iv-percentile"})

# Slots whose data is one session's worth and whose cron fires once per
# session. The Dagster trading-day catch-up (22:30 ET) re-fires the same two
# slots ~4.5h after the 22:00 UTC primary; a second enqueue inside the window
# is skipped unless forced, so the catch-up only lands when the primary left
# no jobs behind.
# (kinds, payload field naming the session). The dedup key is the session
# itself, not a time window: a catch-up fires ~4.5h after the 22:00 UTC
# primary, and a Monday morning run must not be skipped because Friday's run
# was "recent". Only a non-failed job for the *same session* blocks a re-enqueue.
SESSION_ONCE_SLOTS: dict[str, tuple[tuple[str, ...], str]] = {
    "stock-eod": (("stock_daily",), "to"),
    "eod-pipeline": (("option_snapshot",), "trade_date"),
}

# Retired slots — the current Massive subscriptions do not cover the data.
# Kept in SLOT_NAMES so Dagster / CLI callers get a skip instead of a 400
# until the plan changes; the wording lives in subscription.py.
UNENTITLED_SLOTS: dict[str, str] = {
    slot: req["reason"] for slot, req in SLOT_REQUIREMENTS.items()
}

# Slots that skip enqueue on NYSE closed / weekend (must match adherence logic).
SKIP_ON_HOLIDAY_SLOTS = frozenset(
    {
        "stock-eod",
        "eod-pipeline",
        "universe-daily",
        "corporate",
        "option-bars",
        "option-trades",
        "minute-bars",
        "fundamentals-rotate",
        "related-rotate",
        "stock-snapshot",
        "stock-movers",
    }
)

DEFAULT_WATCHLIST_QUERY = """
SELECT DISTINCT symbol FROM public.watchlist
WHERE sec_type = 'STK' AND optionable = true
  AND symbol IS NOT NULL AND trim(symbol) <> ''
""".strip()

# Same filter as Research dim_universe / Stock Screener Technical.
CS_UNIVERSE_QUERY = """
SELECT symbol
FROM raw_market.ticker
WHERE instrument_type = 'CS'
  AND market = 'stocks'
  AND COALESCE(active, true) = true
  AND lower(COALESCE(currency, 'usd')) = 'usd'
  AND symbol IS NOT NULL AND trim(symbol) <> ''
""".strip()

INCOME_STATEMENT_COVERED_QUERY = """
SELECT DISTINCT UPPER(TRIM(symbol)) AS symbol
FROM raw_market.income_statement
WHERE symbol IS NOT NULL AND trim(symbol) <> ''
""".strip()

# Wave A IV Radar market-weather ETFs — unioned into eod-pipeline / option paths.
DEFAULT_IV_RADAR_BENCHMARKS = ("SPY", "QQQ", "IWM")


def default_schedule_path() -> Path | None:
    env = (os.environ.get("SCHEDULE_CONFIG") or "").strip()
    if env:
        p = Path(env)
        if p.is_file():
            return p
    for candidate in (
        Path("/config/schedule.yaml"),
        Path(__file__).resolve().parents[3] / "config" / "schedule.yaml",
        Path(__file__).resolve().parents[3] / "config" / "schedule.yaml.example",
    ):
        if candidate.is_file():
            return candidate
    return None


def load_schedule(path: str | Path | None = None) -> dict[str, Any]:
    """Load schedule.yaml (scheduler section). Falls back to empty scheduler dict."""
    resolved: Path | None
    if path is not None:
        resolved = Path(path)
    else:
        resolved = default_schedule_path()
    if resolved is None or not resolved.is_file():
        return {"scheduler": {}}
    with resolved.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        return {"scheduler": {}}
    return raw


def load_watchlist_from_platform(platform_url: str, *, timeout: float = 15.0) -> list[str] | None:
    """Fetch watchlist union from Platform API.

    Returns sorted unique symbol list on success, or None on failure (caller
    should fall back to DB watchlist).
    """
    url = platform_url.rstrip("/") + "/api/v1/watchlist/union"
    logger.info("fetching watchlist union from %s", url)
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                logger.warning("platform-api returned HTTP %s", resp.status)
                return None
            body = json.loads(resp.read())
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("platform-api watchlist union unreachable: %s", exc)
        return None

    if not body.get("ok"):
        logger.warning("platform-api watchlist union ok=false: %s", body)
        return None

    symbols = body.get("symbols")
    if not isinstance(symbols, list):
        logger.warning("platform-api returned non-list symbols: %r", type(symbols))
        return None

    result = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
    logger.info("platform-api watchlist union: %d symbols", len(result))
    return result


def today_ny() -> date:
    """The NY calendar date right now — the date a cron fire happens on."""
    return datetime.now(timezone.utc).astimezone(_NY).date()


def _session_evidence_exists(
    conn: Any, kinds: Sequence[str], *, session_field: str, session: str
) -> bool:
    """True when a non-failed job of ``kinds`` already covers ``session``."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM ops_jobs.job_ingest
                WHERE kind = ANY(%s)
                  AND payload ->> %s = %s
                  AND status <> 'failed'
                LIMIT 1
                """,
                (list(kinds), str(session_field), str(session)),
            )
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
    except Exception as exc:  # noqa: BLE001 — evidence probe must not block enqueue
        logger.warning("session evidence probe failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    return row is not None


def _store_watchlist_cache(conn: Any, symbols: Sequence[str], *, source: str) -> None:
    """Remember the last good union so an outage does not shrink the universe."""
    syms = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
    if not syms:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ops_jobs.watchlist_cache")
            cur.executemany(
                "INSERT INTO ops_jobs.watchlist_cache (symbol, source, updated_at) VALUES (%s, %s, now())",
                [(s, source) for s in syms],
            )
        if hasattr(conn, "commit"):
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — the cache is a courtesy, not the source
        logger.warning("watchlist cache store failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass


def _read_watchlist_cache(conn: Any) -> list[str]:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM ops_jobs.watchlist_cache ORDER BY symbol")
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001
        logger.warning("watchlist cache read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    return _rows_to_symbols(rows)


def resolve_target_date(value: str | date | None = None) -> date:
    """Resolve target trading date. Default: latest weekday on NY calendar (today if weekday)."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if value:
        return date.fromisoformat(str(value).strip()[:10])
    ny_today = datetime.now(timezone.utc).astimezone(_NY).date()
    # If weekend, roll back to Friday
    while ny_today.weekday() >= 5:
        ny_today -= timedelta(days=1)
    return ny_today


def is_trading_day(conn: Any, d: date) -> bool:
    """NYSE session check via ``market.us_market_holiday`` (weekday − closed)."""
    from bifrost_market_data.trading_calendar import is_trading_day as _is_trading_day

    return _is_trading_day(conn, d)


def load_watchlist_symbols(
    conn: Any,
    scheduler_cfg: Mapping[str, Any],
) -> list[str]:
    """Return watchlist symbols from config override, platform-api, or DB query.

    Resolution order:
    1. ``watchlist_symbols`` hard override (always wins)
    2. ``watchlist_source: platform-api`` → fetch from Platform API union endpoint
       (falls back to DB on failure)
    3. Default: DB query via ``watchlist_query``
    """
    override = scheduler_cfg.get("watchlist_symbols")
    if override:
        return sorted({str(s).strip().upper() for s in override if str(s).strip()})

    source = str(scheduler_cfg.get("watchlist_source") or "db").strip().lower()
    if source == "platform-api":
        platform_url = str(scheduler_cfg.get("platform_api_url") or "").strip()
        if not platform_url:
            platform_url = os.environ.get("PLATFORM_API_URL", "").strip()
        if platform_url:
            symbols = load_watchlist_from_platform(platform_url)
            if symbols is not None:
                _store_watchlist_cache(conn, symbols, source="platform-api")
                return symbols
            cached = _read_watchlist_cache(conn)
            if cached:
                logger.warning(
                    "platform-api watchlist union unreachable; using the cached union (%d symbols)",
                    len(cached),
                )
                return cached
            logger.warning("platform-api fallback to DB watchlist query")
        else:
            logger.warning(
                "watchlist_source=platform-api but no platform_api_url configured; "
                "falling back to DB"
            )

    query = str(scheduler_cfg.get("watchlist_query") or DEFAULT_WATCHLIST_QUERY).strip()
    symbols: list[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
            if rows is None:
                rows = []
            for row in rows:
                if isinstance(row, Mapping):
                    sym = row.get("symbol") or next(iter(row.values()), None)
                else:
                    sym = row[0] if row else None
                if sym:
                    symbols.append(str(sym).strip().upper())
    except Exception as exc:
        # Golden Source no longer hosts public.watchlist (Trade-owned). A missing
        # table must not fail the CronJob after platform-api union is unreachable.
        logger.warning("watchlist DB fallback failed: %s; returning empty list", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    return sorted(set(symbols))


def _option_contract_underlyings(conn: Any, *, limit: int = 200) -> list[str]:
    """Fallback symbol set when Trade watchlist is not on Golden Source."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT UPPER(TRIM(underlying)) AS sym
                FROM raw_market.option_contract
                WHERE TRIM(COALESCE(underlying, '')) <> ''
                GROUP BY UPPER(TRIM(underlying))
                ORDER BY COUNT(*) DESC, sym ASC
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:
        logger.warning("option_contract underlyings fallback failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    out: list[str] = []
    for row in rows or []:
        if isinstance(row, Mapping):
            sym = row.get("sym") or next(iter(row.values()), None)
        else:
            sym = row[0] if row else None
        if sym:
            out.append(str(sym).strip().upper())
    return sorted(set(out))


def resolve_watchlist_symbols_for_coverage(
    conn: Any,
    *,
    limit: int = 200,
    scheduler_cfg: Mapping[str, Any] | None = None,
) -> list[str]:
    """Resolve coverage/quality watchlist; see ``resolve_watchlist_with_source``."""
    symbols, _source = resolve_watchlist_with_source(
        conn, limit=limit, scheduler_cfg=scheduler_cfg
    )
    return symbols


def resolve_watchlist_with_source(
    conn: Any,
    *,
    limit: int = 200,
    scheduler_cfg: Mapping[str, Any] | None = None,
) -> tuple[list[str], str]:
    """Resolve coverage/quality watchlist the same way CronJobs do.

    Order:
    1. ``schedule.yaml`` scheduler block (``watchlist_source: platform-api`` union)
    2. DB ``public.watchlist`` (usually absent on Golden Source)
    3. ``market.option_contract`` underlyings (inventory-compatible fallback)

    Returns ``(symbols, source)`` where source is ``watchlist``,
    ``option_contract_underlyings``, or ``empty``.
    """
    if scheduler_cfg is None:
        raw = load_schedule()
        sched = raw.get("scheduler") if isinstance(raw, dict) else {}
        scheduler_cfg = sched if isinstance(sched, dict) else {}
    symbols = load_watchlist_symbols(conn, scheduler_cfg)
    if symbols:
        clipped = symbols[: int(limit)] if limit else symbols
        return clipped, "watchlist"
    fallback = _option_contract_underlyings(conn, limit=limit)
    if fallback:
        return fallback, "option_contract_underlyings"
    return [], "empty"


def _rows_to_symbols(rows: Any) -> list[str]:
    symbols: list[str] = []
    for row in rows or []:
        if isinstance(row, Mapping):
            sym = row.get("symbol") or next(iter(row.values()), None)
        else:
            sym = row[0] if row else None
        if sym:
            symbols.append(str(sym).strip().upper())
    return sorted(set(symbols))


def load_cs_universe(conn: Any) -> list[str]:
    """Active USD common stock universe from ``market.ticker``."""
    try:
        with conn.cursor() as cur:
            cur.execute(CS_UNIVERSE_QUERY)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:
        logger.warning("CS universe query failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    return _rows_to_symbols(rows)


def load_income_statement_symbols(conn: Any) -> set[str]:
    """Symbols that already have an income_statement row."""
    try:
        with conn.cursor() as cur:
            cur.execute(INCOME_STATEMENT_COVERED_QUERY)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:
        logger.warning("income-statement coverage query failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return set()
    return set(_rows_to_symbols(rows))


def _rotate_symbols(symbols: Sequence[str], day_s: str) -> list[str]:
    items = [str(s).strip().upper() for s in symbols if str(s).strip()]
    if not items:
        return []
    offset = int(hashlib.sha256(day_s.encode("utf-8")).hexdigest(), 16) % len(items)
    return items[offset:] + items[:offset]


def resolve_fundamentals_rotate_symbols(
    conn: Any,
    *,
    watchlist_symbols: Sequence[str],
    scheduler_cfg: Mapping[str, Any],
    slot_cfg: Mapping[str, Any],
    day_s: str,
) -> list[str]:
    """CS universe (or watchlist fallback), missing income statements first."""
    universe_mode = str(slot_cfg.get("universe") or "watchlist").strip().lower()
    if universe_mode == "cs":
        cs = load_cs_universe(conn)
        pool = union_iv_radar_benchmarks(cs or watchlist_symbols, scheduler_cfg)
    else:
        pool = union_iv_radar_benchmarks(watchlist_symbols, scheduler_cfg)

    # Names the vendor answered nothing for are skipped for a month; the
    # missing-first rule used to put exactly those at the front every day.
    voided = load_voided_symbols(conn, "financials")
    if voided:
        pool = [s for s in pool if s not in voided]

    prioritize_missing = bool(slot_cfg.get("prioritize_missing", True))
    if not prioritize_missing or not pool:
        return _rotate_symbols(pool, day_s)

    covered = load_income_statement_symbols(conn)
    missing = [s for s in pool if s not in covered]
    have = [s for s in pool if s in covered]
    return _rotate_symbols(missing, day_s) + _rotate_symbols(have, day_s)


def union_iv_radar_benchmarks(
    symbols: Sequence[str],
    scheduler_cfg: Mapping[str, Any] | None = None,
) -> list[str]:
    """Watchlist ∪ Wave A Benchmarks (SPY/QQQ/IWM) for ATM IV / IV Percentile slots."""
    cfg = scheduler_cfg or {}
    raw = cfg.get("iv_radar_benchmarks")
    if raw is None:
        benches = DEFAULT_IV_RADAR_BENCHMARKS
    elif isinstance(raw, str):
        benches = tuple(s.strip().upper() for s in raw.split(",") if s.strip())
    else:
        benches = tuple(str(s).strip().upper() for s in raw if str(s).strip())
    merged = {str(s).strip().upper() for s in symbols if str(s).strip()}
    merged.update(benches)
    return sorted(merged)


def option_trades_universe(
    symbols: Sequence[str],
    *,
    limit: int = 50,
    always_include: str = "SPX",
) -> list[str]:
    """SPX ∪ watchlist — sorted union truncated to ``limit``, always keep SPX.

    Owner-locked tape universe (plugin-options-tape): daily REST ingest only.
    """
    cap = max(1, int(limit))
    must = str(always_include or "SPX").strip().upper() or "SPX"
    merged = {str(s).strip().upper() for s in symbols if str(s).strip()}
    merged.add(must)
    sorted_syms = sorted(merged)
    if len(sorted_syms) <= cap:
        return sorted_syms
    others = [s for s in sorted_syms if s != must]
    keep = others[: cap - 1]
    return sorted([*keep, must])


def load_option_tickers_near_spot(
    conn: Any,
    underlyings: Sequence[str],
    *,
    as_of: date,
    expiries: int = 3,
    strikes_each_side: int = 10,
) -> list[str]:
    """Contracts around the money: the next ``expiries`` expiries per underlying
    and, per expiry and right, the ``2·strikes_each_side+1`` strikes nearest the
    latest close. The old selection took the lowest strikes of the nearest
    expiry — for a $230 stock that was C50…C105 expiring that day.

    Underlyings without a close in ``stock_daily`` (index roots such as SPX on
    a plan without index levels) are skipped.
    """
    syms = [str(s).strip().upper() for s in underlyings if str(s).strip()]
    if not syms:
        return []
    n_exp = max(1, int(expiries))
    per_right = max(1, 2 * int(strikes_each_side) + 1)
    with conn.cursor() as cur:
        cur.execute(
            """
            /* near-spot */
            WITH spot AS (
              SELECT DISTINCT ON (symbol) symbol, close
              FROM raw_market.stock_daily
              WHERE symbol = ANY(%s) AND bar_date <= %s AND close IS NOT NULL
              ORDER BY symbol, bar_date DESC
            ),
            exp AS (
              SELECT underlying, expiry,
                     DENSE_RANK() OVER (PARTITION BY underlying ORDER BY expiry) AS erank
              FROM (
                SELECT DISTINCT underlying, expiry
                FROM raw_market.option_contract
                WHERE underlying = ANY(%s) AND expiry >= %s
              ) d
            ),
            ranked AS (
              SELECT c.option_ticker,
                     ROW_NUMBER() OVER (
                       PARTITION BY c.underlying, c.expiry, c.option_right
                       ORDER BY abs(c.strike - s.close), c.strike
                     ) AS srank
              FROM raw_market.option_contract c
              JOIN spot s ON s.symbol = c.underlying
              JOIN exp e ON e.underlying = c.underlying AND e.expiry = c.expiry
              WHERE e.erank <= %s
            )
            SELECT option_ticker FROM ranked WHERE srank <= %s ORDER BY option_ticker
            """,
            (syms, as_of, syms, as_of, n_exp, per_right),
        )
        rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    out: list[str] = []
    for row in rows or []:
        t = row.get("option_ticker") if isinstance(row, Mapping) else (row[0] if row else None)
        if t:
            out.append(str(t).strip().upper())
    return out


def _slot_cfg(scheduler_cfg: Mapping[str, Any], slot: str) -> dict[str, Any]:
    slots = dict(scheduler_cfg.get("slots") or {})
    return dict(slots.get(slot) or {})


def enqueue_slot(
    conn: Any,
    slot: str,
    *,
    target_date: date | None = None,
    watchlist_symbols: Sequence[str] | None = None,
    scheduler_cfg: Mapping[str, Any] | None = None,
    force: bool = False,
    fire_date: date | None = None,
) -> dict[str, Any]:
    """Generate jobs for one schedule slot. Returns summary dict.

    ``force=True`` runs holiday-gated slots on weekends (used for CS financials
    catch-up) and bypasses the session-once dedup. ``fire_date`` is the NY date
    the cron fired on; holiday-gated slots are skipped when *that* day is not a
    session, so a Saturday fire cannot re-run Friday just because the target
    date rolled back to Friday.
    """
    slot_key = str(slot).strip().lower()
    if slot_key in MIGRATED_ANALYTICS_SLOTS:
        msg = (
            f"slot {slot_key!r} moved to bifrost_research.scheduler.volatility "
            "(Research NS); plugin no longer computes market_analytics upserts"
        )
        logger.error(msg)
        raise ValueError(msg)
    if slot_key not in SLOT_NAMES:
        raise ValueError(f"unknown slot: {slot!r} (expected one of {SLOT_NAMES})")

    cfg = dict(scheduler_cfg or {})
    scfg = _slot_cfg(cfg, slot_key)
    priority = int(scfg.get("priority") or 0)
    day = resolve_target_date(target_date)
    day_s = day.isoformat()

    if slot_key == "trim":
        keep_days = int(scfg.get("keep_days") or 7)
        keep_max = int(scfg.get("keep_max") or 5000)
        deleted = trim_old_jobs(conn, keep_days=keep_days, keep_max=keep_max)
        try:
            update_freshness(conn, "job_trim", int(deleted or 0), status="ok")
        except Exception as exc:  # noqa: BLE001 — freshness must not fail trim
            logger.warning("job_trim freshness update failed: %s", exc)
        trades_keep = int(scfg.get("option_trades_keep_days") or 30)
        # Retention is counted in trading sessions, not calendar days: the table
        # now holds exactly one observation per session, so "keep 90 sessions"
        # is the promise Research depends on. Holidays and a long weekend used
        # to quietly shorten a 90-day window by several sessions.
        snapshot_keep_sessions = int(scfg.get("option_snapshot_keep_sessions") or 90)
        snapshot_keep = int(scfg.get("option_snapshot_keep_days") or 90)
        try:
            from bifrost_market_data.trading_calendar import fetch_recent_trading_days

            sessions = fetch_recent_trading_days(conn, snapshot_keep_sessions, as_of=day)
            if len(sessions) >= snapshot_keep_sessions:
                snapshot_keep = max(1, (day - sessions[0]).days)
        except Exception as exc:  # noqa: BLE001 — calendar gap falls back to days
            logger.warning("session-based snapshot retention unavailable: %s", exc)
        partitions_dropped = 0
        snapshot_partitions_dropped = 0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ops_jobs.drop_day_partitions_older_than('raw_market', 'option_trades', %s)",
                    (trades_keep,),
                )
                row = cur.fetchone() if hasattr(cur, "fetchone") else None
            if row is not None:
                partitions_dropped = int(row[0] if not isinstance(row, Mapping) else next(iter(row.values())))
            if hasattr(conn, "commit"):
                conn.commit()
            # Re-create near-term day partitions after drops.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ops_jobs.ensure_day_partitions('raw_market', 'option_trades', 35, 2)"
                )
            if hasattr(conn, "commit"):
                conn.commit()
        except Exception as exc:  # noqa: BLE001 — retention best-effort
            logger.warning("option_trades partition retention failed: %s", exc)
            if hasattr(conn, "rollback"):
                conn.rollback()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ops_jobs.drop_month_partitions_older_than"
                    "('raw_market', 'option_snapshot', %s)",
                    (snapshot_keep,),
                )
                row = cur.fetchone() if hasattr(cur, "fetchone") else None
            if row is not None:
                snapshot_partitions_dropped = int(
                    row[0] if not isinstance(row, Mapping) else next(iter(row.values()))
                )
            if hasattr(conn, "commit"):
                conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_snapshot', 3, 3)"
                )
            if hasattr(conn, "commit"):
                conn.commit()
        except Exception as exc:  # noqa: BLE001 — retention best-effort
            logger.warning("option_snapshot partition retention failed: %s", exc)
            if hasattr(conn, "rollback"):
                conn.rollback()
        return {
            "slot": slot_key,
            "trimmed": deleted,
            "option_trades_partitions_dropped": partitions_dropped,
            "option_trades_keep_days": trades_keep,
            "option_snapshot_partitions_dropped": snapshot_partitions_dropped,
            "option_snapshot_keep_days": snapshot_keep,
            "option_snapshot_keep_sessions": snapshot_keep_sessions,
            "enqueued": 0,
            "deduped": 0,
        }

    if slot_key == "readiness-refresh":
        # Wave 14G-A: slot kept for CLI/schedule compat; no-op skip only.
        # stock_readiness_daily dropped; SEPA readiness via Plugin /market/readiness/* + dbt.
        logger.info(
            "readiness-refresh skipped — retired (stock_readiness_daily gone; "
            "use /market/readiness/* + dw_stock.mart_sepa_*)"
        )
        return {
            "slot": slot_key,
            "skipped": True,
            "reason": "retired",
            "enqueued": 0,
            "deduped": 0,
        }

    if slot_key in UNENTITLED_SLOTS:
        logger.info("slot=%s retired: %s", slot_key, UNENTITLED_SLOTS[slot_key])
        return {
            "slot": slot_key,
            "target_date": day_s,
            "skipped": True,
            "reason": "unentitled",
            "detail": UNENTITLED_SLOTS[slot_key],
            "enqueued": 0,
            "deduped": 0,
            "jobs": [],
        }

    skip_on_holiday = slot_key in SKIP_ON_HOLIDAY_SLOTS
    gate_day = fire_date or day
    if skip_on_holiday and not force and not is_trading_day(conn, gate_day):
        logger.info(
            "slot=%s fire_date=%s is not a trading day (target=%s), skipping",
            slot_key,
            gate_day.isoformat(),
            day_s,
        )
        return {
            "slot": slot_key,
            "target_date": day_s,
            "skipped": True,
            "reason": "non_trading_day",
            "enqueued": 0,
            "deduped": 0,
            "jobs": [],
        }

    if slot_key in SESSION_ONCE_SLOTS and not force:
        kinds_seen, session_field = SESSION_ONCE_SLOTS[slot_key]
        if _session_evidence_exists(
            conn, kinds_seen, session_field=session_field, session=day_s
        ):
            logger.info(
                "slot=%s already enqueued for session %s (kinds=%s), skipping",
                slot_key,
                day_s,
                ",".join(kinds_seen),
            )
            return {
                "slot": slot_key,
                "target_date": day_s,
                "skipped": True,
                "reason": "evidence_exists",
                "enqueued": 0,
                "deduped": 0,
                "jobs": [],
            }

    # Full-market / calendar-like slots do not need the watchlist; skip the
    # platform-api + DB lookup so a union 404 cannot fail ticker_sync / grouped EOD.
    _slots_need_watchlist = {
        "stock-eod",
        "eod-pipeline",
        "corporate",
        "option-refresh",
        "option-bars",
        "option-trades",
        "minute-bars",
        "fundamentals-rotate",
        "related-rotate",
    }
    if watchlist_symbols is not None:
        symbols = list(watchlist_symbols)
    elif slot_key in _slots_need_watchlist:
        symbols = load_watchlist_symbols(conn, cfg)
    else:
        symbols = []
    jobs: list[dict[str, Any]] = []
    specs: list[tuple[str, dict[str, Any], int, int]] = []

    def _add(kind: str, payload: dict[str, Any], pri: int | None = None) -> None:
        # Collected here, written in one statement below.
        specs.append((kind, payload, pri if pri is not None else priority, 3))
        jobs.append({"kind": kind, "payload": payload, "id": None, "deduped": True})

    if slot_key == "stock-eod":
        for sym in symbols:
            _add("stock_daily", {"symbol": sym, "from": day_s, "to": day_s})

    elif slot_key == "eod-pipeline":
        # One chain snapshot per underlying; the handler derives the session's
        # open interest from the same response, so no second download.
        # Index spot (I:SPX) is not enqueued: it needs an Indices plan.
        pipeline_syms = union_iv_radar_benchmarks(symbols, cfg)
        for sym in pipeline_syms:
            storage = storage_underlying(sym)
            _add("option_snapshot", {"underlying": storage, "trade_date": day_s})

    elif slot_key == "universe-daily":
        _add(
            "stock_daily_grouped",
            {"from": day_s, "to": day_s, "market": "stocks"},
            pri=priority,
        )

    elif slot_key == "corporate":
        # Whole market by date window (a few pages) instead of one call per
        # watchlist symbol: Research needs ex-dates for every name it screens.
        back = int(scfg.get("lookback_days") or 7)
        ahead = int(scfg.get("lookahead_days") or 60)
        window = {
            "from": (day - timedelta(days=back)).isoformat(),
            "to": (day + timedelta(days=ahead)).isoformat(),
        }
        _add("splits_market", dict(window))
        _add("dividends_market", dict(window))

    elif slot_key == "option-refresh":
        batch_size = int(scfg.get("batch_size") or 12)
        benches = union_iv_radar_benchmarks([], cfg)
        bench_set = set(benches)
        if symbols:
            # Deterministic rotation so the whole watchlist is covered over days.
            offset = int(hashlib.sha256(day_s.encode("utf-8")).hexdigest(), 16) % len(symbols)
            rotated = symbols[offset:] + symbols[:offset]
            rest = [s for s in rotated if s not in bench_set]
            batch = list(benches) + rest[: max(0, batch_size)]
        else:
            batch = list(benches)
        # The contract handler upserts option_expiration from the same page
        # walk, so a separate expiration job would re-download the catalogue.
        for sym in batch:
            _add("option_contract", {"underlying": sym, "expired": False})

    elif slot_key == "option-bars":
        bars_syms = union_iv_radar_benchmarks(symbols, cfg)
        tickers = load_option_tickers_near_spot(
            conn,
            bars_syms,
            as_of=day,
            expiries=int(scfg.get("expiries") or 3),
            strikes_each_side=int(scfg.get("strikes_each_side") or 10),
        )
        for ot in tickers:
            _add("option_daily", {"option_ticker": ot, "from": day_s, "to": day_s})

    elif slot_key == "minute-bars":
        # Stock intraday: 1min / 5min / 1hour (replaces retired Trade stocks_ib Celery path).
        for sym in symbols:
            for multiplier, timespan in ((1, "minute"), (5, "minute"), (1, "hour")):
                _add(
                    "stock_minute",
                    {
                        "symbol": sym,
                        "from": day_s,
                        "to": day_s,
                        "multiplier": multiplier,
                        "timespan": timespan,
                    },
                )
        # Option minute bars: rotate a bounded batch of at-the-money contracts.
        batch_size = int(scfg.get("batch_size") or 80)
        tickers = load_option_tickers_near_spot(
            conn,
            symbols,
            as_of=day,
            expiries=int(scfg.get("expiries") or 2),
            strikes_each_side=int(scfg.get("strikes_each_side") or 5),
        )
        if tickers:
            offset = int(hashlib.sha256(day_s.encode("utf-8")).hexdigest(), 16) % len(tickers)
            rotated = tickers[offset:] + tickers[:offset]
            batch = rotated[: max(0, batch_size)]
        else:
            batch = []
        for ot in batch:
            _add(
                "option_minute",
                {
                    "option_ticker": ot,
                    "from": day_s,
                    "to": day_s,
                    "multiplier": 1,
                    "timespan": "minute",
                },
            )

    elif slot_key == "calendar":
        _add("calendar", {})

    elif slot_key == "reference":
        # Universe ticker sync — run on weekends/holidays too (calendar-like).
        _add("ticker_sync", {"mode": "universe"}, pri=priority)

    elif slot_key == "fundamentals-rotate":
        # Per-symbol financials (+ optional SEPA extras) with deterministic rotation.
        # universe=cs → market.ticker CS (Stock Screener); watchlist remains the
        # fallback when ticker table is empty. Missing income_statement first.
        batch_size = int(scfg.get("batch_size") or 40)
        include_ratios = bool(scfg.get("include_ratios", True))
        include_short_interest = bool(scfg.get("include_short_interest", True))
        include_short_volume = bool(scfg.get("include_short_volume", True))
        rotated = resolve_fundamentals_rotate_symbols(
            conn,
            watchlist_symbols=symbols,
            scheduler_cfg=cfg,
            slot_cfg=scfg,
            day_s=day_s,
        )
        batch = rotated[: max(0, batch_size)]
        for sym in batch:
            _add("financials", {"symbol": sym}, pri=priority)
            if include_ratios:
                _add("ratios", {"symbol": sym}, pri=priority)
            if include_short_interest:
                _add("short_interest", {"symbol": sym}, pri=priority)
            if include_short_volume:
                _add("short_volume", {"symbol": sym}, pri=priority)

    elif slot_key == "related-rotate":
        # Per-symbol related-companies with deterministic daily rotation.
        batch_size = int(scfg.get("batch_size") or 40)
        if symbols:
            offset = int(hashlib.sha256(day_s.encode("utf-8")).hexdigest(), 16) % len(symbols)
            rotated = symbols[offset:] + symbols[:offset]
            batch = rotated[: max(0, batch_size)]
        else:
            batch = []
        for sym in batch:
            _add("ticker_related", {"symbol": sym}, pri=priority)

    elif slot_key == "fundamentals-market":
        # Ratios and short data for the last completed session, whole market.
        # Short interest settles twice a month and FINRA publishes it about ten
        # days later: a 45-day window always holds the latest published
        # settlement (20 days missed it — the 08-14 settlement was the newest
        # on 09-04) without re-pulling history.
        from bifrost_market_data.quality import fetch_completed_trading_days

        sessions = fetch_completed_trading_days(conn, 1, as_of=day)
        session = sessions[-1] if sessions else day
        session_s = session.isoformat()
        si_back = int(scfg.get("short_interest_lookback_days") or 45)
        _add("ratios_market", {"date": session_s}, pri=priority)
        _add("short_volume_market", {"date": session_s}, pri=priority)
        _add(
            "short_interest_market",
            {"settlement_date_gte": (session - timedelta(days=si_back)).isoformat()},
            pri=priority,
        )

    elif slot_key == "stock-snapshot":
        # Full-market All Tickers Snapshot (D2=A); one job, mode=all.
        _add(
            "stock_snapshot",
            {"mode": "all", "session_date": day_s},
            pri=priority,
        )

    elif slot_key == "stock-movers":
        # One job handles both gainers + losers (handler loops directions).
        _add(
            "stock_movers",
            {"direction": "both", "session_date": day_s},
            pri=priority,
        )

    ids = insert_jobs_bulk(conn, specs)
    for job_entry, job_id in zip(jobs, ids):
        job_entry["id"] = job_id
        job_entry["deduped"] = job_id is None
    enqueued = sum(1 for j in jobs if not j["deduped"])
    deduped = len(jobs) - enqueued

    return {
        "slot": slot_key,
        "target_date": day_s,
        "symbols": len(symbols),
        "enqueued": enqueued,
        "deduped": deduped,
        "jobs": jobs,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enqueue market-data ingest jobs for a schedule slot")
    parser.add_argument(
        "--slot",
        required=True,
        choices=SLOT_NAMES,
        help="Schedule slot to enqueue",
    )
    parser.add_argument("--date", default=None, help="Target date YYYY-MM-DD (default: latest NY weekday)")
    parser.add_argument("--config", default=None, help="Path to market-data.yaml")
    parser.add_argument("--schedule", default=None, help="Path to schedule.yaml")
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbol override (skips watchlist query)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run holiday-gated slots on weekends/holidays (CS financials catch-up)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    cfg = load_config(args.config)
    schedule = load_schedule(args.schedule)
    scheduler_cfg = dict(schedule.get("scheduler") or {})
    # Allow market-data.yaml to overlay scheduler section
    if isinstance(cfg.get("scheduler"), dict):
        merged = dict(scheduler_cfg)
        merged.update(cfg["scheduler"])
        scheduler_cfg = merged

    symbols_override: list[str] | None = None
    if args.symbols:
        symbols_override = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    import time

    import psycopg

    # CNPG / ClusterIP occasionally resets the first TCP handshake from short-lived
    # CronJob pods; retry briefly before failing the Job.
    kw = postgres_connect_kwargs(cfg)
    conn = None
    last_err: Exception | None = None
    for attempt in range(1, 6):
        try:
            conn = psycopg.connect(**kw, connect_timeout=10)
            break
        except psycopg.OperationalError as exc:
            last_err = exc
            logger.warning(
                "postgres connect attempt %s/5 failed: %s", attempt, exc
            )
            time.sleep(min(2 * attempt, 8))
    if conn is None:
        raise last_err if last_err is not None else RuntimeError("postgres connect failed")
    try:
        result = enqueue_slot(
            conn,
            args.slot,
            target_date=resolve_target_date(args.date),
            watchlist_symbols=symbols_override,
            scheduler_cfg=scheduler_cfg,
            force=bool(args.force),
            fire_date=None if args.date else today_ny(),
        )
    finally:
        conn.close()

    logger.info(
        "slot=%s enqueued=%s deduped=%s trimmed=%s",
        result.get("slot"),
        result.get("enqueued"),
        result.get("deduped"),
        result.get("trimmed"),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
