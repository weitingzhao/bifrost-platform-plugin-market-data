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

from bifrost_market_data.logging_setup import configure_logging
from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.ingest.index_options import storage_underlying
from bifrost_market_data.freshness import update_freshness
from bifrost_market_data.scheduler.enqueue import (
    TRIM_BUDGET_SEC,
    TRIM_MAX_ROWS,
    insert_jobs_bulk,
    trim_option_snapshots,
    trim_old_jobs,
)
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
    "intraday-chain",
    "treasury",
    "option-backfill",
    # Reference data, not session data: it has no holiday gate because a
    # company's listing date does not depend on the market being open.
    "ticker-details",
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
# slot -> (kinds that evidence the slot, payload field holding the session,
# payload field naming the symbol). The symbol field makes the guard ask
# "is this session *covered*" instead of "did anything at all run".
SESSION_ONCE_SLOTS: dict[str, tuple[tuple[str, ...], str, str]] = {
    "stock-eod": (("stock_daily",), "to", "symbol"),
    "eod-pipeline": (("option_snapshot",), "trade_date", "underlying"),
}

# Retired slots — the current Massive subscriptions do not cover the data.
# Kept in SLOT_NAMES so Dagster / CLI callers get a skip instead of a 400
# until the plan changes; the wording lives in subscription.py.
UNENTITLED_SLOTS: dict[str, str] = {slot: req["reason"] for slot, req in SLOT_REQUIREMENTS.items()}

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


def _month_start(day: date, months_back: int) -> date:
    """First day of the month ``months_back`` before ``day``'s month."""
    total = day.year * 12 + (day.month - 1) - months_back
    return date(total // 12, total % 12 + 1, 1)


def _month_end(first: date) -> date:
    """Last day of ``first``'s month."""
    return _month_start(first, -1) - timedelta(days=1)


def _session_symbols_enqueued(
    conn: Any,
    kinds: Sequence[str],
    *,
    session_field: str,
    session: str,
    symbol_field: str,
) -> set[str]:
    """The symbols a non-failed job of ``kinds`` already covers for ``session``.

    Intraday rows do not count. The intraday chain writes ``option_snapshot``
    for the same ``trade_date`` as the EOD slot, so a probe that only asked
    "does any such job exist" answered yes from 15:30 ET onwards and the EOD
    snapshot skipped itself every trading day — invisibly, because skipping is
    a success. It only became load-bearing when the universe grew past the
    handful of names the intraday chain covers.
    """
    try:
        with conn.cursor() as cur:
            # No index reaches a payload key, so this reads the session's rows out
            # of a table that is millions deep during a backfill. It runs once per
            # slot fire; the role's 2s does not cover it and failing open would
            # make the guard quietly inert — the exact shape of the bug above.
            cur.execute("SET LOCAL statement_timeout = '30s'")
            cur.execute(
                """
                SELECT DISTINCT payload ->> %s
                FROM ops_jobs.job_ingest
                WHERE kind = ANY(%s)
                  AND payload ->> %s = %s
                  AND status <> 'failed'
                  AND NOT coalesce((payload ->> 'intraday')::boolean, false)
                """,
                (str(symbol_field), list(kinds), str(session_field), str(session)),
            )
            rows = cur.fetchall() or []
    except Exception as exc:  # noqa: BLE001 — evidence probe must not block enqueue
        logger.warning("session evidence probe failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return set()
    return {str(r[0]) for r in rows if r and r[0] is not None}


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


def _is_not_owner(exc: BaseException) -> bool:
    """True when Postgres refused for want of ownership rather than a real fault.

    ``insufficient_privilege`` is 42501; the message check covers drivers that
    do not surface a sqlstate.
    """
    if getattr(getattr(exc, "diag", None), "sqlstate", None) == "42501":
        return True
    return "must be owner of" in str(exc)


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


def resolve_scheduler_cfg() -> dict[str, Any]:
    """The scheduler block as the slots see it: schedule.yaml, then config overrides.

    Callers that pass ``{}`` instead get the DB fallback path, which on Golden
    Source means ``public.watchlist`` — a table that does not exist — so the
    watchlist half of any scope they build comes back silently empty.
    """
    schedule = load_schedule() or {}
    cfg = dict(schedule.get("scheduler") or {})
    app = load_config() or {}
    if isinstance(app.get("scheduler"), dict):
        cfg.update(app["scheduler"])
    return cfg


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


#: Research publishes the option universe as a rule (research.option_universe:
#: resident / core / edge). When a slot is configured with `universe: research`
#: the option slots enumerate that list instead of the watchlist, with the tier
#: deciding priority and the row deciding how much history to backfill.
RESEARCH_UNIVERSE_QUERY = """
SELECT symbol, tier, history_months
FROM research.option_universe
ORDER BY CASE tier WHEN 'resident' THEN 0 WHEN 'core' THEN 1 ELSE 2 END, symbol
""".strip()

#: Added to the slot's base priority so a resident name is claimed before a
#: core name, and both before an edge name or the standing backfill.
TIER_PRIORITY_BUMP = {"resident": 3, "core": 2, "edge": 1}


def load_research_universe(conn: Any) -> list[dict[str, Any]]:
    """`[{symbol, tier, history_months}]` from research.option_universe, or [] when unreadable.

    Empty means "fall back to the watchlist": a database without the table, or
    one where Research has not run the rule yet, must not stall the option
    slots.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(RESEARCH_UNIVERSE_QUERY)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001 — the watchlist path remains
        logger.warning(
            "research.option_universe unreadable; option slots use the watchlist: %s", exc
        )
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    out: list[dict[str, Any]] = []
    for row in rows or []:
        if isinstance(row, Mapping):
            sym, tier, months = row.get("symbol"), row.get("tier"), row.get("history_months")
        else:
            sym, tier, months = (list(row) + [None, None, None])[:3]
        if not sym:
            continue
        out.append(
            {
                "symbol": str(sym).strip().upper(),
                "tier": str(tier or "edge").strip().lower(),
                "history_months": int(months or 12),
            }
        )
    return out


#: Underlyings ordered by how long their contract catalogue has waited for a
#: re-enumeration, oldest first, never-enumerated first of all. One index probe
#: per underlying on (underlying, updated_at DESC) — the same loose-index-scan
#: shape ``enumerated_underlyings`` uses, for the same reason.
STALEST_UNDERLYINGS_QUERY = """
/* stalest_underlyings */
WITH RECURSIVE u AS (
    SELECT min(underlying) AS s FROM raw_market.option_contract
    UNION ALL
    SELECT (SELECT min(underlying) FROM raw_market.option_contract WHERE underlying > u.s)
    FROM u WHERE u.s IS NOT NULL
)
SELECT s, (SELECT max(updated_at) FROM raw_market.option_contract c WHERE c.underlying = u.s)
FROM u WHERE s IS NOT NULL
""".strip()


def stalest_underlyings(conn: Any) -> dict[str, Any] | None:
    """``{underlying: last refreshed}`` or None when the read fails.

    option-refresh rotated on ``sha256(target_date)``, which is the same for all
    four of its six-hourly runs. Measured 2026-09-10, the 06:20 and 12:20 runs
    enqueued an identical twelve names, so the universe came round about every
    48 days rather than the 12 the cron rate implies — and three runs in four
    re-fetched a catalogue that had just been fetched.

    Ordering by the oldest ``updated_at`` advances on every run, needs no clock,
    and repairs itself when a run is missed. ``option_contract`` stamps
    ``updated_at = now()`` on every row a job touches, so the maximum per
    underlying is when that catalogue was last walked.

    None means "could not tell" and the caller must fall back rather than treat
    every name as equally stale.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '30s'")
            cur.execute(STALEST_UNDERLYINGS_QUERY)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001 — the hash rotation remains
        logger.warning("stalest_underlyings unreadable; option-refresh rotates by date: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    out: dict[str, Any] = {}
    for row in rows or []:
        if isinstance(row, Mapping):
            sym, when = row.get("s"), row.get("max")
        else:
            sym, when = (row[0], row[1]) if row and len(row) > 1 else (None, None)
        if sym:
            out[str(sym).strip().upper()] = when
    return out


def enumerated_underlyings(conn: Any) -> set[str] | None:
    """Every underlying already tried: contracts on file, or a finished contract job this week.

    Returns None when the lookup fails. The caller must treat None as "do not
    ramp", never as "nothing is enumerated yet": on 2026-09-08 the previous
    lookup timed out once the table had grown, its fallback returned [], and
    every six-hourly run re-enumerated the same first 150 names from "A" while
    the tail of the universe was never reached.

    Two sources, unioned. Contracts on file is what the rest of the pipeline
    cares about; finished jobs cover the names the vendor lists no options for,
    which would otherwise be "new" forever. Both queries are index-shaped —
    no UPPER(TRIM()) over the contract table.
    """
    have: set[str] = set()
    for sql in (
        # A loose index scan: one probe per distinct underlying on the
        # (underlying, expiry) index, milliseconds regardless of row count.
        # Plain DISTINCT is a sequential scan — 1.7s at 660k rows against the
        # role's 2s statement_timeout, and growing with the backfill.
        """
        /* enumerated_underlyings: contracts */
        WITH RECURSIVE u AS (
            SELECT min(underlying) AS s FROM raw_market.option_contract
            UNION ALL
            SELECT (SELECT min(underlying) FROM raw_market.option_contract WHERE underlying > u.s)
            FROM u WHERE u.s IS NOT NULL
        )
        SELECT s FROM u WHERE s IS NOT NULL
        """,
        """
        /* enumerated_underlyings: finished jobs */
        SELECT DISTINCT payload->>'underlying'
        FROM ops_jobs.job_ingest
        WHERE kind = 'option_contract' AND status = 'done'
          AND created_at >= now() - interval '7 days'
        """,
    ):
        try:
            with conn.cursor() as cur:
                # The role caps statements at 2s; this pair is index-shaped but
                # the jobs table is large, so give the lookup room of its own.
                cur.execute("SET LOCAL statement_timeout = '20s'")
                cur.execute(sql)
                rows = cur.fetchall() if hasattr(cur, "fetchall") else []
        except Exception as exc:  # noqa: BLE001 — the caller fails closed
            logger.warning("enumerated_underlyings lookup failed; not ramping this run: %s", exc)
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        for row in rows or []:
            sym = row.get("underlying") if isinstance(row, Mapping) else (row[0] if row else None)
            if sym:
                have.add(str(sym).strip().upper())
    return have


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
    symbols, _source = resolve_watchlist_with_source(conn, limit=limit, scheduler_cfg=scheduler_cfg)
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


def load_snapshot_windows(
    conn: Any,
    symbols: Sequence[str],
    *,
    as_of: date,
    expiries: int = 3,
    strike_pct: float = 0.15,
) -> dict[str, tuple[float, float, str]]:
    """Per underlying: (strike_gte, strike_lte, expiration_lte) — the near-the-money window.

    Spot is the latest close; the expiry bound is the ``expiries``-th listed
    expiry at or after ``as_of``. A name with no close or no listed expiries
    gets no window and is snapshotted whole, which is the safer failure: an
    unbounded chain costs storage, a wrong bound costs the data.
    """
    syms = sorted({str(x).strip().upper() for x in symbols if str(x).strip()})
    if not syms:
        return {}
    n_exp = max(1, int(expiries))
    pct = max(0.0, float(strike_pct))
    out: dict[str, tuple[float, float, str]] = {}
    # Two shapes, both index-shaped. Spot for all names is one DISTINCT ON over
    # stock_daily, which its (symbol, bar_date) index serves. The expiry bound
    # is one point query per underlying on the (underlying, expiry) index —
    # milliseconds each, and immune to the planner: a set query over forty
    # names took forty seconds the day the contract table grew twentyfold and
    # its statistics had not caught up.
    spots: dict[str, float] = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '60s'")
            cur.execute(
                """
                /* snapshot-window: spot */
                SELECT DISTINCT ON (symbol) symbol, close
                FROM raw_market.stock_daily
                WHERE symbol = ANY(%s) AND bar_date >= %s AND close IS NOT NULL AND close > 0
                ORDER BY symbol, bar_date DESC
                """,
                # The date floor is what makes this fast: unbounded, DISTINCT ON
                # over 13.6M rows for 548 names timed out at 60s; the last week
                # takes well under a second on the (symbol, bar_date) index.
                (syms, as_of - timedelta(days=7)),
            )
            for row in cur.fetchall() or []:
                sym, close = (
                    (row.get("symbol"), row.get("close"))
                    if isinstance(row, Mapping)
                    else (row[0], row[1])
                )
                if sym and close is not None:
                    spots[str(sym).upper()] = float(close)
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — no window beats a wrong one
        logger.warning("snapshot window spot lookup failed; snapshotting whole chains: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return {}
    for sym in syms:
        spot = spots.get(sym)
        if spot is None:
            continue
        try:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '10s'")
                cur.execute(
                    """
                    /* snapshot-window: expiry */
                    SELECT DISTINCT expiry FROM raw_market.option_contract
                    WHERE underlying = %s AND expiry >= %s
                    ORDER BY expiry LIMIT %s
                    """,
                    (sym, as_of, n_exp),
                )
                exps = [
                    (r.get("expiry") if isinstance(r, Mapping) else r[0])
                    for r in (cur.fetchall() or [])
                ]
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "snapshot window expiry lookup failed for %s; snapshotting it whole: %s", sym, exc
            )
            try:
                conn.rollback()
            except Exception:
                pass
            continue
        if len(exps) < n_exp:
            continue
        out[sym] = (
            round(spot * (1 - pct), 2),
            round(spot * (1 + pct), 2),
            exps[n_exp - 1].isoformat(),
        )
    return out


TICKERS_NEEDING_DETAIL_QUERY = """
SELECT symbol
FROM raw_market.ticker
WHERE active
ORDER BY (list_date IS NOT NULL), updated_at NULLS FIRST, symbol
LIMIT %s
"""


def tickers_needing_detail(conn: Any, *, limit: int = 200) -> list[str]:
    """Active tickers whose overview fields are missing, stalest first.

    ``(list_date IS NOT NULL)`` sorts false before true, so every ticker that
    has never had a detail fetch comes before every one that has. Once the
    backlog is gone the same query keeps the refresh honest by taking the
    stalest ``updated_at`` — one rotation rather than two.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(TICKERS_NEEDING_DETAIL_QUERY, (int(limit),))
            rows = cur.fetchall() or []
    except Exception as exc:  # noqa: BLE001 — a slot that cannot pick a batch enqueues nothing
        logger.warning("ticker detail rotation query failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    out: list[str] = []
    for r in rows:
        v = tuple(r.values())[0] if hasattr(r, "values") else (r[0] if r else None)
        if v:
            out.append(str(v).strip().upper())
    return out


def load_option_tickers_near_spot(
    conn: Any,
    underlyings: Sequence[str],
    *,
    as_of: date,
    expiries: int = 3,
    strikes_each_side: int = 10,
) -> list[tuple[str, str]]:
    """Contracts around the money: the next ``expiries`` expiries per underlying
    and, per expiry and right, the ``2·strikes_each_side+1`` strikes nearest the
    latest close. The old selection took the lowest strikes of the nearest
    expiry — for a $230 stock that was C50…C105 expiring that day.

    Underlyings without a close in ``stock_daily`` (index roots such as SPX on
    a plan without index levels) are skipped.

    Returns ``(option_ticker, underlying)``. The underlying comes from the
    catalogue rather than the ticker because an adjusted contract's root is not
    its underlying: O:BDX1260918C00085000 belongs to BDX, and option_contract
    says so. Parsing the root instead put BDX1 in option_daily as a symbol of
    its own while option_snapshot held the same family under BDX.
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
              SELECT c.option_ticker, c.underlying,
                     ROW_NUMBER() OVER (
                       PARTITION BY c.underlying, c.expiry, c.option_right
                       ORDER BY abs(c.strike - s.close), c.strike
                     ) AS srank
              FROM raw_market.option_contract c
              JOIN spot s ON s.symbol = c.underlying
              JOIN exp e ON e.underlying = c.underlying AND e.expiry = c.expiry
              WHERE e.erank <= %s
            )
            SELECT option_ticker, underlying FROM ranked WHERE srank <= %s
            ORDER BY option_ticker
            """,
            (syms, as_of, syms, as_of, n_exp, per_right),
        )
        rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    out: list[tuple[str, str]] = []
    for row in rows or []:
        if isinstance(row, Mapping):
            t, u = row.get("option_ticker"), row.get("underlying")
        else:
            t, u = (row[0], row[1]) if row and len(row) > 1 else (None, None)
        if t and u:
            out.append((str(t).strip().upper(), str(u).strip().upper()))
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
        # Retention is a window. `keep_days` is still read so an older config
        # keeps working, but the row cap is now only a runaway backstop.
        keep_hours = float(
            scfg.get("keep_hours") or (float(scfg.get("keep_days") or 2) * 24.0)
        )
        keep_max = int(scfg.get("keep_max") or TRIM_MAX_ROWS)
        deleted = trim_old_jobs(
            conn,
            keep_hours=keep_hours,
            keep_max=keep_max,
            # No gateway in front of the CLI Dagster fires, and a backfill day
            # can leave millions of rows; the API path keeps the shorter default.
            budget_sec=float(scfg.get("budget_sec") or TRIM_BUDGET_SEC),
        )
        # The job rows go; the record of what they did stays for the retention
        # window, because that series is the only history the queue has.
        try:
            from bifrost_market_data.queue_history import KEEP_DAYS, trim_samples

            samples_dropped = trim_samples(
                conn, keep_days=int(scfg.get("queue_sample_keep_days") or KEEP_DAYS)
            )
            if samples_dropped:
                logger.info("trimmed %s queue_sample rows", samples_dropped)
        except Exception as exc:  # noqa: BLE001 — sample retention must not fail the trim
            logger.warning("queue_sample trim skipped: %s", exc)
        # The matrix's memory, on the same schedule and for the same reason: it
        # is a history the source tables cannot reproduce. Its trim keeps the
        # newest row whatever the window says — retention bounds the history,
        # not the present.
        try:
            from bifrost_market_data.coverage_history import (
                KEEP_DAYS as COVERAGE_KEEP_DAYS,
                trim_samples as trim_coverage_samples,
            )

            coverage_dropped = trim_coverage_samples(
                conn,
                keep_days=int(scfg.get("coverage_sample_keep_days") or COVERAGE_KEEP_DAYS),
            )
            if coverage_dropped:
                logger.info("trimmed %s coverage_sample rows", coverage_dropped)
        except Exception as exc:  # noqa: BLE001 — sample retention must not fail the trim
            logger.warning("coverage_sample trim skipped: %s", exc)
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
        intraday_deleted = 0
        past_window_deleted = 0
        # option_trades is retired: option trades are not in Options Starter, the
        # slot has been gone since 0.10.3, and the table holds zero rows. Its 42
        # day partitions are owned by `postgres`, so the plugin's role cannot
        # drop them, and every nightly trim logged a failure it could never fix.
        # Rotating partitions for a table nothing writes to buys nothing either.
        # If the plan is ever upgraded, restore both calls with the slot.
        try:
            intraday_keep = int(scfg.get("option_snapshot_intraday_keep_days") or 30)
            # Bounded and committed per batch. One unbounded statement took 22
            # seconds across eighteen partitions to delete nothing, so under the
            # scheduler CLI's two-second default this had never once completed.
            snapshot_budget = float(scfg.get("snapshot_budget_sec") or 60.0)
            intraday_deleted = trim_option_snapshots(
                conn,
                keep_days=intraday_keep,
                intraday_only=True,
                budget_sec=snapshot_budget,
            )
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
            if _is_not_owner(exc):
                # Not a fault to raise every night: the partitions predate the
                # plugin's role and only their owner can drop them. The row
                # delete below does the retention regardless. To get the cheaper
                # path back, an elevated session runs:
                #   ALTER TABLE raw_market.option_snapshot_yYYYYmMM OWNER TO bifrost;
                logger.info(
                    "option_snapshot partitions are owned by another role, so they are "
                    "not dropped; rows past the window are deleted instead (%s)",
                    str(exc).splitlines()[0],
                )
            else:
                logger.warning("option_snapshot partition retention failed: %s", exc)
            if hasattr(conn, "rollback"):
                conn.rollback()
        # Extend every partitioned table's forward window. This lived only in
        # apply_ddl, which nothing re-runs on a schedule, so four tables sat 83
        # days from having nowhere to put the next insert.
        partitions_ensured = False
        try:
            from bifrost_market_data.schema.ddl import ensure_partitions

            ensure_partitions(conn)
            partitions_ensured = True
        except Exception as exc:  # noqa: BLE001 — retention must not fail on provisioning
            logger.warning("partition provisioning failed: %s", exc)
            if hasattr(conn, "rollback"):
                conn.rollback()

        # Whether or not the month could be dropped, the rows past the window go.
        # Dropping is cheaper and the plugin's role cannot do it — all eighteen
        # partitions are owned by `postgres` — so retention must not depend on
        # it. Deleting needs only the DML grant the role has.
        try:
            past_window_deleted = trim_option_snapshots(
                conn,
                keep_days=snapshot_keep,
                budget_sec=float(scfg.get("snapshot_budget_sec") or 60.0),
            )
        except Exception as exc:  # noqa: BLE001 — retention best-effort
            logger.warning("option_snapshot row retention failed: %s", exc)
            if hasattr(conn, "rollback"):
                conn.rollback()
        return {
            "slot": slot_key,
            "trimmed": deleted,
            "option_trades_partitions_dropped": partitions_dropped,
            "option_trades_keep_days": trades_keep,
            "option_trades_retention": "retired — the data face is not collected",
            "option_snapshot_partitions_dropped": snapshot_partitions_dropped,
            "option_snapshot_keep_days": snapshot_keep,
            "option_snapshot_keep_sessions": snapshot_keep_sessions,
            "option_snapshot_intraday_deleted": intraday_deleted,
            "option_snapshot_past_window_deleted": past_window_deleted,
            "partitions_ensured": partitions_ensured,
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

    # Research publishes the option universe as a rule (three tiers, no hand
    # list). An option slot configured with `universe: research` enumerates that
    # list; the watchlist stays as the fallback for a database where the table
    # is empty or absent, so the slot never stalls.
    tier_of: dict[str, str] = {}
    months_of: dict[str, int] = {}
    if (
        slot_key in ("option-refresh", "option-backfill", "eod-pipeline", "option-bars")
        and str(scfg.get("universe") or "").lower() == "research"
    ):
        universe = load_research_universe(conn)
        if universe:
            symbols = [u["symbol"] for u in universe]
            tier_of = {u["symbol"]: u["tier"] for u in universe}
            months_of = {u["symbol"]: u["history_months"] for u in universe}
        else:
            logger.warning(
                "universe=research but research.option_universe is empty; using the watchlist"
            )

    # Session-once: skip only when this session is already *covered*. The probe
    # runs here rather than earlier because "covered" needs the symbol list.
    if slot_key in SESSION_ONCE_SLOTS and not force:
        kinds_seen, session_field, symbol_field = SESSION_ONCE_SLOTS[slot_key]
        if slot_key == "eod-pipeline":
            expected = {storage_underlying(x) for x in union_iv_radar_benchmarks(symbols, cfg)}
        else:
            expected = set(symbols)
        seen = _session_symbols_enqueued(
            conn,
            kinds_seen,
            session_field=session_field,
            session=day_s,
            symbol_field=symbol_field,
        )
        missing = expected - seen
        if expected and not missing:
            logger.info(
                "slot=%s already covers session %s (%d symbols, kinds=%s), skipping",
                slot_key,
                day_s,
                len(expected),
                ",".join(kinds_seen),
            )
            return {
                "slot": slot_key,
                "target_date": day_s,
                "skipped": True,
                "reason": "evidence_exists",
                "covered": len(seen),
                "enqueued": 0,
                "deduped": 0,
                "jobs": [],
            }
        if seen:
            logger.info(
                "slot=%s session %s partially covered: %d of %d symbols seen, enqueueing the rest",
                slot_key,
                day_s,
                len(expected) - len(missing),
                len(expected),
            )

    def _tier_pri(sym: str) -> int | None:
        bump = TIER_PRIORITY_BUMP.get(tier_of.get(sym, ""))
        return None if bump is None else priority + bump

    jobs: list[dict[str, Any]] = []
    specs: list[tuple[str, dict[str, Any], int, int]] = []

    def _add(kind: str, payload: dict[str, Any], pri: int | None = None) -> None:
        # Collected here, written in one statement below.
        effective = pri if pri is not None else priority
        specs.append((kind, payload, effective, 3))
        jobs.append(
            {"kind": kind, "payload": payload, "id": None, "deduped": True, "priority": effective}
        )

    if slot_key == "stock-eod":
        for sym in symbols:
            _add("stock_daily", {"symbol": sym, "from": day_s, "to": day_s})

    elif slot_key == "eod-pipeline":
        # One chain snapshot per underlying; the handler derives the session's
        # open interest from the same response, so no second download.
        # Index spot (I:SPX) is not enqueued: it needs an Indices plan.
        pipeline_syms = union_iv_radar_benchmarks(symbols, cfg)
        # Research universe: resident names are snapshotted whole — that is the
        # watchlist and the benchmarks, the chains the Loop reads today. Core
        # and edge get the near-the-money window: the next few expiries and a
        # strike band around spot, the same shape option-bars already prices.
        windows: dict[str, tuple[float, float, str]] = {}
        if tier_of:
            bounded = [s for s in pipeline_syms if tier_of.get(s) in ("core", "edge")]
            windows = load_snapshot_windows(
                conn,
                bounded,
                as_of=day,
                expiries=int(scfg.get("expiries") or 3),
                strike_pct=float(scfg.get("strike_pct") or 0.15),
            )
        for sym in pipeline_syms:
            storage = storage_underlying(sym)
            payload: dict[str, Any] = {"underlying": storage, "trade_date": day_s}
            win = windows.get(sym)
            if win is not None:
                payload["strike_gte"], payload["strike_lte"], payload["expiration_lte"] = win
            _add("option_snapshot", payload, pri=_tier_pri(sym))

    elif slot_key == "intraday-chain":
        # Several observations a session: the model keys each row to the instant
        # it was taken, so these sit alongside the 16:00 EOD row instead of
        # fighting it for the same primary key.
        observed = datetime.now(timezone.utc)
        for sym in union_iv_radar_benchmarks(symbols, cfg):
            _add(
                "option_snapshot",
                {
                    "underlying": storage_underlying(sym),
                    "trade_date": day_s,
                    "intraday": True,
                    "observed_at": observed.isoformat(),
                },
            )

    elif slot_key == "treasury":
        _add("treasury_yields", {"lookback_days": int(scfg.get("lookback_days") or 30)})

    elif slot_key == "option-backfill":
        # One planner job per underlying per expiry month. Each enumerates that
        # month's contracts and queues the aggregate jobs, so no single request
        # has to walk 50,000 contracts.
        months = int(scfg.get("months") or 24)
        strike_pct = float(scfg.get("strike_pct") or 0.30)
        dte = int(scfg.get("dte") or 90)
        for sym in union_iv_radar_benchmarks(symbols, cfg):
            storage = storage_underlying(sym)
            # An edge name carries a year of history, a core or resident name
            # two; the row says which, the slot default covers the watchlist.
            for back in range(months_of.get(sym, months)):
                first = _month_start(day, back)
                last = _month_end(first)
                _add(
                    "option_backfill_plan",
                    {
                        "underlying": storage,
                        "expiry_gte": first.isoformat(),
                        "expiry_lte": last.isoformat(),
                        "strike_pct": strike_pct,
                        "dte": dte,
                    },
                    pri=_tier_pri(sym),
                )

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
        # Names the universe lists but nothing has enumerated yet go first,
        # up to a per-run cap — that is how a 500-name core ramps in over a
        # day or two of six-hourly runs without a one-off trigger, and how an
        # edge newcomer gets its chain the next session. Re-listing the same
        # name before its job finished is a no-op: jobs dedup on payload hash.
        # The rotation below then keeps everyone fresh.
        max_new = int(scfg.get("max_new_per_run") or 0)
        fresh: list[str] = []
        if symbols and max_new > 0 and tier_of:
            have = enumerated_underlyings(conn)
            # None means the lookup failed. Ramping on an unknown state is how
            # the same 150 names got re-enumerated all afternoon; skip instead.
            if have is not None:
                fresh = [s for s in symbols if s not in have and s not in bench_set][:max_new]
        fresh_set = set(fresh)
        if symbols:
            # Stalest first. The old rotation hashed the *target date*, which is
            # identical across all four six-hourly runs, so three of them
            # re-fetched what the first had just done and the universe came round
            # about every 48 days. Ordering by when each catalogue was last
            # walked advances on every run and repairs a missed one by itself.
            last_seen = stalest_underlyings(conn)
            candidates = [s for s in symbols if s not in bench_set and s not in fresh_set]
            if last_seen is not None:
                # Never enumerated sorts before any timestamp.
                candidates.sort(key=lambda s: (last_seen.get(s) is not None, last_seen.get(s), s))
            else:
                # Could not tell — keep the old deterministic rotation rather
                # than treat every name as equally stale.
                offset = int(hashlib.sha256(day_s.encode("utf-8")).hexdigest(), 16) % len(symbols)
                rotated = symbols[offset:] + symbols[:offset]
                candidates = [s for s in rotated if s not in bench_set and s not in fresh_set]
            batch = list(benches) + fresh + candidates[: max(0, batch_size)]
        else:
            batch = list(benches)
        # The contract handler upserts option_expiration from the same page
        # walk, so a separate expiration job would re-download the catalogue.
        for sym in batch:
            _add("option_contract", {"underlying": sym, "expired": False}, pri=_tier_pri(sym))

    elif slot_key == "option-bars":
        # Scope follows the universe, not the watchlist. The P4 backfill bought
        # two years of option_daily for 575 underlyings while this slot renewed
        # only the watchlist union — measured 2026-09-10, depth 503/575 at
        # target against breadth 25/575, so 550 names' history would have
        # stopped advancing the day the backfill ended.
        bars_syms = union_iv_radar_benchmarks(symbols, cfg)
        tickers = load_option_tickers_near_spot(
            conn,
            bars_syms,
            as_of=day,
            expiries=int(scfg.get("expiries") or 3),
            strikes_each_side=int(scfg.get("strikes_each_side") or 10),
        )
        for ot, und in tickers:
            # The catalogue's underlying, not the ticker's root: an adjusted
            # contract reads O:BDX1… and belongs to BDX.
            _add(
                "option_daily",
                {"option_ticker": ot, "underlying": und, "from": day_s, "to": day_s},
            )

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
        for ot, und in batch:
            _add(
                "option_minute",
                {
                    "option_ticker": ot,
                    "underlying": und,
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

    elif slot_key == "ticker-details":
        # The overview fields — list_date, sector, market_cap, description —
        # come only from /v3/reference/tickers/{ticker}. The list endpoint does
        # not carry them, which is why ticker_sync declares them
        # never-overwrite-on-conflict and why `list_date` is null for all 5,317
        # active tickers (measured 2026-09-11).
        #
        # The handler for this has existed since ticker_sync gained its
        # `mode: "detail"` branch. Nothing ever enqueued it. That absence is
        # what blocks declaring "an instrument listed after the window opened
        # cannot reach a five-year target", which is three of the four depth
        # partials on the board.
        #
        # Null list_date first, then the stalest: the backlog drains before the
        # refresh starts competing with it.
        batch_size = int(scfg.get("batch_size") or 200)
        for sym in tickers_needing_detail(conn, limit=batch_size):
            _add("ticker_sync", {"mode": "detail", "symbol": sym}, pri=priority)

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
        si_pages = int(scfg.get("short_interest_max_pages") or 400)
        _add("ratios_market", {"date": session_s}, pri=priority)
        _add("short_volume_market", {"date": session_s}, pri=priority)
        _add(
            "short_interest_market",
            {
                "settlement_date_gte": (session - timedelta(days=si_back)).isoformat(),
                "max_pages": si_pages,
            },
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
    parser = argparse.ArgumentParser(
        description="Enqueue market-data ingest jobs for a schedule slot"
    )
    parser.add_argument(
        "--slot",
        required=True,
        choices=SLOT_NAMES,
        help="Schedule slot to enqueue",
    )
    parser.add_argument(
        "--date", default=None, help="Target date YYYY-MM-DD (default: latest NY weekday)"
    )
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

    configure_logging(logging.INFO)

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
    # The `bifrost` role defaults to statement_timeout=2s, which is a writer
    # safety net sized for handler batches, not for the slots. Three separate
    # failures traced back to this one omission: the job trim's batched delete,
    # the intraday snapshot delete, and CREATE TABLE ... PARTITION OF, each
    # cancelled at two seconds while the API and the workers — which do raise it
    # — ran the same statements fine. The slots raise it once, here.
    kw = postgres_connect_kwargs(cfg, statement_timeout="120s")
    conn = None
    last_err: Exception | None = None
    for attempt in range(1, 6):
        try:
            conn = psycopg.connect(**kw, connect_timeout=10)
            break
        except psycopg.OperationalError as exc:
            last_err = exc
            logger.warning("postgres connect attempt %s/5 failed: %s", attempt, exc)
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
