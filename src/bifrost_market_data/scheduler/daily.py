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
from bifrost_market_data.ingest.index_options import (
    load_index_option_roots_from_cfg,
    spot_api_symbol,
    storage_underlying,
)
from bifrost_market_data.scheduler.enqueue import insert_job, trim_old_jobs

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
    "oi-gap-heal",
)

# Wave 2.1: analytics upserts moved to bifrost_research.scheduler.volatility
MIGRATED_ANALYTICS_SLOTS = frozenset({"max-pain", "atm-iv-pcr", "iv-percentile"})

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
FROM raw_market.stock_financials
WHERE report_type = 'income_statement'
  AND symbol IS NOT NULL AND trim(symbol) <> ''
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
                return symbols
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
    """Symbols that already have an income_statement row in stock_financials."""
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


def load_option_tickers(
    conn: Any,
    underlyings: Sequence[str],
    *,
    as_of: date,
    expiry_days: int = 60,
    max_per_underlying: int = 40,
) -> list[str]:
    """Load near-term option_tickers from market.option_contract for underlyings.

    Caps per-underlying count to keep Polygon job volume bounded.
    """
    syms = [str(s).strip().upper() for s in underlyings if str(s).strip()]
    if not syms:
        return []
    expiry_days = max(0, int(expiry_days))
    max_per = max(1, int(max_per_underlying))
    end = as_of + timedelta(days=expiry_days)
    tickers: list[str] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT option_ticker FROM (
              SELECT
                option_ticker,
                underlying,
                ROW_NUMBER() OVER (
                  PARTITION BY underlying
                  ORDER BY expiry ASC, strike ASC, option_right ASC
                ) AS rn
              FROM raw_market.option_contract
              WHERE underlying = ANY(%s)
                AND expiry >= %s
                AND expiry <= %s
            ) ranked
            WHERE rn <= %s
            ORDER BY option_ticker
            """,
            (syms, as_of, end, max_per),
        )
        rows = cur.fetchall() if hasattr(cur, "fetchall") else []
        if rows is None:
            rows = []
        for row in rows:
            if isinstance(row, Mapping):
                t = row.get("option_ticker") or next(iter(row.values()), None)
            else:
                t = row[0] if row else None
            if t:
                tickers.append(str(t).strip().upper())
    return tickers


def _run_readiness_refresh(conn: Any) -> int:
    """RETIRED: public.stock_readiness_daily removed from Trade DB (core 0.10.7+)."""
    _ = conn
    logger.info(
        "readiness-refresh retired — stock_readiness_daily dropped; "
        "use dw_stock.mart_sepa_* via dbt (CronJob suspend must stay true)"
    )
    return 0


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
) -> dict[str, Any]:
    """Generate jobs for one schedule slot. Returns summary dict.

    ``force=True`` runs holiday-gated slots on weekends (used for CS financials catch-up).
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
        trades_keep = int(scfg.get("option_trades_keep_days") or 30)
        snapshot_keep = int(scfg.get("option_snapshot_keep_days") or 90)
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
            "enqueued": 0,
            "deduped": 0,
        }

    if slot_key == "readiness-refresh":
        # Inline slot retired — stock_readiness_daily no longer exists on Trade DB.
        rows_updated = _run_readiness_refresh(conn)
        logger.info("readiness-refresh retired (rows_updated=%d)", rows_updated)
        return {"slot": slot_key, "rows_updated": rows_updated, "enqueued": 0, "deduped": 0}

    if slot_key == "oi-gap-heal":
        # D6=B: weekly DB-to-DB extract over recent trading days (no Polygon).
        # Inline like readiness-refresh — pure SQL gap-fill, no worker job kind.
        from bifrost_market_data.ingest.option_oi_extract import extract_oi_from_snapshots
        from bifrost_market_data.quality import fetch_recent_trading_days

        lookback = int(scfg.get("lookback_days") or 14)
        symbols = (
            list(watchlist_symbols)
            if watchlist_symbols is not None
            else load_watchlist_symbols(conn, cfg)
        )
        trading_days = fetch_recent_trading_days(conn, lookback, as_of=day)
        if not trading_days:
            return {
                "slot": slot_key,
                "lookback_days": lookback,
                "symbols": len(symbols),
                "skipped": True,
                "reason": "no trading days",
                "enqueued": 0,
                "deduped": 0,
            }
        extract_result = extract_oi_from_snapshots(
            conn,
            underlyings=symbols or None,
            from_date=trading_days[0],
            to_date=trading_days[-1],
        )
        logger.info(
            "oi-gap-heal from=%s to=%s candidates=%s skipped=%s",
            extract_result.get("from_date"),
            extract_result.get("to_date"),
            extract_result.get("candidates"),
            extract_result.get("skipped"),
        )
        return {
            "slot": slot_key,
            "lookback_days": lookback,
            "symbols": len(symbols),
            "enqueued": 0,
            "deduped": 0,
            **extract_result,
        }

    skip_on_holiday = slot_key in (
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
    )
    if skip_on_holiday and not force and not is_trading_day(conn, day):
        logger.info("slot=%s target_date=%s is not a trading day, skipping", slot_key, day_s)
        return {
            "slot": slot_key,
            "target_date": day_s,
            "skipped": True,
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
    enqueued = 0
    deduped = 0
    jobs: list[dict[str, Any]] = []

    def _add(kind: str, payload: dict[str, Any], pri: int | None = None) -> None:
        nonlocal enqueued, deduped
        job_id = insert_job(conn, kind=kind, payload=payload, priority=pri if pri is not None else priority)
        if job_id is None:
            deduped += 1
            jobs.append({"kind": kind, "payload": payload, "id": None, "deduped": True})
        else:
            enqueued += 1
            jobs.append({"kind": kind, "payload": payload, "id": job_id, "deduped": False})

    if slot_key == "stock-eod":
        for sym in symbols:
            _add("stock_daily", {"symbol": sym, "from": day_s, "to": day_s})

    elif slot_key == "eod-pipeline":
        pipeline_syms = union_iv_radar_benchmarks(symbols, cfg)
        for sym in pipeline_syms:
            storage = storage_underlying(sym)
            _add("option_snapshot", {"underlying": storage})
            _add("option_open_interest", {"underlying": storage, "trade_date": day_s})
        # Index spot (I:SPX → store as SPX) so Research GEX has a close.
        for root in load_index_option_roots_from_cfg(cfg):
            spot = spot_api_symbol(root)
            if not spot:
                continue
            _add(
                "stock_daily",
                {
                    "symbol": spot,
                    "storage_symbol": root,
                    "from": day_s,
                    "to": day_s,
                },
            )

    elif slot_key == "universe-daily":
        _add(
            "stock_daily_grouped",
            {"from": day_s, "to": day_s, "market": "stocks"},
            pri=priority,
        )

    elif slot_key == "corporate":
        for sym in symbols:
            _add("splits", {"symbol": sym})
            _add("dividends", {"symbol": sym})

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
        for sym in batch:
            _add("option_contract", {"underlying": sym, "expired": False})
            _add("option_expiration", {"underlying": sym})

    elif slot_key == "option-bars":
        expiry_days = int(scfg.get("expiry_days") or 60)
        max_per = int(scfg.get("max_per_underlying") or 40)
        tickers = load_option_tickers(
            conn,
            symbols,
            as_of=day,
            expiry_days=expiry_days,
            max_per_underlying=max_per,
        )
        for ot in tickers:
            _add("option_daily", {"option_ticker": ot, "from": day_s, "to": day_s})

    elif slot_key == "option-trades":
        # Daily REST tape (not WebSocket). Universe: SPX ∪ watchlist top 50.
        universe_limit = int(scfg.get("universe_limit") or 50)
        underlyings = option_trades_universe(symbols, limit=universe_limit)
        expiry_days = int(scfg.get("expiry_days") or 60)
        max_per = int(scfg.get("max_per_underlying") or 40)
        tickers = load_option_tickers(
            conn,
            underlyings,
            as_of=day,
            expiry_days=expiry_days,
            max_per_underlying=max_per,
        )
        for ot in tickers:
            _add(
                "option_trades",
                {
                    "option_ticker": ot,
                    "from": day_s,
                    "to": day_s,
                    "trade_date": day_s,
                },
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
        # Option minute bars: rotate a bounded batch of near-term contracts.
        expiry_days = int(scfg.get("expiry_days") or 45)
        max_per = int(scfg.get("max_per_underlying") or 10)
        batch_size = int(scfg.get("batch_size") or 80)
        tickers = load_option_tickers(
            conn,
            symbols,
            as_of=day,
            expiry_days=expiry_days,
            max_per_underlying=max_per,
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
