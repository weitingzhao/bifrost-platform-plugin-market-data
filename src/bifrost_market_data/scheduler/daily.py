"""Daily / EOD job generation — CronJob-driven enqueue into data_ops.job_ingest."""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import yaml

from bifrost_market_data.config import load_config, postgres_connect_kwargs
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
    "minute-bars",
    "calendar",
    "trim",
)

DEFAULT_WATCHLIST_QUERY = """
SELECT DISTINCT symbol FROM public.watchlist
WHERE sec_type = 'STK' AND optionable = true
  AND symbol IS NOT NULL AND trim(symbol) <> ''
""".strip()


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
    """Check ``data_ops.us_trading_calendar``.

    Missing rows fall back to weekday check (calendar may not be populated yet).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT is_trading FROM data_ops.us_trading_calendar WHERE cal_date = %s",
            (d,),
        )
        row = cur.fetchone() if hasattr(cur, "fetchone") else None
    if row is None:
        return d.weekday() < 5
    if isinstance(row, Mapping):
        return bool(row["is_trading"])
    return bool(row[0])


def load_watchlist_symbols(
    conn: Any,
    scheduler_cfg: Mapping[str, Any],
) -> list[str]:
    """Return watchlist symbols from config override or DB query."""
    override = scheduler_cfg.get("watchlist_symbols")
    if override:
        return sorted({str(s).strip().upper() for s in override if str(s).strip()})

    query = str(scheduler_cfg.get("watchlist_query") or DEFAULT_WATCHLIST_QUERY).strip()
    symbols: list[str] = []
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall() if hasattr(cur, "fetchall") else []
        # Support FakeConn that may not implement fetchall — fall back to empty
        if rows is None:
            rows = []
        for row in rows:
            if isinstance(row, Mapping):
                sym = row.get("symbol") or next(iter(row.values()), None)
            else:
                sym = row[0] if row else None
            if sym:
                symbols.append(str(sym).strip().upper())
    return sorted(set(symbols))


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
              FROM market.option_contract
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
) -> dict[str, Any]:
    """Generate jobs for one schedule slot. Returns summary dict."""
    slot_key = str(slot).strip().lower()
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
        return {"slot": slot_key, "trimmed": deleted, "enqueued": 0, "deduped": 0}

    skip_on_holiday = slot_key in (
        "stock-eod",
        "eod-pipeline",
        "universe-daily",
        "corporate",
        "option-bars",
        "minute-bars",
    )
    if skip_on_holiday and not is_trading_day(conn, day):
        logger.info("slot=%s target_date=%s is not a trading day, skipping", slot_key, day_s)
        return {
            "slot": slot_key,
            "target_date": day_s,
            "skipped": True,
            "enqueued": 0,
            "deduped": 0,
            "jobs": [],
        }

    symbols = list(watchlist_symbols) if watchlist_symbols is not None else load_watchlist_symbols(conn, cfg)
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
        for sym in symbols:
            _add("option_snapshot", {"underlying": sym})
            _add("option_open_interest", {"underlying": sym, "trade_date": day_s})

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
        if symbols:
            # Deterministic rotation so the whole watchlist is covered over days.
            offset = int(hashlib.sha256(day_s.encode("utf-8")).hexdigest(), 16) % len(symbols)
            rotated = symbols[offset:] + symbols[:offset]
            batch = rotated[: max(0, batch_size)]
        else:
            batch = []
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

    elif slot_key == "minute-bars":
        for sym in symbols:
            _add(
                "stock_minute",
                {
                    "symbol": sym,
                    "from": day_s,
                    "to": day_s,
                    "multiplier": 1,
                    "timespan": "minute",
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
