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
    "reference",
    "fundamentals-rotate",
    "readiness-refresh",
    "trim",
    "stock-snapshot",
    "stock-movers",
    "oi-gap-heal",
    "max-pain",
    "atm-iv-pcr",
    "iv-percentile",
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


_READINESS_REFRESH_SQL = """
UPDATE public.stock_readiness_daily srd
SET bar_count_lookback = v.bar_rows,
    first_bar_date = v.first_bar_date,
    last_bar_date = v.last_bar_date,
    null_close_rows = v.null_close_rows,
    null_volume_rows = v.null_volume_rows,
    price_ready = v.price_ready
FROM public.v_sepa_symbol_price_readiness v
WHERE srd.as_of_date = v.as_of_date AND srd.symbol = v.symbol
""".strip()


def _run_readiness_refresh(conn: Any) -> int:
    """Execute readiness UPDATE and return number of rows affected."""
    with conn.cursor() as cur:
        cur.execute(_READINESS_REFRESH_SQL)
        rows = cur.rowcount if hasattr(cur, "rowcount") else 0
    conn.commit()
    return rows


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

    if slot_key == "readiness-refresh":
        rows_updated = _run_readiness_refresh(conn)
        logger.info("readiness-refresh updated %d rows", rows_updated)
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
        trading_days = fetch_recent_trading_days(conn, lookback)
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

    if slot_key == "max-pain":
        # D8=B: today + lookback_days trading days for gap heal after holidays.
        # Inline DB compute from market.option_open_interest (no Polygon).
        from bifrost_market_data.analytics.max_pain import compute_max_pain_for_date
        from bifrost_market_data.quality import fetch_recent_trading_days

        lookback = int(scfg.get("lookback_days") or 3)
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
        day_results: list[dict[str, Any]] = []
        total_written = 0
        total_groups = 0
        for td in trading_days:
            one = compute_max_pain_for_date(
                conn,
                trade_date=td,
                underlyings=symbols or None,
            )
            day_results.append(one)
            total_written += int(one.get("rows_written") or 0)
            total_groups += int(one.get("groups") or 0)
        logger.info(
            "max-pain lookback=%s days=%s..%s groups=%s rows_written=%s",
            lookback,
            trading_days[0].isoformat(),
            trading_days[-1].isoformat(),
            total_groups,
            total_written,
        )
        return {
            "slot": slot_key,
            "lookback_days": lookback,
            "symbols": len(symbols),
            "trading_days": [d.isoformat() for d in trading_days],
            "groups": total_groups,
            "rows_written": total_written,
            "days": day_results,
            "enqueued": 0,
            "deduped": 0,
        }

    if slot_key == "atm-iv-pcr":
        # D12=A: merged ATM IV + PCR over lookback trading days (inline, no Polygon).
        from bifrost_market_data.analytics.atm_iv import compute_atm_iv_for_date
        from bifrost_market_data.analytics.pcr import compute_pcr_for_date
        from bifrost_market_data.quality import fetch_recent_trading_days

        lookback = int(scfg.get("lookback_days") or 3)
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
        atm_days: list[dict[str, Any]] = []
        pcr_days: list[dict[str, Any]] = []
        atm_written = 0
        pcr_written = 0
        for td in trading_days:
            atm_one = compute_atm_iv_for_date(
                conn,
                trade_date=td,
                underlyings=symbols or None,
            )
            pcr_one = compute_pcr_for_date(
                conn,
                trade_date=td,
                underlyings=symbols or None,
            )
            atm_days.append(atm_one)
            pcr_days.append(pcr_one)
            atm_written += int(atm_one.get("rows_written") or 0)
            pcr_written += int(pcr_one.get("rows_written") or 0)
        logger.info(
            "atm-iv-pcr lookback=%s days=%s..%s atm_rows=%s pcr_rows=%s",
            lookback,
            trading_days[0].isoformat(),
            trading_days[-1].isoformat(),
            atm_written,
            pcr_written,
        )
        return {
            "slot": slot_key,
            "lookback_days": lookback,
            "symbols": len(symbols),
            "trading_days": [d.isoformat() for d in trading_days],
            "atm_rows_written": atm_written,
            "pcr_rows_written": pcr_written,
            "atm_days": atm_days,
            "pcr_days": pcr_days,
            "enqueued": 0,
            "deduped": 0,
        }

    if slot_key == "iv-percentile":
        # D12=A: after atm-iv-pcr; reads market_analytics.atm_iv_daily.
        from bifrost_market_data.analytics.iv_percentile import (
            DEFAULT_PERCENTILE_WINDOW,
            compute_iv_percentile_for_date,
        )
        from bifrost_market_data.quality import fetch_recent_trading_days

        lookback = int(scfg.get("lookback_days") or 3)
        pct_window = int(scfg.get("percentile_window") or DEFAULT_PERCENTILE_WINDOW)
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
                "percentile_window": pct_window,
                "symbols": len(symbols),
                "skipped": True,
                "reason": "no trading days",
                "enqueued": 0,
                "deduped": 0,
            }
        day_results: list[dict[str, Any]] = []
        total_written = 0
        for td in trading_days:
            one = compute_iv_percentile_for_date(
                conn,
                trade_date=td,
                underlyings=symbols or None,
                percentile_window=pct_window,
            )
            day_results.append(one)
            total_written += int(one.get("rows_written") or 0)
        logger.info(
            "iv-percentile lookback=%s window=%s days=%s..%s rows_written=%s",
            lookback,
            pct_window,
            trading_days[0].isoformat(),
            trading_days[-1].isoformat(),
            total_written,
        )
        return {
            "slot": slot_key,
            "lookback_days": lookback,
            "percentile_window": pct_window,
            "symbols": len(symbols),
            "trading_days": [d.isoformat() for d in trading_days],
            "rows_written": total_written,
            "days": day_results,
            "enqueued": 0,
            "deduped": 0,
        }

    skip_on_holiday = slot_key in (
        "stock-eod",
        "eod-pipeline",
        "universe-daily",
        "corporate",
        "option-bars",
        "minute-bars",
        "fundamentals-rotate",
        "stock-snapshot",
        "stock-movers",
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

    elif slot_key == "reference":
        # Universe ticker sync — run on weekends/holidays too (calendar-like).
        _add("ticker_sync", {"mode": "universe"}, pri=priority)

    elif slot_key == "fundamentals-rotate":
        # Per-symbol financials with deterministic daily rotation (not full universe).
        batch_size = int(scfg.get("batch_size") or 40)
        if symbols:
            offset = int(hashlib.sha256(day_s.encode("utf-8")).hexdigest(), 16) % len(symbols)
            rotated = symbols[offset:] + symbols[:offset]
            batch = rotated[: max(0, batch_size)]
        else:
            batch = []
        for sym in batch:
            _add("financials", {"symbol": sym}, pri=priority)

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
