"""Option daily backfill CLI — Wave LO-5.

Enqueues ``option_daily`` jobs into ``ops_jobs.job_ingest`` for historical
fill.  CronJob ``market-data-option-backfill`` is suspended until Owner
Polygon tier decision (see docs/OPTION_BACKFILL_PROGRAM.md).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.scheduler.enqueue import insert_job

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("market_data.backfill")


def _parse_symbols(raw: str) -> list[str]:
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def enqueue_option_daily_backfill(
    conn,
    *,
    symbols: list[str],
    days: int,
    dry_run: bool,
) -> dict[str, int]:
    end = date.today()
    start = end - timedelta(days=max(1, days))
    enqueued = 0
    skipped = 0
    for sym in symbols:
        day = start
        while day <= end:
            payload = {
                "option_ticker": sym,
                "underlying": sym,
                "from": day.isoformat(),
                "to": day.isoformat(),
            }
            if dry_run:
                logger.info("dry-run enqueue option_daily %s", payload)
            else:
                job_id = insert_job(conn, kind="option_daily", payload=payload)
                if job_id is None:
                    skipped += 1
                else:
                    enqueued += 1
            day += timedelta(days=1)
    return {"enqueued": enqueued, "skipped": skipped, "symbols": len(symbols)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enqueue option_daily backfill jobs")
    parser.add_argument("--kind", default="option_daily", choices=["option_daily"])
    parser.add_argument("--symbols", default="NVDA,TSLA")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.kind != "option_daily":
        logger.error("only option_daily supported in LO-5 stub")
        return 1

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        logger.error("no symbols")
        return 1

    import psycopg2

    cfg = load_config()
    conn = psycopg2.connect(**postgres_connect_kwargs(cfg))
    try:
        stats = enqueue_option_daily_backfill(
            conn, symbols=symbols, days=args.days, dry_run=args.dry_run
        )
        logger.info("backfill plan: %s", stats)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
