"""CLI: extract historical OI from market.option_snapshot into option_open_interest."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.ingest.option_oi_extract import extract_oi_from_snapshots
from bifrost_market_data.scheduler.daily import (
    DEFAULT_WATCHLIST_QUERY,
    load_schedule,
    load_watchlist_symbols,
)

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Gap-fill market.option_open_interest from market.option_snapshot "
            "(ON CONFLICT DO NOTHING; never overwrites live ingest rows)"
        )
    )
    parser.add_argument(
        "--underlying",
        default=None,
        help="Comma-separated underlyings (default: scheduler watchlist query)",
    )
    parser.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--config", default=None, help="Path to market-data.yaml")
    parser.add_argument("--schedule", default=None, help="Path to schedule.yaml")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    from_d = date.fromisoformat(args.from_date)
    to_d = date.fromisoformat(args.to_date)

    cfg = load_config(args.config)
    schedule = load_schedule(args.schedule)
    scheduler_cfg = dict(schedule.get("scheduler") or {})
    if isinstance(cfg.get("scheduler"), dict):
        merged = dict(scheduler_cfg)
        merged.update(cfg["scheduler"])
        scheduler_cfg = merged

    import psycopg

    conn = psycopg.connect(**postgres_connect_kwargs(cfg))
    try:
        if args.underlying:
            underlyings = [s.strip().upper() for s in args.underlying.split(",") if s.strip()]
        else:
            underlyings = load_watchlist_symbols(
                conn,
                scheduler_cfg or {"watchlist_query": DEFAULT_WATCHLIST_QUERY},
            )
        result = extract_oi_from_snapshots(
            conn,
            underlyings=underlyings,
            from_date=from_d,
            to_date=to_d,
        )
    finally:
        conn.close()

    logger.info(
        "oi extract from=%s to=%s underlyings=%s candidates=%s skipped=%s",
        result["from_date"],
        result["to_date"],
        result["underlyings"],
        result["candidates"],
        result["skipped"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
