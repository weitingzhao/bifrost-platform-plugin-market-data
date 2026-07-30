"""Manual backfill: enqueue historical stock_daily / option_daily jobs."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.scheduler.backfill import enqueue_backfill

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enqueue historical market-data backfill jobs")
    parser.add_argument("--kind", required=True, choices=("stock_daily", "option_daily"))
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated equity symbols (stock_daily)",
    )
    parser.add_argument(
        "--option-tickers",
        default=None,
        help="Comma-separated option tickers (option_daily), e.g. O:AAPL250620C00150000",
    )
    parser.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--chunk-days", type=int, default=365)
    parser.add_argument("--priority", type=int, default=0)
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    if args.kind == "stock_daily":
        if not args.symbols:
            parser.error("--symbols required for stock_daily")
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        if not args.option_tickers:
            parser.error("--option-tickers required for option_daily")
        symbols = [s.strip().upper() for s in args.option_tickers.split(",") if s.strip()]

    cfg = load_config(args.config)
    import psycopg

    conn = psycopg.connect(**postgres_connect_kwargs(cfg))
    try:
        result = enqueue_backfill(
            conn,
            kind=args.kind,
            symbols=symbols,
            from_date=date.fromisoformat(args.from_date),
            to_date=date.fromisoformat(args.to_date),
            chunk_days=int(args.chunk_days),
            priority=int(args.priority),
        )
    finally:
        conn.close()

    logger.info(
        "backfill kind=%s enqueued=%s deduped=%s chunks=%s",
        result["kind"],
        result["enqueued"],
        result["deduped"],
        result["chunks"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
