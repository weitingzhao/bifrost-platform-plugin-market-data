"""Manual backfill: enqueue historical market-data ingest jobs."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.scheduler.backfill import SUPPORTED_BACKFILL_KINDS, enqueue_backfill

logger = logging.getLogger(__name__)

_KIND_CHOICES = tuple(sorted(SUPPORTED_BACKFILL_KINDS))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enqueue historical market-data backfill jobs")
    parser.add_argument("--kind", required=True, choices=_KIND_CHOICES)
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated equity symbols (stock_daily / financials / option_*)",
    )
    parser.add_argument(
        "--option-tickers",
        default=None,
        help="Comma-separated option tickers (option_daily / option_minute), e.g. O:AAPL250620C00150000",
    )
    parser.add_argument("--from", dest="from_date", default=None, help="YYYY-MM-DD (date-range kinds)")
    parser.add_argument("--to", dest="to_date", default=None, help="YYYY-MM-DD (date-range kinds)")
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=None,
        help="Max days per job (default: 30 for *_minute, 365 otherwise)",
    )

    parser.add_argument("--priority", type=int, default=0)
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    kind = args.kind
    symbols: list[str] = []

    if kind == "option_daily" or kind == "option_minute":
        if not args.option_tickers:
            parser.error(f"--option-tickers required for {kind}")
        symbols = [s.strip().upper() for s in args.option_tickers.split(",") if s.strip()]
    elif kind in ("stock_daily", "stock_minute", "financials", "option_snapshot", "option_contract"):
        if not args.symbols:
            parser.error(f"--symbols required for {kind}")
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    elif kind == "stock_daily_grouped":
        if not args.from_date or not args.to_date:
            parser.error("--from and --to required for stock_daily_grouped")
    elif kind == "ticker_sync":
        pass
    else:
        parser.error(f"unsupported kind: {kind}")

    if kind in (
        "stock_daily",
        "option_daily",
        "stock_daily_grouped",
        "stock_minute",
        "option_minute",
    ):
        if not args.from_date or not args.to_date:
            parser.error(f"--from and --to required for {kind}")

    from_d = date.fromisoformat(args.from_date) if args.from_date else None
    to_d = date.fromisoformat(args.to_date) if args.to_date else None

    cfg = load_config(args.config)
    import psycopg

    conn = psycopg.connect(**postgres_connect_kwargs(cfg))
    try:
        result = enqueue_backfill(
            conn,
            kind=kind,
            symbols=symbols or None,
            from_date=from_d,
            to_date=to_d,
            chunk_days=int(args.chunk_days) if args.chunk_days is not None else None,
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
