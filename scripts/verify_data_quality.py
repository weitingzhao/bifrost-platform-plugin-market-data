"""P7 data quality verification — continuity, completeness, freshness.

Exit 0 when all checks pass; exit 1 on any failure.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.quality import (
    FRESHNESS_MAX_AGE_HOURS,
    STOCK_DAILY_GAP_LOOKBACK_DAYS,
    STOCK_DAILY_MIN_SYMBOLS,
    run_all_checks,
)

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify market-data quality (P7)")
    parser.add_argument("--config", default=None, help="Path to market-data.yaml")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON report")
    parser.add_argument("--min-symbols", type=int, default=STOCK_DAILY_MIN_SYMBOLS)
    parser.add_argument("--lookback-days", type=int, default=STOCK_DAILY_GAP_LOOKBACK_DAYS)
    parser.add_argument("--max-age-hours", type=float, default=FRESHNESS_MAX_AGE_HOURS)
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated watchlist override (skips public.watchlist query)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    symbols_override: list[str] | None = None
    if args.symbols:
        symbols_override = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    cfg = load_config(args.config)
    import psycopg

    conn = psycopg.connect(**postgres_connect_kwargs(cfg))
    try:
        report = run_all_checks(
            conn,
            watchlist_symbols=symbols_override,
            min_symbols=int(args.min_symbols),
            lookback_days=int(args.lookback_days),
            max_age_hours=float(args.max_age_hours),
        )
    finally:
        conn.close()

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(f"Market Data Quality: {report['summary']}")
        for check in report.get("checks") or []:
            flag = "PASS" if check.get("ok") else "FAIL"
            print(f"  [{flag}] {check.get('check')}: {check.get('detail')}")

    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
