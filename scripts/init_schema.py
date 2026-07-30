#!/usr/bin/env python3
"""Initialize market.* and data_ops.* schemas on the target PostgreSQL database."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running without install: python scripts/init_schema.py
_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from bifrost_market_data.config import load_config, postgres_connect_kwargs  # noqa: E402
from bifrost_market_data.schema.ddl import (  # noqa: E402
    DATA_OPS_TABLES,
    MARKET_TABLES,
    MARKET_VIEWS,
    apply_ddl,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply market-data DDL to PostgreSQL")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to market-data.yaml (default: MARKET_DATA_CONFIG or config/*.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print connection target and expected objects without applying DDL",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    kw = postgres_connect_kwargs(cfg)
    target = f"{kw['user']}@{kw['host']}:{kw['port']}/{kw['dbname']}"
    print(f"Target: {target}")
    print(f"market tables: {', '.join(MARKET_TABLES)}")
    print(f"data_ops tables: {', '.join(DATA_OPS_TABLES)}")
    print(f"market views: {', '.join(MARKET_VIEWS)}")

    if args.dry_run:
        print("Dry run — no DDL applied.")
        return 0

    try:
        import psycopg
    except ImportError as e:
        print(f"psycopg not installed: {e}", file=sys.stderr)
        return 2

    try:
        with psycopg.connect(**kw) as conn:
            apply_ddl(conn)
    except Exception as e:
        print(f"DDL failed: {e}", file=sys.stderr)
        return 1

    print("DDL applied successfully (schemas market + data_ops).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
