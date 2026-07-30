"""CLI entry point for market-data workers."""

from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    """Entry for ``market-data-worker`` console script. Stub for P0."""
    parser = argparse.ArgumentParser(description="Bifrost market-data worker")
    parser.add_argument(
        "--pool",
        choices=("stocks", "options"),
        default="stocks",
        help="Worker pool slice (stocks | options)",
    )
    args = parser.parse_args(argv)
    print(f"market-data worker stub (pool={args.pool})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
