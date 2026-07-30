"""Thin wrapper around the package entry point."""

from __future__ import annotations

import sys

from bifrost_market_data.worker.runner import main

if __name__ == "__main__":
    sys.exit(main())
