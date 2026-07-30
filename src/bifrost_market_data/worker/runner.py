"""CLI entry point for market-data workers."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from typing import Any


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    """Entry for ``market-data-worker`` console script."""
    parser = argparse.ArgumentParser(description="Bifrost market-data worker")
    parser.add_argument(
        "--pool",
        choices=("stocks", "options"),
        default=None,
        help="Worker pool slice (stocks | options)",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to market-data.yaml (default: MARKET_DATA_CONFIG / example)",
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=8080,
        help="HTTP /health listen port (default 8080)",
    )
    parser.add_argument(
        "--poll-interval-sec",
        type=float,
        default=None,
        help="Override worker.poll_interval_sec",
    )
    args = parser.parse_args(argv)
    _configure_logging()

    from bifrost_market_data.config import load_config
    from bifrost_market_data.worker.loop import run_loop

    cfg = load_config(args.config)
    worker_cfg = dict(cfg.get("worker") or {})
    pool = (args.pool or worker_cfg.get("pool") or "stocks").strip().lower()

    shutdown = asyncio.Event()

    def _request_shutdown(*_args: Any) -> None:
        logging.getLogger(__name__).info("shutdown signal received")
        shutdown.set()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except NotImplementedError:
                # Windows / restricted environments
                signal.signal(sig, lambda *_: _request_shutdown())

        loop.run_until_complete(
            run_loop(
                pool=pool,
                cfg=cfg,
                shutdown_event=shutdown,
                health_port=int(args.health_port),
                poll_interval_sec=args.poll_interval_sec,
            )
        )
    except KeyboardInterrupt:
        shutdown.set()
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
