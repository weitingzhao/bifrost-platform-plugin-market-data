"""Entry point: Polygon Options WebSocket ingestor (Plugin-native).

Usage:
  python scripts/run_polygon_ws.py
  python scripts/run_polygon_ws.py --config config/market-data.yaml
  python scripts/run_polygon_ws.py --log-level DEBUG
  POLYGON_API_KEY=xxx python scripts/run_polygon_ws.py
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from bifrost_market_data.config import load_config  # noqa: E402
from bifrost_market_data.ws.polygon_ws_ingestor import (  # noqa: E402
    PolygonWsIngestor,
    _get_polygon_ws_cfg,
)


def _setup_logging(level: int) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(name)s [%(levelname)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def _make_redis_client(cfg: dict):
    """Create Redis client from config redis_massive block or env vars."""
    import redis

    rm = cfg.get("redis_massive") or {}
    host = os.environ.get("REDIS_MASSIVE_HOST") or rm.get("host") or "redis-massive"
    port = int(os.environ.get("REDIS_MASSIVE_PORT") or rm.get("port") or 6379)
    db = int(rm.get("db") or 0)
    username = os.environ.get("REDIS_MASSIVE_USERNAME") or rm.get("username") or None
    password = os.environ.get("REDIS_MASSIVE_PASSWORD") or rm.get("password") or None

    return redis.Redis(
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
        decode_responses=True,
        socket_connect_timeout=10,
        socket_timeout=10,
    )


def _get_watchlist_pg_params(cfg: dict) -> dict:
    """Build PG connection params for watchlist query.

    Uses watchlist_pg block if present, falls back to the main postgres block.
    This allows pointing the WS ingestor at a Trade env's PG for watchlist
    while the main postgres targets bifrost_golden_source.
    """
    wl = cfg.get("watchlist_pg") or {}
    pg = cfg.get("postgres") or {}
    source = wl if wl else pg

    return {
        "host": os.environ.get("WATCHLIST_PG_HOST") or source.get("host") or "localhost",
        "port": int(os.environ.get("WATCHLIST_PG_PORT") or source.get("port") or 5432),
        "dbname": os.environ.get("WATCHLIST_PG_DB") or source.get("dbname") or "bifrost_dev",
        "user": os.environ.get("WATCHLIST_PG_USER") or source.get("user") or "data_writer",
        "password": os.environ.get("WATCHLIST_PG_PASSWORD") or source.get("password") or "",
    }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Polygon Options WS Ingestor")
    parser.add_argument("--config", default=None, help="Path to YAML config")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    _setup_logging(getattr(logging, args.log_level))
    log = logging.getLogger(__name__)

    cfg = load_config(args.config)
    log.info("Config loaded")

    ws_cfg = _get_polygon_ws_cfg(cfg)
    if not ws_cfg["api_key"]:
        log.error("No Polygon API key. Set POLYGON_API_KEY env or polygon.api_key in config.")
        sys.exit(1)

    rds = _make_redis_client(cfg)
    pg_params = _get_watchlist_pg_params(cfg)

    app = PolygonWsIngestor(cfg, redis_client=rds, pg_params=pg_params)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
