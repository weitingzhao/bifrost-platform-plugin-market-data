"""PostgreSQL schema DDL for raw_market.* and ops_jobs.*."""

from bifrost_market_data.schema.ddl import (
    DATA_OPS_TABLES,
    MARKET_ANALYTICS_TABLES,
    MARKET_TABLES,
    MARKET_VIEWS,
    apply_ddl,
)

__all__ = [
    "DATA_OPS_TABLES",
    "MARKET_ANALYTICS_TABLES",
    "MARKET_TABLES",
    "MARKET_VIEWS",
    "apply_ddl",
]
