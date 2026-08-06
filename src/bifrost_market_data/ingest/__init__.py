"""Ingest handlers: Polygon response → market.* upsert (P4)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from bifrost_market_data.ingest._upsert import make_handler
from bifrost_market_data.ingest.calendar import handle_calendar
from bifrost_market_data.ingest.corporate_action import handle_dividends, handle_splits
from bifrost_market_data.ingest.financials import handle_financials
from bifrost_market_data.ingest.option_contract import handle_option_contract
from bifrost_market_data.ingest.option_daily import handle_option_daily
from bifrost_market_data.ingest.option_expiration import handle_option_expiration
from bifrost_market_data.ingest.option_minute import handle_option_minute
from bifrost_market_data.ingest.option_oi import handle_option_open_interest
from bifrost_market_data.ingest.option_snapshot import handle_option_snapshot
from bifrost_market_data.ingest.stock_daily import handle_stock_daily
from bifrost_market_data.ingest.stock_daily_grouped import handle_stock_daily_grouped
from bifrost_market_data.ingest.stock_minute import handle_stock_minute
from bifrost_market_data.ingest.stock_movers import handle_stock_movers
from bifrost_market_data.ingest.stock_snapshot import handle_stock_snapshot
from bifrost_market_data.ingest.ticker_sync import handle_ticker_sync
from bifrost_market_data.worker.claim import JobRow

Handler = Callable[[JobRow], Any]

_RAW_HANDLERS: dict[str, Any] = {
    "stock_daily": handle_stock_daily,
    "stock_daily_grouped": handle_stock_daily_grouped,
    "stock_minute": handle_stock_minute,
    "stock_snapshot": handle_stock_snapshot,
    "stock_movers": handle_stock_movers,
    "option_daily": handle_option_daily,
    "option_minute": handle_option_minute,
    "option_snapshot": handle_option_snapshot,
    "option_contract": handle_option_contract,
    "option_expiration": handle_option_expiration,
    "option_open_interest": handle_option_open_interest,
    "ticker_sync": handle_ticker_sync,
    "financials": handle_financials,
    "splits": handle_splits,
    "dividends": handle_dividends,
    "calendar": handle_calendar,
}


def build_handler_registry(
    client: Any,
    *,
    connect: Callable[[], Any],
) -> dict[str, Handler]:
    """Build kind → Handler mapping with shared PolygonClient + PG connect factory."""
    return {
        kind: make_handler(fn, client=client, connect=connect)
        for kind, fn in _RAW_HANDLERS.items()
    }


def raw_handler_kinds() -> tuple[str, ...]:
    return tuple(_RAW_HANDLERS.keys())


__all__ = [
    "build_handler_registry",
    "raw_handler_kinds",
    "handle_stock_daily",
    "handle_stock_daily_grouped",
    "handle_stock_minute",
    "handle_stock_snapshot",
    "handle_stock_movers",
    "handle_option_daily",
    "handle_option_minute",
    "handle_option_snapshot",
    "handle_option_contract",
    "handle_option_expiration",
    "handle_option_open_interest",
    "handle_ticker_sync",
    "handle_financials",
    "handle_splits",
    "handle_dividends",
    "handle_calendar",
]
