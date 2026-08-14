"""FastAPI application factory for Bifrost Market Data API."""

from __future__ import annotations

from fastapi import FastAPI

from bifrost_market_data import __version__
from bifrost_market_data.api.analytics import router as analytics_router
from bifrost_market_data.api.corp_actions import router as corp_actions_router
from bifrost_market_data.api.coverage import router as coverage_router
from bifrost_market_data.api.filings import router as filings_router
from bifrost_market_data.api.fundamentals import router as fundamentals_router
from bifrost_market_data.api.fundamentals_db import router as fundamentals_db_router
from bifrost_market_data.api.health import router as health_router
from bifrost_market_data.api.ingest import router as ingest_router
from bifrost_market_data.api.market_ops import router as market_ops_router
from bifrost_market_data.api.options import router as options_router
from bifrost_market_data.api.reference import router as reference_router
from bifrost_market_data.api.reference_db import router as reference_db_router
from bifrost_market_data.api.status_ext import router as status_ext_router
from bifrost_market_data.api.stocks import router as stocks_router
from bifrost_market_data.api.stocks_db import router as stocks_db_router
from bifrost_market_data.api.technical import router as technical_router
from bifrost_market_data.api.trades_quotes import router as trades_quotes_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Bifrost Market Data API",
        version=__version__,
        description=(
            "Bifrost Market Data Plugin API (port 8790). "
            "Polygon pass-through, DB coverage, analytics, and ingest enqueue "
            "under /market/*. Deep Celery/SSE paths remain on Trade API until P7."
        ),
    )
    app.include_router(health_router)
    # Mount under /market (P5 contract). Order: static prefixes before /stocks/{symbol}.
    market_prefix = "/market"
    app.include_router(analytics_router, prefix=market_prefix)
    app.include_router(ingest_router, prefix=market_prefix)
    app.include_router(options_router, prefix=market_prefix)
    app.include_router(fundamentals_db_router, prefix=market_prefix)
    app.include_router(fundamentals_router, prefix=market_prefix)
    app.include_router(filings_router, prefix=market_prefix)
    app.include_router(stocks_db_router, prefix=market_prefix)
    app.include_router(stocks_router, prefix=market_prefix)
    app.include_router(market_ops_router, prefix=market_prefix)
    app.include_router(reference_router, prefix=market_prefix)
    app.include_router(reference_db_router, prefix=market_prefix)
    app.include_router(coverage_router, prefix=market_prefix)
    app.include_router(corp_actions_router, prefix=market_prefix)
    app.include_router(status_ext_router, prefix=market_prefix)
    app.include_router(technical_router, prefix=market_prefix)
    app.include_router(trades_quotes_router, prefix=market_prefix)
    return app
