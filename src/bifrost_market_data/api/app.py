"""FastAPI application factory for Bifrost Market Data API."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from bifrost_market_data import __version__
from bifrost_market_data.api.analytics import router as analytics_router
from bifrost_market_data.api.chain_by_expiry import router as chain_by_expiry_router
from bifrost_market_data.api.corp_actions import router as corp_actions_router
from bifrost_market_data.api.coverage import router as coverage_router
from bifrost_market_data.api.filings import router as filings_router
from bifrost_market_data.api.fundamentals import router as fundamentals_router
from bifrost_market_data.api.fundamentals_db import router as fundamentals_db_router
from bifrost_market_data.api.fundamentals_sepa import router as fundamentals_sepa_router
from bifrost_market_data.api.deps import run_startup_schema_guard
from bifrost_market_data.api.health import router as health_router
from bifrost_market_data.api.ingest import router as ingest_router
from bifrost_market_data.api.ingest_ticker import router as ingest_ticker_router
from bifrost_market_data.api.ingest_options import router as ingest_options_router
from bifrost_market_data.api.ingest_bars import router as ingest_bars_router
from bifrost_market_data.api.market_ops import router as market_ops_router
from bifrost_market_data.api.option_daily import router as option_daily_router
from bifrost_market_data.api.option_minute import router as option_minute_router
from bifrost_market_data.api.options import router as options_router
from bifrost_market_data.api.pcr import router as pcr_router
from bifrost_market_data.api.readiness_data import router as readiness_data_router
from bifrost_market_data.api.readiness_summary import router as readiness_summary_router
from bifrost_market_data.api.reference import router as reference_router
from bifrost_market_data.api.reference_db import router as reference_db_router
from bifrost_market_data.api.source_void import router as source_void_router
from bifrost_market_data.api.status_ext import router as status_ext_router
from bifrost_market_data.api.stocks import router as stocks_router
from bifrost_market_data.api.stocks_db import router as stocks_db_router
from bifrost_market_data.api.technical import router as technical_router
from bifrost_market_data.api.trades_quotes import router as trades_quotes_router


@asynccontextmanager
async def _lifespan(app: FastAPI):
    run_startup_schema_guard()
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="Bifrost Market Data API",
        version=__version__,
        description=(
            "Bifrost Market Data Plugin API (port 8790). "
            "Polygon pass-through, DB coverage, analytics, and ingest enqueue "
            "under /market/*. Deep Celery/SSE paths remain on Trade API until P7."
        ),
        lifespan=_lifespan,
    )
    app.include_router(health_router)
    # Mount under /market (P5 contract). Order: static prefixes before /stocks/{symbol}.
    market_prefix = "/market"
    app.include_router(analytics_router, prefix=market_prefix)
    app.include_router(chain_by_expiry_router, prefix=market_prefix)
    app.include_router(pcr_router, prefix=market_prefix)
    app.include_router(ingest_router, prefix=market_prefix)
    app.include_router(ingest_bars_router, prefix=market_prefix)
    app.include_router(ingest_options_router, prefix=market_prefix)
    app.include_router(option_daily_router, prefix=market_prefix)
    app.include_router(option_minute_router, prefix=market_prefix)
    app.include_router(options_router, prefix=market_prefix)
    app.include_router(fundamentals_db_router, prefix=market_prefix)
    app.include_router(fundamentals_sepa_router, prefix=market_prefix)
    app.include_router(fundamentals_router, prefix=market_prefix)
    app.include_router(filings_router, prefix=market_prefix)
    app.include_router(stocks_db_router, prefix=market_prefix)
    app.include_router(stocks_router, prefix=market_prefix)
    app.include_router(market_ops_router, prefix=market_prefix)
    app.include_router(readiness_data_router, prefix=market_prefix)
    app.include_router(source_void_router, prefix=market_prefix)
    app.include_router(readiness_summary_router, prefix=market_prefix)
    app.include_router(reference_router, prefix=market_prefix)
    app.include_router(reference_db_router, prefix=market_prefix)
    app.include_router(ingest_ticker_router, prefix=market_prefix)
    app.include_router(coverage_router, prefix=market_prefix)
    app.include_router(corp_actions_router, prefix=market_prefix)
    app.include_router(status_ext_router, prefix=market_prefix)
    app.include_router(technical_router, prefix=market_prefix)
    app.include_router(trades_quotes_router, prefix=market_prefix)
    return app
