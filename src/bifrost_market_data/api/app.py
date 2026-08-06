"""FastAPI application factory for Bifrost Market Data API."""

from __future__ import annotations

from fastapi import FastAPI

from bifrost_market_data import __version__
from bifrost_market_data.api.analytics import router as analytics_router
from bifrost_market_data.api.health import router as health_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Bifrost Market Data API",
        version=__version__,
    )
    app.include_router(health_router)
    # Mount under /market so paths match P5 contract: /market/analytics/*
    app.include_router(analytics_router, prefix="/market")
    return app
