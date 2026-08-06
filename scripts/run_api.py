"""Run Bifrost Market Data API (uvicorn on port 8790)."""

from __future__ import annotations

import os

import uvicorn

from bifrost_market_data.api.app import create_app


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8790"))
    app = create_app()
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
