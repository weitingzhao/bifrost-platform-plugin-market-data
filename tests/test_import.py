"""Smoke tests: package and subpackages import cleanly."""

from __future__ import annotations


def test_package_version() -> None:
    import bifrost_market_data

    assert bifrost_market_data.__version__ == "0.1.0"


def test_subpackages_import() -> None:
    import bifrost_market_data.config  # noqa: F401
    import bifrost_market_data.ingest  # noqa: F401
    import bifrost_market_data.polygon  # noqa: F401
    import bifrost_market_data.polygon.client  # noqa: F401
    import bifrost_market_data.polygon.endpoints  # noqa: F401
    import bifrost_market_data.polygon.rate_limit  # noqa: F401
    import bifrost_market_data.scheduler  # noqa: F401
    import bifrost_market_data.scheduler.daily  # noqa: F401
    import bifrost_market_data.schema  # noqa: F401
    import bifrost_market_data.schema.ddl  # noqa: F401
    import bifrost_market_data.worker  # noqa: F401
    import bifrost_market_data.worker.claim  # noqa: F401
    import bifrost_market_data.worker.health  # noqa: F401
    import bifrost_market_data.worker.loop  # noqa: F401
    import bifrost_market_data.worker.runner  # noqa: F401
