"""GET /readiness/bar-aggregate is gone; the query behind it stays for the summary.

Nothing called the route: the Console's ``fetchReadinessBarAggregate`` and
trade-api's ``fetch_readiness_bar_aggregate`` had no callers, and the API log
showed no request in the 24 hours before it was removed (2026-09-26). The
readiness summary still reads ``query_bar_aggregate`` in process.
"""

from __future__ import annotations

import inspect

from bifrost_market_data.api import readiness_data
from bifrost_market_data.api.app import create_app


def test_route_is_not_served() -> None:
    paths = create_app().openapi()["paths"]
    assert "/market/readiness/bar-aggregate" not in paths
    # Its neighbours are still there.
    assert "/market/readiness/latest-bar-per-symbol" in paths


def test_query_has_no_totals_only_variant() -> None:
    assert "summary" not in inspect.signature(readiness_data.query_bar_aggregate).parameters
