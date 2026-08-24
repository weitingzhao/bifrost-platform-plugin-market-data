"""Plugin analytics — thin compatibility for live Max Pain API compute.

Daily upserts (max-pain / ATM IV / PCR / IV percentile) moved to
``bifrost_research.engines.volatility`` and ``bifrost_research.scheduler.volatility``.
Plugin API reads ``features.option_metric_*`` from Golden Source and offers live
max-pain compute via pure math in ``max_pain_math``.
"""

from __future__ import annotations

from bifrost_market_data.analytics.max_pain_math import (
    compute_max_pain_curve,
    normalize_expiry_for_oi,
    strike_map_for_expiry,
)

__all__ = [
    "compute_max_pain_curve",
    "normalize_expiry_for_oi",
    "strike_map_for_expiry",
]
