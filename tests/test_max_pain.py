"""Tests for Max Pain pure math (Plugin live-compute helpers).

``compute_max_pain_for_date`` upsert path moved to bifrost-research.
"""

from __future__ import annotations

from datetime import date

import pytest

from bifrost_market_data.analytics.max_pain_math import (
    compute_max_pain_curve,
    normalize_expiry_for_oi,
    strike_map_for_expiry,
)


def test_strike_map_and_known_max_pain() -> None:
    """Classic fixture: heavy call OI above spot → max pain pulled upward.

    Strikes 90/100/110. Calls concentrated at 90, puts at 110 → pain minimized near 100.
    """
    expiry = date(2025, 6, 20)
    rows = [
        {"expiry": expiry, "strike": 90.0, "option_right": "C", "open_interest": 10},
        {"expiry": expiry, "strike": 100.0, "option_right": "C", "open_interest": 5},
        {"expiry": expiry, "strike": 110.0, "option_right": "C", "open_interest": 1},
        {"expiry": expiry, "strike": 90.0, "option_right": "P", "open_interest": 1},
        {"expiry": expiry, "strike": 100.0, "option_right": "P", "open_interest": 5},
        {"expiry": expiry, "strike": 110.0, "option_right": "P", "open_interest": 10},
    ]
    skmap = strike_map_for_expiry(rows, expiry)
    assert skmap[90.0] == (10, 1)
    assert skmap[100.0] == (5, 5)
    assert skmap[110.0] == (1, 10)

    max_pain, min_pain, points, total_oi = compute_max_pain_curve(skmap)
    assert total_oi == 32
    assert max_pain == 100.0
    assert min_pain > 0
    best_point = next(p for p in points if p["strike"] == max_pain)
    assert abs(best_point["pain"] - min_pain) / max(min_pain, 1.0) < 0.001


def test_empty_oi_returns_zeros() -> None:
    strike, pain, points, total_oi = compute_max_pain_curve({})
    assert strike == 0.0
    assert pain == 0.0
    assert points == []
    assert total_oi == 0


def test_multi_expiry_independent() -> None:
    """Two expiries produce independent max-pain strikes."""
    e1 = date(2025, 6, 20)
    e2 = date(2025, 7, 18)
    rows_e1 = [
        {"expiry": e1, "strike": 100.0, "option_right": "C", "open_interest": 50},
        {"expiry": e1, "strike": 100.0, "option_right": "P", "open_interest": 50},
    ]
    rows_e2 = [
        {"expiry": e2, "strike": 200.0, "option_right": "C", "open_interest": 30},
        {"expiry": e2, "strike": 200.0, "option_right": "P", "open_interest": 30},
    ]
    mp1, _, _, _ = compute_max_pain_curve(strike_map_for_expiry(rows_e1, e1))
    mp2, _, _, _ = compute_max_pain_curve(strike_map_for_expiry(rows_e2, e2))
    assert mp1 == 100.0
    assert mp2 == 200.0


def test_normalize_expiry_formats() -> None:
    assert normalize_expiry_for_oi(date(2025, 6, 20)) == "20250620"
    assert normalize_expiry_for_oi("2025-06-20") == "20250620"
    assert normalize_expiry_for_oi("20250620") == "20250620"


@pytest.mark.skip(reason="moved to bifrost-research (compute_max_pain_for_date)")
def test_compute_max_pain_for_date_upserts() -> None:
    pass
