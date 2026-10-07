"""TD-102: the plugin no longer computes a live max-pain curve.

Research serves the filtered curve. Frontend mentions of the old routes are
comments, not calls.
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"


def test_compute_max_pain_curve_is_gone() -> None:
    assert not (SRC / "bifrost_market_data/analytics/max_pain_math.py").exists()
    hits = [
        str(path.relative_to(SRC))
        for path in SRC.rglob("*.py")
        if "def compute_max_pain_curve" in path.read_text()
    ]
    assert hits == []


def test_live_compute_routes_are_gone() -> None:
    client = TestClient(create_app())
    paths = client.app.openapi()["paths"]
    assert "/market/analytics/max-pain/compute" not in paths
    assert "/market/analytics/max-pain/compute/history" not in paths
    assert "/market/analytics/max-pain" in paths
