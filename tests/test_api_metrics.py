"""/metrics serves the doctor's conclusion, never a second policy."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from bifrost_market_data.api import doctor as doc
from bifrost_market_data.api import metrics as m
from bifrost_market_data.api.app import create_app


def _report(**kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "generated_at": "2026-09-27T23:00:00+00:00",
        "findings": [
            {"id": "stock_daily:2026-09-25", "slot": "universe-daily", "severity": "crit",
             "title": "Stock daily bars (whole market)"},
            {"id": "fundamentals_market:2026-09-25", "slot": "fundamentals-market",
             "severity": "warn", "title": 'Ratios + "short" volume'},
            {"id": "chain_spot:2026-09-25", "slot": "eod-pipeline", "severity": "boundary",
             "title": "Spot behind the chain"},
            {"id": "calendar", "slot": "calendar", "severity": "ok", "title": "calendar"},
        ],
        "prescriptions": [{"slot": "universe-daily"}],
    }
    base.update(kw)
    return base


def test_counts_every_severity_and_lists_only_the_news() -> None:
    text = m.render_metrics(_report(), version="9.9.9")
    assert 'bifrost_market_data_plugin_info{version="9.9.9"} 1' in text
    for sev, n in (("crit", 1), ("warn", 1), ("boundary", 1), ("ok", 1)):
        assert f'bifrost_market_data_doctor_findings{{severity="{sev}"}} {n}' in text
    listed = [ln for ln in text.splitlines() if ln.startswith("bifrost_market_data_doctor_finding{")]
    assert len(listed) == 2
    assert any('id="stock_daily:2026-09-25"' in ln and 'severity="crit"' in ln for ln in listed)
    # Label values are escaped, not trusted.
    assert any('title="Ratios + \\"short\\" volume"' in ln for ln in listed)
    assert "bifrost_market_data_doctor_prescriptions 1" in text
    assert "bifrost_market_data_doctor_report_timestamp_seconds 1790550000.0" in text


def test_before_the_first_report_every_count_is_zero_and_the_age_says_so() -> None:
    """A pod that has not finished its first doctor run must not read healthy by omission."""
    text = m.render_metrics(dict(doc._DOCTOR_EMPTY))
    assert "bifrost_market_data_doctor_report_timestamp_seconds 0.0" in text
    assert 'bifrost_market_data_doctor_findings{severity="crit"} 0' in text


def test_the_route_reads_the_cached_report(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def _read(key: str, compute: Any, *, empty: Any = None) -> dict[str, Any]:
        seen.append(key)
        return _report()

    monkeypatch.setattr(m.DOCTOR_CACHE, "read", _read)
    res = TestClient(create_app()).get("/metrics")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/plain")
    assert 'bifrost_market_data_doctor_findings{severity="crit"} 1' in res.text
    # The Console's default key: one computation serves both.
    assert seen == ["probes=False"]
