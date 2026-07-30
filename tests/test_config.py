"""Config loader stubs (expanded in later phases)."""

from __future__ import annotations

from bifrost_market_data.config import load_config


def test_load_config_stub_returns_dict() -> None:
    cfg = load_config()
    assert isinstance(cfg, dict)
    assert cfg == {}
