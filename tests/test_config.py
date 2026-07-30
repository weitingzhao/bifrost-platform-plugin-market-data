"""Config loader tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from bifrost_market_data.config import load_config, postgres_connect_kwargs


def test_load_config_reads_example_yaml() -> None:
    root = Path(__file__).resolve().parents[1]
    example = root / "config" / "market-data.yaml.example"
    cfg = load_config(example)
    assert isinstance(cfg, dict)
    assert "polygon" in cfg
    assert "postgres" in cfg
    assert cfg["postgres"]["dbname"] == "bifrost_dev"


def test_postgres_connect_kwargs_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)
    kw = postgres_connect_kwargs({"postgres": {}})
    assert kw["host"] == "localhost"
    assert kw["port"] == 5432
    assert kw["dbname"] == "bifrost_dev"
    assert kw["user"] == "data_writer"
