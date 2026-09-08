"""YAML configuration loading for market-data workers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def default_config_path() -> Path | None:
    """Resolve config path from MARKET_DATA_CONFIG env or well-known locations."""
    env = (os.environ.get("MARKET_DATA_CONFIG") or "").strip()
    if env:
        p = Path(env)
        return p if p.is_file() else None
    here = Path(__file__).resolve().parents[2]
    for candidate in (
        here / "config" / "market-data.yaml",
        here / "config" / "market-data.yaml.example",
    ):
        if candidate.is_file():
            return candidate
    return None


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load market-data YAML config; overlay postgres fields from env when set."""
    cfg: dict[str, Any] = {}
    resolved: Path | None
    if path is not None:
        resolved = Path(path)
    else:
        resolved = default_config_path()
    if resolved is not None and resolved.is_file():
        with resolved.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if isinstance(raw, dict):
            cfg = raw

    pg = dict(cfg.get("postgres") or {})
    env_map = {
        "host": "POSTGRES_HOST",
        "port": "POSTGRES_PORT",
        "dbname": "POSTGRES_DB",
        "user": "POSTGRES_USER",
        "password": "POSTGRES_PASSWORD",
    }
    for key, env_name in env_map.items():
        val = os.environ.get(env_name)
        if val is not None and str(val).strip() != "":
            if key == "port":
                try:
                    pg[key] = int(val)
                except ValueError:
                    pg[key] = val
            else:
                pg[key] = val
    if pg:
        cfg["postgres"] = pg
    return cfg


def postgres_connect_kwargs(
    cfg: dict[str, Any] | None = None,
    *,
    statement_timeout: str | None = None,
) -> dict[str, Any]:
    """Build psycopg connect kwargs from config / env.

    The ``bifrost`` role carries ``statement_timeout=2s`` — writer safety for
    ad-hoc sessions, and far too tight for anything that reads a table which
    grows with the universe. Pass ``statement_timeout`` to raise the session
    limit; it goes through libpq ``options`` because Postgres rejects bind
    parameters on ``SET``.
    """
    data = cfg if cfg is not None else load_config()
    pg = dict(data.get("postgres") or {})
    kwargs: dict[str, Any] = {
        "host": pg.get("host") or os.environ.get("POSTGRES_HOST") or "localhost",
        "port": int(pg.get("port") or os.environ.get("POSTGRES_PORT") or 5432),
        "dbname": pg.get("dbname") or os.environ.get("POSTGRES_DB") or "bifrost_golden_source",
        "user": pg.get("user") or os.environ.get("POSTGRES_USER") or "data_writer",
        "password": pg.get("password") or os.environ.get("POSTGRES_PASSWORD") or "",
    }
    sto = str(statement_timeout or "").strip()
    if sto:
        existing = str(pg.get("options") or "").strip()
        flag = f"-c statement_timeout={sto}"
        kwargs["options"] = f"{existing} {flag}".strip() if existing else flag
    return kwargs
