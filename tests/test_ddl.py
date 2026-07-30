"""DDL unit tests: statement coverage + optional live PostgreSQL idempotency."""

from __future__ import annotations

import os
from typing import Any

import pytest

from bifrost_market_data.schema.ddl import (
    DATA_OPS_TABLES,
    MARKET_TABLES,
    MARKET_VIEWS,
    apply_ddl,
)


class _FakeCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, query: str, params: Any = None) -> None:
        _ = params
        self.statements.append(query)

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.cur = _FakeCursor()
        self.committed = False

    def cursor(self) -> _FakeCursor:
        return self.cur

    def commit(self) -> None:
        self.committed = True


def test_apply_ddl_emits_schemas_tables_and_helpers() -> None:
    conn = _FakeConn()
    apply_ddl(conn)
    assert conn.committed
    blob = "\n".join(conn.cur.statements)
    assert "CREATE SCHEMA IF NOT EXISTS market" in blob
    assert "CREATE SCHEMA IF NOT EXISTS data_ops" in blob
    for name in MARKET_TABLES:
        assert f"market.{name}" in blob, f"missing market.{name}"
    for name in DATA_OPS_TABLES:
        assert f"data_ops.{name}" in blob, f"missing data_ops.{name}"
    for name in MARKET_VIEWS:
        assert name in blob, f"missing view {name}"
    assert "ensure_year_partitions" in blob
    assert "ensure_month_partitions" in blob
    assert "SELECT data_ops.ensure_year_partitions('market', 'stock_daily'" in blob
    assert "job_ingest_dedup" in blob
    assert "SELECT FOR UPDATE" not in blob  # claim logic is P3, not DDL


def test_apply_ddl_is_idempotent_on_mock() -> None:
    conn = _FakeConn()
    apply_ddl(conn)
    first = list(conn.cur.statements)
    apply_ddl(conn)
    second = conn.cur.statements[len(first) :]
    assert len(second) == len(first)
    assert second == first


def test_expected_object_counts() -> None:
    assert len(MARKET_TABLES) == 11
    assert len(DATA_OPS_TABLES) == 3
    assert len(MARKET_VIEWS) == 2


@pytest.mark.skipif(
    not (os.environ.get("MARKET_DATA_DDL_LIVE") or "").strip(),
    reason="Set MARKET_DATA_DDL_LIVE=1 (and POSTGRES_*) to run live DDL idempotency",
)
def test_apply_ddl_live_idempotent() -> None:
    import psycopg

    from bifrost_market_data.config import postgres_connect_kwargs

    kw = postgres_connect_kwargs()
    with psycopg.connect(**kw) as conn:
        apply_ddl(conn)
        apply_ddl(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'market' AND table_type = 'BASE TABLE'
                ORDER BY 1
                """
            )
            tables = [r[0] for r in cur.fetchall()]
            for name in MARKET_TABLES:
                assert name in tables
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'data_ops' AND table_type = 'BASE TABLE'
                ORDER BY 1
                """
            )
            ops = [r[0] for r in cur.fetchall()]
            for name in DATA_OPS_TABLES:
                assert name in ops


def test_create_roles_sql_exists() -> None:
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "scripts" / "create_roles.sql"
    text = path.read_text(encoding="utf-8")
    assert "data_writer" in text
    assert "market_reader" in text
    assert "GRANT" in text
