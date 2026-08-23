"""Tests for ops_jobs.ingest_freshness UPSERT helpers."""

from __future__ import annotations

from typing import Any

import pytest

from bifrost_market_data.freshness import (
    dimension_for_kind,
    rows_written_from_result,
    update_freshness,
)


class _Cur:
    def __init__(self, parent: _Conn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))

    def __enter__(self) -> _Cur:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.commits = 0

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.commits += 1


def test_dimension_for_kind_alias() -> None:
    assert dimension_for_kind("stock_daily_grouped") == "stock_daily"
    assert dimension_for_kind("option_snapshot") == "option_snapshot"


def test_rows_written_from_result() -> None:
    assert rows_written_from_result(None) == 0
    assert rows_written_from_result({}) == 0
    assert rows_written_from_result({"rows_written": 12}) == 12
    assert rows_written_from_result({"rows_written": "3"}) == 3
    assert rows_written_from_result({"rows_written": "x"}) == 0


def test_update_freshness_upsert() -> None:
    conn = _Conn()
    update_freshness(conn, "stock_daily", 42)
    assert conn.commits == 1
    assert len(conn.statements) == 1
    sql, params = conn.statements[0]
    assert "ingest_freshness" in sql.lower()
    assert "on conflict" in sql.lower()
    assert params == ("stock_daily", 42, "ok")


def test_update_freshness_requires_dimension() -> None:
    with pytest.raises(ValueError):
        update_freshness(_Conn(), "", 1)
