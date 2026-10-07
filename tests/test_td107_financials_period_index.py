"""TD-107: period_date_symbol is an unconditional apply_ddl step; symbol_period_date is gone."""

from __future__ import annotations

import inspect
from pathlib import Path

from bifrost_market_data.schema import ddl as ddl_mod
from bifrost_market_data.schema.wave8_migrations import (
    FINANCIALS_ENTITY_TABLES,
    create_financials_entity_tables,
    ensure_financials_period_date_symbol,
)

_SQL = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "ddl"
    / "2026-10-07-td107-period-date-symbol.sql"
)


def test_symbol_period_date_is_not_declared() -> None:
    source = inspect.getsource(create_financials_entity_tables)
    assert "CREATE INDEX IF NOT EXISTS {table}_symbol_period_date" not in source
    ensure = inspect.getsource(ensure_financials_period_date_symbol)
    assert "CREATE INDEX IF NOT EXISTS {table}_symbol_period_date" not in ensure
    assert "CREATE INDEX IF NOT EXISTS {table}_period_date_symbol" in ensure


def test_apply_ddl_calls_the_index_step_unconditionally() -> None:
    source = inspect.getsource(ddl_mod.apply_ddl)
    assert "ensure_financials_period_date_symbol(cur)" in source
    # The schema Job is --wave8-only and runs as the plugin role. These tables
    # are owned by postgres, so the build must not be on that path.
    assert "ensure_financials_period_date_symbol" not in inspect.getsource(
        ddl_mod.apply_wave8_migrations
    )


def test_owner_script_builds_each_table_concurrently() -> None:
    text = _SQL.read_text()
    assert "CONCURRENTLY" in text
    for table in FINANCIALS_ENTITY_TABLES:
        assert f"{table}_period_date_symbol" in text
        assert f"ON raw_market.{table} (period_date, symbol)" in text
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" in text
    assert "_symbol_period_date" not in text
