"""Ownership is part of the schema, not an operational afterthought.

An object the plugin does not own is one it cannot drop, re-create or
partition — and on 2026-09-09 it owned none of the 162 relations in
raw_market, which dates a write failure: ensure_month_partitions builds three
months ahead, so it starts failing in 2026-10 and inserts for 2027-01 have
nowhere to land.
"""

from __future__ import annotations

import pytest

from bifrost_market_data.schema.ddl import (
    PLUGIN_ROLE,
    PLUGIN_SCHEMAS,
    ownership_statements,
)


def test_it_covers_the_plugin_schemas_and_only_those() -> None:
    """features.* belongs to bifrost-research and dw_stock.* to its dbt models."""
    assert PLUGIN_SCHEMAS == ("raw_market", "ops_jobs")
    sql = "\n".join(ownership_statements())
    for schema in PLUGIN_SCHEMAS:
        assert f"'{schema}'" in sql
    for foreign in ("features", "dw_stock", "public", "bifrost_prod"):
        assert f"'{foreign}'" not in sql


def test_every_relkind_gets_the_verb_postgres_wants() -> None:
    """ALTER TABLE does not work on a sequence, and a partition is a table."""
    sql = ownership_statements()[0]
    for verb in ("ALTER TABLE", "ALTER SEQUENCE", "ALTER VIEW", "ALTER MATERIALIZED VIEW"):
        assert verb in sql
    assert "'r', 'p', 'v', 'm', 'S'" in sql


def test_it_skips_what_is_already_owned() -> None:
    """A second run must report zero changes, not churn every object."""
    sql = ownership_statements()[0]
    assert "IF obj.current_owner = target_role THEN" in sql
    assert "CONTINUE;" in sql


def test_functions_and_schemas_move_too() -> None:
    """CREATE OR REPLACE FUNCTION fails on a function the caller does not own."""
    sql = ownership_statements()[0]
    assert "ALTER FUNCTION" in sql
    assert "ALTER SCHEMA" in sql


def test_the_consumers_keep_their_grants() -> None:
    stmts = ownership_statements()
    joined = "\n".join(stmts)
    assert "market_reader" in joined and "data_writer" in joined
    assert f"FOR ROLE {PLUGIN_ROLE}" in joined


def test_the_role_name_is_an_identifier_not_a_sentence() -> None:
    """It is interpolated into SQL, so it may not be anything but a bare name."""
    with pytest.raises(ValueError):
        ownership_statements("bad; DROP TABLE x")
    with pytest.raises(ValueError):
        ownership_statements("has space")
    assert ownership_statements("bifrost")
