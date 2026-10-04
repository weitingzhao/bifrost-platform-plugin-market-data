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
    DATA_OPS_TABLES,
    PLUGIN_OPS_TABLES,
    PLUGIN_OWNED_SCHEMA,
    PLUGIN_ROLE,
    PLUGIN_SCHEMAS,
    ownership_statements,
)


def test_it_covers_the_plugin_schemas_and_only_those() -> None:
    """features.* belongs to bifrost-research and dw_stock.* to its dbt models."""
    assert PLUGIN_SCHEMAS == ("raw_market", "ops_jobs")
    sql = "\n".join(ownership_statements())
    assert f"'{PLUGIN_OWNED_SCHEMA}'" in sql
    for foreign in ("features", "dw_stock", "public", "bifrost_prod", "research", "raw_broker"):
        assert f"'{foreign}'" not in sql


def test_the_plugin_role_is_data_writer() -> None:
    """D6 (2026-10-04): market-data signs in as data_writer, not bifrost."""
    assert PLUGIN_ROLE == "data_writer"
    assert "target_role CONSTANT text := 'data_writer'" in ownership_statements()[0]


def test_ops_jobs_is_shared_so_only_the_market_tables_move() -> None:
    """D6 F5: the Flex Query plugin owns its own ops_jobs tables; the schema is postgres's."""
    assert PLUGIN_OPS_TABLES == DATA_OPS_TABLES
    sql = ownership_statements()[0]
    for name in PLUGIN_OPS_TABLES:
        assert f"'{name}'" in sql
    for flex in ("job_flex_ingest", "flex_ingest_freshness", "flex_worker_heartbeat", "flex_settings"):
        assert flex not in sql
    assert "n.nspname = 'ops_jobs' AND c.relname IN (" in sql
    # Only raw_market's schema changes hands.
    assert "ALTER SCHEMA %I OWNER TO %I', 'raw_market'" in sql


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


def test_the_shared_helpers_do_not_move() -> None:
    """The partition helpers are SECURITY INVOKER and shared with Research's Dagster:
    EXECUTE is all the plugin needs, and an owner could rewrite code Research runs."""
    sql = ownership_statements()[0]
    assert "ALTER FUNCTION" not in sql
    assert "pg_proc" not in sql


def test_sequences_come_after_their_tables() -> None:
    """A sequence linked to a column follows its table; ALTER SEQUENCE on it would fail."""
    sql = ownership_statements()[0]
    assert "ORDER BY c.relkind = 'S'" in sql


def test_the_consumers_keep_their_grants() -> None:
    stmts = ownership_statements()
    joined = "\n".join(stmts)
    assert f"FOR ROLE {PLUGIN_ROLE} IN SCHEMA raw_market" in joined
    for reader in ("analytics_writer", "analytics_reader", "market_reader", "brokerage_reader"):
        assert reader in stmts[1]
    assert "FOR ROLE data_writer IN SCHEMA ops_jobs GRANT SELECT ON TABLES TO analytics_reader, market_reader" in stmts[2]


def test_the_role_name_is_an_identifier_not_a_sentence() -> None:
    """It is interpolated into SQL, so it may not be anything but a bare name."""
    with pytest.raises(ValueError):
        ownership_statements("bad; DROP TABLE x")
    with pytest.raises(ValueError):
        ownership_statements("has space")
    assert ownership_statements("bifrost")
