"""DDL unit tests: statement coverage + optional live PostgreSQL idempotency."""

from __future__ import annotations

import os
from typing import Any

import pytest

from bifrost_market_data.schema.ddl import (
    DATA_OPS_TABLES,
    MARKET_ANALYTICS_TABLES,
    MARKET_TABLES,
    MARKET_VIEWS,
    apply_ddl,
)

FORBIDDEN_LEGACY_SCHEMAS = (
    "features_daily",
    "features_option",
    "features_signals",
    "features_forecasts",
    "features_backtests",
    "market_analytics",
)


class _FakeCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, query: str, params: Any = None) -> None:
        _ = params
        self.statements.append(query)

    def fetchone(self) -> None:
        return None

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
    assert "CREATE SCHEMA IF NOT EXISTS raw_market" in blob
    assert "CREATE SCHEMA IF NOT EXISTS ops_jobs" in blob
    for legacy in FORBIDDEN_LEGACY_SCHEMAS:
        assert legacy not in blob, f"legacy schema {legacy} must not appear in DDL"
    for name in MARKET_TABLES:
        assert f"raw_market.{name}" in blob, f"missing raw_market.{name}"
    for name in DATA_OPS_TABLES:
        assert f"ops_jobs.{name}" in blob, f"missing ops_jobs.{name}"
    for name in MARKET_VIEWS:
        assert name in blob, f"missing view {name}"
    assert "ensure_year_partitions" in blob
    assert "ensure_month_partitions" in blob
    assert "ensure_day_partitions" in blob
    assert "drop_day_partitions_older_than" in blob
    assert "drop_month_partitions_older_than" in blob
    assert "SELECT ops_jobs.ensure_year_partitions('raw_market', 'stock_daily'" in blob
    assert "DROP VIEW IF EXISTS data_ops.ingest_freshness" in blob
    assert "DROP SCHEMA IF EXISTS data_ops CASCADE" in blob
    assert "SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_open_interest'" in blob
    # option_trades is retired: option trades are not in Options Starter, the
    # slot went in 0.10.3, and the table holds zero rows. Provisioning day
    # partitions for it every night bought nothing.
    assert "ensure_day_partitions('raw_market', 'option_trades'" not in blob
    for name in ("stock_minute", "option_daily", "option_minute", "option_snapshot"):
        assert f"ensure_month_partitions('raw_market', '{name}'" in blob
    assert "job_ingest_dedup" in blob
    assert "DROP TABLE IF EXISTS ops_jobs.us_trading_calendar" in blob
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
    assert len(MARKET_TABLES) == 23
    assert "stock_snapshot" in MARKET_TABLES
    assert "income_statement" in MARKET_TABLES
    assert "balance_sheet" in MARKET_TABLES
    assert "cash_flow" in MARKET_TABLES
    assert "stock_movers" in MARKET_TABLES
    assert "option_trades" in MARKET_TABLES
    assert "us_market_holiday" in MARKET_TABLES
    assert "treasury_yield" in MARKET_TABLES
    assert "ticker_related" in MARKET_TABLES
    assert "ticker_type" in MARKET_TABLES
    assert "stock_financials" not in MARKET_TABLES
    assert len(MARKET_ANALYTICS_TABLES) == 0
    assert len(DATA_OPS_TABLES) == 7
    assert "queue_sample" in DATA_OPS_TABLES  # the queue's only history
    assert "coverage_sample" in DATA_OPS_TABLES  # the matrix's only memory
    assert "data_source_void" in DATA_OPS_TABLES
    assert "symbol_source_void" in DATA_OPS_TABLES
    assert "watchlist_cache" in DATA_OPS_TABLES
    assert "us_trading_calendar" not in DATA_OPS_TABLES
    assert len(MARKET_VIEWS) == 4
    assert "stock_financials" in MARKET_VIEWS


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
                WHERE table_schema = 'raw_market' AND table_type = 'BASE TABLE'
                ORDER BY 1
                """
            )
            tables = [r[0] for r in cur.fetchall()]
            for name in MARKET_TABLES:
                assert name in tables
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'ops_jobs' AND table_type = 'BASE TABLE'
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
    assert "features_daily" not in text
    assert "GRANT USAGE ON SCHEMA features TO market_reader" in text
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA features TO market_reader" in text


def test_filing_date_migration_is_additive_and_indexed() -> None:
    """filing_date must arrive as a nullable column, never as a table rewrite.

    The six financials entity tables hold ~590k rows. ADD COLUMN with no default
    is metadata-only in PostgreSQL 11+, and the migration must stay that way.
    """
    from bifrost_market_data.schema.wave8_migrations import (
        FINANCIALS_ENTITY_TABLES,
        add_financials_filing_date,
    )

    cur = _FakeCursor()
    add_financials_filing_date(cur)
    sql = "\n".join(cur.statements)

    for table in FINANCIALS_ENTITY_TABLES:
        assert f"ALTER TABLE IF EXISTS raw_market.{table}" in sql
        assert f"{table}_filing_date" in sql
    assert "ADD COLUMN IF NOT EXISTS filing_date date" in sql
    # A default would force a rewrite of every row.
    assert "ADD COLUMN IF NOT EXISTS filing_date date DEFAULT" not in sql
    # Partial index: TTM rows never carry one.
    assert "WHERE filing_date IS NOT NULL" in sql
    # The compat view has to expose it or nothing downstream can read it.
    assert "CREATE OR REPLACE VIEW raw_market.stock_financials" in sql
    assert sql.count("filing_date, fetched_at") == len(FINANCIALS_ENTITY_TABLES)


def test_filing_date_migration_not_on_bifrost_role_path() -> None:
    """apply_wave8_migrations runs as the bifrost role; the ALTER needs ownership.

    raw_market tables are owned by postgres. Wiring this migration into the
    wave8 path would make that job fail on every run — it belongs on apply_ddl,
    which the job manifest documents as the superuser path.
    """
    import inspect

    from bifrost_market_data.schema import ddl as ddl_mod

    # The call, not the mention — apply_wave8_migrations' docstring names it to
    # explain the exclusion.
    assert "add_financials_filing_date(cur)" not in inspect.getsource(
        ddl_mod.apply_wave8_migrations
    )
    assert "add_financials_filing_date(cur)" in inspect.getsource(ddl_mod.apply_ddl)


def test_partition_provisioning_has_one_list_and_two_callers() -> None:
    """It lived only in apply_ddl, which nothing re-runs, so the forward window
    never advanced: four tables sat 83 days from having nowhere to insert."""
    import inspect

    from bifrost_market_data.schema.ddl import ensure_partitions
    from bifrost_market_data.scheduler import daily

    conn = _FakeConn()
    ensure_partitions(conn)
    blob = "\n".join(conn.cur.statements)
    assert "ensure_year_partitions('raw_market', 'stock_daily'" in blob
    assert blob.count("ensure_month_partitions") == 5
    assert conn.committed
    # The scheduler calls the shared one rather than keeping its own list.
    assert "ensure_partitions(conn)" in inspect.getsource(daily.enqueue_slot)


#: ops_jobs tables that only ``apply_ddl`` creates. The cluster never runs
#: apply_ddl — the schema Job is ``init_schema.py --wave8-only`` — so anything
#: listed here exists in DEV only because it predates that Job. A *new* table
#: added to this list would never be created in the cluster at all, which is
#: exactly what happened to coverage_sample on 2026-09-10: declared, printed in
#: the Job's own summary line, and absent, with the API answering "relation does
#: not exist" on every write.
FRESH_INSTALL_ONLY = frozenset(
    {
        "job_ingest",
        "queue_sample",
        "ingest_freshness",
        "data_source_void",
        "symbol_source_void",
        "watchlist_cache",
    }
)


def test_the_deploy_path_creates_every_ops_jobs_table_it_is_responsible_for() -> None:
    """The migration Job is the only schema path that runs in the cluster.

    ops_jobs is the plugin's own schema, so its tables *can* be created by the
    un-privileged role this Job uses — which means there is no excuse for a new
    one to reach the cluster through apply_ddl alone.
    """
    from bifrost_market_data.schema.ddl import apply_wave8_migrations

    conn = _FakeConn()
    apply_wave8_migrations(conn)
    blob = "\n".join(conn.cur.statements)
    for name in DATA_OPS_TABLES:
        if name in FRESH_INSTALL_ONLY:
            continue
        assert f"ops_jobs.{name}" in blob, (
            f"ops_jobs.{name} is declared but the deploy path never creates it — "
            "add it to apply_wave8_migrations, or to FRESH_INSTALL_ONLY if it "
            "genuinely predates that Job"
        )
