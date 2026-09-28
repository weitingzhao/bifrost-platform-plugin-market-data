"""short_volume vacuums on its daily inserts, on the path the cluster runs.

2026-09-28: the insert-driven autovacuum had not run since 09-09. At the
default 0.2 of a 7M-row table it waits for about 1.4M inserts, two months of
sessions, and the doctor's breadth read made 442,146 heap fetches through an
index that could otherwise have answered alone.
"""

from __future__ import annotations

from typing import Any

from bifrost_market_data.schema.ddl import (
    SHORT_VOLUME_AUTOVACUUM_OPTION,
    apply_ddl,
    apply_wave8_migrations,
    tune_short_volume_autovacuum,
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

    def cursor(self) -> _FakeCursor:
        return self.cur

    def commit(self) -> None:
        return None


def _short_volume_alters(apply: Any) -> list[str]:
    conn = _FakeConn()
    apply(conn)
    return [s for s in conn.cur.statements if "ALTER TABLE raw_market.short_volume SET (" in s]


def test_both_paths_set_the_insert_scale_factor() -> None:
    """The cluster's Job is ``--wave8-only``; a statement only apply_ddl reaches runs nowhere."""
    for apply in (apply_ddl, apply_wave8_migrations):
        alters = _short_volume_alters(apply)
        assert len(alters) == 1, f"{apply.__name__} does not tune short_volume's autovacuum"
        assert "autovacuum_vacuum_insert_scale_factor = 0.01" in alters[0]


def test_only_the_insert_scale_factor_is_set() -> None:
    """The Owner approved this one option (2026-09-28), nothing beside it."""
    [stmt] = _short_volume_alters(apply_wave8_migrations)
    options = [line.strip() for line in stmt.splitlines() if "=" in line]
    assert options == ["autovacuum_vacuum_insert_scale_factor = 0.01"]


def test_a_deploy_that_finds_the_option_set_takes_no_lock() -> None:
    """SET waits for any vacuum of the table, and the role's lock_timeout is 5s.

    The 0.64.0 Job failed three times on the same shape of statement against
    raw_market.ticker (155e240). Only the deploy that changes the option may
    take the lock.
    """

    class _SetCursor(_FakeCursor):
        def execute(self, query: str, params: Any = None) -> None:
            super().execute(query, params)
            self.last_params = params

        def fetchone(self) -> tuple[int] | None:
            return (1,) if self.last_params == (SHORT_VOLUME_AUTOVACUUM_OPTION,) else None

    cur = _SetCursor()
    tune_short_volume_autovacuum(cur)
    assert len(cur.statements) == 1 and "reloptions" in cur.statements[0]
    assert not [s for s in cur.statements if "ALTER TABLE" in s]


def test_the_probe_matches_what_the_alter_stores() -> None:
    """pg_class.reloptions holds 'name=value' with no spaces, as the cluster shows
    for ops_jobs.job_ingest; a probe spelled like the ALTER would never match."""
    [stmt] = _short_volume_alters(apply_wave8_migrations)
    name, value = (part.strip() for part in stmt.split("(", 1)[1].rsplit(")", 1)[0].split("="))
    assert SHORT_VOLUME_AUTOVACUUM_OPTION == f"{name}={value}"


def test_the_table_exists_before_it_is_tuned() -> None:
    """On an empty database migrate_stock_financials_split creates short_volume."""
    for apply in (apply_ddl, apply_wave8_migrations):
        conn = _FakeConn()
        apply(conn)
        stmts = conn.cur.statements
        created = next(
            i for i, s in enumerate(stmts) if "CREATE TABLE IF NOT EXISTS raw_market.short_volume" in s
        )
        tuned = next(i for i, s in enumerate(stmts) if "ALTER TABLE raw_market.short_volume SET (" in s)
        assert created < tuned, apply.__name__
