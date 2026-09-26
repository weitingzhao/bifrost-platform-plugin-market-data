"""option_open_interest's ``underlying`` indexes must exist whatever state the table is in.

They were declared from the start and never existed on the cluster: the
migration returned early once the table was partitioned, and the one pass that
did create it ran the index DDL while the renamed legacy table still held the
same index names, so ``IF NOT EXISTS`` skipped them and the DROP removed them.
Every per-symbol read of the table was a sequential scan as a result.
"""

from __future__ import annotations

from typing import Any, Self

import pytest

from bifrost_market_data.schema.ddl import apply_wave8_migrations
from bifrost_market_data.schema.wave8_migrations import (
    OPTION_OI_INDEXES,
    migrate_option_open_interest_partitioned,
)


class _Cursor:
    def __init__(self, relkind: str | None) -> None:
        self.relkind = relkind
        self.statements: list[str] = []
        self._row: Any = None

    def execute(self, query: str, params: Any = None) -> None:
        self.statements.append(" ".join(query.split()))
        if "c.relkind" in query and params == ("raw_market", "option_open_interest"):
            self._row = (self.relkind,) if self.relkind else None
        else:
            self._row = None

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list[Any]:
        return []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_a: object) -> None:
        return None


def _index_statements(cur: _Cursor) -> list[str]:
    return [s for s in cur.statements if s.startswith("CREATE INDEX IF NOT EXISTS option_oi_")]


def _expected() -> list[str]:
    return [
        f"CREATE INDEX IF NOT EXISTS {name} ON raw_market.option_open_interest {cols}"
        for name, cols in OPTION_OI_INDEXES
    ]


@pytest.mark.parametrize("relkind", ["p", "r", None])
def test_indexes_are_ensured_in_every_table_state(relkind: str | None) -> None:
    cur = _Cursor(relkind)
    migrate_option_open_interest_partitioned(cur)
    assert _index_statements(cur) == _expected()


def test_already_partitioned_table_gets_indexes_and_nothing_else() -> None:
    # The state every deployed environment is in: no rename, no copy, no drop.
    cur = _Cursor("p")
    migrate_option_open_interest_partitioned(cur)
    assert not any("RENAME" in s or "DROP TABLE" in s or "INSERT INTO" in s for s in cur.statements)
    assert not any(s.startswith("CREATE TABLE") for s in cur.statements)


def test_legacy_conversion_creates_indexes_only_after_the_legacy_table_is_gone() -> None:
    cur = _Cursor("r")
    migrate_option_open_interest_partitioned(cur)
    drop_at = cur.statements.index("DROP TABLE raw_market.option_open_interest_legacy")
    for stmt in _index_statements(cur):
        assert cur.statements.index(stmt) > drop_at


def test_readers_filter_on_the_leading_columns() -> None:
    columns = {cols for _, cols in OPTION_OI_INDEXES}
    assert "(underlying, trade_date DESC)" in columns
    assert "(underlying, expiry, trade_date DESC)" in columns


def test_the_cluster_migration_path_reaches_them() -> None:
    # The schema Job runs ``init_schema.py --wave8-only``; apply_ddl alone is
    # not a path the cluster ever takes.
    class _Conn:
        def __init__(self) -> None:
            self.cur = _Cursor("p")

        def cursor(self) -> _Cursor:
            return self.cur

        def commit(self) -> None:
            return None

    conn = _Conn()
    apply_wave8_migrations(conn)
    assert _index_statements(conn.cur) == _expected()
