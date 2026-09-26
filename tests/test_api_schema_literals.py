"""Every relation in these reads is named from the schema it lives in (R9 C3-P1).

``market`` is a logical alias the resolver understands; Postgres does not. Naming
it in SQL made ``/market/options/chain/eod`` answer 500 on every request, and
naming it in an existence check made two endpoints silently take their fallback
path forever. Both mistakes are the same one, so both are pinned here.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any

from bifrost_market_data.api import chain_by_expiry as cbe
from bifrost_market_data.api import options as opt

#: ``market.<relation>`` but not ``raw_market.<relation>``.
BAD_SCHEMA = re.compile(r"(?<!raw_)\bmarket\.[a-z_]", re.IGNORECASE)


class _Cur:
    def __init__(self, owner: _Conn) -> None:
        self.owner = owner

    def execute(self, sql: str, params: Any = None) -> None:
        self.owner.sql.append(sql)

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self.owner.rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        # information_schema lookups: the relation exists under raw_market only.
        sql = self.owner.sql[-1] if self.owner.sql else ""
        if "information_schema" in sql:
            schema = (self.owner.last_params or ("",))[0]
            return (1,) if schema == "raw_market" else None
        return None

    def __enter__(self) -> _Cur:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


class _Conn:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.sql: list[str] = []
        self.rows = rows or []
        self.last_params: tuple[Any, ...] | None = None

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


class _ResolvingCur(_Cur):
    """Cursor that answers the resolver's information_schema probes."""

    def execute(self, sql: str, params: Any = None) -> None:
        self.owner.sql.append(sql)
        self.owner.last_params = tuple(params) if params else None


class _ResolvingConn(_Conn):
    def cursor(self) -> _ResolvingCur:
        return _ResolvingCur(self)


def _query_sql(conn: _Conn) -> list[str]:
    return [s for s in conn.sql if "information_schema" not in s]


EOD_SNAPSHOT = {
    "snap_day": date(2026, 9, 14),
    "iv": 0.37,
    "delta": 0.21,
    "underlying_price": 245.0,
    "snapshot_ts": datetime(2026, 9, 14, 20, 0, tzinfo=timezone.utc),
    "_option_ticker": "O:NVDA261120C00245000",
    "_underlying": "NVDA",
    "_expiry": date(2026, 11, 20),
    "_strike": 245.0,
    "_option_right": "C",
}


def test_chain_eod_names_the_schema_the_snapshot_relation_lives_in() -> None:
    conn = _ResolvingConn([EOD_SNAPSHOT])
    rows = opt._fetch_chain_eod(conn, ["NVDA|OPT|20261120|245.0|C"], datetime(2026, 9, 1, tzinfo=timezone.utc))
    sql = _query_sql(conn)
    assert sql, "the read must have run a query"
    for statement in sql:
        assert not BAD_SCHEMA.search(statement), statement
    assert any("raw_market.v_option_snapshot_with_stock" in s for s in sql)
    assert rows and rows[0]["contract_key"] == "NVDA|OPT|20261120|245.0|C"


#: What the database actually hands back. No connection in this plugin sets a
#: dict row factory, so a test that feeds mappings tests a shape production never
#: sees — which is how chain/eod stayed empty after the schema fix.
EOD_TUPLE_IB = (
    date(2026, 9, 14),
    0.37,
    0.21,
    245.0,
    datetime(2026, 9, 14, 20, 0, tzinfo=timezone.utc),
    "O:NVDA261120C00245000",
    "NVDA",
    date(2026, 11, 20),
    245.0,
    "C",
    "NVDA|OPT|20261120|245.0|C",
)

EOD_TUPLE_POLYGON = EOD_TUPLE_IB[:-1]


def test_chain_eod_reads_the_tuple_rows_the_database_returns() -> None:
    conn = _ResolvingConn([EOD_TUPLE_IB])
    rows = opt._fetch_chain_eod(
        conn, ["NVDA|OPT|20261120|245.0|C"], datetime(2026, 9, 1, tzinfo=timezone.utc)
    )
    assert rows, "a tuple row is a row"
    assert rows[0]["contract_key"] == "NVDA|OPT|20261120|245.0|C"
    assert rows[0]["iv"] == 0.37 and rows[0]["snap_day"] == "2026-09-14"
    assert rows[0]["delta"] == 0.21, "the caller asked for the greek, not only the vol"
    assert rows[0]["underlying_price"] == 245.0


def test_chain_eod_reads_a_polygon_keyed_tuple_row_too() -> None:
    conn = _ResolvingConn([EOD_TUPLE_POLYGON])
    rows = opt._fetch_chain_eod(
        conn, ["O:NVDA261120C00245000"], datetime(2026, 9, 1, tzinfo=timezone.utc)
    )
    assert rows and rows[0]["contract_key"] == "NVDA|OPT|20261120|245.0|C"


def test_chain_latest_takes_its_materialised_path_when_the_view_is_there() -> None:
    conn = _ResolvingConn([])
    opt._fetch_chain_latest(conn, ["O:NVDA261120C00245000"])
    sql = _query_sql(conn)
    # The view exists under raw_market, so the endpoint uses it instead of falling
    # back — the old check asked for it under `market` and never matched.
    assert any("raw_market.v_option_chain_latest" in s for s in sql)
    for statement in sql:
        assert not BAD_SCHEMA.search(statement), statement


def test_chain_latest_ib_keys_do_not_join_the_view() -> None:
    conn = _ResolvingConn([])
    opt._fetch_chain_latest(conn, ["NVDA|OPT|20261120|245.0|C"])
    sql = _query_sql(conn)
    # The Polygon branch filters the view on its DISTINCT ON column with a
    # constant, which is pushed down; an IB key only becomes a ticker after the
    # join, and a join cannot be pushed into the view (4.1 s against 2.2 ms for
    # 20 AAPL contracts on 2026-09-26).
    assert any("JOIN raw_market.option_snapshot s" in s and "DISTINCT ON (oc.option_ticker)" in s for s in sql)
    assert not any("v_option_chain_latest" in s for s in sql)
    for statement in sql:
        assert not BAD_SCHEMA.search(statement), statement


def test_chain_by_expiry_reads_each_contract_through_its_key() -> None:
    conn = _ResolvingConn([])
    result = cbe.query_chain_by_expiry(conn, symbol="NVDA")
    # Same latest snapshot per contract as the view, so the label stays; but the
    # view is a DISTINCT ON over all of option_snapshot that a join cannot push
    # into (8.9 s against 44 ms for AAPL on 2026-09-26), so it is not joined.
    assert result["basis"] == "option_snapshots_latest"
    sql = _query_sql(conn)
    assert any("LEFT JOIN LATERAL" in s and "raw_market.option_snapshot s" in s for s in sql)
    assert not any("v_option_chain_latest" in s for s in sql)
    for statement in sql:
        assert not BAD_SCHEMA.search(statement), statement
