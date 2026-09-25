"""The checklist's per-symbol probes have to be able to use their indexes.

``query_daily_checklist`` asks three point questions per symbol, and the
Console panel that calls it holds 40 watchlist symbols and refetches every 60
seconds — 120 statements a minute against the shared database. Each of the
three tables carries an index leading with exactly the column being compared
(``stock_daily (symbol, bar_date)``, ``option_open_interest (underlying,
trade_date)``, ``corporate_action (symbol, ex_date)``), and wrapping the column
in ``UPPER(TRIM())`` made every one of them a sequential scan. Research
measured a batch of these beside the snapshot-coverage join on 2026-09-25,
while an unrelated row-by-row repair job ran three to four times slow.

Normalising the *input* is equivalent here because the columns already hold
normalised values: ``underlying <> UPPER(TRIM(underlying))`` returned 0 on
2026-09-08. This is not a general rule about the wrapper — on the whole-table
aggregates removing it measured *slower* (401s against 151s on 2026-09-09),
because either way every row is read.
"""

from __future__ import annotations

from typing import Any

from bifrost_market_data.api.corp_actions import query_daily_checklist

#: The three probes, and the column each one's index leads with.
INDEXED_PROBES = (
    ("raw_market.stock_daily", "symbol"),
    ("raw_market.option_open_interest", "underlying"),
    ("raw_market.corporate_action", "symbol"),
)


class _Cursor:
    def __init__(self, seen: list[tuple[str, Any]]) -> None:
        self._seen = seen
        self._row: Any = None

    def execute(self, query: str, params: Any = None) -> None:
        self._seen.append((query, params))
        q = query.lower()
        # resolve_market_schema's existence probe
        self._row = (1,) if "information_schema" in q or "to_regclass" in q else (7,)

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list[Any]:
        return []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_a: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.seen: list[tuple[str, Any]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.seen)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def _probe_statements(conn: _Conn) -> list[str]:
    return [q for q, _p in conn.seen for t, _c in INDEXED_PROBES if t in q]


def test_every_probe_compares_the_bare_indexed_column() -> None:
    conn = _Conn()
    query_daily_checklist(conn, symbols=["aapl"], trade_date="2026-09-24")
    stmts = _probe_statements(conn)
    assert len(stmts) == len(INDEXED_PROBES), "all three probes ran"
    joined = " ".join(stmts).upper()
    assert "UPPER(TRIM(" not in joined, "a wrapped column cannot be probed by its index"
    for table, col in INDEXED_PROBES:
        target = next(q for q in stmts if table in q)
        assert f"WHERE {col} = %s" in target, f"{table} must compare {col} directly"


def test_the_caller_input_is_normalised_instead() -> None:
    """The wrapper is dropped from the column, not from the comparison."""
    conn = _Conn()
    result = query_daily_checklist(conn, symbols=["  aapl "], trade_date="2026-09-24")
    assert "AAPL" in result["symbols"]
    params = [p for q, p in conn.seen for t, _c in INDEXED_PROBES if t in q]
    assert all(p[0] == "AAPL" for p in params)
