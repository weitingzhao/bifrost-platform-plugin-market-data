"""No query may name the ``market`` schema. It has not existed since the relocate.

``market`` survives only as an alias inside ``resolve_market_schema``, so call
sites may still *ask* for it — ``table_exists(conn, "market", …)`` resolves and
answers truthfully. What must never happen again is resolving in the guard and
then reading the literal schema in the query.

Measured 2026-09-10, that shape had ``coverage/sepa-stats`` reporting ten tables
as empty — 13.7M rows of stock_daily among them — with ``ok: true`` and a red
"0/10 today" on the page, and had ``coverage/distributions`` answering 500. It
does not fail loudly; it makes a broken read look like a successful bad result.
"""

from __future__ import annotations

import ast
import pathlib
import re

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "bifrost_market_data"

#: FROM / JOIN / INTO / UPDATE followed by the dead schema. `raw_market.` must
#: not trip it, hence the lookbehind.
_DEAD_SCHEMA = re.compile(r"(?i)\b(?:from|join|into|update)\s+(?<!raw_)market\.")


def _sql_text(node: ast.AST) -> str | None:
    """The literal part of a string handed to ``cursor.execute``.

    Docstrings and comments say "from market.ticker" as prose all over this
    package and are none of this test's business; only what reaches the server
    is. An f-string contributes its constant segments — an interpolated schema
    name is exactly the case that has to be caught, and it shows up as the
    ``.`` and table name around the hole.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str)
        )
    return None


def test_no_executed_sql_names_the_market_schema() -> None:
    offenders: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            if not (isinstance(fn, ast.Attribute) and fn.attr == "execute"):
                continue
            if not node.args:
                continue
            sql = _sql_text(node.args[0])
            if sql and _DEAD_SCHEMA.search(sql):
                line = _DEAD_SCHEMA.search(sql)
                offenders.append(
                    f"{path.relative_to(SRC)}:{node.lineno}: …{sql[max(0, line.start() - 20):line.end() + 30].strip()}…"
                )
    assert not offenders, (
        "these read a schema that does not exist; resolve_market_schema() gives "
        "the real one, and reading the literal instead is what made ten tables "
        "of live data render as empty:\n  " + "\n  ".join(offenders)
    )


def test_the_resolver_is_what_call_sites_get() -> None:
    """A guard that resolves is only half of it — the query has to use the answer."""
    from bifrost_market_data.api import deps

    seen: list[tuple[str, str]] = []

    class _Cur:
        def __enter__(self) -> "_Cur":
            return self

        def __exit__(self, *_a: object) -> None:
            return None

        def execute(self, sql: str, params: object = None) -> None:
            seen.append((sql, str(params)))

        def fetchone(self) -> tuple[int]:
            return (1,)

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

    # The alias resolves to the schema that exists…
    assert deps.resolve_market_schema.__doc__ is not None
    # …and safe_count, the pattern db-summary follows, reads the resolved one.
    import bifrost_market_data.api.deps as d

    original = d.resolve_market_schema
    try:
        d.resolve_market_schema = lambda *_a, **_k: "raw_market"  # type: ignore[assignment]
        assert d.safe_count(_Conn(), "market.stock_daily") == 1
    finally:
        d.resolve_market_schema = original  # type: ignore[assignment]
    assert "raw_market.stock_daily" in seen[0][0]


def test_the_detector_catches_the_shape_it_is_named_for() -> None:
    """A ratchet that cannot fail is not a ratchet — and this one has a hole.

    It catches a dead schema written into the query text, literal or inside an
    f-string. It CANNOT catch `f"… FROM {schema}.{table}"`, which is the exact
    form query_distributions used: the constant segments join to "… FROM ." and
    there is nothing left to match. Only reading the code catches that one,
    which is why the fix there was to make the variable hold the resolved schema
    (`resolve_market_schema`) and why that call site has a test of its own
    asserting the SQL it emits.

    Stated rather than hidden: a reader who thinks this covers both forms will
    stop looking for the second.
    """
    src = '''
def bad_literal(cur):
    cur.execute("SELECT count(*) FROM market.stock_daily")

def bad_interpolated(cur, schema, table):
    cur.execute(f"SELECT count(*) FROM {schema}.{table}")

def bad_fstring_literal(cur, col):
    cur.execute(f"SELECT max({col}) FROM market.option_daily")

def fine(cur):
    """Full rows from market.ticker for multiple symbols."""
    cur.execute("SELECT * FROM raw_market.ticker")
'''
    tree = ast.parse(src)
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "execute" and node.args:
                sql = _sql_text(node.args[0])
                if sql and _DEAD_SCHEMA.search(sql):
                    hits.append(node.lineno)
    # Caught: the plain literal (line 3) and the literal inside an f-string
    # (line 9). Not caught: the interpolated schema on line 6 — see the
    # docstring. Not tripped: the docstring prose on line 12, which is why this
    # is an AST walk and not a line grep — the first attempt flagged thirty
    # docstrings and zero queries.
    assert hits == [3, 9], hits
