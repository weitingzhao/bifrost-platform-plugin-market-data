"""Contract-level sums of option rows must drop adjusted roots (TD-102).

An adjusted OCC root ends in a digit. Research's filter is
``substr(ticker, 3, length(ticker) - 17) !~ '[0-9]$'``. A query that sums
open interest or volume without it mixes the adjusted contract into the curve.
Coverage counts how complete the store is, so it keeps every ticker and is
named here instead.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

API = Path(__file__).resolve().parents[1] / "src" / "bifrost_market_data" / "api"
_TABLE = re.compile(r"raw_market\.option_(open_interest|snapshot|daily)", re.IGNORECASE)
_AGG = re.compile(r"\bSUM\s*\(|\bGROUP\s+BY\b", re.IGNORECASE)
_PREDICATE = "!~ '[0-9]$'"
# Quality and completeness counts, not a put/call or pain curve.
_ALLOW = {"coverage.py"}


def test_option_aggregates_exclude_adjusted_roots() -> None:
    missing: list[str] = []
    for path in sorted(API.glob("*.py")):
        if path.name in _ALLOW:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            sql = node.value
            if _TABLE.search(sql) and _AGG.search(sql) and _PREDICATE not in sql:
                missing.append(f"{path.name}:{node.lineno}")
    assert missing == []
