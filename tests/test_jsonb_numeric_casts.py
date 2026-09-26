"""Vendor numbers in jsonb go through ``numeric`` before they become integers.

``(data->>'short_volume')::bigint`` raises on "6485654.248821", and the vendor
has sent short volumes with fractional shares on every row since the table began
(2024-09-09) — so /stocks/fundamentals/db/short-volume answered 500 for every
symbol. A text-to-integer cast is a bet on the vendor's formatting; ``numeric``
accepts both, and ``round()`` gives the contract its whole shares.
"""

from __future__ import annotations

import pathlib
import re
from typing import Any, Self

import bifrost_market_data
from bifrost_market_data.api import fundamentals_db

SRC = pathlib.Path(bifrost_market_data.__file__).resolve().parent

DIRECT_INT_CAST = re.compile(r"->>\s*'[^']+'\s*\)\s*::\s*(bigint|integer|int|smallint)\b", re.IGNORECASE)


def test_no_jsonb_text_is_cast_straight_to_an_integer() -> None:
    offenders = [
        f"{path.relative_to(SRC)}:{i}: {line.strip()}"
        for path in sorted(SRC.rglob("*.py"))
        for i, line in enumerate(path.read_text().splitlines(), 1)
        if DIRECT_INT_CAST.search(line)
    ]
    assert not offenders, "cast through numeric first:\n" + "\n".join(offenders)


class _Cursor:
    def __init__(self, seen: list[str]) -> None:
        self._seen = seen

    def execute(self, query: str, params: Any = None) -> None:
        self._seen.append(" ".join(query.split()))

    def fetchall(self) -> list[Any]:
        return []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_a: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.seen: list[str] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.seen)


def test_short_volume_ratio_is_a_ratio_not_the_vendor_percent(monkeypatch: Any) -> None:
    monkeypatch.setattr(fundamentals_db, "table_exists", lambda *_a, **_k: True)
    conn = _Conn()
    fundamentals_db.query_short_volume(conn, symbols=["AAPL"], trade_days=5)
    sql = conn.seen[-1]
    # Computed from the unrounded volumes; the vendor's field is a percent
    # (58.33 for AAPL on 2026-09-25, against a computed 0.5833).
    assert "NULLIF(NULLIF(data->>'short_volume', '')::numeric, 0) / NULLIF(NULLIF(data->>'total_volume', '')::numeric, 0)" in sql
    assert "NULLIF(data->>'short_volume_ratio', '')::numeric / 100" in sql
    assert "round(NULLIF(data->>'short_volume', '')::numeric)::bigint AS short_volume" in sql
    assert "round(NULLIF(data->>'total_volume', '')::numeric)::bigint AS total_volume" in sql
