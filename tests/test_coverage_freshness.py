"""``coverage/freshness`` is db-summary's ``freshness`` without the counts (TD-259).

The platform's liveness probe reads it every 30 s. If the two ever answer
different rows, the probe and the Coverage page disagree about the same table.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from bifrost_market_data.api import coverage as mod
from bifrost_market_data.api.app import create_app

_ROWS: list[tuple[Any, ...]] = [
    (
        "option_snapshot",
        datetime(2026, 10, 7, 21, 0, 5, 123456, tzinfo=timezone.utc),
        1234,
        "ok",
        datetime(2026, 10, 7, 21, 0, 6, tzinfo=timezone.utc),
    ),
    ("ratios", None, None, None, None),
    (
        "slot:ticker-details",
        datetime(2026, 10, 6, 3, 15, tzinfo=timezone.utc),
        0,
        "skipped",
        datetime(2026, 10, 6, 3, 15, tzinfo=timezone.utc),
    ),
    (
        "stock_daily",
        datetime(2026, 10, 7, 22, 0, 16, 455334, tzinfo=timezone.utc),
        20836,
        "ok",
        datetime(2026, 10, 7, 22, 0, 17, tzinfo=timezone.utc),
    ),
]

_FIELDS = ("dimension", "last_run_at", "rows_written", "status", "updated_at")


class _Cur:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.conn.sql.append(" ".join(sql.split()))
        if self.conn.boom:
            raise RuntimeError("boom")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self.conn.rows)


class _Conn:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.sql: list[str] = []
        self.boom = False

    def cursor(self) -> _Cur:
        return _Cur(self)

    def close(self) -> None:
        return None


@pytest.fixture
def counted(monkeypatch) -> list[str]:
    seen: list[str] = []
    monkeypatch.setattr(mod, "safe_count", lambda _c, t: seen.append(t) or 7)
    monkeypatch.setattr(mod, "estimated_rows", lambda _c, t: seen.append(t) or 9)
    monkeypatch.setattr(mod, "table_exists", lambda *_a, **_k: True)
    return seen


def _get(monkeypatch, path: str, conn: _Conn) -> dict[str, Any]:
    monkeypatch.setattr(mod, "require_db", lambda: conn)
    resp = TestClient(create_app()).get(path)
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_freshness_answers_the_same_rows_as_db_summary(monkeypatch, counted) -> None:
    summary = _get(monkeypatch, "/market/coverage/db-summary", _Conn(_ROWS))
    fresh = _get(monkeypatch, "/market/coverage/freshness", _Conn(_ROWS))

    assert len(fresh["freshness"]) == len(_ROWS) == len(summary["freshness"])
    for a, b in zip(fresh["freshness"], summary["freshness"], strict=True):
        assert set(a) == set(b) == set(_FIELDS)
        for field in _FIELDS:
            assert a[field] == b[field], field
    assert [r["dimension"] for r in fresh["freshness"]] == sorted(r[0] for r in _ROWS)
    # Nulls stay null; the platform fills its own COALESCE defaults.
    ratios = next(r for r in fresh["freshness"] if r["dimension"] == "ratios")
    assert ratios == {f: (None if f != "dimension" else "ratios") for f in _FIELDS}


def test_freshness_does_not_count_the_database(monkeypatch, counted) -> None:
    conn = _Conn(_ROWS)
    out = _get(monkeypatch, "/market/coverage/freshness", conn)

    assert counted == []
    assert "counts" not in out and "estimated" not in out
    assert out["ok"] is True and out["source"] == "db"
    assert len(conn.sql) == 1 and "FROM ops_jobs.ingest_freshness" in conn.sql[0]

    _get(monkeypatch, "/market/coverage/db-summary", _Conn(_ROWS))
    assert counted, "db-summary still carries the counts its pages read"


def test_a_failed_read_is_an_empty_list_on_both(monkeypatch, counted) -> None:
    for path in ("/market/coverage/freshness", "/market/coverage/db-summary"):
        conn = _Conn(_ROWS)
        conn.boom = True
        assert _get(monkeypatch, path, conn)["freshness"] == []


def test_there_is_one_freshness_query_in_coverage() -> None:
    """Two copies of the SELECT would drift; both endpoints go through one."""
    src = Path(mod.__file__).read_text(encoding="utf-8")
    assert src.count("FROM ops_jobs.ingest_freshness") == 1
