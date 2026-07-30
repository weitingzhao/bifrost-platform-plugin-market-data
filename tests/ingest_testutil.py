"""Shared fake PG connection for ingest handler tests."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from bifrost_market_data.worker.claim import JobRow


class FakeCursor:
    def __init__(self, parent: FakeConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))

    def executemany(self, query: str, params_seq: Any) -> None:
        self.parent.statements.append((query, list(params_seq)))

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class FakeConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.committed = 0
        self.rolled_back = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1

    def close(self) -> None:
        self.closed = True

    def upsert_sqls(self) -> list[str]:
        return [s[0] for s in self.statements if "INSERT INTO" in s[0]]


def make_job(kind: str, payload: dict[str, Any] | None = None, job_id: int = 1) -> JobRow:
    return JobRow(
        id=job_id,
        kind=kind,
        payload=payload or {},
        payload_hash=None,
        priority=0,
        status="running",
        result=None,
        attempts=1,
        max_attempts=3,
        created_at=None,
        updated_at=None,
        started_at=None,
        finished_at=None,
    )


def mock_client(**methods: Any) -> MagicMock:
    client = MagicMock()
    for name, value in methods.items():
        if isinstance(value, Exception):
            getattr(client, name).side_effect = value
        else:
            setattr(client, name, AsyncMock(return_value=value))
    return client
