"""A worker's SQL budget, and the index its claim needs — 0.18.0.

Two failures on 2026-09-08 had the same root: the ``bifrost`` role's 2s
statement_timeout, which is writer safety for an ad-hoc session and far too
tight for anything that grows with the universe.

* 21 jobs (stock_daily, option_backfill_plan, option_contract) exhausted their
  attempts on "canceling statement due to statement timeout" and landed in
  ``failed`` — the handlers never got 2 seconds of runway, though the worker's
  own job budget is 900.
* The claim filters on ``kind``, which the ``(status, priority, created_at)``
  index does not carry. With 3.3M pending option rows, the stocks pool walked
  the whole pending set to prove it had nothing waiting: measured past 30s, so
  those workers could not claim at all while the backfill ran.
"""

from __future__ import annotations

from typing import Any

from bifrost_market_data.config import postgres_connect_kwargs
from bifrost_market_data.schema import ddl
from bifrost_market_data.worker import loop as worker_loop


def _cfg() -> dict[str, Any]:
    return {
        "postgres": {"host": "db", "port": 5432, "dbname": "gs", "user": "bifrost", "password": "x"}
    }


def test_a_connection_carries_no_budget_unless_asked() -> None:
    assert "options" not in postgres_connect_kwargs(_cfg())


def test_the_budget_travels_as_a_libpq_option_because_set_takes_no_bind_params() -> None:
    kw = postgres_connect_kwargs(_cfg(), statement_timeout="60s")
    assert kw["options"] == "-c statement_timeout=60s"
    assert kw["host"] == "db" and kw["user"] == "bifrost"


def test_an_existing_option_string_is_kept() -> None:
    cfg = _cfg()
    cfg["postgres"]["options"] = "-c application_name=worker"
    kw = postgres_connect_kwargs(cfg, statement_timeout="30s")
    assert kw["options"] == "-c application_name=worker -c statement_timeout=30s"


def test_workers_read_their_budget_from_config_and_default_to_sixty_seconds() -> None:
    assert worker_loop._statement_timeout({}) == worker_loop.DEFAULT_STATEMENT_TIMEOUT == "60s"
    assert worker_loop._statement_timeout({"worker": {"statement_timeout": "120s"}}) == "120s"
    # An empty value is not a budget of zero; it falls back.
    assert worker_loop._statement_timeout({"worker": {"statement_timeout": ""}}) == "60s"


def test_the_claim_has_an_index_that_carries_kind() -> None:
    statements: list[str] = []

    class _Cur:
        def __enter__(self) -> "_Cur":
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

        def execute(self, sql: str, params: Any = None) -> None:
            statements.append(" ".join(str(sql).split()))

        def fetchone(self) -> Any:
            return None

        def fetchall(self) -> list[Any]:
            return []

    ddl._create_data_ops_tables(_Cur())  # type: ignore[arg-type]
    claim_index = [s for s in statements if "job_ingest_pending_kind_priority" in s]
    assert len(claim_index) == 1
    sql = claim_index[0]
    # kind leads, so a pool with nothing waiting proves it with a seek.
    assert "(kind, priority DESC, created_at)" in sql
    # Partial: the claim only ever reads pending, and finished rows never grow it.
    assert "WHERE status = 'pending'" in sql
