"""TD-169: every freshness dimension has a writer that still runs.

``ops_jobs.ingest_freshness.option_expiration`` sat at 2026-09-06 12:58 with
status ``ok`` for a month: the ``option_expiration`` kind stopped being enqueued
when the contract walk took over the expiry list, and the contract handler said
nothing about the dimension. A row nobody writes still reads "ok" in every
freshness listing, which teaches the reader to ignore staleness.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Self

import pytest
from ingest_testutil import FakeConn, make_job, mock_client
from test_slot_freshness import _RUNNABLE, _jobs_of

from bifrost_market_data import quality
from bifrost_market_data.api.ingest_dashboard import SLOT_EVIDENCE
from bifrost_market_data.contracts import CONTRACTS
from bifrost_market_data.freshness import (
    SHAPE_NAMED_SLOTS,
    dimension_for_kind,
    slot_freshness_key,
)
from bifrost_market_data.ingest.option_contract import handle_option_contract
from bifrost_market_data.scheduler.backfill import SUPPORTED_BACKFILL_KINDS
from bifrost_market_data.worker.health import HealthState
from bifrost_market_data.worker.loop import process_one_job

SRC = Path(__file__).resolve().parents[1] / "src" / "bifrost_market_data"

#: The dimensions ``ops_jobs.ingest_freshness`` held on 2026-10-06 (read-only
#: SELECT, 18:3x UTC), ``slot:*`` rows aside. Each must keep a writer; a new
#: dimension is added here when it first appears.
LIVE_DIMENSIONS_2026_10_06 = frozenset(
    {
        "calendar",
        "dividends",
        "financials",
        "job_trim",
        "option_contract",
        "option_daily",
        "option_expiration",
        "option_minute",
        "option_open_interest",
        "option_snapshot",
        "ratios",
        "sec_filings",
        "short_interest",
        "short_volume",
        "splits",
        "stock_daily",
        "stock_daily_unadjusted",
        "stock_minute",
        "stock_movers",
        "stock_snapshot",
        "ticker_related",
        "ticker_sync",
        "treasury_yields",
    }
)


def _extra_dimensions_in_handlers() -> set[str]:
    """Keys of every ``"freshness_extra": {...}`` literal a handler returns or sets."""
    out: set[str] = set()
    for path in (SRC / "ingest").glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            value: ast.AST | None = None
            if isinstance(node, ast.Dict):
                for k, v in zip(node.keys, node.values):
                    if isinstance(k, ast.Constant) and k.value == "freshness_extra":
                        value = v
            elif isinstance(node, ast.Assign):
                for t in node.targets:
                    if (
                        isinstance(t, ast.Subscript)
                        and isinstance(t.slice, ast.Constant)
                        and t.slice.value == "freshness_extra"
                    ):
                        value = node.value
            if isinstance(value, ast.Dict):
                out |= {k.value for k in value.keys if isinstance(k, ast.Constant)}
    return out


def _inline_dimensions() -> set[str]:
    """``update_freshness(conn, "<literal>", …)`` outside the worker (trim)."""
    out: set[str] = set()
    for path in SRC.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and getattr(node.func, "id", getattr(node.func, "attr", None)) == "update_freshness"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
            ):
                out.add(node.args[1].value)
    return out


def _writers(monkeypatch: pytest.MonkeyPatch) -> set[str]:
    kinds: set[str] = set(SUPPORTED_BACKFILL_KINDS)
    undriven: list[str] = []
    for slot in _RUNNABLE:
        try:
            kinds |= {j["kind"] for j in _jobs_of(slot, monkeypatch)}
        except Exception:  # noqa: BLE001 — a slot the fake cannot drive; its declaration below
            undriven.append(slot)
        kinds |= set(SLOT_EVIDENCE.get(slot, {}).get("kinds") or [])
    return (
        {dimension_for_kind(k) for k in kinds}
        | _extra_dimensions_in_handlers()
        | _inline_dimensions()
        | {slot_freshness_key(s) for s in SHAPE_NAMED_SLOTS}
    )


def test_every_live_or_read_dimension_has_a_current_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ratchet: no dimension may be listed or read without something that writes it."""
    read = (
        {c.freshness_dimension for c in CONTRACTS if c.freshness_dimension}
        | {
            ev["freshness"]
            for ev in SLOT_EVIDENCE.values()
            if ev.get("freshness") and not ev.get("retired") and not ev.get("migrated")
        }
        | set(quality.EXPECTED_FRESHNESS_DIMENSIONS)
    )
    orphans = sorted((LIVE_DIMENSIONS_2026_10_06 | read) - _writers(monkeypatch))
    assert orphans == [], f"freshness dimensions nothing writes any more: {orphans}"


def test_the_ratchet_sees_option_expiration_only_through_the_contract_walk() -> None:
    """The writer is the handler's ``freshness_extra``, not the retired kind."""
    assert "option_expiration" in _extra_dimensions_in_handlers()
    assert "job_trim" in _inline_dimensions()


def _listed() -> Any:
    return mock_client(
        fetch_options_contracts={
            "results": [
                {
                    "ticker": f"O:ESQ2611{d}C00050000",
                    "underlying_ticker": "ESQ",
                    "expiration_date": f"2026-11-{d}",
                    "strike_price": 50,
                    "contract_type": "call",
                }
                for d in ("20", "27")
            ],
            "pages": 1,
            "truncated": False,
        }
    )


@pytest.mark.asyncio
async def test_the_contract_walk_reports_the_expirations_it_wrote() -> None:
    result = await handle_option_contract(
        make_job("option_contract", {"underlying": "ESQ", "expired": False}), _listed(), FakeConn()
    )
    assert result["freshness_extra"] == {"option_expiration": 2}


@pytest.mark.asyncio
async def test_an_empty_walk_does_not_bump_option_expiration() -> None:
    empty = mock_client(fetch_options_contracts={"results": [], "pages": 1, "truncated": False})
    result = await handle_option_contract(
        make_job("option_contract", {"underlying": "ESQ", "expired": False}), empty, FakeConn()
    )
    assert "freshness_extra" not in result


class _Cur:
    def __init__(self, parent: _Conn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


@pytest.mark.asyncio
async def test_the_worker_writes_the_option_expiration_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_slot_freshness import _job

    monkeypatch.setattr("bifrost_market_data.worker.loop.mark_done", lambda *a, **k: None)

    async def handler(job: Any) -> Any:
        return await handle_option_contract(job, _listed(), FakeConn())

    conn = _Conn()
    await process_one_job(
        conn,
        _job("option_contract", {"underlying": "ESQ", "expired": False}),
        handlers={"option_contract": handler},
        health=HealthState(pool="options"),
    )
    writes = [p[0] for q, p in conn.statements if "ingest_freshness" in q.lower()]
    assert writes == ["option_contract", "slot:option-refresh", "option_expiration"]
