"""TD-118: option-refresh remembers "no listed options", and reads no window the queue does not keep.

Measured 2026-10-06: 17 names (ATLCL, ESQ, PLPC, NVR, NPK, …) were enqueued by
every six-hourly run, ten times each in 48 hours, every one 0 rows — never
enumerated, so ``stalest_underlyings`` sorted them first forever. And the
"finished this week" guard read ``ops_jobs.job_ingest``, which keeps finished
rows ``TRIM_KEEP_HOURS`` (48): the week was two days.
"""

from __future__ import annotations

import ast
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from ingest_testutil import FakeConn, make_job, mock_client
from test_daily import _DailyConn

from bifrost_market_data.ingest.option_contract import handle_option_contract
from bifrost_market_data.scheduler.daily import enqueue_slot
from bifrost_market_data.scheduler.enqueue import TRIM_KEEP_HOURS
from bifrost_market_data.symbol_void import NO_LISTED_OPTIONS

SRC = Path(__file__).resolve().parents[1] / "src" / "bifrost_market_data"


def _void_writes(conn: FakeConn) -> list[tuple[str, Any]]:
    return [(q, p) for q, p in conn.statements if "symbol_source_void" in q]


def _empty_catalogue() -> Any:
    return mock_client(fetch_options_contracts={"results": [], "pages": 1, "truncated": False})


@pytest.mark.asyncio
async def test_an_empty_live_catalogue_is_remembered_as_no_listed_options() -> None:
    conn = FakeConn()
    result = await handle_option_contract(
        make_job("option_contract", {"underlying": "esq", "expired": False}),
        _empty_catalogue(),
        conn,
    )
    assert result["rows_written"] == 0
    (sql, params), = _void_writes(conn)
    assert sql.lstrip().startswith("INSERT INTO ops_jobs.symbol_source_void")
    assert params[:2] == ("ESQ", NO_LISTED_OPTIONS)


@pytest.mark.asyncio
async def test_a_listed_catalogue_clears_the_verdict() -> None:
    client = mock_client(
        fetch_options_contracts={
            "results": [
                {
                    "ticker": "O:ESQ261120C00050000",
                    "underlying_ticker": "ESQ",
                    "expiration_date": "2026-11-20",
                    "strike_price": 50,
                    "contract_type": "call",
                }
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    await handle_option_contract(
        make_job("option_contract", {"underlying": "ESQ", "expired": False}), client, conn
    )
    (sql, params), = _void_writes(conn)
    assert sql.lstrip().startswith("DELETE FROM ops_jobs.symbol_source_void")
    assert params == ("ESQ", NO_LISTED_OPTIONS)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"underlying": "ESQ", "expired": True, "expiration_date_gte": "2026-01-01"},
        {"underlying": "ESQ", "expired": False, "expiration_date": "2026-11-20"},
    ],
)
async def test_a_dated_or_expired_walk_says_nothing_about_todays_listing(
    payload: dict[str, Any],
) -> None:
    conn = FakeConn()
    await handle_option_contract(make_job("option_contract", payload), _empty_catalogue(), conn)
    assert _void_writes(conn) == []


def _refresh(conn: _DailyConn, batch_size: int) -> list[str]:
    r = enqueue_slot(
        conn,
        "option-refresh",
        target_date=date(2026, 10, 6),
        scheduler_cfg={
            "slots": {
                "option-refresh": {
                    "universe": "research",
                    "max_new_per_run": 12,
                    "batch_size": batch_size,
                }
            }
        },
    )
    return [j["payload"]["underlying"] for j in r["jobs"]]


def test_a_recent_verdict_keeps_a_name_out_of_the_rotation_head() -> None:
    walked = datetime(2026, 10, 5, tzinfo=UTC)
    conn = _DailyConn(
        research_universe=[("ESQ", "core", 24), ("AAPL", "core", 24), ("MSFT", "core", 24)],
        option_contracts=[("O:AAPL1", "AAPL", date(2026, 11, 20)), ("O:MSFT1", "MSFT", date(2026, 11, 20))],
        catalogue_updated={"AAPL": walked, "MSFT": walked},
        no_options=["ESQ"],
    )
    unds = _refresh(conn, batch_size=1)
    assert "ESQ" not in unds
    assert unds[3:] == ["AAPL"], "the stalest walked name, not the void"


def test_a_verdict_past_its_recheck_is_asked_again_first() -> None:
    walked = datetime(2026, 10, 5, tzinfo=UTC)
    conn = _DailyConn(
        research_universe=[("ESQ", "core", 24), ("AAPL", "core", 24)],
        option_contracts=[("O:AAPL1", "AAPL", date(2026, 11, 20))],
        catalogue_updated={"AAPL": walked},
        no_options_aged=["ESQ"],
    )
    unds = _refresh(conn, batch_size=1)
    assert unds[3:] == ["ESQ"], "once, through the rotation — not as a newcomer as well"


def test_the_recheck_window_is_asked_of_the_void_table() -> None:
    conn = _DailyConn(research_universe=[("ESQ", "core", 24)], no_options=["ESQ"])
    _refresh(conn, batch_size=5)
    void_reads = [
        p
        for q, p in conn.statements
        if "symbol_source_void" in q and "enumerated_underlyings" not in q
    ]
    assert (NO_LISTED_OPTIONS, 7) in void_reads


# ── the lookback lint ───────────────────────────────────────────────────────

_INTERVAL = re.compile(
    r"interval\s+'(\d+(?:\.\d+)?)\s*(minute|minutes|hour|hours|day|days|week|weeks)'",
    re.IGNORECASE,
)
_HOURS = {"minute": 1 / 60, "hour": 1.0, "day": 24.0, "week": 168.0}


def _sql_literals() -> list[tuple[str, int, str]]:
    out: list[tuple[str, int, str]] = []
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                out.append((str(path.relative_to(SRC)), node.lineno, node.value))
    return out


def test_no_job_ingest_lookback_is_longer_than_the_queue_keeps() -> None:
    """A window over ``ops_jobs.job_ingest`` past ``TRIM_KEEP_HOURS`` reads rows that are gone."""
    offenders: list[str] = []
    for rel, line, text in _sql_literals():
        if "job_ingest" not in text.lower():
            continue
        for m in _INTERVAL.finditer(text):
            unit = m.group(2).lower().rstrip("s")
            hours = float(m.group(1)) * _HOURS[unit]
            if hours > TRIM_KEEP_HOURS:
                offenders.append(f"{rel}:{line}: {m.group(0)}")
    assert not offenders, (
        f"ops_jobs.job_ingest keeps finished rows {TRIM_KEEP_HOURS:g}h:\n" + "\n".join(offenders)
    )


def test_the_lint_sees_the_shape_it_guards() -> None:
    sample = "SELECT 1 FROM ops_jobs.job_ingest WHERE created_at >= now() - interval '7 days'"
    (m,) = list(_INTERVAL.finditer(sample))
    assert float(m.group(1)) * _HOURS[m.group(2).lower().rstrip("s")] == 168.0
