"""TD-106: one trim runs, the second caller is told it is already running.

The client timeout lives here and in research
(plugin_http.TRIM_CLIENT_TIMEOUT_SEC). Neither CI can import the other, so
both tests assert the same number, 1200.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import yaml

from bifrost_market_data.scheduler.trim_flight import (
    TRIM_CLIENT_TIMEOUT_SEC,
    start_single_flight,
)

ROOT = Path(__file__).resolve().parents[1]
#: option_daily and short_volume each take dated_budget_sec once. The key is
#: absent from the schedule, and the trim uses 60.
_DATED_DEFAULT_SEC = 60.0


def _trim(path: Path) -> dict[str, Any]:
    if path.name == "configmap-schedule.yaml":
        cm = yaml.safe_load(path.read_text())
        doc = yaml.safe_load(cm["data"]["schedule.yaml"])
    else:
        doc = yaml.safe_load(path.read_text())
    return doc["scheduler"]["slots"]["trim"]


def test_trim_budgets_finish_inside_the_client_timeout() -> None:
    assert TRIM_CLIENT_TIMEOUT_SEC == 1200.0
    for path in (
        ROOT / "config" / "schedule.yaml",
        ROOT / "k8s" / "base" / "configmap-schedule.yaml",
    ):
        slot = _trim(path)
        # job rows once, the snapshot window twice (intraday then the rest,
        # or the archive pair — never both), and two dated tables.
        total = (
            float(slot["budget_sec"])
            + 2 * float(slot["snapshot_budget_sec"])
            + 2 * float(slot.get("dated_budget_sec") or _DATED_DEFAULT_SEC)
        )
        assert total < TRIM_CLIENT_TIMEOUT_SEC, f"{path.name} budgets sum to {total}"


def test_two_concurrent_starts_run_trim_once() -> None:
    lock = threading.Lock()
    ran: list[int] = []
    finished: list[tuple[Any, str]] = []
    started = threading.Event()
    release = threading.Event()

    def try_lock() -> bool:
        return lock.acquire(blocking=False)

    def run() -> dict[str, Any]:
        ran.append(1)
        started.set()
        assert release.wait(2)
        return {"retention_archive": {"raw_market.option_daily": {"rows": 1}}}

    def finish(job_id: Any, status: str, result: dict[str, Any]) -> None:
        assert "retention_archive" in result
        finished.append((job_id, status))

    def spawn(fn: Any) -> None:
        threading.Thread(target=fn, daemon=True).start()

    first = start_single_flight(
        try_lock=try_lock,
        running_job=lambda: "9",
        begin=lambda: "9",
        run=run,
        finish=finish,
        unlock=lock.release,
        spawn=spawn,
    )
    assert started.wait(1)
    second = start_single_flight(
        try_lock=try_lock,
        running_job=lambda: "9",
        begin=lambda: "should-not-run",
        run=run,
        finish=finish,
        unlock=lock.release,
        spawn=spawn,
    )
    assert first == {"ok": True, "status": "accepted", "job_id": "9"}
    assert second == {"ok": True, "status": "already_running", "job_id": "9"}
    release.set()
    for _ in range(50):
        if finished:
            break
        threading.Event().wait(0.02)
    assert ran == [1]
    assert finished == [("9", "done")]
