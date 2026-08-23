"""Tests for cron helpers and queue dashboard builders."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from bifrost_market_data.api.ingest_dashboard import build_queue_dashboard
from bifrost_market_data.scheduler.cronutil import next_fires, parse_cron, previous_fire


def test_parse_cron_hourly_step() -> None:
    minutes, hours, dows = parse_cron("20 */6 * * *")
    assert minutes == {20}
    assert hours == {0, 6, 12, 18}
    assert dows is None


def test_next_fires_stock_eod() -> None:
    after = datetime(2026, 8, 17, 15, 0, tzinfo=timezone.utc)
    fires = next_fires("30 21 * * *", after=after, count=2)
    assert fires[0] == datetime(2026, 8, 17, 21, 30, tzinfo=timezone.utc)
    assert fires[1] == datetime(2026, 8, 18, 21, 30, tzinfo=timezone.utc)


def test_previous_fire_weekly() -> None:
    before = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)  # Monday
    last = previous_fire("0 4 * * 6", before=before, lookback_days=14)
    assert last is not None
    # Saturday 2026-08-15 04:00 UTC (cron dow 6 = Saturday)
    assert last == datetime(2026, 8, 15, 4, 0, tzinfo=timezone.utc)


class _DashCur:
    def __init__(self, parent: _DashConn) -> None:
        self.parent = parent
        self._rows: list[Any] = []

    def execute(self, query: str, params: Any = None) -> None:
        q = query.lower()
        self.parent.statements.append((query, params))
        if "min(created_at)" in q and "group by kind" in q:
            self._rows = list(self.parent.activity_rows)
        elif "where status in ('pending', 'running')" in q and "group by kind" in q:
            self._rows = list(self.parent.queue_rows)
        elif "finished_at >=" in q:
            self._rows = list(self.parent.finished_rows)
        elif "min(created_at)" in q:
            self._rows = [(self.parent.oldest_pending,)]
        elif "from data_ops.ingest_freshness" in q:
            self._rows = list(self.parent.freshness_rows)
        elif "from data_ops.job_ingest" in q and "created_at >=" in q:
            self._rows = list(self.parent.window_rows)
        else:
            self._rows = []

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None

    def __enter__(self) -> _DashCur:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _DashConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.queue_rows = [
            ("stock_daily", "pending", 10),
            ("stock_daily", "running", 2),
        ]
        self.finished_rows = [("done", 30), ("failed", 1)]
        self.oldest_pending = datetime(2026, 8, 17, 20, 0, tzinfo=timezone.utc)
        self.freshness_rows = [
            ("stock_daily", datetime(2026, 8, 16, 22, 0, tzinfo=timezone.utc), 100, "ok"),
            ("option_snapshot", datetime(2026, 8, 16, 22, 30, tzinfo=timezone.utc), 50, "ok"),
            ("option_open_interest", datetime(2026, 8, 16, 22, 35, tzinfo=timezone.utc), 50, "ok"),
            ("calendar", datetime(2026, 8, 16, 22, 0, tzinfo=timezone.utc), 1, "ok"),
        ]
        self.window_rows = [("done", 5)]
        self.activity_rows = [
            (
                "stock_daily",
                datetime(2026, 8, 16, 21, 30, tzinfo=timezone.utc),
                datetime(2026, 8, 16, 22, 10, tzinfo=timezone.utc),
                12,
            ),
        ]

    def cursor(self) -> _DashCur:
        return _DashCur(self)

    def rollback(self) -> None:
        return None


def test_build_queue_dashboard(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.api import ingest_dashboard as mod

    monkeypatch.setattr(
        mod,
        "load_schedule",
        lambda: {
            "scheduler": {
                "slots": {
                    "stock-eod": {"cron": "30 21 * * *"},
                    "eod-pipeline": {"cron": "0 22 * * *"},
                }
            }
        },
    )
    conn = _DashConn()
    now = datetime(2026, 8, 17, 20, 30, tzinfo=timezone.utc)
    report = build_queue_dashboard(conn, now=now, grace_minutes=45)
    assert report["ok"] is True
    assert report["queue"]["ready_now"] == 10
    assert report["queue"]["scheduled_future"] == 0
    assert report["queue"]["running"] == 2
    assert report["throughput"]["done_last_15m"] == 30
    assert len(report["schedule"]["slots"]) == 5  # 2 active + 3 migrated
    migrated = [s for s in report["schedule"]["slots"] if s.get("migrated")]
    assert len(migrated) == 3
    assert all(s["adherence"] == "migrated" for s in migrated)
    assert all("moved to Research" in s["note"] for s in migrated)
    assert all("next_fires" in s for s in report["schedule"]["slots"])
    assert report["schedule"]["horizon"]["start"] == "2026-08-16T20:30:00Z"
    assert report["schedule"]["horizon"]["end"] == "2026-08-18T02:30:00Z"
    stock = next(s for s in report["schedule"]["slots"] if s["slot"] == "stock-eod")
    assert "2026-08-16T21:30:00Z" in (stock.get("fires_in_window") or [])
    assert stock["drain"]["started_at"] == "2026-08-16T21:30:00Z"
    assert stock["drain"]["active"] is True
    assert stock["drain"]["ended_at"] is None
