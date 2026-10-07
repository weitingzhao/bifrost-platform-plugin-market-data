"""No CronJob ships from this plugin: Dagster fires every slot (TD-124).

Until 2026-10-06, 14 CronJobs in k8s/base had been suspended since 2026-08-29
(Dagster ``market_slot_schedules`` took the slots) yet were still applied and
re-pinned on every release, and ``k8s/cronjob-option-backfill.yaml`` sat outside
the kustomization pinned to 0.10.0. Each slot's cron was written in three places;
unsuspending one would have doubled a writer. A slot is scheduled in Dagster and
declared in ``config/schedule.yaml``; it is never a CronJob here.

``k8s/archive/`` is not applied (not in any kustomization) and is skipped.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

K8S = Path(__file__).resolve().parents[1] / "k8s"


def test_no_manifest_outside_the_archive_is_a_cronjob() -> None:
    manifests = [p for p in K8S.rglob("*.yaml") if "archive" not in p.relative_to(K8S).parts]
    assert manifests, "the manifests moved; this test would pass by accident"
    hits = [
        f"{p.relative_to(K8S)}:{doc['metadata']['name']}"
        for p in manifests
        for doc in yaml.safe_load_all(p.read_text(encoding="utf-8"))
        if isinstance(doc, dict) and doc.get("kind") == "CronJob"
    ]
    assert hits == []


# The docs described a CronJob scheduler for a month after TD-124 deleted every
# CronJob (TD-201: README "scheduler/ # CronJob enqueue", CLAUDE.md "CronJob
# scheduler", STG_PROMOTE's per-slot CronJob steps). An agent following them looks
# for -- or recreates -- a CronJob. Dagster (bifrost-research
# orchestration/market_slot_schedules.py) fires every slot via
# POST /market/ingest/enqueue-slot. A doc line may name a CronJob only as history,
# listed here as (path, a substring of that line).
ROOT = K8S.parent
CRONJOB_WORD = re.compile(r"cron\s?job", re.IGNORECASE)
HISTORY_LINES: tuple[tuple[str, str], ...] = (
    ("docs/OPTION_BACKFILL_PROGRAM.md", "(history: deleted 2026-10-06, TD-124"),
    ("docs/SUBSCRIPTION_FOCUS_PROGRAM.md", "`k8s/base/cronjob-option-trades.yaml` 删除"),
    ("docs/SUBSCRIPTION_FOCUS_PROGRAM.md", "/ CronJob / Dagster 资产与调度全部删除"),
)


def _doc_files() -> list[Path]:
    files = [*ROOT.glob("*.md"), *(ROOT / "docs").rglob("*.md"), *(ROOT / "src").rglob("*.md")]
    files += [p for p in K8S.rglob("*.md") if "archive" not in p.relative_to(K8S).parts]
    return sorted(set(files))


def _cronjob_lines() -> list[tuple[str, int, str]]:
    return [
        (str(p.relative_to(ROOT)), n, line)
        for p in _doc_files()
        for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
        if CRONJOB_WORD.search(line)
    ]


def _is_history(path: str, line: str) -> bool:
    return any(path == hp and marker in line for hp, marker in HISTORY_LINES)


def test_docs_do_not_describe_a_cronjob_scheduler() -> None:
    names = {str(p.relative_to(ROOT)) for p in _doc_files()}
    assert {"README.md", "CLAUDE.md", "docs/STG_PROMOTE.md"} <= names, (
        "the docs moved; this test would pass by accident"
    )
    stale = [
        f"{p}:{n}: {line.strip()}" for p, n, line in _cronjob_lines() if not _is_history(p, line)
    ]
    assert stale == [], (
        "Docs name a CronJob, but none ships (TD-124): Dagster fires every slot via "
        "POST /market/ingest/enqueue-slot. Reword, or add a history line to HISTORY_LINES: "
        f"{stale}"
    )


def test_every_history_line_still_exists() -> None:
    """A stale allowlist entry would let a new line with the same words through."""
    hits = _cronjob_lines()
    unused = [
        (hp, m) for hp, m in HISTORY_LINES if not any(p == hp and m in line for p, _, line in hits)
    ]
    assert unused == []
