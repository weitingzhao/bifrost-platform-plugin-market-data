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
