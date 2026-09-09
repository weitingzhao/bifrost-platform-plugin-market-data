"""The schedule exists twice; the two copies must say the same thing.

``config/schedule.yaml`` is what a local run and the tests read.
``k8s/base/configmap-schedule.yaml`` is what the cluster mounts. A slot that
drifts between them behaves in production differently from how it reads in the
repo — on 2026-09-09 the trim's retention was changed in one and not the other,
and the cluster kept the old row cap.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _repo_scheduler() -> dict[str, Any]:
    return yaml.safe_load((ROOT / "config" / "schedule.yaml").read_text())["scheduler"]


def _cluster_scheduler() -> dict[str, Any]:
    cm = yaml.safe_load((ROOT / "k8s" / "base" / "configmap-schedule.yaml").read_text())
    return yaml.safe_load(cm["data"]["schedule.yaml"])["scheduler"]


def test_every_slot_is_configured_the_same_in_both_copies() -> None:
    repo, cluster = _repo_scheduler()["slots"], _cluster_scheduler()["slots"]
    assert sorted(repo) == sorted(cluster)
    drifted = {k: (repo[k], cluster[k]) for k in repo if repo[k] != cluster[k]}
    assert drifted == {}, f"slot config drifted between the repo and the cluster: {drifted}"


def test_the_shared_scheduler_settings_agree() -> None:
    repo, cluster = _repo_scheduler(), _cluster_scheduler()
    for key in ("watchlist_source", "iv_radar_benchmarks", "platform_api_url"):
        assert repo.get(key) == cluster.get(key), f"{key} differs"


def test_retention_is_expressed_as_a_window() -> None:
    """A row count made the answer depend on how busy the queue had been."""
    for scheduler in (_repo_scheduler(), _cluster_scheduler()):
        trim = scheduler["slots"]["trim"]
        assert "keep_hours" in trim
        assert trim["keep_hours"] >= 36, "slot adherence looks back about 30 hours"
