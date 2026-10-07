"""TD-119: the schema Job is not in the API apply, and deploy waits for it first."""

from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _images(path: Path) -> list[dict]:
    return yaml.safe_load(path.read_text())["images"]


def _kinds(kustomization: str) -> list[str]:
    out = subprocess.check_output(
        ["kubectl", "kustomize", kustomization],
        cwd=ROOT,
        text=True,
    )
    return [
        doc["kind"]
        for doc in yaml.safe_load_all(out)
        if isinstance(doc, dict) and doc.get("kind")
    ]


def test_base_build_contains_no_job() -> None:
    assert "Job" not in _kinds("k8s/base")


def test_migrate_build_is_the_schema_job() -> None:
    kinds = _kinds("k8s/migrate")
    assert kinds.count("Job") == 1
    assert "Deployment" not in kinds


def test_migrate_namespace_matches_base() -> None:
    base = yaml.safe_load((ROOT / "k8s/base/namespace.yaml").read_text())
    migrate = yaml.safe_load((ROOT / "k8s/migrate/namespace.yaml").read_text())
    assert migrate == base


def test_migrate_image_pin_matches_base() -> None:
    assert _images(ROOT / "k8s/migrate/kustomization.yaml") == _images(
        ROOT / "k8s/base/kustomization.yaml"
    )


def test_deploy_applies_migration_and_waits_before_base() -> None:
    text = (ROOT / "Makefile").read_text()
    deploy = text.split("deploy:\n", 1)[1].split("\nverify-market-data:", 1)[0]
    migrate_at = deploy.index("kubectl apply -k k8s/migrate")
    wait_at = deploy.index("wait --for=condition=complete")
    base_at = deploy.index("kubectl apply -k k8s/base")
    assert migrate_at < wait_at < base_at
