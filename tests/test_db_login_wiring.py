"""D6 (2026-10-04): every workload takes its Postgres login from the Secret.

The switch from bifrost to data_writer (and its rollback) is one patch of
``market-data-secrets``: ``postgres-user`` and ``postgres-password`` together.
A workload that maps the password but not the user would sign in with the
ConfigMap's name and the Secret's password — a failed login after the switch.
"""

from __future__ import annotations

from pathlib import Path

import yaml

BASE = Path(__file__).resolve().parents[1] / "k8s" / "base"


def _containers(doc: dict) -> list[dict]:
    spec = doc.get("spec") or {}
    if doc.get("kind") == "CronJob":
        spec = spec["jobTemplate"]["spec"]
    pod = spec["template"]["spec"]
    return list(pod.get("containers") or []) + list(pod.get("initContainers") or [])


def _secret_env(container: dict) -> dict[str, tuple[str, str, bool]]:
    out = {}
    for env in container.get("env") or []:
        ref = (env.get("valueFrom") or {}).get("secretKeyRef")
        if ref:
            out[env["name"]] = (ref["name"], ref["key"], bool(ref.get("optional")))
    return out


def _workloads() -> list[tuple[str, dict]]:
    found = []
    for path in sorted(BASE.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if isinstance(doc, dict) and doc.get("kind") in ("Deployment", "CronJob", "Job"):
                found.append((f"{path.name}:{doc['metadata']['name']}", doc))
    return found


def test_every_password_comes_with_its_user() -> None:
    checked = 0
    for name, doc in _workloads():
        for c in _containers(doc):
            env = _secret_env(c)
            if "POSTGRES_PASSWORD" not in env:
                continue
            checked += 1
            assert env.get("POSTGRES_USER") == ("market-data-secrets", "postgres-user", True), name
            assert env["POSTGRES_PASSWORD"][:2] == ("market-data-secrets", "postgres-password"), name
    # 3 Deployments, 14 CronJobs, the migrate Job.
    assert checked == 18


def test_the_user_key_is_optional_so_old_secrets_keep_working() -> None:
    """Until the Owner adds postgres-user, the ConfigMap's postgres.user applies."""
    for name, doc in _workloads():
        for c in _containers(doc):
            ref = _secret_env(c).get("POSTGRES_USER")
            if ref:
                assert ref[2] is True, name
