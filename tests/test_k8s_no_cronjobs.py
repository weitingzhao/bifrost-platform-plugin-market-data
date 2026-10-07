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

import io
import re
import tokenize
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
#
# Code read the same way for a month more (TD-202): the queue dashboard rendered
# "CronJob archived" and verify-market-data.sh blamed running CronJobs. Python
# string literals and comments under src/ and scripts/, and every line of the
# shell scripts, are held to the same rule; a script line that checks none exists
# is allowed like a history line.
ROOT = K8S.parent
CRONJOB_WORD = re.compile(r"cron\s?job", re.IGNORECASE)
HISTORY_LINES: tuple[tuple[str, str], ...] = (
    ("docs/OPTION_BACKFILL_PROGRAM.md", "(history: deleted 2026-10-06, TD-124"),
    ("docs/SUBSCRIPTION_FOCUS_PROGRAM.md", "`k8s/base/cronjob-option-trades.yaml` 删除"),
    ("docs/SUBSCRIPTION_FOCUS_PROGRAM.md", "/ CronJob / Dagster 资产与调度全部删除"),
    ("src/bifrost_market_data/worker/backfill.py", "``market-data-option-backfill`` CronJob that"),
    ("scripts/verify-market-data.sh", "No CronJobs (Dagster owns every slot, TD-124)"),
    ("scripts/verify-market-data.sh", "get cronjob -o name"),
    ("scripts/verify-market-data.sh", "FAIL: CronJobs must not exist in"),
    ("scripts/verify-market-data.sh", 'echo "  0 CronJobs"'),
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


# Text a reader sees: string literals (docstrings, labels, messages) and comments.
# Identifiers are code and are not scanned.
_TEXT_TOKENS = {tokenize.STRING, tokenize.COMMENT} | (
    {tokenize.FSTRING_MIDDLE} if hasattr(tokenize, "FSTRING_MIDDLE") else set()
)


def _code_files() -> tuple[list[Path], list[Path]]:
    py = sorted({*(ROOT / "src").rglob("*.py"), *(ROOT / "scripts").rglob("*.py")})
    sh = sorted((ROOT / "scripts").rglob("*.sh"))
    return py, sh


def _python_text_lines(path: Path) -> list[tuple[int, str]]:
    src = path.read_text(encoding="utf-8")
    lines = src.splitlines()
    rows: set[int] = set()
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type in _TEXT_TOKENS:
            text_lines = tok.string.splitlines() or [""]
            for offset, text in enumerate(text_lines):
                if CRONJOB_WORD.search(text):
                    rows.add(tok.start[0] + offset)
    return [(n, lines[n - 1]) for n in sorted(rows)]


def _code_cronjob_lines() -> list[tuple[str, int, str]]:
    py, sh = _code_files()
    out = [(str(p.relative_to(ROOT)), n, line) for p in py for n, line in _python_text_lines(p)]
    out += [
        (str(p.relative_to(ROOT)), n, line)
        for p in sh
        for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
        if CRONJOB_WORD.search(line)
    ]
    return out


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


def test_code_text_does_not_describe_a_cronjob_scheduler() -> None:
    py, sh = _code_files()
    names = {str(p.relative_to(ROOT)) for p in py + sh}
    assert {
        "src/bifrost_market_data/api/ingest_dashboard.py",
        "scripts/verify-market-data.sh",
    } <= names, "the code moved; this test would pass by accident"
    stale = [
        f"{p}:{n}: {line.strip()}"
        for p, n, line in _code_cronjob_lines()
        if not _is_history(p, line)
    ]
    assert stale == [], (
        "Code text names a CronJob, but none ships (TD-124, TD-202): Dagster fires every "
        "slot via POST /market/ingest/enqueue-slot. Reword, or add a history line to "
        f"HISTORY_LINES: {stale}"
    )


def test_the_python_scan_reads_strings_and_comments_not_code() -> None:
    """The scan would pass by accident if it saw no text at all."""
    sample = ROOT / "src" / "bifrost_market_data" / "api" / "ingest_dashboard.py"
    src = sample.read_text(encoding="utf-8")
    texts = [
        t.string
        for t in tokenize.generate_tokens(io.StringIO(src).readline)
        if t.type in _TEXT_TOKENS
    ]
    assert any("Readiness rollup" in t for t in texts)
    assert any(t.startswith("#") for t in texts)
    assert not any(t == "build_queue_dashboard" for t in texts)


def test_every_history_line_still_exists() -> None:
    """A stale allowlist entry would let a new line with the same words through."""
    hits = _cronjob_lines() + _code_cronjob_lines()
    unused = [
        (hp, m) for hp, m in HISTORY_LINES if not any(p == hp and m in line for p, _, line in hits)
    ]
    assert unused == []
