"""GET /metrics — Prometheus exposition of what the doctor concluded.

Nothing here reads a table. The doctor is the one place that knows what a
session owes, when it is due and what counts as short; an alert rule that
re-derived that from freshness rows would be a second policy, wrong on weekends
and holidays in its own way. So this serves the doctor's cached report — the
same one the Console reads, recomputed behind the read every ten minutes — and
when it was produced, because an alert on that age is what catches the doctor
itself no longer running.

Until this existed the plugin exported nothing. A Dagster run that succeeds
only confirms an enqueue, so a slot that never fired, or fired into a stuck
queue, surfaced as a stale panel someone happened to open.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from bifrost_market_data import __version__
from bifrost_market_data.api.doctor import DOCTOR_CACHE, _DOCTOR_EMPTY, _doctor_payload
from bifrost_market_data.api.http_metrics import HTTP_METRICS

router = APIRouter(tags=["metrics"])

SEVERITIES = ("crit", "warn", "boundary", "ok")
#: Findings a person should hear about, one series each. ``boundary`` and ``ok``
#: are counted, not listed: they are the report's shape, not its news.
LISTED_SEVERITIES = ("crit", "warn")
#: The key the Console's default read uses, so a scrape and a page share one
#: computation instead of each paying seconds for their own.
_DOCTOR_KEY = "probes=False"


def _label(v: Any) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _line(name: str, value: float | int, labels: Mapping[str, Any] | None = None) -> str:
    if labels:
        body = ",".join(f'{k}="{_label(v)}"' for k, v in labels.items())
        return f"{name}{{{body}}} {value}"
    return f"{name} {value}"


def _generated_ts(report: Mapping[str, Any]) -> float:
    raw = report.get("generated_at")
    if not raw:
        return 0.0
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def render_metrics(report: Mapping[str, Any], *, version: str = __version__) -> str:
    """Text exposition from a doctor report (see ``bifrost_market_data.doctor``)."""
    out: list[str] = []

    def head(name: str, help_: str) -> None:
        out.append(f"# HELP {name} {help_}")
        out.append(f"# TYPE {name} gauge")

    head("bifrost_market_data_plugin_info", "Market Data plugin version.")
    out.append(_line("bifrost_market_data_plugin_info", 1, {"version": version}))

    head(
        "bifrost_market_data_doctor_report_timestamp_seconds",
        "When the served doctor report was computed; 0 before the first one.",
    )
    out.append(_line("bifrost_market_data_doctor_report_timestamp_seconds", _generated_ts(report)))

    findings = [f for f in report.get("findings") or [] if isinstance(f, Mapping)]
    counts = {s: 0 for s in SEVERITIES}
    for f in findings:
        sev = str(f.get("severity") or "")
        if sev in counts:
            counts[sev] += 1
    head("bifrost_market_data_doctor_findings", "Doctor findings by severity.")
    for sev in SEVERITIES:
        out.append(_line("bifrost_market_data_doctor_findings", counts[sev], {"severity": sev}))

    head(
        "bifrost_market_data_doctor_finding",
        "One series per critical or warning finding, labelled with what and where.",
    )
    for f in findings:
        sev = str(f.get("severity") or "")
        if sev not in LISTED_SEVERITIES:
            continue
        out.append(
            _line(
                "bifrost_market_data_doctor_finding",
                1,
                {
                    "id": f.get("id") or "",
                    "slot": f.get("slot") or "",
                    "severity": sev,
                    "title": f.get("title") or "",
                },
            )
        )

    head(
        "bifrost_market_data_doctor_prescriptions",
        "Prescriptions the doctor would execute on the next heal.",
    )
    out.append(
        _line("bifrost_market_data_doctor_prescriptions", len(report.get("prescriptions") or []))
    )
    return "\n".join(out) + "\n"


@router.get("/metrics", response_class=PlainTextResponse)
def metrics() -> PlainTextResponse:
    report = DOCTOR_CACHE.read(
        _DOCTOR_KEY, lambda: _doctor_payload(False), empty=dict(_DOCTOR_EMPTY)
    )
    # Request counts and latency for the API alert rules (TD-161).
    text = render_metrics(report) + HTTP_METRICS.render()
    return PlainTextResponse(text, media_type="text/plain; version=0.0.4")


__all__ = ["router", "render_metrics"]
