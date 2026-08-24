"""Golden Source schema guard — reject legacy feature schema names at API startup."""

from __future__ import annotations

from typing import Any

FORBIDDEN_LEGACY_SCHEMAS: tuple[str, ...] = (
    "features_daily",
    "features_option",
    "features_signals",
    "features_forecasts",
    "features_backtests",
    "signals",
    "forecasts",
    "backtests",
    "market_analytics",
)


def assert_no_legacy_schemas(conn: Any) -> None:
    """Raise when any forbidden legacy schema exists in Golden Source."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)",
            (list(FORBIDDEN_LEGACY_SCHEMAS),),
        )
        found = [r[0] for r in (cur.fetchall() or [])]
    if found:
        raise RuntimeError(f"Legacy schemas present in Golden Source: {found}")
