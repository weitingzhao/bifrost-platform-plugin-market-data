"""Extended plugin status under ``/market/status`` (Wave 5-B)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from bifrost_market_data.api.deps import connect_db, polygon_key_configured, table_exists
from bifrost_market_data.api.health import _probe_db

router = APIRouter(tags=["status"])


def query_status_summary(conn: Any) -> dict[str, Any]:
    db = _probe_db()
    freshness: list[dict[str, Any]] = []
    if table_exists(conn, "data_ops", "ingest_freshness"):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dimension, last_run_at, status, rows_written
                    FROM data_ops.ingest_freshness
                    ORDER BY last_run_at DESC NULLS LAST
                    LIMIT 20
                    """
                )
                for row in cur.fetchall() or []:
                    freshness.append(
                        {
                            "dimension": row[0],
                            "last_run_at": row[1].isoformat() if hasattr(row[1], "isoformat") else row[1],
                            "status": row[2],
                            "rows_written": row[3],
                        }
                    )
        except Exception:
            freshness = []
    return {
        "ok": db == "ok",
        "service": "market-data-api",
        "db": db,
        "polygon_configured": polygon_key_configured(),
        "freshness_summary": freshness,
    }


@router.get("/status")
def market_status() -> dict[str, Any]:
    """Plugin status: service health, DB probe, Polygon key flag, freshness summary."""
    try:
        conn = connect_db(timeout=2)
    except Exception:
        return {
            "ok": False,
            "service": "market-data-api",
            "db": "unreachable",
            "polygon_configured": polygon_key_configured(),
            "freshness_summary": [],
        }
    try:
        return query_status_summary(conn)
    finally:
        conn.close()


_connect = connect_db
