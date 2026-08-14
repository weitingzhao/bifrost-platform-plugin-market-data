"""Write routes for option discovery data (expirations replace)."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from bifrost_market_data.api.deps import normalize_symbol, require_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/options", tags=["options-ingest"])


class ReplaceExpirationsRequest(BaseModel):
    symbol: str
    expirations: List[str]

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, v: str) -> str:
        s = v.strip().upper()
        if not s:
            raise ValueError("symbol must not be empty")
        return s

    @field_validator("expirations")
    @classmethod
    def _parse_expirations(cls, v: List[str]) -> List[str]:
        out: List[str] = []
        for raw in v:
            s = raw.strip()
            if not s:
                continue
            if len(s) == 8 and s.isdigit():
                s = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            try:
                date.fromisoformat(s)
            except ValueError as exc:
                raise ValueError(f"Invalid date: {raw!r}") from exc
            out.append(s)
        return out


def _replace_expirations(conn: Any, symbol: str, expirations: List[str]) -> int:
    """Atomically delete + insert expirations for *symbol* in one transaction."""
    sym = normalize_symbol(symbol)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM market.option_expiration WHERE underlying = %s",
            (sym,),
        )
        if expirations:
            values = [(sym, exp) for exp in expirations]
            cur.executemany(
                """
                INSERT INTO market.option_expiration (underlying, expiry, updated_at)
                VALUES (%s, %s::date, now())
                ON CONFLICT (underlying, expiry) DO UPDATE SET updated_at = now()
                """,
                values,
            )
    conn.commit()
    return len(expirations)


@router.post("/expirations/replace")
def replace_expirations(body: ReplaceExpirationsRequest) -> dict[str, Any]:
    """Atomically replace all option expirations for a symbol.

    DELETE existing rows for the underlying, then INSERT the provided list.
    Runs in a single transaction (atomic).
    """
    if not body.expirations:
        raise HTTPException(status_code=400, detail="expirations list must not be empty")

    conn = require_db()
    try:
        replaced = _replace_expirations(conn, body.symbol, body.expirations)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("replace_expirations failed for %s", body.symbol)
        raise HTTPException(status_code=500, detail=f"write failed: {exc}") from exc
    finally:
        conn.close()

    return {
        "ok": True,
        "symbol": normalize_symbol(body.symbol),
        "replaced": replaced,
    }
