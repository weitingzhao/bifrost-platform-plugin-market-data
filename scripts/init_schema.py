#!/usr/bin/env python3
"""Initialize market.*, market_analytics.*, and data_ops.* schemas on the target PostgreSQL database."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running without install: python scripts/init_schema.py
_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from bifrost_market_data.config import load_config, postgres_connect_kwargs  # noqa: E402
from bifrost_market_data.schema.ddl import (  # noqa: E402
    DATA_OPS_TABLES,
    MARKET_ANALYTICS_TABLES,
    MARKET_TABLES,
    MARKET_VIEWS,
    apply_ddl,
)

_ROLES_SQL = _ROOT / "scripts" / "create_roles.sql"


def _apply_roles(conn: object) -> None:
    """Best-effort apply create_roles.sql (needs elevated privileges)."""
    if not _ROLES_SQL.is_file():
        print(f"WARNING: roles SQL not found at {_ROLES_SQL}", file=sys.stderr)
        return
    sql = _ROLES_SQL.read_text(encoding="utf-8")
    try:
        with conn.cursor() as cur:  # type: ignore[attr-defined]
            cur.execute(sql)
        conn.commit()  # type: ignore[attr-defined]
        print(f"Roles applied from {_ROLES_SQL.name}.")
    except Exception as e:
        # data_writer may lack CREATE ROLE / GRANT privileges — do not abort DDL success.
        try:
            conn.rollback()  # type: ignore[attr-defined]
        except Exception:
            pass
        print(
            f"WARNING: roles apply skipped ({type(e).__name__}: {e}). "
            f"Run `make apply-roles` as a superuser/owner if needed.",
            file=sys.stderr,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply market-data DDL to PostgreSQL")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to market-data.yaml (default: MARKET_DATA_CONFIG or config/*.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print connection target and expected objects without applying DDL",
    )
    parser.add_argument(
        "--skip-roles",
        action="store_true",
        help="Skip best-effort create_roles.sql after DDL",
    )
    parser.add_argument(
        "--roles-only",
        action="store_true",
        help="Only apply create_roles.sql (no DDL)",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    kw = postgres_connect_kwargs(cfg)
    target = f"{kw['user']}@{kw['host']}:{kw['port']}/{kw['dbname']}"
    print(f"Target: {target}")
    print(f"market tables: {', '.join(MARKET_TABLES)}")
    print(f"market_analytics tables: {', '.join(MARKET_ANALYTICS_TABLES)}")
    print(f"data_ops tables: {', '.join(DATA_OPS_TABLES)}")
    print(f"market views: {', '.join(MARKET_VIEWS)}")

    if args.dry_run:
        print("Dry run — no DDL applied.")
        return 0

    try:
        import psycopg
    except ImportError as e:
        print(f"psycopg not installed: {e}", file=sys.stderr)
        return 2

    try:
        with psycopg.connect(**kw) as conn:
            if args.roles_only:
                _apply_roles(conn)
            else:
                apply_ddl(conn)
                if not args.skip_roles:
                    _apply_roles(conn)
    except Exception as e:
        print(f"DDL failed: {e}", file=sys.stderr)
        return 1

    if args.roles_only:
        print("Roles apply finished.")
    else:
        print("DDL applied successfully (schemas market + market_analytics + data_ops).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
