"""Main asyncio worker loop — claim, dispatch, mark done/failed."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.freshness import (
    dimension_for_kind,
    rows_written_from_result,
    update_freshness,
)
from bifrost_market_data.worker.claim import JobRow, claim_job, mark_done, mark_failed
from bifrost_market_data.worker.health import HealthState, start_health_server

logger = logging.getLogger(__name__)

Handler = Callable[[JobRow], Awaitable[Mapping[str, Any] | None]]

POOL_KINDS: dict[str, tuple[str, ...]] = {
    "stocks": (
        "stock_daily",
        "stock_daily_grouped",
        "stock_minute",
        "stock_snapshot",
        "stock_movers",
        "ticker_sync",
        "financials",
        "splits",
        "dividends",
        "calendar",
    ),
    "options": (
        "option_daily",
        "option_minute",
        "option_snapshot",
        "option_contract",
        "option_expiration",
        "option_open_interest",
    ),
}


def kinds_for_pool(pool: str) -> tuple[str, ...]:
    key = (pool or "stocks").strip().lower()
    if key not in POOL_KINDS:
        raise ValueError(f"unknown worker pool: {pool!r} (expected stocks|options)")
    return POOL_KINDS[key]


def _polygon_api_key(config: Mapping[str, Any]) -> str:
    poly = dict(config.get("polygon") or {})
    key = str(poly.get("api_key") or os.environ.get("POLYGON_API_KEY") or "").strip()
    return key


def build_default_handlers(
    config: Mapping[str, Any],
    *,
    client: Any | None = None,
    connect: Callable[[], Any] | None = None,
) -> tuple[dict[str, Handler], Any]:
    """Create ingest handler registry and PolygonClient (caller must aclose client)."""
    from bifrost_market_data.ingest import build_handler_registry
    from bifrost_market_data.polygon.client import PolygonClient

    def _default_connect() -> Any:
        import psycopg

        return psycopg.connect(**postgres_connect_kwargs(dict(config)))

    open_conn = connect or _default_connect
    if client is None:
        poly = dict(config.get("polygon") or {})
        api_key = _polygon_api_key(config)
        if not api_key:
            raise ValueError(
                "polygon.api_key (or POLYGON_API_KEY) is required for ingest handlers"
            )
        client = PolygonClient(
            api_key,
            tier=str(poly.get("tier") or "developer"),
            rest_base=str(poly.get("rest_base") or "https://api.polygon.io"),
        )
    registry = build_handler_registry(client, connect=open_conn)
    return registry, client


async def _run_handler(handler: Handler, job: JobRow) -> Mapping[str, Any] | None:
    result = handler(job)
    if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
        return await result  # type: ignore[misc]
    return result  # type: ignore[return-value]


async def process_one_job(
    conn: Any,
    job: JobRow,
    *,
    handlers: Mapping[str, Handler],
    health: HealthState | None = None,
) -> None:
    """Dispatch a claimed job and persist done/failed outcome."""
    handler = handlers.get(job.kind)
    if handler is None:
        err = f"no handler registered for kind={job.kind!r}"
        logger.error(err)
        mark_failed(
            conn,
            job.id,
            err,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
        )
        if health is not None:
            health.record_failed()
        return

    try:
        result = await _run_handler(handler, job)
        mark_done(conn, job.id, result)
        try:
            update_freshness(
                conn,
                dimension_for_kind(job.kind),
                rows_written_from_result(result),
            )
        except Exception as freshness_err:
            logger.warning(
                "job %s kind=%s freshness update failed: %s",
                job.id,
                job.kind,
                freshness_err,
            )
        if health is not None:
            health.record_done()
        logger.info("job %s kind=%s done", job.id, job.kind)
    except Exception as e:
        logger.exception("job %s kind=%s failed: %s", job.id, job.kind, e)
        mark_failed(
            conn,
            job.id,
            str(e),
            attempts=job.attempts,
            max_attempts=job.max_attempts,
        )
        if health is not None:
            health.record_failed()


async def run_loop(
    *,
    pool: str,
    cfg: Mapping[str, Any] | None = None,
    shutdown_event: asyncio.Event | None = None,
    handlers: Mapping[str, Handler] | None = None,
    health_port: int = 8080,
    connect: Callable[[], Any] | None = None,
    claim_fn: Callable[..., JobRow | None] = claim_job,
    poll_interval_sec: float | None = None,
    concurrency: int | None = None,
    polygon_client: Any | None = None,
) -> None:
    """Claim and execute ingest jobs until ``shutdown_event`` is set.

    ``connect`` may be injected in tests (returns a sync psycopg-like connection).
    When ``handlers`` is omitted, real ingest handlers are built (requires API key).
    """
    config = dict(cfg) if cfg is not None else load_config()
    worker_cfg = dict(config.get("worker") or {})
    pool_name = (pool or worker_cfg.get("pool") or "stocks").strip().lower()
    kinds = kinds_for_pool(pool_name)
    interval = float(
        poll_interval_sec
        if poll_interval_sec is not None
        else worker_cfg.get("poll_interval_sec", 5)
    )
    max_concurrency = int(
        concurrency if concurrency is not None else worker_cfg.get("concurrency", 1)
    )
    max_concurrency = max(1, max_concurrency)

    stop = shutdown_event or asyncio.Event()
    health = HealthState(pool=pool_name)

    def _default_connect() -> Any:
        import psycopg

        return psycopg.connect(**postgres_connect_kwargs(config))

    open_conn = connect or _default_connect
    owned_client: Any | None = None
    if handlers is not None:
        registry: dict[str, Handler] = dict(handlers)
    else:
        registry, owned_client = build_default_handlers(
            config,
            client=polygon_client,
            connect=open_conn,
        )
        if polygon_client is not None:
            owned_client = None  # caller owns lifecycle

    health_server = await start_health_server(health, port=health_port)
    sem = asyncio.Semaphore(max_concurrency)
    in_flight: set[asyncio.Task[None]] = set()

    logger.info(
        "worker loop starting pool=%s kinds=%s concurrency=%s poll=%.1fs",
        pool_name,
        ",".join(kinds),
        max_concurrency,
        interval,
    )

    async def claim_and_run() -> bool:
        """Claim at most one job. Returns True if a job was claimed."""
        await sem.acquire()
        conn = None
        try:
            conn = await asyncio.to_thread(open_conn)
            job = await asyncio.to_thread(claim_fn, conn, list(kinds))
            if job is None:
                await asyncio.to_thread(conn.close)
                conn = None
                sem.release()
                return False

            health.record_claim()

            async def _run(job_row: JobRow = job, c: Any = conn) -> None:
                try:
                    await process_one_job(c, job_row, handlers=registry, health=health)
                finally:
                    try:
                        await asyncio.to_thread(c.close)
                    finally:
                        sem.release()

            task = asyncio.create_task(_run())
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)
            conn = None  # ownership transferred to task
            return True
        except Exception:
            if conn is not None:
                try:
                    await asyncio.to_thread(conn.close)
                except Exception:
                    pass
            sem.release()
            raise

    try:
        while not stop.is_set():
            try:
                claimed_any = False
                for _ in range(max_concurrency):
                    if stop.is_set():
                        break
                    if len(in_flight) >= max_concurrency:
                        break
                    claimed = await claim_and_run()
                    if not claimed:
                        break
                    claimed_any = True

                if stop.is_set():
                    break

                if not claimed_any:
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=interval)
                    except TimeoutError:
                        pass
                elif in_flight:
                    await asyncio.wait(
                        in_flight,
                        timeout=0.05,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
            except Exception:
                logger.exception("worker loop iteration error")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=min(interval, 2.0))
                except TimeoutError:
                    pass
    finally:
        if in_flight:
            logger.info("draining %s in-flight jobs", len(in_flight))
            await asyncio.gather(*list(in_flight), return_exceptions=True)
        health_server.close()
        await health_server.wait_closed()
        if owned_client is not None:
            try:
                await owned_client.aclose()
            except Exception:
                logger.exception("failed to close PolygonClient")
        logger.info("worker loop stopped pool=%s", pool_name)
