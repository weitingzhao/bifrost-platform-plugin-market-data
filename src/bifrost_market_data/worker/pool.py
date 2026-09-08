"""A worker's connection pool — the difference between capacity and throughput.

The loop is written to run ``worker.concurrency`` jobs at once, but it opened a
connection per job and filled its slots one at a time. Measured 2026-09-08
against the DEV cluster: opening a connection costs ~298ms (110–594ms), a round
trip on an open one 16.7ms — eighteen times cheaper. With jobs averaging 0.78s
of real work, filling eight slots cost ~2.8s of pure setup, by which time the
first jobs had already finished. The pool was configured for eight and the
queue dashboard showed one in flight per pod, which is exactly what that
arithmetic predicts.

Kept deliberately small: borrow, return, discard. Connections are handed to
worker threads via ``asyncio.to_thread`` and are owned exclusively between
``acquire`` and ``release``, so one lock around the idle list is enough.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import Any, Callable

logger = logging.getLogger(__name__)


class ConnectionPool:
    """Reuse connections across jobs; never hand out a broken one."""

    def __init__(self, connect: Callable[[], Any], *, size: int) -> None:
        self._connect = connect
        self._size = max(1, int(size))
        self._idle: deque[Any] = deque()
        self._lock = threading.Lock()
        self._opened = 0

    @property
    def idle(self) -> int:
        with self._lock:
            return len(self._idle)

    @property
    def opened(self) -> int:
        """Connections this pool has had to open — the cost it is avoiding."""
        return self._opened

    def acquire(self) -> Any:
        """An open connection, reused when one is idle."""
        while True:
            with self._lock:
                conn = self._idle.popleft() if self._idle else None
            if conn is None:
                break
            if not _is_broken(conn):
                return conn
            _close_quietly(conn)
        self._opened += 1
        return self._connect()

    def release(self, conn: Any, *, reuse: bool = True) -> None:
        """Return a connection, or drop it when its state is not worth trusting."""
        if conn is None:
            return
        if not reuse or _is_broken(conn) or not _reset(conn):
            _close_quietly(conn)
            return
        with self._lock:
            if len(self._idle) >= self._size:
                spare = conn
            else:
                self._idle.append(conn)
                spare = None
        if spare is not None:
            _close_quietly(spare)

    def close(self) -> None:
        with self._lock:
            idle, self._idle = list(self._idle), deque()
        for conn in idle:
            _close_quietly(conn)


def _is_broken(conn: Any) -> bool:
    closed = getattr(conn, "closed", False)
    return bool(closed)


def _reset(conn: Any) -> bool:
    """Leave no open transaction behind.

    The ``bifrost`` role carries ``idle_in_transaction_session_timeout=15s``, so
    a connection returned mid-transaction is killed by the server and the next
    borrower inherits a corpse.
    """
    try:
        conn.rollback()
        return True
    except Exception as exc:  # noqa: BLE001 — a connection we cannot reset is one we drop
        logger.debug("pooled connection would not reset: %s", exc)
        return False


def _close_quietly(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        pass
