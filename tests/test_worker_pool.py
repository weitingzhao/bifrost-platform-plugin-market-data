"""The worker's connection pool — capacity that the loop can actually reach.

The loop is written for ``worker.concurrency`` jobs at once but opened a
connection per job and filled its slots serially. Measured on DEV 2026-09-08:
~298ms to open one against 16.7ms to reuse, with jobs averaging 0.78s of work —
so eight slots cost ~2.8s of setup and the dashboard showed one job in flight
per pod against a configured eight.
"""

from __future__ import annotations


from bifrost_market_data.worker.pool import ConnectionPool


class _Conn:
    def __init__(self, ident: int) -> None:
        self.ident = ident
        self.closed = False
        self.rollbacks = 0
        self.reset_raises = False

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.reset_raises:
            raise RuntimeError("connection is not resettable")

    def close(self) -> None:
        self.closed = True


def _pool(size: int = 3) -> tuple[ConnectionPool, list[_Conn]]:
    made: list[_Conn] = []

    def connect() -> _Conn:
        made.append(_Conn(len(made)))
        return made[-1]

    return ConnectionPool(connect, size=size), made


def test_a_returned_connection_is_the_next_one_handed_out() -> None:
    pool, made = _pool()

    first = pool.acquire()
    pool.release(first)
    second = pool.acquire()

    assert second is first
    assert len(made) == 1  # the second job paid no connect cost
    assert pool.opened == 1


def test_concurrent_slots_each_get_their_own_connection() -> None:
    pool, made = _pool()

    held = [pool.acquire() for _ in range(3)]

    assert len({c.ident for c in held}) == 3
    for c in held:
        pool.release(c)
    assert pool.idle == 3
    # The fourth job reuses rather than opening a fourth.
    pool.acquire()
    assert len(made) == 3


def test_a_connection_is_reset_before_it_is_reused() -> None:
    """The role kills a session idle in a transaction after 15s; the next
    borrower would inherit a corpse."""
    pool, _made = _pool()

    conn = pool.acquire()
    pool.release(conn)

    assert conn.rollbacks == 1
    assert conn.closed is False


def test_a_connection_that_will_not_reset_is_dropped_not_reused() -> None:
    pool, made = _pool()

    conn = pool.acquire()
    conn.reset_raises = True
    pool.release(conn)

    assert conn.closed is True
    assert pool.idle == 0
    assert pool.acquire() is not conn
    assert len(made) == 2


def test_a_closed_connection_is_never_handed_out() -> None:
    pool, made = _pool()

    conn = pool.acquire()
    pool.release(conn)
    conn.closed = True  # the server went away while it sat idle

    fresh = pool.acquire()
    assert fresh is not conn
    assert len(made) == 2


def test_a_failed_job_returns_its_connection_without_reuse() -> None:
    pool, _made = _pool()

    conn = pool.acquire()
    pool.release(conn, reuse=False)

    assert conn.closed is True
    assert pool.idle == 0


def test_the_pool_does_not_grow_past_its_size() -> None:
    pool, _made = _pool(size=2)

    held = [pool.acquire() for _ in range(4)]
    for c in held:
        pool.release(c)

    assert pool.idle == 2
    assert sum(1 for c in held if c.closed) == 2  # the spares are closed, not leaked


def test_close_releases_everything_it_holds() -> None:
    pool, made = _pool()
    for c in [pool.acquire() for _ in range(3)]:
        pool.release(c)

    pool.close()

    assert all(c.closed for c in made)
    assert pool.idle == 0


def test_release_tolerates_no_connection() -> None:
    pool, _made = _pool()
    pool.release(None)  # the claim path releases before it has borrowed
    assert pool.idle == 0
