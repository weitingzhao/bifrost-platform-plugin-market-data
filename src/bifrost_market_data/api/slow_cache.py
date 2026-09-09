"""Answer at once from a cached figure, recompute behind it, say how old it is.

Three endpoints ask questions whose answers move about once a session and cost
minutes to produce: the three-axis coverage read, the inventory, and the SEPA
readiness summary. Measured on DEV under backfill load, they took 80s, 141s and
81s inside the pod, against a 60-second API gateway — so two panels that were
themselves correct simply never received data, and the Overview tab marked all
six analytics products blocked because its inventory call had timed out.

The shape that fixes it is not a faster query. Nothing here can be made to
answer in a second: the inventory's widest read is one full pass over 13.6M
``stock_daily`` rows to count distinct symbols, which is 150s however it is
written (the ``UPPER(TRIM())`` wrapper measured *faster* than the bare column
on 2026-09-09, so unwrapping it is not the fix either). What fixes it is not
making the reader wait: serve the last answer immediately, start a recompute
behind it, and state the age rather than dressing a cached number as live.

A caller arriving while another refresh is already running is told ``computing``
is true — the question is "is a fresh answer on its way", not "did I start one".
"""

from __future__ import annotations

import logging
import threading
from time import monotonic
from typing import Any, Callable

logger = logging.getLogger(__name__)

#: These figures move once a session. Several viewers polling must not each pay
#: for a full-table pass, and a longer window would make a mid-session repair
#: invisible for too long to act on.
DEFAULT_TTL_SEC = 600.0


class BackgroundCache:
    """Per-key cached payloads with a single background recompute in flight."""

    def __init__(self, name: str, *, ttl_sec: float = DEFAULT_TTL_SEC) -> None:
        self.name = name
        self.ttl_sec = float(ttl_sec)
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._refreshing: dict[str, bool] = {}
        self._lock = threading.Lock()

    # ── reading ──

    def read(
        self,
        key: str,
        compute: Callable[[], dict[str, Any]],
        *,
        empty: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """The cached answer with its age, starting a refresh when it is stale.

        With nothing cached yet the caller gets ``empty`` plus ``computing``, so
        a page can render its own "still working" state and poll, instead of
        holding a request open past the gateway's patience.
        """
        now = monotonic()
        hit = self._cache.get(key)
        fresh_enough = hit is not None and now - hit[0] <= self.ttl_sec
        if not fresh_enough:
            computing = self.start_refresh(key, compute)
            if hit is None:
                out = dict(empty or {})
                out["age_sec"] = None
                out["computing"] = computing
                return out
        assert hit is not None  # a miss returned above
        out = dict(hit[1])
        out["age_sec"] = round(now - hit[0], 1)
        out["computing"] = not fresh_enough
        return out

    def compute_now(self, key: str, compute: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        """Recompute synchronously and store it — the explicit ``?refresh=true`` path."""
        payload = self._timed(compute)
        self._store(key, payload)
        out = dict(payload)
        out["age_sec"] = 0.0
        out["computing"] = False
        return out

    # ── refreshing ──

    def start_refresh(self, key: str, compute: Callable[[], dict[str, Any]]) -> bool:
        """Ensure a recompute is in flight for this key; True when one is."""
        with self._lock:
            if self._refreshing.get(key):
                return True
            self._refreshing[key] = True

        def run() -> None:
            try:
                self._store(key, self._timed(compute))
            except Exception:  # noqa: BLE001 — a failed refresh keeps the last good answer
                logger.exception("%s refresh failed for %s", self.name, key)
            finally:
                with self._lock:
                    self._refreshing[key] = False

        threading.Thread(target=run, name=f"{self.name}-{key}", daemon=True).start()
        return True

    def is_refreshing(self, key: str) -> bool:
        with self._lock:
            return bool(self._refreshing.get(key))

    # ── internals ──

    def _timed(self, compute: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        started = monotonic()
        payload = dict(compute())
        payload["computed_ms"] = int((monotonic() - started) * 1000)
        return payload

    def _store(self, key: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self._cache[key] = (monotonic(), payload)

    def clear(self) -> None:
        """Drop every cached answer — for tests, which must not inherit state."""
        with self._lock:
            self._cache.clear()
            self._refreshing.clear()


__all__ = ["BackgroundCache", "DEFAULT_TTL_SEC"]
