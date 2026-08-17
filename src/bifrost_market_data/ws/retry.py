"""Lightweight reconnect backoff policy (inlined from bifrost-core for Plugin independence)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReconnectPolicy:
    """Exponential backoff with a configurable cap.

    Usage::

        policy = ReconnectPolicy()
        attempt = 0
        while running:
            try:
                await connect_and_run()
                attempt = 0
            except Exception:
                attempt += 1
                await asyncio.sleep(policy.delay_for_attempt(attempt))
    """

    initial_delay: float = 2.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    max_exp: int = 6

    def delay_for_attempt(self, attempt: int) -> float:
        """Return backoff seconds for the given 1-based attempt number."""
        if attempt < 1:
            attempt = 1
        exp = min(attempt - 1, self.max_exp)
        return min(self.initial_delay * (self.backoff_factor**exp), self.max_delay)
