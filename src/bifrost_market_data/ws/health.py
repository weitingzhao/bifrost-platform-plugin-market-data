"""Redis health-hash writer (inlined from bifrost-core for Plugin independence).

Writes a ``bifrost:health:ws_*`` hash to Redis so the Monitor dashboard can
show connection status. Automatically appends ``updated_at`` and refreshes TTL.
"""

from __future__ import annotations

import time
from typing import Any, Dict


_DEFAULT_TTL = 180


class HealthHashWriter:
    """Write a Redis Hash health record for the Polygon WS service."""

    def __init__(
        self,
        redis_client: Any,
        key: str,
        *,
        ttl: int = _DEFAULT_TTL,
    ) -> None:
        self._r = redis_client
        self._key = key
        self._ttl = ttl

    def write(self, fields: Dict[str, Any]) -> None:
        """Write *fields* to the hash and refresh its TTL."""
        mapping = {k: _to_str(v) for k, v in fields.items()}
        mapping["updated_at"] = _to_str(time.time())
        self._r.hset(self._key, mapping=mapping)
        self._r.expire(self._key, self._ttl)

    def delete(self) -> None:
        """Remove the hash entirely."""
        self._r.delete(self._key)

    @property
    def key(self) -> str:
        return self._key


def _to_str(v: Any) -> str:
    if isinstance(v, bool):
        return "1" if v else "0"
    if v is None:
        return ""
    return str(v)
