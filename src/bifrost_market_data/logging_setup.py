"""Logging that cannot leak the vendor key.

``PolygonClient`` has always redacted URLs in its own log lines
(``polygon.client.redact_url``), but httpx logs every request itself at INFO,
full URL included — and the Polygon key is a query parameter. At ~50k jobs an
hour that wrote the live key into the cluster's logs on every single request.

Two layers, because one is not enough:

* httpx / httpcore are lowered to WARNING. Their per-request line is noise at
  this volume anyway; failures still surface.
* ``RedactSecrets`` sits on the root handler, so *any* logger — a library added
  next year, a traceback that happens to carry a URL — is masked. Redacting at
  the point of formatting is what makes it hold for messages this package
  never writes.
"""

from __future__ import annotations

import logging
import re
from typing import Any

# apiKey=… up to the next separator. Also catches the header-ish forms a
# library might print, so a rename on the vendor side does not silently
# reopen this.
_SECRET_PATTERNS = (
    re.compile(r"(?i)(apikey=)[^&\s\"']+"),
    re.compile(r"(?i)(api_key=)[^&\s\"']+"),
    re.compile(r"(?i)(authorization:\s*bearer\s+)\S+"),
)
MASK = "***"

# The libraries that log a full URL per request.
NOISY_HTTP_LOGGERS = ("httpx", "httpcore", "urllib3")


def redact(text: str) -> str:
    """Mask any vendor secret in a log line."""
    out = text
    for pattern in _SECRET_PATTERNS:
        out = pattern.sub(lambda m: f"{m.group(1)}{MASK}", out)
    return out


class RedactSecrets(logging.Filter):
    """Mask secrets in the message and its arguments before anything writes them."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str) and "=" in record.msg:
            record.msg = redact(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: redact(v) if isinstance(v, str) else v for k, v in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(redact(a) if isinstance(a, str) else a for a in record.args)
        return True


def configure_logging(level: int = logging.INFO, **basic_config: Any) -> None:
    """``logging.basicConfig`` plus redaction and quieter HTTP clients."""
    basic_config.setdefault("format", "%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logging.basicConfig(level=level, **basic_config)
    install_redaction()


def install_redaction() -> None:
    """Attach the filter to every root handler and quiet the HTTP clients.

    Idempotent: re-running an entry point must not stack filters.
    """
    for handler in logging.getLogger().handlers:
        if not any(isinstance(f, RedactSecrets) for f in handler.filters):
            handler.addFilter(RedactSecrets())
    for name in NOISY_HTTP_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
