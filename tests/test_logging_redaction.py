"""The vendor key must not reach a log line — 0.18.1.

PolygonClient redacted its own URLs from the start, but httpx logs every
request itself at INFO with the full URL, and the Polygon key is a query
parameter. At ~50k jobs an hour the live key was written into the cluster's
logs on every request; it was found in a worker pod's log on 2026-09-08.
"""

from __future__ import annotations

import logging

from bifrost_market_data.logging_setup import (
    MASK,
    NOISY_HTTP_LOGGERS,
    RedactSecrets,
    configure_logging,
    install_redaction,
    redact,
)

SECRET = "MYCd1tboShKK7UTYgKN_fjtBrZUr9zhe"  # noqa: S105 — shape only, not a live key


def test_the_key_is_masked_wherever_it_appears_in_a_url() -> None:
    line = f"HTTP Request: GET https://api.polygon.io/v2/aggs/ticker/O:X/range?sort=asc&apiKey={SECRET} 200 OK"
    out = redact(line)
    assert SECRET not in out
    assert f"apiKey={MASK}" in out
    assert "sort=asc" in out  # only the secret is touched


def test_other_secret_shapes_are_masked_too() -> None:
    assert SECRET not in redact(f"?api_key={SECRET}&x=1")
    assert SECRET not in redact(f"Authorization: Bearer {SECRET}")
    assert redact("nothing to hide") == "nothing to hide"


def test_the_filter_masks_the_message_and_its_arguments() -> None:
    f = RedactSecrets()
    record = logging.LogRecord(
        "httpx",
        logging.INFO,
        __file__,
        1,
        "GET %s -> %s",
        (f"https://x?apiKey={SECRET}", "200"),
        None,
    )
    assert f.filter(record) is True
    assert SECRET not in record.getMessage()
    assert MASK in record.getMessage()
    assert "200" in record.getMessage()


def test_install_is_idempotent_and_quiets_the_http_clients() -> None:
    root = logging.getLogger()
    before = list(root.handlers)
    try:
        configure_logging(logging.INFO)
        install_redaction()
        install_redaction()
        for handler in logging.getLogger().handlers:
            assert sum(isinstance(x, RedactSecrets) for x in handler.filters) == 1
        for name in NOISY_HTTP_LOGGERS:
            assert logging.getLogger(name).level == logging.WARNING
    finally:
        root.handlers = before


def test_a_record_with_no_equals_sign_is_left_alone() -> None:
    f = RedactSecrets()
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "job 1 done", None, None)
    assert f.filter(record) is True
    assert record.getMessage() == "job 1 done"
