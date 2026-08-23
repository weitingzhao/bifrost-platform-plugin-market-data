"""PCR upsert compute moved to bifrost-research (Wave 2.1)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.skip(
    reason="moved to bifrost-research (engines.volatility.pcr)"
)


def test_placeholder_migration_note() -> None:
    """Kept so discovery still finds this module; body skipped via pytestmark."""
    assert False, "unreachable"
