"""W0-P2 tests: options_bridge utility + bridged chain/contracts/strikes/expirations endpoints."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api.options_bridge import (
    ib_contract_key_from_parts,
    identity_key,
    is_polygon_option_ticker,
    parse_ib_contract_key,
    split_contract_keys,
)


# ──────────────────────────────────────────────────────────────────
# Bridge unit tests
# ──────────────────────────────────────────────────────────────────


class TestIsPolygonOptionTicker:
    def test_polygon_ticker(self) -> None:
        assert is_polygon_option_ticker("O:NVDA260919C00150000") is True

    def test_polygon_lower(self) -> None:
        assert is_polygon_option_ticker("o:nvda260919c00150000") is True

    def test_ib_key(self) -> None:
        assert is_polygon_option_ticker("NVDA|OPT|20260919|150.0|C") is False

    def test_empty(self) -> None:
        assert is_polygon_option_ticker("") is False

    def test_none(self) -> None:
        assert is_polygon_option_ticker(None) is False  # type: ignore[arg-type]


class TestParseIbContractKey:
    def test_standard(self) -> None:
        result = parse_ib_contract_key("NVDA|OPT|20260919|150.0|C")
        assert result is not None
        assert result.underlying == "NVDA"
        assert result.expiry == date(2026, 9, 19)
        assert result.strike == 150.0
        assert result.option_right == "C"
        assert result.original_key == "NVDA|OPT|20260919|150.0|C"

    def test_put(self) -> None:
        result = parse_ib_contract_key("AAPL|OPT|20260620|200.5|P")
        assert result is not None
        assert result.option_right == "P"
        assert result.strike == 200.5

    def test_call_long_form(self) -> None:
        result = parse_ib_contract_key("SPY|OPT|20260918|450.0|CALL")
        assert result is not None
        assert result.option_right == "C"

    def test_put_long_form(self) -> None:
        result = parse_ib_contract_key("SPY|OPT|20260918|450.0|PUT")
        assert result is not None
        assert result.option_right == "P"

    def test_iso_date(self) -> None:
        result = parse_ib_contract_key("NVDA|OPT|2026-09-19|150.0|C")
        assert result is not None
        assert result.expiry == date(2026, 9, 19)

    def test_polygon_returns_none(self) -> None:
        assert parse_ib_contract_key("O:NVDA260919C00150000") is None

    def test_invalid_parts(self) -> None:
        assert parse_ib_contract_key("NVDA|OPT|bad|150.0|C") is None
        assert parse_ib_contract_key("NVDA|STK|20260919|150.0|C") is None
        assert parse_ib_contract_key("short") is None
        assert parse_ib_contract_key("") is None

    def test_invalid_strike(self) -> None:
        assert parse_ib_contract_key("NVDA|OPT|20260919|bad|C") is None

    def test_invalid_right(self) -> None:
        assert parse_ib_contract_key("NVDA|OPT|20260919|150.0|X") is None


class TestIbContractKeyFromParts:
    def test_from_date(self) -> None:
        ck = ib_contract_key_from_parts("NVDA", date(2026, 9, 19), 150.0, "C")
        assert ck == "NVDA|OPT|20260919|150.0|C"

    def test_from_string_date(self) -> None:
        ck = ib_contract_key_from_parts("NVDA", "2026-09-19", 150.0, "C")
        assert ck == "NVDA|OPT|20260919|150.0|C"

    def test_from_yyyymmdd(self) -> None:
        ck = ib_contract_key_from_parts("AAPL", "20260620", 200.5, "P")
        assert ck == "AAPL|OPT|20260620|200.5|P"

    def test_normalizes_right(self) -> None:
        ck = ib_contract_key_from_parts("SPY", date(2026, 9, 18), 450.0, "CALL")
        assert ck == "SPY|OPT|20260918|450.0|C"


class TestIdentityKey:
    def test_roundtrip(self) -> None:
        k = identity_key("nvda", date(2026, 9, 19), 150.0, "call")
        assert k == ("NVDA", date(2026, 9, 19), 150.0, "C")

    def test_normalizes_case_and_right(self) -> None:
        k = identity_key("  aapl  ", date(2026, 6, 20), 200.5, "put")
        assert k == ("AAPL", date(2026, 6, 20), 200.5, "P")

    def test_precision(self) -> None:
        k = identity_key("AAPL", date(2026, 6, 20), 200.123456789, "P")
        assert k[2] == round(200.123456789, 8)


class TestSplitContractKeys:
    def test_mixed(self) -> None:
        keys = [
            "NVDA|OPT|20260919|150.0|C",
            "O:NVDA260919C00150000",
            "AAPL|OPT|20260620|200.0|P",
        ]
        polygon, ib_parts = split_contract_keys(keys)
        assert polygon == ["O:NVDA260919C00150000"]
        assert len(ib_parts) == 2
        assert ib_parts[0].underlying == "NVDA"
        assert ib_parts[1].underlying == "AAPL"

    def test_dedup(self) -> None:
        keys = [
            "NVDA|OPT|20260919|150.0|C",
            "NVDA|OPT|20260919|150.0|C",
            "O:NVDA260919C00150000",
            "O:NVDA260919C00150000",
        ]
        polygon, ib_parts = split_contract_keys(keys)
        assert len(polygon) == 1
        assert len(ib_parts) == 1

    def test_empty_and_invalid(self) -> None:
        keys = ["", "  ", "invalid", None]  # type: ignore[list-item]
        polygon, ib_parts = split_contract_keys(keys)
        assert polygon == []
        assert ib_parts == []


# ──────────────────────────────────────────────────────────────────
# Endpoint tests (mock DB layer)
# ──────────────────────────────────────────────────────────────────


class _FakeCursor:
    """Minimal cursor that returns canned results based on the SQL executed."""

    def __init__(self, results: list[Any] | None = None) -> None:
        self._results = results or []

    def execute(self, sql: str, params: Any = None) -> None:
        self._last_sql = sql

    def fetchall(self) -> list[Any]:
        return self._results

    def fetchone(self) -> Any:
        return self._results[0] if self._results else None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_: Any) -> None:
        pass


class _FakeConn:
    """Fake connection that routes cursor() to a _FakeCursor."""

    def __init__(self, cursor: _FakeCursor | None = None) -> None:
        self._cursor = cursor or _FakeCursor()

    def close(self) -> None:
        pass

    def cursor(self) -> _FakeCursor:
        return self._cursor


def _client() -> TestClient:
    return TestClient(create_app())


def test_chain_latest_rejects_empty() -> None:
    resp = _client().get("/market/options/chain/latest", params={"keys": ""})
    assert resp.status_code == 400


def test_chain_latest_rejects_over_120() -> None:
    keys = ",".join([f"O:T{i}" for i in range(130)])
    resp = _client().get("/market/options/chain/latest", params={"keys": keys})
    assert resp.status_code == 400


def test_chain_latest_missing_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.api import options as mod

    monkeypatch.setattr(mod, "require_db", lambda: _FakeConn(_FakeCursor([None])))
    monkeypatch.setattr(mod, "table_exists", lambda _c, _s, _t: False)
    resp = _client().get("/market/options/chain/latest", params={"keys": "O:X"})
    assert resp.status_code == 200
    assert resp.json()["rows"] == []


def test_chain_eod_missing_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.api import options as mod

    monkeypatch.setattr(mod, "require_db", lambda: _FakeConn(_FakeCursor([None])))
    monkeypatch.setattr(mod, "table_exists", lambda _c, _s, _t: False)
    resp = _client().get(
        "/market/options/chain/eod",
        params={"keys": "O:X", "since": "2026-01-01"},
    )
    assert resp.status_code == 200
    assert resp.json()["rows"] == []


def test_contracts_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.api import options as mod

    fake_rows = [
        {
            "option_ticker": "O:NVDA260919C00150000",
            "underlying": "NVDA",
            "expiry": date(2026, 9, 19),
            "strike": 150.0,
            "option_right": "C",
        },
    ]
    cursor = _FakeCursor(fake_rows)
    monkeypatch.setattr(mod, "require_db", lambda: _FakeConn(cursor))
    monkeypatch.setattr(mod, "table_exists", lambda _c, _s, _t: True)

    resp = _client().get("/market/options/contracts", params={"symbol": "NVDA"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["count"] == 1
    c = data["contracts"][0]
    assert c["option_ticker"] == "O:NVDA260919C00150000"
    assert c["ib_contract_key"] == "NVDA|OPT|20260919|150.0|C"


def test_contracts_with_expiry_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.api import options as mod

    cursor = _FakeCursor([])
    monkeypatch.setattr(mod, "require_db", lambda: _FakeConn(cursor))
    monkeypatch.setattr(mod, "table_exists", lambda _c, _s, _t: True)

    resp = _client().get(
        "/market/options/contracts",
        params={"symbol": "NVDA", "expiry": "20260919"},
    )
    assert resp.status_code == 200
    assert resp.json()["count"] == 0


def test_strikes_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.api import options as mod

    cursor = _FakeCursor([{"strike": 100.0}, {"strike": 105.0}, {"strike": 110.0}])
    monkeypatch.setattr(mod, "require_db", lambda: _FakeConn(cursor))
    monkeypatch.setattr(mod, "table_exists", lambda _c, _s, _t: True)

    resp = _client().get(
        "/market/options/strikes",
        params={"symbol": "NVDA", "expiry": "20260919"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["strikes"] == [100.0, 105.0, 110.0]
    assert data["count"] == 3


def test_strikes_invalid_expiry() -> None:
    resp = _client().get(
        "/market/options/strikes",
        params={"symbol": "NVDA", "expiry": "bad"},
    )
    assert resp.status_code == 400


def test_expirations_yyyymmdd(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.api import options as mod

    cursor = _FakeCursor([
        {"expiry": date(2026, 9, 19)},
        {"expiry": date(2026, 10, 16)},
    ])
    monkeypatch.setattr(mod, "require_db", lambda: _FakeConn(cursor))
    monkeypatch.setattr(mod, "table_exists", lambda _c, _s, _t: True)

    resp = _client().get(
        "/market/options/expirations/yyyymmdd",
        params={"symbol": "NVDA"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["expirations"] == ["20260919", "20261016"]
    assert data["count"] == 2


def test_expirations_yyyymmdd_missing_table(monkeypatch: pytest.MonkeyPatch) -> None:
    from bifrost_market_data.api import options as mod

    monkeypatch.setattr(mod, "require_db", lambda: _FakeConn())
    monkeypatch.setattr(mod, "table_exists", lambda _c, _s, _t: False)

    resp = _client().get(
        "/market/options/expirations/yyyymmdd",
        params={"symbol": "NVDA"},
    )
    assert resp.status_code == 200
    assert resp.json()["expirations"] == []


def test_existing_expirations_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify original /expirations endpoint still works after W0-P2 additions."""
    from bifrost_market_data.api import options as mod

    monkeypatch.setattr(mod, "require_db", lambda: _FakeConn())
    monkeypatch.setattr(
        mod,
        "query_expirations",
        lambda *_a, **_k: {
            "symbol": "AAPL",
            "expirations": ["2025-06-20"],
            "strikes": [100.0],
            "provider": "db",
            "source": "option_expiration",
            "expiration_for_strikes": "2025-06-20",
        },
    )
    resp = _client().get("/market/options/expirations", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    assert resp.json()["expirations"] == ["2025-06-20"]
