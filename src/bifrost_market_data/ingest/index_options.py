"""Index-option underlying mapping (Polygon dual-ticker).

Polygon index options (e.g. SPX):
  - Snapshot / OI API path uses ``I:SPX``
  - Contracts reference API uses ``SPX`` (no ``I:``)
  - Contract tickers may be ``O:SPX…`` (monthly) or ``O:SPXW…`` (weekly)
  - Spot aggs use ``I:SPX``

Canonical **storage** underlying is always the root without prefix (``SPX``)
so Research GEX / Trade UI can query a single key.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class IndexOptionSpec:
    """One index option root."""

    storage: str
    """Canonical DB underlying (e.g. SPX)."""

    snapshot_api: str
    """Polygon options snapshot / OI path underlying (e.g. I:SPX)."""

    contracts_api: str
    """Polygon contracts ``underlying_ticker`` (e.g. SPX)."""

    spot_api: str
    """Polygon aggregates ticker for the index spot (e.g. I:SPX)."""

    aliases: tuple[str, ...] = ()
    """Extra tickers that should normalize to ``storage`` (e.g. SPXW, I:SPX)."""


# Built-in map — extend carefully; each entry must match Polygon conventions.
_INDEX_OPTION_SPECS: dict[str, IndexOptionSpec] = {
    "SPX": IndexOptionSpec(
        storage="SPX",
        snapshot_api="I:SPX",
        contracts_api="SPX",
        spot_api="I:SPX",
        aliases=("I:SPX", "SPXW"),
    ),
}

# Reverse alias → storage
_ALIAS_TO_STORAGE: dict[str, str] = {}
for _spec in _INDEX_OPTION_SPECS.values():
    _ALIAS_TO_STORAGE[_spec.storage] = _spec.storage
    for _a in _spec.aliases:
        _ALIAS_TO_STORAGE[_a.upper()] = _spec.storage


def get_index_option_spec(underlying: str) -> IndexOptionSpec | None:
    key = str(underlying or "").strip().upper()
    if not key:
        return None
    storage = _ALIAS_TO_STORAGE.get(key)
    if storage is None:
        return None
    return _INDEX_OPTION_SPECS.get(storage)


def storage_underlying(underlying: str) -> str:
    """Normalize any alias / API ticker to the DB storage key."""
    key = str(underlying or "").strip().upper()
    if not key:
        return key
    return _ALIAS_TO_STORAGE.get(key, key)


def snapshot_api_underlying(underlying: str) -> str:
    """Ticker for ``/v3/snapshot/options/{underlying}`` (and OI via snapshot)."""
    spec = get_index_option_spec(underlying)
    if spec is not None:
        return spec.snapshot_api
    return str(underlying or "").strip().upper()


def contracts_api_underlying(underlying: str) -> str:
    """Ticker for contracts reference ``underlying_ticker=``."""
    spec = get_index_option_spec(underlying)
    if spec is not None:
        return spec.contracts_api
    return str(underlying or "").strip().upper()


def spot_api_symbol(underlying: str) -> str | None:
    """Aggregates ticker for index spot, or None if not an index option root."""
    spec = get_index_option_spec(underlying)
    return spec.spot_api if spec is not None else None


def is_index_option_underlying(underlying: str) -> bool:
    return get_index_option_spec(underlying) is not None


def index_option_storage_roots() -> tuple[str, ...]:
    return tuple(sorted(_INDEX_OPTION_SPECS.keys()))


def load_index_option_roots_from_cfg(scheduler_cfg: Mapping[str, Any] | None) -> list[str]:
    """Optional ``index_option_underlyings`` override; default = built-in roots present in benchmarks."""
    cfg = scheduler_cfg or {}
    raw = cfg.get("index_option_underlyings")
    if raw is None:
        # When SPX (etc.) appears in iv_radar_benchmarks, treat as enabled.
        benches_raw = cfg.get("iv_radar_benchmarks")
        if benches_raw is None:
            return []
        if isinstance(benches_raw, str):
            benches = {s.strip().upper() for s in benches_raw.split(",") if s.strip()}
        else:
            benches = {str(s).strip().upper() for s in benches_raw if str(s).strip()}
        return sorted(benches & set(_INDEX_OPTION_SPECS.keys()))
    if isinstance(raw, str):
        items = [s.strip().upper() for s in raw.split(",") if s.strip()]
    else:
        items = [str(s).strip().upper() for s in raw if str(s).strip()]
    out: list[str] = []
    for item in items:
        storage = storage_underlying(item)
        if storage in _INDEX_OPTION_SPECS:
            out.append(storage)
    return sorted(set(out))


def merge_index_option_roots(symbols: Sequence[str], scheduler_cfg: Mapping[str, Any] | None) -> list[str]:
    """Union symbols with configured index-option roots (storage keys)."""
    merged = {str(s).strip().upper() for s in symbols if str(s).strip()}
    merged.update(load_index_option_roots_from_cfg(scheduler_cfg))
    return sorted(merged)
