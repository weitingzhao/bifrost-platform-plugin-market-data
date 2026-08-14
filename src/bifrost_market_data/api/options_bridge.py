"""Bridge IB ``contract_key`` (SYM|OPT|…) ↔ Polygon ``option_ticker`` (O:…).

Trade frontend uses IB keys while market.* stores Polygon tickers.
This module provides parsing, splitting, and reconstruction utilities
so the Plugin API can accept either format and return IB-shaped keys.
"""

from __future__ import annotations

from datetime import date
from typing import List, NamedTuple, Optional, Sequence, Tuple


class IbContractParts(NamedTuple):
    underlying: str
    expiry: date
    strike: float
    option_right: str  # "C" or "P"
    original_key: str


def is_polygon_option_ticker(ck: str) -> bool:
    """True when *ck* looks like ``O:NVDA260919C00150000``."""
    return (ck or "").strip().upper().startswith("O:")


def parse_ib_contract_key(ck: str) -> Optional[IbContractParts]:
    """Parse ``SYM|OPT|expiry|strike|C`` into parts. Returns None if not IB-shaped."""
    raw = (ck or "").strip()
    if not raw or is_polygon_option_ticker(raw):
        return None
    parts = raw.split("|")
    if len(parts) < 5:
        return None
    if (parts[1] or "").strip().upper() != "OPT":
        return None
    underlying = (parts[0] or "").strip().upper()
    if not underlying:
        return None
    exp_raw = (parts[2] or "").strip()
    expiry = _expiry_to_date(exp_raw)
    if expiry is None:
        return None
    try:
        strike = round(float(parts[3]), 8)
    except (TypeError, ValueError):
        return None
    right = (parts[4] or "").strip().upper()
    if right == "CALL":
        right = "C"
    if right == "PUT":
        right = "P"
    if right not in ("C", "P"):
        return None
    return IbContractParts(underlying, expiry, strike, right, raw)


def ib_contract_key_from_parts(
    underlying: str,
    expiry: date | str,
    strike: float,
    option_right: str,
) -> str:
    """Rebuild IB ``contract_key`` from market.option_contract columns."""
    sym = (underlying or "").strip().upper()
    if isinstance(expiry, date):
        exp_s = expiry.strftime("%Y%m%d")
    else:
        exp_s = _norm_expiry_str(str(expiry))
    r = (option_right or "").strip().upper()
    if r == "CALL":
        r = "C"
    if r == "PUT":
        r = "P"
    sk = round(float(strike), 8)
    return f"{sym}|OPT|{exp_s}|{sk}|{r}"


def identity_key(
    underlying: str,
    expiry: date,
    strike: float,
    option_right: str,
) -> Tuple[str, date, float, str]:
    """Canonical tuple for matching IB parts against DB contract rows."""
    return (
        (underlying or "").strip().upper(),
        expiry,
        round(float(strike), 8),
        (option_right or "").strip().upper()[:1],
    )


def split_contract_keys(
    keys: Sequence[str],
) -> Tuple[List[str], List[IbContractParts]]:
    """Split request keys into Polygon tickers and parsed IB parts (deduped)."""
    polygon: List[str] = []
    ib_parts: List[IbContractParts] = []
    seen_poly: set[str] = set()
    seen_ib: set[str] = set()
    for raw in keys:
        k = (raw or "").strip()
        if not k:
            continue
        if is_polygon_option_ticker(k):
            canon = "O:" + k.split(":", 1)[1].upper() if ":" in k else k.upper()
            if canon not in seen_poly:
                seen_poly.add(canon)
                polygon.append(canon)
            continue
        parts = parse_ib_contract_key(k)
        if parts is None:
            continue
        if parts.original_key not in seen_ib:
            seen_ib.add(parts.original_key)
            ib_parts.append(parts)
    return polygon, ib_parts


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _expiry_to_date(expiry: str) -> Optional[date]:
    e = (expiry or "").strip()
    if not e:
        return None
    if len(e) >= 10 and e[4] == "-":
        try:
            return date.fromisoformat(e[:10])
        except ValueError:
            return None
    digits = "".join(c for c in e if c.isdigit())
    if len(digits) >= 8:
        try:
            return date(int(digits[0:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None


def _norm_expiry_str(s: str) -> str:
    """Normalize expiration to YYYYMMDD."""
    s = (s or "").strip()
    if len(s) >= 10 and s[4] == "-":
        return s[:4] + s[5:7] + s[8:10]
    return s
