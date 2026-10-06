"""Page budget for ``/v3/reference/options/contracts`` enumerations (TD-90).

Three handlers walk the vendor's contract catalogue page by page:
``option_contract`` (the live catalogue), ``option_expiration`` (its expiry
list) and ``option_backfill_plan`` (expired contracts for one window). They
share one page size and one cap, defined here, so the cap arithmetic in a
comment can never drift from the number the request actually sends again.

Measured 2026-10-05: the request sent ``limit=250`` while the comments assumed
1,000 a page, so SPX's ~29,300 live contracts took 118 of a 120-page cap — a
listing cycle away from silently losing the far SPXW expiries. The endpoint
accepts 1,000 (checked against the live API the same day), which puts SPX at
~30 of 120 pages.

A walk that stops at the cap is a failure, never a result: the doctor's chain
coverage divides by this catalogue and the option-bars slot reads it, so a
short catalogue hides its own gap. Whole-market handlers already raise on
``truncated``; these do too.
"""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest.index_options import is_index_option_underlying
from bifrost_market_data.polygon.endpoints import OPTIONS_CONTRACTS_PAGE_LIMIT

#: Contracts per page the request asks for (the vendor's maximum for this endpoint).
PAGE_LIMIT = OPTIONS_CONTRACTS_PAGE_LIMIT
#: Index chains (SPX) list several times an equity's contracts.
INDEX_PAGE_CAP = 120
EQUITY_PAGE_CAP = 60
#: The doctor warns once an enumeration uses more than this share of its cap,
#: so the next growth shows up as amber before it turns into a failed job.
PAGE_CAP_WARN_RATIO = 0.80

#: Kinds that enumerate the contracts endpoint and must never succeed truncated.
CATALOGUE_KINDS: tuple[str, ...] = ("option_contract", "option_expiration", "option_backfill_plan")


def contract_page_cap(storage: str) -> int:
    """Default ``max_pages`` for one underlying's live-catalogue walk."""
    return INDEX_PAGE_CAP if is_index_option_underlying(storage) else EQUITY_PAGE_CAP


class CatalogueTruncatedError(RuntimeError):
    """The contract walk hit its page cap before the vendor ran out of pages."""


def reject_truncated_catalogue(kind: str, underlying: str, data: Mapping[str, Any], max_pages: int) -> None:
    """Raise before anything is written when the walk stopped at the cap.

    Raising first means a short walk never touches the stored catalogue: rows
    are never replaced by a subset, ``updated_at`` does not advance (so the
    rotation keeps the name at the front of the queue), and the job turns red
    where the doctor's failure check sees it.
    """
    if data.get("truncated"):
        raise CatalogueTruncatedError(
            f"{kind} {underlying}: contract catalogue hit the page cap "
            f"({data.get('pages')} of {max_pages} pages at {PAGE_LIMIT}/page, "
            f"{len(data.get('results') or [])} contracts) and the vendor has more — "
            "raise the page cap; a partial catalogue is not written"
        )

