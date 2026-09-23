"""SEC filings as text — 8-K text, the vendor's 8-K classification, 10-K sections.

Two job kinds over the same three writes:

``sec_filings_market``  every filing in a filing-date window, whole market,
                        kept for the Research universe. The daily path: one
                        request per endpoint per day instead of one per name.
``sec_filings_symbol``  one name, from ``since`` forward. The backfill path,
                        and the only way a name that joins the universe later
                        gets its history.

Why the 8-K *text* is the spine and the classification is not
--------------------------------------------------------------
Measured 2026-09-23: the vendor classifies about two 8-Ks in five (273 of 703
filings on 2026-08-06) and its earnings class is sparse (one PLTR earnings 8-K
classified since 2020). The text endpoint returns every 8-K, and the SEC item
numbers in it are a fixed public taxonomy — Item 2.02 is Results of Operations,
the earnings print. Parsed here, deterministically, it gave eight or nine
quarters of print dates for each of six names sampled. So ``items`` is the
complete record and the disclosures table is an enrichment of part of it.
"""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Any, Iterable, Mapping

from bifrost_market_data.ingest._upsert import parse_date
from bifrost_market_data.scopes import universe_symbols
from bifrost_market_data.symbol_void import clear_symbol_void, record_symbol_void
from bifrost_market_data.worker.claim import JobRow

logger = logging.getLogger(__name__)

#: The annual-report sections kept. Owner scope 2026-09-23: risk factors and
#: management's discussion — the two a narrative reading quotes from. The
#: vendor also serves ``business``; it is not collected.
TEN_K_SECTIONS: tuple[str, ...] = ("risk_factors", "mda")

#: Tickers per 10-K sections request on the daily path. Each row is a whole
#: section of an annual report (about 126 KB on average, measured), so the
#: universe is asked for in slices rather than in one page run.
TEN_K_TICKERS_PER_REQUEST = 50

#: ``ops_jobs.symbol_source_void.data_type`` per collection. Two, not one: a
#: fund files no 10-K but may file 8-Ks, and a void on one must not subtract
#: the name from the other's denominator.
VOID_8K = "sec_8k"
VOID_10K = "sec_10k"

# "Item 2.02", "ITEM 5.02.", "Item 9.01 Financial Statements…". Two digits after
# the point is the SEC's own format; a bare "Item 2" is a 10-K part, not an 8-K
# item, and is deliberately not matched.
_ITEM_RE = re.compile(r"\bitem\s+(\d{1,2}\.\d{2})\b", re.IGNORECASE)


def parse_8k_items(text: str | None) -> list[str]:
    """SEC item numbers named in an 8-K's text, sorted and de-duplicated.

    Normalised to the SEC's spelling (``2.02``), so a leading zero or case
    difference cannot split one item into two.
    """
    if not text:
        return []
    found = set()
    for m in _ITEM_RE.finditer(text):
        major, minor = m.group(1).split(".")
        found.add(f"{int(major)}.{minor}")
    return sorted(found, key=lambda s: tuple(int(p) for p in s.split(".")))


def filings_since(day: date, days: int) -> str:
    """The backfill's first filing date, pinned to the first of a month.

    ``day - 730`` would move every day, and a catch-up job still queued from
    yesterday would then differ from today's by payload alone and run twice.
    The first of the month keeps one payload per name for a month.
    """
    back = day - timedelta(days=int(days))
    return back.replace(day=1).isoformat()


def universe_missing_filings(conn: Any, *, limit: int) -> list[str]:
    """Universe names holding no 8-K at all and not voided for 8-Ks this month.

    Best-effort: it runs inside the ``fundamentals-market`` enqueue, and a
    failure here must cost the catch-up, not the ratios pull beside it.
    """
    if limit <= 0:
        return []
    try:
        rows = _missing_filings_rows(conn, limit)
    except Exception as exc:  # noqa: BLE001
        logger.warning("filings catch-up skipped: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    return [str(r[0]).strip().upper() for r in rows or [] if r and r[0]]


def _missing_filings_rows(conn: Any, limit: int) -> list[Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.symbol
            FROM research.option_universe u
            WHERE NOT EXISTS (
                SELECT 1 FROM raw_market.sec_8k_filing f WHERE f.symbol = u.symbol
            )
              AND NOT EXISTS (
                SELECT 1 FROM ops_jobs.symbol_source_void v
                WHERE v.symbol = u.symbol AND v.data_type = %s
                  AND v.last_checked >= now() - interval '30 days'
            )
            ORDER BY u.symbol
            LIMIT %s
            """,
            (VOID_8K, int(limit)),
        )
        return list(cur.fetchall() if hasattr(cur, "fetchall") else [])


def _sym(value: Any) -> str:
    return str(value or "").strip().upper()


def _reject_truncation(what: str, data: Mapping[str, Any]) -> None:
    # Same rule as the whole-market fundamentals: a pull that stopped at the
    # page cap has silently dropped the rest of the window.
    if data.get("truncated"):
        raise RuntimeError(
            f"{what}: pull hit the page cap after {data.get('pages')} pages and covers "
            "only part of the window — raise max_pages or narrow the window"
        )


def _filing_rows(results: Iterable[Any], keep: set[str] | None) -> list[tuple[Any, ...]]:
    rows: dict[tuple[str, str], tuple[Any, ...]] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        sym = _sym(item.get("ticker"))
        acc = str(item.get("accession_number") or "").strip()
        filed = parse_date(item.get("filing_date"))
        if not sym or not acc or filed is None:
            continue
        if keep is not None and sym not in keep:
            continue
        text = item.get("items_text")
        rows[(acc, sym)] = (
            acc,
            sym,
            item.get("cik"),
            item.get("form_type"),
            filed,
            item.get("filing_url"),
            parse_8k_items(text),
            text,
        )
    return list(rows.values())


def _disclosure_rows(results: Iterable[Any], keep: set[str] | None) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        acc = str(item.get("accession_number") or "").strip()
        filed = parse_date(item.get("filing_date"))
        if not acc or filed is None:
            continue
        # A disclosure can name several tickers (a parent and its listed
        # subsidiary); it is one row per ticker so a symbol read finds it.
        for raw in item.get("tickers") or []:
            sym = _sym(raw)
            if not sym or (keep is not None and sym not in keep):
                continue
            rows.append(
                (
                    acc,
                    sym,
                    item.get("cik"),
                    filed,
                    item.get("filing_url"),
                    item.get("primary_category"),
                    item.get("secondary_category"),
                    item.get("tertiary_category"),
                    item.get("supporting_text"),
                )
            )
    return rows


def _section_rows(results: Iterable[Any], keep: set[str] | None) -> list[tuple[Any, ...]]:
    rows: dict[tuple[str, str, Any, Any], tuple[Any, ...]] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        sym = _sym(item.get("ticker"))
        section = str(item.get("section") or "").strip()
        period_end = parse_date(item.get("period_end"))
        filed = parse_date(item.get("filing_date"))
        if not sym or not section or period_end is None or filed is None:
            continue
        if keep is not None and sym not in keep:
            continue
        rows[(sym, section, period_end, filed)] = (
            sym,
            section,
            period_end,
            filed,
            item.get("cik"),
            item.get("filing_url"),
            item.get("text"),
        )
    return list(rows.values())


def _write_filings(cur: Any, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    cur.executemany(
        """
        INSERT INTO raw_market.sec_8k_filing
            (accession_number, symbol, cik, form_type, filing_date, filing_url, items, items_text)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (accession_number, symbol) DO UPDATE SET
            cik = EXCLUDED.cik,
            form_type = EXCLUDED.form_type,
            filing_date = EXCLUDED.filing_date,
            filing_url = EXCLUDED.filing_url,
            items = EXCLUDED.items,
            items_text = EXCLUDED.items_text,
            fetched_at = now()
        """,
        rows,
    )
    return len(rows)


def _write_disclosures(cur: Any, rows: list[tuple[Any, ...]]) -> int:
    """Replace every row of the (accession, symbol) pairs this pull returned.

    The table has no natural key (see the DDL), so an upsert would accumulate:
    a reclassified filing would keep its old category beside the new one. The
    delete is scoped to exactly the pairs present, so a filing the pull did not
    return is left alone rather than erased.
    """
    if not rows:
        return 0
    pairs = sorted({(r[0], r[1]) for r in rows})
    cur.execute(
        """
        DELETE FROM raw_market.sec_8k_disclosure d
        USING unnest(%s::text[], %s::text[]) AS k(accession_number, symbol)
        WHERE d.accession_number = k.accession_number AND d.symbol = k.symbol
        """,
        ([p[0] for p in pairs], [p[1] for p in pairs]),
    )
    cur.executemany(
        """
        INSERT INTO raw_market.sec_8k_disclosure
            (accession_number, symbol, cik, filing_date, filing_url,
             primary_category, secondary_category, tertiary_category, supporting_text)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    return len(rows)


def _write_sections(cur: Any, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    cur.executemany(
        """
        INSERT INTO raw_market.sec_10k_section
            (symbol, section, period_end, filing_date, cik, filing_url, text)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, section, period_end, filing_date) DO UPDATE SET
            cik = EXCLUDED.cik,
            filing_url = EXCLUDED.filing_url,
            text = EXCLUDED.text,
            fetched_at = now()
        """,
        rows,
    )
    return len(rows)


def _write_all(
    conn: Any,
    filings: list[tuple[Any, ...]],
    disclosures: list[tuple[Any, ...]],
    sections: list[tuple[Any, ...]],
) -> dict[str, int]:
    """One transaction for the three writes: a filing and its classification
    land together or not at all."""
    try:
        with conn.cursor() as cur:
            out = {
                "sec_8k_filing": _write_filings(cur, filings),
                "sec_8k_disclosure": _write_disclosures(cur, disclosures),
                "sec_10k_section": _write_sections(cur, sections),
            }
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return out


async def handle_sec_filings_market(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    gte = str(payload.get("filing_date_gte") or "").strip()
    lte = str(payload.get("filing_date_lte") or "").strip() or None
    if not gte:
        raise ValueError("sec_filings_market payload requires filing_date_gte")

    keep = universe_symbols(conn)
    if not keep:
        # Filtering to an empty set writes nothing and reports success, which
        # is the silent-skip this plugin has been bitten by before. research.
        # option_universe answering empty is a fault to see, not a filter.
        raise RuntimeError("sec_filings_market: research.option_universe is empty — refusing to filter to nothing")

    text = await client.fetch_sec_8k_text(filing_date_gte=gte, filing_date_lte=lte)
    _reject_truncation("sec 8-K text", text)
    disc = await client.fetch_sec_8k_disclosures(filing_date_gte=gte, filing_date_lte=lte)
    _reject_truncation("sec 8-K disclosures", disc)
    names = tuple(sorted(keep))
    section_results: list[Any] = []
    for i in range(0, len(names), TEN_K_TICKERS_PER_REQUEST):
        secs = await client.fetch_sec_10k_sections(
            sections=TEN_K_SECTIONS,
            tickers=names[i : i + TEN_K_TICKERS_PER_REQUEST],
            filing_date_gte=gte,
            filing_date_lte=lte,
        )
        _reject_truncation("sec 10-K sections", secs)
        section_results.extend(secs.get("results") or [])

    written = _write_all(
        conn,
        _filing_rows(text.get("results") or [], keep),
        _disclosure_rows(disc.get("results") or [], keep),
        _section_rows(section_results, keep),
    )
    return {
        "rows_written": sum(written.values()),
        "written": written,
        # What the vendor returned beside what the universe kept — for 8-Ks
        # the difference is the filter, not a loss. Sections are asked for by
        # universe name, so there the two agree.
        "seen": {
            "sec_8k_filing": len(text.get("results") or []),
            "sec_8k_disclosure": len(disc.get("results") or []),
            "sec_10k_section": len(section_results),
        },
        "filing_date_gte": gte,
        "filing_date_lte": lte,
        "universe": len(keep),
    }


async def handle_sec_filings_symbol(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    sym = _sym(payload.get("symbol"))
    since = str(payload.get("since") or "").strip()
    if not sym or not since:
        raise ValueError("sec_filings_symbol payload requires symbol and since")

    text = await client.fetch_sec_8k_text(ticker=sym, filing_date_gte=since)
    _reject_truncation(f"sec 8-K text {sym}", text)
    disc = await client.fetch_sec_8k_disclosures(ticker=sym, filing_date_gte=since)
    _reject_truncation(f"sec 8-K disclosures {sym}", disc)
    secs = await client.fetch_sec_10k_sections(
        sections=TEN_K_SECTIONS, tickers=(sym,), filing_date_gte=since
    )
    _reject_truncation(f"sec 10-K sections {sym}", secs)

    # ``keep={sym}``: the disclosures endpoint returns a filing that names
    # this ticker among others, and the other names are not this job's to
    # write.
    filings = _filing_rows(text.get("results") or [], {sym})
    disclosures = _disclosure_rows(disc.get("results") or [], {sym})
    sections = _section_rows(secs.get("results") or [], {sym})
    written = _write_all(conn, filings, disclosures, sections)

    # Voids are per collection. No disclosures is not a void: the vendor
    # classifies a minority of filings, so an unclassified name is normal.
    for rows, void in ((filings, VOID_8K), (sections, VOID_10K)):
        if rows:
            clear_symbol_void(conn, sym, void)
        else:
            record_symbol_void(conn, sym, void, note=f"no rows since {since}")

    return {
        "rows_written": sum(written.values()),
        "written": written,
        "symbol": sym,
        "since": since,
        "earnings_prints": sum(1 for r in filings if "2.02" in r[6]),
    }
