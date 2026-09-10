"""Two documents, one rule: the blueprint says what should be, the calibration
says what is. Status glyphs in the blueprint would be the two collapsing back
into one page — and a contract with no status line is a contract nobody is
answering for."""

from __future__ import annotations

import re

from fastapi.testclient import TestClient

from bifrost_market_data.api import docs
from bifrost_market_data.api.app import create_app

STATUS_GLYPHS = ("✅", "⚠️", "❌", "⏳")
CONTRACT = re.compile(r"\| \*{0,2}(C-[BCDFG]\d+)\*{0,2} \|")


def test_both_documents_are_served_with_front_matter_parsed() -> None:
    client = TestClient(create_app())
    for slug, first_line in (("blueprint", "# Massive 蓝图"), ("calibration", "# Massive 校准")):
        r = client.get(f"/market/docs/{slug}")
        assert r.status_code == 200, slug
        d = r.json()["data"]
        assert d["slug"] == slug
        assert d["version"] and d["updated"] and d["status"], slug
        assert not d["markdown"].lstrip().startswith("---"), slug
        assert d["markdown"].lstrip().startswith(first_line), slug


def test_the_blueprint_defines_the_axes_and_carries_no_status() -> None:
    body = docs.read_doc("blueprint")["markdown"]
    for anchor in (
        "## 2. 四个轴",
        "## 3. 数据集契约表",
        "whole-market",
        "universe",
        "benchmark-only",
    ):
        assert anchor in body, anchor
    for glyph in STATUS_GLYPHS:
        assert glyph not in body, (
            f"status glyph {glyph} belongs in the calibration, not the blueprint"
        )


def test_every_axis_defines_at_least_one_contract() -> None:
    defined = set(CONTRACT.findall(docs.read_doc("blueprint")["markdown"]))
    for prefix, axis in (
        ("C-B", "广度"),
        ("C-D", "深度"),
        ("C-F", "新鲜度"),
        ("C-C", "厚度"),
        ("C-G", "治理"),
    ):
        assert any(c.startswith(prefix) for c in defined), f"{axis} has no contract"


def test_the_calibration_reports_status_against_every_blueprint_contract() -> None:
    defined = set(CONTRACT.findall(docs.read_doc("blueprint")["markdown"]))
    reported = set(CONTRACT.findall(docs.read_doc("calibration")["markdown"]))
    assert defined, "no contracts defined"
    assert defined <= reported, f"contracts with no status: {sorted(defined - reported)}"
    assert any(g in docs.read_doc("calibration")["markdown"] for g in STATUS_GLYPHS)


def test_an_unknown_document_is_a_404_not_a_directory_listing() -> None:
    client = TestClient(create_app())
    assert client.get("/market/docs/../../pyproject").status_code in (404, 422)
    assert client.get("/market/docs/nope").status_code == 404


def _blueprint_text() -> str:
    filename, _title = docs.DOCS["blueprint"]
    return (docs._DOCS_DIR / filename).read_text(encoding="utf-8")


def _calibration_text() -> str:
    filename, _title = docs.DOCS["calibration"]
    return (docs._DOCS_DIR / filename).read_text(encoding="utf-8")


def test_no_two_sections_share_a_number() -> None:
    """The calibration grew two `## 2c.` headings and nobody noticed.

    Sections are how the release log points at its own evidence, so a duplicate
    means one of those pointers is silently ambiguous.
    """
    heads = re.findall(r"^## (\d[a-z]?)\. ", _calibration_text(), re.M)
    dupes = sorted({h for h in heads if heads.count(h) > 1})
    assert dupes == [], f"duplicate section numbers: {dupes}"


def test_every_section_reference_resolves() -> None:
    """A §-reference that points at nothing is worse than no reference.

    Four rounds of inserting sections in front of an anchor left the reading
    order at 2c, 2d, 2e, 2f, 2g, 2b, 2c; renumbering it moved four sections and
    every pointer at them.

    Either document may be the target — the calibration cites "蓝图 §6" — so a
    reference resolves if either one has that section.
    """
    text = _calibration_text()
    heads = set(re.findall(r"^## (\d[a-z]?)\. ", text, re.M))
    heads |= set(re.findall(r"^## (\d[a-z]?)\. ", _blueprint_text(), re.M))
    refs = set(re.findall(r"§(\d[a-z]?)", text))
    assert refs <= heads, f"dangling references: {sorted(refs - heads)}"


def test_sections_are_lettered_in_reading_order() -> None:
    """Out-of-order letters are how the duplicate arose in the first place."""
    letters = re.findall(r"^## 2([a-z])\. ", _calibration_text(), re.M)
    assert letters == sorted(letters), f"sections out of order: {letters}"
