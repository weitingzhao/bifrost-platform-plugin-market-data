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
