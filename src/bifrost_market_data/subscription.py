"""What the current Massive subscriptions cover — the one place that says so.

Owner decision 2026-09-06: use Options Starter, Stocks Starter and
Financials & Ratios fully before any upgrade. Capabilities the plan does not
cover are *planned*, not abandoned: they return when the business needs them
and the subscription is upgraded. The scheduler, the queue dashboard, the
Console and the Trade UI all read this module so the wording stays in step.
"""

from __future__ import annotations

from typing import Any

POLICY = (
    "Exploit the subscribed data first. Capabilities outside the current plan "
    "are planned, not dropped: they are enabled by upgrading the subscription "
    "once the existing data is fully used and the business case is there."
)

SUBSCRIPTIONS: list[dict[str, Any]] = [
    {
        "id": "stocks-starter",
        "label": "Stocks Starter",
        "window": "rolling 5 years",
        "calls": "unlimited",
        "delay": "15 minutes",
    },
    {
        "id": "options-starter",
        "label": "Options Starter",
        "window": "rolling 2 years",
        "calls": "unlimited",
        "delay": "15 minutes",
    },
    {
        "id": "financials-ratios",
        "label": "Financials & Ratios",
        "window": "statements from 2009; ratios and short data by date",
        "calls": "unlimited",
        "delay": None,
    },
]

# status: entitled | planned (needs an upgrade) | unavailable (endpoint gone)
CAPABILITIES: list[dict[str, Any]] = [
    {"id": "stock_aggregates", "label": "Stock daily / minute bars", "status": "entitled", "subscription": "stocks-starter", "used_by": ["stock-eod", "universe-daily", "minute-bars"]},
    {"id": "stock_snapshot", "label": "Full-market stock snapshot and movers", "status": "entitled", "subscription": "stocks-starter", "used_by": ["stock-snapshot", "stock-movers"]},
    {"id": "corporate_actions", "label": "Dividends and splits", "status": "entitled", "subscription": "stocks-starter", "used_by": ["corporate"]},
    {"id": "reference", "label": "Tickers, related companies, calendar, indicators, news", "status": "entitled", "subscription": "stocks-starter", "used_by": ["reference", "related-rotate", "calendar"]},
    {"id": "option_chain_snapshot", "label": "Option chain snapshot (IV, greeks, OI)", "status": "entitled", "subscription": "options-starter", "used_by": ["eod-pipeline"]},
    {"id": "option_contracts", "label": "Option contract catalogue (incl. expired)", "status": "entitled", "subscription": "options-starter", "used_by": ["option-refresh"]},
    {"id": "option_aggregates", "label": "Option daily / minute bars", "status": "entitled", "subscription": "options-starter", "used_by": ["option-bars", "minute-bars"]},
    {"id": "financial_statements", "label": "Income, balance sheet, cash flow", "status": "entitled", "subscription": "financials-ratios", "used_by": ["fundamentals-rotate"]},
    {"id": "ratios_short", "label": "Financial ratios, short interest, short volume", "status": "entitled", "subscription": "financials-ratios", "used_by": ["fundamentals-market"]},
    {"id": "treasury_yields", "label": "Treasury yields and inflation", "status": "entitled", "subscription": None, "used_by": []},
    {
        "id": "option_trades",
        "label": "Option trades tape",
        "status": "planned",
        "requires": "Options Developer",
        "used_by": ["option-trades", "Research order flow", "Discovery liquidity"],
        "note": "Retired from the schedule until the plan is upgraded; the slot answers skipped, not failed.",
    },
    {
        "id": "option_quotes",
        "label": "Option quotes and last trade",
        "status": "planned",
        "requires": "Options Developer / Advanced",
        "used_by": ["Discovery liquidity"],
    },
    {
        "id": "stock_trades_quotes",
        "label": "Stock trades and quotes",
        "status": "planned",
        "requires": "Stocks Developer / Advanced",
        "used_by": [],
    },
    {
        "id": "indices",
        "label": "Index levels (I:SPX, I:VIX)",
        "status": "planned",
        "requires": "Indices Starter",
        "used_by": ["eod-pipeline index spot", "Research GEX close"],
    },
    {
        "id": "filings_float",
        "label": "SEC filings and float",
        "status": "unavailable",
        "requires": None,
        "used_by": [],
        "note": "The endpoints answer 404; not a plan question.",
    },
]

# Schedule slots the plan does not cover, with the upgrade that enables them.
SLOT_REQUIREMENTS: dict[str, dict[str, str]] = {
    "option-trades": {
        "capability": "option_trades",
        "requires": "Options Developer",
        "reason": "option trades need Options Developer; current plan is Options Starter",
    },
}


def capability_matrix() -> dict[str, Any]:
    return {
        "ok": True,
        "policy": POLICY,
        "subscriptions": SUBSCRIPTIONS,
        "capabilities": CAPABILITIES,
        "retired_slots": [
            {"slot": slot, **req} for slot, req in sorted(SLOT_REQUIREMENTS.items())
        ],
    }
