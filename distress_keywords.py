"""
Shared distress-language keywords used across DealFlow.

Substring-matched (case-insensitive) against the aggregated text of a
listing — description, agent remarks, status text, broker name, etc.
Used by:
  - filter.py    → bonus signal on the Apify pipeline (has_deal_keywords)
  - scorer.py    → indirectly via has_deal_keywords / matched_keywords
  - mcp_server.py search_distressed → distress signal in MCP tool
  - rescore.py   → bulk re-scoring of existing deals

Shortest forms preferred where they subsume longer phrases
(e.g. "investor" catches "investor special" and "investor's dream").
"""

DISTRESS_KEYWORDS = [
    # Condition / repair language
    "tlc",
    "needs work",
    "fixer",
    "rehab",
    "as-is",
    "as is",
    "no repairs",
    "bring your contractor",
    "deferred maintenance",
    "cosmetic fixer",
    "diamond in the rough",

    # Tear-down / lot-value signals
    "tear-down",
    "teardown",
    "scrape",
    "lot value",

    # Investor / cash-buyer language
    "investor",
    "handyman",
    "wholesale",
    "cash only",
    "bring offers",
    "bring all offers",

    # Motivated seller / urgency
    "motivated",
    "must sell",
    "priced to sell",
    "priced to move",
    "below market",

    # Legal / foreclosure / probate
    "probate",
    "estate sale",
    "trustee sale",
    "short sale",
    "bank owned",
    "reo",
    "foreclosure",
    "auction",
]
