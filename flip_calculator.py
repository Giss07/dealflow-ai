"""
Flip profit calculator — the owner's 3-month-hold spreadsheet, line for line.

    BUYING     closing / title 1% of purchase; reinstatement / misc $0
    RENOVATION repairs = rehab estimate; monthly maintenance $0
    HOLDING    loan = 100% of (purchase + buying closing + repairs)
               interest 12%/yr on the loan for the hold; points 0%
               property tax 1.2% of purchase per year (Prop 13 reset), for the hold
               insurance $80/mo, utilities $80/mo
    SELLING    staging / marketing / photos $100
               closing costs / concessions 1% of PURCHASE price (not ARV)
               seller credit to buyer 0%
               listing agent 2% of ARV, buyer's agent 2% of ARV
    PROFIT     ARV − (purchase + buying + repairs + holding + selling)

Every cost is linear in the purchase price, so the maximum allowable offer
(highest price that still leaves the target profit) is solved exactly rather
than searched for, then rounded down to the nearest $100.

This is deliberately separate from offer_calculator.py, which uses different
assumptions (5% selling, ARV-based closing) and backs score_deal.
"""

import math

DEFAULTS = {
    "hold_months": 3,
    "buy_closing_pct": 0.01,        # of purchase
    "reinstatement": 0.0,
    "monthly_maintenance": 0.0,
    "interest_rate": 0.12,          # per year, on the loan
    "points_pct": 0.0,              # of the loan
    "tax_rate": 0.012,              # per year, of purchase (Prop 13 reset)
    "insurance_monthly": 80.0,
    "utilities_monthly": 80.0,
    "staging": 100.0,
    "sell_closing_pct": 0.01,       # of PURCHASE price
    "seller_credit_pct": 0.0,       # of ARV
    "listing_agent_pct": 0.02,      # of ARV
    "buyers_agent_pct": 0.02,       # of ARV
    "target_profit_pct": 0.10,      # of ARV, for the MAO
}


def flip_costs(purchase, arv, repairs, annual_property_tax=None, **overrides):
    """Full cost / profit breakdown at one purchase price.

    annual_property_tax overrides the 1.2%-of-purchase rule with a fixed yearly
    amount. The distressed search never passes it (it always uses 1.2%); it
    exists so the calculator can be checked against a spreadsheet that does.
    """
    c = {**DEFAULTS, **overrides}
    months = c["hold_months"]

    buying_closing = purchase * c["buy_closing_pct"]
    acquisition = purchase + buying_closing + c["reinstatement"]

    renovation = repairs + c["monthly_maintenance"] * months

    loan = purchase + buying_closing + repairs
    interest = loan * c["interest_rate"] * months / 12
    points = loan * c["points_pct"]
    yearly_tax = (annual_property_tax if annual_property_tax is not None
                  else purchase * c["tax_rate"])
    taxes = yearly_tax * months / 12
    insurance = c["insurance_monthly"] * months
    utilities = c["utilities_monthly"] * months
    holding = interest + points + taxes + insurance + utilities

    selling = (c["staging"]
               + purchase * c["sell_closing_pct"]
               + arv * c["seller_credit_pct"]
               + arv * c["listing_agent_pct"]
               + arv * c["buyers_agent_pct"])

    total = acquisition + renovation + holding + selling
    return {
        "purchase": purchase, "arv": arv,
        "buying_closing": buying_closing, "acquisition": acquisition,
        "repairs": renovation,
        "loan": loan, "interest": interest, "points": points, "taxes": taxes,
        "insurance": insurance, "utilities": utilities, "holding": holding,
        "selling": selling,
        "total_costs": total,
        "profit": arv - total,
    }


def max_allowable_offer(arv, repairs, target_profit_pct=None, annual_property_tax=None,
                        round_to=100, **overrides):
    """Highest purchase price whose profit is at least target_profit_pct of ARV.

    Returns (mao, breakdown_at_mao), or (None, None) when even a $0 purchase
    cannot reach the target.
    """
    pct = DEFAULTS["target_profit_pct"] if target_profit_pct is None else target_profit_pct
    target = arv * pct
    p0 = flip_costs(0, arv, repairs, annual_property_tax, **overrides)["profit"]
    p1 = flip_costs(1, arv, repairs, annual_property_tax, **overrides)["profit"]
    slope = p0 - p1                       # profit lost per $1 of purchase price
    if slope <= 0 or p0 < target:
        return None, None
    exact = (p0 - target) / slope
    mao = math.floor(exact / round_to) * round_to
    return mao, flip_costs(mao, arv, repairs, annual_property_tax, **overrides)
