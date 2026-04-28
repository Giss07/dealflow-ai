"""
DealFlow Offer Calculator — Full profit analysis with iterative Max Offer formula.
"""

import logging

logger = logging.getLogger(__name__)

# Constants
CLOSING_RATE = 0.01          # 1% of purchase price
LTC_RATE = 1.00              # 100% LTC
INTEREST_RATE = 0.12         # 12% annual
HOLD_MONTHS = 3
PROPERTY_TAX_ANNUAL = 904
INSURANCE_MONTHLY = 80
UTILITIES_MONTHLY = 80
STAGING_MARKETING = 100
CLOSING_SELL_RATE = 0.01     # 1% of ARV
LISTING_AGENT_RATE = 0.02    # 2% of ARV
BUYERS_AGENT_RATE = 0.02     # 2% of ARV
TARGET_PROFIT_RATE = 0.10    # 10% of ARV


def calculate_offer(listing, overrides=None):
    """
    Calculate max offer and full profit analysis.

    MAX OFFER formula (solve iteratively):
    ARV - Repairs - TotalHolding - TotalSelling - Closing(1% Purchase) - TargetProfit(10% ARV) = Purchase
    Where Holding depends on Loan = Purchase + Repairs, Interest = Loan * 12% / 4

    overrides dict can replace default constants:
      hold_months, interest_rate, selling_cost_pct, target_profit_pct
    """
    ov = overrides or {}

    hold_months = ov.get("hold_months", HOLD_MONTHS)
    interest_rate = ov.get("interest_rate", INTEREST_RATE)
    target_profit_rate = ov.get("target_profit_pct", TARGET_PROFIT_RATE)

    # selling_cost_pct overrides the sum of listing + buyer agent + closing sell
    if "selling_cost_pct" in ov:
        sell_pct = ov["selling_cost_pct"]
        listing_agent_rate = sell_pct * 0.4   # ~2% of the 5%
        buyers_agent_rate = sell_pct * 0.4    # ~2% of the 5%
        closing_sell_rate = sell_pct * 0.2    # ~1% of the 5%
    else:
        listing_agent_rate = LISTING_AGENT_RATE
        buyers_agent_rate = BUYERS_AGENT_RATE
        closing_sell_rate = CLOSING_SELL_RATE

    arv = listing.get("arv")
    repair_data = listing.get("repair_estimate", {})
    repairs_mid = repair_data.get("total_mid", 0)
    repairs_worst = repair_data.get("total_worst", 0)

    if not arv or arv <= 0:
        listing["offer_analysis"] = {"error": "No ARV available"}
        return listing

    # --- SELLING COSTS (fixed, based on ARV) ---
    closing_sell = arv * closing_sell_rate
    listing_agent = arv * listing_agent_rate
    buyers_agent = arv * buyers_agent_rate
    total_selling = STAGING_MARKETING + closing_sell + listing_agent + buyers_agent

    # --- TARGET PROFIT ---
    target_profit = arv * target_profit_rate

    # --- SOLVE FOR MAX OFFER ITERATIVELY ---
    # ARV = Purchase + Closing(1%P) + Repairs + Holding(depends on P) + Selling + Profit
    # Holding Interest = (Purchase + Repairs) * interest_rate * hold_months/12
    # Fixed holding = taxes + insurance + utilities
    hold_fraction = hold_months / 12.0
    interest_factor = interest_rate * hold_fraction
    property_tax = PROPERTY_TAX_ANNUAL * hold_fraction
    insurance = INSURANCE_MONTHLY * hold_months
    utilities = UTILITIES_MONTHLY * hold_months
    fixed_holding = property_tax + insurance + utilities

    # Algebra:
    # ARV = P + 0.01P + R + (P+R)*interest_factor + fixed_holding + total_selling + target_profit
    # ARV = P(1 + 0.01 + interest_factor) + R(1 + interest_factor) + fixed_holding + total_selling + target_profit
    # P = (ARV - (1+interest_factor)*R - fixed_holding - total_selling - target_profit) / (1 + 0.01 + interest_factor)

    p_divisor = 1 + CLOSING_RATE + interest_factor
    r_factor = 1 + interest_factor

    purchase_price = (arv - r_factor * repairs_mid - fixed_holding - total_selling - target_profit) / p_divisor
    purchase_price = max(0, round(purchase_price))

    # Now compute all costs with the solved purchase price
    closing_buy = purchase_price * CLOSING_RATE
    total_acquisition = purchase_price + closing_buy

    total_renovation = repairs_mid

    loan_amount = purchase_price + repairs_mid
    interest = loan_amount * interest_factor
    total_holding = interest + property_tax + insurance + utilities

    total_all_costs = total_acquisition + total_renovation + total_holding + total_selling
    estimated_profit = arv - total_all_costs
    cash_requirements = closing_buy  # With 100% LTC, cash needed is just closing
    roi = (estimated_profit / total_all_costs * 100) if total_all_costs > 0 else 0

    # Also compute worst case
    purchase_worst = (arv - r_factor * repairs_worst - fixed_holding - total_selling - target_profit) / p_divisor
    purchase_worst = max(0, round(purchase_worst))

    analysis = {
        "arv": arv,
        "repairs_mid": repairs_mid,
        "repairs_worst": repairs_worst,

        # BUYING
        "purchase_price": purchase_price,
        "closing_buy": round(closing_buy),
        "total_acquisition": round(total_acquisition),

        # RENOVATION
        "total_renovation": total_renovation,

        # HOLDING (itemized)
        "loan_amount": round(loan_amount),
        "interest_rate": interest_rate,
        "private_money_interest": round(interest),
        "property_taxes": round(property_tax),
        "insurance": round(insurance),
        "utilities": round(utilities),
        "total_holding": round(total_holding),

        # SELLING (itemized)
        "staging_marketing": STAGING_MARKETING,
        "closing_sell": round(closing_sell),
        "listing_agent_commission": round(listing_agent),
        "buyers_agent_commission": round(buyers_agent),
        "total_selling": round(total_selling),

        # TOTALS
        "total_all_costs": round(total_all_costs),
        "target_profit": round(target_profit),
        "estimated_profit": round(estimated_profit),
        "max_offer": purchase_price,
        "max_offer_worst": purchase_worst,
        "cash_requirements": round(cash_requirements),
        "roi_pct": round(roi, 1),
    }

    listing["offer_analysis"] = analysis
    logger.info(
        f"Offer for {listing.get('address', 'Unknown')}: "
        f"Max ${purchase_price:,} | Profit ${estimated_profit:,.0f} | ROI {roi:.1f}%"
    )

    return listing


def calculate_all_offers(listings):
    """Calculate offers for all listings."""
    for listing in listings:
        calculate_offer(listing)
    return listings


def format_profit_sheet(analysis):
    """Format the profit analysis as a readable text sheet."""
    if "error" in analysis:
        return f"Error: {analysis['error']}"

    lines = [
        "=" * 60,
        "DEAL PROFIT ANALYSIS",
        "=" * 60,
        "",
        "BUYING:",
        f"  Purchase Price (Max Offer):  ${analysis['purchase_price']:>12,}",
        f"  Closing/Title (1%):          ${analysis['closing_buy']:>12,}",
        f"  Total Acquisition:           ${analysis['total_acquisition']:>12,}",
        "",
        "RENOVATION:",
        f"  Repairs (Mid Range):         ${analysis['repairs_mid']:>12,}",
        f"  Repairs (Worst Case):        ${analysis['repairs_worst']:>12,}",
        f"  Total Renovation:            ${analysis['total_renovation']:>12,}",
        "",
        "HOLDING (3 months):",
        f"  Loan Amount (100% LTC):      ${analysis['loan_amount']:>12,}",
        f"  Private Money Interest (12%): ${analysis['private_money_interest']:>12,}",
        f"  Property Taxes:              ${analysis['property_taxes']:>12,}",
        f"  Insurance:                   ${analysis['insurance']:>12,}",
        f"  Utilities:                   ${analysis['utilities']:>12,}",
        f"  Total Holding:               ${analysis['total_holding']:>12,}",
        "",
        "SELLING:",
        f"  Staging/Marketing:           ${analysis['staging_marketing']:>12,}",
        f"  Closing Costs (1% ARV):      ${analysis['closing_sell']:>12,}",
        f"  Listing Agent (2% ARV):      ${analysis['listing_agent_commission']:>12,}",
        f"  Buyers Agent (2% ARV):       ${analysis['buyers_agent_commission']:>12,}",
        f"  Total Selling:               ${analysis['total_selling']:>12,}",
        "",
        "-" * 60,
        f"  ARV:                         ${analysis['arv']:>12,}",
        f"  Total All Costs:             ${analysis['total_all_costs']:>12,}",
        f"  Target Profit (10% ARV):     ${analysis['target_profit']:>12,}",
        f"  Estimated Profit:            ${analysis['estimated_profit']:>12,}",
        f"  Max Offer Price:             ${analysis['max_offer']:>12,}",
        f"  Max Offer (Worst Case):      ${analysis['max_offer_worst']:>12,}",
        f"  Cash Requirements:           ${analysis['cash_requirements']:>12,}",
        f"  ROI:                         {analysis['roi_pct']:>11.1f}%",
        "=" * 60,
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test = {
        "address": "123 Main St",
        "arv": 650000,
        "repair_estimate": {"total_mid": 75000, "total_worst": 120000},
    }
    calculate_offer(test)
    print(format_profit_sheet(test["offer_analysis"]))
