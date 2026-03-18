"""
DealFlow Repair Estimator — IE 2026 pricing for fix-and-flip repairs.
"""

import logging

logger = logging.getLogger(__name__)

# IE 2026 Repair Cost Ranges
# Format: (mid_low, mid_high, worst_low, worst_high)
FIXED_COSTS = {
    "Roof":       (15000, 20000, 22000, 28000),
    "HVAC":       (8000, 12000, 14000, 18000),
    "Plumbing":   (5000, 10000, 15000, 25000),
    "Kitchen":    (25000, 45000, 50000, 80000),
    "Bath":       (10000, 18000, 20000, 35000),
    "Foundation": (5000, 15000, 20000, 50000),
}

# Per-sqft costs for Interior
INTERIOR_COST_PER_SQFT = {
    "mid": (8, 15),
    "worst": (15, 25),
}


def estimate_zone_cost(zone, grade, sqft=None):
    """
    Estimate repair cost for a single zone based on photo grade.
    Returns (mid_estimate, worst_estimate)
    """
    if zone == "Interior":
        sqft = sqft or 1500  # default assumption
        mid_low, mid_high = INTERIOR_COST_PER_SQFT["mid"]
        worst_low, worst_high = INTERIOR_COST_PER_SQFT["worst"]
        mid_avg = ((mid_low + mid_high) / 2) * sqft
        worst_avg = ((worst_low + worst_high) / 2) * sqft
    elif zone in FIXED_COSTS:
        mid_low, mid_high, worst_low, worst_high = FIXED_COSTS[zone]
        mid_avg = (mid_low + mid_high) / 2
        worst_avg = (worst_low + worst_high) / 2
    else:
        return 0, 0

    # Adjust by grade
    if grade == "Good":
        return 0, 0
    elif grade == "Fair":
        return round(mid_avg), round(worst_avg)
    elif grade == "Poor":
        return round(worst_avg), round(worst_avg * 1.1)  # worst case slightly higher
    else:  # Unknown — assume mid range
        return round(mid_avg), round(worst_avg)


def estimate_repairs(listing):
    """
    Estimate total repairs based on photo grades.
    Returns detailed breakdown and totals.
    """
    grades = listing.get("photo_grades", {})
    sqft = listing.get("sqft") or 1500

    breakdown = {}
    total_mid = 0
    total_worst = 0

    for zone in ["Roof", "HVAC", "Plumbing", "Interior", "Kitchen", "Bath", "Foundation"]:
        grade = grades.get(zone, "Unknown")
        mid, worst = estimate_zone_cost(zone, grade, sqft)
        breakdown[zone] = {
            "grade": grade,
            "mid_estimate": mid,
            "worst_estimate": worst,
        }

        # Show the range for reference
        if zone == "Interior":
            mid_low, mid_high = INTERIOR_COST_PER_SQFT["mid"]
            worst_low, worst_high = INTERIOR_COST_PER_SQFT["worst"]
            breakdown[zone]["mid_range"] = f"${mid_low}-${mid_high}/sqft"
            breakdown[zone]["worst_range"] = f"${worst_low}-${worst_high}/sqft"
        elif zone in FIXED_COSTS:
            ml, mh, wl, wh = FIXED_COSTS[zone]
            breakdown[zone]["mid_range"] = f"${ml:,}-${mh:,}"
            breakdown[zone]["worst_range"] = f"${wl:,}-${wh:,}"

        total_mid += mid
        total_worst += worst

    result = {
        "breakdown": breakdown,
        "total_mid": total_mid,
        "total_worst": total_worst,
        "sqft_used": sqft,
    }

    listing["repair_estimate"] = result
    logger.info(f"Repairs for {listing.get('address', 'Unknown')}: Mid ${total_mid:,} / Worst ${total_worst:,}")

    return result


def estimate_all_repairs(listings):
    """Estimate repairs for all listings."""
    for listing in listings:
        estimate_repairs(listing)
    return listings


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test = {
        "address": "123 Main St",
        "sqft": 1800,
        "photo_grades": {
            "Roof": "Poor",
            "HVAC": "Fair",
            "Plumbing": "Fair",
            "Interior": "Fair",
            "Kitchen": "Poor",
            "Bath": "Fair",
            "Foundation": "Good",
        },
    }
    result = estimate_repairs(test)
    print(f"\nRepair Breakdown for {test['address']} ({test['sqft']} sqft):")
    for zone, info in result["breakdown"].items():
        print(f"  {zone}: {info['grade']} → Mid ${info['mid_estimate']:,} / Worst ${info['worst_estimate']:,}")
    print(f"\nTotal Mid: ${result['total_mid']:,}")
    print(f"Total Worst: ${result['total_worst']:,}")
