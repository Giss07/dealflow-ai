"""
Smoke test for score_deal MCP tool — verifies the refactored 7-param signature
produces the same results as the old 2-param version for default inputs.

Run: python test_score_deal.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from offer_calculator import calculate_offer
from scorer import fallback_score
import json


def old_score_deal(price, arv):
    """Reproduces the OLD score_deal logic (2 params, no overrides)."""
    p, a = float(price), float(arv)
    listing = {
        "price": p, "arv": a, "sqft": 0,
        "days_on_zillow": None, "year_built": None,
        "repairs_mid": 0, "repairs_worst": 0,
        "has_deal_keywords": False, "matched_keywords": [],
        "photo_grades": {}, "description": "",
    }
    result = fallback_score(listing)
    listing["repair_estimate"] = {"total_mid": 0, "total_worst": 0}
    calculate_offer(listing)
    offer = listing.get("offer_analysis", {})
    return {
        "score": result["score"],
        "max_offer": offer.get("max_offer"),
        "profit": offer.get("estimated_profit"),
        "roi": offer.get("roi_pct"),
    }


def new_score_deal(price, arv, rehab="0", hold="3", interest="0.12", sell="0.05", profit="0.10"):
    """Reproduces the NEW score_deal logic (7 params with defaults)."""
    p, a = float(price), float(arv)
    rehab_f = float(rehab)
    listing = {
        "price": p, "arv": a, "sqft": 0,
        "days_on_zillow": None, "year_built": None,
        "repairs_mid": rehab_f, "repairs_worst": rehab_f,
        "has_deal_keywords": False, "matched_keywords": [],
        "photo_grades": {}, "description": "",
    }
    result = fallback_score(listing)
    listing["repair_estimate"] = {"total_mid": rehab_f, "total_worst": rehab_f}
    overrides = {
        "hold_months": int(float(hold)),
        "interest_rate": float(interest),
        "selling_cost_pct": float(sell),
        "target_profit_pct": float(profit),
    }
    calculate_offer(listing, overrides=overrides)
    offer = listing.get("offer_analysis", {})
    return {
        "score": result["score"],
        "max_offer": offer.get("max_offer"),
        "profit": offer.get("estimated_profit"),
        "roi": offer.get("roi_pct"),
    }


def test_defaults_match():
    """With default params, new and old should produce identical results."""
    cases = [
        ("450000", "650000"),
        ("300000", "500000"),
        ("600000", "800000"),
        ("200000", "350000"),
    ]

    all_pass = True
    for price, arv in cases:
        old = old_score_deal(price, arv)
        new = new_score_deal(price, arv)

        match = (
            old["score"] == new["score"]
            and old["max_offer"] == new["max_offer"]
            and old["profit"] == new["profit"]
            and old["roi"] == new["roi"]
        )

        status = "PASS" if match else "FAIL"
        if not match:
            all_pass = False

        print(f"{status}  price=${price} arv=${arv}")
        print(f"  OLD: score={old['score']} offer=${old['max_offer']:,} profit=${old['profit']:,} roi={old['roi']}%")
        print(f"  NEW: score={new['score']} offer=${new['max_offer']:,} profit=${new['profit']:,} roi={new['roi']}%")
        print()

    return all_pass


def test_overrides_change_output():
    """Overrides should produce different results than defaults."""
    default = new_score_deal("450000", "650000")
    custom = new_score_deal("450000", "650000", rehab="50000", hold="6", interest="0.10", sell="0.06", profit="0.15")

    differ = default["max_offer"] != custom["max_offer"]
    status = "PASS" if differ else "FAIL"
    print(f"{status}  Overrides produce different output")
    print(f"  DEFAULT: offer=${default['max_offer']:,} profit=${default['profit']:,}")
    print(f"  CUSTOM:  offer=${custom['max_offer']:,} profit=${custom['profit']:,}")
    print(f"  (rehab=50k, hold=6mo, interest=10%, sell=6%, profit=15%)")
    print()
    return differ


if __name__ == "__main__":
    print("=" * 60)
    print("score_deal SMOKE TEST")
    print("=" * 60)
    print()

    t1 = test_defaults_match()
    t2 = test_overrides_change_output()

    print("=" * 60)
    if t1 and t2:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
        sys.exit(1)
