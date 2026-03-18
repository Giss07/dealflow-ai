"""
DealFlow Analyze Deal — Run photo analysis on a single property by address.

Usage:
    python3 analyze_deal.py "123 Main St, Fontana, CA 92335"
    python3 analyze_deal.py --id 42
"""

from dotenv import load_dotenv
load_dotenv()

import sys
import json
import logging
from database import init_db, get_session, Deal, save_deal, deal_to_dict
from photo_analyzer import analyze_photos, ZONES
from repair_estimator import estimate_repairs
from offer_calculator import calculate_offer, format_profit_sheet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def analyze_by_address(address):
    """Find a deal by address and run photo analysis."""
    session = get_session()
    try:
        deal = session.query(Deal).filter(Deal.address.ilike(f"%{address}%")).first()
        if not deal:
            # Try full address match
            deal = session.query(Deal).filter(
                Deal.address.ilike(f"%{address.split(',')[0].strip()}%")
            ).first()
        if not deal:
            print(f"No deal found matching: {address}")
            print("Try a partial address or use --id <deal_id>")
            return None
        return analyze_deal(deal, session)
    finally:
        session.close()


def analyze_by_id(deal_id):
    """Find a deal by ID and run photo analysis."""
    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            print(f"No deal found with ID: {deal_id}")
            return None
        return analyze_deal(deal, session)
    finally:
        session.close()


def analyze_deal(deal, session):
    """Run photo analysis, update repairs and offer, save to DB."""
    print(f"\n{'='*60}")
    print(f"Analyzing: {deal.address}, {deal.city}, {deal.state} {deal.zip_code}")
    print(f"Price: ${deal.price:,.0f}" if deal.price else "Price: N/A")
    print(f"{'='*60}\n")

    # Build listing dict from DB record
    listing = deal_to_dict(deal)

    # Run photo analysis
    print("Running photo analysis with Claude Vision...")
    grades = analyze_photos(listing)
    listing["photo_grades"] = grades

    # Calculate overall condition
    grade_scores = {"Good": 3, "Fair": 2, "Poor": 1, "Unknown": 0}
    known = [grade_scores[g] for g in grades.values() if g != "Unknown"]
    if known:
        avg = sum(known) / len(known)
        listing["overall_condition"] = "Good" if avg >= 2.5 else "Fair" if avg >= 1.5 else "Poor"
    else:
        listing["overall_condition"] = "Unknown"

    print(f"\nPhoto Grades:")
    for zone, grade in grades.items():
        icon = "✅" if grade == "Good" else "⚠️" if grade == "Fair" else "❌" if grade == "Poor" else "❓"
        print(f"  {icon} {zone}: {grade}")
    print(f"\nOverall Condition: {listing['overall_condition']}")

    # Re-estimate repairs with new grades
    print("\nUpdating repair estimates...")
    estimate_repairs(listing)
    repair_data = listing.get("repair_estimate", {})
    print(f"  Mid Range: ${repair_data.get('total_mid', 0):,}")
    print(f"  Worst Case: ${repair_data.get('total_worst', 0):,}")

    # Re-calculate offer with updated repairs
    if listing.get("arv"):
        print("\nRecalculating offer...")
        calculate_offer(listing)
        analysis = listing.get("offer_analysis", {})
        print(format_profit_sheet(analysis))

    # Save back to database
    deal.photo_grades = json.dumps(grades)
    deal.overall_condition = listing["overall_condition"]
    deal.repairs_mid = repair_data.get("total_mid")
    deal.repairs_worst = repair_data.get("total_worst")
    deal.repair_breakdown = json.dumps(repair_data.get("breakdown")) if repair_data.get("breakdown") else None

    offer = listing.get("offer_analysis", {})
    if offer and "error" not in offer:
        deal.max_offer = offer.get("max_offer")
        deal.max_offer_worst = offer.get("max_offer_worst")
        deal.estimated_profit = offer.get("estimated_profit")
        deal.roi_pct = offer.get("roi_pct")
        deal.offer_analysis = json.dumps(offer)

    session.commit()
    print(f"\nSaved to database (deal #{deal.id})")

    return listing


if __name__ == "__main__":
    init_db()

    if len(sys.argv) < 2:
        print("Usage:")
        print('  python3 analyze_deal.py "123 Main St, Fontana"')
        print("  python3 analyze_deal.py --id 42")
        sys.exit(1)

    if sys.argv[1] == "--id":
        if len(sys.argv) < 3:
            print("Error: --id requires a deal ID")
            sys.exit(1)
        analyze_by_id(int(sys.argv[2]))
    else:
        address = " ".join(sys.argv[1:])
        analyze_by_address(address)
