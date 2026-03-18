"""
Re-score ALL deals with Claude AI and extract keywords.
Usage:
    python3 rescore.py          # re-score all deals
    python3 rescore.py test     # test 5 sample deals only
"""

from dotenv import load_dotenv
load_dotenv()

import sys
import json
import logging
from database import init_db, get_session, Deal, deal_to_dict
from scorer import score_deal, get_anthropic_client, ANTHROPIC_API_KEY
from filter import INCLUDE_KEYWORDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def extract_keywords(listing):
    """Extract distress keywords from all available text fields."""
    texts = []
    for field in ["description", "address", "status", "home_type", "score_reasoning",
                   "broker", "full_address"]:
        val = listing.get(field)
        if val and isinstance(val, str):
            texts.append(val)

    # Also check listing URL for signals
    url = listing.get("listing_url") or ""
    if url:
        texts.append(url.replace("-", " ").replace("/", " "))

    all_text = " ".join(texts).lower()

    matched = []
    for kw in INCLUDE_KEYWORDS:
        if kw in all_text:
            matched.append(kw)

    # Additional price-based signals
    price = listing.get("price") or 0
    sqft = listing.get("sqft") or 0
    year = listing.get("year_built")

    if price and sqft and price / sqft < 200:
        matched.append("low $/sqft")
    if year and int(year) < 1970:
        matched.append("pre-1970")
    if listing.get("days_on_zillow") and listing["days_on_zillow"] > 60:
        matched.append("stale listing")

    return list(set(matched))  # dedupe


def rescore_deals(test_mode=False):
    init_db()
    session = get_session()

    if test_mode:
        # Pick 5 diverse deals for testing
        deals = []
        # Cheapest
        d = session.query(Deal).order_by(Deal.price.asc()).first()
        if d: deals.append(d)
        # Most expensive
        d = session.query(Deal).order_by(Deal.price.desc()).first()
        if d and d not in deals: deals.append(d)
        # Oldest
        d = session.query(Deal).filter(Deal.year_built != None).order_by(Deal.year_built.asc()).first()
        if d and d not in deals: deals.append(d)
        # Random mid-range
        d = session.query(Deal).filter(Deal.price > 300000, Deal.price < 500000).first()
        if d and d not in deals: deals.append(d)
        # Another random
        d = session.query(Deal).filter(Deal.price > 500000, Deal.price < 700000).first()
        if d and d not in deals: deals.append(d)
    else:
        deals = session.query(Deal).all()

    print(f"Re-scoring {len(deals)} deals with Claude AI...")
    print()

    client = None
    if ANTHROPIC_API_KEY:
        try:
            client = get_anthropic_client()
            print("Claude API connected")
        except Exception as e:
            print(f"Claude API failed: {e}")
            return

    for i, deal in enumerate(deals):
        listing = deal_to_dict(deal)

        # Set default photo grades if missing
        if not listing.get("photo_grades") or all(v == "Unknown" for v in listing.get("photo_grades", {}).values()):
            listing["photo_grades"] = {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
            listing["overall_condition"] = "Unknown"

        # Extract keywords
        keywords = extract_keywords(listing)
        listing["matched_keywords"] = keywords
        listing["has_deal_keywords"] = len(keywords) > 0

        # Score with Claude AI
        result = score_deal(listing, client)

        # Update database
        deal.score = result["score"]
        deal.score_reasoning = result["reasoning"]
        deal.matched_keywords = json.dumps(keywords) if keywords else None
        deal.has_deal_keywords = len(keywords) > 0

        if (i + 1) % 25 == 0 or test_mode:
            session.commit()

        if test_mode or (i + 1) % 50 == 0:
            kw_str = ", ".join(keywords) if keywords else "none"
            print(f"  [{i+1}/{len(deals)}] Score: {result['score']:>3}/100  ${deal.price:>10,.0f}  Built {deal.year_built or '?':>5}  Keywords: {kw_str}")
            print(f"         {deal.address}, {deal.city} {deal.zip_code}")
            print(f"         {result['reasoning'][:120]}")
            print()

    session.commit()
    session.close()

    print(f"Done! Re-scored {len(deals)} deals.")


if __name__ == "__main__":
    test_mode = len(sys.argv) > 1 and sys.argv[1] == "test"
    rescore_deals(test_mode)
