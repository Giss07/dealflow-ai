"""
DealFlow DB Cleaner — Remove duplicates, mobile homes, then re-score and recalculate.
Usage: python3 clean_db.py
"""

from dotenv import load_dotenv
load_dotenv()

import json
import os
import re
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Address patterns that indicate mobile homes / manufactured / lots
MOBILE_HOME_ADDRESS_PATTERNS = [
    r'\bspace\b', r'\bspc\b', r'\bspc\.', r'\bsp\s*#',
    r'\btrailer\b',
    r'\blot\b', r'\blot\s*#',
    r'\bunit\b', r'\bunit\s*#',
]
MOBILE_HOME_ADDRESS_RE = re.compile('|'.join(MOBILE_HOME_ADDRESS_PATTERNS), re.IGNORECASE)

# Home type exclusions
EXCLUDE_HOME_TYPES = ["MANUFACTURED", "MOBILE", "LOT", "LAND"]


def load_data():
    """Load deals from DB or cache file."""
    from database import init_db, get_session, Deal, deal_to_dict
    init_db()
    session = get_session()
    count = session.query(Deal).count()
    session.close()

    if count > 0:
        logger.info(f"Loading {count} deals from database")
        session = get_session()
        deals = session.query(Deal).all()
        listings = [deal_to_dict(d) for d in deals]
        session.close()
        return listings

    cache_path = os.path.join(os.path.dirname(__file__), "scraped_cache.json")
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            listings = json.load(f)
        logger.info(f"Loading {len(listings)} deals from cache file")
        return listings

    logger.error("No data in DB or cache file")
    return []


def remove_duplicates(listings):
    """Remove duplicate listings by zpid, then by address+zip."""
    seen_zpids = set()
    seen_addresses = set()
    unique = []
    dupes = 0

    for listing in listings:
        zpid = listing.get("zpid")
        if zpid and zpid != "None" and zpid in seen_zpids:
            dupes += 1
            continue
        if zpid and zpid != "None":
            seen_zpids.add(zpid)

        addr_key = (
            (listing.get("address") or "").strip().lower(),
            (listing.get("zip_code") or "").strip(),
        )
        if addr_key[0] and addr_key in seen_addresses:
            dupes += 1
            continue
        if addr_key[0]:
            seen_addresses.add(addr_key)

        unique.append(listing)

    logger.info(f"Duplicates removed: {dupes} ({len(listings)} → {len(unique)})")
    return unique


def is_mobile_home(listing):
    """Check if listing is a mobile/manufactured home by address or type."""
    address = listing.get("address") or ""
    full_address = listing.get("full_address") or ""

    # Check address patterns
    if MOBILE_HOME_ADDRESS_RE.search(address) or MOBILE_HOME_ADDRESS_RE.search(full_address):
        return True

    # Check home type
    home_type = (listing.get("home_type") or "").upper()
    for excluded in EXCLUDE_HOME_TYPES:
        if excluded in home_type:
            return True

    # Check description for mobile home indicators
    desc = (listing.get("description") or "").lower()
    for kw in ["mobile home", "manufactured home", "trailer park", "space rent",
               "land lease", "lot rent", "pad rent", "mobile/manufactured"]:
        if kw in desc:
            return True

    return False


def remove_mobile_homes(listings):
    """Remove mobile homes, manufactured homes, trailers, lots."""
    clean = []
    removed = 0
    removed_examples = []

    for listing in listings:
        if is_mobile_home(listing):
            removed += 1
            if len(removed_examples) < 10:
                removed_examples.append(listing.get("address", "?"))
            continue
        clean.append(listing)

    logger.info(f"Mobile homes removed: {removed} ({len(listings)} → {len(clean)})")
    if removed_examples:
        logger.info(f"  Examples: {removed_examples[:5]}")
    return clean


def run_clean():
    """Full clean pipeline."""
    from database import init_db, save_deals, get_session, Deal, PipelineRun
    from scorer import score_deals
    from repair_estimator import estimate_all_repairs
    from offer_calculator import calculate_all_offers
    from alerts import send_alerts

    started_at = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("DealFlow DB CLEAN — Remove dupes & mobile homes, re-score, recalculate")
    logger.info("=" * 60)

    # Load data
    listings = load_data()
    if not listings:
        return

    original_count = len(listings)

    # Step 1: Remove duplicates
    logger.info("[1/5] Removing duplicates...")
    listings = remove_duplicates(listings)

    # Step 2: Remove mobile homes
    logger.info("[2/5] Removing mobile homes / manufactured / trailers...")
    listings = remove_mobile_homes(listings)

    logger.info(f"Clean dataset: {len(listings)} deals (removed {original_count - len(listings)} total)")

    # Clear DB and save clean data
    logger.info("Clearing database...")
    init_db()
    session = get_session()
    session.query(Deal).delete()
    session.query(PipelineRun).delete()
    session.commit()
    session.close()

    # Set default photo grades
    for listing in listings:
        if not listing.get("photo_grades") or all(v == "Unknown" for v in (listing.get("photo_grades") or {}).values()):
            listing["photo_grades"] = {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
            listing["overall_condition"] = "Unknown"

    # Step 3: Re-score with Claude AI
    logger.info(f"[3/5] Re-scoring {len(listings)} deals with Claude AI...")
    scored = score_deals(listings)

    # Step 4: Re-estimate repairs
    logger.info("[4/5] Estimating repair costs...")
    with_repairs = estimate_all_repairs(scored)

    # Step 5: Recalculate offers
    logger.info("[5/5] Calculating max offers...")
    with_offers = calculate_all_offers(with_repairs)

    # Send alerts
    logger.info("Checking for alerts...")
    alerts_sent = send_alerts(with_offers)

    # Save to clean DB
    logger.info("Saving clean deals to database...")
    save_deals(with_offers)

    logger.info("=" * 60)
    logger.info(f"DB Clean Complete!")
    logger.info(f"  Original: {original_count}")
    logger.info(f"  After cleaning: {len(with_offers)}")
    logger.info(f"  Removed: {original_count - len(with_offers)}")
    logger.info(f"  Alerts Sent: {alerts_sent}")
    top5 = sorted(with_offers, key=lambda x: x.get("score", 0), reverse=True)[:5]
    logger.info(f"  Top 5 scores:")
    for d in top5:
        logger.info(f"    {d.get('score', '?')}/100 — {d.get('address', '?')} — ${d.get('price', 0):,.0f}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_clean()
