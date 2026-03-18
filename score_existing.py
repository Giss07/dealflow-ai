"""
Score and calculate offers for existing DB deals that are missing values.
No scraping. No filtering. No Apify.
"""

from dotenv import load_dotenv
load_dotenv()

import json
import logging
from database import init_db, get_session, Deal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

init_db()
session = get_session()

total = session.query(Deal).count()
needs_score = session.query(Deal).filter((Deal.score == None) | (Deal.score == 0)).count()
needs_arv = session.query(Deal).filter(Deal.arv == None).count()
needs_repairs = session.query(Deal).filter(Deal.repairs_mid == None).count()
needs_offer = session.query(Deal).filter(Deal.max_offer == None).count()

print(f"Total deals: {total}")
print(f"Needs scoring: {needs_score}")
print(f"Needs ARV: {needs_arv}")
print(f"Needs repairs: {needs_repairs}")
print(f"Needs offer calc: {needs_offer}")
print()

# --- SCORE deals missing scores ---
from scorer import score_deals
from database import deal_to_dict

deals_to_score = session.query(Deal).filter((Deal.score == None) | (Deal.score == 0)).all()
if deals_to_score:
    logger.info(f"[1/4] Scoring {len(deals_to_score)} deals...")
    listings = [deal_to_dict(d) for d in deals_to_score]
    for l in listings:
        if not l.get("photo_grades"):
            l["photo_grades"] = {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
            l["overall_condition"] = "Unknown"
    scored = score_deals(listings)
    for listing in scored:
        deal = session.query(Deal).filter_by(id=listing["id"]).first()
        if deal:
            deal.score = listing.get("score")
            deal.score_reasoning = listing.get("score_reasoning")
    session.commit()
    logger.info(f"  Scored {len(scored)} deals")
else:
    logger.info("[1/4] All deals already scored")

# --- ARV for deals missing ARV ---
from arv_calculator import compute_arv_for_listing

deals_no_arv = session.query(Deal).filter(Deal.arv == None).all()
if deals_no_arv:
    logger.info(f"[2/4] Computing ARV for {len(deals_no_arv)} deals...")
    computed = 0
    for i, deal in enumerate(deals_no_arv):
        listing = deal_to_dict(deal)
        if i % 50 == 0:
            logger.info(f"  ARV {i+1}/{len(deals_no_arv)}: {deal.address}")
        compute_arv_for_listing(listing)
        if listing.get("arv"):
            deal.arv = listing["arv"]
            deal.comp_count = listing.get("comp_count")
            deal.comps = json.dumps(listing.get("comps")) if listing.get("comps") else None
            deal.privy_url = listing.get("privy_url")
            computed += 1
    session.commit()
    logger.info(f"  Computed ARV for {computed}/{len(deals_no_arv)} deals")
else:
    logger.info("[2/4] All deals already have ARV")

# --- Repairs for deals missing repair estimates ---
from repair_estimator import estimate_repairs

deals_no_repairs = session.query(Deal).filter(Deal.repairs_mid == None).all()
if deals_no_repairs:
    logger.info(f"[3/4] Estimating repairs for {len(deals_no_repairs)} deals...")
    for deal in deals_no_repairs:
        listing = deal_to_dict(deal)
        if not listing.get("photo_grades"):
            listing["photo_grades"] = {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
        estimate_repairs(listing)
        repair_est = listing.get("repair_estimate", {})
        deal.repairs_mid = repair_est.get("total_mid")
        deal.repairs_worst = repair_est.get("total_worst")
        deal.repair_breakdown = json.dumps(repair_est.get("breakdown")) if repair_est.get("breakdown") else None
    session.commit()
    logger.info(f"  Estimated repairs for {len(deals_no_repairs)} deals")
else:
    logger.info("[3/4] All deals already have repair estimates")

# --- Offers for deals missing max_offer (requires ARV) ---
from offer_calculator import calculate_offer

deals_no_offer = session.query(Deal).filter(Deal.max_offer == None, Deal.arv != None).all()
if deals_no_offer:
    logger.info(f"[4/4] Calculating offers for {len(deals_no_offer)} deals...")
    for deal in deals_no_offer:
        listing = deal_to_dict(deal)
        listing["arv"] = deal.arv
        listing["repair_estimate"] = {
            "total_mid": deal.repairs_mid or 0,
            "total_worst": deal.repairs_worst or 0,
        }
        calculate_offer(listing)
        offer = listing.get("offer_analysis", {})
        if "error" not in offer:
            deal.max_offer = offer.get("max_offer")
            deal.max_offer_worst = offer.get("max_offer_worst")
            deal.estimated_profit = offer.get("estimated_profit")
            deal.roi_pct = offer.get("roi_pct")
            deal.offer_analysis = json.dumps(offer)
    session.commit()
    logger.info(f"  Calculated offers for {len(deals_no_offer)} deals")
else:
    logger.info("[4/4] All deals with ARV already have offers")

session.close()

# Final stats
session = get_session()
print(f"\n{'='*50}")
print(f"DONE")
print(f"  Total: {session.query(Deal).count()}")
print(f"  Scored: {session.query(Deal).filter(Deal.score != None).count()}")
print(f"  With ARV: {session.query(Deal).filter(Deal.arv != None).count()}")
print(f"  With repairs: {session.query(Deal).filter(Deal.repairs_mid != None).count()}")
print(f"  With offers: {session.query(Deal).filter(Deal.max_offer != None).count()}")
session.close()
