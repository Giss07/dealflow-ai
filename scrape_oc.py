"""
Scrape distressed single-family homes in Orange County CA.
Usage: python3 scrape_oc.py
"""

from dotenv import load_dotenv
load_dotenv()

import os, sys, time, re, json, logging
import requests as req

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
API_URL = "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items"

OC_ZIPS = [
    "92701", "92703", "92704", "92707", "90680",
    "92843", "92841", "92840", "92804", "92805",
    "92627", "92647", "92833",
]

INCLUDE_KEYWORDS = [
    "as-is", "as is", "fixer", "cash only", "estate sale",
    "investor special", "tlc", "needs work", "handyman",
    "bring all offers", "motivated seller", "rehab",
]

EXCLUDE_KEYWORDS = [
    "new construction", "newly built", "builder", "brand new",
    "move-in ready", "move in ready", "turnkey", "remodeled",
    "renovated", "fully updated", "new build",
]


def scrape():
    if not APIFY_API_KEY:
        logger.error("APIFY_API_KEY not set")
        return

    logger.info(f"Scraping {len(OC_ZIPS)} Orange County zip codes for distressed SFR...")

    all_listings = []
    for i, zip_code in enumerate(OC_ZIPS):
        logger.info(f"  [{i+1}/{len(OC_ZIPS)}] Zip {zip_code}")
        try:
            resp = req.post(API_URL, params={"token": APIFY_API_KEY},
                            json={"zipCodes": [zip_code], "maxItems": 200},
                            headers={"Content-Type": "application/json"}, timeout=300)
            if resp.status_code not in (200, 201):
                logger.error(f"    Apify {resp.status_code}: {resp.text[:100]}")
                continue
            results = resp.json()
            logger.info(f"    Got {len(results)} listings")
            all_listings.extend([(zip_code, r) for r in results])
            time.sleep(3)
        except Exception as e:
            logger.error(f"    Error: {e}")

    logger.info(f"\nTotal raw listings: {len(all_listings)}")

    # Apply filters
    filtered = []
    for zip_code, item in all_listings:
        hi = item.get("hdpData", {}).get("homeInfo", {})
        sub = hi.get("listing_sub_type", {}) or {}
        status_text = (item.get("statusText") or "").lower()

        # Must be for sale
        home_status = (hi.get("homeStatus") or "").upper()
        if "FOR_SALE" not in home_status:
            continue

        # Skip auction
        if "auction" in status_text or sub.get("is_forAuction"):
            continue

        # Single family only
        home_type = (hi.get("homeType") or "").upper()
        if home_type and "SINGLE_FAMILY" not in home_type and "HOUSE" not in home_type:
            continue

        # Price range $500k-$900k
        price = hi.get("price") or item.get("unformattedPrice") or 0
        try:
            price = float(str(price).replace("$", "").replace(",", ""))
        except:
            continue
        if price < 500000 or price > 900000:
            continue

        # Days on market 45+
        days = hi.get("daysOnZillow")
        if days is not None and days < 45:
            continue

        # Year built 1950-1989
        year = hi.get("yearBuilt")
        if year:
            if year > 1989 or year < 1950:
                continue

        address = item.get("addressStreet") or hi.get("streetAddress") or ""
        city = item.get("addressCity") or hi.get("city") or ""
        sqft = hi.get("livingArea") or item.get("area") or 0
        beds = hi.get("bedrooms") or item.get("beds")
        baths = hi.get("bathrooms") or item.get("baths")
        zestimate = hi.get("zestimate") or item.get("zestimate")
        description = (item.get("description") or "").lower()

        # Exclude keywords
        excluded = False
        for kw in EXCLUDE_KEYWORDS:
            if kw in description or kw in status_text:
                excluded = True
                break
        if excluded:
            continue

        # Check include keywords
        matched_kw = [kw for kw in INCLUDE_KEYWORDS if kw in description]

        filtered.append({
            "address": address,
            "city": city,
            "state": "CA",
            "zip_code": zip_code,
            "price": price,
            "bedrooms": beds,
            "bathrooms": baths,
            "sqft": sqft,
            "year_built": year,
            "days_on_zillow": days,
            "zestimate": zestimate,
            "home_type": "SINGLE_FAMILY",
            "description": description[:200],
            "matched_keywords": matched_kw,
            "has_keywords": len(matched_kw) > 0,
            "listing_url": "https://www.zillow.com" + item.get("detailUrl", "") if item.get("detailUrl", "").startswith("/") else item.get("detailUrl", ""),
            "latitude": (item.get("latLong") or {}).get("latitude"),
            "longitude": (item.get("latLong") or {}).get("longitude"),
        })

    logger.info(f"Filtered: {len(filtered)} distressed SFR listings")

    # Sort by keywords + days on market
    filtered.sort(key=lambda x: (-len(x.get("matched_keywords", [])), -(x.get("days_on_zillow") or 0)))

    # Show results
    print(f"\n{'='*80}")
    print(f"ORANGE COUNTY DISTRESSED SFR — {len(filtered)} properties found")
    print(f"{'='*80}")
    for j, p in enumerate(filtered):
        kw = ", ".join(p["matched_keywords"]) if p["matched_keywords"] else "none"
        print(f"\n{j+1}. {p['address']}, {p['city']} {p['zip_code']}")
        print(f"   ${p['price']:,.0f} | {p['bedrooms'] or '?'}bd/{p['bathrooms'] or '?'}ba | {p['sqft'] or '?'}sqft | Built {p['year_built'] or '?'} | {p['days_on_zillow'] or '?'} days")
        print(f"   Keywords: {kw}")

    # Save to database
    print(f"\nSaving to database...")
    from database import init_db, get_session, Deal
    from arv_calculator import build_privy_url
    from scorer import fallback_score

    init_db()
    db = get_session()
    saved = 0
    for p in filtered:
        existing = db.query(Deal).filter(Deal.address == p["address"], Deal.zip_code == p["zip_code"]).first()
        if existing:
            continue

        deal = Deal()
        deal.address = p["address"]
        deal.city = p["city"]
        deal.state = "CA"
        deal.zip_code = p["zip_code"]
        deal.price = p["price"]
        deal.bedrooms = p["bedrooms"]
        deal.bathrooms = p["bathrooms"]
        deal.sqft = p["sqft"]
        deal.year_built = p["year_built"]
        deal.days_on_zillow = p["days_on_zillow"]
        deal.home_type = "SINGLE_FAMILY"
        deal.description = p["description"]
        deal.listing_url = p["listing_url"]
        deal.latitude = p["latitude"]
        deal.longitude = p["longitude"]
        deal.source = "zillow"
        deal.has_deal_keywords = p["has_keywords"]
        deal.matched_keywords = json.dumps(p["matched_keywords"]) if p["matched_keywords"] else None

        # ARV
        zest = p.get("zestimate")
        deal.arv = float(zest) if zest else round(p["price"] * 1.25)

        # Privy URL
        deal.privy_url = build_privy_url({"address": p["address"], "city": p["city"], "state": "CA", "zip_code": p["zip_code"]})

        # Score
        result = fallback_score({
            "price": p["price"], "arv": deal.arv, "sqft": p["sqft"],
            "days_on_zillow": p["days_on_zillow"], "year_built": p["year_built"],
            "repairs_mid": 0, "repairs_worst": 0,
            "has_deal_keywords": p["has_keywords"], "matched_keywords": p["matched_keywords"],
            "photo_grades": {}, "description": p["description"],
        })
        deal.score = result["score"]
        deal.score_reasoning = result["reasoning"]

        db.add(deal)
        saved += 1

    db.commit()
    db.close()
    print(f"Saved {saved} new deals (skipped {len(filtered)-saved} duplicates)")
    print("Done!")


if __name__ == "__main__":
    scrape()
