"""
Fetch full listing descriptions for DB deals missing them.
Uses Apify zillow-detail-scraper in batches.
Then re-filters to remove newly-discovered renovated properties.
"""

from dotenv import load_dotenv
load_dotenv()

import json
import logging
import time
import requests
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
DETAIL_ACTOR = "maxcopell~zillow-detail-scraper"
BATCH_SIZE = 25


def fetch_batch(urls):
    """Fetch details for a batch of URLs."""
    api_url = f"https://api.apify.com/v2/acts/{DETAIL_ACTOR}/run-sync-get-dataset-items"
    params = {"token": APIFY_API_KEY}
    payload = {
        "startUrls": [{"url": u} for u in urls],
        "maxItems": len(urls),
    }
    try:
        resp = requests.post(api_url, params=params, json=payload,
                             headers={"Content-Type": "application/json"}, timeout=300)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Batch fetch failed: {e}")
        try:
            logger.error(f"Response: {resp.text[:300]}")
        except Exception:
            pass
        return None  # None = error, [] = empty


from database import init_db, get_session, Deal
from filter import EXCLUDE_KEYWORDS, get_all_searchable_text

init_db()
session = get_session()

# Find deals without descriptions
all_deals = session.query(Deal).all()
needs_desc = [d for d in all_deals if not d.description and d.listing_url]
has_desc = len(all_deals) - len(needs_desc)

print(f"Total deals: {len(all_deals)}")
print(f"Already have descriptions: {has_desc}")
print(f"Need descriptions: {len(needs_desc)}")

if not needs_desc:
    print("All deals already have descriptions!")
    session.close()
    exit()

# Fetch in batches
enriched = 0
failed_batches = 0

for i in range(0, len(needs_desc), BATCH_SIZE):
    batch = needs_desc[i:i + BATCH_SIZE]
    batch_num = (i // BATCH_SIZE) + 1
    total_batches = (len(needs_desc) + BATCH_SIZE - 1) // BATCH_SIZE

    urls = [d.listing_url for d in batch]
    logger.info(f"Batch {batch_num}/{total_batches}: fetching {len(urls)} descriptions")

    results = fetch_batch(urls)

    if results is None:
        # API error (likely rate limit)
        failed_batches += 1
        if failed_batches >= 3:
            logger.error("3 consecutive failures, stopping (Apify limit likely hit)")
            break
        continue

    failed_batches = 0  # reset on success

    # Match results back to deals by URL
    result_by_url = {}
    for r in results:
        r_url = r.get("url", "")
        result_by_url[r_url] = r
        # Also index by zpid from URL
        for part in r_url.split("/"):
            if part.endswith("_zpid"):
                result_by_url[part.replace("_zpid", "")] = r

    for deal in batch:
        matched = None
        # Try exact URL match
        for r_url, r in result_by_url.items():
            if r_url and deal.listing_url and (r_url in deal.listing_url or deal.listing_url in r_url):
                matched = r
                break
        # Try zpid match
        if not matched and deal.zpid:
            matched = result_by_url.get(deal.zpid)

        if matched:
            desc = matched.get("description", "")
            if desc:
                deal.description = desc
                deal.year_built = deal.year_built or matched.get("yearBuilt")
                enriched += 1

    session.commit()

    if i + BATCH_SIZE < len(needs_desc):
        time.sleep(3)

print(f"\nEnriched {enriched}/{len(needs_desc)} deals with descriptions")

# Now check for exclude keywords in newly fetched descriptions
print("\nChecking for renovated/exclude keywords in new descriptions...")
removed = 0
removed_examples = []

for deal in all_deals:
    if not deal.description:
        continue
    desc_lower = deal.description.lower()
    for kw in EXCLUDE_KEYWORDS:
        if kw in desc_lower:
            if len(removed_examples) < 10:
                removed_examples.append(f"{deal.address}: '{kw}'")
            session.delete(deal)
            removed += 1
            break

session.commit()
remaining = session.query(Deal).count()
session.close()

print(f"Removed {removed} renovated/excluded deals based on descriptions")
if removed_examples:
    for ex in removed_examples:
        print(f"  - {ex}")
print(f"\nDatabase now has {remaining} deals.")
