"""
DealFlow Scraper — Pulls Zillow listings via Apify API for 74 IE zip codes.
Uses maxcopell/zillow-zip-search actor for search results,
and maxcopell/zillow-detail-scraper for full property details.
"""

import os
import time
import json
import logging
import requests

logger = logging.getLogger(__name__)

APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
SEARCH_ACTOR = "maxcopell~zillow-zip-search"
DETAIL_ACTOR = "maxcopell~zillow-detail-scraper"

IE_ZIP_CODES = [
    "91701","91708","91709","91710","91730","91737","91739","91743","91750","91752",
    "91761","91762","91763","91764","91766","91767","91768","91784","91786",
    "92223","92316","92318","92320","92324","92335","92336","92337","92345","92346",
    "92350","92354","92357","92358","92359","92373","92374","92376","92377","92382",
    "92392","92394","92395","92399","92401","92404","92405","92407","92408","92410",
    "92411","92501","92503","92504","92505","92506","92507","92508","92509","92530",
    "92532","92543","92544","92545","92548","92549","92551","92552","92553","92555",
    "92557","92562","92563","92567","92571",
]

BATCH_SIZE = 5  # zip codes per Apify run


def run_zip_search(zip_codes, max_items=100):
    """Run the Zillow ZIP Code Search actor."""
    api_url = f"https://api.apify.com/v2/acts/{SEARCH_ACTOR}/run-sync-get-dataset-items"
    params = {"token": APIFY_API_KEY}
    payload = {
        "zipCodes": zip_codes,
        "maxItems": max_items,
    }

    logger.info(f"Searching {len(zip_codes)} zip codes via Apify...")
    try:
        resp = requests.post(api_url, params=params, json=payload,
                             headers={"Content-Type": "application/json"}, timeout=300)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"Apify search failed: {e}")
        # Try to get error details
        try:
            logger.error(f"Response: {resp.text[:500]}")
        except Exception:
            pass
        return []


def scrape_listings():
    """Scrape all IE zip codes and return raw listing data."""
    all_listings = []

    for i in range(0, len(IE_ZIP_CODES), BATCH_SIZE):
        batch = IE_ZIP_CODES[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(IE_ZIP_CODES) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"Processing batch {batch_num}/{total_batches}: zips {batch}")

        results = run_zip_search(batch)
        if results:
            all_listings.extend(results)
            logger.info(f"Got {len(results)} listings from batch {batch_num}")
        else:
            logger.warning(f"No results from batch {batch_num}")

        # Rate limiting between batches
        if i + BATCH_SIZE < len(IE_ZIP_CODES):
            time.sleep(3)

    logger.info(f"Total raw listings scraped: {len(all_listings)}")
    return all_listings


def normalize_listing(raw):
    """Normalize a raw Apify zip-search listing into a standard dict."""
    # The zip search actor returns hdpData.homeInfo with detailed fields
    home_info = raw.get("hdpData", {}).get("homeInfo", {})
    lat_long = raw.get("latLong", {})

    # Build photo URLs from carousel data
    photos = []
    carousel = raw.get("carouselPhotosComposable", {})
    base_url = carousel.get("baseUrl", "")
    for photo in carousel.get("photoData", []):
        key = photo.get("photoKey", "")
        if key and base_url:
            photos.append(base_url.replace("{photoKey}", key))

    # Also check imgSrc as fallback
    if not photos and raw.get("imgSrc"):
        photos.append(raw["imgSrc"])

    price = home_info.get("price") or raw.get("unformattedPrice")
    if isinstance(price, str):
        price = int(price.replace("$", "").replace(",", "").strip()) if price.replace("$","").replace(",","").strip().isdigit() else None

    return {
        "zpid": str(home_info.get("zpid") or raw.get("zpid", "")),
        "address": raw.get("addressStreet") or home_info.get("streetAddress", ""),
        "full_address": raw.get("address", ""),
        "city": raw.get("addressCity") or home_info.get("city", ""),
        "state": raw.get("addressState") or home_info.get("state", "CA"),
        "zip_code": str(raw.get("addressZipcode") or home_info.get("zipcode", "")),
        "price": price,
        "bedrooms": home_info.get("bedrooms") or raw.get("beds"),
        "bathrooms": home_info.get("bathrooms") or raw.get("baths"),
        "sqft": home_info.get("livingArea") or raw.get("area"),
        "lot_sqft": home_info.get("lotAreaValue"),
        "year_built": home_info.get("yearBuilt"),
        "description": raw.get("description", ""),
        "home_type": home_info.get("homeType", ""),
        "listing_url": "https://www.zillow.com" + raw.get("detailUrl", "") if raw.get("detailUrl", "").startswith("/") else raw.get("detailUrl", ""),
        "photos": photos,
        "latitude": lat_long.get("latitude") or home_info.get("latitude"),
        "longitude": lat_long.get("longitude") or home_info.get("longitude"),
        "days_on_zillow": home_info.get("daysOnZillow"),
        "status": home_info.get("homeStatus") or raw.get("statusType", ""),
        "zestimate": home_info.get("zestimate") or raw.get("zestimate"),
        "broker": raw.get("brokerName", ""),
        "raw_data": raw,
    }


def fetch_details_batch(urls, batch_size=25):
    """Fetch full property details for a batch of URLs in a single Apify run."""
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
        logger.warning(f"Detail batch fetch failed: {e}")
        return []


def enrich_with_details(listings):
    """Fetch full details for ALL listings to get descriptions. Batches URLs for efficiency."""
    # Collect listings that need descriptions
    needs_detail = []
    for listing in listings:
        if not listing.get("description") and listing.get("listing_url"):
            needs_detail.append(listing)

    if not needs_detail:
        logger.info("All listings already have descriptions")
        return listings

    logger.info(f"Fetching descriptions for {len(needs_detail)} listings via detail scraper...")

    # Build URL → listing index map
    url_to_listings = {}
    for listing in needs_detail:
        url = listing["listing_url"]
        if url not in url_to_listings:
            url_to_listings[url] = []
        url_to_listings[url].append(listing)

    urls = list(url_to_listings.keys())
    enriched = 0
    batch_size = 25

    for i in range(0, len(urls), batch_size):
        batch_urls = urls[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(urls) + batch_size - 1) // batch_size
        logger.info(f"Detail batch {batch_num}/{total_batches} ({len(batch_urls)} URLs)")

        results = fetch_details_batch(batch_urls)
        if not results:
            logger.warning(f"No results from detail batch {batch_num}, Apify limit may be hit")
            break

        # Match results back to listings by URL or address
        for detail in results:
            detail_url = detail.get("url") or ""
            detail_addr = (detail.get("streetAddress") or detail.get("address") or "").lower()

            matched = False
            # Try matching by URL
            for url, listing_list in url_to_listings.items():
                if detail_url and url in detail_url or detail_url in url:
                    for listing in listing_list:
                        listing["description"] = detail.get("description", "")
                        listing["year_built"] = listing.get("year_built") or detail.get("yearBuilt")
                        detail_photos = detail.get("photos") or detail.get("images") or []
                        if detail_photos and not listing.get("photos"):
                            listing["photos"] = [p.get("url", p) if isinstance(p, dict) else p for p in detail_photos]
                        enriched += 1
                    matched = True
                    break

            # Fallback: match by address
            if not matched and detail_addr:
                for listing in needs_detail:
                    if detail_addr in (listing.get("address") or "").lower():
                        listing["description"] = detail.get("description", "")
                        listing["year_built"] = listing.get("year_built") or detail.get("yearBuilt")
                        enriched += 1
                        break

        # Rate limit between batches
        if i + batch_size < len(urls):
            time.sleep(3)

    logger.info(f"Enriched {enriched}/{len(needs_detail)} listings with full descriptions")
    return listings


def scrape_and_normalize():
    """Full scrape pipeline: search → normalize → enrich with details."""
    raw_listings = scrape_listings()
    normalized = []
    seen_zpids = set()

    for raw in raw_listings:
        try:
            listing = normalize_listing(raw)
            zpid = listing.get("zpid")
            if zpid and zpid in seen_zpids:
                continue
            if zpid:
                seen_zpids.add(zpid)
            if listing["price"] and listing["address"]:
                normalized.append(listing)
        except Exception as e:
            logger.warning(f"Failed to normalize listing: {e}")

    logger.info(f"Normalized {len(normalized)} unique listings")

    # Enrich with details to get descriptions for filtering
    normalized = enrich_with_details(normalized)

    return normalized


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    listings = scrape_and_normalize()
    print(f"\nScraped {len(listings)} listings")
    if listings:
        sample = listings[0]
        print(f"\nSample: {sample['address']}, {sample['city']} {sample['zip_code']}")
        print(f"  Price: ${sample['price']:,}")
        print(f"  Beds/Baths: {sample['bedrooms']}/{sample['bathrooms']}")
        print(f"  Sqft: {sample['sqft']}")
        print(f"  Photos: {len(sample.get('photos', []))}")
        print(f"  URL: {sample['listing_url']}")
