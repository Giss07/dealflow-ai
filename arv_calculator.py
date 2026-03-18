"""
DealFlow ARV Calculator — Uses Zestimate as ARV, falls back to price * 1.25.
No Apify needed.
"""

import re
import logging
from urllib.parse import quote, quote_plus

logger = logging.getLogger(__name__)


def build_privy_url(listing):
    """Build Privy URL that auto-triggers search."""
    address = listing.get("address") or ""
    city = listing.get("city") or ""
    state = listing.get("state") or "CA"

    search_text = quote_plus(f"{address}, {city}, {state}")

    return (
        f"https://app.privy.pro/dashboard?"
        f"update_history=true"
        f"&search_text={search_text}"
        f"&location_type=free_form"
        f"&include_detached=true"
        f"&include_active=true"
        f"&date_range=6_month"
        f"&spread_type=arv"
        f"&sort_by=days-on-market"
        f"&sort_dir=asc"
    )


def compute_arv_for_listing(listing):
    """Compute ARV from Zestimate or fallback to price * 1.25."""
    zestimate = listing.get("zestimate")
    price = listing.get("price") or 0

    if zestimate and float(zestimate) > 0:
        arv = round(float(zestimate))
        source = "Zestimate"
    elif price and float(price) > 0:
        arv = round(float(price) * 1.25)
        source = "price x 1.25"
    else:
        arv = None
        source = "none"

    listing["arv"] = arv
    listing["comps"] = []
    listing["comp_count"] = 0
    listing["privy_url"] = build_privy_url(listing)

    if arv:
        logger.info(f"ARV for {listing.get('address')}: ${arv:,} ({source})")
    else:
        logger.warning(f"No ARV for {listing.get('address')}")

    return listing


def compute_arv_for_all(listings):
    """Compute ARV for all listings."""
    for i, listing in enumerate(listings):
        compute_arv_for_listing(listing)
    zest_count = sum(1 for l in listings if l.get("zestimate") and float(l.get("zestimate", 0)) > 0)
    logger.info(f"ARV computed for {len(listings)} listings ({zest_count} from Zestimate, {len(listings) - zest_count} from price fallback)")
    return listings


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test = {
        "address": "13320 Winter Park St", "city": "Victorville", "state": "CA",
        "zip_code": "92394", "price": 450000, "zestimate": 520000,
    }
    result = compute_arv_for_listing(test)
    print(f"ARV: ${result.get('arv', 'N/A'):,}")
    print(f"Privy: {result.get('privy_url')}")
