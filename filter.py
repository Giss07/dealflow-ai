"""
DealFlow Filter — Filters listings by price, age, keywords, and property type.
Checks ALL available Apify fields for exclude keywords.
"""

import logging

logger = logging.getLogger(__name__)

MAX_PRICE = 900_000
MAX_YEAR_BUILT = 2009  # built before 2010

INCLUDE_KEYWORDS = [
    "tlc", "needs work", "cash only", "as-is", "as is", "fixer", "rehab",
    "investor special", "motivated seller", "handyman special", "bring all offers",
]

EXCLUDE_KEYWORDS = [
    # Original excludes
    "new build", "new construction", "newly remodeled", "turnkey",
    "move in ready", "move-in ready", "fully renovated", "brand new",
    # Renovated / updated indicators
    "remodeled", "renovated", "updated", "fully updated", "completely updated",
    "beautifully remodeled", "recently remodeled", "recently renovated",
    "completely remodeled", "completely renovated", "totally remodeled",
    "just remodeled", "just renovated", "freshly remodeled", "freshly renovated",
    "new floors", "new flooring", "new kitchen", "new bath", "new bathroom",
    "new roof", "new hvac", "new a/c", "new ac unit", "new plumbing",
    "new windows", "new appliances", "new cabinets", "new countertops",
    "new granite", "new quartz",
    # Move-in ready variations
    "move-in-ready", "movein ready", "ready to move in", "nothing to do",
    "no work needed", "turn key", "turn-key",
]

EXCLUDE_PROPERTY_TYPES = [
    "mobile home", "manufactured home", "manufactured", "mobile/manufactured",
    "trailer", "trailer park", "mobile home park",
    "land lease", "space rent", "lot rent", "pad rent",
]

# New construction development address patterns to exclude
import re
NEW_CONSTRUCTION_RE = re.compile(
    r'(\bplan\s*[1-4]\b|\bresidence\s*(one|two|three|four)\b|^lot\s'
    r'|\bpaseo\b|\bvista\b|\bpointe\b)',
    re.IGNORECASE
)

NEW_CONSTRUCTION_KEYWORDS = [
    "new_construction", "contact builder", "builder",
]

EXCLUDE_HOME_TYPES = [
    "MANUFACTURED", "MOBILE", "LOT", "LAND",
]


def get_all_searchable_text(listing):
    """Extract ALL text fields from listing and raw Apify data for keyword matching."""
    texts = []

    # Direct listing fields
    for field in ["description", "status", "home_type", "broker"]:
        val = listing.get(field)
        if val and isinstance(val, str):
            texts.append(val)

    # Full address
    texts.append(listing.get("full_address", ""))

    # Raw Apify data — dig into all nested fields
    raw = listing.get("raw_data", {})
    if raw:
        # Top-level text fields
        for field in ["statusText", "statusType", "brokerName", "flexFieldText"]:
            val = raw.get(field)
            if val and isinstance(val, str):
                texts.append(val)

        # hdpData.homeInfo fields
        home_info = raw.get("hdpData", {}).get("homeInfo", {})
        for field in ["homeType", "homeStatus", "homeStatusForHDP", "description"]:
            val = home_info.get(field)
            if val and isinstance(val, str):
                texts.append(val)

        # listing_sub_type flags
        sub_type = home_info.get("listing_sub_type", {})
        if isinstance(sub_type, dict):
            for key, val in sub_type.items():
                if val:
                    texts.append(key)

        # variableData (agent remarks, listing remarks)
        var_data = raw.get("variableData", {})
        if isinstance(var_data, dict):
            for field in ["text", "type"]:
                val = var_data.get(field)
                if val and isinstance(val, str):
                    texts.append(val)

    return " ".join(texts).lower()


def passes_price_filter(listing):
    """Check if listing is under $900k."""
    price = listing.get("price")
    if price is None:
        return False
    try:
        price = float(price)
    except (ValueError, TypeError):
        return False
    return price <= MAX_PRICE


def passes_year_filter(listing):
    """Check if listing was built before 2010."""
    year = listing.get("year_built")
    if year is None:
        return True  # include if unknown
    try:
        year = int(year)
    except (ValueError, TypeError):
        return True
    return year <= MAX_YEAR_BUILT


def passes_property_type_filter(listing):
    """Exclude mobile homes, manufactured homes, land leases, etc."""
    # Check home_type field
    home_type = (listing.get("home_type") or "").upper()
    for excluded in EXCLUDE_HOME_TYPES:
        if excluded in home_type:
            return False

    # Check raw data homeType
    raw = listing.get("raw_data", {})
    raw_type = raw.get("hdpData", {}).get("homeInfo", {}).get("homeType", "").upper()
    for excluded in EXCLUDE_HOME_TYPES:
        if excluded in raw_type:
            return False

    # Check all text fields for property type keywords
    all_text = get_all_searchable_text(listing)
    for kw in EXCLUDE_PROPERTY_TYPES:
        if kw in all_text:
            return False

    # Check address for new construction development patterns
    address = listing.get("address") or ""
    if NEW_CONSTRUCTION_RE.search(address):
        return False

    # Check for new construction keywords in any field
    for kw in NEW_CONSTRUCTION_KEYWORDS:
        if kw.lower() in all_text:
            return False

    # Exclude year_built 2020 or newer
    yb = listing.get("year_built")
    if yb:
        try:
            if int(yb) > 2019:
                return False
        except (ValueError, TypeError):
            pass

    return True


def has_include_keyword(listing):
    """Check if any field contains include keywords (bonus signal)."""
    all_text = get_all_searchable_text(listing)
    for kw in INCLUDE_KEYWORDS:
        if kw in all_text:
            return True
    return False


def has_exclude_keyword(listing):
    """Check ALL available fields for exclude keywords."""
    all_text = get_all_searchable_text(listing)
    for kw in EXCLUDE_KEYWORDS:
        if kw in all_text:
            return True
    return False


def get_exclude_reason(listing):
    """Return the first exclude keyword found (for logging)."""
    all_text = get_all_searchable_text(listing)
    for kw in EXCLUDE_KEYWORDS:
        if kw in all_text:
            return f"exclude keyword: '{kw}'"
    for kw in EXCLUDE_PROPERTY_TYPES:
        if kw in all_text:
            return f"property type: '{kw}'"
    return None


def filter_listings(listings):
    """
    Apply all filters. Returns list of filtered listings with match info.
    Logic: Must pass price + year + property type filters, must NOT have exclude keywords.
    Include keywords are a bonus signal (stored but not required).
    """
    filtered = []
    excluded_counts = {"price": 0, "year": 0, "property_type": 0, "keywords": 0}

    for listing in listings:
        if not passes_price_filter(listing):
            excluded_counts["price"] += 1
            continue
        if not passes_year_filter(listing):
            excluded_counts["year"] += 1
            continue
        if not passes_property_type_filter(listing):
            excluded_counts["property_type"] += 1
            continue
        if has_exclude_keyword(listing):
            excluded_counts["keywords"] += 1
            continue

        listing["has_deal_keywords"] = has_include_keyword(listing)
        matched_kw = []
        all_text = get_all_searchable_text(listing)
        for kw in INCLUDE_KEYWORDS:
            if kw in all_text:
                matched_kw.append(kw)
        listing["matched_keywords"] = matched_kw

        filtered.append(listing)

    logger.info(f"Filtered {len(listings)} → {len(filtered)} listings")
    logger.info(f"  Excluded: price={excluded_counts['price']}, year={excluded_counts['year']}, "
                f"property_type={excluded_counts['property_type']}, keywords={excluded_counts['keywords']}")
    return filtered


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_listings = [
        {"price": 500000, "year_built": 1985, "description": "Great fixer upper, needs TLC", "address": "123 Main St"},
        {"price": 1200000, "year_built": 2000, "description": "Beautiful home", "address": "456 Oak Ave"},
        {"price": 400000, "year_built": 2022, "description": "Brand new construction", "address": "789 Pine Rd"},
        {"price": 600000, "year_built": 1990, "description": "Investor special, as-is", "address": "321 Elm St"},
        {"price": 350000, "year_built": 1975, "description": "Completely remodeled, new kitchen, new floors, turnkey!", "address": "555 Maple Dr"},
        {"price": 200000, "year_built": 1970, "description": "Mobile home in park, space rent $800/mo", "address": "100 Trailer Park Ln", "home_type": "MANUFACTURED"},
        {"price": 450000, "year_built": 1988, "description": "Updated kitchen and new bathrooms, move-in ready", "address": "777 Updated St"},
    ]
    results = filter_listings(test_listings)
    print(f"\nPassed: {len(results)}/{len(test_listings)}")
    for r in results:
        print(f"  ✅ {r['address']} - ${r['price']:,} - keywords: {r.get('matched_keywords', [])}")
