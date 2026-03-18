"""
fix_db.py — Clean database: delete all, load cache, remove junk, dedupe, save.
"""

from dotenv import load_dotenv
load_dotenv()

import json
import re
from database import init_db, get_session, Deal, PipelineRun, save_deals

# Address patterns to EXCLUDE
BAD_ADDRESS_RE = re.compile(
    r'(\bspace\b|\bspc\b|\btrailer\b|\blot\b|\bunit\b|\bapt\b|#'
    r'|\bplan\b|\bresidence\b|\bat the\b|\bcollection\b|\breserve\b'
    r'|\bpaseo\b|\bvista\b|\bpointe\b'
    r'|\bflora\b|\barboretum\b|\bnewman\b|\bmeadow\w*\b.*\bplan\b)',
    re.IGNORECASE
)

# Home types to EXCLUDE
BAD_HOME_TYPES = ["mobile", "manufactured", "land", "lot", "lot_land"]

# Addresses starting with "0 " are vacant lots, "lot " are development lots
LOT_ADDRESS_RE = re.compile(r'^(0\s|lot\s)', re.IGNORECASE)

# New construction keywords in any field
NEW_CONSTRUCTION_KEYWORDS = [
    "new_construction", "contact builder", "builder",
]

MAX_YEAR_BUILT = 2019  # exclude 2020 and newer


def get_all_text(listing):
    """Get all searchable text from listing."""
    texts = []
    for key, val in listing.items():
        if key == "raw_data":
            continue
        if isinstance(val, str):
            texts.append(val)
    raw = listing.get("raw_data") or {}
    if isinstance(raw, dict):
        for key, val in raw.items():
            if isinstance(val, str):
                texts.append(val)
        hdp = raw.get("hdpData", {})
        if isinstance(hdp, dict):
            hi = hdp.get("homeInfo", {})
            if isinstance(hi, dict):
                for key, val in hi.items():
                    if isinstance(val, str):
                        texts.append(val)
                sub = hi.get("listing_sub_type", {})
                if isinstance(sub, dict):
                    for k, v in sub.items():
                        if v:
                            texts.append(k)
    return " ".join(texts).lower()


def is_new_construction(listing):
    """Check if listing is new construction."""
    all_text = get_all_text(listing)
    for kw in NEW_CONSTRUCTION_KEYWORDS:
        if kw.lower() in all_text:
            return True
    # Check year_built
    yb = listing.get("year_built")
    if yb:
        try:
            if int(yb) > MAX_YEAR_BUILT:
                return True
        except (ValueError, TypeError):
            pass
    # New listing + high price = likely new construction
    days = listing.get("days_on_zillow")
    price = listing.get("price") or 0
    if days is not None and days <= 1 and price > 400000 and not yb:
        return True
    if days is not None and days == 0 and price > 600000:
        return True
    return False


init_db()

# Step 1: Delete everything
session = get_session()
deleted = session.query(Deal).delete()
session.query(PipelineRun).delete()
session.commit()
session.close()
print(f"Deleted {deleted} deals from database")

# Step 2: Load cache
with open("scraped_cache.json", "r") as f:
    listings = json.load(f)
print(f"Loaded {len(listings)} from scraped_cache.json")

# Step 3: Remove bad addresses
clean = []
removed_address = 0
for l in listings:
    addr = l.get("address") or ""
    full = l.get("full_address") or ""
    if BAD_ADDRESS_RE.search(addr) or BAD_ADDRESS_RE.search(full):
        removed_address += 1
        continue
    if LOT_ADDRESS_RE.search(addr):
        removed_address += 1
        continue
    clean.append(l)
print(f"Removed {removed_address} bad addresses")

# Step 4: Remove bad home types
clean2 = []
removed_type = 0
for l in clean:
    ht = (l.get("home_type") or "").lower()
    if any(bad in ht for bad in BAD_HOME_TYPES):
        removed_type += 1
        continue
    clean2.append(l)
print(f"Removed {removed_type} bad home types")

# Step 5: Remove new construction
clean3 = []
removed_new = 0
removed_new_examples = []
for l in clean2:
    if is_new_construction(l):
        removed_new += 1
        if len(removed_new_examples) < 10:
            yb = l.get("year_built", "?")
            removed_new_examples.append(f"{l.get('address', '?')} (built {yb})")
        continue
    clean3.append(l)
print(f"Removed {removed_new} new construction / builder listings")
if removed_new_examples:
    for ex in removed_new_examples:
        print(f"  - {ex}")

# Step 6: Dedupe by address
seen = set()
unique = []
dupes = 0
for l in clean3:
    key = (l.get("address") or "").strip().lower()
    if not key:
        dupes += 1
        continue
    if key in seen:
        dupes += 1
        continue
    seen.add(key)
    unique.append(l)
print(f"Removed {dupes} duplicates")

# Step 7: Save to database
print(f"\nSaving {len(unique)} clean deals to database...")
save_deals(unique)

# Verify
session = get_session()
final = session.query(Deal).count()
session.close()
print(f"\nDONE. Database now has {final} deals.")
