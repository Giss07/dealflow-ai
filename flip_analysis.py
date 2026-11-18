"""
Per-listing flip analysis for search_distressed: sold comps, ARV, rehab, MAO.

    comps   SOLD in the last 6 months, within 0.5 mi, same property type,
            sqft within ±20%, beds within ±1. Pool and 55+ must match the
            subject; with fewer than 3 matching comps, the closest
            non-matching ones fill in to 3 and the row is flagged
            "POOL MISMATCH" / "55+ MISMATCH". Up to 5 comps, nearest first.
    ARV     median $/sqft × subject sqft, over the REMODELED comps when at
            least 2 are remodeled, otherwise over all comps (noted). Fewer
            than 3 comps behind the ARV is "LOW CONFIDENCE"; none means no ARV
            and no MAO.
    rehab   Light $30 / Medium $55 / Heavy $90 per sqft from the description,
            one tier up when built before 1960.
    MAO     flip_calculator.max_allowable_offer at 10% of ARV profit.

Pure functions over Zillow-shaped dicts. The data fetchers are passed in, so
this module never calls the network itself (and tests need no API key).
"""

import math
import re
import statistics
import time

from flip_calculator import max_allowable_offer

COMP_RADIUS_MI = 0.5
COMP_MAX_AGE_DAYS = 183           # 6 months
COMP_SQFT_TOLERANCE = 0.20
COMP_BEDS_TOLERANCE = 1
COMPS_MIN = 3
COMPS_MAX = 5
MAX_COMP_DETAIL_LOOKUPS = 12      # per subject: candidates checked for pool / 55+

REHAB_TIERS = {"light": 30, "medium": 55, "heavy": 90}   # $ per sqft
REHAB_OLD_HOME_YEAR = 1960        # built before this → one tier up

HEAVY_WORDS = [
    "tear-down", "teardown", "tear down", "scrape", "gut", "bring your contractor",
    "needs everything", "fire damage", "fire-damaged", "water damage", "flood damage",
    "foundation", "major repairs", "uninhabitable", "not livable", "lot value",
]
MEDIUM_WORDS = [
    "fixer", "needs work", "rehab", "handyman", "investor special",
    "deferred maintenance", "cash only", "needs repairs",
]
LIGHT_WORDS = [
    "tlc", "cosmetic fixer", "cosmetic", "needs paint", "needs flooring",
    "as-is", "as is", "motivated", "probate", "priced to sell",
]

SENIOR_PATTERNS = [
    r"\b55\s*\+", r"\b55\s*(?:and|&)\s*(?:over|older|up)\b", r"\bsenior (?:community|living|park)\b",
    r"\bage[- ]restricted\b", r"\bactive adult\b", r"\b62\s*\+",
]
POOL_PATTERN = r"\b(?:swimming\s+)?pool\b"
NOT_PRIVATE_POOL = r"\b(?:community|association|hoa|club ?house|shared|complex|park|no)\s+(?:swimming\s+)?pool\b"


# ── Feature detection ─────────────────────────────────────────────────

def _features(detail, key):
    vals = (detail.get("resoFacts") or {}).get(key) or []
    if isinstance(vals, str):
        vals = [vals]
    return [str(v).strip() for v in vals if str(v).strip().lower() not in ("", "none")]


def detect_pool(detail):
    """(has_private_pool, source). The listing's data fields decide when present;
    the description is used only when they are missing."""
    rf = detail.get("resoFacts") or {}
    if rf.get("hasPrivatePool") is True:
        return True, "data"
    private = [f for f in _features(detail, "poolFeatures")
               if not re.search(r"community|association|hoa", f, re.I)]
    if private:
        return True, "data"
    if rf.get("hasPrivatePool") is False or "poolFeatures" in rf:
        return False, "data"
    desc = (detail.get("description") or "").lower()
    if len(re.findall(POOL_PATTERN, desc)) > len(re.findall(NOT_PRIVATE_POOL, desc)):
        return True, "description"
    return False, "description"


def detect_senior(detail):
    """(is_55_plus, source). The listing's data fields decide when present;
    the description is used only when they are missing."""
    rf = detail.get("resoFacts") or {}
    if rf.get("isSeniorCommunity") is True:
        return True, "data"
    for f in _features(detail, "communityFeatures") + _features(detail, "seniorCommunityFeatures"):
        if re.search(r"55|senior|age restricted|age-restricted|active adult", f, re.I):
            return True, "data"
    if rf.get("isSeniorCommunity") is False:
        return False, "data"
    desc = (detail.get("description") or "").lower()
    if any(re.search(p, desc) for p in SENIOR_PATTERNS):
        return True, "description"
    return False, "description"


REMODEL_WORDS = [
    "remodeled", "remodelled", "newly remodeled", "remodel", "renovated", "renovation",
    "updated", "upgraded", "rehabbed", "new kitchen", "new bathroom", "new bathrooms",
    "new baths", "new flooring", "new floors", "turnkey", "turn-key",
]
NOT_REMODELED_WORDS = [
    "needs updating", "needs updates", "need updating", "needs to be updated",
    "could use updating", "could use some updating", "in need of updating",
    "not updated", "original condition", "original kitchen", "original bathroom",
    "fixer", "needs work", "tlc", "bring your contractor", "needs remodel",
    "needs renovation", "ready for your remodel", "ready for your renovation",
]


def _words_in(text, words):
    return [w for w in words if re.search(r"(?<![a-z])" + re.escape(w) + r"(?![a-z])", text)]


def detect_remodeled(detail):
    """(is_remodeled, source, evidence).

    The listing's condition field decides when it says something conclusive;
    otherwise the description and photo captions are searched. Not-remodeled
    wording ("needs updating", "fixer", "TLC") wins over remodel wording, so
    "remodeled in 1998, needs TLC" is NOT remodeled. Photo images themselves
    are not analysed — only their captions.
    """
    cond = str((detail.get("resoFacts") or {}).get("propertyCondition") or "").lower()
    if cond:
        if _words_in(cond, ["fixer", "needs work", "teardown", "tear down"]):
            return False, "data", f"condition: {cond}"
        if re.search(r"updated|remodel|renovat|turnkey|turn key|new construction", cond):
            return True, "data", f"condition: {cond}"
    captions = " ".join(str(p.get("caption") or "") for p in (detail.get("responsivePhotos") or [])
                        if isinstance(p, dict))
    text = f"{detail.get('description') or ''} {captions}".lower()
    against = _words_in(text, NOT_REMODELED_WORDS)
    if against:
        return False, "description", "not remodeled: " + ", ".join(against)
    found = _words_in(text, REMODEL_WORDS)
    if found:
        return True, "description", ", ".join(found)
    return False, "description", "no remodel wording"


# ── Rehab ─────────────────────────────────────────────────────────────

def estimate_rehab(description, year_built, sqft):
    """{'tier', 'per_sqft', 'amount', 'reason'}; amount is None without sqft."""
    text = (description or "").lower()
    order = ["light", "medium", "heavy"]
    def found(words):     # whole words: "gut" must not match "gutter"
        return [w for w in words if re.search(r"(?<![a-z])" + re.escape(w) + r"(?![a-z])", text)]
    hits = {"heavy": found(HEAVY_WORDS), "medium": found(MEDIUM_WORDS), "light": found(LIGHT_WORDS)}
    tier = "heavy" if hits["heavy"] else "medium" if hits["medium"] else "light"
    reasons = [f"{tier}: {', '.join(hits[tier])}"] if hits[tier] else ["light: no condition words (default)"]
    if year_built and year_built < REHAB_OLD_HOME_YEAR and tier != "heavy":
        tier = order[order.index(tier) + 1]
        reasons.append(f"built {year_built} (pre-{REHAB_OLD_HOME_YEAR}) → {tier}")
    rate = REHAB_TIERS[tier]
    return {"tier": tier, "per_sqft": rate,
            "amount": round(rate * sqft) if sqft else None,
            "reason": "; ".join(reasons)}


# ── Comps ─────────────────────────────────────────────────────────────

def miles_between(lat1, lon1, lat2, lon2):
    r = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _num(v):
    try:
        return float(str(v).replace("$", "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def sold_fields(item):
    """Normalise one sold search result. None if it lacks what a comp needs."""
    hi = (item.get("hdpData") or {}).get("homeInfo") or {}
    price = _num(item.get("soldPrice")) or _num(hi.get("price")) or _num(item.get("unformattedPrice"))
    sqft = _num(item.get("livingArea") or hi.get("livingArea") or item.get("area"))
    lat = _num(item.get("latitude") or hi.get("latitude"))
    lon = _num(item.get("longitude") or hi.get("longitude"))
    sold_ms = item.get("dateSold") or hi.get("dateSold")
    if not (price and sqft and lat is not None and lon is not None and sold_ms):
        return None
    street = item.get("streetAddress") or hi.get("streetAddress") or ""
    city = item.get("city") or hi.get("city") or ""
    zipc = item.get("zipcode") or hi.get("zipcode") or ""
    url = item.get("detailUrl") or ""
    if url.startswith("/"):
        url = "https://www.zillow.com" + url
    return {
        "zpid": str(item.get("zpid") or hi.get("zpid") or ""),
        "address": f"{street}, {city}, CA {zipc}".strip(", "),
        "sold_price": price, "sold_ms": sold_ms,
        "sold_date": time.strftime("%Y-%m-%d", time.gmtime(sold_ms / 1000)),
        "sqft": sqft, "price_per_sqft": price / sqft,
        "beds": _num(item.get("bedrooms") or hi.get("bedrooms")),
        "home_type": (item.get("homeType") or hi.get("homeType") or "").upper(),
        "lat": lat, "lon": lon, "url": url,
    }


def comp_candidates(subject, sold_items, now_ms=None):
    """Sold items passing the cheap filters, nearest first (no pool / 55+ yet)."""
    now_ms = now_ms or time.time() * 1000
    min_ms = now_ms - COMP_MAX_AGE_DAYS * 86400 * 1000
    lo, hi = subject["sqft"] * (1 - COMP_SQFT_TOLERANCE), subject["sqft"] * (1 + COMP_SQFT_TOLERANCE)
    seen, out = set(), []
    for item in sold_items:
        c = sold_fields(item)
        if not c or c["zpid"] in seen or c["zpid"] == subject.get("zpid"):
            continue
        seen.add(c["zpid"])
        if c["sold_ms"] < min_ms:
            continue
        if subject.get("home_type") and c["home_type"] != subject["home_type"]:
            continue
        if not (lo <= c["sqft"] <= hi):
            continue
        if subject.get("beds") is not None and c["beds"] is not None \
                and abs(c["beds"] - subject["beds"]) > COMP_BEDS_TOLERANCE:
            continue
        c["distance_mi"] = miles_between(subject["lat"], subject["lon"], c["lat"], c["lon"])
        if c["distance_mi"] > COMP_RADIUS_MI:
            continue
        out.append(c)
    out.sort(key=lambda c: c["distance_mi"])
    return out


def pick_comps(subject, candidates, fetch_detail):
    """Choose comps, matching pool and 55+ to the subject.

    fetch_detail(comp) returns the comp's property-details dict (or None).
    Returns (comps, flags) where flags lists "POOL MISMATCH" / "55+ MISMATCH"
    when non-matching comps had to fill in.
    """
    matching, others = [], []
    for c in candidates[:MAX_COMP_DETAIL_LOOKUPS]:
        detail = fetch_detail(c) or {}
        c["pool"], c["pool_source"] = detect_pool(detail)
        c["senior"], c["senior_source"] = detect_senior(detail)
        c["remodeled"], c["remodeled_source"], c["remodeled_evidence"] = detect_remodeled(detail)
        c["pool_match"] = c["pool"] == subject["pool"]
        c["senior_match"] = c["senior"] == subject["senior"]
        (matching if c["pool_match"] and c["senior_match"] else others).append(c)
        if len(matching) >= COMPS_MAX:
            break
    comps = matching[:COMPS_MAX]
    flags = []
    if len(comps) < COMPS_MIN:
        fill = others[:COMPS_MIN - len(comps)]
        comps = sorted(comps + fill, key=lambda c: c["distance_mi"])
        if any(not c["pool_match"] for c in fill):
            flags.append("POOL MISMATCH")
        if any(not c["senior_match"] for c in fill):
            flags.append("55+ MISMATCH")
    return comps, flags


REMODELED_MIN_FOR_ARV = 2


def arv_basis(comps):
    """(comps_behind_the_arv, label). Remodeled comps only when at least 2 are
    remodeled — that is what a flip resells against — otherwise all comps."""
    remodeled = [c for c in comps if c.get("remodeled")]
    if len(remodeled) >= REMODELED_MIN_FOR_ARV:
        return remodeled, f"remodeled comps only ({len(remodeled)} of {len(comps)})"
    return comps, (f"all {len(comps)} comps — only {len(remodeled)} remodeled, "
                   f"need {REMODELED_MIN_FOR_ARV} to use remodeled only") if comps else ""


def compute_arv(subject_sqft, comps):
    if not comps or not subject_sqft:
        return None
    return round(statistics.median(c["price_per_sqft"] for c in comps) * subject_sqft)


def neighbor_zip_codes(detail, own_zip, limit=4):
    """ZIP codes from a property-details `nearbyZipcodes` list, minus its own.

    The API returns entries like {'name': '92701', 'regionUrl': {...}}; plain
    strings and {'zipcode': ...} are accepted too.
    """
    out = []
    for z in detail.get("nearbyZipcodes") or []:
        if isinstance(z, dict):
            z = z.get("zipcode") or z.get("name")
        z = str(z or "").strip()
        if re.fullmatch(r"\d{5}", z) and z != str(own_zip) and z not in out:
            out.append(z)
    return out[:limit]


# ── One listing ───────────────────────────────────────────────────────

def analyze_listing(subject, sold_items, fetch_detail, more_sold_items=None, now_ms=None):
    """Comps, ARV, rehab and MAO for one subject.

    subject: {zpid, address, list_price, sqft, beds, home_type, lat, lon,
              year_built, description, pool, senior}
    sold_items: sold search results for the subject's ZIP.
    more_sold_items(): optional, returns sold results from neighbouring ZIPs;
              called only when the subject's ZIP yields fewer than 3 candidates.
    """
    out = {"comps": [], "comp_flags": [], "arv": None, "comps_used": 0,
           "arv_comps_used": 0, "remodeled_comps": 0, "arv_basis": "",
           "confidence": None, "rehab": None, "mao": None, "profit_at_mao": None,
           "mao_vs_list": None, "mao_vs_list_pct": None, "note": None,
           "neighbor_zips_searched": False}
    out["rehab"] = estimate_rehab(subject.get("description"), subject.get("year_built"), subject.get("sqft"))
    if not subject.get("sqft") or subject.get("lat") is None:
        out["note"] = "no sqft or location on the listing — cannot pull comps"
        return out

    candidates = comp_candidates(subject, sold_items, now_ms)
    if len(candidates) < COMPS_MIN and more_sold_items:
        out["neighbor_zips_searched"] = True
        candidates = comp_candidates(subject, list(sold_items) + list(more_sold_items()), now_ms)

    comps, flags = pick_comps(subject, candidates, fetch_detail)
    basis, out["arv_basis"] = arv_basis(comps)
    out["comps"], out["comp_flags"], out["comps_used"] = comps, flags, len(comps)
    out["arv_comps_used"] = len(basis)
    for c in comps:
        c["in_arv"] = any(c is b for b in basis)
    out["remodeled_comps"] = sum(1 for c in comps if c.get("remodeled"))
    out["confidence"] = "LOW CONFIDENCE" if len(basis) < COMPS_MIN else "OK"
    out["arv"] = compute_arv(subject["sqft"], basis)
    if out["arv"] is None:
        out["note"] = "no sold comps matched (6 mo, 0.5 mi, ±20% sqft, ±1 bed, same type)"
        return out
    if out["rehab"]["amount"] is None:
        return out
    mao, at_mao = max_allowable_offer(out["arv"], out["rehab"]["amount"])
    if mao is None:
        out["note"] = "no purchase price reaches 10% profit at this ARV and rehab"
        return out
    out["mao"], out["profit_at_mao"] = mao, round(at_mao["profit"])
    if subject.get("list_price"):
        out["mao_vs_list"] = mao - subject["list_price"]
        out["mao_vs_list_pct"] = out["mao_vs_list"] / subject["list_price"]
    return out
