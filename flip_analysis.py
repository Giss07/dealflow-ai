"""
Per-listing flip analysis for search_distressed: sold comps, ARV, rehab, MAO.

    comps   SOLD in the last 6 months, within 0.5 mi, same property type,
            sqft within ±20%, beds within ±1. Pool and 55+ must match the
            subject; with fewer than 3 matching comps, the closest
            non-matching ones fill in to 3 and the row is flagged
            "POOL MISMATCH" / "55+ MISMATCH". Each comp is REMODELED, NOT
            (clear fixer wording) or UNKNOWN (no wording either way). Among
            every comp that passes the filters, REMODELED come first, then
            UNKNOWN, nearest first within each; NOT is never used. Up to 5.
            Also never used, and shown with the reason: sales by public record
            only (no MLS listing), lots over 2x the subject's, and $/sqft over
            1.5x the median of the other comps.
    ARV     median sold price of those comps, plus $100 per sqft the subject
            is larger (minus, if smaller) than the comps' median sqft, capped
            between the lowest and highest comp sold price. Fewer than 3 comps
            is "LOW CONFIDENCE"; none means no ARV and no MAO.
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
LOT_MAX_RATIO = 2.0               # comp lot over 2x the subject's → excluded
OUTLIER_RATIO = 1.5               # comp $/sqft over 1.5x the other comps' median → excluded
MLS_LOOKBACK_DAYS = 365           # an MLS listing this long before the sale makes it an MLS sale
COMPS_MAX = 5
MAX_COMP_DETAIL_LOOKUPS = 30      # per subject: candidates checked for pool / 55+ / remodel

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


REMODELED, NOT_REMODELED, UNKNOWN = "REMODELED", "NOT", "UNKNOWN"


def detect_remodeled(detail):
    """(state, source, evidence) with state REMODELED, NOT or UNKNOWN.

    NOT means clear fixer wording ("fixer", "needs updating", "TLC"); UNKNOWN
    means no wording either way — a finished house whose listing simply does
    not say "remodeled" is UNKNOWN, not NOT. The listing's condition field
    decides when it is conclusive; otherwise the description and photo
    captions are searched, and fixer wording wins over remodel wording, so
    "Fixer! Remodeled in 1998" is NOT. Photo images themselves are not
    analysed — only their captions.
    """
    cond = str((detail.get("resoFacts") or {}).get("propertyCondition") or "").lower()
    if cond:
        if _words_in(cond, ["fixer", "needs work", "teardown", "tear down"]):
            return NOT_REMODELED, "data", f"condition: {cond}"
        if re.search(r"updated|remodel|renovat|turnkey|turn key|new construction", cond):
            return REMODELED, "data", f"condition: {cond}"
    captions = " ".join(str(p.get("caption") or "") for p in (detail.get("responsivePhotos") or [])
                        if isinstance(p, dict))
    text = f"{detail.get('description') or ''} {captions}".lower()
    against = _words_in(text, NOT_REMODELED_WORDS)
    if against:
        return NOT_REMODELED, "description", "fixer wording: " + ", ".join(against)
    found = _words_in(text, REMODEL_WORDS)
    if found:
        return REMODELED, "description", ", ".join(found)
    return UNKNOWN, "description", "no wording either way"


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


# ── Mobile / manufactured homes ───────────────────────────────────────

MOBILE_HOME_TYPES = {"MANUFACTURED", "MOBILE", "MOBILE_HOME"}
# "Space 61", "Spc 157", "Sp 4", "Lot #12", "Lot 11" — park spaces, not land.
MOBILE_ADDRESS = re.compile(r"\b(?:space|spc|sp)\s*#?\s*\d|\blot\s*#\s*\w|\blot\s+\d", re.I)


def is_mobile_home(address, home_type):
    """Mobile / manufactured homes are not flipped or comped like houses."""
    return (str(home_type or "").upper() in MOBILE_HOME_TYPES
            or bool(MOBILE_ADDRESS.search(str(address or ""))))


# ── Lot size and sale source ──────────────────────────────────────────

def lot_sqft(value, unit):
    """Lot size in sqft from Zillow's value + unit ('sqft', 'Square Feet', 'acres', 'Acres')."""
    v = _num(value)
    if not v:
        return None
    return round(v * 43_560) if str(unit or "").lower().startswith("acre") else round(v)


def detail_lot_sqft(detail):
    lot = lot_sqft(detail.get("lotAreaValue"), detail.get("lotAreaUnits") or detail.get("lotAreaUnit"))
    if lot:
        return lot
    m = re.match(r"\s*([\d,.]+)\s*(acres?|sq\s*ft|sqft|square feet)", str((detail.get("resoFacts") or {}).get("lotSize") or ""), re.I)
    return lot_sqft(m.group(1), "acres" if m and m.group(2).lower().startswith("acre") else "sqft") if m else None


MLS_EVENTS = ("listed for sale", "pending sale", "contingent", "price change", "listing removed")


def sold_by_public_record_only(detail, sold_ms):
    """True when the sale on record was not an MLS sale.

    The Sold event nearest the comp's sale date must come from "Public Record",
    with no MLS listing event (listed, pending, contingent, price change) in the
    year before it. Unknown history → False (the comp is kept).
    """
    history = [e for e in (detail.get("priceHistory") or []) if isinstance(e, dict)]
    sold = [e for e in history if str(e.get("event") or "").lower() == "sold" and e.get("time")]
    if not sold or not sold_ms:
        return False
    sale = min(sold, key=lambda e: abs(e["time"] - sold_ms))
    if abs(sale["time"] - sold_ms) > 45 * 86400 * 1000:
        return False
    if "public record" not in str(sale.get("source") or "").lower():
        return False
    since = sale["time"] - MLS_LOOKBACK_DAYS * 86400 * 1000
    return not any(str(e.get("event") or "").lower() in MLS_EVENTS and since <= (e.get("time") or 0) <= sale["time"]
                   for e in history)


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
        "lot_sqft": lot_sqft(item.get("lotAreaValue") or hi.get("lotAreaValue"),
                             item.get("lotAreaUnit") or hi.get("lotAreaUnit")),
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
        if is_mobile_home(c["address"], c["home_type"]):     # a "Space 12" typed SINGLE_FAMILY
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
    """Choose up to 5 comps from every candidate that passed comp_candidates.

    Each candidate's details give pool, 55+, remodel state, sale source and
    lot. A candidate is EXCLUDED (kept for display, never in the ARV) when it
    is a clear fixer, sold by public record only, or sits on a lot over 2x the
    subject's. Among the rest, comps matching the subject's pool and 55+ come
    first: REMODELED, then UNKNOWN, nearest first within each; with fewer than
    3 matching, the closest non-matching ones fill in to 3 and the result is
    flagged "POOL MISMATCH" / "55+ MISMATCH". Then any chosen comp whose $/sqft
    is over 1.5x the median of the other chosen comps is excluded as an
    outlier and the next candidate takes its place, until none are left.

    fetch_detail(comp) returns the comp's property-details dict (or None).
    Returns (comps, flags, excluded) — excluded comps carry "excluded_reason".
    """
    checked = candidates[:MAX_COMP_DETAIL_LOOKUPS]
    subject_lot = subject.get("lot_sqft")
    excluded = []
    for c in checked:
        detail = fetch_detail(c) or {}
        c["pool"], c["pool_source"] = detect_pool(detail)
        c["senior"], c["senior_source"] = detect_senior(detail)
        c["remodeled"], c["remodeled_source"], c["remodeled_evidence"] = detect_remodeled(detail)
        c["pool_match"] = c["pool"] == subject["pool"]
        c["senior_match"] = c["senior"] == subject["senior"]
        c["lot_sqft"] = c.get("lot_sqft") or detail_lot_sqft(detail)
        reason = None
        if c["remodeled"] == NOT_REMODELED:
            reason = f"Fixer ({c['remodeled_evidence']})"
        elif sold_by_public_record_only(detail, c.get("sold_ms")):
            reason = "Sold by public record only (not on the MLS)"
        elif subject_lot and c["lot_sqft"] and c["lot_sqft"] > LOT_MAX_RATIO * subject_lot:
            reason = f"Lot {c['lot_sqft']:,} sqft is over {LOT_MAX_RATIO:g}x the subject's {subject_lot:,} sqft"
        if reason:
            c["excluded_reason"] = reason
            excluded.append(c)

    def ranked(cs):            # REMODELED before UNKNOWN, then nearest
        return sorted(cs, key=lambda c: (c["remodeled"] != REMODELED, c["distance_mi"]))

    usable = [c for c in checked if not c.get("excluded_reason")]

    def choose(pool):
        matching = ranked([c for c in pool if c["pool_match"] and c["senior_match"]])
        others = ranked([c for c in pool if not (c["pool_match"] and c["senior_match"])])
        comps, flags = matching[:COMPS_MAX], []
        if len(comps) < COMPS_MIN:
            fill = others[:COMPS_MIN - len(comps)]
            comps = comps + fill
            if any(not c["pool_match"] for c in fill):
                flags.append("POOL MISMATCH")
            if any(not c["senior_match"] for c in fill):
                flags.append("55+ MISMATCH")
        return comps, flags

    comps, flags = choose(usable)
    while len(comps) >= 2:                       # outliers need someone to compare against
        worst, worst_ratio, worst_median = None, OUTLIER_RATIO, None
        for c in comps:
            median_others = statistics.median(o["price_per_sqft"] for o in comps if o is not c)
            ratio = c["price_per_sqft"] / median_others if median_others else 0
            if ratio > worst_ratio:
                worst, worst_ratio, worst_median = c, ratio, median_others
        if worst is None:
            break
        worst["excluded_reason"] = (f"${worst['price_per_sqft']:,.0f}/sqft is over {OUTLIER_RATIO:g}x the other "
                                    f"comps' median ${worst_median:,.0f}/sqft (outlier)")
        excluded.append(worst)
        usable = [c for c in usable if c is not worst]
        comps, flags = choose(usable)
    return comps, flags, excluded


def arv_basis(comps, excluded):
    """Plain-language note on what the ARV rests on."""
    if not comps:
        return ""
    r = sum(1 for c in comps if c["remodeled"] == REMODELED)
    note = f"{len(comps)} comps: {r} remodeled, {len(comps) - r} unknown"
    if excluded:
        kinds = {}
        for c in excluded:
            k = ("fixer" if c["excluded_reason"].startswith("Fixer") else
                 "public record" if "public record" in c["excluded_reason"] else
                 "big lot" if c["excluded_reason"].startswith("Lot ") else "outlier")
            kinds[k] = kinds.get(k, 0) + 1
        note += f"; {len(excluded)} excluded (" + ", ".join(f"{n} {k}" for k, n in kinds.items()) + ")"
    return note


SIZE_ADJ_PER_SQFT = 100          # $ per sqft of size difference from the comps' median


def arv_calculation(subject_sqft, comps):
    """How the ARV is built, or None without comps or subject sqft.

    median sold price + $100 × (subject sqft − comps' median sqft), then held
    between the lowest and highest comp sold price. (Replaced median $/sqft ×
    sqft on 2026-11-18: 1485 N Fillmore Ave went from $712,270 — above every
    comp it was built from — to $648,000, its highest comp.)
    """
    if not comps or not subject_sqft:
        return None
    prices = [c["sold_price"] for c in comps]
    median_price = statistics.median(prices)
    median_sqft = statistics.median(c["sqft"] for c in comps)
    size_adj = SIZE_ADJ_PER_SQFT * (subject_sqft - median_sqft)
    raw = median_price + size_adj
    low, high = min(prices), max(prices)
    cap = "highest" if raw > high else "lowest" if raw < low else None
    arv = high if cap == "highest" else low if cap == "lowest" else raw
    sign = "+" if size_adj >= 0 else "−"
    explain = f"Median ${median_price:,.0f} {sign} size adj ${abs(size_adj):,.0f} = ${raw:,.0f}"
    if cap:
        explain += f" → capped at {cap} comp ${arv:,.0f}"
    return {"arv": round(arv), "median_price": round(median_price), "median_sqft": round(median_sqft),
            "subject_sqft": round(subject_sqft), "size_adj": round(size_adj), "raw": round(raw),
            "cap": cap, "low": round(low), "high": round(high), "explain": explain}


def compute_arv(subject_sqft, comps):
    calc = arv_calculation(subject_sqft, comps)
    return calc["arv"] if calc else None


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
           "remodeled_comps": 0, "unknown_comps": 0, "skipped_fixers": [], "excluded_comps": [], "arv_basis": "",
           "mobile_home": False, "arv_calc": None, "arv_explain": "",
           "confidence": None, "rehab": None, "mao": None, "profit_at_mao": None,
           "mao_vs_list": None, "mao_vs_list_pct": None, "note": None,
           "neighbor_zips_searched": False}
    if is_mobile_home(subject.get("address"), subject.get("home_type")):
        out["mobile_home"] = True                  # no comp lookups, no rehab, no MAO
        out["note"] = "Mobile home"
        return out
    out["rehab"] = estimate_rehab(subject.get("description"), subject.get("year_built"), subject.get("sqft"))
    if not subject.get("sqft") or subject.get("lat") is None:
        out["note"] = "no sqft or location on the listing — cannot pull comps"
        return out

    candidates = comp_candidates(subject, sold_items, now_ms)
    if len(candidates) < COMPS_MIN and more_sold_items:
        out["neighbor_zips_searched"] = True
        candidates = comp_candidates(subject, list(sold_items) + list(more_sold_items()), now_ms)

    comps, flags, excluded = pick_comps(subject, candidates, fetch_detail)
    out["comps"], out["comp_flags"], out["comps_used"] = comps, flags, len(comps)
    out["excluded_comps"] = excluded
    out["arv_basis"] = arv_basis(comps, excluded)
    out["remodeled_comps"] = sum(1 for c in comps if c["remodeled"] == REMODELED)
    out["unknown_comps"] = sum(1 for c in comps if c["remodeled"] == UNKNOWN)
    out["skipped_fixers"] = [{"address": c["address"], "evidence": c["remodeled_evidence"]}
                             for c in excluded if c["excluded_reason"].startswith("Fixer")]
    out["confidence"] = "LOW CONFIDENCE" if len(comps) < COMPS_MIN else "OK"
    calc = arv_calculation(subject["sqft"], comps)
    out["arv"] = calc["arv"] if calc else None
    out["arv_calc"] = calc
    out["arv_explain"] = calc["explain"] if calc else ""
    if out["arv"] is None:
        out["note"] = "no usable sold comps (6 mo, 0.5 mi, ±20% sqft, ±1 bed, same type, not a fixer)"
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
