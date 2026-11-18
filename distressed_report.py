"""
Rows and CSVs for search_distressed results.

Shared by scripts/run_distressed.py (terminal tables + CSV export) and the
web app's Distressed Deals page, so both produce the same agent files. Pure
functions over the JSON search_distressed returns; no network.
"""

import csv
import io

LISTING_COLUMNS = [
    "zip", "address", "list_price", "arv", "arv_basis", "comps_used", "remodeled_comps",
    "unknown_comps", "fixers_excluded", "confidence", "comp_flags",
    "rehab_tier", "rehab_per_sqft", "rehab", "rehab_reason", "mao", "mao_vs_list",
    "mao_vs_list_pct", "profit_at_mao", "days_on_market", "beds", "baths", "sqft",
    "year_built", "pool", "55_plus", "distress_signals", "note", "zillow_url",
]
COMP_COLUMNS = [
    "subject_address", "comp_address", "sold_price", "sold_date", "sqft",
    "price_per_sqft", "distance_mi", "pool", "55_plus", "remodeled", "remodeled_evidence",
    "matches_subject", "zillow_url",
]


def money(v):
    return f"${v:,.0f}" if isinstance(v, (int, float)) else "—"


def yn(v):
    return "yes" if v else "no"


def rows_for(zip_code, listing):
    a = listing.get("analysis") or {}
    r = a.get("rehab") or {}
    pct = a.get("mao_vs_list_pct")
    row = {
        "zip": zip_code, "address": listing.get("address"),
        "list_price": listing.get("price"), "arv": a.get("arv"), "arv_basis": a.get("arv_basis") or "",
        "comps_used": a.get("comps_used", 0), "remodeled_comps": a.get("remodeled_comps", 0),
        "unknown_comps": a.get("unknown_comps", 0),
        "fixers_excluded": "; ".join(f"{f['address']} ({f['evidence']})" for f in a.get("skipped_fixers") or []),
        "confidence": a.get("confidence") or "",
        "comp_flags": " / ".join(a.get("comp_flags") or []),
        "rehab_tier": r.get("tier"), "rehab_per_sqft": r.get("per_sqft"), "rehab": r.get("amount"),
        "rehab_reason": r.get("reason"), "mao": a.get("mao"), "mao_vs_list": a.get("mao_vs_list"),
        "mao_vs_list_pct": f"{pct:.1%}" if pct is not None else "",
        "profit_at_mao": a.get("profit_at_mao"), "days_on_market": listing.get("days"),
        "beds": listing.get("beds"), "baths": listing.get("baths"), "sqft": listing.get("sqft"),
        "year_built": a.get("year_built"), "pool": yn(a.get("pool")), "55_plus": yn(a.get("senior")),
        "distress_signals": "; ".join(listing.get("distress_signals") or []),
        "note": a.get("note") or "", "zillow_url": a.get("zillow_url") or "",
    }
    comps = []
    for c in a.get("comps") or []:
        mism = [m for m, ok in (("pool", c.get("pool_match")), ("55+", c.get("senior_match"))) if not ok]
        comps.append({
            "subject_address": listing.get("address"), "comp_address": c.get("address"),
            "sold_price": c.get("sold_price"), "sold_date": c.get("sold_date"), "sqft": c.get("sqft"),
            "price_per_sqft": round(c.get("price_per_sqft") or 0), "distance_mi": round(c.get("distance_mi") or 0, 2),
            "pool": yn(c.get("pool")), "55_plus": yn(c.get("senior")),
            "remodeled": c.get("remodeled") or "UNKNOWN",
            "remodeled_evidence": c.get("remodeled_evidence") or "",
            "matches_subject": "yes" if not mism else "MISMATCH: " + ", ".join(mism),
            "zillow_url": c.get("url") or "",
        })
    return row, comps


MULTI_FAMILY_TYPES = {"MULTI_FAMILY", "DUPLEX", "TRIPLEX", "QUADRUPLEX"}
# Deal tag by how far the max offer is below list price.
GOOD_DEAL_MAX_BELOW = 0.15       # 0-15% below (or above list) → GOOD DEAL
MAKE_OFFER_MAX_BELOW = 0.30      # over 15% up to 30% below → MAKE AN OFFER; beyond → LONG SHOT


def deal_tag(pct_below_list):
    """'good' / 'offer' / 'long', or None when there is no max offer."""
    if pct_below_list is None:
        return None
    if pct_below_list <= GOOD_DEAL_MAX_BELOW:
        return "good"
    if pct_below_list <= MAKE_OFFER_MAX_BELOW:
        return "offer"
    return "long"


def card(zip_code, listing):
    """The few fields one Distressed Deals card shows, plus its comps."""
    a = listing.get("analysis") or {}
    r = a.get("rehab") or {}
    pct = a.get("mao_vs_list_pct")
    pct_below = -pct if pct is not None else None
    multi = (a.get("home_type") or "").upper() in MULTI_FAMILY_TYPES
    address = listing.get("address") or ""
    street, _, rest = address.partition(", ")
    return {
        "zip": zip_code, "address": address, "street": street, "city": rest.rsplit(",", 1)[0] if rest else "",
        "list_price": listing.get("price"), "mao": a.get("mao"), "pct_below_list": pct_below,
        "arv": a.get("arv"), "rehab": r.get("amount"), "rehab_tier": r.get("tier"),
        "profit_at_mao": a.get("profit_at_mao"), "days_on_market": listing.get("days"),
        "deal_tag": deal_tag(pct_below),
        "unpriced_reason": None if a.get("mao") is not None else (
            "Multi-family — house sales can't price it" if multi
            else "No usable sold comps nearby" if (a.get("note") or "").startswith("no usable sold comps")
            else (a.get("note") or "No max offer")),
        "low_confidence": a.get("confidence") == "LOW CONFIDENCE" and not multi,
        "multi_family": multi,
        "comp_flags": a.get("comp_flags") or [], "arv_basis": a.get("arv_basis") or "",
        "note": a.get("note") or "", "zillow_url": a.get("zillow_url") or "",
        "comps": [{"address": c.get("address"), "sold_price": c.get("sold_price"),
                   "sold_date": c.get("sold_date"), "sqft": c.get("sqft"),
                   "price_per_sqft": c.get("price_per_sqft"), "distance_mi": c.get("distance_mi"),
                   "remodeled": c.get("remodeled") or "UNKNOWN", "url": c.get("url")}
                  for c in a.get("comps") or []],
        "skipped_fixers": a.get("skipped_fixers") or [],
    }


def sort_cards(cards):
    """Best deals first: highest MAO-vs-list; cards without an MAO last."""
    return sorted(cards, key=lambda c: (c["pct_below_list"] is None, c["pct_below_list"] or 0))


def write_csv(columns, rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def property_csv(zip_code, listing):
    """One property for the agent: its summary, a blank line, then its comps."""
    row, comps = rows_for(zip_code, listing)
    out = io.StringIO()
    w = csv.writer(out)
    for k in LISTING_COLUMNS:
        w.writerow([k, row.get(k, "")])
    w.writerow([])
    out.write(write_csv(COMP_COLUMNS, comps))
    return out.getvalue()
