"""
DealFlow Builder Zones — track new-construction listings and the resale
listings sitting next to them.

Thesis: when a builder opens a tract, the older homes, lots and teardowns
within walking distance get pulled up with it. This module finds the new
builds (job 1), then finds the ordinary for-sale listings within
BUILDER_ZONE_RADIUS_MILES of one (job 2).

Data source: OpenWeb Ninja Real-Time Zillow Data /search — same API and key
the MLS scanner uses ($0.0025/call). The /search endpoint has NO server-side
new-construction filter (listing_type only accepts BY_AGENT and
BY_OWNER_OTHER — verified against the live API 2026-09-23), so we pull the
zip's FOR_SALE listings once and classify new construction client-side off
the Zillow marketing flags.

Recipients come from BUILDER_ZONE_ALERT_EMAILS only — this feature does not
read the shared ALERT_EMAILS, so turning it on cannot change who receives any
existing alert.

Cost control: both daily jobs read the SAME zip search. search_zip() caches
each (zip, page, listing_type) response for BUILDER_ZONE_SEARCH_TTL seconds
(default 2h) in-process, so scheduling the match job within the TTL of the
new-build job makes the second job effectively free. One zip page = one API
call = $0.0025.
"""

import os
import time
import math
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

SEARCH_URL = "https://api.openwebninja.com/realtime-zillow-data/search"
SOURCE = "openweb_ninja"

# Cost per OpenWeb Ninja call — mirrors worker.COST_OPENWEB_NINJA.
COST_PER_CALL = 0.0025

# The 74 Inland Empire target zips (restored from the retired scraper.py —
# commit 744c68a removed it when Apify was dropped). Override with
# BUILDER_ZONE_ZIPS="92503,92505" to scope a run.
IE_ZIP_CODES = [
    "91701", "91708", "91709", "91710", "91730", "91737", "91739", "91743", "91750", "91752",
    "91761", "91762", "91763", "91764", "91766", "91767", "91768", "91784", "91786",
    "92223", "92316", "92318", "92320", "92324", "92335", "92336", "92337", "92345", "92346",
    "92350", "92354", "92357", "92358", "92359", "92373", "92374", "92376", "92377", "92382",
    "92392", "92394", "92395", "92399", "92401", "92404", "92405", "92407", "92408", "92410",
    "92411", "92501", "92503", "92504", "92505", "92506", "92507", "92508", "92509", "92530",
    "92532", "92543", "92544", "92545", "92548", "92549", "92551", "92552", "92553", "92555",
    "92557", "92562", "92563", "92567", "92571",
]


def target_zips():
    """Zips to scan — BUILDER_ZONE_ZIPS env override, else the 74 IE zips."""
    raw = os.getenv("BUILDER_ZONE_ZIPS", "").strip()
    if raw:
        return [z.strip() for z in raw.split(",") if z.strip()]
    return list(IE_ZIP_CODES)


def radius_miles():
    return float(os.getenv("BUILDER_ZONE_RADIUS_MILES", "0.5"))


def excluded_new_build_types():
    """Home types that do NOT count as a builder zone, even when Zillow flags
    them new construction.

    Default excludes MANUFACTURED: a manufactured-home dealer listing carries
    the same is_newHome flag as a tract build, but a $139k coach in a park is
    not a builder opening a tract, and in the live 92503 test it pulled a
    $1.22M house in as a "match". Set BUILDER_ZONE_EXCLUDE_HOME_TYPES="" to
    count every flagged listing, or add types (comma-separated) to drop more.
    """
    raw = os.getenv("BUILDER_ZONE_EXCLUDE_HOME_TYPES", "MANUFACTURED")
    return {t.strip().upper() for t in raw.split(",") if t.strip()}


def max_pages():
    """Pages of search results per zip. Each page is one billable call.

    Default 2 (~80 listings/zip) — enough for IE zip inventory without
    paging into stale tail results on the big zips.
    """
    return int(os.getenv("BUILDER_ZONE_MAX_PAGES", "2"))


# ── Zip search (shared, cached) ───────────────────────────────────────

_search_cache = {}   # {(zip, page, listing_type): (epoch, items)}
_call_count = 0      # billable calls made this process — for cost logging


def _cache_ttl():
    return int(os.getenv("BUILDER_ZONE_SEARCH_TTL", "7200"))


def calls_made():
    """Billable OpenWeb Ninja calls this process has made via this module."""
    return _call_count


def _fetch_page(zip_code, page, api_key, listing_type):
    """One /search page. Returns (items, error). Cached per TTL.

    Retries 429 and 5xx with backoff, mirroring worker._scan_via_openweb_ninja.
    """
    global _call_count
    import requests

    key = (zip_code, page, listing_type)
    hit = _search_cache.get(key)
    if hit:
        ts, items = hit
        age = int(time.time() - ts)
        if age < _cache_ttl():
            logger.info(f"  [bz] CACHE HIT zip={zip_code} page={page} age={age}s items={len(items)}")
            return items, None

    params = {"location": zip_code, "home_status": "FOR_SALE", "page": page}
    if listing_type:
        params["listing_type"] = listing_type

    last_error = None
    for attempt in range(3):
        try:
            r = requests.get(SEARCH_URL, params=params,
                             headers={"x-api-key": api_key}, timeout=30)
            if r.status_code in (200, 201):
                _call_count += 1
                body = r.json()
                items = body.get("data")
                if not isinstance(items, list):
                    return [], "unexpected response format (data is not a list)"
                _search_cache[key] = (time.time(), items)
                logger.info(f"  [bz] zip={zip_code} page={page} {len(items)} listings")
                return items, None
            if r.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning(f"  [bz] rate limited (429) zip={zip_code} — waiting {wait}s")
                time.sleep(wait)
                last_error = "rate limited (429)"
                continue
            if r.status_code in (500, 502, 503, 504):
                wait = 2 ** (attempt + 1)
                logger.warning(f"  [bz] HTTP {r.status_code} zip={zip_code} — retrying in {wait}s")
                time.sleep(wait)
                last_error = f"HTTP {r.status_code}"
                continue
            # 400 on a page past the end is a normal stop, not a failure.
            _call_count += 1
            return [], f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.exceptions.Timeout:
            last_error = "timeout"
            if attempt < 2:
                time.sleep(2 ** attempt)
        except requests.exceptions.ConnectionError as e:
            last_error = f"connection error: {str(e)[:100]}"
            if attempt < 2:
                time.sleep(2 ** attempt)
        except ValueError as e:
            return [], f"response parse error: {str(e)[:100]}"

    return [], last_error or "all attempts failed"


def search_zip(zip_code, api_key, pages=None, listing_type="BY_AGENT"):
    """All FOR_SALE listings for a zip, across up to `pages` pages.

    Returns (items, error). A partial result with an error on a later page
    still returns the items collected so far — a page-3 hiccup should not
    throw away pages 1-2.
    """
    pages = pages or max_pages()
    collected = []
    seen_zpids = set()
    for page in range(1, pages + 1):
        items, err = _fetch_page(zip_code, page, api_key, listing_type)
        if err:
            return collected, (None if collected and page > 1 else err)
        if not items:
            break
        for it in items:
            z = str(it.get("zpid") or "")
            if z and z in seen_zpids:
                continue
            if z:
                seen_zpids.add(z)
            collected.append(it)
        # A short page means we reached the end of this zip's inventory.
        if len(items) < 20:
            break
    return collected, None


# ── Classification ────────────────────────────────────────────────────

def is_new_build(item):
    """True when Zillow marks this listing as builder/new construction.

    Verified against the live 92503 payload (2026-09-23): a new-construction
    listing carries listingSubType.is_newHome, marketingStatusSimplifiedCd
    'New Construction', statusText 'New construction', and a
    newConstructionType. Any one of them is enough — Zillow does not set
    them uniformly across feeds.
    """
    if (item.get("listingSubType") or {}).get("is_newHome"):
        return True
    if (item.get("marketingStatusSimplifiedCd") or "").strip().lower() == "new construction":
        return True
    if "new construction" in (item.get("statusText") or "").strip().lower():
        return True
    if item.get("newConstructionType"):
        return True
    if item.get("isPaidBuilderNewConstruction") or item.get("isPremierBuilder"):
        return True
    return False


def builder_name(item):
    """Best available builder name for a new-construction listing.

    /search exposes the listing office as brokerName — for builder listings
    that IS the builder (e.g. 'Pacific Manufactured Homes', 'Lennar Homes of
    California'). No separate builder field exists on this endpoint.
    """
    for field in ("brokerName", "communityName", "info2String"):
        val = (item.get(field) or "").strip()
        if val:
            return val[:255]
    return "Unknown builder"


def _price(item):
    raw = item.get("unformattedPrice") or item.get("price")
    if raw is None:
        return None
    try:
        return float(str(raw).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        return None


def _latlng(item):
    lat = item.get("latitude")
    lng = item.get("longitude")
    if lat is None or lng is None:
        ll = item.get("latLong") or {}
        lat, lng = ll.get("latitude"), ll.get("longitude")
    if lat is None or lng is None:
        hi = (item.get("hdpData") or {}).get("homeInfo") or {}
        lat, lng = hi.get("latitude"), hi.get("longitude")
    try:
        return (float(lat), float(lng)) if lat is not None and lng is not None else (None, None)
    except (ValueError, TypeError):
        return (None, None)


def normalize(item, zip_code):
    """Flatten a /search result into the shape both jobs and the DB use."""
    lat, lng = _latlng(item)
    return {
        "zpid": str(item.get("zpid") or "") or None,
        "address": item.get("streetAddress") or item.get("addressStreet") or item.get("address") or "",
        "city": item.get("city") or item.get("addressCity") or "",
        "state": item.get("state") or item.get("addressState") or "CA",
        "zip_code": item.get("zipcode") or item.get("addressZipcode") or zip_code,
        "latitude": lat,
        "longitude": lng,
        "price": _price(item),
        "home_type": item.get("homeType") or "",
        "listing_url": item.get("detailUrl") or "",
        "days_on_zillow": item.get("daysOnZillow"),
        "beds": item.get("bedrooms") or item.get("beds"),
        "baths": item.get("bathrooms") or item.get("baths"),
        "sqft": item.get("livingArea") or item.get("area"),
        "builder_name": builder_name(item),
    }


def split_listings(items, zip_code):
    """Partition a zip's listings into (new_builds, resale).

    New construction of an excluded home type (see excluded_new_build_types)
    is dropped from both sides. Listings without coordinates are dropped too — every
    downstream step is distance-based, so an unmappable listing cannot
    participate in a match.
    """
    excluded_types = excluded_new_build_types()
    new_builds, resale, unmappable, excluded = [], [], 0, 0
    for it in items:
        rec = normalize(it, zip_code)
        if rec["latitude"] is None or rec["longitude"] is None:
            unmappable += 1
            continue
        if is_new_build(it):
            if (rec["home_type"] or "").upper() in excluded_types:
                # Not a builder zone — and not a resale candidate either.
                excluded += 1
                continue
            new_builds.append(rec)
        else:
            resale.append(rec)
    if unmappable:
        logger.info(f"  [bz] zip={zip_code} skipped {unmappable} listing(s) with no coordinates")
    if excluded:
        logger.info(f"  [bz] zip={zip_code} skipped {excluded} new-construction listing(s) of "
                    f"excluded type(s) {sorted(excluded_types)}")
    return new_builds, resale


# ── Distance ──────────────────────────────────────────────────────────

EARTH_RADIUS_MILES = 3958.8


def haversine_miles(lat1, lon1, lat2, lon2):
    """Great-circle distance in miles between two lat/lng points."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2
    return 2 * EARTH_RADIUS_MILES * math.asin(math.sqrt(a))


def match_listings(resale, new_builds, radius=None):
    """Pair each resale listing with its NEAREST new build inside `radius`.

    Returns a list of match dicts sorted by distance (closest first). A
    listing with several new builds nearby is reported once, against the
    closest one — the point is "this listing sits in a builder zone", not
    every builder near it.
    """
    radius = radius if radius is not None else radius_miles()
    matches = []
    for listing in resale:
        best = None
        best_dist = None
        for nb in new_builds:
            d = haversine_miles(listing["latitude"], listing["longitude"],
                                nb["latitude"], nb["longitude"])
            if d <= radius and (best_dist is None or d < best_dist):
                best, best_dist = nb, d
        if best is not None:
            matches.append({
                "listing": listing,
                "new_build": best,
                "distance_miles": round(best_dist, 3),
            })
    matches.sort(key=lambda m: m["distance_miles"])
    return matches


# ── Persistence ───────────────────────────────────────────────────────

def _dedupe_key(rec):
    """Stable identity for a listing: zpid when present, else address+zip."""
    if rec.get("zpid"):
        return f"zpid:{rec['zpid']}"
    addr = " ".join((rec.get("address") or "").split()).lower()
    return f"addr:{addr}|{rec.get('zip_code') or ''}"


def save_new_builds(records, db=None):
    """Insert new-build records, skipping ones already stored.

    Dedupe is by zpid, falling back to normalized address+zip for the rare
    listing with no zpid. An already-known build has its last_seen and price
    refreshed rather than being inserted again.

    Returns {"added": n, "skipped": n, "updated": n}.
    """
    from database import get_session, NewBuild

    close = False
    if db is None:
        db = get_session()
        close = True

    added = skipped = updated = 0
    try:
        for rec in records:
            key = _dedupe_key(rec)
            existing = None
            if rec.get("zpid"):
                existing = db.query(NewBuild).filter_by(zpid=rec["zpid"]).first()
            if existing is None:
                # No zpid (or no zpid match) — fall back to address+zip.
                for cand in db.query(NewBuild).filter_by(zip_code=rec.get("zip_code")).all():
                    if _dedupe_key({"zpid": cand.zpid, "address": cand.address,
                                    "zip_code": cand.zip_code}) == key:
                        existing = cand
                        break
            if existing is not None:
                existing.last_seen = datetime.utcnow()
                if rec.get("price") is not None:
                    existing.price = rec["price"]
                skipped += 1
                updated += 1
                continue

            db.add(NewBuild(
                zpid=rec.get("zpid"),
                address=rec.get("address"),
                city=rec.get("city"),
                state=rec.get("state") or "CA",
                zip_code=rec.get("zip_code"),
                latitude=rec.get("latitude"),
                longitude=rec.get("longitude"),
                builder_name=rec.get("builder_name"),
                price=rec.get("price"),
                home_type=rec.get("home_type"),
                listing_url=rec.get("listing_url"),
                source=SOURCE,
                date_found=datetime.utcnow(),
                last_seen=datetime.utcnow(),
            ))
            added += 1
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"save_new_builds failed: {e}", exc_info=True)
        raise
    finally:
        if close:
            db.close()

    return {"added": added, "skipped": skipped, "updated": updated}


def save_matches(matches, db=None):
    """Record matches, returning only the ones not already alerted on.

    A match is identified by (listing zpid/address, new_build id) so the
    daily job does not re-email the same pairing every morning.

    Returns [{"id": <builder_zone_matches.id>, "match": <match dict>}] for the
    pairings still needing an email. Ids, not ORM rows: the caller may close
    this session before it marks the alert sent, and a detached instance would
    raise on attribute access (which is exactly how the first version silently
    failed to set alert_sent and re-emailed every run).
    """
    from database import get_session, NewBuild, BuilderZoneMatch

    close = False
    if db is None:
        db = get_session()
        close = True

    pending = []
    try:
        for m in matches:
            listing = m["listing"]
            nb = m["new_build"]

            nb_row = None
            if nb.get("zpid"):
                nb_row = db.query(NewBuild).filter_by(zpid=nb["zpid"]).first()
            if nb_row is None:
                # The build was seen live this run but isn't stored (match job
                # ran before the scan job, or the scan errored). Store it now so
                # the match has a stable foreign key.
                res = save_new_builds([nb], db=db)
                logger.info(f"  [bz] stored unseen new build for match ({res})")
                if nb.get("zpid"):
                    nb_row = db.query(NewBuild).filter_by(zpid=nb["zpid"]).first()
            if nb_row is None:
                logger.warning(f"  [bz] could not resolve new build for {listing.get('address')} — skipping match")
                continue

            listing_key = _dedupe_key(listing)
            existing = (db.query(BuilderZoneMatch)
                        .filter_by(listing_key=listing_key, new_build_id=nb_row.id)
                        .first())
            if existing is not None:
                existing.last_seen = datetime.utcnow()
                existing.listing_price = listing.get("price")
                existing.distance_miles = m["distance_miles"]
                if existing.alert_sent:
                    continue
                pending.append((existing, m))
                continue

            row = BuilderZoneMatch(
                listing_key=listing_key,
                listing_zpid=listing.get("zpid"),
                listing_address=listing.get("address"),
                city=listing.get("city"),
                state=listing.get("state") or "CA",
                zip_code=listing.get("zip_code"),
                listing_price=listing.get("price"),
                latitude=listing.get("latitude"),
                longitude=listing.get("longitude"),
                home_type=listing.get("home_type"),
                listing_url=listing.get("listing_url"),
                days_on_zillow=listing.get("days_on_zillow"),
                new_build_id=nb_row.id,
                builder_name=nb_row.builder_name,
                new_build_address=nb_row.address,
                distance_miles=m["distance_miles"],
                alert_sent=False,
                date_found=datetime.utcnow(),
                last_seen=datetime.utcnow(),
            )
            db.add(row)
            pending.append((row, m))
        db.commit()
        # Read the ids while the session is still open — after commit the rows
        # are expired, and a refresh needs a live session.
        result = [{"id": row.id, "match": m} for row, m in pending]
    except Exception as e:
        db.rollback()
        logger.error(f"save_matches failed: {e}", exc_info=True)
        raise
    finally:
        if close:
            db.close()

    return result


# ── Email ─────────────────────────────────────────────────────────────

def _money(v):
    return f"${v:,.0f}" if v else "N/A"


def format_match_email(matches):
    """Build (subject, html, text) for a builder-zone digest.

    `matches` are plain match dicts: {"listing", "new_build", "distance_miles"}.
    """
    n = len(matches)
    radius = radius_miles()
    subject = f"{n} listing{'s' if n != 1 else ''} near new construction (Builder Zones)"

    rows = []
    text_lines = [f"{n} for-sale listing(s) within {radius} mi of a new build:", ""]
    for m in matches:
        l, nb, d = m["listing"], m["new_build"], m["distance_miles"]
        addr = f"{l.get('address','')}, {l.get('city','')} {l.get('zip_code','')}".strip()
        url = l.get("listing_url") or ""
        addr_cell = f"<a href='{url}'>{addr}</a>" if url else addr
        meta = " · ".join(str(x) for x in [
            f"{l['beds']:g} bd" if l.get("beds") else None,
            f"{l['baths']:g} ba" if l.get("baths") else None,
            f"{l['sqft']:,.0f} sqft" if l.get("sqft") else None,
            (l.get("home_type") or "").replace("_", " ").title() or None,
            f"{l['days_on_zillow']} days on Zillow" if l.get("days_on_zillow") is not None else None,
        ] if x)
        rows.append(
            "<tr>"
            f"<td><b>{addr_cell}</b><br><span style='color:#666;font-size:12px;'>{meta}</span></td>"
            f"<td>{_money(l.get('price'))}</td>"
            f"<td>{d:.2f} mi</td>"
            f"<td>{nb.get('builder_name') or 'Unknown builder'}<br>"
            f"<span style='color:#666;font-size:12px;'>{nb.get('address','')} — {_money(nb.get('price'))}</span></td>"
            "</tr>"
        )
        text_lines.append(
            f"- {addr} — {_money(l.get('price'))} — {d:.2f} mi from "
            f"{nb.get('builder_name') or 'Unknown builder'} ({nb.get('address','')})"
        )

    html = (
        "<html><body style='font-family:system-ui,Arial,sans-serif;'>"
        "<h2 style='color:#2563eb;'>Builder Zones — listings next to new construction</h2>"
        f"<p>{n} for-sale listing{'s' if n != 1 else ''} within "
        f"<b>{radius} miles</b> of an active new build.</p>"
        "<table border='1' cellpadding='8' style='border-collapse:collapse;font-size:14px;'>"
        "<tr style='background:#1e3a8a;color:white;'>"
        "<th align='left'>Listing</th><th align='left'>Price</th>"
        "<th align='left'>Distance</th><th align='left'>Nearby builder</th></tr>"
        + "".join(rows) +
        "</table>"
        "<p style='color:#666;font-size:12px;'>DealFlow Builder Zones · "
        f"source: OpenWeb Ninja · {datetime.utcnow().strftime('%Y-%m-%d')}</p>"
        "</body></html>"
    )
    return subject, html, "\n".join(text_lines)


def alert_recipients():
    """Who gets the Builder Zones digest.

    Deliberately its OWN variable, not the shared ALERT_EMAILS: builder-zone
    matches are a different audience from auction and counter-offer alerts.
    There is no fallback — an unset BUILDER_ZONE_ALERT_EMAILS means the digest
    does not send, and the matches keep alert_sent=false so they go out on the
    next run once the variable is set. Nothing is lost, but nothing arrives
    either, so set it before enabling the jobs.
    """
    raw = os.getenv("BUILDER_ZONE_ALERT_EMAILS", "")
    return [a.strip() for a in raw.split(",") if a.strip()]


def send_match_email(matches):
    """Send the digest via Resend. Returns (sent_ok, error)."""
    if not matches:
        return False, "no matches"
    from email_sender import send_via_resend

    recipients = alert_recipients()
    if not recipients:
        return False, "BUILDER_ZONE_ALERT_EMAILS not set"

    subject, html, text = format_match_email(matches)
    return send_via_resend(recipients, subject, html, text)


# ── Orchestration (shared by worker jobs and the MCP tool) ────────────

def scan_zip(zip_code, api_key, pages=None):
    """New builds + resale listings for one zip. Returns (new_builds, resale, error)."""
    items, err = search_zip(zip_code, api_key, pages=pages)
    if err:
        return [], [], err
    new_builds, resale = split_listings(items, zip_code)
    return new_builds, resale, None


def find_builder_zones_for_zip(zip_code, api_key, pages=None, radius=None):
    """Full read-only analysis of one zip — used by the MCP tool and tests."""
    new_builds, resale, err = scan_zip(zip_code, api_key, pages=pages)
    if err:
        return {"error": err}
    matches = match_listings(resale, new_builds, radius=radius)
    return {
        "zip": zip_code,
        "new_builds": new_builds,
        "resale_count": len(resale),
        "matches": matches,
        "radius_miles": radius if radius is not None else radius_miles(),
    }
