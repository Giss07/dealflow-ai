"""
DealFlow MCP Server — Remote MCP for Claude to search real estate listings.

Hardened Apify integration with retry, caching, and structured logging.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime

os.environ['PYTHONUNBUFFERED'] = '1'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("DealFlow AI")


# ── APIFY CACHE + RETRY ──────────────────────────────────────────────
#
# ROOT CAUSE (documented to prevent regression):
# MCP SDK 1.27 has DEFAULT_REQUEST_TIMEOUT_MSEC = 60000 (60s) hardcoded
# in the Protocol class. Claude Desktop's MCP client enforces this per
# tool call. Apify's zillow-zip-search actor takes 20-90s depending on
# zip code density. If the Apify call exceeds 60s, the client aborts
# with "Tool execution failed" before the server can respond.
#
# FIX: timeout=55s on each Apify attempt (leaves 5s buffer for JSON
# parsing + MCP framing). maxItems=25 keeps most calls under 30s.
# Cache prevents repeated calls for the same zip within an hour.
#
# ALSO: FastMCP 1.27 rejects tool calls when optional params are
# omitted. ALL tool params must be required (no default values).
# Return type annotations (-> str) must be omitted to avoid
# outputSchema validation errors.
# ──────────────────────────────────────────────────────────────────────

_apify_cache = {}  # {zip_code: (epoch_time, [raw_items])}
CACHE_TTL = 3600   # 1 hour

DISTRESSED_KEYWORDS = [
    "as-is", "as is", "fixer", "investor", "handyman", "estate",
    "probate", "needs work", "tlc", "cash only", "below market",
    "motivated", "must sell", "priced to sell", "short sale",
    "bank owned", "reo", "foreclosure", "auction", "deferred maintenance",
]


def _call_apify(zip_code, max_items=25):
    """Call Apify zillow-zip-search with retry + exponential backoff + caching.

    Returns a list of raw Apify item dicts on success, or a dict with
    "error" key on failure. All Apify-calling tools delegate here.
    """
    import requests

    # 1. Check cache
    if zip_code in _apify_cache:
        ts, items = _apify_cache[zip_code]
        age = int(time.time() - ts)
        if age < CACHE_TTL:
            logger.info(f"[apify] CACHE HIT zip={zip_code} age={age}s items={len(items)}")
            return items

    key = os.getenv("APIFY_API_KEY", "")
    if not key:
        return {"error": "APIFY_API_KEY not configured on server"}

    # 2. Retry loop with backoff
    last_error = None
    for attempt in range(3):
        try:
            start = time.time()
            r = requests.post(
                "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items",
                params={"token": key},
                json={"zipCodes": [zip_code], "maxItems": max_items},
                headers={"Content-Type": "application/json"},
                timeout=55,
            )
            elapsed = time.time() - start
            logger.info(f"[apify] zip={zip_code} attempt={attempt+1} status={r.status_code} time={elapsed:.1f}s")

            if r.status_code in (200, 201):
                items = r.json()
                _apify_cache[zip_code] = (time.time(), items)
                logger.info(f"[apify] zip={zip_code} cached {len(items)} items")
                return items

            if r.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning(f"[apify] rate limited (429), waiting {wait}s before retry")
                time.sleep(wait)
                continue

            # Other HTTP errors — don't retry
            return {"error": f"Apify returned HTTP {r.status_code}", "detail": r.text[:300]}

        except requests.exceptions.Timeout:
            last_error = "timeout"
            logger.warning(f"[apify] timeout on attempt {attempt+1}/3 zip={zip_code}")
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue

        except requests.exceptions.ConnectionError as e:
            last_error = str(e)[:200]
            logger.warning(f"[apify] connection error attempt {attempt+1}/3: {last_error}")
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue

    return {"error": f"All 3 Apify attempts failed (last: {last_error})"}


def _filter_for_sale(items):
    """Filter raw Apify items to active for-sale listings (not auctions)."""
    out = []
    for item in items:
        hi = item.get("hdpData", {}).get("homeInfo", {})
        if "FOR_SALE" not in (hi.get("homeStatus") or "").upper():
            continue
        if (item.get("statusText") or "").lower() == "auction":
            continue
        price = hi.get("price") or item.get("unformattedPrice") or 0
        try:
            price = float(str(price).replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            continue
        out.append({
            "raw": item,
            "hi": hi,
            "price": price,
        })
    return out


def _format_listing(item, hi, price, zip_code):
    """Format a single listing for tool output."""
    return {
        "address": f"{item.get('addressStreet', '')}, {item.get('addressCity', '')}, CA {zip_code}",
        "price": price,
        "beds": hi.get("bedrooms"),
        "baths": hi.get("bathrooms"),
        "sqft": hi.get("livingArea") or item.get("area"),
        "year": hi.get("yearBuilt"),
        "days": hi.get("daysOnZillow"),
        "zestimate": hi.get("zestimate"),
        "type": hi.get("homeType", ""),
    }


# ── TOOLS ─────────────────────────────────────────────────────────────


@mcp.tool()
def search_zillow(zip_code: str):
    """Search Zillow for homes currently for sale in a California zip code.
    Returns up to 20 active listings with price, beds, baths, sqft, year built, and days on market.
    Results are cached for 1 hour to save Apify credits.
    """
    logger.info(f"[search_zillow] zip={zip_code}")

    result = _call_apify(zip_code)
    if isinstance(result, dict) and "error" in result:
        return json.dumps({"status": "error", **result})

    listings = _filter_for_sale(result)
    out = [_format_listing(l["raw"], l["hi"], l["price"], zip_code) for l in listings[:20]]

    logger.info(f"[search_zillow] zip={zip_code} returning {len(out)}/{len(result)} listings")
    return json.dumps({"status": "ok", "zip": zip_code, "count": len(out), "listings": out})


@mcp.tool()
def search_distressed(zip_code: str):
    """Search for likely distressed/flip-candidate properties in a zip code.
    Filters for: 60+ days on market, price cuts, as-is/fixer/estate/probate
    language, and investor-friendly signals. Returns matches sorted by days
    on market (most stale first) with distress signals explained.
    Results are cached for 1 hour to save Apify credits.
    """
    logger.info(f"[search_distressed] zip={zip_code}")

    result = _call_apify(zip_code)
    if isinstance(result, dict) and "error" in result:
        return json.dumps({"status": "error", **result})

    listings = _filter_for_sale(result)
    distressed = []

    for l in listings:
        item, hi, price = l["raw"], l["hi"], l["price"]
        signals = []

        # High days on market
        days = hi.get("daysOnZillow")
        if days is not None and days >= 60:
            signals.append(f"On market {days} days")

        # Price below zestimate by >10%
        zest = hi.get("zestimate")
        if zest and price and zest > 0:
            discount = (zest - price) / zest
            if discount > 0.10:
                signals.append(f"Priced {discount:.0%} below Zestimate (${zest:,.0f})")

        # Price change/reduction
        price_change = hi.get("priceChange")
        if price_change and price_change < 0:
            signals.append(f"Price cut: ${abs(price_change):,.0f}")

        # Distressed keywords in description
        desc = (item.get("hdpData", {}).get("homeInfo", {}).get("description") or "").lower()
        # Also check the top-level description if present
        desc2 = (item.get("description") or "").lower()
        full_desc = desc + " " + desc2
        matched_kw = [kw for kw in DISTRESSED_KEYWORDS if kw in full_desc]
        if matched_kw:
            signals.append(f"Keywords: {', '.join(matched_kw)}")

        # Year built (pre-1980 = likely needs work)
        year = hi.get("yearBuilt")
        if year and year < 1980:
            signals.append(f"Built {year}")

        # Include if at least one strong signal (60+ days or keywords or price cut)
        has_dom = days is not None and days >= 60
        has_keywords = len(matched_kw) > 0
        has_price_cut = (price_change and price_change < 0) or (zest and price and zest > 0 and (zest - price) / zest > 0.10)
        if has_dom or has_keywords or has_price_cut:
            entry = _format_listing(item, hi, price, zip_code)
            entry["distress_signals"] = signals
            entry["signal_count"] = len(signals)
            distressed.append(entry)

    # Sort by days on market descending (most stale first)
    distressed.sort(key=lambda x: (x.get("days") or 0), reverse=True)

    logger.info(f"[search_distressed] zip={zip_code} found {len(distressed)} distressed out of {len(listings)} for-sale")
    return json.dumps({"status": "ok", "zip": zip_code, "count": len(distressed), "listings": distressed})


@mcp.tool()
def check_mls_status(address: str, zip_code: str):
    """Check if a specific property address is currently listed for sale on Zillow.
    Uses cached Apify results when available.
    """
    logger.info(f"[check_mls_status] {address}, {zip_code}")

    result = _call_apify(zip_code, max_items=20)
    if isinstance(result, dict) and "error" in result:
        return json.dumps({"status": "error", **result})

    parts = address.lower().replace(",", " ").split()
    match_parts = [p for p in parts if len(p) > 1][:3]
    logger.info(f"[check_mls_status] matching {match_parts} in {len(result)} listings")

    for item in result:
        addr = (item.get("addressStreet") or item.get("address") or "").lower()
        if len(match_parts) >= 2 and match_parts[0] in addr and match_parts[1] in addr:
            hi = item.get("hdpData", {}).get("homeInfo", {})
            st = (hi.get("homeStatus") or "").upper()
            logger.info(f"[check_mls_status] FOUND: {item.get('address')} = {st}")
            return json.dumps({
                "found": True,
                "address": item.get("address", address),
                "status": "for-sale" if "FOR_SALE" in st else "pending" if "PENDING" in st else st.lower(),
                "price": hi.get("price"),
                "zestimate": hi.get("zestimate"),
                "beds": hi.get("bedrooms"),
                "baths": hi.get("bathrooms"),
                "sqft": hi.get("livingArea"),
                "year": hi.get("yearBuilt"),
                "days": hi.get("daysOnZillow"),
            })

    logger.info(f"[check_mls_status] NOT FOUND in {len(result)} listings")
    return json.dumps({"found": False, "message": f"Not found in {len(result)} listings for {zip_code}"})


@mcp.tool()
def score_deal(price: str, arv: str, rehab_estimate: str, holding_months: str, interest_rate: str, selling_cost_pct: str, target_profit_pct: str):
    """Score a real estate deal 1-100 for fix-and-flip profit potential.
    Returns score, reasoning, max offer, estimated profit, and ROI.
    Uses the iterative offer formula from offer_calculator.py with
    cost assumptions you can override per deal.

    Default values (pass these if the user doesn't specify):
      rehab_estimate: "0"
      holding_months: "3"
      interest_rate: "0.12" (12% annual hard money rate)
      selling_cost_pct: "0.05" (5% = 2% listing + 2% buyer agent + 1% closing)
      target_profit_pct: "0.10" (10% of ARV)
    """
    logger.info(f"[score_deal] price={price} arv={arv} rehab={rehab_estimate} hold={holding_months} int={interest_rate} sell={selling_cost_pct} profit={target_profit_pct}")
    try:
        from scorer import fallback_score
        from offer_calculator import calculate_offer

        p = float(price)
        a = float(arv)
        rehab = float(rehab_estimate)
        hold = int(float(holding_months))
        interest = float(interest_rate)
        sell_pct = float(selling_cost_pct)
        profit_pct = float(target_profit_pct)

        listing = {
            "price": p, "arv": a, "sqft": 0,
            "days_on_zillow": None, "year_built": None,
            "repairs_mid": rehab, "repairs_worst": rehab,
            "has_deal_keywords": False, "matched_keywords": [],
            "photo_grades": {}, "description": "",
        }
        result = fallback_score(listing)

        listing["repair_estimate"] = {"total_mid": rehab, "total_worst": rehab}
        overrides = {
            "hold_months": hold,
            "interest_rate": interest,
            "selling_cost_pct": sell_pct,
            "target_profit_pct": profit_pct,
        }
        calculate_offer(listing, overrides=overrides)
        offer = listing.get("offer_analysis", {})

        out = {
            "score": result["score"],
            "reasoning": result["reasoning"],
            "margin": f"{((a - p) / a * 100):.1f}%" if a > 0 else "N/A",
            "assumptions": {
                "rehab_estimate": rehab,
                "holding_months": hold,
                "interest_rate": interest,
                "selling_cost_pct": sell_pct,
                "target_profit_pct": profit_pct,
            },
        }
        if "error" not in offer:
            out["max_offer"] = offer.get("max_offer")
            out["profit"] = offer.get("estimated_profit")
            out["roi"] = offer.get("roi_pct")
        return json.dumps(out)

    except ValueError as e:
        return json.dumps({"status": "error", "error": f"Invalid number: {e}"})
    except Exception as e:
        logger.error(f"[score_deal] ERROR: {e}")
        return json.dumps({"status": "error", "error": f"{type(e).__name__}: {e}"})


@mcp.tool()
def get_deals(zip_code: str):
    """Get top 20 deals from the DealFlow database. Pass a zip code to filter, or pass empty string for all deals."""
    logger.info(f"[get_deals] zip={zip_code}")
    try:
        from database import init_db, get_session, Deal
        init_db()
        db = get_session()
        q = db.query(Deal).filter((Deal.is_archived == False) | (Deal.is_archived == None))
        if zip_code:
            q = q.filter(Deal.zip_code == zip_code)
        deals = q.order_by(Deal.score.desc()).limit(20).all()
        out = [{"address": f"{d.address}, {d.city} {d.zip_code}", "price": d.price,
                "arv": d.arv, "offer": d.max_offer, "profit": d.estimated_profit,
                "score": d.score} for d in deals]
        db.close()
        return json.dumps({"status": "ok", "count": len(out), "deals": out})
    except Exception as e:
        logger.error(f"[get_deals] ERROR: {e}")
        return json.dumps({"status": "error", "error": f"{type(e).__name__}: {e}"})


@mcp.tool()
def submit_offer(address: str, city: str, zip_code: str, offer_amount: str, arv: str):
    """Submit a real estate offer to the Google Sheet tracker."""
    logger.info(f"[submit_offer] {address}, {city} {zip_code} ${offer_amount}")
    try:
        from sheets import write_offer_to_sheet
        amt, a = float(offer_amount), float(arv)
        ok = write_offer_to_sheet(
            {"address": address, "city": city, "state": "CA", "zip_code": zip_code,
             "arv": a, "price": amt, "repairs_mid": 0, "repairs_worst": 0},
            offer_amount=amt, offer_date=datetime.now().strftime("%Y-%m-%d"),
            offer_status="Submitted", offer_notes="Via MCP")
        return json.dumps({"status": "ok", "success": ok, "address": f"{address}, {city} {zip_code}", "offer": amt})
    except Exception as e:
        logger.error(f"[submit_offer] ERROR: {e}")
        return json.dumps({"status": "error", "error": f"{type(e).__name__}: {e}"})


# ── SERVER ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", 8080))

    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.responses import PlainTextResponse
    import uvicorn

    # --- Health endpoint (shared by both transports) ---
    async def health(request):
        k = "yes" if os.getenv("APIFY_API_KEY") else "NO"
        sha = "unknown"
        try:
            with open(os.path.join(os.path.dirname(__file__), ".build_sha")) as f:
                sha = f.read().strip()
        except:
            pass
        try:
            import mcp as _mcp
            mcpv = getattr(_mcp, "__version__", "?")
        except:
            mcpv = "?"
        cache_zips = list(_apify_cache.keys())
        return PlainTextResponse(f"ok sha={sha} apify={k} mcp={mcpv} cached={cache_zips}")

    # --- Legacy SSE transport (kept for rollback, remove once /mcp is proven) ---
    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        from starlette.responses import Response
        async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
            await mcp._mcp_server.run(streams[0], streams[1], mcp._mcp_server.create_initialization_options())
        return Response()

    # --- Streamable HTTP transport at /mcp (stateless, survives deploys) ---
    # Uses the low-level Server.streamable_http_app() which returns a full
    # Starlette app with session management built in. We add our custom
    # routes (health, SSE) alongside the /mcp endpoint.
    try:
        from mcp.server.transport_security import TransportSecuritySettings

        # Disable DNS rebinding protection — server runs on Railway, not localhost
        security = TransportSecuritySettings(
            enable_dns_rebinding_protection=False,
        )

        app = mcp._mcp_server.streamable_http_app(
            streamable_http_path="/mcp",
            json_response=False,
            stateless_http=True,
            transport_security=security,
            host="0.0.0.0",
            custom_starlette_routes=[
                Route("/health", health),
                Route("/sse", endpoint=handle_sse, methods=["GET"]),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )
        logger.info(f"MCP server on port {PORT} — /mcp (streamable HTTP) + /sse (legacy) + /health")

    except Exception as e:
        # Fallback: if streamable HTTP isn't available in this SDK version,
        # use the SSE-only server (same as before)
        logger.warning(f"Streamable HTTP not available ({e}), falling back to SSE-only")
        app = Starlette(routes=[
            Route("/health", health),
            Route("/sse", endpoint=handle_sse, methods=["GET"]),
            Mount("/messages/", app=sse.handle_post_message),
        ])
        logger.info(f"MCP server on port {PORT} — /sse + /health (SSE-only fallback)")

    uvicorn.run(app, host="0.0.0.0", port=PORT)
