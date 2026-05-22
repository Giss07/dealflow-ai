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

from distress_keywords import DISTRESS_KEYWORDS

os.environ['PYTHONUNBUFFERED'] = '1'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("DealFlow AI")


# ── OPENWEB NINJA ─────────────────────────────────────────────────────
# All Zillow lookups use OpenWeb Ninja Real-Time Zillow Data API.
# $0.0025/call, 1-2s response, direct address/zip lookup.
#
# NOTE: FastMCP 1.27 rejects tool calls when optional params are
# omitted. ALL tool params must be required (no default values).
# Return type annotations (-> str) must be omitted to avoid
# outputSchema validation errors.

CACHE_TTL = 3600   # 1 hour

_owin_cache = {}  # {cache_key: (epoch_time, data)}

def _call_openweb_ninja_search(zip_code, status="FOR_SALE"):
    """Search listings by zip code via OpenWeb Ninja.

    Returns list of listing dicts on success, or dict with "error" key on failure.
    Results cached for 1 hour.
    """
    import requests

    cache_key = f"{zip_code}:{status}"
    if cache_key in _owin_cache:
        ts, items = _owin_cache[cache_key]
        age = int(time.time() - ts)
        if age < CACHE_TTL:
            logger.info(f"[owin] CACHE HIT zip={zip_code} status={status} age={age}s items={len(items)}")
            return items

    key = os.getenv("OPENWEB_NINJA_API_KEY", "")
    if not key:
        return {"error": "OPENWEB_NINJA_API_KEY not configured on server"}

    for attempt in range(3):
        try:
            start = time.time()
            r = requests.get(
                "https://api.openwebninja.com/realtime-zillow-data/search",
                params={"location": zip_code, "status": status},
                headers={"x-api-key": key},
                timeout=30,
            )
            elapsed = time.time() - start
            logger.info(f"[owin] zip={zip_code} attempt={attempt+1} status={r.status_code} time={elapsed:.1f}s")

            if r.status_code in (200, 201):
                body = r.json()
                items = body.get("data", [])
                if isinstance(items, list):
                    _owin_cache[cache_key] = (time.time(), items)
                    logger.info(f"[owin] zip={zip_code} cached {len(items)} items")
                    return items
                return {"error": "Unexpected response format"}

            if r.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning(f"[owin] rate limited (429), waiting {wait}s")
                time.sleep(wait)
                continue

            return {"error": f"OpenWeb Ninja HTTP {r.status_code}", "detail": r.text[:300]}

        except requests.exceptions.Timeout:
            logger.warning(f"[owin] timeout attempt {attempt+1}/3 zip={zip_code}")
            if attempt < 2:
                time.sleep(2 ** attempt)
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"[owin] connection error attempt {attempt+1}/3: {str(e)[:100]}")
            if attempt < 2:
                time.sleep(2 ** attempt)

    return {"error": "All 3 OpenWeb Ninja attempts failed"}


def _call_openweb_ninja_address(address):
    """Look up a single property by address via OpenWeb Ninja.

    Returns property dict on success, None if not found, or dict with "error" key.
    """
    import requests

    key = os.getenv("OPENWEB_NINJA_API_KEY", "")
    if not key:
        return {"error": "OPENWEB_NINJA_API_KEY not configured on server"}

    try:
        r = requests.get(
            "https://api.openwebninja.com/realtime-zillow-data/property-details-address",
            params={"address": address},
            headers={"x-api-key": key},
            timeout=30,
        )
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            return {"error": "OpenWeb Ninja rate limited (429)"}
        if r.status_code not in (200, 201):
            return {"error": f"OpenWeb Ninja HTTP {r.status_code}"}

        body = r.json()
        data = body.get("data") or body
        if not data or not data.get("zpid"):
            return None
        return data
    except Exception as e:
        return {"error": f"OpenWeb Ninja error: {str(e)[:100]}"}


def _owin_format_listing(item, zip_code):
    """Format an OpenWeb Ninja search result for tool output."""
    hi = item.get("hdpData", {}).get("homeInfo", {})
    return {
        "address": f"{item.get('streetAddress', '')}, {item.get('city', '')}, {item.get('state', 'CA')} {zip_code}",
        "price": item.get("price") or item.get("unformattedPrice") or 0,
        "beds": item.get("bedrooms") or item.get("beds"),
        "baths": item.get("bathrooms") or item.get("baths"),
        "sqft": item.get("livingArea") or item.get("area"),
        "year": hi.get("yearBuilt"),
        "days": item.get("daysOnZillow") or hi.get("daysOnZillow"),
        "zestimate": item.get("zestimate"),
        "type": item.get("homeType", ""),
    }


# ── TOOLS ─────────────────────────────────────────────────────────────


@mcp.tool()
def search_zillow(zip_code: str):
    """Search Zillow for homes currently for sale in a California zip code.
    Returns up to 20 active listings with price, beds, baths, sqft, year built, and days on market.
    Results are cached for 1 hour.
    """
    logger.info(f"[search_zillow] zip={zip_code}")

    result = _call_openweb_ninja_search(zip_code, "FOR_SALE")
    if isinstance(result, dict) and "error" in result:
        return json.dumps({"status": "error", **result})
    out = [_owin_format_listing(item, zip_code) for item in result[:20]
           if (item.get("homeStatus") or "").upper() == "FOR_SALE"
           and (item.get("statusText") or "").lower() != "auction"]
    logger.info(f"[search_zillow] zip={zip_code} returning {len(out)}/{len(result)} listings")
    return json.dumps({"status": "ok", "zip": zip_code, "count": len(out), "listings": out})


@mcp.tool()
def search_distressed(zip_code: str):
    """Search for likely distressed/flip-candidate properties in a zip code.
    Filters for: 60+ days on market, price cuts, as-is/fixer/estate/probate
    language, and investor-friendly signals. Returns matches sorted by days
    on market (most stale first) with distress signals explained.
    Results are cached for 1 hour.
    """
    logger.info(f"[search_distressed] zip={zip_code}")

    result = _call_openweb_ninja_search(zip_code, "FOR_SALE")
    if isinstance(result, dict) and "error" in result:
        return json.dumps({"status": "error", **result})

    # Normalize items to a common format for distress analysis
    distressed = []
    for item in result:
        hi = item.get("hdpData", {}).get("homeInfo", {})
        home_status = (hi.get("homeStatus") or item.get("homeStatus") or "").upper()
        if "FOR_SALE" not in home_status:
            continue
        if (item.get("statusText") or "").lower() == "auction":
            continue

        price = item.get("price") or hi.get("price") or item.get("unformattedPrice") or 0
        try:
            price = float(str(price).replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            continue

        signals = []

        days = item.get("daysOnZillow") or hi.get("daysOnZillow")
        if days is not None and days >= 60:
            signals.append(f"On market {days} days")

        zest = item.get("zestimate") or hi.get("zestimate")
        if zest and price and zest > 0:
            discount = (zest - price) / zest
            if discount > 0.10:
                signals.append(f"Priced {discount:.0%} below Zestimate (${zest:,.0f})")

        price_change = hi.get("priceChange") or item.get("priceChange")
        if price_change and price_change < 0:
            signals.append(f"Price cut: ${abs(price_change):,.0f}")

        desc = (hi.get("description") or item.get("description") or "").lower()
        matched_kw = [kw for kw in DISTRESS_KEYWORDS if kw in desc]
        if matched_kw:
            signals.append(f"Keywords: {', '.join(matched_kw)}")

        year = hi.get("yearBuilt") or item.get("yearBuilt")
        if year and year < 1980:
            signals.append(f"Built {year}")

        has_dom = days is not None and days >= 60
        has_keywords = len(matched_kw) > 0
        has_price_cut = (price_change and price_change < 0) or (zest and price and zest > 0 and (zest - price) / zest > 0.10)
        if has_dom or has_keywords or has_price_cut:
            entry = _owin_format_listing(item, zip_code)
            entry["distress_signals"] = signals
            entry["signal_count"] = len(signals)
            distressed.append(entry)

    distressed.sort(key=lambda x: (x.get("days") or 0), reverse=True)

    logger.info(f"[search_distressed] zip={zip_code} found {len(distressed)} distressed")
    return json.dumps({"status": "ok", "zip": zip_code, "count": len(distressed), "listings": distressed})


@mcp.tool()
def search_investor_listings(zip_code: str):
    """Test stub for tool registration."""
    return json.dumps({"status": "ok", "test": "stub_registered", "zip": zip_code})


@mcp.tool()
def check_mls_status(address: str, zip_code: str):
    """Check if a specific property address is currently listed for sale on Zillow."""
    logger.info(f"[check_mls_status] {address}, {zip_code}")

    full_addr = f"{address}, CA {zip_code}".strip()
    data = _call_openweb_ninja_address(full_addr)
    if isinstance(data, dict) and "error" in data:
        return json.dumps({"status": "error", **data})
    if not data:
        return json.dumps({"found": False, "message": f"Not found on Zillow"})
    home_status = (data.get("homeStatus") or "").upper()
    days = data.get("daysOnZillow")
    if "FOR_SALE" in home_status and days is not None:
        status = "for-sale"
    elif "PENDING" in home_status:
        status = "pending"
    elif "SOLD" in home_status or "RECENTLY_SOLD" in home_status:
        status = "sold"
    elif "OTHER" in home_status or "OFF_MARKET" in home_status:
        status = "off-market"
    else:
        status = "off-market"
    return json.dumps({
        "found": True,
        "address": data.get("streetAddress") or data.get("address") or address,
        "status": status,
        "price": data.get("price"),
        "zestimate": data.get("zestimate"),
        "beds": data.get("bedrooms"),
        "baths": data.get("bathrooms"),
        "sqft": data.get("livingArea"),
        "year": data.get("yearBuilt"),
        "days": data.get("daysOnZillow"),
    })


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

    import contextlib
    from mcp.server.sse import SseServerTransport
    from mcp.server.streamable_http import StreamableHTTPServerTransport
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from mcp.server.transport_security import TransportSecuritySettings
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.responses import PlainTextResponse
    import uvicorn

    # --- Health endpoint ---
    async def health(request):
        k = "yes" if os.getenv("OPENWEB_NINJA_API_KEY") else "NO"
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
        cache_zips = list(_owin_cache.keys())
        return PlainTextResponse(f"ok sha={sha} owin={k} mcp={mcpv} cached={cache_zips}")

    # --- Legacy SSE transport (kept for rollback) ---
    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        from starlette.responses import Response
        async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
            await mcp._mcp_server.run(streams[0], streams[1], mcp._mcp_server.create_initialization_options())
        return Response()

    # --- Streamable HTTP transport at /mcp (stateless, survives deploys) ---
    # Disable DNS rebinding protection — server runs on Railway, not localhost
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    )

    session_manager = StreamableHTTPSessionManager(
        app=mcp._mcp_server,
        stateless=True,
        json_response=False,
        security_settings=security,
    )

    async def handle_mcp(request):
        await session_manager.handle_request(
            request.scope, request.receive, request._send,
        )

    @contextlib.asynccontextmanager
    async def lifespan(app):
        async with session_manager.run():
            logger.info("StreamableHTTP session manager started")
            yield

    app = Starlette(
        routes=[
            Route("/health", health),
            Route("/mcp", endpoint=handle_mcp, methods=["GET", "POST", "DELETE"]),
            Route("/sse", endpoint=handle_sse, methods=["GET"]),
            Mount("/messages/", app=sse.handle_post_message),
        ],
        lifespan=lifespan,
    )

    logger.info(f"MCP server on port {PORT} — /mcp (streamable HTTP) + /sse (legacy) + /health")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
