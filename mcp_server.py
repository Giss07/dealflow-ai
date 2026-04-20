"""
DealFlow MCP Server — Remote MCP server for Claude to search real estate listings.
"""

import os
import sys
import json
import logging
from datetime import datetime

os.environ['PYTHONUNBUFFERED'] = '1'

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

print("MCP server: importing FastMCP...", flush=True)

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("DealFlow AI")
print("MCP server: FastMCP initialized", flush=True)


# ── TOOLS ──────────────────────────────────────────────────────────────

@mcp.tool()
def search_zillow(
    zip_code: str,
    min_price: str = "0",
    max_price: str = "900000",
    max_results: str = "20",
) -> str:
    """Search Zillow listings in a zip code. Returns active for-sale listings with price, beds, baths, sqft, year built.

    Args:
        zip_code: 5-digit zip code to search
        min_price: Minimum listing price (default 0)
        max_price: Maximum listing price (default 900000)
        max_results: Maximum results to return (default 20)
    """
    import requests

    # Convert string params to numbers
    try:
        min_price_n = int(float(min_price or 0))
        max_price_n = int(float(max_price or 900000))
        max_results_n = int(float(max_results or 20))
    except:
        min_price_n, max_price_n, max_results_n = 0, 900000, 20

    APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
    print(f"search_zillow: zip={zip_code}, key={'set' if APIFY_API_KEY else 'MISSING'}", flush=True)

    if not APIFY_API_KEY:
        return json.dumps({"error": "APIFY_API_KEY not set on MCP server"})

    try:
        resp = requests.post(
            "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items",
            params={"token": APIFY_API_KEY},
            json={"zipCodes": [zip_code], "maxItems": 50},
            headers={"Content-Type": "application/json"}, timeout=120)

        print(f"search_zillow: Apify status={resp.status_code}", flush=True)

        if resp.status_code not in (200, 201):
            return json.dumps({"error": f"Apify {resp.status_code}", "details": resp.text[:200]})

        listings = []
        for item in resp.json():
            hi = item.get("hdpData", {}).get("homeInfo", {})
            status = (hi.get("homeStatus") or "").upper()
            if "FOR_SALE" not in status:
                continue
            if (item.get("statusText") or "").lower() == "auction":
                continue

            price = hi.get("price") or item.get("unformattedPrice") or 0
            try:
                price = float(str(price).replace("$", "").replace(",", ""))
            except:
                continue
            if price < min_price_n or price > max_price_n:
                continue

            listings.append({
                "address": f"{item.get('addressStreet', '')}, {item.get('addressCity', '')}, CA {zip_code}",
                "price": price,
                "bedrooms": hi.get("bedrooms"),
                "bathrooms": hi.get("bathrooms"),
                "sqft": hi.get("livingArea") or item.get("area"),
                "year_built": hi.get("yearBuilt"),
                "days_on_market": hi.get("daysOnZillow"),
                "zestimate": hi.get("zestimate"),
                "home_type": hi.get("homeType", ""),
            })
            if len(listings) >= max_results_n:
                break

        print(f"search_zillow: returning {len(listings)} listings", flush=True)
        return json.dumps({"zip_code": zip_code, "total": len(listings), "listings": listings}, indent=2)

    except Exception as e:
        import traceback
        print(f"search_zillow ERROR: {e}", flush=True)
        traceback.print_exc()
        return json.dumps({"error": str(e)})


@mcp.tool()
def check_mls_status(address: str, zip_code: str) -> str:
    """Check if a specific property is currently listed on Zillow.

    Args:
        address: Street address
        zip_code: 5-digit zip code
    """
    import requests

    APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
    if not APIFY_API_KEY:
        return json.dumps({"error": "APIFY_API_KEY not set"})

    try:
        resp = requests.post(
            "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items",
            params={"token": APIFY_API_KEY},
            json={"zipCodes": [zip_code], "maxItems": 50},
            headers={"Content-Type": "application/json"}, timeout=120)

        if resp.status_code not in (200, 201):
            return json.dumps({"error": f"Apify {resp.status_code}"})

        addr_parts = address.lower().split()
        for item in resp.json():
            item_addr = (item.get("addressStreet") or item.get("address") or "").lower()
            if len(addr_parts) >= 2 and addr_parts[0] in item_addr and addr_parts[1] in item_addr:
                hi = item.get("hdpData", {}).get("homeInfo", {})
                status = (hi.get("homeStatus") or "").upper()
                return json.dumps({
                    "found": True,
                    "address": item.get("address", address),
                    "status": "on-market" if "FOR_SALE" in status else "pending" if "PENDING" in status else status.lower(),
                    "price": hi.get("price"),
                    "zestimate": hi.get("zestimate"),
                    "bedrooms": hi.get("bedrooms"),
                    "bathrooms": hi.get("bathrooms"),
                    "sqft": hi.get("livingArea"),
                    "year_built": hi.get("yearBuilt"),
                    "days_on_market": hi.get("daysOnZillow"),
                }, indent=2)

        return json.dumps({"found": False, "message": f"Not found in {zip_code}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def score_deal(price: str, arv: str, sqft: str = "0", year_built: str = "0", days_on_market: str = "0", repair_estimate: str = "0") -> str:
    """Score a real estate deal for fix-and-flip profit potential (1-100).

    Args:
        price: Listing price
        arv: After Repair Value
        sqft: Square footage
        year_built: Year built
        days_on_market: Days on market
        repair_estimate: Estimated repair cost
    """
    from scorer import fallback_score
    from offer_calculator import calculate_offer

    p, a = float(price), float(arv)
    rep = float(repair_estimate or 0)
    listing = {
        "price": p, "arv": a, "sqft": float(sqft or 0),
        "days_on_zillow": int(float(days_on_market or 0)) or None,
        "year_built": int(float(year_built or 0)) or None,
        "repairs_mid": rep, "repairs_worst": rep * 1.3 if rep else 0,
        "has_deal_keywords": False, "matched_keywords": [], "photo_grades": {}, "description": "",
    }
    result = fallback_score(listing)
    listing["repair_estimate"] = {"total_mid": rep, "total_worst": rep * 1.3 if rep else 0}
    calculate_offer(listing)
    offer = listing.get("offer_analysis", {})

    out = {"score": result["score"], "reasoning": result["reasoning"],
           "arv_margin": f"{((a - p) / a * 100):.1f}%" if a > 0 else "N/A"}
    if "error" not in offer:
        out.update({"max_offer": offer.get("max_offer"), "estimated_profit": offer.get("estimated_profit"), "roi_pct": offer.get("roi_pct")})
    return json.dumps(out, indent=2)


@mcp.tool()
def get_deals(min_score: int = 0, zip_code: str = "", limit: int = 20) -> str:
    """Get deals from the DealFlow database.

    Args:
        min_score: Minimum score filter
        zip_code: Filter by zip code
        limit: Max results
    """
    from database import init_db, get_session, Deal
    init_db()
    db = get_session()
    try:
        query = db.query(Deal).filter((Deal.is_archived == False) | (Deal.is_archived == None))
        if min_score:
            query = query.filter(Deal.score >= min_score)
        if zip_code:
            query = query.filter(Deal.zip_code == zip_code)
        deals = query.order_by(Deal.score.desc()).limit(limit).all()
        return json.dumps({"total": len(deals), "deals": [
            {"address": f"{d.address}, {d.city} {d.zip_code}", "price": d.price, "arv": d.arv,
             "max_offer": d.max_offer, "profit": d.estimated_profit, "score": d.score}
            for d in deals
        ]}, indent=2)
    finally:
        db.close()


@mcp.tool()
def submit_offer(address: str, city: str, zip_code: str, offer_amount: str, arv: str, repairs: str = "0") -> str:
    """Submit an offer to the Google Sheet tracker.

    Args:
        address: Street address
        city: City name
        zip_code: Zip code
        offer_amount: Offer amount
        arv: After Repair Value
        repairs: Repair estimate
    """
    from sheets import write_offer_to_sheet
    amt, a, r = float(offer_amount), float(arv), float(repairs or 0)
    result = write_offer_to_sheet(
        {"address": address, "city": city, "state": "CA", "zip_code": zip_code,
         "arv": a, "price": amt, "repairs_mid": r, "repairs_worst": r},
        offer_amount=amt, offer_date=datetime.now().strftime("%Y-%m-%d"),
        offer_status="Submitted", offer_notes="Submitted via MCP")
    return json.dumps({"success": result, "address": f"{address}, {city}, CA {zip_code}", "offer": amt})


# ── SERVER ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", 8080))
    print(f"Starting MCP SSE server on port {PORT}...", flush=True)

    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.responses import PlainTextResponse
    import uvicorn

    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
            await mcp._mcp_server.run(
                streams[0], streams[1], mcp._mcp_server.create_initialization_options()
            )

    async def health(request):
        apify_set = "yes" if os.getenv("APIFY_API_KEY") else "NO"
        db_set = "yes" if os.getenv("DATABASE_URL") else "NO"
        return PlainTextResponse(f"ok | apify={apify_set} db={db_set}")

    app = Starlette(
        routes=[
            Route("/health", health),
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )

    print(f"MCP server ready — /health /sse /messages/ on port {PORT}", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
