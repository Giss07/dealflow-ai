"""
DealFlow MCP Server — Remote MCP server for Claude to search real estate listings.
Deployed on Railway, connects to Apify Zillow + DealFlow database.
Uses SSE transport for remote MCP connections.
"""

import os
import sys
import json
import logging
import re
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# MCP imports (Python 3.10+ only — runs on Railway's Python 3.11)
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "DealFlow AI",
    description="Search California real estate listings, check MLS status, score deals, and submit offers",
)


# ── TOOLS ──────────────────────────────────────────────────────────────

@mcp.tool()
def search_zillow(
    zip_code: str,
    min_price: int = 0,
    max_price: int = 900000,
    min_year: int = 0,
    max_year: int = 2025,
    max_results: int = 20,
) -> str:
    """Search Zillow listings in a California zip code.

    Args:
        zip_code: 5-digit zip code to search (e.g. "92704")
        min_price: Minimum listing price (default 0)
        max_price: Maximum listing price (default 900000)
        min_year: Minimum year built (default 0 = any)
        max_year: Maximum year built (default 2025)
        max_results: Maximum results to return (default 20)
    """
    import requests

    APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
    if not APIFY_API_KEY:
        return json.dumps({"error": "APIFY_API_KEY not configured"})

    api_url = "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items"

    try:
        resp = requests.post(
            api_url,
            params={"token": APIFY_API_KEY},
            json={"zipCodes": [zip_code], "maxItems": 200},
            headers={"Content-Type": "application/json"},
            timeout=300,
        )
        if resp.status_code not in (200, 201):
            return json.dumps({"error": f"Apify returned {resp.status_code}", "details": resp.text[:200]})

        all_results = resp.json()
        listings = []

        for item in all_results:
            hi = item.get("hdpData", {}).get("homeInfo", {})
            sub = hi.get("listing_sub_type", {}) or {}
            status_text = (item.get("statusText") or "").lower()
            home_status = (hi.get("homeStatus") or "").upper()

            if "FOR_SALE" not in home_status:
                continue
            if "auction" in status_text or sub.get("is_forAuction"):
                continue

            price = hi.get("price") or item.get("unformattedPrice") or 0
            try:
                price = float(str(price).replace("$", "").replace(",", ""))
            except:
                continue

            if price < min_price or price > max_price:
                continue

            year = hi.get("yearBuilt")
            if year and min_year > 0 and year < min_year:
                continue
            if year and year > max_year:
                continue

            address = item.get("addressStreet") or hi.get("streetAddress") or ""
            city = item.get("addressCity") or hi.get("city") or ""
            sqft = hi.get("livingArea") or item.get("area") or 0
            beds = hi.get("bedrooms") or item.get("beds")
            baths = hi.get("bathrooms") or item.get("baths")
            zestimate = hi.get("zestimate") or item.get("zestimate")
            days = hi.get("daysOnZillow")
            home_type = hi.get("homeType", "")
            zillow_url = "https://www.zillow.com" + item.get("detailUrl", "") if item.get("detailUrl", "").startswith("/") else item.get("detailUrl", "")

            listings.append({
                "address": f"{address}, {city}, CA {zip_code}",
                "price": price,
                "bedrooms": beds,
                "bathrooms": baths,
                "sqft": sqft,
                "year_built": year,
                "days_on_market": days,
                "zestimate": zestimate,
                "home_type": home_type,
                "zillow_url": zillow_url,
            })

            if len(listings) >= max_results:
                break

        return json.dumps({
            "zip_code": zip_code,
            "total_found": len(listings),
            "filters": {"price": f"${min_price:,}-${max_price:,}", "year": f"{min_year}-{max_year}"},
            "listings": listings,
        }, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def check_mls_status(address: str, zip_code: str) -> str:
    """Check if a specific property is currently listed on Zillow/MLS.

    Args:
        address: Street address (e.g. "123 Main St")
        zip_code: 5-digit zip code
    """
    import requests

    APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
    if not APIFY_API_KEY:
        return json.dumps({"error": "APIFY_API_KEY not configured"})

    api_url = "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items"

    try:
        resp = requests.post(
            api_url,
            params={"token": APIFY_API_KEY},
            json={"zipCodes": [zip_code], "maxItems": 50},
            headers={"Content-Type": "application/json"},
            timeout=300,
        )
        if resp.status_code not in (200, 201):
            return json.dumps({"error": f"Apify returned {resp.status_code}"})

        results = resp.json()
        addr_parts = address.lower().split()

        for item in results:
            item_addr = (item.get("addressStreet") or item.get("address") or "").lower()
            if len(addr_parts) >= 2 and addr_parts[0] in item_addr and addr_parts[1] in item_addr:
                hi = item.get("hdpData", {}).get("homeInfo", {})
                status_text = (item.get("statusText") or "").upper()
                home_status = (hi.get("homeStatus") or "").upper()
                price = hi.get("price") or item.get("unformattedPrice")
                zestimate = hi.get("zestimate")

                is_auction = "AUCTION" in status_text or (hi.get("listing_sub_type", {}) or {}).get("is_forAuction")

                return json.dumps({
                    "address": item.get("address", address),
                    "found": True,
                    "mls_status": "auction" if is_auction else "on-market" if "FOR_SALE" in home_status else "pending" if "PENDING" in home_status else home_status.lower(),
                    "price": price,
                    "zestimate": zestimate,
                    "bedrooms": hi.get("bedrooms"),
                    "bathrooms": hi.get("bathrooms"),
                    "sqft": hi.get("livingArea"),
                    "year_built": hi.get("yearBuilt"),
                    "days_on_market": hi.get("daysOnZillow"),
                    "home_type": hi.get("homeType"),
                    "zillow_url": "https://www.zillow.com" + item.get("detailUrl", ""),
                }, indent=2)

        return json.dumps({"address": address, "found": False, "message": f"Not found on Zillow in {zip_code} ({len(results)} listings checked)"})

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def score_deal(
    price: float,
    arv: float,
    sqft: float = 0,
    year_built: int = 0,
    days_on_market: int = 0,
    repair_estimate: float = 0,
) -> str:
    """Score a real estate deal for fix-and-flip profit potential (1-100).

    Args:
        price: Listing/purchase price
        arv: After Repair Value
        sqft: Square footage (0 if unknown)
        year_built: Year built (0 if unknown)
        days_on_market: Days on market (0 if unknown)
        repair_estimate: Estimated repair cost (0 if unknown)
    """
    from scorer import fallback_score
    from offer_calculator import calculate_offer

    listing = {
        "price": price, "arv": arv, "sqft": sqft,
        "days_on_zillow": days_on_market if days_on_market else None,
        "year_built": year_built if year_built else None,
        "repairs_mid": repair_estimate, "repairs_worst": repair_estimate * 1.3 if repair_estimate else 0,
        "has_deal_keywords": False, "matched_keywords": [],
        "photo_grades": {}, "description": "",
    }

    score_result = fallback_score(listing)

    # Calculate offer
    listing["repair_estimate"] = {"total_mid": repair_estimate or 0, "total_worst": repair_estimate * 1.3 if repair_estimate else 0}
    calculate_offer(listing)
    offer = listing.get("offer_analysis", {})

    result = {
        "score": score_result["score"],
        "reasoning": score_result["reasoning"],
        "arv": arv,
        "price": price,
        "arv_margin": f"{((arv - price) / arv * 100):.1f}%" if arv > 0 else "N/A",
    }

    if "error" not in offer:
        result.update({
            "max_offer": offer.get("max_offer"),
            "estimated_profit": offer.get("estimated_profit"),
            "roi_pct": offer.get("roi_pct"),
            "total_costs": offer.get("total_all_costs"),
        })

    return json.dumps(result, indent=2)


@mcp.tool()
def get_deals(
    min_score: int = 0,
    zip_code: str = "",
    limit: int = 20,
) -> str:
    """Get deals from the DealFlow database.

    Args:
        min_score: Minimum score filter (0 for all)
        zip_code: Filter by zip code (empty for all)
        limit: Maximum results (default 20)
    """
    from database import init_db, get_session, Deal

    init_db()
    db = get_session()
    try:
        query = db.query(Deal).filter(
            (Deal.is_archived == False) | (Deal.is_archived == None),
            (Deal.is_hidden == False) | (Deal.is_hidden == None),
        )
        if min_score:
            query = query.filter(Deal.score >= min_score)
        if zip_code:
            query = query.filter(Deal.zip_code == zip_code)

        query = query.order_by(Deal.score.desc()).limit(limit)
        deals = query.all()

        results = []
        for d in deals:
            results.append({
                "id": d.id,
                "address": f"{d.address}, {d.city}, {d.state} {d.zip_code}",
                "price": d.price,
                "arv": d.arv,
                "max_offer": d.max_offer,
                "estimated_profit": d.estimated_profit,
                "score": d.score,
                "score_reasoning": d.score_reasoning,
                "days_on_market": d.days_on_zillow,
                "year_built": d.year_built,
                "source": d.source,
            })

        return json.dumps({"total": len(results), "deals": results}, indent=2)
    finally:
        db.close()


@mcp.tool()
def submit_offer(
    address: str,
    city: str,
    state: str,
    zip_code: str,
    offer_amount: float,
    arv: float,
    repairs: float = 0,
) -> str:
    """Submit an offer to the Google Sheet tracker.

    Args:
        address: Street address
        city: City name
        state: State (default CA)
        zip_code: Zip code
        offer_amount: Offer amount in dollars
        arv: After Repair Value
        repairs: Estimated repair cost (0 if unknown)
    """
    from sheets import write_offer_to_sheet

    deal_dict = {
        "address": address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "arv": arv,
        "price": offer_amount,
        "estimated_profit": arv - offer_amount - repairs if repairs else None,
        "repairs_mid": repairs,
        "repairs_worst": repairs,
    }

    result = write_offer_to_sheet(
        deal_dict,
        offer_amount=offer_amount,
        offer_date=datetime.now().strftime("%Y-%m-%d"),
        offer_status="Submitted",
        offer_notes="Submitted via MCP",
    )

    return json.dumps({
        "success": result,
        "address": f"{address}, {city}, {state} {zip_code}",
        "offer_amount": offer_amount,
        "arv": arv,
        "repairs": repairs,
        "message": "Offer written to Google Sheet" if result else "Failed to write to Google Sheet",
    }, indent=2)


# ── RUN SERVER ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.responses import PlainTextResponse
    from mcp.server.sse import SseServerTransport

    port = int(os.getenv("PORT", 8080))
    logger.info(f"Starting DealFlow MCP Server on port {port}")

    # SSE transport
    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
            await mcp._mcp_server.run(
                streams[0], streams[1], mcp._mcp_server.create_initialization_options()
            )

    async def health(request):
        return PlainTextResponse("ok")

    app = Starlette(
        routes=[
            Route("/health", health),
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )

    logger.info(f"MCP server ready — SSE at /sse, health at /health")
    uvicorn.run(app, host="0.0.0.0", port=port)
