"""
DealFlow MCP Server — Remote MCP for Claude to search real estate listings.
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

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("DealFlow AI")


@mcp.tool()
def search_zillow(zip_code: str):
    """Search Zillow for homes currently for sale in a California zip code.
    Returns up to 20 active listings with price, beds, baths, sqft, year built, and days on market.
    """
    import requests
    print(f"[search_zillow] zip={zip_code}", flush=True)

    key = os.getenv("APIFY_API_KEY", "")
    if not key:
        return json.dumps({"status": "error", "error": "APIFY_API_KEY not configured on server"})

    try:
        r = requests.post(
            "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items",
            params={"token": key},
            json={"zipCodes": [zip_code], "maxItems": 25},
            headers={"Content-Type": "application/json"},
            timeout=55,
        )
        print(f"[search_zillow] apify status={r.status_code}", flush=True)
        if r.status_code not in (200, 201):
            return json.dumps({"status": "error", "error": f"Apify returned HTTP {r.status_code}", "detail": r.text[:300]})

        out = []
        for item in r.json():
            hi = item.get("hdpData", {}).get("homeInfo", {})
            if "FOR_SALE" not in (hi.get("homeStatus") or "").upper():
                continue
            if (item.get("statusText") or "").lower() == "auction":
                continue
            price = hi.get("price") or item.get("unformattedPrice") or 0
            try:
                price = float(str(price).replace("$", "").replace(",", ""))
            except:
                continue
            out.append({
                "address": f"{item.get('addressStreet','')}, {item.get('addressCity','')}, CA {zip_code}",
                "price": price,
                "beds": hi.get("bedrooms"),
                "baths": hi.get("bathrooms"),
                "sqft": hi.get("livingArea") or item.get("area"),
                "year": hi.get("yearBuilt"),
                "days": hi.get("daysOnZillow"),
                "zestimate": hi.get("zestimate"),
                "type": hi.get("homeType", ""),
            })
            if len(out) >= 20:
                break

        print(f"[search_zillow] returning {len(out)} listings", flush=True)
        return json.dumps({"status": "ok", "zip": zip_code, "count": len(out), "listings": out})

    except requests.exceptions.Timeout:
        print(f"[search_zillow] TIMEOUT", flush=True)
        return json.dumps({"status": "error", "error": "Apify timed out after 120s", "zip": zip_code})
    except requests.exceptions.ConnectionError as e:
        print(f"[search_zillow] CONNECTION ERROR: {e}", flush=True)
        return json.dumps({"status": "error", "error": "Connection to Apify failed", "detail": str(e)[:200]})
    except Exception as e:
        print(f"[search_zillow] ERROR: {e}", flush=True)
        return json.dumps({"status": "error", "error": f"{type(e).__name__}: {e}"})


@mcp.tool()
def check_mls_status(address: str, zip_code: str):
    """Check if a specific property address is currently listed for sale on Zillow."""
    import requests
    print(f"[check_mls_status] {address}, {zip_code}", flush=True)

    key = os.getenv("APIFY_API_KEY", "")
    if not key:
        return json.dumps({"status": "error", "error": "APIFY_API_KEY not configured"})

    try:
        r = requests.post(
            "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items",
            params={"token": key},
            json={"zipCodes": [zip_code], "maxItems": 20},
            headers={"Content-Type": "application/json"},
            timeout=90,
        )
        print(f"[check_mls_status] apify {r.status_code}", flush=True)
        if r.status_code not in (200, 201):
            return json.dumps({"status": "error", "error": f"Apify returned HTTP {r.status_code}", "detail": r.text[:200]})

        data = r.json()
        parts = address.lower().replace(",", " ").split()
        match_parts = [p for p in parts if len(p) > 1][:3]
        print(f"[check_mls_status] matching {match_parts} in {len(data)} listings", flush=True)

        for item in data:
            addr = (item.get("addressStreet") or item.get("address") or "").lower()
            if len(match_parts) >= 2 and match_parts[0] in addr and match_parts[1] in addr:
                hi = item.get("hdpData", {}).get("homeInfo", {})
                st = (hi.get("homeStatus") or "").upper()
                print(f"[check_mls_status] FOUND: {item.get('address')} = {st}", flush=True)
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

        print(f"[check_mls_status] NOT FOUND", flush=True)
        return json.dumps({"found": False, "message": f"Not found in {len(data)} listings for {zip_code}"})

    except requests.exceptions.Timeout:
        print(f"[check_mls_status] TIMEOUT", flush=True)
        return json.dumps({"status": "error", "error": "Apify timed out after 90s", "address": address})
    except Exception as e:
        print(f"[check_mls_status] ERROR: {e}", flush=True)
        return json.dumps({"status": "error", "error": f"{type(e).__name__}: {e}"})


@mcp.tool()
def score_deal(price: str, arv: str):
    """Score a real estate deal 1-100 for fix-and-flip profit potential. Returns score, reasoning, max offer, and estimated profit."""
    print(f"[score_deal] price={price} arv={arv}", flush=True)
    try:
        from scorer import fallback_score
        from offer_calculator import calculate_offer

        p, a = float(price), float(arv)
        listing = {
            "price": p, "arv": a, "sqft": 0,
            "days_on_zillow": None, "year_built": None,
            "repairs_mid": 0, "repairs_worst": 0,
            "has_deal_keywords": False, "matched_keywords": [],
            "photo_grades": {}, "description": "",
        }
        result = fallback_score(listing)
        listing["repair_estimate"] = {"total_mid": 0, "total_worst": 0}
        calculate_offer(listing)
        offer = listing.get("offer_analysis", {})

        out = {"score": result["score"], "reasoning": result["reasoning"],
               "margin": f"{((a-p)/a*100):.1f}%" if a > 0 else "N/A"}
        if "error" not in offer:
            out["max_offer"] = offer.get("max_offer")
            out["profit"] = offer.get("estimated_profit")
            out["roi"] = offer.get("roi_pct")
        return json.dumps(out)

    except Exception as e:
        return json.dumps({"status": "error", "error": f"{type(e).__name__}: {e}"})


@mcp.tool()
def get_deals(zip_code: str):
    """Get top 20 deals from the DealFlow database. Pass a zip code to filter, or pass empty string for all deals."""
    print(f"[get_deals] zip={zip_code}", flush=True)
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
        print(f"[get_deals] ERROR: {e}", flush=True)
        return json.dumps({"status": "error", "error": f"{type(e).__name__}: {e}"})


@mcp.tool()
def submit_offer(address: str, city: str, zip_code: str, offer_amount: str, arv: str):
    """Submit a real estate offer to the Google Sheet tracker."""
    print(f"[submit_offer] {address}, {city} {zip_code} ${offer_amount}", flush=True)
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
        print(f"[submit_offer] ERROR: {e}", flush=True)
        return json.dumps({"status": "error", "error": f"{type(e).__name__}: {e}"})


# ── SERVER ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", 8080))

    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.responses import PlainTextResponse
    import uvicorn

    sse = SseServerTransport("/messages/")

    async def handle_sse(request):
        from starlette.responses import Response
        async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
            await mcp._mcp_server.run(streams[0], streams[1], mcp._mcp_server.create_initialization_options())
        return Response()

    async def health(request):
        k = "yes" if os.getenv("APIFY_API_KEY") else "NO"
        sha = "unknown"
        try:
            with open(os.path.join(os.path.dirname(__file__), ".build_sha")) as f:
                sha = f.read().strip()
        except:
            pass
        try:
            import mcp
            mcpv = getattr(mcp, "__version__", "?")
        except:
            mcpv = "?"
        return PlainTextResponse(f"ok sha={sha} apify={k} mcp={mcpv}")

    app = Starlette(routes=[
        Route("/health", health),
        Route("/sse", endpoint=handle_sse, methods=["GET"]),
        Mount("/messages/", app=sse.handle_post_message),
    ])

    print(f"MCP server on port {PORT} — /health /sse /messages/", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
