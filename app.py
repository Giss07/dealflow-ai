"""
DealFlow Flask App — Serves the dashboard and API endpoints.
"""

import os
import json
import logging
import time
from datetime import timedelta
from dotenv import load_dotenv
load_dotenv()
from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from functools import wraps
from database import init_db, get_session, get_all_deals, deal_to_dict, Deal

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

logger.info(f"Starting DealFlow app, PORT={os.getenv('PORT', 'not set')}")

app = Flask(__name__, template_folder="dashboard", static_folder="dashboard/static")
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(32).hex())
app.permanent_session_lifetime = timedelta(days=7)

DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "dealflow2026")

# Rate limiting for login
login_attempts = {}  # ip -> {"count": N, "locked_until": timestamp}
MAX_ATTEMPTS = 5
LOCKOUT_SECONDS = 900  # 15 minutes


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        if session.get("authenticated"):
            return f(*args, **kwargs)
        if request.path.startswith("/api/"):
            return jsonify({"error": "Unauthorized"}), 401
        return redirect(url_for("login"))
    return decorated


# Initialize database on startup
try:
    init_db()
    logger.info("Database initialized")
except Exception as e:
    logger.warning(f"DB init on startup: {e}")


@app.before_request
def check_auth():
    """Require login for all routes except health, login, and static."""
    open_paths = {"/health", "/login", "/logout"}
    if not DASHBOARD_PASSWORD:
        return
    if request.path in open_paths or request.path.startswith("/static/"):
        return
    if not session.get("authenticated"):
        if request.path.startswith("/api/"):
            return jsonify({"error": "Unauthorized"}), 401
        return redirect(url_for("login"))


@app.route("/health")
def health():
    """Healthcheck endpoint for Railway."""
    return "ok", 200


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("authenticated"):
        return redirect("/")
    error = ""
    if request.method == "POST":
        ip = request.remote_addr
        now = time.time()
        info = login_attempts.get(ip, {"count": 0, "locked_until": 0})
        if info["locked_until"] > now:
            remaining = int(info["locked_until"] - now)
            error = f"Too many attempts. Try again in {remaining // 60}m {remaining % 60}s."
        elif request.form.get("password") == DASHBOARD_PASSWORD:
            session.permanent = True
            session["authenticated"] = True
            login_attempts.pop(ip, None)
            return redirect("/")
        else:
            info["count"] += 1
            if info["count"] >= MAX_ATTEMPTS:
                info["locked_until"] = now + LOCKOUT_SECONDS
                error = "Too many failed attempts. Locked for 15 minutes."
            else:
                error = f"Wrong password. {MAX_ATTEMPTS - info['count']} attempts remaining."
            login_attempts[ip] = info
    return f'''<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#3b82f6"><title>DealFlow AI — Login</title></head>
<body style="font-family:-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;">
<div style="background:#1e293b;padding:40px;border-radius:12px;width:100%;max-width:380px;border:1px solid #334155;">
<h1 style="text-align:center;margin:0 0 8px;font-size:24px;">DealFlow <span style="color:#3b82f6;">AI</span></h1>
<p style="text-align:center;color:#94a3b8;margin:0 0 24px;font-size:14px;">Inland Empire Deal Finder</p>
{"<p style='color:#fca5a5;background:#991b1b;padding:10px;border-radius:6px;font-size:13px;text-align:center;'>" + error + "</p>" if error else ""}
<form method="POST"><input type="password" name="password" placeholder="Enter password" autofocus
style="width:100%;padding:12px;background:#334155;border:1px solid #475569;color:#f8fafc;border-radius:8px;font-size:16px;margin-bottom:12px;box-sizing:border-box;">
<button type="submit" style="width:100%;padding:12px;background:#3b82f6;color:white;border:none;border-radius:8px;font-size:16px;font-weight:600;cursor:pointer;">Sign In</button></form>
</div></body></html>'''


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def index():
    """Serve the dashboard."""
    try:
        return render_template("index.html")
    except Exception as e:
        logger.error(f"Template error: {e}")
        return f"<h1>DealFlow AI</h1><p>Dashboard loading error: {e}</p>", 500


@app.route("/api/deals")
def api_deals():
    """Get all deals with optional filters."""
    session = get_session()
    try:
        query = session.query(Deal)

        # Tab filter: active, archived, offers, pending
        tab = request.args.get("tab", "active")
        if tab == "archived":
            query = query.filter(Deal.is_archived == True)
        elif tab == "offers":
            query = query.filter((Deal.offer_amount != None) | (Deal.offer_status == "Submitted"))
        elif tab == "pending":
            query = query.filter(Deal.offer_status == "Pending")
        else:
            query = query.filter(
                (Deal.is_archived == False) | (Deal.is_archived == None),
                (Deal.offer_status != "Pending") | (Deal.offer_status == None),
                (Deal.offer_status != "Submitted") | (Deal.offer_status == None),
                Deal.offer_amount == None
            )
            if not request.args.get("show_hidden"):
                query = query.filter((Deal.is_hidden == False) | (Deal.is_hidden == None))

        # Filters
        zip_code = request.args.get("zip_code")
        if zip_code:
            query = query.filter(Deal.zip_code == zip_code)

        score_min = request.args.get("score_min", type=int)
        if score_min is not None:
            query = query.filter(Deal.score >= score_min)

        score_max = request.args.get("score_max", type=int)
        if score_max is not None:
            query = query.filter(Deal.score <= score_max)

        price_min = request.args.get("price_min", type=float)
        if price_min is not None:
            query = query.filter(Deal.price >= price_min)

        price_max = request.args.get("price_max", type=float)
        if price_max is not None:
            query = query.filter(Deal.price <= price_max)

        # Source filter
        source = request.args.get("source")
        if source:
            query = query.filter(Deal.source == source)

        # Sort
        sort_by = request.args.get("sort", "score")
        sort_dir = request.args.get("dir", "desc")

        sort_column = getattr(Deal, sort_by, Deal.score)
        if sort_dir == "asc":
            query = query.order_by(sort_column.asc().nullslast())
        else:
            query = query.order_by(sort_column.desc().nullslast())

        deals = query.all()
        return jsonify([deal_to_dict(d) for d in deals])

    finally:
        session.close()


@app.route("/api/deals/<int:deal_id>")
def api_deal_detail(deal_id):
    """Get a single deal with full details."""
    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            return jsonify({"error": "Deal not found"}), 404
        return jsonify(deal_to_dict(deal))
    finally:
        session.close()


@app.route("/api/stats")
def api_stats():
    """Get dashboard statistics."""
    session = get_session()
    try:
        total = session.query(Deal).count()
        high_score = session.query(Deal).filter(Deal.score >= 80).count()
        avg_score = session.query(Deal).with_entities(
            Deal.score
        ).all()
        avg = sum(s[0] for s in avg_score if s[0]) / max(len([s for s in avg_score if s[0]]), 1)

        return jsonify({
            "total_deals": total,
            "high_score_deals": high_score,
            "average_score": round(avg, 1),
        })
    finally:
        session.close()


@app.route("/api/run-pipeline", methods=["POST"])
def api_run_pipeline():
    """Manually trigger the pipeline."""
    try:
        from main import run_full_pipeline
        import threading
        thread = threading.Thread(target=run_full_pipeline)
        thread.start()
        return jsonify({"status": "Pipeline started"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/deals/<int:deal_id>/analyze-photos", methods=["POST"])
def api_analyze_photos(deal_id):
    """Run photo analysis for a single deal."""
    import threading
    from analyze_deal import analyze_by_id

    def run_analysis():
        try:
            analyze_by_id(deal_id)
        except Exception as e:
            logger.error(f"Photo analysis failed for deal {deal_id}: {e}")

    thread = threading.Thread(target=run_analysis)
    thread.start()
    return jsonify({"status": "Photo analysis started", "deal_id": deal_id})


@app.route("/api/deals/<int:deal_id>/save-comps", methods=["POST"])
def api_save_comps(deal_id):
    """Save user comps and ARV justification."""
    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            return jsonify({"error": "Deal not found"}), 404
        data = request.get_json() or {}
        comps = data.get("comps", [])
        # Filter out empty strings
        comps = [c.strip() for c in comps if c and c.strip()]
        deal.user_comps = json.dumps(comps) if comps else None
        deal.arv_justification = data.get("arv_justification") or None
        session.commit()
        return jsonify(deal_to_dict(deal))
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/deals/<int:deal_id>/mark-pending", methods=["POST"])
def api_mark_pending(deal_id):
    """Toggle pending status. Does NOT write to Google Sheet."""
    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            return jsonify({"error": "Deal not found"}), 404
        data = request.get_json() or {}
        if data.get("pending"):
            deal.offer_status = "Pending"
        else:
            deal.offer_status = None
        session.commit()
        return jsonify(deal_to_dict(deal))
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/deals/add", methods=["POST"])
def api_add_deal():
    """Manually add a new deal."""
    from arv_calculator import build_privy_url
    from repair_estimator import estimate_repairs
    from offer_calculator import calculate_offer
    from scorer import fallback_score

    data = request.get_json() or {}
    if not data.get("address"):
        return jsonify({"error": "Address is required"}), 400

    session = get_session()
    try:
        deal = Deal()
        deal.address = data["address"]
        deal.zip_code = data.get("zip_code", "")
        deal.city = data.get("city", "")
        deal.state = data.get("state", "CA")
        deal.price = float(data["price"]) if data.get("price") else None
        deal.bedrooms = int(data["bedrooms"]) if data.get("bedrooms") else None
        deal.bathrooms = float(data["bathrooms"]) if data.get("bathrooms") else None
        deal.sqft = float(data["sqft"]) if data.get("sqft") else None
        deal.year_built = int(data["year_built"]) if data.get("year_built") else None
        deal.arv = float(data["arv"]) if data.get("arv") else None
        deal.home_type = "SINGLE_FAMILY"
        deal.source = "manual"
        deal.offer_notes = data.get("notes", "")

        # Set repairs if provided
        repairs = float(data["repairs"]) if data.get("repairs") else None
        if repairs:
            deal.repairs_mid = repairs
            deal.repairs_worst = repairs

        # Build privy URL
        listing = {"address": deal.address, "city": deal.city, "state": deal.state, "zip_code": deal.zip_code}
        deal.privy_url = build_privy_url(listing)

        # Score
        listing_dict = {
            "price": deal.price, "arv": deal.arv, "sqft": deal.sqft,
            "days_on_zillow": None, "year_built": deal.year_built,
            "repairs_mid": deal.repairs_mid or 0, "repairs_worst": deal.repairs_worst or 0,
            "has_deal_keywords": False, "matched_keywords": [],
            "photo_grades": {}, "description": "",
        }
        result = fallback_score(listing_dict)
        deal.score = result["score"]
        deal.score_reasoning = result["reasoning"]

        # Estimate repairs if not provided
        if not repairs:
            listing_dict["photo_grades"] = {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
            estimate_repairs(listing_dict)
            deal.repairs_mid = listing_dict.get("repair_estimate", {}).get("total_mid")
            deal.repairs_worst = listing_dict.get("repair_estimate", {}).get("total_worst")

        # Calculate offer if ARV exists
        if deal.arv:
            listing_dict["arv"] = deal.arv
            listing_dict["repair_estimate"] = {"total_mid": deal.repairs_mid or 0, "total_worst": deal.repairs_worst or 0}
            calculate_offer(listing_dict)
            offer = listing_dict.get("offer_analysis", {})
            if "error" not in offer:
                deal.max_offer = offer.get("max_offer")
                deal.estimated_profit = offer.get("estimated_profit")
                deal.roi_pct = offer.get("roi_pct")
                deal.offer_analysis = json.dumps(offer)

        session.add(deal)
        session.commit()
        return jsonify(deal_to_dict(deal))
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to add deal: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/deals/<int:deal_id>/hide", methods=["POST"])
def api_hide_deal(deal_id):
    """Toggle hidden status. Does NOT archive."""
    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            return jsonify({"error": "Deal not found"}), 404
        data = request.get_json() or {}
        deal.is_hidden = data.get("hidden", not deal.is_hidden)
        session.commit()
        return jsonify(deal_to_dict(deal))
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/deals/<int:deal_id>/archive", methods=["POST"])
def api_archive_deal(deal_id):
    """Toggle archive status."""
    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            return jsonify({"error": "Deal not found"}), 404
        data = request.get_json() or {}
        deal.is_archived = data.get("archived", not deal.is_archived)
        session.commit()
        return jsonify(deal_to_dict(deal))
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/deals/<int:deal_id>/offer", methods=["POST"])
def api_submit_offer(deal_id):
    """Submit or update an offer."""
    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            return jsonify({"error": "Deal not found"}), 404
        data = request.get_json() or {}
        if "amount" in data and data["amount"]:
            deal.offer_amount = float(data["amount"])
        if "date" in data:
            deal.offer_date = data["date"]
        if "notes" in data:
            deal.offer_notes = data["notes"]
        status_val = data.get("status")
        if status_val == "":
            deal.offer_status = None
        elif status_val:
            deal.offer_status = status_val
        elif data.get("amount") and not deal.offer_status:
            deal.offer_status = "Submitted"
        session.commit()

        # Write to Google Sheet
        try:
            from sheets import write_offer_to_sheet, update_offer_status_in_sheet
            deal_data = deal_to_dict(deal)
            logger.info(f"Writing offer to Google Sheet: {deal.address} ${deal.offer_amount}")
            if "amount" in data:
                result = write_offer_to_sheet(
                    deal_data, deal.offer_amount, deal.offer_date,
                    deal.offer_status, deal.offer_notes
                )
                logger.info(f"Sheet write result: {result}")
            elif data.get("status"):
                update_offer_status_in_sheet(deal.address, deal.offer_status)
        except Exception as e:
            logger.warning(f"Sheet write failed (non-fatal): {e}", exc_info=True)

        return jsonify(deal_to_dict(deal))
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/deals/<int:deal_id>/update-repairs", methods=["POST"])
def api_update_repairs(deal_id):
    """Update repair estimate and recalculate offer."""
    from offer_calculator import calculate_offer

    data = request.get_json()
    new_repairs = data.get("repairs")
    if new_repairs is None:
        return jsonify({"error": "Repairs value required"}), 400

    try:
        new_repairs = float(new_repairs)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid repairs value"}), 400

    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            return jsonify({"error": "Deal not found"}), 404

        deal.repairs_mid = new_repairs
        deal.repairs_worst = new_repairs

        listing = deal_to_dict(deal)
        listing["arv"] = deal.arv
        listing["repair_estimate"] = {"total_mid": new_repairs, "total_worst": new_repairs}

        calculate_offer(listing)
        offer = listing.get("offer_analysis", {})
        if "error" not in offer:
            deal.max_offer = offer.get("max_offer")
            deal.max_offer_worst = offer.get("max_offer_worst")
            deal.estimated_profit = offer.get("estimated_profit")
            deal.roi_pct = offer.get("roi_pct")
            deal.offer_analysis = json.dumps(offer)

        session.commit()
        return jsonify(deal_to_dict(deal))
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/deals/<int:deal_id>/update-arv", methods=["POST"])
def api_update_arv(deal_id):
    """Update ARV from Privy and recalculate offer."""
    from offer_calculator import calculate_offer
    from repair_estimator import estimate_repairs

    data = request.get_json()
    new_arv = data.get("arv")
    if new_arv is None:
        return jsonify({"error": "ARV value required"}), 400

    try:
        new_arv = float(new_arv)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid ARV value"}), 400

    session = get_session()
    try:
        deal = session.query(Deal).filter_by(id=deal_id).first()
        if not deal:
            return jsonify({"error": "Deal not found"}), 404

        # Update ARV
        deal.arv = new_arv

        # Rebuild listing dict for recalculation
        listing = deal_to_dict(deal)
        listing["arv"] = new_arv

        # Ensure repairs exist
        if not listing.get("repair_estimate") or not listing["repair_estimate"].get("total_mid"):
            listing["photo_grades"] = listing.get("photo_grades") or {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
            estimate_repairs(listing)
            repair_est = listing.get("repair_estimate", {})
            deal.repairs_mid = repair_est.get("total_mid")
            deal.repairs_worst = repair_est.get("total_worst")
            deal.repair_breakdown = json.dumps(repair_est.get("breakdown")) if repair_est.get("breakdown") else None
        else:
            listing["repair_estimate"] = {
                "total_mid": deal.repairs_mid or 0,
                "total_worst": deal.repairs_worst or 0,
            }

        # Recalculate offer
        calculate_offer(listing)
        offer = listing.get("offer_analysis", {})

        if "error" not in offer:
            deal.max_offer = offer.get("max_offer")
            deal.max_offer_worst = offer.get("max_offer_worst")
            deal.estimated_profit = offer.get("estimated_profit")
            deal.roi_pct = offer.get("roi_pct")
            deal.offer_analysis = json.dumps(offer)

        session.commit()
        return jsonify(deal_to_dict(deal))

    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/tracker")
def api_tracker():
    """Fetch Google Sheet tracker data via Apps Script."""
    import requests as req
    TRACKER_URL = os.getenv(
        "TRACKER_WEBHOOK_URL",
        "https://script.google.com/macros/s/AKfycbwcmUwh3Z6GkSgFnzpHjE_2lzqAn6e_jfsNFceiQ_3Um5sk6qg90wUcE22RUUQY_Qjl/exec"
    )
    if not TRACKER_URL:
        return jsonify({"error": "TRACKER_WEBHOOK_URL not set"}), 500
    try:
        resp = req.get(TRACKER_URL, timeout=15, params={"t": request.args.get("t", "")})
        response = jsonify(resp.json())
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response
    except Exception as e:
        logger.error(f"Tracker fetch failed: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "0") == "1")
