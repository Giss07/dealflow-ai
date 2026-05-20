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
from database import init_db, get_session, get_all_deals, deal_to_dict, Deal, PreForeclosure, preforeclosure_to_dict

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

        # If this deal came from pre-foreclosure, archive the PF record
        if deal.source == "pre-foreclosure" and deal.offer_amount:
            pf_linked = session.query(PreForeclosure).filter_by(linked_deal_id=deal.id).first()
            if pf_linked:
                pf_linked.is_archived = True
                pf_linked.mls_status = "offer-submitted"

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


@app.route("/api/preforeclosure")
def api_preforeclosure_list():
    """List all pre-foreclosure properties."""
    db = get_session()
    try:
        query = db.query(PreForeclosure)
        # Archive filter
        if not request.args.get("show_archived"):
            query = query.filter((PreForeclosure.is_archived == False) | (PreForeclosure.is_archived == None))
        status = request.args.get("status")
        if status and status != "all":
            query = query.filter(PreForeclosure.mls_status == status)
        search = request.args.get("search")
        if search:
            q = f"%{search}%"
            query = query.filter(
                PreForeclosure.address.ilike(q) | PreForeclosure.city.ilike(q) | PreForeclosure.zip_code.ilike(q)
            )
        query = query.order_by(PreForeclosure.date_added.desc())
        return jsonify([preforeclosure_to_dict(p) for p in query.all()])
    finally:
        db.close()


@app.route("/api/preforeclosure", methods=["POST"])
def api_preforeclosure_add():
    """Add a pre-foreclosure property manually."""
    data = request.get_json() or {}
    if not data.get("address"):
        return jsonify({"error": "Address required"}), 400
    db = get_session()
    try:
        pf = PreForeclosure(
            address=data["address"],
            city=data.get("city", ""),
            state=data.get("state", "CA"),
            zip_code=data.get("zip_code", ""),
            property_type=data.get("property_type", "SFR"),
            source_list=data.get("source_list", "pre-foreclosure"),
            estimated_value=float(data["estimated_value"]) if data.get("estimated_value") else None,
            auction_date=data.get("auction_date", ""),
            notes=data.get("notes", ""),
            mls_status="unknown",
        )
        db.add(pf)
        db.commit()
        return jsonify(preforeclosure_to_dict(pf))
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>", methods=["PUT"])
def api_preforeclosure_update(pf_id):
    """Update a pre-foreclosure property."""
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404
        data = request.get_json() or {}
        for field in ["address", "city", "state", "zip_code", "property_type", "source_list", "auction_date", "notes", "mls_status", "zillow_url"]:
            if field in data:
                setattr(pf, field, data[field])
        if "estimated_value" in data:
            pf.estimated_value = float(data["estimated_value"]) if data["estimated_value"] else None
        db.commit()
        return jsonify(preforeclosure_to_dict(pf))
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>", methods=["DELETE"])
def api_preforeclosure_delete(pf_id):
    """Delete a pre-foreclosure property."""
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404
        db.delete(pf)
        db.commit()
        return jsonify({"status": "deleted"})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/import-csv", methods=["POST"])
def api_preforeclosure_import():
    """Import pre-foreclosure properties from CSV."""
    import csv, io
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    text = file.read().decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    # Flexible column mapping — prioritizes "property" prefixed columns, skips empty matches
    def find_col(row, candidates):
        # First pass: try candidates with "property" prefix
        for c in candidates:
            for key in row:
                if c in key.lower() and "property" in key.lower():
                    val = row[key].strip()
                    if val:
                        return val
        # Second pass: try any match, skip empty
        for c in candidates:
            for key in row:
                if c in key.lower():
                    val = row[key].strip()
                    if val:
                        return val
        return ""

    db = get_session()
    added = 0
    skipped = 0
    skipped_details = []
    try:
        for row_num, row in enumerate(reader, start=2):
            address = find_col(row, ["address", "street", "situs", "site address", "property address"])
            if not address:
                skipped += 1
                skipped_details.append({"row": row_num, "reason": "No address found", "data": str(dict(row))[:100]})
                continue
            city = find_col(row, ["city", "situs city"])
            zip_code = find_col(row, ["zip", "postal", "situs zip"]).replace(".0", "")
            # Dedup
            existing = db.query(PreForeclosure).filter(
                PreForeclosure.address.ilike(f"%{address}%"),
                PreForeclosure.zip_code == zip_code
            ).first()
            if existing:
                skipped += 1
                skipped_details.append({"row": row_num, "reason": "Duplicate", "address": address, "zip": zip_code})
                continue
            val_raw = find_col(row, ["value", "avm", "estimated", "market value", "assessed"])
            val_clean = val_raw.replace("$", "").replace(",", "").strip()
            auction = find_col(row, ["auction date", "sale date", "trustee sale", "foreclosure date"])
            source = "auction" if auction else "pre-foreclosure"
            pf = PreForeclosure(
                address=address,
                city=city,
                state=find_col(row, ["state", "situs state"]) or "CA",
                zip_code=zip_code,
                property_type=find_col(row, ["property type", "type", "land use"]) or "SFR",
                source_list=source,
                estimated_value=float(val_clean) if val_clean and val_clean.replace(".", "").isdigit() else None,
                auction_date=auction,
                mls_status="unknown",
            )
            db.add(pf)
            added += 1
        db.commit()
        return jsonify({"added": added, "skipped": skipped, "skipped_details": skipped_details})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>/archive", methods=["POST"])
def api_preforeclosure_archive(pf_id):
    """Archive with reason, or restore (unarchive)."""
    from datetime import datetime as dt
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404
        data = request.get_json() or {}

        if data.get("archived") == False:
            # Restore (unarchive)
            pf.is_archived = False
            pf.archive_reason = None
            pf.archive_notes = None
            pf.archived_at = None
        else:
            # Archive with reason
            reason = data.get("reason", "")
            if reason and reason not in ("already_sold", "no_equity", "not_real_lead"):
                return jsonify({"error": "Invalid reason"}), 400
            pf.is_archived = True
            pf.archive_reason = reason or None
            pf.archive_notes = data.get("notes", "").strip() or None
            pf.archived_at = dt.utcnow()

        db.commit()
        return jsonify(preforeclosure_to_dict(pf))
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>/submit-offer", methods=["POST"])
def api_preforeclosure_submit_offer(pf_id):
    """Submit or update an offer on a pre-foreclosure property.

    Creates a Deal record (or reuses existing linked one), sets offer fields,
    marks the pre-foreclosure as offer-submitted. Clears offer if amount is empty.
    """
    from datetime import datetime as dt
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404

        data = request.get_json() or {}
        offer_amount = data.get("offer_amount", "")
        offer_arv = data.get("offer_arv", "")
        offer_notes = data.get("notes", "").strip()

        # Clear offer if amount is empty
        if not offer_amount:
            if pf.linked_deal_id:
                deal = db.query(Deal).filter_by(id=pf.linked_deal_id).first()
                if deal:
                    deal.offer_amount = None
                    deal.offer_date = None
                    deal.offer_notes = None
                    deal.offer_status = None
            pf.mls_status = pf.previous_mls_status or "unknown"
            pf.notification_priority = "auto"
            db.commit()
            return jsonify(preforeclosure_to_dict(pf))

        # Parse amounts
        try:
            amt = float(str(offer_amount).replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid offer amount"}), 400
        arv = None
        if offer_arv:
            try:
                arv = float(str(offer_arv).replace("$", "").replace(",", ""))
            except (ValueError, TypeError):
                pass

        # Get or create Deal
        deal = None
        if pf.linked_deal_id:
            deal = db.query(Deal).filter_by(id=pf.linked_deal_id).first()

        if not deal:
            deal = Deal()
            deal.address = pf.address
            deal.city = pf.city
            deal.state = pf.state
            deal.zip_code = pf.zip_code
            deal.price = pf.mls_price or pf.estimated_value
            deal.home_type = pf.property_type or "SINGLE_FAMILY"
            deal.source = "pre-foreclosure"
            deal.description = pf.ai_notes or ""
            db.add(deal)
            db.flush()
            pf.linked_deal_id = deal.id

        # Set offer fields on Deal
        deal.offer_amount = amt
        deal.arv = arv or deal.arv or pf.estimated_value
        deal.offer_date = dt.utcnow().strftime("%Y-%m-%d")
        deal.offer_notes = offer_notes or deal.offer_notes
        deal.offer_status = "Submitted"

        # Mark pre-foreclosure as offer-submitted; auto-watch so auction reminders fire
        pf.mls_status = "offer-submitted"
        pf.notification_priority = "watch"

        db.commit()
        logger.info(f"Offer submitted for {pf.address}: ${amt:,.0f} (Deal ID {deal.id})")
        return jsonify({
            "preforeclosure": preforeclosure_to_dict(pf),
            "deal": deal_to_dict(deal),
        })
    except Exception as e:
        db.rollback()
        logger.error(f"Submit offer failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>/create-deal", methods=["POST"])
def api_preforeclosure_create_deal(pf_id):
    """Create a Deal from a pre-foreclosure property so it appears in Active Deals."""
    from arv_calculator import build_privy_url
    from repair_estimator import estimate_repairs
    from offer_calculator import calculate_offer
    from scorer import fallback_score

    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404

        # Check if already linked
        if pf.linked_deal_id:
            existing = db.query(Deal).filter_by(id=pf.linked_deal_id).first()
            if existing:
                return jsonify(deal_to_dict(existing))

        # Create new Deal from pre-foreclosure data
        deal = Deal()
        deal.address = pf.address
        deal.city = pf.city
        deal.state = pf.state
        deal.zip_code = pf.zip_code
        deal.price = pf.mls_price or pf.estimated_value
        deal.home_type = pf.property_type or "SINGLE_FAMILY"
        deal.source = "pre-foreclosure"
        deal.arv = pf.estimated_value
        deal.listing_url = ""
        deal.description = pf.ai_notes or ""

        # Build privy URL
        listing = {"address": deal.address, "city": deal.city, "state": deal.state, "zip_code": deal.zip_code}
        deal.privy_url = build_privy_url(listing)

        # Score
        listing_dict = {
            "price": deal.price, "arv": deal.arv, "sqft": None,
            "days_on_zillow": None, "year_built": None,
            "repairs_mid": 0, "repairs_worst": 0,
            "has_deal_keywords": True, "matched_keywords": ["pre-foreclosure"],
            "photo_grades": {}, "description": "",
        }
        result = fallback_score(listing_dict)
        deal.score = result["score"]
        deal.score_reasoning = result["reasoning"]

        # Estimate repairs
        listing_dict["photo_grades"] = {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
        estimate_repairs(listing_dict)
        deal.repairs_mid = listing_dict.get("repair_estimate", {}).get("total_mid")
        deal.repairs_worst = listing_dict.get("repair_estimate", {}).get("total_worst")

        # Calculate offer
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

        db.add(deal)
        db.flush()  # Get the deal.id

        # Link pre-foreclosure to deal
        pf.linked_deal_id = deal.id
        db.commit()

        return jsonify(deal_to_dict(deal))
    except Exception as e:
        db.rollback()
        logger.error(f"Create deal from PF failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/scan/<int:pf_id>", methods=["POST"])
def api_preforeclosure_scan(pf_id):
    """Scan a single property via OpenWeb Ninja."""
    from datetime import datetime as dt

    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404

        openweb_key = os.getenv("OPENWEB_NINJA_API_KEY", "")
        if not openweb_key:
            return jsonify({"error": "OPENWEB_NINJA_API_KEY not configured"}), 500

        from worker import _scan_via_openweb_ninja
        new_on_market = []
        _scan_via_openweb_ninja(pf, openweb_key, 0, new_on_market)
        db.commit()
        logger.info(f"Scan for {pf.address}: {pf.mls_status}")
        return jsonify(preforeclosure_to_dict(pf))

    except Exception as e:
        db.rollback()
        logger.error(f"Scan failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>/set-url", methods=["POST"])
def api_preforeclosure_set_url(pf_id):
    """Set or clear a Zillow URL override for a pre-foreclosure property."""
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404
        data = request.get_json() or {}
        pf.zillow_url = data.get("zillow_url", "").strip() or None
        db.commit()
        return jsonify(preforeclosure_to_dict(pf))
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>/set-auction-date", methods=["POST"])
def api_preforeclosure_set_auction_date(pf_id):
    """Set auction date on a NOD property, moving it to Auction stage."""
    from datetime import datetime as dt
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404
        data = request.get_json() or {}
        auction_date_str = data.get("auction_date", "")
        if not auction_date_str:
            return jsonify({"error": "auction_date required"}), 400

        # Parse date — accept multiple formats
        from worker import _parse_date_safe
        parsed = _parse_date_safe(auction_date_str)
        if not parsed:
            return jsonify({"error": f"Could not parse date: {auction_date_str}"}), 400

        pf.foreclosure_auction_time = parsed
        pf.foreclosure_stage = "Auction"
        pf.foreclosure_stage_manual_override = True
        if data.get("notes"):
            pf.notes = data["notes"]
        db.commit()
        logger.info(f"Set auction date for {pf.address}: {parsed} (manual override)")
        return jsonify(preforeclosure_to_dict(pf))
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>/move-to-nod", methods=["POST"])
def api_preforeclosure_move_to_nod(pf_id):
    """Move an Auction property back to NOD stage (clear auction date)."""
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404
        pf.foreclosure_auction_time = None
        pf.foreclosure_stage = "NOD"
        pf.foreclosure_stage_manual_override = True
        db.commit()
        logger.info(f"Moved {pf.address} back to NOD (manual override)")
        return jsonify(preforeclosure_to_dict(pf))
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>/notification-priority", methods=["PATCH", "POST"])
def api_preforeclosure_notification_priority(pf_id):
    """Set notification priority for a property (auto/watch/mute)."""
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404
        data = request.get_json() or {}
        priority = data.get("priority", "").lower()
        if priority not in ("auto", "watch", "mute"):
            return jsonify({"error": "priority must be auto, watch, or mute"}), 400
        pf.notification_priority = priority
        db.commit()
        result = preforeclosure_to_dict(pf)
        if priority == "watch" and (pf.foreclosure_stage != "Auction" or not pf.foreclosure_auction_time):
            reason = "no auction date set" if not pf.foreclosure_auction_time else f"stage is {pf.foreclosure_stage}"
            result["warning"] = (
                f"Marked as watch, but this property won't trigger notifications until it reaches "
                f"Auction stage with an auction date ({reason}). Use 'Set Auction Date' to enable alerts."
            )
        return jsonify(result)
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/<int:pf_id>/dismiss", methods=["POST"])
def api_preforeclosure_dismiss(pf_id):
    """Dismiss the 'new listing' flag for a property (mark as seen)."""
    db = get_session()
    try:
        pf = db.query(PreForeclosure).filter_by(id=pf_id).first()
        if not pf:
            return jsonify({"error": "Not found"}), 404
        pf.is_new = False
        db.commit()
        return jsonify(preforeclosure_to_dict(pf))
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/dismiss-all", methods=["POST"])
def api_preforeclosure_dismiss_all():
    """Dismiss all 'new listing' flags."""
    db = get_session()
    try:
        count = db.query(PreForeclosure).filter(PreForeclosure.is_new == True).update({"is_new": False})
        db.commit()
        return jsonify({"dismissed": count})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/preforeclosure/new-count")
def api_preforeclosure_new_count():
    """Get count of properties with is_new=True (for badge display)."""
    db = get_session()
    try:
        count = db.query(PreForeclosure).filter(
            PreForeclosure.is_new == True,
            (PreForeclosure.is_archived == False) | (PreForeclosure.is_archived == None),
        ).count()
        return jsonify({"count": count})
    finally:
        db.close()


@app.route("/api/preforeclosure/scan-cost-estimate", methods=["POST"])
def api_preforeclosure_scan_cost_estimate():
    """Estimate cost for scanning a set of properties."""
    data = request.get_json() or {}
    property_ids = data.get("property_ids", [])
    if not property_ids:
        return jsonify({"error": "property_ids array required"}), 400
    try:
        from worker import estimate_scan_cost
        estimate = estimate_scan_cost(property_ids)
        estimate["show_cost_preview"] = os.getenv("SHOW_COST_PREVIEW", "true").lower() != "false"
        return jsonify(estimate)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/scan-jobs", methods=["POST"])
def api_scan_job_create():
    """Create an async scan job. Returns job ID immediately.

    Request body: {"property_ids": [1, 2, 3]}
    The worker service picks up the job and runs it in the background.
    """
    from database import ScanJob, scan_job_to_dict
    from datetime import timedelta
    data = request.get_json() or {}
    property_ids = data.get("property_ids", [])
    if not property_ids:
        return jsonify({"error": "property_ids array required"}), 400

    db = get_session()
    try:
        # Block duplicate: reject if a pending/running job already exists
        active = db.query(ScanJob).filter(ScanJob.status.in_(["pending", "running"])).first()
        if active:
            return jsonify({"error": "A scan is already in progress", "job": scan_job_to_dict(active)}), 409

        from datetime import datetime as dt
        job = ScanJob(
            status="pending",
            property_ids=json.dumps(property_ids),
            total=len(property_ids),
            created_at=dt.utcnow(),
            expires_at=dt.utcnow() + timedelta(hours=2),
        )
        db.add(job)
        db.commit()
        logger.info(f"[SCAN_JOB_CREATED] id={job.id} created_at={job.created_at.isoformat()} property_count={len(property_ids)}")
        return jsonify(scan_job_to_dict(job))
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()


@app.route("/api/scan-jobs/<int:job_id>")
def api_scan_job_status(job_id):
    """Get status of a scan job (for polling). Fresh session per request."""
    from database import ScanJob, scan_job_to_dict
    db = get_session()
    db.close()  # Close immediately — discard any pooled connection state
    db = get_session()  # Brand new session + connection
    try:
        job = db.query(ScanJob).filter_by(id=job_id).first()
        if not job:
            return jsonify({"error": "Job not found"}), 404
        return jsonify(scan_job_to_dict(job))
    finally:
        db.close()


@app.route("/api/scan-jobs/latest")
def api_scan_job_latest():
    """Get the most recent scan job (for detecting in-progress scans on page load)."""
    from database import ScanJob, scan_job_to_dict
    db = get_session()
    try:
        job = db.query(ScanJob).order_by(ScanJob.created_at.desc()).first()
        if not job:
            return jsonify({"job": None})
        return jsonify({"job": scan_job_to_dict(job)})
    finally:
        db.close()


@app.route("/admin/run-cron/<job_name>", methods=["POST"])
def admin_run_cron(job_name):
    """Manually trigger a cron job. Password-protected."""
    import threading
    jobs = {
        "check-upcoming-auctions": ("check_upcoming_auctions", "worker"),
        "rescan-nod-properties": ("rescan_nod_properties", "worker"),
    }
    if job_name not in jobs:
        return jsonify({"error": f"Unknown job: {job_name}", "available": list(jobs.keys())}), 404
    func_name, module = jobs[job_name]
    def _run():
        try:
            from worker import check_upcoming_auctions, rescan_nod_properties
            {"check_upcoming_auctions": check_upcoming_auctions, "rescan_nod_properties": rescan_nod_properties}[func_name]()
        except Exception as e:
            logger.error(f"Manual cron trigger failed: {e}", exc_info=True)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "job": job_name})


@app.route("/admin/test-notifications", methods=["POST"])
def admin_test_notifications():
    """Send test emails using both templates. Password-protected."""
    from datetime import timedelta
    from notifications import send_auction_scheduled, send_auction_digest

    class FakePF:
        def __init__(self, **kw):
            for k, v in kw.items(): setattr(self, k, v)

    now = datetime.utcnow()
    test_pf = FakePF(
        id=0, address="TEST: 123 Main St", city="Los Angeles", state="CA", zip_code="90210",
        foreclosure_auction_time=now + timedelta(days=5), foreclosing_bank="Test Bank N.A.",
        foreclosure_auction_city="Los Angeles", foreclosure_auction_location="Test Location",
        foreclosure_unpaid_balance=250000.0,
    )
    r1 = send_auction_scheduled(test_pf)
    r2 = send_auction_digest([test_pf])
    return jsonify({"smtp_status": "ok" if (r1 and r2) else "partial", "auction_scheduled": r1, "auction_digest": r2})


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
