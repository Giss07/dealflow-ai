"""
Distressed Deals page: run search_distressed over a few ZIPs and show cards.

A search takes 1-5 minutes per ZIP (listing descriptions plus comp lookups),
longer than gunicorn's 30s request timeout, so POST starts a background thread
and the page polls the DistressedSearch row. Results are saved after each ZIP,
so cards appear as ZIPs finish, from whichever gunicorn worker answers.

Read-only: nothing here submits offers or writes to the DealFlow sheet.
"""

import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta

from flask import Blueprint, Response, jsonify, render_template, request

import distressed_report as report
from database import DistressedSearch, get_session

logger = logging.getLogger(__name__)
bp = Blueprint("distressed", __name__)

MAX_ZIPS = 5
STALE_AFTER = timedelta(minutes=45)      # a run whose thread died (deploy, restart)
SEARCH_ATTEMPTS = 3                      # the vendor intermittently 500s on a ZIP


def _parse_zips(text):
    zips = []
    for z in re.findall(r"\b\d{5}\b", text or ""):
        if z not in zips:
            zips.append(z)
    return zips


def _run_search(search_id, zips):
    results = {}
    db = get_session()
    try:
        # Imported here, inside the try: any failure (missing package, bad
        # config) must mark the search failed instead of killing the thread
        # and leaving it "running" until it goes stale.
        import mcp_server
        for n, z in enumerate(zips, 1):
            data = None
            for attempt in range(1, SEARCH_ATTEMPTS + 1):
                try:
                    data = json.loads(mcp_server.search_distressed(z))
                except Exception as e:
                    data = {"status": "error", "error": f"{type(e).__name__}: {str(e)[:200]}"}
                if data.get("status") == "ok":
                    break
                logger.warning(f"[distressed] search {search_id} zip {z} attempt {attempt}: {data.get('error')}")
                time.sleep(5 * attempt)
            results[z] = data
            row = db.get(DistressedSearch, search_id)
            row.result, row.zips_done = json.dumps(results), n
            db.commit()
        row = db.get(DistressedSearch, search_id)
        row.status, row.completed_at = "completed", datetime.utcnow()
        db.commit()
    except Exception as e:
        logger.error(f"[distressed] search {search_id} failed: {e}", exc_info=True)
        db.rollback()
        row = db.get(DistressedSearch, search_id)
        if row:
            row.status, row.error_message = "failed", f"{type(e).__name__}: {str(e)[:300]}"
            row.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()


def _expire_stale(db):
    cutoff = datetime.utcnow() - STALE_AFTER
    for row in db.query(DistressedSearch).filter(DistressedSearch.status == "running",
                                                 DistressedSearch.created_at < cutoff):
        row.status, row.error_message = "failed", "stopped before finishing (server restarted?)"
    db.commit()


def _cards(row):
    results = json.loads(row.result or "{}")
    cards, errors = [], {}
    for z, data in results.items():
        if data.get("status") != "ok":
            errors[z] = data.get("error") or "search failed"
            continue
        cards.extend(report.card(z, lst) for lst in data.get("listings", []))
    return report.sort_cards(cards), errors


def _find_listing(row, zip_code, address):
    data = json.loads(row.result or "{}").get(zip_code) or {}
    for lst in data.get("listings", []):
        if lst.get("address") == address:
            return lst
    return None


@bp.route("/distressed")
def page():
    return render_template("distressed.html")


@bp.route("/api/distressed/search", methods=["POST"])
def start_search():
    zips = _parse_zips((request.get_json(silent=True) or {}).get("zips", ""))
    if not zips:
        return jsonify({"error": "Enter at least one 5-digit ZIP"}), 400
    if len(zips) > MAX_ZIPS:
        return jsonify({"error": f"Up to {MAX_ZIPS} ZIPs per search (about $0.40 and a few minutes each)"}), 400
    db = get_session()
    try:
        _expire_stale(db)
        running = db.query(DistressedSearch).filter(DistressedSearch.status == "running").first()
        if running:
            return jsonify({"error": "A search is already running", "id": running.id}), 409
        row = DistressedSearch(zips=",".join(zips), status="running", zips_done=0, result="{}")
        db.add(row)
        db.commit()
        search_id = row.id
    finally:
        db.close()
    threading.Thread(target=_run_search, args=(search_id, zips), daemon=True,
                     name=f"distressed-{search_id}").start()
    logger.info(f"[distressed] search {search_id} started: {zips}")
    return jsonify({"id": search_id, "zips": zips})


@bp.route("/api/distressed/search/latest")
def latest_search():
    db = get_session()
    try:
        _expire_stale(db)
        row = db.query(DistressedSearch).order_by(DistressedSearch.id.desc()).first()
        return jsonify({"id": row.id if row else None})
    finally:
        db.close()


@bp.route("/api/distressed/search/<int:search_id>")
def search_status(search_id):
    db = get_session()
    try:
        _expire_stale(db)
        row = db.get(DistressedSearch, search_id)
        if not row:
            return jsonify({"error": "Not found"}), 404
        cards, errors = _cards(row)
        return jsonify({
            "id": row.id, "status": row.status, "zips": row.zips.split(","),
            "zips_done": row.zips_done or 0, "error": row.error_message,
            "zip_errors": errors, "cards": cards,
            "started": row.created_at.isoformat() + "Z" if row.created_at else None,
        })
    finally:
        db.close()


@bp.route("/api/distressed/search/<int:search_id>/csv")
def search_csv(search_id):
    """?zip=&address= → that property for the agent; no params → every listing."""
    db = get_session()
    try:
        row = db.get(DistressedSearch, search_id)
        if not row:
            return jsonify({"error": "Not found"}), 404
        zip_code, address = request.args.get("zip"), request.args.get("address")
        if zip_code and address:
            lst = _find_listing(row, zip_code, address)
            if not lst:
                return jsonify({"error": "Property not in this search"}), 404
            body = report.property_csv(zip_code, lst)
            name = re.sub(r"[^A-Za-z0-9]+", "_", address.split(",")[0]).strip("_") + ".csv"
        else:
            rows = []
            for z, data in json.loads(row.result or "{}").items():
                for lst in (data.get("listings") or []) if data.get("status") == "ok" else []:
                    rows.append(report.rows_for(z, lst)[0])
            body = report.write_csv(report.LISTING_COLUMNS, rows)
            name = f"distressed_{row.zips.replace(',', '-')}.csv"
        return Response(body, mimetype="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="{name}"'})
    finally:
        db.close()
