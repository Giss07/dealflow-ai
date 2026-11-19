"""
Distressed Deals page: run search_distressed over a few ZIPs and show cards.

A search takes 1-5 minutes per ZIP (listing descriptions plus comp lookups),
longer than gunicorn's 30s request timeout, so POST starts a background thread
and the page polls the DistressedSearch row. Results are saved after each ZIP,
so cards appear as ZIPs finish, from whichever gunicorn worker answers.

Nothing here submits an offer. "Add to Tracker" appends one row to the offer
tracker sheet (tracker_sheet.py) for the agent to submit — values in A–I and
the three comps in O–Q, no Notes — the page's only write outside its own
tables.

The owner's decisions (Pass, sent to tracker, Undo) are kept by address in
property_decisions, so later searches sort the same property the same way: a
passed property stays in "Passed" until its list price drops below the price
it was passed at.
"""

import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta

from flask import Blueprint, Response, jsonify, render_template, request

import distressed_report as report
import tracker_sheet
from database import DistressedSearch, PropertyDecision, get_session

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


def _mark_tracker(cards):
    """card["tracker"] = 'tracker' / 'archive' / None from the sheet (cached ~1 min).
    Returns an error message when the sheet can't be read (cards stay None)."""
    try:
        for c in cards:
            hit = tracker_sheet.find_property(c["address"])
            c["tracker"] = hit["where"] if hit else None
            c["tracker_row"] = hit["row"] if hit else None
        return None
    except Exception as e:
        logger.warning(f"[distressed] tracker lookup failed: {e}")
        for c in cards:
            c.setdefault("tracker", None)
        return f"Couldn't check the tracker sheet: {type(e).__name__}"


PASS_REASONS = ("Price too high", "Bad area", "Too much rehab", "Bad comps", "Other")


def decision_key(address, zip_code=""):
    """'street|zip' — the same street normalization the tracker match uses."""
    street, z, _ = tracker_sheet.address_key(address)
    return f"{street}|{z or str(zip_code or '')}"


def apply_decisions(cards, decisions):
    """Sort each card into the main list, "sent" or "passed".

    decisions: {decision_key: PropertyDecision}. Sets card["decision"]
    ('sent' / 'passed' / None for the main list), card["pass_reason"] and,
    for a passed property whose list price has dropped since, card["price_drop"]
    (the dollar drop) with the card back in the main list. A property already
    in the tracker sheet counts as sent unless the owner pressed Undo on it."""
    for c in cards:
        d = decisions.get(decision_key(c["address"], c.get("zip")))
        c["decision"], c["pass_reason"], c["price_drop"] = None, None, None
        if d is None:
            if c.get("tracker"):
                c["decision"] = "sent"
        elif d.decision == "sent":
            c["decision"] = "sent"
        elif d.decision == "passed":
            c["pass_reason"] = d.reason
            price = c.get("list_price")
            if price and d.list_price and price < d.list_price:
                c["price_drop"] = round(d.list_price - price)
            else:
                c["decision"] = "passed"
    return cards


def _decisions(db):
    return {d.address_key: d for d in db.query(PropertyDecision).all()}


def record_decision(db, address, zip_code, decision, reason=None, list_price=None, tracker_row=None):
    key = decision_key(address, zip_code)
    d = db.query(PropertyDecision).filter(PropertyDecision.address_key == key).first()
    if d is None:
        d = PropertyDecision(address_key=key)
        db.add(d)
    d.address, d.decision, d.reason = address, decision, reason
    d.list_price, d.tracker_row, d.decided_at = list_price, tracker_row, datetime.utcnow()
    db.commit()
    logger.info(f"[distressed] {decision}: {address}" + (f" ({reason})" if reason else ""))
    return d


def _find_listing(row, zip_code, address):
    data = json.loads(row.result or "{}").get(zip_code) or {}
    for lst in data.get("listings", []):
        if lst.get("address") == address:
            return lst
    return None


@bp.route("/distressed")
def page():
    from flip_calculator import DEFAULTS
    return render_template("distressed.html", wholesale_fee=int(DEFAULTS["wholesale_fee"]))


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
        tracker_error = _mark_tracker(cards) if row.status != "running" else None
        apply_decisions(cards, _decisions(db))
        return jsonify({
            "id": row.id, "status": row.status, "zips": row.zips.split(","),
            "zips_done": row.zips_done or 0, "error": row.error_message,
            "zip_errors": errors, "cards": cards, "tracker_error": tracker_error,
            "started": row.created_at.isoformat() + "Z" if row.created_at else None,
        })
    finally:
        db.close()


def _money_or_none(v, name, upper=50_000_000):
    if v in (None, ""):
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number")
    if not (0 <= n <= upper):
        raise ValueError(f"{name} must be between $0 and ${upper:,}")
    return round(n)


def tracker_comps(card, chosen=None):
    """[(address, zillow_url)] for O, P, Q.

    chosen: the owner's checked comps from "See comps", anchor first — any of
    the card's comps or its not-used-in-ARV comps, at most 3. Without it, the
    default pick (median comp as anchor, then the closest). Addresses are
    matched against the saved search, and links come from there too."""
    pool = {c["address"]: c for c in card["comps"] + card.get("excluded_comps", []) if c.get("address")}
    if chosen is None:
        chosen = report.default_tracker_comps(card)
    picked = [pool[a] for a in chosen]
    anchor, rest = picked[:1], sorted(picked[1:], key=lambda c: c.get("distance_mi") or 0)
    return [(c["address"], c.get("url") or "") for c in anchor + rest]


def _chosen_comps(card, body_comps):
    """Validate the page's comp choice → list of addresses, anchor first, or None."""
    if body_comps in (None, []):
        return None
    if not isinstance(body_comps, list) or not all(isinstance(a, str) for a in body_comps):
        raise ValueError("comps must be a list of addresses")
    if len(body_comps) > 3:
        raise ValueError("Max 3 comps")
    if len(set(body_comps)) != len(body_comps):
        raise ValueError("A comp is listed twice")
    known = {c["address"] for c in card["comps"] + card.get("excluded_comps", [])}
    unknown = [a for a in body_comps if a not in known]
    if unknown:
        raise ValueError(f"Not a comp of this property: {unknown[0]}")
    return body_comps


@bp.route("/api/distressed/tracker", methods=["POST"])
def add_to_tracker():
    """Append a property from a saved search to the offer tracker sheet.

    The row is rebuilt from the saved search; only the offer and any ARV /
    rehab the owner edited come from the page."""
    from flip_calculator import max_allowable_offer
    body = request.get_json(silent=True) or {}
    try:
        offer = _money_or_none(body.get("offer"), "Offer")
        arv_in = _money_or_none(body.get("arv"), "ARV")
        rehab_in = _money_or_none(body.get("rehab"), "Rehab", upper=5_000_000)
        fee_in = _money_or_none(body.get("fee"), "Wholesale fee", upper=1_000_000)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if not offer:
        return jsonify({"error": "Enter an offer price"}), 400
    db = get_session()
    try:
        row = db.get(DistressedSearch, int(body.get("search_id") or 0))
        lst = _find_listing(row, str(body.get("zip") or ""), body.get("address") or "") if row else None
    finally:
        db.close()
    if not lst:
        return jsonify({"error": "Property not in this search"}), 404
    card = report.card(str(body.get("zip")), lst)
    if card["mao"] is None:
        return jsonify({"error": "This property has no max offer, so it can't go to the tracker from here"}), 400
    try:
        chosen = _chosen_comps(card, body.get("comps"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    # ARV: a typed ARV wins; otherwise the owner's checked comps, run through the
    # same rule as the search; otherwise the search's ARV.
    if arv_in is not None:
        arv = arv_in
    elif chosen:
        from flip_analysis import arv_calculation
        pool = {c["address"]: c for c in card["comps"] + card["excluded_comps"]}
        calc = arv_calculation(card.get("sqft"), [pool[a] for a in chosen], anchor=chosen[0])
        if not calc:
            return jsonify({"error": "Can't work out an ARV from those comps"}), 400
        arv = calc["arv"]
    else:
        arv = round(card["arv"])
    rehab = rehab_in if rehab_in is not None else round(card["rehab"] or 0)
    fee = {"wholesale_fee": fee_in} if fee_in is not None else {}
    mao, _ = max_allowable_offer(arv, rehab, **fee)
    if mao is None:
        return jsonify({"error": "No offer reaches a 10% profit at that ARV, rehab and wholesale fee"}), 400
    try:
        result = tracker_sheet.add_property(card["address"], card["zillow_url"], arv, offer, rehab,
                                            tracker_comps(card, chosen))
    except Exception as e:
        logger.error(f"[distressed] add to tracker failed for {card['address']}: {e}", exc_info=True)
        return jsonify({"error": f"Couldn't write to the tracker sheet: {type(e).__name__}"}), 502
    db = get_session()
    try:
        record_decision(db, card["address"], card["zip"], "sent", list_price=card.get("list_price"),
                        tracker_row=result.get("row"))
    except Exception as e:                       # the row is in the sheet; the card just stays put
        logger.error(f"[distressed] couldn't save sent decision for {card['address']}: {e}")
        db.rollback()
    finally:
        db.close()
    return jsonify(result)


@bp.route("/api/distressed/decision", methods=["POST"])
def set_decision():
    """{search_id, zip, address, action: 'pass' | 'undo', reason?}.

    Pass saves the list price from the saved search, so a later drop brings
    the property back. Undo puts it in the main list, even when it is already
    in the tracker sheet."""
    body = request.get_json(silent=True) or {}
    action, reason = body.get("action"), (body.get("reason") or None)
    if action not in ("pass", "undo"):
        return jsonify({"error": "action must be pass or undo"}), 400
    if reason is not None and reason not in PASS_REASONS:
        return jsonify({"error": "Unknown reason"}), 400
    db = get_session()
    try:
        row = db.get(DistressedSearch, int(body.get("search_id") or 0))
        zip_code = str(body.get("zip") or "")
        lst = _find_listing(row, zip_code, body.get("address") or "") if row else None
        if not lst:
            return jsonify({"error": "Property not in this search"}), 404
        if action == "pass":
            record_decision(db, lst["address"], zip_code, "passed", reason, lst.get("price"))
        else:
            record_decision(db, lst["address"], zip_code, "active")
        return jsonify({"ok": True})
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
