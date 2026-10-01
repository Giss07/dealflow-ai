"""
Builder Zones tests — classification, distance, dedupe, email, worker jobs.

    python test_builder_zones.py           # offline only, no API calls, free
    python test_builder_zones.py --live    # also hits OpenWeb Ninja for one zip
                                           # (2 calls, ~$0.005) — default 92503
    python test_builder_zones.py --live 92505

Uses a throwaway SQLite file in the system temp dir, so it never touches
dealflow.db or Railway Postgres. Email sending is stubbed — the offline run
sends nothing.
"""

import os
import sys
import json
import tempfile
import datetime as _dt

LIVE = "--live" in sys.argv
LIVE_ZIP = next((a for a in sys.argv[1:] if a.isdigit()), "92503")

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="dealflow_bz_"), "test_bz.db")
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_DB}"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_DB}"   # .env must not override the test DB
os.environ["BUILDER_ZONE_ZIPS"] = LIVE_ZIP

import logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

import builder_zones as bz
from database import init_db, get_session, NewBuild, BuilderZoneMatch

FAILS = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{(' — ' + str(detail)) if detail else ''}")
    if not cond:
        FAILS.append(label)


# A builder listing and an ordinary agent listing, in the shape /search returns.
NEW_BUILD_ITEM = {
    "zpid": "464402754", "streetAddress": "3750 McAllister Pkwy", "city": "Riverside",
    "state": "CA", "zipcode": "92503", "latitude": 33.887005, "longitude": -117.44456,
    "homeType": "TOWNHOUSE", "price": 549990, "unformattedPrice": 549990,
    "listingSubType": {"is_newHome": True}, "marketingStatusSimplifiedCd": "New Construction",
    "statusText": "New construction", "newConstructionType": "NEW_CONSTRUCTION_TYPE_OTHER",
    "brokerName": "BMC REALTY ADVISORS", "detailUrl": "https://www.zillow.com/x/",
}
RESALE_ITEM = {
    "zpid": "17812383", "streetAddress": "7129 Rutland Ave", "city": "Riverside",
    "state": "CA", "zipcode": "92503", "latitude": 33.958916, "longitude": -117.46914,
    "homeType": "SINGLE_FAMILY", "price": 559000, "unformattedPrice": 559000,
    "listingSubType": {"is_FSBA": True}, "marketingStatusSimplifiedCd": "For Sale by Agent",
    "statusText": "House for sale", "brokerName": "Keller Williams Realty",
    "bedrooms": 3, "bathrooms": 3, "livingArea": 1325, "daysOnZillow": 50,
}


def test_distance():
    print("\n=== Distance ===")
    # 1 degree of latitude is ~69 miles, so 0.5/69 degrees north is half a mile.
    d = bz.haversine_miles(33.95, -117.45, 33.95 + 0.5 / 69.0, -117.45)
    check("haversine 0.5 mi north", abs(d - 0.5) < 0.01, f"{d:.4f} mi")
    check("haversine identity is 0", bz.haversine_miles(33.95, -117.45, 33.95, -117.45) == 0)


def test_classification():
    print("\n=== Classification ===")
    check("builder listing flagged", bz.is_new_build(NEW_BUILD_ITEM))
    check("agent resale not flagged", not bz.is_new_build(RESALE_ITEM))
    check("is_newHome alone is enough", bz.is_new_build({"listingSubType": {"is_newHome": True}}))
    check("marketing status alone is enough",
          bz.is_new_build({"marketingStatusSimplifiedCd": "New Construction"}))
    check("builder name from brokerName",
          bz.builder_name(NEW_BUILD_ITEM) == "BMC REALTY ADVISORS")
    check("missing broker falls back", bz.builder_name({}) == "Unknown builder")

    os.environ["BUILDER_ZONE_EXCLUDE_HOME_TYPES"] = ""
    nb, rs = bz.split_listings([NEW_BUILD_ITEM, RESALE_ITEM], "92503")
    check("split_listings partitions", len(nb) == 1 and len(rs) == 1, f"{len(nb)} new / {len(rs)} resale")
    check("coordinates carried through", nb[0]["latitude"] == 33.887005)

    manufactured = dict(NEW_BUILD_ITEM, homeType="MANUFACTURED", zpid="999")
    os.environ["BUILDER_ZONE_EXCLUDE_HOME_TYPES"] = "MANUFACTURED"
    nb2, rs2 = bz.split_listings([manufactured, RESALE_ITEM], "92503")
    check("excluded home type drops from both sides", len(nb2) == 0 and len(rs2) == 1,
          f"{len(nb2)} new / {len(rs2)} resale")
    os.environ.pop("BUILDER_ZONE_EXCLUDE_HOME_TYPES", None)

    unmappable = dict(RESALE_ITEM, zpid="888", latitude=None, longitude=None, latLong=None)
    nb3, rs3 = bz.split_listings([unmappable], "92503")
    check("listing with no coordinates dropped", len(nb3) == 0 and len(rs3) == 0)


BUILD = {"zpid": "B1", "address": "1 Builder Way", "city": "Riverside", "zip_code": "92503",
         "latitude": 33.9, "longitude": -117.4, "builder_name": "Testbuilder", "price": 700000,
         "home_type": "SINGLE_FAMILY", "listing_url": ""}
INSIDE = {"zpid": "L1", "address": "2 Old St", "city": "Riverside", "zip_code": "92503",
          "latitude": 33.9 + 0.4 / 69.0, "longitude": -117.4, "price": 400000,
          "home_type": "SINGLE_FAMILY", "listing_url": "", "days_on_zillow": 90,
          "beds": 3, "baths": 2, "sqft": 1400}
OUTSIDE = dict(INSIDE, zpid="L2", address="3 Far Rd", latitude=33.9 + 0.9 / 69.0, home_type="LOT")


def test_matching():
    print("\n=== Radius matching ===")
    m = bz.match_listings([INSIDE, OUTSIDE], [BUILD], radius=0.5)
    check("0.4 mi in, 0.9 mi out", len(m) == 1 and m[0]["listing"]["zpid"] == "L1",
          [x["listing"]["zpid"] for x in m])
    check("distance recorded", abs(m[0]["distance_miles"] - 0.4) < 0.01, m[0]["distance_miles"])

    nearer = dict(BUILD, zpid="B2", address="4 Closer Ct", latitude=33.9 + 0.35 / 69.0,
                  builder_name="Closerbuilder")
    m2 = bz.match_listings([INSIDE], [BUILD, nearer], radius=0.5)
    check("reports the NEAREST builder once",
          len(m2) == 1 and m2[0]["new_build"]["builder_name"] == "Closerbuilder",
          [(x["new_build"]["builder_name"], x["distance_miles"]) for x in m2])


def _reset_db():
    init_db()
    db = get_session()
    db.query(BuilderZoneMatch).delete()
    db.query(NewBuild).delete()
    db.commit()
    db.close()


def test_persistence():
    print("\n=== Persistence and dedupe ===")
    _reset_db()
    rec = bz.normalize(NEW_BUILD_ITEM, "92503")
    r1 = bz.save_new_builds([rec])
    r2 = bz.save_new_builds([rec])
    check("first save inserts", r1["added"] == 1, r1)
    check("second save dedupes on zpid", r2["added"] == 0 and r2["skipped"] == 1, r2)

    db = get_session()
    rows = db.query(NewBuild).all()
    row = rows[0]
    check("one row stored", len(rows) == 1, len(rows))
    check("spec columns populated (address, zip, lat/lng, builder, price, date_found, source)",
          all([row.address, row.zip_code, row.latitude, row.longitude, row.builder_name,
               row.price, row.date_found, row.source]),
          f"{row.address} | {row.zip_code} | {row.latitude},{row.longitude} | "
          f"{row.builder_name} | ${row.price:,.0f} | {row.source}")
    db.close()

    nozpid = {"zpid": None, "address": "99  Lot  Ln", "city": "Riverside", "zip_code": "92503",
              "latitude": 33.9, "longitude": -117.4, "builder_name": "NoZpid Homes",
              "price": 500000, "home_type": "LOT", "listing_url": ""}
    a1 = bz.save_new_builds([nozpid])
    a2 = bz.save_new_builds([nozpid])
    check("address+zip fallback dedupe", a1["added"] == 1 and a2["added"] == 0, (a1, a2))


def test_alert_dedupe():
    print("\n=== Alert dedupe ===")
    _reset_db()
    bz.save_new_builds([BUILD])
    matches = bz.match_listings([INSIDE], [BUILD], radius=0.5)

    p1 = bz.save_matches(matches)
    check("new match pending an alert", len(p1) == 1 and p1[0]["id"] is not None, p1)

    db = get_session()
    for r in db.query(BuilderZoneMatch).all():
        r.alert_sent = True
    db.commit()
    db.close()

    p2 = bz.save_matches(matches)
    check("alerted match not queued again", len(p2) == 0, len(p2))

    # An id must survive the caller closing the session — the bug that made
    # the first version re-email every run.
    _reset_db()
    bz.save_new_builds([BUILD])
    db = get_session()
    p3 = bz.save_matches(matches, db=db)
    db.close()
    check("ids usable after the caller closes the session",
          p3 and isinstance(p3[0]["id"], int), p3[0] if p3 else None)


def test_email():
    print("\n=== Email rendering ===")
    m = bz.match_listings([INSIDE], [BUILD], radius=0.5)
    subject, html, text = bz.format_match_email(m)
    check("subject counts the matches", "1 listing" in subject, subject)
    check("price in body", "$400,000" in html and "$400,000" in text)
    check("distance in body", "0.40 mi" in html and "0.40 mi" in text)
    check("builder name in body", "Testbuilder" in html and "Testbuilder" in text)
    check("listing address in body", "2 Old St" in html and "2 Old St" in text)
    s2, _h2, _t2 = bz.format_match_email(m + m)
    check("plural subject", "2 listings" in s2, s2)


def test_recipients():
    print("\n=== Digest recipients ===")
    saved = {k: os.environ.get(k) for k in
             ("BUILDER_ZONE_ALERT_EMAILS", "ALERT_EMAILS", "ALERT_EMAIL")}
    try:
        # Only BUILDER_ZONE_ALERT_EMAILS counts. ALERT_EMAILS must NOT leak in —
        # enabling Builder Zones cannot change who gets the other alerts.
        os.environ.pop("BUILDER_ZONE_ALERT_EMAILS", None)
        os.environ["ALERT_EMAILS"] = "someone@example.com"
        os.environ["ALERT_EMAIL"] = "someone-else@example.com"
        check("ALERT_EMAILS is not used as a fallback", bz.alert_recipients() == [],
              bz.alert_recipients())

        m = bz.match_listings([INSIDE], [BUILD], radius=0.5)
        ok, err = bz.send_match_email(m)
        check("send refuses with no recipients, naming the right variable",
              ok is False and err == "BUILDER_ZONE_ALERT_EMAILS not set", (ok, err))

        os.environ["BUILDER_ZONE_ALERT_EMAILS"] = " a@example.com , b@example.com ,, "
        check("comma list parsed and trimmed",
              bz.alert_recipients() == ["a@example.com", "b@example.com"],
              bz.alert_recipients())
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_worker_jobs():
    print("\n=== Worker jobs (hour gate patched, email stubbed) ===")
    _reset_db()
    import worker

    class _At9:
        @staticmethod
        def now(tz=None):
            return _dt.datetime(2026, 1, 1, 9, 5, tzinfo=tz)

    class _At3:
        @staticmethod
        def now(tz=None):
            return _dt.datetime(2026, 1, 1, 3, 0, tzinfo=tz)

    real_datetime = worker.datetime
    real_scan_zip = bz.scan_zip
    real_send = bz.send_match_email
    sent = {}

    def _fake_scan_zip(zip_code, api_key, pages=None):
        return [BUILD], [INSIDE, OUTSIDE], None

    def _fake_send(matches):
        sent["matches"] = matches
        return True, None

    try:
        worker.datetime = _At3
        check("jobs no-op outside 9 AM Pacific",
              worker.run_builder_zone_scan() is None and worker.run_builder_zone_matches() is None)

        worker.datetime = _At9
        bz.scan_zip = _fake_scan_zip
        bz.send_match_email = _fake_send

        scan = worker.run_builder_zone_scan()
        check("scan job stores the new build", scan["added"] == 1 and scan["zips_scanned"] == 1, scan)

        match = worker.run_builder_zone_matches()
        check("match job emails the in-radius listing only",
              match["emailed"] == 1 and len(sent["matches"]) == 1, match)
        check("emailed match carries distance and builder",
              sent["matches"][0]["distance_miles"] < 0.5
              and sent["matches"][0]["new_build"]["builder_name"] == "Testbuilder")

        sent.clear()
        scan2 = worker.run_builder_zone_scan()
        match2 = worker.run_builder_zone_matches()
        check("re-run adds no duplicate build", scan2["added"] == 0, scan2)
        check("re-run sends no duplicate email", not sent and match2["emailed"] == 0, match2)
    finally:
        worker.datetime = real_datetime
        bz.scan_zip = real_scan_zip
        bz.send_match_email = real_send


def test_live(zip_code):
    print(f"\n=== LIVE {zip_code} (OpenWeb Ninja) ===")
    key = os.getenv("OPENWEB_NINJA_API_KEY", "")
    if not key:
        check("OPENWEB_NINJA_API_KEY set", False, "not configured — skipping live test")
        return
    before = bz.calls_made()
    result = bz.find_builder_zones_for_zip(zip_code, key)
    if "error" in result:
        check(f"live scan of {zip_code}", False, result["error"])
        return

    for b in result["new_builds"]:
        print(f"    BUILD  {b['address']}, {b['city']} {b['zip_code']} — {b['builder_name']} — "
              f"${(b['price'] or 0):,.0f} — {b['home_type']}")
    for m in result["matches"]:
        l = m["listing"]
        print(f"    NEAR   {l['address']} — ${(l['price'] or 0):,.0f} — {m['distance_miles']:.2f} mi "
              f"from {m['new_build']['builder_name']}")

    check("live search returned listings", result["resale_count"] > 0, result["resale_count"])
    check("every new build has coordinates",
          all(b["latitude"] and b["longitude"] for b in result["new_builds"]))
    check("every match is inside the radius",
          all(m["distance_miles"] <= result["radius_miles"] for m in result["matches"]))
    print(f"  billable API calls: {bz.calls_made() - before} "
          f"(${(bz.calls_made() - before) * bz.COST_PER_CALL:.4f})")


if __name__ == "__main__":
    print(f"Builder Zones tests — DB {_TMP_DB}")
    test_distance()
    test_classification()
    test_matching()
    test_persistence()
    test_alert_dedupe()
    test_email()
    test_recipients()
    test_worker_jobs()
    if LIVE:
        test_live(LIVE_ZIP)
    else:
        print("\n(skipping live OpenWeb Ninja test — pass --live to run it)")

    print(f"\n=== {'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)} ===")
    sys.exit(1 if FAILS else 0)
