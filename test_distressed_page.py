"""
Tests for the Distressed Deals page (distressed_page.py + distressed_report.py).

Runs the Flask app against a throwaway SQLite database with search_distressed
stubbed, so there are no API calls and nothing is spent.

    python test_distressed_page.py
"""

import json
import os
import sys
import tempfile
import threading
import time
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_TMP_DB = os.path.join(tempfile.mkdtemp(), "page.db")
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_DB}"
os.environ["DASHBOARD_PASSWORD"] = ""          # no login wall in the test client

FAILS = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{(' — ' + str(detail)) if detail else ''}")
    if not cond:
        FAILS.append(label)


def listing(address, price, mao=None, arv=None, home_type="SINGLE_FAMILY", confidence="OK", comps=2, days=30):
    pct = (mao - price) / price if mao is not None else None
    return {
        "address": address, "price": price, "days": days, "beds": 3, "baths": 2, "sqft": 1400,
        "distress_signals": ["Keywords: fixer"],
        "analysis": {
            "arv": arv, "mao": mao, "mao_vs_list": (mao - price) if mao is not None else None,
            "mao_vs_list_pct": pct, "profit_at_mao": 70_000 if mao else None,
            "rehab": {"tier": "medium", "per_sqft": 55, "amount": 77_000, "reason": "medium: fixer"},
            "confidence": confidence, "comp_flags": [], "comps_used": comps, "home_type": home_type,
            "arv_basis": f"{comps} comps: 1 remodeled, {comps - 1} unknown" if comps else "",
            "remodeled_comps": 1, "unknown_comps": max(comps - 1, 0), "skipped_fixers": [],
            "zillow_url": "https://www.zillow.com/homedetails/x/", "pool": False, "senior": False,
            "year_built": 1975, "note": None if mao else "no usable sold comps",
            "comps": [{"address": f"{i} Comp St, Santa Ana, CA 92703", "sold_price": 800_000 + i, "sold_date": "2026-10-01",
                       "sqft": 1400, "price_per_sqft": 571.4, "distance_mi": 0.2, "pool": False, "senior": False,
                       "pool_match": True, "senior_match": True, "remodeled": "REMODELED" if i == 0 else "UNKNOWN",
                       "remodeled_evidence": "renovated", "url": ""} for i in range(comps)],
        },
    }


FIXTURE = {
    "92703": [
        listing("1 Far St, Santa Ana, CA 92703", 800_000, mao=560_000, arv=850_000),         # exactly 30% below
        listing("6 Long Way, Santa Ana, CA 92703", 1_000_000, mao=600_000, arv=900_000),     # 40% below
        listing("7 Edge Ln, Santa Ana, CA 92703", 1_000_000, mao=850_000, arv=1_100_000),    # exactly 15% below
        listing("8 Bogus Ave, Rialto, CA 92376", 360_000, mao=1_034_600, arv=1_320_000,       # ARV 3.7x list
                confidence="LOW CONFIDENCE", comps=1),
        listing("9 Park Rd Space 63, Rialto, CA 92376", 110_000, mao=20_800, arv=55_000,     # mobile home
                home_type="MANUFACTURED", confidence="LOW CONFIDENCE", comps=1),
        listing("2 Near St, Santa Ana, CA 92703", 700_000, mao=630_000, arv=900_000),        # 10% below → GOOD DEAL
        listing("3 Units Ave, Santa Ana, CA 92703", 900_000, home_type="MULTI_FAMILY", confidence="LOW CONFIDENCE", comps=0),
        listing("4 Thin Ct, Santa Ana, CA 92703", 600_000, mao=520_000, arv=700_000, confidence="LOW CONFIDENCE", comps=2),
    ],
    "92704": [listing("5 Over Rd, Santa Ana, CA 92704", 500_000, mao=510_000, arv=750_000)],  # MAO above list
}
next(l for l in FIXTURE["92703"] if l["address"].startswith("2 Near St"))["analysis"]["arv_explain"] = (
    "($635,000 + $648,000 + $645,000) ÷ 3 = $642,667")
next(l for l in FIXTURE["92703"] if l["address"].startswith("2 Near St"))["analysis"]["excluded_comps"] = [{
    "address": "530 W Rialto Ave, Rialto, CA 92376", "sold_price": 1_500_000, "sold_date": "2026-08-17",
    "sqft": 900, "price_per_sqft": 1666.7, "distance_mi": 0.41, "pool": False, "senior": False,
    "remodeled": "UNKNOWN", "remodeled_evidence": "no wording either way", "lot_sqft": 37_897,
    "reason": "Sold by public record only (not on the MLS)", "url": ""}]
release = threading.Event()


def fake_search(zip_code):
    release.wait(10)
    if zip_code == "99999":
        return json.dumps({"status": "error", "error": "OpenWeb Ninja HTTP 500"})
    rows = FIXTURE.get(zip_code, [])
    return json.dumps({"status": "ok", "zip": zip_code, "count": len(rows), "listings": rows})


sys.modules["mcp_server"] = types.SimpleNamespace(search_distressed=fake_search)
import distressed_page                         # noqa: E402
distressed_page.SEARCH_ATTEMPTS = 1
import app as webapp                           # noqa: E402

client = webapp.app.test_client()


def wait_done(search_id, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        d = client.get(f"/api/distressed/search/{search_id}").get_json()
        if d["status"] != "running":
            return d
        time.sleep(0.05)
    return d


def test_page_and_validation():
    print("\n=== Page and input ===")
    r = client.get("/distressed")
    check("GET /distressed renders the page", r.status_code == 200 and b"Distressed Deals" in r.data)
    check("page has the three deal tags, Couldn't price, and the Not used in ARV list",
          all(t in r.data for t in (b"GOOD DEAL", b"MAKE AN OFFER", b"LONG SHOT", "Couldn’t price".encode(),
                                    b"Not used in ARV")))
    check("dashboard links to it", b'href="/distressed"' in client.get("/").data)
    r = client.post("/api/distressed/search", json={"zips": "abc"})
    check("no ZIP → 400", r.status_code == 400)
    r = client.post("/api/distressed/search", json={"zips": "90001 90002 90003 90004 90005 90006"})
    check("more than 5 ZIPs → 400", r.status_code == 400)


def test_search_flow():
    print("\n=== Search, cards and tags ===")
    release.clear()
    r = client.post("/api/distressed/search", json={"zips": "92703, 92704 92703"})
    sid = r.get_json()["id"]
    check("POST starts a search, duplicate ZIPs dropped", r.status_code == 200 and r.get_json()["zips"] == ["92703", "92704"],
          r.get_json())
    running = client.get(f"/api/distressed/search/{sid}").get_json()
    check("status is running while it works", running["status"] == "running" and running["zips_done"] == 0)
    r2 = client.post("/api/distressed/search", json={"zips": "92705"})
    check("a second search while one runs → 409 with the running id", r2.status_code == 409 and r2.get_json()["id"] == sid)
    release.set()
    d = wait_done(sid)
    check("completes", d["status"] == "completed" and d["zips_done"] == 2, d["status"])
    order = [c["street"] for c in d["cards"]]
    check("trusted deals first, then CHECK COMPS ones, then the unpriced",
          order[:4] == ["5 Over Rd", "2 Near St", "7 Edge Ln", "1 Far St"] and order[4] == "6 Long Way"
          and order[5:7] == ["8 Bogus Ave", "4 Thin Ct"] and set(order[7:]) == {"3 Units Ave", "9 Park Rd Space 63"},
          order)
    by = {c["street"]: c for c in d["cards"]}
    tag = lambda st: by[st]["deal_tag"]
    check("10% below list → GOOD DEAL", tag("2 Near St") == "good")
    check("MAO above list → GOOD DEAL", tag("5 Over Rd") == "good")
    check("exactly 15% below → GOOD DEAL (0–15%)", tag("7 Edge Ln") == "good")
    check("exactly 30% below → MAKE AN OFFER (15–30%)", tag("1 Far St") == "offer")
    check("40% below → LONG SHOT (over 30%)", tag("6 Long Way") == "long")
    check("no max offer → no deal tag, with a reason",
          tag("3 Units Ave") is None and "Multi-family" in by["3 Units Ave"]["unpriced_reason"])
    check("priced cards have no unpriced reason", by["2 Near St"]["unpriced_reason"] is None)
    check("LOW CONFIDENCE tag on a thin-comp house", by["4 Thin Ct"]["low_confidence"] is True)
    check("LOW CONFIDENCE at 13% below → CHECK COMPS, never GOOD DEAL",
          by["4 Thin Ct"]["check"] == "CHECK COMPS" and by["4 Thin Ct"]["deal_tag"] is None)
    check("ARV over 1.5x list → ARV LOOKS TOO HIGH – CHECK COMPS, no deal tag",
          by["8 Bogus Ave"]["check"] == "ARV LOOKS TOO HIGH – CHECK COMPS" and by["8 Bogus Ave"]["deal_tag"] is None)
    check("a trusted card has no check tag", by["2 Near St"]["check"] is None)
    check("mobile home → Couldn't price, reason 'Mobile home', no MAO shown",
          by["9 Park Rd Space 63"]["mao"] is None and by["9 Park Rd Space 63"]["unpriced_reason"] == "Mobile home"
          and by["9 Park Rd Space 63"]["deal_tag"] is None)
    check("multi-family: MULTI-FAMILY tag, not LOW CONFIDENCE",
          by["3 Units Ave"]["multi_family"] is True and by["3 Units Ave"]["low_confidence"] is False)
    check("card carries comps with price, $/sqft, distance, remodeled",
          {"sold_price", "price_per_sqft", "distance_mi", "remodeled"} <= set(by["2 Near St"]["comps"][0]))
    check("city split from address", by["2 Near St"]["city"] == "Santa Ana", by["2 Near St"]["city"])
    check("the ARV calculation reaches the card", by["2 Near St"]["arv_explain"].startswith("($635,000 + $648,000"))
    check("excluded comps reach the card with their reason",
          by["2 Near St"]["excluded_comps"][0]["reason"].startswith("Sold by public record"))

    r = client.get(f"/api/distressed/search/{sid}/csv", query_string={"zip": "92703", "address": "2 Near St, Santa Ana, CA 92703"})
    body = r.get_data(as_text=True)
    check("per-property CSV downloads as an attachment",
          r.status_code == 200 and "attachment" in r.headers["Content-Disposition"] and "2_Near_St.csv" in r.headers["Content-Disposition"])
    check("it has the summary and the comps table",
          body.startswith("zip,92703") and "mao,630000" in body and "comp_address" in body and "0 Comp St" in body)
    check("the property CSV carries the ARV calculation", 'arv_calc,"($635,000 + $648,000 + $645,000) ÷ 3 = $642,667"' in body)
    check("the CSV lists the excluded comp as not used, with the reason",
          "used_in_arv" in body and ",no,Sold by public record only (not on the MLS)," in body)
    r = client.get(f"/api/distressed/search/{sid}/csv")
    check("download-all CSV has one row per listing", r.status_code == 200 and r.get_data(as_text=True).count("\n") == 10)
    check("unknown property → 404", client.get(f"/api/distressed/search/{sid}/csv",
                                                 query_string={"zip": "92703", "address": "nope"}).status_code == 404)
    check("latest points at this search", client.get("/api/distressed/search/latest").get_json()["id"] == sid)


def test_zip_error():
    print("\n=== A failing ZIP ===")
    release.set()
    sid = client.post("/api/distressed/search", json={"zips": "99999 92704"}).get_json()["id"]
    d = wait_done(sid)
    check("other ZIPs still return cards, the failure is reported",
          d["status"] == "completed" and len(d["cards"]) == 1 and "99999" in d["zip_errors"], d["zip_errors"])


def test_import_failure_marks_failed():
    print("\n=== A search that cannot start ===")
    saved = sys.modules.get("mcp_server")
    sys.modules["mcp_server"] = None          # import raises ImportError
    try:
        sid = client.post("/api/distressed/search", json={"zips": "92703"}).get_json()["id"]
        d = wait_done(sid)
        check("it is marked failed with the reason, not left running",
              d["status"] == "failed" and "mcp_server" in (d["error"] or ""), (d["status"], d["error"]))
    finally:
        sys.modules["mcp_server"] = saved


def test_no_offer_routes():
    print("\n=== Read-only ===")
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "distressed_page.py")).read()
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith(("#", '"', "'")))
    check("the page never calls submit_offer, gspread or the sheet updater",
          not any(w in code for w in ("submit_offer", "gspread", "dealflow_updater", "update_cell")))
    rules = [str(r) for r in webapp.app.url_map.iter_rules() if "distressed" in str(r)]
    methods = {m for r in webapp.app.url_map.iter_rules() if "distressed" in str(r) for m in r.methods}
    check("its only write is starting a search", methods - {"GET", "HEAD", "OPTIONS"} == {"POST"}
          and [r for r in webapp.app.url_map.iter_rules() if "distressed" in str(r) and "POST" in r.methods][0].rule
          == "/api/distressed/search", rules)


if __name__ == "__main__":
    test_page_and_validation()
    test_search_flow()
    test_zip_error()
    test_import_failure_marks_failed()
    test_no_offer_routes()
    print(f"\n=== {'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)} ===")
    sys.stdout.flush()
    os._exit(1 if FAILS else 0)            # don't wait on the stub search threads
