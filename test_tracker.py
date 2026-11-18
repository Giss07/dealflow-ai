"""
Tests for "Add to Tracker": tracker_sheet.py (address identity, the new row),
the page endpoint, and the JavaScript flip calculator against the Python one.

A fake Sheets API stands in for Google, so nothing is written to the real sheet.

    python test_tracker.py
"""

import json
import os
import random
import subprocess
import sys
import tempfile
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_TMP_DB = os.path.join(tempfile.mkdtemp(), "tracker.db")
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_DB}"
os.environ["DASHBOARD_PASSWORD"] = ""

import tracker_sheet as ts                       # noqa: E402
from flip_calculator import max_allowable_offer, flip_costs   # noqa: E402

FAILS = []
HERE = os.path.dirname(os.path.abspath(__file__))


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{(' — ' + str(detail)) if detail else ''}")
    if not cond:
        FAILS.append(label)


# ── Fake Sheets API ───────────────────────────────────────────────────

class Resp:
    def __init__(self, data, code=200):
        self.status_code, self._data, self.text = code, data, json.dumps(data)

    def json(self):
        return self._data


class FakeSheets:
    def __init__(self, tracker, stp=(), closed=()):
        self.cols = {"Properties_Offer_Tracker_Template": ["Address", *tracker],
                     "Archive_STP": ["Address", *stp], "Archive_Closed": ["Address", *closed]}
        self.batches = []

    def get(self, url, params=None):
        params = params or {}
        if url.endswith("values:batchGet"):
            out = []
            for r in params["ranges"]:
                tab = r.split("'")[1]
                out.append({"values": [self.cols[tab][1:]]})
            return Resp({"valueRanges": out})
        if "/values/" in url:
            tab = url.split("/values/")[1].split("'")[1]
            return Resp({"values": [self.cols[tab]]})
        return Resp({"sheets": [
            {"properties": {"sheetId": 516040325, "title": "Properties_Offer_Tracker_Template", "index": 0,
                            "gridProperties": {"rowCount": 1019, "columnCount": 23}}},
            {"properties": {"sheetId": 1, "title": "Archive_STP", "index": 2, "gridProperties": {}}},
            {"properties": {"sheetId": 2, "title": "Archive_Closed", "index": 3, "gridProperties": {}}},
            {"properties": {"sheetId": 3, "title": "Dashboard", "index": 4, "gridProperties": {}}},
        ]})

    def post(self, url, json=None):
        self.batches.append(json["requests"])
        return Resp({"replies": []})


def use(fake):
    ts._session = lambda: (fake, "https://sheets.example/v4/spreadsheets/X")
    ts._index["rows"] = None


TRACKER = ["5728 Hillsdale Blvd, Sacramento, CA 95842", "27335 Potomac Dr Sun City, CA, 92586",
           "1517 S Klein Avenue, Reedley, CA 93654"]


def test_address_identity():
    print("\n=== Same property, across sheet formats ===")
    check("'Avenue' vs 'Ave'", ts.same_property("1517 S Klein Ave, Reedley, CA 93654", TRACKER[2]))
    check("city run into the street ('Potomac Dr Sun City, CA, 92586')",
          ts.same_property("27335 Potomac Dr, Sun City, CA 92586", TRACKER[1]))
    check("archive's two-line address ('3060 Linder\\nHemet, CA 92545')",
          ts.same_property("3060 Linder, Hemet, CA 92545", "3060 Linder\nHemet, CA 92545"))
    check("same street, other ZIP → different property",
          not ts.same_property("1206 S Palm St, Anaheim, CA 92805", "1206 S Palm St, Hemet, CA 92543"))
    check("'123 Main St' is not '1123 Main St'",
          not ts.same_property("123 Main St, Riverside, CA 92503", "1123 Main St, Riverside, CA 92503"))
    check("unit ignored for identity ('APT B')",
          ts.same_property("182 W Walnut Ave APT B, Rialto, CA 92376", "182 W Walnut Ave, Rialto, CA 92376"))


def test_add_row():
    print("\n=== The new row ===")
    fake = FakeSheets(TRACKER)
    use(fake)
    r = ts.add_property("1485 N Fillmore Ave, Rialto, CA 92376", "https://www.zillow.com/homedetails/x/", 712_270,
                        540_000, 40_000, "From Distressed Deals – 3 comps, light rehab, max offer $540,300",
                        today=date(2026, 11, 18))
    check("added after the last address (row 5 here), not inserted", r == {"status": "added", "row": 5,
                                                                          "tab": "Properties_Offer_Tracker_Template"}, r)
    reqs = fake.batches[0]
    check("no row insertion — only copy formatting/dropdowns and set cells",
          {list(x)[0] for x in reqs} == {"copyPaste", "updateCells"})
    pastes = [x["copyPaste"] for x in reqs if "copyPaste" in x]
    check("formats and dropdowns copied from the row above",
          {p["pasteType"] for p in pastes} == {"PASTE_FORMAT", "PASTE_DATA_VALIDATION"}
          and all(p["source"]["startRowIndex"] == 3 and p["destination"]["startRowIndex"] == 4 for p in pastes))
    cells = {x["updateCells"]["range"]["startColumnIndex"]: x["updateCells"] for x in reqs if "updateCells" in x}
    val = lambda c: (cells[c]["rows"][0]["values"][0].get("userEnteredValue") or {})
    check("A: today's date as a sheet date", val(0) == {"numberValue": 46344}, val(0))
    check("B: Lead Source 'Other'", val(1) == {"stringValue": "Other"})
    check("C: full address, linked to Zillow",
          val(2) == {"stringValue": "1485 N Fillmore Ave, Rialto, CA 92376"}
          and cells[2]["rows"][0]["values"][0]["userEnteredFormat"]["textFormat"]["link"]["uri"] == "https://www.zillow.com/homedetails/x/")
    check("D ARV, E offer, F rehab", (val(3), val(4), val(5)) == ({"numberValue": 712270}, {"numberValue": 540000},
                                                                   {"numberValue": 40000}))
    check("G '10 Days ', H '3 Days ', I 1%", (val(6), val(7), val(8)) == ({"stringValue": "10 Days "},
                                                                          {"stringValue": "3 Days "}, {"numberValue": 0.01}))
    check("J Sent Date and K Status left blank", val(9) == {} and val(10) == {})
    check("N: notes", val(13) == {"stringValue": "From Distressed Deals – 3 comps, light rehab, max offer $540,300"})
    check("nothing else written (L, M, O+ untouched)", set(cells) == {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13}, sorted(cells))

    fake2 = FakeSheets(TRACKER)
    use(fake2)
    r2 = ts.add_property("1517 S Klein Ave, Reedley, CA 93654", "", 1, 1, 1, "x")
    check("already in the tracker → nothing written", r2["status"] == "exists" and r2["where"] == "tracker"
          and r2["row"] == 4 and fake2.batches == [], r2)
    fake3 = FakeSheets(TRACKER, stp=["3060 Linder\nHemet, CA 92545"])
    use(fake3)
    r3 = ts.add_property("3060 Linder, Hemet, CA 92545", "", 1, 1, 1, "x")
    check("in Archive_STP → 'archive', nothing written", r3["status"] == "exists" and r3["where"] == "archive"
          and fake3.batches == [], r3)


def test_endpoint():
    print("\n=== POST /api/distressed/tracker ===")
    import app as webapp
    from database import DistressedSearch, get_session
    listing = {"address": "1485 N Fillmore Ave, Rialto, CA 92376", "price": 600_000, "days": 9,
               "analysis": {"arv": 712_270, "mao": 528_100, "mao_vs_list_pct": -0.12, "profit_at_mao": 71_331,
                            "rehab": {"tier": "light", "per_sqft": 30, "amount": 54_000}, "confidence": "OK",
                            "comp_flags": [], "comps_used": 3, "home_type": "SINGLE_FAMILY",
                            "zillow_url": "https://www.zillow.com/homedetails/1485/",
                            "comps": [{"address": f"c{i}", "price_per_sqft": 400, "remodeled": "REMODELED"} for i in range(3)]}}
    db = get_session()
    row = DistressedSearch(zips="92376", status="completed", zips_done=1,
                           result=json.dumps({"92376": {"status": "ok", "listings": [listing]}}))
    db.add(row); db.commit(); sid = row.id; db.close()
    calls = []
    ts.add_property = lambda *a: (calls.append(a), {"status": "added", "row": 183, "tab": "T"})[1]
    ts.find_property = lambda address, force=False: None
    client = webapp.app.test_client()
    post = lambda **b: client.post("/api/distressed/tracker", json={"search_id": sid, "zip": "92376",
                                                                    "address": listing["address"], **b})
    r = post(offer=528_100)
    check("unedited: adds with the search's ARV and rehab", r.status_code == 200 and calls[-1][2:5] == (712_270, 528_100, 54_000),
          calls[-1][2:5] if calls else r.get_json())
    check("notes: comps, tier, max offer", calls[-1][5] == "From Distressed Deals – 3 comps, light rehab, max offer $528,100",
          calls[-1][5])
    exp_mao, _ = max_allowable_offer(712_270, 40_000)
    r = post(offer=540_000, rehab=40_000)
    check("rehab edited to $40,000 → F = 40,000, E = the typed offer", calls[-1][2:5] == (712_270, 540_000, 40_000))
    check("notes say the rehab was edited and give the updated max offer",
          calls[-1][5] == f"From Distressed Deals – 3 comps, light rehab, max offer ${exp_mao:,}; Rehab edited from $54,000 to $40,000",
          calls[-1][5])
    r = post(offer=500_000, arv=690_000, rehab=40_000)
    check("ARV edit is noted too", calls[-1][5].endswith("; Rehab edited from $54,000 to $40,000; ARV edited from $712,270 to $690,000"),
          calls[-1][5])
    check("no offer → 400", post(offer=None).status_code == 400)
    check("nonsense numbers → 400", post(offer=500_000, arv="abc").status_code == 400
          and post(offer=-5).status_code == 400)
    check("unknown property → 404", client.post("/api/distressed/tracker", json={"search_id": sid, "zip": "92376",
                                                                              "address": "nope", "offer": 1}).status_code == 404)
    ts.find_property = lambda address, force=False: {"where": "tracker", "row": 140, "tab": "T", "address": address}
    d = client.get(f"/api/distressed/search/{sid}").get_json()
    check("search results mark the card 'tracker' with its row", d["cards"][0]["tracker"] == "tracker"
          and d["cards"][0]["tracker_row"] == 140, d["cards"][0].get("tracker"))
    page = client.get("/distressed").get_data(as_text=True)
    check("page: Add to Tracker, In tracker, Already in tracker; no 'Download for agent'",
          all(t in page for t in ("Add to Tracker", "✓ In tracker", "Already in tracker", "Download all (CSV)"))
          and "Download for agent" not in page)
    check("page loads the JS calculator", '<script src="/static/flip_calculator.js">' in page
          and client.get("/static/flip_calculator.js").status_code == 200)


def test_js_matches_python():
    print("\n=== JavaScript calculator = Python calculator ===")
    rnd = random.Random(7)
    cases = [(665_000, 45_000), (712_270, 40_000), (712_270, 54_000), (100_000, 200_000)]
    cases += [(rnd.randrange(80_000, 2_500_000), rnd.randrange(0, 300_000)) for _ in range(3000)]
    js = subprocess.run(["node", "-e", f"""
        const f = require({json.dumps(os.path.join(HERE, 'dashboard/static/flip_calculator.js'))});
        const cases = {json.dumps(cases)};
        console.log(JSON.stringify(cases.map(([a, r]) => {{ const m = f.maxAllowableOffer(a, r);
            return [m.mao, m.profit == null ? null : Math.round(m.profit * 100) / 100]; }})));"""],
        capture_output=True, text=True, timeout=60)
    got = json.loads(js.stdout)
    bad = []
    for (arv, rehab), (jm, jp) in zip(cases, got):
        pm, at = max_allowable_offer(arv, rehab)
        pp = round(at["profit"], 2) if at else None
        if pm != jm or (pp is not None and abs(pp - jp) > 0.01) or (pp is None) != (jp is None):
            bad.append((arv, rehab, pm, jm, pp, jp))
    check(f"same MAO and profit on {len(cases)} cases", not bad, bad[:3])
    check("owner's test through JS: MAO $498,400", got[0][0] == 498_400)
    check("1485 N Fillmore, rehab $40,000: JS = Python", got[1][0] == max_allowable_offer(712_270, 40_000)[0], got[1])


if __name__ == "__main__":
    test_address_identity()
    test_add_row()
    test_endpoint()
    test_js_matches_python()
    print(f"\n=== {'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)} ===")
    sys.stdout.flush()
    os._exit(1 if FAILS else 0)
