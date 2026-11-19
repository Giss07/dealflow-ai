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
    comps = [("1648 N Althea Ave, Rialto, CA 92376", "https://www.zillow.com/homedetails/althea/"),
             ("19449 Lurelane St, Rialto, CA 92376", "https://www.zillow.com/homedetails/lurelane/"),
             ("1730 N Vista Ave, Rialto, CA 92376", "https://www.zillow.com/homedetails/vista/")]
    r = ts.add_property("1485 N Fillmore Ave, Rialto, CA 92376", "https://www.zillow.com/homedetails/x/", 648_000,
                        455_600, 60_000, comps, today=date(2026, 11, 18))
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
    check("D ARV, E offer, F rehab", (val(3), val(4), val(5)) == ({"numberValue": 648000}, {"numberValue": 455600},
                                                                   {"numberValue": 60000}))
    check("G '10 Days ', H '3 Days ', I 1%", (val(6), val(7), val(8)) == ({"stringValue": "10 Days "},
                                                                          {"stringValue": "3 Days "}, {"numberValue": 0.01}))
    link = lambda c: cells[c]["rows"][0]["values"][0]["userEnteredFormat"]["textFormat"]["link"]["uri"]
    check("O Anchor Comp / P Comp #2 / Q Comp #3: street only, linked to Zillow",
          [val(c) for c in (14, 15, 16)] == [{"stringValue": "1648 N Althea Ave"}, {"stringValue": "19449 Lurelane St"},
                                             {"stringValue": "1730 N Vista Ave"}]
          and [link(c) for c in (14, 15, 16)] == [u for _, u in comps], [val(c) for c in (14, 15, 16)])
    check("Notes (N), Sent Date (J) and Status (K) never written", not {9, 10, 13} & set(cells), sorted(cells))
    check("only A–I and O–Q written", set(cells) == {0, 1, 2, 3, 4, 5, 6, 7, 8, 14, 15, 16}, sorted(cells))

    fake2 = FakeSheets(TRACKER)
    use(fake2)
    r2 = ts.add_property("1517 S Klein Ave, Reedley, CA 93654", "", 1, 1, 1, [])
    check("already in the tracker → nothing written", r2["status"] == "exists" and r2["where"] == "tracker"
          and r2["row"] == 4 and fake2.batches == [], r2)
    fake3 = FakeSheets(TRACKER, stp=["3060 Linder\nHemet, CA 92545"])
    use(fake3)
    r3 = ts.add_property("3060 Linder, Hemet, CA 92545", "", 1, 1, 1, [])
    check("in Archive_STP → 'archive', nothing written", r3["status"] == "exists" and r3["where"] == "archive"
          and fake3.batches == [], r3)


def test_endpoint():
    print("\n=== POST /api/distressed/tracker ===")
    import app as webapp
    from database import DistressedSearch, get_session
    listing = {"address": "1485 N Fillmore Ave, Rialto, CA 92376", "price": 600_000, "days": 9, "sqft": 1_800,
               "analysis": {"arv": 648_000, "mao": 461_000, "mao_vs_list_pct": -0.23, "profit_at_mao": 64_800,
                            "rehab": {"tier": "light", "per_sqft": 30, "amount": 54_000}, "confidence": "OK",
                            "comp_flags": [], "comps_used": 3, "home_type": "SINGLE_FAMILY",
                            "zillow_url": "https://www.zillow.com/homedetails/1485/",
                            "arv_calc": {"median_comp": "1648 N Althea Ave, Rialto, CA 92376"},
                            "comps": [{"address": a, "sold_price": p, "distance_mi": d, "url": f"https://z/{n}",
                                       "price_per_sqft": 400, "remodeled": "REMODELED", "sqft": q, "listed_ppsf": z}
                                      for n, (a, p, d, q, z) in enumerate([("19449 Lurelane St, Rialto, CA 92376", 635_000, 0.38, 1566, 405),
                                                                     ("1730 N Vista Ave, Rialto, CA 92376", 648_000, 0.47, 1748, 371),
                                                                     ("1648 N Althea Ave, Rialto, CA 92376", 645_000, 0.25, 1630, 396)])]}}
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
    r = post(offer=455_600)
    check("unedited: adds with the search's ARV and rehab", r.status_code == 200 and calls[-1][2:5] == (648_000, 455_600, 54_000),
          calls[-1][2:5] if calls else r.get_json())
    check("comps: Anchor = the median comp, then the other two closest first",
          [a.split(",")[0] for a, _ in calls[-1][5]] == ["1648 N Althea Ave", "19449 Lurelane St", "1730 N Vista Ave"]
          and [u for _, u in calls[-1][5]] == ["https://z/2", "https://z/0", "https://z/1"], calls[-1][5])
    post(offer=470_000, rehab=60_000, arv=650_000)
    check("edited ARV / rehab go to D / F, the typed offer to E", calls[-1][2:5] == (650_000, 470_000, 60_000), calls[-1][2:5])
    check("no notes are sent anywhere", len(calls[-1]) == 6 and not any("Distressed" in str(x) for x in calls[-1]))
    check("an edited wholesale fee is accepted", post(offer=480_000, fee=10_000).status_code == 200)
    from flip_analysis import arv_calculation
    picked = ["1730 N Vista Ave, Rialto, CA 92376", "1648 N Althea Ave, Rialto, CA 92376"]
    r = post(offer=450_000, comps=picked)
    exp = arv_calculation(1_800, [c for c in listing["analysis"]["comps"] if c["address"] in picked])["arv"]
    check("checked comps: Vista is the anchor (O), Althea next; links from the saved search",
          r.status_code == 200 and calls[-1][5] == [("1730 N Vista Ave, Rialto, CA 92376", "https://z/1"),
                                                    ("1648 N Althea Ave, Rialto, CA 92376", "https://z/2")], calls[-1][5])
    check("ARV recomputed on the server from the checked comps", calls[-1][2] == exp, (calls[-1][2], exp))
    post(offer=450_000, comps=picked, arv=700_000)
    check("…but a typed ARV still wins", calls[-1][2] == 700_000, calls[-1][2])
    four = [c["address"] for c in listing["analysis"]["comps"]] + ["x"]
    check("more than 3 comps → 400 'Max 3 comps'", post(offer=1, comps=four).get_json()["error"] == "Max 3 comps")
    check("a comp that isn't this property's → 400", post(offer=1, comps=["9 Other St"]).status_code == 400)
    check("the same comp twice → 400", post(offer=1, comps=[picked[0], picked[0]]).status_code == 400)
    check("a fee so high no offer works → 400 that says so",
          post(offer=1, fee=900_000).status_code == 400 and "wholesale fee" in post(offer=1, fee=900_000).get_json()["error"])
    check("nonsense fee → 400", post(offer=480_000, fee="abc").status_code == 400 and post(offer=480_000, fee=-1).status_code == 400)
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
    check("page: comp checkboxes, anchor, 'Max 3 comps.', and the pick goes to the server",
          all(t in page for t in ("togglePick(", "Make anchor", "Max 3 comps.", "comps: v.pickEdited ? pickOf(c) : null")))
    check("page: the wholesale fee is an editable number and goes to the server",
          "editable('fee', 'Wholesale fee'" in page and "fee: v.feeEdited ? v.fee : null" in page)
    check("page loads the JS calculator", '<script src="/static/flip_calculator.js">' in page
          and client.get("/static/flip_calculator.js").status_code == 200)


def test_js_arv_matches_python():
    print("\n=== JavaScript ARV rule = Python ARV rule ===")
    from flip_analysis import arv_calculation
    rnd = random.Random(11)
    sets = []
    for _ in range(1500):
        comps = [{"address": f"c{k}", "sold_price": rnd.randrange(150_000, 1_500_000, 500), "sqft": rnd.randrange(600, 3_500),
                  "listed_ppsf": rnd.choice([None, rnd.randrange(150, 900)])} for k in range(rnd.randint(1, 5))]
        sets.append((rnd.choice([rnd.randrange(600, 3_600), 1_500.5]), comps))
    sets.append((1_800, [{"address": "L", "sold_price": 635_000, "sqft": 1_566, "listed_ppsf": 405},
                         {"address": "V", "sold_price": 648_000, "sqft": 1_748, "listed_ppsf": 371},
                         {"address": "A", "sold_price": 645_000, "sqft": 1_630, "listed_ppsf": 396}]))
    js = subprocess.run(["node", "-e", f"""
        const f = require({json.dumps(os.path.join(HERE, 'dashboard/static/flip_calculator.js'))});
        console.log(JSON.stringify({json.dumps(sets)}.map(([sq, cs]) => {{ const r = f.arvFromComps(sq, cs); return [r.arv, r.explain]; }})));"""],
        capture_output=True, text=True, timeout=60)
    got = json.loads(js.stdout)
    bad = [(sq, py["arv"], g[0]) for (sq, cs), g in zip(sets, got)
           for py in [arv_calculation(sq, cs)] if py["arv"] != g[0] or py["explain"] != g[1]]
    check(f"same ARV and explanation on {len(sets)} random comp sets", not bad, bad[:3])
    check("Fillmore through JS: $648,000", got[-1][0] == 648_000, got[-1])


def test_js_matches_python():
    print("\n=== JavaScript calculator = Python calculator ===")
    rnd = random.Random(7)
    cases = [(665_000, 45_000), (712_270, 40_000), (712_270, 54_000), (100_000, 200_000), (648_000, 60_000)]
    cases += [(rnd.randrange(80_000, 2_500_000), rnd.randrange(0, 300_000)) for _ in range(3000)]
    fee_cases = [(rnd.randrange(80_000, 2_500_000), rnd.randrange(0, 300_000), rnd.choice([0, 5_000, 10_000, 15_000, 25_000, rnd.randrange(0, 60_000)]))
                 for _ in range(2000)] + [(648_000, 60_000, 10_000)]
    js = subprocess.run(["node", "-e", f"""
        const f = require({json.dumps(os.path.join(HERE, 'dashboard/static/flip_calculator.js'))});
        const out = c => c.map(([a, r, fee]) => {{ const m = f.maxAllowableOffer(a, r, fee);
            return [m.mao, m.profit == null ? null : Math.round(m.profit * 100) / 100]; }});
        console.log(JSON.stringify([out({json.dumps(cases)}.map(x => [x[0], x[1]])), out({json.dumps(fee_cases)})]));"""],
        capture_output=True, text=True, timeout=60)
    got, got_fee = json.loads(js.stdout)
    bad = []
    for (arv, rehab), (jm, jp) in zip(cases, got):
        pm, at = max_allowable_offer(arv, rehab)
        pp = round(at["profit"], 2) if at else None
        if pm != jm or (pp is not None and abs(pp - jp) > 0.01) or (pp is None) != (jp is None):
            bad.append((arv, rehab, pm, jm, pp, jp))
    check(f"same MAO and profit on {len(cases)} cases", not bad, bad[:3])
    check("owner's test through JS, now with the $15,000 fee: MAO $484,100 (= Python)",
          got[0][0] == 484_100 == max_allowable_offer(665_000, 45_000)[0], got[0])
    check("row 183 case through JS: ARV $648,000, rehab $60,000 → $455,600", got[4][0] == 455_600, got[4])
    check("1485 N Fillmore, old ARV / rehab $40,000: JS = Python", got[1][0] == max_allowable_offer(712_270, 40_000)[0], got[1])
    bad_fee = [(a, r, fee, max_allowable_offer(a, r, wholesale_fee=fee)[0], jm)
               for (a, r, fee), (jm, _) in zip(fee_cases, got_fee) if max_allowable_offer(a, r, wholesale_fee=fee)[0] != jm]
    check(f"per-card fee: same max offer on {len(fee_cases)} cases", not bad_fee, bad_fee[:3])
    check("ARV $648,000 / rehab $60,000 / fee $10,000: JS = Python",
          got_fee[-1][0] == max_allowable_offer(648_000, 60_000, wholesale_fee=10_000)[0], got_fee[-1])


if __name__ == "__main__":
    test_address_identity()
    test_add_row()
    test_endpoint()
    test_js_matches_python()
    test_js_arv_matches_python()
    print(f"\n=== {'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)} ===")
    sys.stdout.flush()
    os._exit(1 if FAILS else 0)
