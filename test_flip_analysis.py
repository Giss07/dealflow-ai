"""
Tests for the distressed-search flip analysis: calculator, comps, ARV, rehab, MAO.

The calculator tests are the owner's own numbers (ARV $665,000, repairs
$45,000, purchase $490,000). Property tax is 1.2% of purchase per year (Prop 13
reset); the original $904-tax version is kept to show the calculator matches
the spreadsheet line for line when a fixed tax is given.

No network, no API key.

    python test_flip_analysis.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flip_calculator import flip_costs, max_allowable_offer
import flip_analysis as fa

FAILS = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{(' — ' + str(detail)) if detail else ''}")
    if not cond:
        FAILS.append(label)


def test_calculator():
    print("\n=== Calculator: the owner's tests ===")
    ARV, REPAIRS, PURCHASE = 665_000, 45_000, 490_000

    b = flip_costs(PURCHASE, ARV, REPAIRS)            # property tax 1.2% of purchase
    print("  -- property tax 1.2% of purchase (the rule the search uses)")
    check("acquisition $494,900", round(b["acquisition"]) == 494_900, round(b["acquisition"]))
    check("holding $18,147", round(b["holding"]) == 18_147, round(b["holding"]))
    check("  of which property tax $1,470 (490,000 × 1.2% × 3/12)", round(b["taxes"]) == 1_470, round(b["taxes"]))
    check("selling $31,600", round(b["selling"]) == 31_600, round(b["selling"]))
    check("total costs $589,647", round(b["total_costs"]) == 589_647, round(b["total_costs"]))
    check("profit $75,353", round(b["profit"]) == 75_353, round(b["profit"]))
    mao, at = max_allowable_offer(ARV, REPAIRS)
    check("MAO at 10% of ARV = $498,400", mao == 498_400, mao)
    check("profit at MAO $66,505 (≥ $66,500)", round(at["profit"]) == 66_505 and at["profit"] >= 66_500,
          round(at["profit"]))
    check("$100 more would miss the 10% target",
          flip_costs(mao + 100, ARV, REPAIRS)["profit"] < 66_500)

    o = flip_costs(PURCHASE, ARV, REPAIRS, annual_property_tax=904)
    print("  -- original spreadsheet test, fixed annual tax $904")
    check("acquisition $494,900", round(o["acquisition"]) == 494_900, round(o["acquisition"]))
    check("holding $16,903", round(o["holding"]) == 16_903, round(o["holding"]))
    check("selling $31,600", round(o["selling"]) == 31_600, round(o["selling"]))
    check("total costs $588,403", round(o["total_costs"]) == 588_403, round(o["total_costs"]))
    check("profit $76,597", round(o["profit"]) == 76_597, round(o["profit"]))
    mao904, at904 = max_allowable_offer(ARV, REPAIRS, annual_property_tax=904)
    check("MAO about $499,600 (profit ≥ $66,500)", mao904 == 499_600 and at904["profit"] >= 66_500,
          f"{mao904}, profit {round(at904['profit'])}")

    check("no MAO when even $0 cannot reach the target",
          max_allowable_offer(100_000, 200_000) == (None, None))


def test_rehab():
    print("\n=== Rehab tiers ===")
    r = fa.estimate_rehab("Fixer! Needs work throughout.", 1985, 1_500)
    check("fixer → medium $55/sqft", r["tier"] == "medium" and r["amount"] == 82_500, r)
    check("bring your contractor → heavy",
          fa.estimate_rehab("as-is, bring your contractor", 1990, 1_000)["tier"] == "heavy")
    check("sale terms only (motivated, probate) → light",
          fa.estimate_rehab("Motivated seller, probate sale", 1995, 1_000)["tier"] == "light")
    check("pre-1960 moves light → medium", fa.estimate_rehab("TLC", 1954, 1_000)["tier"] == "medium")
    check("pre-1960 moves medium → heavy", fa.estimate_rehab("fixer", 1950, 1_000)["tier"] == "heavy")
    check("heavy stays heavy", fa.estimate_rehab("tear-down", 1940, 1_000)["tier"] == "heavy")
    check("1960 itself is not pre-1960", fa.estimate_rehab("TLC", 1960, 1_000)["tier"] == "light")
    check("'gutters' does not read as 'gut'",
          fa.estimate_rehab("new gutters, TLC", 1990, 1_000)["tier"] == "light")
    check("no sqft → no amount", fa.estimate_rehab("fixer", 1990, None)["amount"] is None)


def test_detection():
    print("\n=== Pool / 55+ detection ===")
    check("hasPrivatePool True → pool (data)",
          fa.detect_pool({"resoFacts": {"hasPrivatePool": True}}) == (True, "data"))
    check("poolFeatures ['In Ground'] → pool (data)",
          fa.detect_pool({"resoFacts": {"poolFeatures": ["In Ground"]}}) == (True, "data"))
    check("poolFeatures ['None'] → no pool (data), description ignored",
          fa.detect_pool({"resoFacts": {"poolFeatures": ["None"]}, "description": "sparkling pool"}) == (False, "data"))
    check("community pool only → not a private pool",
          fa.detect_pool({"resoFacts": {"poolFeatures": ["Community"]}}) == (False, "data"))
    check("no data fields, description 'swimming pool' → pool",
          fa.detect_pool({"description": "Backyard with swimming pool"}) == (True, "description"))
    check("no data fields, 'community pool' → no pool",
          fa.detect_pool({"description": "HOA with community pool"})[0] is False)
    check("isSeniorCommunity True → 55+ (data)",
          fa.detect_senior({"resoFacts": {"isSeniorCommunity": True}}) == (True, "data"))
    check("communityFeatures '55 and Over' → 55+",
          fa.detect_senior({"resoFacts": {"communityFeatures": ["55 and Over"]}})[0] is True)
    for text in ("Welcome to this 55+ community", "senior community", "Age-restricted park", "55 and older"):
        check(f"description {text!r} → 55+", fa.detect_senior({"description": text})[0] is True)
    check("no signal → not 55+", fa.detect_senior({"description": "family home near schools"})[0] is False)


NOW = time.time() * 1000
DAY = 86400 * 1000
SUBJECT = {"zpid": "1", "address": "1 Subject St", "list_price": 500_000, "sqft": 1_500, "beds": 3,
           "home_type": "SINGLE_FAMILY", "lat": 34.0, "lon": -117.0, "year_built": 1985,
           "description": "fixer", "pool": False, "senior": False}
DEG_PER_MILE = 1 / 69.0


def sold(zpid, price, sqft=1_500, beds=3, miles=0.1, days_ago=30, home_type="SINGLE_FAMILY"):
    return {"zpid": zpid, "soldPrice": price, "livingArea": sqft, "bedrooms": beds, "homeType": home_type,
            "latitude": 34.0 + miles * DEG_PER_MILE, "longitude": -117.0, "dateSold": NOW - days_ago * DAY,
            "streetAddress": f"{zpid} Comp St", "city": "Town", "zipcode": "90000",
            "detailUrl": f"/homedetails/{zpid}_zpid/"}


def no_pool_no_senior(_c):
    return {"resoFacts": {"hasPrivatePool": False, "isSeniorCommunity": False}}


def test_comp_filters():
    print("\n=== Comp filters ===")
    items = [
        sold("ok", 600_000),
        sold("old", 600_000, days_ago=200),                 # > 6 months
        sold("far", 600_000, miles=0.6),                    # > 0.5 mi
        sold("big", 600_000, sqft=1_900),                   # > +20%
        sold("small", 600_000, sqft=1_150),                 # < -20%
        sold("beds", 600_000, beds=5),                      # > ±1 bed
        sold("condo", 600_000, home_type="CONDO"),          # other type
        sold("edge", 600_000, sqft=1_800, beds=4, miles=0.49, days_ago=180),  # all at the limit
        sold("1", 600_000),                                 # the subject itself
    ]
    got = [c["zpid"] for c in fa.comp_candidates(SUBJECT, items, NOW)]
    check("only in-window, in-radius, similar, same-type sales survive", got == ["ok", "edge"], got)


def test_comps_arv_mao():
    print("\n=== Comps, ARV and MAO ===")
    items = [sold(f"c{i}", p, miles=0.05 * (i + 1)) for i, p in enumerate([600_000, 630_000, 660_000, 690_000, 720_000, 750_000])]
    a = fa.analyze_listing(SUBJECT, items, no_pool_no_senior, now_ms=NOW)
    check("5 comps used, nearest first", [c["zpid"] for c in a["comps"]] == ["c0", "c1", "c2", "c3", "c4"],
          [c["zpid"] for c in a["comps"]])
    check("ARV = median $/sqft × sqft = $660,000", a["arv"] == 660_000, a["arv"])
    check("confidence OK with 5 comps", a["confidence"] == "OK" and a["comp_flags"] == [])
    check("rehab medium (fixer) $82,500", a["rehab"]["amount"] == 82_500, a["rehab"])
    exp_mao, exp_at = max_allowable_offer(660_000, 82_500)
    check("MAO from the calculator", a["mao"] == exp_mao and a["profit_at_mao"] == round(exp_at["profit"]),
          (a["mao"], a["profit_at_mao"]))
    check("MAO vs list = MAO − $500,000", a["mao_vs_list"] == exp_mao - 500_000, a["mao_vs_list"])

    two = fa.analyze_listing(SUBJECT, items[:2], no_pool_no_senior, now_ms=NOW)
    check("2 comps → LOW CONFIDENCE, ARV still given", two["confidence"] == "LOW CONFIDENCE" and two["arv"], two["arv"])
    none = fa.analyze_listing(SUBJECT, [], no_pool_no_senior, now_ms=NOW)
    check("0 comps → no ARV, no MAO, reason given", none["arv"] is None and none["mao"] is None and none["note"],
          none["note"])

    asked = []
    fa.analyze_listing(SUBJECT, items[:1], no_pool_no_senior, more_sold_items=lambda: asked.append(1) or items[1:4],
                       now_ms=NOW)
    check("fewer than 3 in the ZIP → neighbouring ZIPs searched", asked == [1])

    nz = {"nearbyZipcodes": [{"name": "92701", "regionUrl": {"path": "/santa-ana-ca-92701/"}},
                             {"name": "92703"}, {"zipcode": "92704"}, "92706", {"name": "Santa Ana"}]}
    check("neighbouring ZIPs read from the API's {'name': ...} entries, own ZIP dropped",
          fa.neighbor_zip_codes(nz, "92703") == ["92701", "92704", "92706"], fa.neighbor_zip_codes(nz, "92703"))


def test_pool_and_senior_matching():
    print("\n=== Pool / 55+ matching and MISMATCH flags ===")
    items = [sold(f"c{i}", 600_000 + 10_000 * i, miles=0.05 * (i + 1)) for i in range(6)]
    pools = {"c0": True, "c1": False, "c2": True, "c3": False, "c4": False, "c5": False}
    seniors = {z: False for z in pools}

    def detail(c):
        return {"resoFacts": {"hasPrivatePool": pools[c["zpid"]], "isSeniorCommunity": seniors[c["zpid"]]}}

    no_pool_subject = dict(SUBJECT)
    a = fa.analyze_listing(no_pool_subject, items, detail, now_ms=NOW)
    check("no-pool subject uses only no-pool comps", [c["zpid"] for c in a["comps"]] == ["c1", "c3", "c4", "c5"]
          and a["comp_flags"] == [], ([c["zpid"] for c in a["comps"]], a["comp_flags"]))

    pool_subject = dict(SUBJECT, pool=True)
    b = fa.analyze_listing(pool_subject, items, detail, now_ms=NOW)
    check("pool subject with 2 pool comps fills to 3 and flags POOL MISMATCH",
          [c["zpid"] for c in b["comps"]] == ["c0", "c2", "c1"] and b["comp_flags"] == ["POOL MISMATCH"],
          ([c["zpid"] for c in b["comps"]], b["comp_flags"]))
    check("matching comps first, the fill-in comp last and marked",
          [c["pool_match"] for c in b["comps"]] == [True, True, False])

    seniors.update({"c0": True, "c1": True})
    senior_subject = dict(SUBJECT, senior=True, pool=False)
    c = fa.analyze_listing(senior_subject, items, detail, now_ms=NOW)
    check("55+ subject: 1 matching 55+ comp, fills to 3, flags 55+ MISMATCH",
          c["comps_used"] == 3 and "55+ MISMATCH" in c["comp_flags"], (c["comps_used"], c["comp_flags"]))
    d = fa.analyze_listing(dict(SUBJECT), items, detail, now_ms=NOW)
    check("non-55+ subject never uses 55+ comps when enough others exist",
          not any(x["senior"] for x in d["comps"]), [x["zpid"] for x in d["comps"]])


def test_remodeled():
    print("\n=== REMODELED / NOT / UNKNOWN and comp preference ===")
    R, N, U = fa.REMODELED, fa.NOT_REMODELED, fa.UNKNOWN
    for text in ("Fully remodeled 3 bed", "Beautifully renovated", "Updated throughout",
                 "Brand new kitchen and baths", "Turnkey home"):
        check(f"{text!r} → REMODELED", fa.detect_remodeled({"description": text})[0] == R)
    for text in ("Needs updating", "Original condition, great bones", "Fixer! Remodeled in 1998",
                 "Ready for your remodel", "Needs TLC"):
        check(f"{text!r} → NOT", fa.detect_remodeled({"description": text})[0] == N)
    for text in ("Lovely home near schools", ""):
        check(f"{text!r} → UNKNOWN", fa.detect_remodeled({"description": text})[0] == U)
    check("condition field 'Updated/Remodeled' → REMODELED (data)",
          fa.detect_remodeled({"resoFacts": {"propertyCondition": "Updated/Remodeled"}})[:2] == (R, "data"))
    check("condition field 'Fixer' → NOT even if the description says renovated",
          fa.detect_remodeled({"resoFacts": {"propertyCondition": "Fixer"},
                               "description": "renovated"})[:2] == (N, "data"))
    check("photo captions count",
          fa.detect_remodeled({"responsivePhotos": [{"caption": "New kitchen with quartz"}]})[0] == R)

    # Eight candidates, nearest first: c0 fixer, c1-c2 unknown, c3 remodeled,
    # c4 unknown, c5 fixer, c6-c7 remodeled (beyond the nearest five).
    state = {"c0": "Fixer", "c1": "", "c2": "", "c3": "Remodeled", "c4": "",
             "c5": "needs updating", "c6": "Renovated", "c7": "Updated"}
    items = [sold(z, 600_000 + 20_000 * i, miles=0.05 * (i + 1)) for i, z in enumerate(state)]

    def detail(c):
        return {"resoFacts": {"hasPrivatePool": False, "isSeniorCommunity": False},
                "description": state[c["zpid"]]}

    a = fa.analyze_listing(SUBJECT, items, detail, now_ms=NOW)
    got = [(c["zpid"], c["remodeled"]) for c in a["comps"]]
    check("REMODELED first (incl. beyond the nearest 5), then UNKNOWN, nearest first; NOT skipped",
          got == [("c3", R), ("c6", R), ("c7", R), ("c1", U), ("c2", U)], got)
    check("fixers excluded and reported", [f["address"].split(" ")[0] for f in a["skipped_fixers"]] == ["c0", "c5"],
          a["skipped_fixers"])
    # $/sqft: c3 440, c6 480, c7 493.3, c1 413.3, c2 426.7 → median 440 × 1,500
    check("ARV = median of those 5 = $660,000", a["arv"] == 660_000 and a["confidence"] == "OK", a["arv"])
    check("basis note counts remodeled / unknown / exclusions",
          a["arv_basis"] == "5 comps: 3 remodeled, 2 unknown; 2 excluded (2 fixer)", a["arv_basis"])

    only_two = {"c0": "Fixer", "c1": "Remodeled", "c2": "TLC", "c3": ""}
    items2 = [sold(z, 600_000, miles=0.05 * (i + 1)) for i, z in enumerate(only_two)]
    b = fa.analyze_listing(SUBJECT, items2, lambda c: {"resoFacts": {"hasPrivatePool": False,
                                                                       "isSeniorCommunity": False},
                                                       "description": only_two[c["zpid"]]}, now_ms=NOW)
    check("only 2 non-fixer comps left → LOW CONFIDENCE, ARV from those 2",
          b["comps_used"] == 2 and b["confidence"] == "LOW CONFIDENCE" and b["arv"] == 600_000,
          (b["comps_used"], b["confidence"], b["arv"]))


def test_mobile_homes():
    print("\n=== Mobile / manufactured homes ===")
    for addr, t in (("2930 W Rialto Ave Space 63, Rialto, CA", "SINGLE_FAMILY"),
                    ("45465 25th St E Spc 157, Lancaster, CA", ""), ("350 S Mayfield Ave Lot 11, X, CA", ""),
                    ("1 Park Rd Lot #4, X, CA", ""), ("9 Any St, X, CA", "MANUFACTURED"), ("9 Any St", "mobile")):
        check(f"{addr!r} / {t!r} → mobile home", fa.is_mobile_home(addr, t))
    for addr in ("350 N Park Ave, Rialto, CA", "12 Lotus Ave, X, CA", "88 Spaceport Dr, X, CA", "5 Lot Line Rd, X, CA"):
        check(f"{addr!r} → not a mobile home", not fa.is_mobile_home(addr, "SINGLE_FAMILY"))
    looked_up = []
    sub = dict(SUBJECT, address="2930 W Rialto Ave Space 63, Rialto, CA 92376", home_type="MANUFACTURED")
    a = fa.analyze_listing(sub, [sold("c0", 600_000)], lambda c: looked_up.append(c) or {}, now_ms=NOW)
    check("mobile subject: no ARV, no MAO, note 'Mobile home', no comp lookups",
          a["mobile_home"] and a["arv"] is None and a["mao"] is None and a["note"] == "Mobile home" and looked_up == [])
    park = dict(sold("p1", 400_000), streetAddress="77 Main St Space 9")
    got = [c["zpid"] for c in fa.comp_candidates(SUBJECT, [park, sold("h1", 600_000)], NOW)]
    check("a 'Space 9' sale typed SINGLE_FAMILY is never a comp", got == ["h1"], got)


def test_bad_comp_exclusions():
    print("\n=== Public-record, big-lot and outlier comps are excluded ===")
    check("lot 0.87 acres → 37,897 sqft", fa.lot_sqft(0.87, "Acres") == 37_897, fa.lot_sqft(0.87, "Acres"))
    check("lot '8,400 sqft' read from resoFacts",
          fa.detail_lot_sqft({"resoFacts": {"lotSize": "8,400 sqft"}}) == 8_400)
    t = NOW - 30 * DAY
    pub = {"priceHistory": [{"event": "Sold", "time": t, "source": "Public Record"}]}
    mls = {"priceHistory": [{"event": "Sold", "time": t, "source": "CRMLS"},
                            {"event": "Listed for sale", "time": t - 40 * DAY, "source": "CRMLS"}]}
    listed_then_recorded = {"priceHistory": [{"event": "Sold", "time": t, "source": "Public Record"},
                                             {"event": "Listed for sale", "time": t - 90 * DAY, "source": "CRMLS"}]}
    check("Sold by Public Record, no listing → public record only", fa.sold_by_public_record_only(pub, t))
    check("Sold via CRMLS → MLS sale", not fa.sold_by_public_record_only(mls, t))
    check("recorded by Public Record but listed on the MLS 3 months before → MLS sale",
          not fa.sold_by_public_record_only(listed_then_recorded, t))
    check("no price history → kept", not fa.sold_by_public_record_only({}, t))

    # 350 N Park Ave, Rialto: 792 sqft on ~7,000 sqft. Its only comp was 530 W
    # Rialto Ave — 900 sqft, $1.5M, 0.87 acre, sold by public record.
    park = dict(SUBJECT, sqft=792, beds=2, lot_sqft=7_000, list_price=360_000, description="as is")
    land = dict(sold("land", 1_500_000, sqft=900, beds=3, miles=0.41, days_ago=93), lotAreaValue=0.87, lotAreaUnit="acres")
    houses = [dict(sold(f"h{i}", 300_000 + 15_000 * i, sqft=800, beds=2, miles=0.1 * (i + 1)),
                   lotAreaValue=7_200, lotAreaUnit="sqft") for i in range(3)]

    def detail(c):
        d = {"resoFacts": {"hasPrivatePool": False, "isSeniorCommunity": False}, "description": "Clean"}
        if c["zpid"] == "land":
            d["priceHistory"] = [{"event": "Sold", "time": c["sold_ms"], "source": "Public Record"}]
        return d
    a = fa.analyze_listing(park, [land] + houses, detail, now_ms=NOW)
    ex = {c["zpid"]: c["excluded_reason"] for c in a["excluded_comps"]}
    check("the $1.5M land sale is excluded as public record only",
          ex.get("land", "").startswith("Sold by public record only"), ex)
    check("ARV comes from the 3 houses only, not $1.32M: median $315,000 − size adj $800 = $314,200",
          [c["zpid"] for c in a["comps"]] == ["h0", "h1", "h2"] and a["arv"] == 314_200,
          (a["arv"], [c["zpid"] for c in a["comps"]]))

    big = dict(sold("big", 600_000), lotAreaValue=20_000, lotAreaUnit="sqft")
    a2 = fa.analyze_listing(dict(SUBJECT, lot_sqft=7_000), [big, sold("s1", 600_000), sold("s2", 610_000),
                                                             sold("s3", 620_000)], no_pool_no_senior, now_ms=NOW)
    check("lot 20,000 sqft vs subject 7,000 (over 2x) → excluded with the reason",
          [c["zpid"] for c in a2["excluded_comps"]] == ["big"]
          and a2["excluded_comps"][0]["excluded_reason"].startswith("Lot 20,000 sqft is over 2x"),
          [(c["zpid"], c["excluded_reason"]) for c in a2["excluded_comps"]])
    check("lot rule skipped when the subject's lot is unknown",
          fa.analyze_listing(SUBJECT, [big], no_pool_no_senior, now_ms=NOW)["excluded_comps"] == [])

    # Outlier: $700/sqft against others at ~$400 → excluded, next candidate fills in.
    pool = [sold("o1", 600_000, miles=0.05), sold("o2", 615_000, miles=0.1), sold("hi", 1_050_000, miles=0.15),
            sold("o3", 630_000, miles=0.2), sold("o4", 645_000, miles=0.25), sold("o5", 660_000, miles=0.3)]
    a3 = fa.analyze_listing(SUBJECT, pool, no_pool_no_senior, now_ms=NOW)
    check("$700/sqft vs others' median ~$420 → excluded as outlier",
          [c["zpid"] for c in a3["excluded_comps"]] == ["hi"] and "outlier" in a3["excluded_comps"][0]["excluded_reason"],
          [(c["zpid"], c.get("excluded_reason")) for c in a3["excluded_comps"]])
    check("the next candidate takes its place (still 5 comps)",
          [c["zpid"] for c in a3["comps"]] == ["o1", "o2", "o3", "o4", "o5"], [c["zpid"] for c in a3["comps"]])
    check("1.5x exactly is not an outlier",
          fa.analyze_listing(SUBJECT, [sold("a", 600_000), sold("b", 600_000), sold("c", 900_000)],
                             no_pool_no_senior, now_ms=NOW)["excluded_comps"] == [])
    check("excluded comps never reach the ARV", all(c not in a3["comps"] for c in a3["excluded_comps"]))
    check("basis note counts each kind", "1 excluded (1 outlier)" in a3["arv_basis"], a3["arv_basis"])


def test_arv_formula():
    print("\n=== ARV = median sold price + $100/sqft size adjustment, capped ===")
    comp = lambda price, sqft: {"sold_price": price, "sqft": sqft, "price_per_sqft": price / sqft}
    # 1485 N Fillmore Ave, Rialto (1,800 sqft) and its three comps from the 92376 rerun.
    fillmore = [comp(635_000, 1_566), comp(648_000, 1_748), comp(645_000, 1_630)]
    c = fa.arv_calculation(1_800, fillmore)
    check("1485 N Fillmore Ave: median $645,000 + $17,000 = $662,000 → capped at $648,000",
          (c["median_price"], c["size_adj"], c["raw"], c["cap"], c["arv"]) == (645_000, 17_000, 662_000, "highest", 648_000), c)
    check("explanation reads like the card",
          c["explain"] == "Median $645,000 + size adj $17,000 = $662,000 → capped at highest comp $648,000", c["explain"])
    small = fa.arv_calculation(1_200, fillmore)
    check("smaller subject: $645,000 − $43,000 = $602,000 → floored at lowest comp $635,000",
          (small["size_adj"], small["raw"], small["cap"], small["arv"]) == (-43_000, 602_000, "lowest", 635_000), small["explain"])
    check("negative adjustment shown with a minus", small["explain"].startswith("Median $645,000 − size adj $43,000"))
    mid = fa.arv_calculation(1_650, fillmore)
    check("inside the comp range → no cap: $645,000 + $2,000 = $647,000",
          (mid["arv"], mid["cap"]) == (647_000, None) and "capped" not in mid["explain"], mid["explain"])
    even = fa.arv_calculation(1_500, [comp(500_000, 1_400), comp(520_000, 1_500), comp(560_000, 1_600), comp(600_000, 1_700)])
    check("even count: median of the middle two ($540,000), sqft median 1,550 → −$5,000 = $535,000",
          (even["median_price"], even["median_sqft"], even["arv"]) == (540_000, 1_550, 535_000), even)
    one = fa.arv_calculation(2_000, [comp(400_000, 1_000)])
    check("one comp: the cap pins ARV to that sale", one["arv"] == 400_000 and one["cap"] == "highest")
    check("no comps / no sqft → None", fa.arv_calculation(1_500, []) is None and fa.arv_calculation(None, fillmore) is None)


def test_csv_columns():
    print("\n=== Runner table / CSV rows ===")
    import types
    for name in ("mcp", "mcp.server", "mcp.server.fastmcp"):
        sys.modules.setdefault(name, types.ModuleType(name))
    sys.modules["mcp.server.fastmcp"].FastMCP = type("F", (), {"__init__": lambda s, *a, **k: None,
                                                               "tool": lambda s, *a, **k: (lambda f: f)})
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
    import importlib
    rd = importlib.import_module("run_distressed")
    items = [sold(f"c{i}", 600_000, miles=0.05 * (i + 1)) for i in range(3)]
    analysis = fa.analyze_listing(SUBJECT, items, no_pool_no_senior, now_ms=NOW)
    analysis.update({"zillow_url": "https://www.zillow.com/homedetails/1_zpid/", "pool": False, "senior": False,
                     "year_built": 1985})
    row, comps = rd._rows("90000", {"address": "1 Subject St", "price": 500_000, "days": 40, "analysis": analysis})
    need = ["address", "list_price", "arv", "comps_used", "rehab", "mao", "mao_vs_list", "profit_at_mao",
            "days_on_market", "zillow_url", "pool", "55_plus"]
    check("listing row has every requested column", all(k in row for k in need) and set(row) == set(rd.LISTING_COLUMNS),
          sorted(set(need) - set(row)))
    check("comps rows carry sold price/date, sqft, $/sqft, distance, pool, 55+, remodeled",
          len(comps) == 3 and set(comps[0]) == set(rd.COMP_COLUMNS)
          and comps[0]["remodeled"] in ("REMODELED", "UNKNOWN"),
          comps[0] if comps else None)
    check("listing row carries the ARV basis", "arv_basis" in row and row["arv_basis"], row.get("arv_basis"))


if __name__ == "__main__":
    test_calculator()
    test_rehab()
    test_detection()
    test_comp_filters()
    test_comps_arv_mao()
    test_pool_and_senior_matching()
    test_remodeled()
    test_mobile_homes()
    test_bad_comp_exclusions()
    test_arv_formula()
    test_csv_columns()
    print(f"\n=== {'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)} ===")
    sys.exit(1 if FAILS else 0)
