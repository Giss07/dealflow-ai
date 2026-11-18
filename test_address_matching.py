"""
Address matching for the counter-offer scanner (find_matching_address and the
HUD fallback), after the Distressed Deals page started adding non-HUD rows.

The old matcher took the first row whose street appeared anywhere in the email,
so "1206 S Palm St, Hemet" could update an "1206 S Palm St, Anaheim" row, and
"123 Main St" matched inside "1123 Main St". No network.

    python test_address_matching.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dealflow_updater as du

FAILS = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{(' — ' + str(detail)) if detail else ''}")
    if not cond:
        FAILS.append(label)


def hud(addr):
    return (f"Fwd: P260 - HUD - Bid Counter Offer Notice - 048-425809\nAddress: {addr}\n"
            f"The minimum acceptable net to HUD offer amount for this property as 389,000.00")


ROWS = [
    "1206 s palm st, anaheim, ca 92805",          # added from Distressed Deals
    "1206 s palm st, hemet, ca 92543",            # the HUD bid
    "1123 main st, riverside, ca 92503",
    "27335 potomac dr sun city, ca, 92586",       # odd format, as in the real sheet
    "6338 n maroa ave apt 110, fresno, ca 93704",
]


def test_same_street_other_city():
    print("\n=== Same street, different city ===")
    m = du.find_matching_address(hud("1206 S Palm St, Hemet, CA 92543"), ROWS)
    check("HUD email for Hemet → the Hemet row, not the Anaheim one", m == ROWS[1], m)
    m = du.find_matching_address(hud("1206 S Palm St, Anaheim, CA 92805"), list(reversed(ROWS)))
    check("…and the Anaheim email → Anaheim, whatever the row order", m == ROWS[0], m)
    m = du.find_matching_address("Counter on 1206 S Palm St — call me", ROWS)
    check("no city or ZIP in the email → no guess (None)", m is None, m)


def test_street_number_boundaries():
    print("\n=== Street numbers match whole ===")
    rows = ["123 main st, riverside, ca 92503"]
    check("'123 Main St' does not match inside '1123 Main St'",
          du.find_matching_address(hud("1123 Main St, Riverside, CA 92503"), rows) is None)
    check("'1123 Main St' still matches its own row",
          du.find_matching_address(hud("1123 Main St, Riverside, CA 92503"), ROWS) == ROWS[2])


def test_existing_formats_still_match():
    print("\n=== Existing sheet formats ===")
    check("'Potomac Dr Sun City, CA, 92586' row ← HUD email with a normal address",
          du.match_hud_street("27335 potomac dr, sun city, ca 92586", ROWS, hud("27335 Potomac Dr, Sun City, CA 92586"))
          == ROWS[3])
    check("unit row ← email with the unit written differently",
          du.find_matching_address(hud("6338 N Maroa Ave #110, Fresno, CA 93704"), ROWS) == ROWS[4],
          du.find_matching_address(hud("6338 N Maroa Ave #110, Fresno, CA 93704"), ROWS))
    dup = ["1517 s klein avenue, reedley, ca 93654", "1517 S Klein Ave, Reedley, CA 93654".lower()]
    check("two rows for the same property (same street + ZIP) → the first, not 'ambiguous'",
          du.find_matching_address(hud("1517 S Klein Ave, Reedley, CA 93654"), dup) == dup[0])


def test_hud_fallback():
    print("\n=== HUD fallback (street from the subject line) ===")
    m = du.match_hud_street("1206 s palm st", ROWS, hud("1206 S Palm St, Hemet, CA 92543"))
    check("fallback also picks the row in the email's city", m == ROWS[1], m)
    check("fallback: '123 main' does not hit '1123 main st'",
          du.match_hud_street("123 main st", ["1123 main st, riverside, ca 92503"], "123 Main St") is None)


if __name__ == "__main__":
    test_same_street_other_city()
    test_street_number_boundaries()
    test_existing_formats_still_match()
    test_hud_fallback()
    print(f"\n=== {'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)} ===")
    sys.exit(1 if FAILS else 0)
