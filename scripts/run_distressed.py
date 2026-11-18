#!/usr/bin/env python3
"""
run_distressed.py — CLI runner for the MCP search_distressed tool.

Calls mcp_server.search_distressed() directly (no MCP transport) for one or
more ZIP codes, with retry-on-500 since the upstream OpenWeb Ninja Zillow API
intermittently returns HTTP 500 on specific ZIPs. Prints one table per ZIP of
the keyword-matched listings with comps, ARV, rehab and MAO (see flip_analysis),
and writes two CSVs to exports/: one row per listing, and one row per comp.
Read-only: nothing is submitted.

Usage:
    python3 scripts/run_distressed.py 93701 93702 93706
    python3 scripts/run_distressed.py            # defaults to the ZIPs below

Notes:
  - Each attempt busts the 1h zip-search cache so retries hit the API fresh.
  - Requires OPENWEB_NINJA_API_KEY in .env (each ZIP costs ~$0.08-0.10).
  - Stubs the `mcp` framework so the module imports without FastMCP installed
    locally; the tool logic itself only needs requests + dotenv.
"""
import os
import sys
import json
import time
import types

# ── Stub the `mcp` framework so mcp_server imports without FastMCP installed ──
mcp_pkg = types.ModuleType("mcp")
server_pkg = types.ModuleType("mcp.server")
fastmcp_pkg = types.ModuleType("mcp.server.fastmcp")


class _FakeFastMCP:
    def __init__(self, *a, **k):
        pass

    def tool(self, *a, **k):
        return lambda fn: fn  # identity decorator — keep the plain callable


fastmcp_pkg.FastMCP = _FakeFastMCP
server_pkg.fastmcp = fastmcp_pkg
mcp_pkg.server = server_pkg
sys.modules.update({
    "mcp": mcp_pkg,
    "mcp.server": server_pkg,
    "mcp.server.fastmcp": fastmcp_pkg,
})

# Import mcp_server from the repo root (parent of this scripts/ dir).
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)
import mcp_server  # noqa: E402

DEFAULT_ZIPS = ["93701", "93702", "93706"]
MAX_ATTEMPTS = 4
BACKOFF = [5, 8, 12]  # seconds between attempts


from distressed_report import (  # noqa: E402  (shared with the web app)
    LISTING_COLUMNS, COMP_COLUMNS, money as _money, rows_for as _rows,
)


def _print_zip_table(zip_code, rows, comps_by_subject):
    print(f"\nZIP {zip_code} — {len(rows)} distressed listing(s)")
    hdr = f"  {'Address':34} {'List':>10} {'ARV':>10} {'Comps':>14} {'Rehab':>14} {'MAO':>10} {'MAO vs list':>16} {'Profit@MAO':>11} {'DOM':>4}"
    print(hdr); print("  " + "-" * (len(hdr) - 2))
    for r in rows:
        comps = (f"{r['comps_used']} ({r['remodeled_comps']}R/{r['unknown_comps']}U)"
                 f"{' LOW' if r['confidence'] == 'LOW CONFIDENCE' else ''}")
        rehab = f"{(r['rehab_tier'] or '-')[:1].upper()} {_money(r['rehab'])}"
        vs = (f"{_money(r['mao_vs_list'])} ({r['mao_vs_list_pct']})" if r["mao_vs_list"] is not None else "—")
        print(f"  {str(r['address'])[:34]:34} {_money(r['list_price']):>10} {_money(r['arv']):>10} {comps:>14} "
              f"{rehab:>14} {_money(r['mao']):>10} {vs:>16} {_money(r['profit_at_mao']):>11} {str(r['days_on_market'] or ''):>4}")
        flags = [f for f in (r["confidence"] if r["confidence"] == "LOW CONFIDENCE" else "", r["comp_flags"], r["note"]) if f]
        print(f"      pool {r['pool']} · 55+ {r['55_plus']} · rehab: {r['rehab_reason']}"
              + (f" · ⚠ {' · '.join(flags)}" if flags else ""))
        if r["arv_basis"]:
            print(f"      ARV basis: {r['arv_basis']}")
        print(f"      {r['zillow_url']}")
        for c in comps_by_subject.get(r["address"], []):
            print(f"        comp {c['comp_address'][:38]:38} {_money(c['sold_price']):>10} {c['sold_date']} "
                  f"{int(c['sqft'] or 0):>5}sf {_money(c['price_per_sqft']):>6}/sf {c['distance_mi']:.2f}mi "
                  f"pool {c['pool']} 55+ {c['55_plus']} {c['remodeled']:9} ({c['remodeled_evidence'][:40]})"
                  f"{'' if c['matches_subject'] == 'yes' else ' ' + c['matches_subject']}")


def _write_csvs(zips, all_rows, all_comps):
    import csv
    from datetime import date
    out_dir = os.path.join(REPO_ROOT, "exports")
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.join(out_dir, f"distressed_{'-'.join(zips)}_{date.today().isoformat()}")
    for path, cols, rows in ((stem + ".csv", LISTING_COLUMNS, all_rows),
                             (stem + "_comps.csv", COMP_COLUMNS, all_comps)):
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
    return stem + ".csv", stem + "_comps.csv"


def run(zips):
    summary, all_rows, all_comps = {}, [], []
    for z in zips:
        final = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            # Bust the 1h zip-search cache so each attempt is a fresh vendor call.
            mcp_server._owin_cache.pop(f"{z}:FOR_SALE", None)
            data = json.loads(mcp_server.search_distressed(z))
            if data.get("status") == "ok":
                final = data
                break
            print(f"  ZIP {z} attempt {attempt}: {data.get('error', '?')}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF[attempt - 1])
        if final is None:
            summary[z] = "FAILED (all attempts)"
            continue
        rows, comps_by_subject = [], {}
        for lst in final.get("listings", []):
            row, comps = _rows(z, lst)
            rows.append(row)
            comps_by_subject[row["address"]] = comps
            all_comps.extend(comps)
        # Best MAO-vs-list first; rows without an MAO last.
        rows.sort(key=lambda r: (r["mao_vs_list"] is None,
                                 -(r["mao_vs_list"] if r["mao_vs_list"] is not None else 0)))
        all_rows.extend(rows)
        _print_zip_table(z, rows, comps_by_subject)
        summary[z] = f"ok, {final.get('count', 0)} distressed"

    print("\n" + "=" * 64)
    print("SUMMARY")
    for z in zips:
        print(f"  {z}: {summary.get(z)}")
    if all_rows:
        listing_csv, comps_csv = _write_csvs(zips, all_rows, all_comps)
        print(f"\nCSV: {listing_csv}\n     {comps_csv}")


if __name__ == "__main__":
    run(sys.argv[1:] or DEFAULT_ZIPS)
