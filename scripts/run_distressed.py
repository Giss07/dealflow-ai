#!/usr/bin/env python3
"""
run_distressed.py — CLI runner for the MCP search_distressed tool.

Calls mcp_server.search_distressed() directly (no MCP transport) for one or
more ZIP codes, with retry-on-500 since the upstream OpenWeb Ninja Zillow API
intermittently returns HTTP 500 on specific ZIPs. Prints kept (keyword-matched)
listings with their distress signals, and a per-ZIP summary.

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


def run(zips):
    summary = {}
    for z in zips:
        print("=" * 64)
        print(f"ZIP {z}")
        final = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            # Bust the 1h zip-search cache so each attempt is a fresh vendor call.
            mcp_server._owin_cache.pop(f"{z}:FOR_SALE", None)
            data = json.loads(mcp_server.search_distressed(z))
            if data.get("status") == "ok":
                final = data
                print(f"  attempt {attempt}: OK  count={data.get('count')}")
                break
            print(f"  attempt {attempt}: {data.get('error', '?')}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF[attempt - 1])
        if final is None:
            summary[z] = "FAILED (all attempts)"
            continue
        summary[z] = f"ok, {final.get('count', 0)} distressed"
        for lst in final.get("listings", []):
            price = lst.get("price") or 0
            print(f"    - {lst.get('address')} | ${price:,} | "
                  f"{lst.get('beds')}bd/{lst.get('baths')}ba | "
                  f"{lst.get('sqft')}sqft | {lst.get('days')} DOM")
            for s in lst.get("distress_signals", []):
                print(f"        - {s}")

    print("=" * 64)
    print("SUMMARY")
    for z in zips:
        print(f"  {z}: {summary.get(z)}")


if __name__ == "__main__":
    run(sys.argv[1:] or DEFAULT_ZIPS)
