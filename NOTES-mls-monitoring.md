# MLS Monitoring Branch — Status Notes

**Branch:** `mls-monitoring`
**Last updated:** 2026-05-01
**Status:** PAUSED — not merged, not deployed

## What's built

- **5 new DB columns** on `preforeclosures`: `listed_at`, `previous_mls_status`, `zillow_url`, `scan_error_count`, `last_scan_error`
- **Migration script:** `migrate_mls_monitoring.py` (safe to re-run)
- **Refactored worker scan** (`worker.py`): retry + backoff, per-property error tracking, kill switch (`MLS_CHECKER_ENABLED`), configurable batch size + delay, every-3-day schedule
- **6 new API endpoints** (`app.py`): set zillow URL, dismiss new, dismiss all, new count, manual trigger
- **Dashboard UI** (`dashboard/index.html`): New Listings badge tab, New filter, Dismiss buttons, Run Check Now, Zillow URL per row
- **Local test passed:** 7 properties scanned, 0 errors, correct results

## What's missing

### Hybrid detail-scraper fallback (the reason we paused)
The zip-search actor (`maxcopell~zillow-zip-search`) returns a **truncated subset** of listings per zip code. High-density zips (100+ listings) systematically miss properties. Confirmed: 9120 S Van Ness Ave in 90047 is actively listed on Zillow but doesn't appear in the actor's 111 results.

**Planned fix:** After the zip-search pass, run a second pass using `maxcopell~zillow-detail-scraper` ($0.003/call) for properties that came back "not found." This catches misses reliably.

### Error handling refinement
`scan_error_count` should only increment on **lookup failures** (timeouts, 429s, malformed addresses), NOT on "confirmed not listed" (both scrapers ran clean, no match). This was designed but not coded.

## Cost concern (why we paused)

| | Zip-only | Hybrid (zip + detail fallback) |
|---|---|---|
| Calls/run | ~80 | ~640 |
| Cost/run | $23 | $25 |
| Cost/month (every 3 days) | $232 | $249 |

The $249/month total was too high to commit to without more validation.

## Ideas for reducing cost

### Tiered scanning by auction date
Instead of scanning all 800 properties every 3 days:
- **Tier 1 — auction within 30 days:** scan every 3 days (most likely to list soon)
- **Tier 2 — auction within 90 days:** scan weekly
- **Tier 3 — auction 90+ days out or no date:** scan monthly

This could cut the effective property count per run from 800 to ~200, reducing cost by ~75%.

### Other ideas
- Use a cheaper/different actor
- Only scan properties that have never been found (skip confirmed-not-listed after N clean scans)
- Rate-limit the detail fallback to top N highest-value properties

## Key files

| File | What changed |
|---|---|
| `database.py` | 5 new columns + updated `preforeclosure_to_dict` |
| `worker.py` | Refactored scan, extracted `_scan_single_property`, `_send_new_listing_alert` |
| `app.py` | 6 new endpoints, updated PUT + single scan |
| `dashboard/index.html` | New Listings badge, filters, dismiss, Run Check Now, Zillow URL |
| `migrate_mls_monitoring.py` | Schema migration (new file) |

## Env vars (add to Railway when deploying)

| Var | Default | Purpose |
|---|---|---|
| `MLS_CHECKER_ENABLED` | `true` | Kill switch |
| `MLS_BATCH_SIZE` | `0` (all) | Max properties per run |
| `MLS_DELAY_SECONDS` | `3` | Delay between Apify calls |
| `MLS_DETAIL_FALLBACK_ENABLED` | `true` | Enable detail-scraper second pass (not yet implemented) |

## Safe to deploy main

The `main` branch has the scan **commented out** (`# schedule.every().day.at("09:00").do(run_preforeclosure_scan)`). Deploying main will NOT trigger any pre-foreclosure scanning.
