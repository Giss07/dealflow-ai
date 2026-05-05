# MLS Monitoring Branch — Status Notes

**Branch:** `mls-monitoring`
**Last updated:** 2026-05-04 (evening)
**Status:** ASYNC SYSTEM BUILT + LOCALLY TESTED — ready for production deploy after review

## What's built and tested

### Worker (worker.py)
- `run_preforeclosure_scan(property_ids)` — accepts list of IDs, returns summary with actual_cost
- `estimate_scan_cost(property_ids)` — cost breakdown before scan runs
- `_scan_single_property()` — zip-search pass, returns True/False for match
- `_apply_zillow_match()` — shared status mapping for both zip-search and detail-scraper
- `_detail_fallback_pass()` — second pass using zillow-detail-scraper ($0.003/call)
- `_send_new_listing_alert()` — email on Monitoring -> On Market transition
- `MLS_AUTO_SCAN_ENABLED` (default false) — schedule only registers if true
- `MLS_DETAIL_FALLBACK_ENABLED` (default true) — enables detail-scraper second pass
- `MLS_DELAY_SECONDS` (default 3) — delay between Apify calls
- Error handling: only increments scan_error_count on real failures, not clean not-found
- Cost tracking: actual_cost returned in summary for toast display
- TODO in code: replace 70% not-found assumption with historical rate from scan data

### API (app.py)
- `POST /api/preforeclosure/scan-selected` — scans specific IDs, returns summary
- `POST /api/preforeclosure/scan-cost-estimate` — returns cost breakdown + SHOW_COST_PREVIEW
- `POST /api/preforeclosure/<id>/set-url` — set Zillow URL override
- `POST /api/preforeclosure/<id>/dismiss` — mark new listing as seen
- `POST /api/preforeclosure/dismiss-all` — dismiss all new flags
- `GET /api/preforeclosure/new-count` — badge count
- `POST /api/preforeclosure/scan/<id>` — single-property scan (existing, updated)

### Dashboard (index.html)
- Checkboxes on each row + Select All in header
- "Scan Selected (N)" button — disabled when nothing selected
- Cost confirmation modal before scan (zip + detail cost breakdown)
- Summary toast on completion with actual cost
- "New Listings" badge tab (auto-hides when 0)
- "New" filter button with count
- "Dismiss All New" button
- Per-row Zillow URL button (blue when URL set)
- Per-row "Seen" dismiss button for new listings
- Selected rows highlighted blue

### Schema (database.py + migrate_mls_monitoring.py)
- 5 new columns: listed_at, previous_mls_status, zillow_url, scan_error_count, last_scan_error
- Migration script safe to re-run

## Local test results (2026-05-04)

| Test | Result |
|---|---|
| 1. Checkbox UI (select, select-all, filter clears selection) | PASS |
| 2. Scan Selected button (count updates, disabled when empty) | PASS |
| 3. Cost preview modal (zip + detail breakdown, confirm/cancel) | PASS |
| 4. Scan execution + summary toast (with actual cost) | PASS |
| 5. Single-row scan (per-row refresh button) | PASS |

## Blocking issue: Apify detail-scraper outage

- `maxcopell~zillow-detail-scraper` started returning HTTP 400 (run FAILED) at ~22:33 UTC (3:33 PM PST) on 2026-05-04
- Last successful run: 16:26 UTC (9:26 AM PST) same day — 26 consecutive successes before that
- Appears to be a temporary actor outage, not our code
- Our error handling correctly classifies this as a real error (scan_error_count increments)
- `MLS_DETAIL_FALLBACK_ENABLED=false` set in local .env to avoid errors during testing
- **Do not deploy until detail-scraper is confirmed working again**

## Zip-search truncation (known limitation)

- `maxcopell~zillow-zip-search` returns a truncated subset for high-density zips
- Confirmed: 9120 S Van Ness Ave (90047) not in 111 zip-search results despite being on Zillow
- This is why the detail-scraper fallback exists — it catches these misses
- Without fallback, expect ~70% not-found rate from zip-search alone

## Cost math

| | Per call | Calls/run (800 props) | Cost/run |
|---|---|---|---|
| Zip search | $0.29 | ~80 (grouped by zip) | $23.20 |
| Detail fallback | $0.003 | ~560 (70% not-found) | $1.68 |
| **Total** | | **~640** | **$24.88** |

Manual-only scanning — no monthly commitment. Cost is per-use.

## Env vars (add to Railway when deploying)

| Var | Default | Purpose |
|---|---|---|
| `MLS_AUTO_SCAN_ENABLED` | `false` | Enable scheduled every-3-day scan |
| `MLS_DETAIL_FALLBACK_ENABLED` | `true` | Enable detail-scraper second pass |
| `MLS_DELAY_SECONDS` | `3` | Delay between Apify calls |
| `SHOW_COST_PREVIEW` | `true` | Show cost confirmation modal before scan |

## Architecture: Async scan system

Production had request timeouts on bulk scans because the scan ran
synchronously in the Flask request handler. Railway's reverse proxy
killed the connection before Apify calls completed.

Fix: async job queue via database.

- Dashboard creates a `scan_jobs` row (status=pending), returns immediately
- Worker polls `scan_jobs` every 30s, picks up pending jobs
- Worker runs the scan, updates progress (scanned, errors, cost) per zip batch
- Dashboard polls `GET /api/scan-jobs/<id>` every 3s for progress
- On completion: toast with summary + actual cost
- On 3 consecutive poll failures: visible alert instead of silent swallow
- Persistent "Scan running..." badge visible from any tab
- Page refresh / tab switch resumes polling via `checkForActiveJob()`
- Duplicate protection: API returns 409 if a job is already pending/running
- Max runtime: 2 hours (expires_at), worker marks stale jobs as failed

### Production status
- Production currently has the OLD sync code (broken on bulk scans)
- `preforeclosures` table has the 5 new columns (migrated)
- `scan_jobs` table does NOT exist on Railway yet — needs migration
- Env vars already set on Railway (MLS_AUTO_SCAN_ENABLED, etc.)

### Next steps to deploy
1. Review async code one more time
2. Run one local end-to-end test
3. Run scan_jobs migration on Railway:
   `DATABASE_URL="postgresql://..." python migrate_scan_jobs.py`
4. Merge mls-monitoring to main
5. Push to Railway
6. Test scan-selected with 5-10 properties on production data

## Deployment checklist (when ready)

1. Confirm `maxcopell~zillow-detail-scraper` is working (test via Apify console or API)
2. Run scan_jobs migration on Railway: `DATABASE_URL="..." python migrate_scan_jobs.py`
3. Merge mls-monitoring to main and push
4. Test scan-selected with 5-10 properties on production data
5. Verify detail fallback catches properties missed by zip-search
6. Verify progress polling works through Railway's proxy

## Safe to deploy main

The `main` branch has the scan commented out and none of the mls-monitoring code. Deploying main will NOT trigger any pre-foreclosure scanning.
