# OpenWeb Ninja Migration — COMPLETE

**Date completed:** May 5, 2026
**Status:** PRODUCTION VERIFIED

## What shipped

- OpenWeb Ninja Real-Time Zillow Data API replaces Apify for MLS scanning
- Direct address lookup — no zip-search blind spots, no two-pass fallback needed
- 19 new foreclosure/property columns on preforeclosures table
- Detail panel (chevron expand) with Foreclosure, Financial, Property sections
- Sort by Auction Date (soonest) and Days on Zillow
- Auction date filter: <30d, <60d, <90d (future-only)
- Async scan jobs via worker service (fixes Railway request timeouts)
- Cost preview modal before scanning
- Feature flag: USE_OPENWEB_NINJA (default false, Apify stays active)

## Production verification

- Property: 5140 Rhode Island Dr, Sacramento 95841
- Result: 17/19 fields populated, foreclosure data flowing
- Speed: ~2 seconds (vs 30-60s on Apify)

## Bug fixes (May 5, 2026)

### Auction.com → on-market misclassification (FIXED)
- 5140 Rhode Island was marked "On Market" but was actually an Auction.com bank-owned listing
- Root cause: status mapping checked homeStatus (FOR_SALE) but not listingTypeDimension
- Fix: "on-market" only when listingTypeDimension contains "by Agent" AND listingDataSource is not "Auction"
- Default to auction or pre-foreclosure when ambiguous — safer than false on-market
- Verified in production: 5140 now shows yellow "Auction" badge

### Stale Apify data flagged (FIXED)
- ~795 properties scanned by Apify have no OpenWeb Ninja data (listing_type_dimension is NULL)
- Added "⚠ Needs scan" badge on all rows missing listing_type_dimension
- No auto-invalidation — user decides which to re-scan via checkboxes + Scan Selected

### Scan cost modal showing Apify text (FIXED)
- Modal was hardcoded to show "Estimated Apify cost" regardless of provider
- Fix: reads provider field from estimate response, shows OpenWeb Ninja pricing when active
- Display-only bug — actual scans were already using OpenWeb Ninja correctly
- Production verified May 5 evening

## Apify status

- All Apify code is INTACT — zero deletions
- Controlled by USE_OPENWEB_NINJA env var (currently true on Railway)
- To rollback: set USE_OPENWEB_NINJA=false on Railway web + worker services
- Plan: remove Apify code in a separate cleanup commit after 1 week stable production (target: May 12, 2026)

## Remaining decisions (not acted on yet)

1. **~795 stale Apify properties** — decide whether to batch re-scan all via OpenWeb Ninja (costs ~$2 on Pro plan) or re-scan on-demand as needed
2. **Upgrade to OpenWeb Ninja Pro** ($25/month, 10k requests) — currently on free Basic (5/100 used). Upgrade when free tier gets close or before batch re-scan
3. **Apify code cleanup** — remove all Apify functions, env vars, and code paths after 1 week stable production. Target: May 12, 2026

## Cost

- OpenWeb Ninja Basic (free): 100 requests/month — currently 5/100 used
- OpenWeb Ninja Pro ($25/month): 10,000 requests/month — upgrade when free tier gets close
- Actual Apify spend was $30-50/month — OpenWeb Ninja saves $5-25/month with better data and speed

## Env vars (set on Railway web + worker)

| Var | Value | Purpose |
|---|---|---|
| OPENWEB_NINJA_API_KEY | (set) | API authentication |
| USE_OPENWEB_NINJA | true | Feature flag — false reverts to Apify |
| MLS_AUTO_SCAN_ENABLED | false | Manual scanning only (dashboard) |
| MLS_DETAIL_FALLBACK_ENABLED | true | Apify detail-scraper fallback (unused when OpenWeb active) |
| MLS_DELAY_SECONDS | 3 | Delay between API calls |
| SHOW_COST_PREVIEW | true | Cost confirmation modal before scanning |

## Files

| File | What |
|---|---|
| worker.py | _scan_via_openweb_ninja, feature flag, cost constants |
| database.py | 19 new columns, updated preforeclosure_to_dict |
| app.py | Single-property scan respects USE_OPENWEB_NINJA |
| dashboard/index.html | Detail panel, chevron, sort, auction filter |
| migrate_openweb.py | Schema migration (19 columns, safe to re-run) |
| migrate_scan_jobs.py | scan_jobs table (async scan system) |
| migrate_mls_monitoring.py | 5 MLS monitoring columns (earlier migration) |
