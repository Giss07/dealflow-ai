# Builder Zones

**Date:** September 23, 2026
**Status:** BUILT AND TESTED ON 92503 — NOT YET DEPLOYED (off by default)

Find new-construction listings in the target zips, then surface the ordinary
for-sale listings — older homes, lots, teardowns — sitting within half a mile
of one. A builder opening a tract lifts the block around it.

## What shipped

| File | What |
|---|---|
| `builder_zones.py` | Core module: zip search + cache, new-build classification, haversine matching, dedupe, email rendering |
| `database.py` | `NewBuild` + `BuilderZoneMatch` models and their `*_to_dict` helpers |
| `migrate_new_builds.py` | Creates both tables. Safe to re-run |
| `worker.py` | `run_builder_zone_scan` (9 AM PT) + `run_builder_zone_matches` (9:30 AM PT) |
| `mcp_server.py` | `find_builder_zones(zip_code)` tool |
| `app.py` | `/admin/run-cron/builder-zone-scan` and `/admin/run-cron/builder-zone-matches` |
| `test_builder_zones.py` | Offline suite (free) + `--live` zip test |

## How new construction is detected

OpenWeb Ninja's `/search` has **no** server-side new-construction filter —
`listing_type` accepts only `BY_AGENT` and `BY_OWNER_OTHER` (verified against
the live API on 2026-09-23; anything else returns HTTP 400). So we pull the
zip's FOR_SALE listings and classify client-side. A listing is a new build if
**any** of these is set — Zillow does not populate them uniformly:

- `listingSubType.is_newHome`
- `marketingStatusSimplifiedCd == "New Construction"`
- `statusText` contains "new construction"
- `newConstructionType` present
- `isPaidBuilderNewConstruction` or `isPremierBuilder`

**Builder name** comes from `brokerName` — on a builder listing that IS the
builder (e.g. "BMC REALTY ADVISORS", "CENTRAL PACIFIC PROPERTIES"). `/search`
exposes no dedicated builder field.

**MANUFACTURED is excluded by default.** A manufactured-home dealer listing
carries the same `is_newHome` flag as a tract build; in the 92503 test the
$139k coach at 3750 McAllister Pkwy pulled a $1.22M house in as a "match".
Set `BUILDER_ZONE_EXCLUDE_HOME_TYPES=""` to count them.

## The two jobs

1. **`run_builder_zone_scan`** — 9 AM Pacific. Searches each target zip, saves
   new builds to `new_builds`. Dedupe on `zpid` (normalized address+zip when a
   listing has none); a build already stored gets `last_seen` and `price`
   refreshed instead of a second row.
2. **`run_builder_zone_matches`** — 9:30 AM Pacific. Searches the same zips,
   pairs every non-new-construction listing with its **nearest** new build
   inside the radius, writes `builder_zone_matches`, and emails one digest via
   Resend. `alert_sent` flips only after Resend accepts, so a failed send
   retries tomorrow rather than being silently dropped.

Both use the DST-aware gate the rest of the worker uses: registered at two UTC
times, with an inner `hour != 9: return` so exactly one run happens per day.

**Job 2 costs nothing extra.** `builder_zones._search_cache` holds each zip
page for `BUILDER_ZONE_SEARCH_TTL` (default 2h), and the jobs run 30 minutes
apart in the same worker process, so job 2's searches are cache hits. Widen
the gap past the TTL and job 2 re-pays for every zip.

## Cost — read before enabling

One call per zip per page, $0.0025 each.

| Scope | Calls/day | Cost/day | Cost/month |
|---|---|---|---|
| 74 IE zips x 2 pages | 148 | $0.37 | ~$11 |
| 1 zip x 2 pages | 2 | $0.005 | ~$0.15 |

That is **far past the free OpenWeb Ninja Basic tier (100 requests/month)** —
the full zip list needs the Pro plan ($25/mo, 10k requests). This is why the
jobs are **off by default**. Start with a few zips via `BUILDER_ZONE_ZIPS` if
you are still on Basic.

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `BUILDER_ZONES_ENABLED` | `false` | Master switch. The worker schedules nothing until this is `true` |
| `BUILDER_ZONE_ZIPS` | the 74 IE zips | Comma-separated override, e.g. `92503,92505` |
| `BUILDER_ZONE_RADIUS_MILES` | `0.5` | Match radius |
| `BUILDER_ZONE_MAX_PAGES` | `2` | Search pages per zip — each page is one billable call |
| `BUILDER_ZONE_SEARCH_TTL` | `7200` | Seconds a zip search stays cached (keep > the 30-min job gap) |
| `BUILDER_ZONE_EXCLUDE_HOME_TYPES` | `MANUFACTURED` | New-build home types that do not count as a builder zone |
| `BUILDER_ZONE_MAX_EMAIL_ROWS` | `50` | Matches per digest; the remainder keeps `alert_sent=false` and goes out next run |

Reuses `OPENWEB_NINJA_API_KEY`, `RESEND_API_KEY`, and `ALERT_EMAILS` /
`ALERT_EMAIL` — no new credentials.

The 74-zip default list was restored from the retired `scraper.py` (removed in
commit 744c68a when Apify was dropped) — it is the same Inland Empire set the
Apify pipeline scanned, and it includes 92503.

## Verification (2026-09-23, live 92503)

- 4 new builds found (after the MANUFACTURED exclusion), 77 resale listings scanned
- 8 listings within 0.5 mi — closest 0.20 mi (9721 Indiana Ave, $599k, BMC Realty Advisors)
- Second run of both jobs: 0 duplicate rows, 0 duplicate emails
- Match job made 0 API calls (served entirely from job 1's cache)
- Test cost: 2 calls, $0.005
- `test_builder_zones.py`: 30 offline checks pass; `--live 92503` passes

No email has been sent from this feature yet — the live test stubbed the
Resend call. Trigger `POST /admin/run-cron/builder-zone-matches` (during the
9 AM PT hour, or with the gate temporarily relaxed) to see a real one.

## Deploy checklist

1. `DATABASE_URL="postgresql://..." python migrate_new_builds.py`
2. Set `BUILDER_ZONES_ENABLED=true` on the Railway **worker** service
   (optionally `BUILDER_ZONE_ZIPS` to start small)
3. Redeploy the **mcp** service so `find_builder_zones` registers
4. Watch for `[BUILDER_ZONE_SCAN_DONE]` and `[BUILDER_ZONE_MATCH]` in worker logs

## Known limits

- `brokerName` is the listing office, so a builder selling through a brokerage
  shows the brokerage's name, not "Lennar". There is no builder field on this
  endpoint to do better.
- 2 pages/zip (~80 listings) is a cost ceiling, not a complete inventory. Raise
  `BUILDER_ZONE_MAX_PAGES` for dense zips and pay per page.
- Matching is straight-line distance. A new build across a freeway is still
  "0.3 miles away".
- Only listings with coordinates participate; unmappable ones are logged and
  skipped.
