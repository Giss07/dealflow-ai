# DealFlow AI — Inland Empire Real Estate Deal Finder

AI-powered fix-and-flip deal finder for **74 Inland Empire zip codes**. Scrapes Zillow, scores deals, calculates offers, and tracks everything in a dashboard + Google Sheet.

## Architecture

| Module | Purpose |
|--------|---------|
| `scraper.py` | Scrapes Zillow via Apify (74 IE zip codes, batch search) |
| `filter.py` | Excludes renovated, mobile homes, new construction, lots |
| `scorer.py` | Scores deals 1-100 (Claude AI + smart fallback formula) |
| `arv_calculator.py` | ARV from Zestimate, price x 1.25 fallback, manual Privy override |
| `repair_estimator.py` | IE 2026 repair costs (7 zones, mid/worst case) |
| `offer_calculator.py` | Full profit analysis with iterative max offer formula |
| `photo_analyzer.py` | Claude vision grades Roof/HVAC/Plumbing/Interior/Kitchen/Bath/Foundation |
| `alerts.py` | Email alerts for new deals scoring 80+ |
| `sheets.py` | Apps Script webhook writes offers to Google Sheet |
| `database.py` | SQLite local / Postgres on Railway |
| `app.py` | Flask API + dashboard server |
| `dashboard/index.html` | Full web dashboard (PWA, mobile-friendly) |
| `dealflow_updater.py` | Monitors Gmail for counters, checks Zillow status |
| `worker.py` | Railway 24/7 scheduler |

## Dashboard Features

- **Search bar** — real-time search across all tabs by address/city/zip
- **5 tabs**: Active Deals, Pending, My Offers, Archived, Tracker (Google Sheet)
- **Quick score filters**: All, 50+, 60+, 70+, 80+, 90+ with Hide Turnkey toggle
- **Sort** by listing date (newest first), score, price, ARV, profit, ROI
- **Deal detail modal** with full profit analysis (Buying/Renovation/Holding/Selling/Summary)
- **Editable fields**: ARV (from Privy), Est. Repairs, Comps, ARV Justification
- **Actions**: Submit Offer to Google Sheet, Mark Pending, Archive, Analyze Photos
- **Export CSV** for all visible deals
- **Score tooltip** — hover to see breakdown (ARV margin, $/sqft, days, etc.)
- **Mobile PWA** — card view, full-screen modal, installable on home screen

## Scoring Formula (Fallback — No API Needed)

- ARV vs Price margin: 30pts max (30%+ gap = +30)
- Price per sqft: 20pts max (<$200/sqft = +20)
- Days on market: 15pts max (30+ days = +15)
- Repairs/ARV ratio: 15pts max (<10% = +15)
- Deal keywords: +10, Price/year bonuses: +15
- Renovation penalties: -30 to -40 (turnkey, remodeled, new construction)

## Offer Calculator Formula

- **Closing**: 1% of purchase price
- **Holding (3mo)**: 100% LTC @ 12%, property tax $904/yr, insurance+utilities $80/mo
- **Selling**: Staging $100 + 1% closing + 2% listing agent + 2% buyer agent
- **Target Profit**: 10% of ARV
- **Max Offer**: ARV - Repairs - Holding - Selling - Closing - Profit (solved algebraically)

## Google Sheet Integration

- **Submit Offer** writes to Properties_Offer_Tracker_Template via Apps Script webhook
- **Columns**: Date, Source, Address (Zillow hyperlink), ARV, Offer, Repairs, Comps, ARV Justification
- **Tracker tab** — read-only view of Google Sheet with color-coded status (STP/Pending/Active/Resubmit)
- **dealflow_updater.py** — monitors Gmail for counter offers, checks Zillow status every 2 days

## Quick Start (Local)

```bash
pip install -r requirements.txt
cp .env.example .env    # fill in your API keys
python3 database.py     # init database
python3 main.py         # run full pipeline
python3 app.py          # start dashboard at localhost:8080
```

## Deploy to Railway

1. Push to GitHub
2. Connect repo in [Railway](https://railway.app)
3. Add a Postgres database plugin
4. Set environment variables (see below)
5. Add a second service for the worker (`python worker.py`)

## Utility Scripts

| Script | Purpose |
|--------|---------|
| `python3 main.py` | Full pipeline: scrape, filter, score, ARV, repairs, offers |
| `python3 main.py reprocess` | Re-run pipeline on existing DB data (no scraping) |
| `python3 fix_db.py` | Clean DB from cache: remove dupes, mobile homes, new construction |
| `python3 score_existing.py` | Fill missing scores, ARV, repairs, offers |
| `python3 rescore.py` | Re-score all deals with Claude AI |
| `python3 rescore.py test` | Test scoring on 5 sample deals |
| `python3 fetch_descriptions.py` | Fetch listing descriptions via Apify detail scraper |
| `python3 analyze_deal.py "address"` | Run photo analysis on a single property |
| `python3 migrate_to_railway.py` | Sync local SQLite to Railway Postgres |
| `python3 dealflow_updater.py test` | Test all connections (Sheet, Gmail, Apify) |
| `python3 dealflow_updater.py gmail_only` | Check Gmail for counter offers |
| `python3 dealflow_updater.py full` | Full run: Gmail + Zillow status + Alerts |
| `python3 sheets.py` | Test Google Sheet webhook |

## Filters Applied

- Under $900k, pre-2010
- No: mobile homes, manufactured, lots, units, apartments
- No: new construction (Plan, Residence, Flora, Arboretum, Newman, Vista, Pointe)
- No: year 2020+, brand new listings ($400k+ with 0 days)
- No: remodeled, renovated, turnkey, move-in ready (30+ exclude keywords)

## Schedule (Crontab + Railway Worker)

- **Every 2 days 8AM PST**: Full updater run (Gmail + Zillow status checks)
- **Hourly 9AM-6PM PST**: Gmail-only counter offer checks
- **Mon & Thu 7AM PST**: DealFlow AI scraper pipeline

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | Claude AI scoring + photo analysis |
| `APIFY_API_KEY` | Yes | Zillow scraping via Apify |
| `GMAIL_USER` | Yes | Gmail for sending alerts |
| `GMAIL_PASSWORD` | Yes | Gmail app password |
| `ALERT_EMAIL` | Yes | Where deal alerts go |
| `DATABASE_URL` | Railway | Postgres URL (auto-set by Railway plugin) |
| `SPREADSHEET_ID` | For updater | Google Sheet ID |
| `DEALFLOW_ALERTS_GMAIL` | For updater | Alerts sender Gmail |
| `DEALFLOW_ALERTS_PASSWORD` | For updater | Alerts sender app password |
| `CHRISTIAN_GMAIL` | For updater | Counter offer inbox |
| `CHRISTIAN_APP_PASSWORD` | For updater | Counter offer inbox app password |
| `ALERT_EMAILS` | For updater | Comma-separated alert recipients |
| `TRACKER_WEBHOOK_URL` | For tracker | Apps Script doGet URL |
| `SHEETS_WEBHOOK_URL` | For offers | Apps Script doPost URL |

## Database

- **1,750 clean deals** (filtered from 3,167 scraped)
- SQLite locally, Postgres on Railway
- `migrate_to_railway.py` preserves user-edited ARV/repairs/offers/archive status
