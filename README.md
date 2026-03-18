# DealFlow AI — Inland Empire Real Estate Deal Finder

AI-powered fix-and-flip deal finder for the Inland Empire (74 zip codes). Scrapes Zillow, scores deals with Claude AI, analyzes photos, calculates ARV from comps, estimates repairs, and computes max offer prices.

## Architecture

| Module | File | Purpose |
|--------|------|---------|
| Scraper | `scraper.py` | Pulls Zillow listings via Apify for 74 IE zip codes |
| Filter | `filter.py` | Price/age/keyword filtering |
| Scorer | `scorer.py` | Claude AI scores deals 1-100 |
| Photo Analyzer | `photo_analyzer.py` | Claude vision grades property zones |
| ARV Calculator | `arv_calculator.py` | Sold comps + ARV calculation |
| Repair Estimator | `repair_estimator.py` | IE 2026 repair cost estimates |
| Offer Calculator | `offer_calculator.py` | Full profit analysis + max offer |
| Alerts | `alerts.py` | Email alerts for 80+ score deals |
| Database | `database.py` | SQLite local / Postgres on Railway |
| Dashboard | `dashboard/index.html` | Flask-served web dashboard |
| Scheduler | `scheduler.py` | Daily 7am pipeline runs |
| Pipeline | `main.py` | Orchestrates full pipeline |

## Quick Start (Local)

```bash
# Install dependencies
pip install -r requirements.txt

# Copy env file and add your keys
cp .env.example .env

# Initialize database
python database.py

# Run full pipeline once
python main.py

# Start dashboard
python app.py
# Open http://localhost:5000
```

## Deploy to Railway

1. Push to GitHub
2. Connect repo in [Railway](https://railway.app)
3. Add a Postgres database plugin
4. Set environment variables in Railway dashboard:
   - `APIFY_API_KEY`
   - `ANTHROPIC_API_KEY`
   - `GMAIL_USER`
   - `GMAIL_PASSWORD`
5. Railway auto-detects `railway.toml` and deploys web + cron services

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `APIFY_API_KEY` | Yes | Apify API key for Zillow scraping |
| `ANTHROPIC_API_KEY` | Yes | Anthropic API key for Claude AI scoring |
| `GMAIL_USER` | Yes | Gmail address for sending alerts |
| `GMAIL_PASSWORD` | Yes | Gmail app password |
| `DATABASE_URL` | No | Postgres URL (auto-set by Railway) |
| `PORT` | No | Web server port (default: 5000) |

## Pipeline Flow

1. **Scrape** — Pull active Zillow listings for 74 IE zip codes
2. **Filter** — Under $900k, built before 2010, keyword matching
3. **Photos** — Claude vision grades Roof/HVAC/Plumbing/Interior/Kitchen/Bath/Foundation
4. **Score** — Claude rates each deal 1-100 on profit potential
5. **ARV** — Fetch sold comps within 0.5mi, calculate after-repair value
6. **Repairs** — Estimate repair costs (mid + worst case) per zone
7. **Offer** — Calculate max offer, total costs, profit, ROI
8. **Alerts** — Email top deals (score 80+) with full analysis
9. **Save** — Store everything in database

## Existing Script

`dealflow_updater.py` — Separate Zillow status updater + counter alert system (runs independently).
