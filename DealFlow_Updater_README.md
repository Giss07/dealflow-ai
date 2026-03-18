# DealFlow Updater — Complete Documentation

**Script location:** `/Users/Gissel/Desktop/DealFlow/dealflow_updater.py`  
**Last updated:** March 2026

---

## What Does It Do?

The DealFlow Updater is an automated script that monitors your HUD property deals. It does 3 things every time it runs:

1. Checks Gmail for counter offer emails
2. Checks Zillow for property status changes
3. Sends alert emails when action is needed

---

## When Does It Run?

| Time | Mode | What Happens |
|---|---|---|
| 9:00 AM PST daily | Full Run | Gmail + Zillow + Alerts |
| 10AM–8PM PST (hourly) | Gmail Only | Counters only, no Zillow |
| Outside those hours | Skip | Does nothing |

Scheduled via **crontab** on Gissel's Mac. Runs automatically — no manual action needed.

---

## Part 1: Gmail Monitoring

Monitors: `unmatched.dealflow@gmail.com`  
Runs: Every scheduled run (full and gmail_only)

**What it does:**
- Scans all unread emails for a matching property address from the Google Sheet
- If a match is found, extracts the counter offer price
- Supports HUD email format: *"minimum acceptable net to HUD offer amount for this property as $209,000"*
- Supports standard format: any dollar amount near the word "counter"
- If a previous counter exists → saves it to the Notes column (history is never lost)
- Updates the sheet: Counter Price, Counter Date, Status → Counter
- If no address match → ignores the email completely

---

## Part 2: Zillow Status Check

Runs: Full run only (9AM)  
Tool: **Apify** actor `maxcopell~zillow-detail-scraper` with BUYPROXIES94952 proxy

Checks every property in the sheet that isn't already `Accepted`, `Rejected`, `Closed`, or `STP`.

### Status Mapping

| Zillow Returns | Sheet Status | Action |
|---|---|---|
| `FOR_SALE` / `ACTIVE` | Pending | ⚠️ Send alert email |
| `FOR_SALE` / `ACTIVE` | Anything else | No change |
| `PENDING` / `UNDER_CONTRACT` | Sent | Update sheet → Pending |
| `PENDING` / `UNDER_CONTRACT` | Pending | No change |
| `OTHER` (Under Contract) | Any | Treat as Pending, no change |
| `RECENTLY_SOLD` / `SOLD` / `FORECLOSED` / `CLOSED` | Any | Update sheet → STP |
| `PRE_FORECLOSURE` | Any | Could not determine, no action |
| HTTP error / timeout | Any | Could not determine, no action |

> **Note:** Zillow sometimes returns `OTHER` for properties that are Under Contract. The script treats this as Pending — no alert, no change.

---

## Part 3: Alert Emails

Recipients: `gescobarrei@gmail.com` + `christian@unmatchedoffers.com`

### 3 Types of Alerts

**🔴 HOT ALERT** — Counter at or below your offer price
- Shows: Address, Your Offer, Their Counter, Difference, Offer-to-Net (6%)
- Subject: *"🚨 HOT ALERT - Counter At or Below Your Offer Price!"*

**🟠 CLOSE DEAL ALERT** — Counter within $30,000 of your offer
- Shows: Address, Your Offer, Their Counter, Gap, Offer-to-Net (6%)
- Subject: *"⚠️ CLOSE DEAL ALERT - Counter Within $30k of Your Offer!"*

**🔵 BACK ON MARKET** — Was Pending, now FOR_SALE on Zillow
- HUD properties → Red urgent section "🚨 HUD - RESUBMIT NOW!"
- Non-HUD properties → Blue section "Verify & Consider Resubmitting"
- HUD vs non-HUD is determined by the `Lead Source` column in the sheet
- Subject: *"🚨 HUD BACK ON MARKET!"* or *"🔄 BACK ON MARKET"*

### Alert Deduplication
Once an alert fires for a property, `Alert Sent = Yes` is written to the sheet. The script will **never alert on the same property twice** unless you manually clear that flag in the sheet.

---

## Google Sheet Updates

Sheet ID: `1GMp9LbZLgY_uaTjiDQ9cTcy4I1QxOqLsNZWORwkUMCY`

| Column | What Gets Updated |
|---|---|
| `Status (/Accepted/Rejected/Counter)` | Pending → STP, Sent → Pending |
| `Counter Price` | New counter price from email |
| `Counter Date` | Date counter was received |
| `Notes` | Previous counter history saved here |
| `Alert Sent` | Set to `Yes` after alert fires |

---

## Manual Usage

```bash
# Navigate to folder
cd ~/Desktop/DealFlow

# Full run (Gmail + Zillow + Alerts)
python3 dealflow_updater.py full

# Gmail only (counters, no Zillow)
python3 dealflow_updater.py gmail_only

# Test mode (connection checks only, no sheet writes)
python3 dealflow_updater.py test

# Auto-detect based on current time
python3 dealflow_updater.py
```

---

## Configuration

| Setting | Value |
|---|---|
| Alert sender email | dealflow.alerts@gmail.com |
| Gmail monitored | unmatched.dealflow@gmail.com |
| Alert recipients | gescobarrei@gmail.com, christian@unmatchedoffers.com |
| Close deal threshold | $30,000 |
| Zillow scraper | maxcopell~zillow-detail-scraper |
| Proxy group | BUYPROXIES94952 |

---

## Crontab Schedule

```bash
# Full run at 9AM PST daily
0 9 * * * TZ=America/Los_Angeles /usr/bin/python3 /Users/Gissel/Desktop/DealFlow/dealflow_updater.py full >> /Users/Gissel/Desktop/DealFlow/dealflow_log.txt 2>&1

# Gmail-only check every hour 10AM–8PM PST
0 10-20 * * * TZ=America/Los_Angeles /usr/bin/python3 /Users/Gissel/Desktop/DealFlow/dealflow_updater.py gmail_only >> /Users/Gissel/Desktop/DealFlow/dealflow_log.txt 2>&1
```

To view or edit: `crontab -l` (view) / `crontab -e` (edit)  
Log file: `/Users/Gissel/Desktop/DealFlow/dealflow_log.txt`

---

## Troubleshooting

**Script not running at scheduled time?**
```bash
crontab -l  # verify crontab is still set
```

**Zillow returning wrong status?**
- `OTHER` = Under Contract (treated as Pending — correct)
- `PRE_FORECLOSURE` = no action taken
- HTTP 400 = Apify couldn't find the property URL

**Getting duplicate alerts?**
- Check `Alert Sent` column in sheet — should be `Yes` for already-alerted properties
- If blank, the script will re-alert on next run

**Counter price not extracted?**
- Check the email format matches HUD or standard counter format
- Run `python3 dealflow_updater.py test` to verify Gmail connection
