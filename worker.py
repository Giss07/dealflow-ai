"""
DealFlow Worker — Runs on Railway 24/7.
- 8AM PST: Full run (Gmail + Zillow + Alerts)
- 9AM-6PM PST hourly: Gmail-only (counter offer checks)
"""

import sys
import os

# Force unbuffered output so Railway sees logs immediately
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ['PYTHONUNBUFFERED'] = '1'

print("Worker script starting...", flush=True)

from dotenv import load_dotenv
load_dotenv()

import time
import logging
import schedule
import pytz
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

PST = pytz.timezone("America/Los_Angeles")

# Add current dir to path so imports work
sys.path.insert(0, os.path.dirname(__file__))


def run_full():
    """Run dealflow_updater in full mode (every 3 days)."""
    now_pst = datetime.now(PST)
    if now_pst.day % 3 != 0:
        logger.info(f"Skipping full run — runs every 3 days (day {now_pst.day})")
        return
    logger.info(f"=== FULL RUN started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")
    try:
        import subprocess
        result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), 'dealflow_updater.py'), 'full'],
            timeout=1800, capture_output=True, text=True
        )
        if result.returncode != 0:
            logger.error(f"Full run exited {result.returncode}: {result.stderr[:500]}")
        else:
            logger.info("Full run completed successfully")
    except subprocess.TimeoutExpired:
        logger.error("Full run timed out after 30 minutes")
    except Exception as e:
        logger.error(f"Full run failed: {e}")


def run_gmail_only():
    """Run dealflow_updater in gmail_only mode (only during 9AM-6PM PST)."""
    now_pst = datetime.now(PST)
    hour = now_pst.hour
    if hour < 9 or hour > 18:
        logger.info(f"Skipping Gmail check — outside 9AM-6PM PST (currently {hour}:00)")
        return
    logger.info(f"=== GMAIL-ONLY RUN started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")
    try:
        import subprocess
        result = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), 'dealflow_updater.py'), 'gmail_only'],
            timeout=300, capture_output=True, text=True
        )
        if result.returncode != 0:
            logger.error(f"Gmail check exited {result.returncode}: {result.stderr[:500]}")
        else:
            logger.info("Gmail check completed")
    except subprocess.TimeoutExpired:
        logger.error("Gmail check timed out after 5 minutes")
    except Exception as e:
        logger.error(f"Gmail-only run failed: {e}")


def run_preforeclosure_scan():
    """Scan pre-foreclosure properties on Zillow for MLS status changes.

    Runs every 3 days. Groups properties by zip code to minimize Apify calls.
    Detects Monitoring → On Market transitions and sends email + sets is_new flag.

    Config env vars:
      MLS_CHECKER_ENABLED: "true" (default) or "false" — kill switch
      MLS_BATCH_SIZE: max properties per run (default: all)
      MLS_DELAY_SECONDS: delay between zip code searches (default: 3)
    """
    # Kill switch
    if os.getenv("MLS_CHECKER_ENABLED", "true").lower() == "false":
        logger.info("MLS checker disabled via MLS_CHECKER_ENABLED=false, skipping")
        return

    now_pst = datetime.now(PST)
    logger.info(f"=== PRE-FORECLOSURE SCAN started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")

    batch_size = int(os.getenv("MLS_BATCH_SIZE", "0")) or None  # 0 = unlimited
    delay_seconds = int(os.getenv("MLS_DELAY_SECONDS", "3"))

    try:
        import requests as req
        from database import init_db, get_session, PreForeclosure
        from datetime import datetime as dt

        APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
        if not APIFY_API_KEY:
            logger.error("APIFY_API_KEY not set, skipping pre-foreclosure scan")
            return

        init_db()
        db = get_session()
        query = db.query(PreForeclosure).filter(
            (PreForeclosure.is_archived == False) | (PreForeclosure.is_archived == None)
        ).filter(PreForeclosure.mls_status != "offer-submitted")
        if batch_size:
            query = query.limit(batch_size)
        properties = query.all()
        logger.info(f"Scanning {len(properties)} pre-foreclosure properties (batch_size={batch_size or 'all'}, delay={delay_seconds}s)...")

        # Group by zip code to minimize Apify calls
        by_zip = {}
        for pf in properties:
            z = pf.zip_code or "unknown"
            if z not in by_zip:
                by_zip[z] = []
            by_zip[z].append(pf)

        scanned = 0
        errors = 0
        apify_calls = 0
        new_on_market = []

        for zip_code, pf_list in by_zip.items():
            if zip_code == "unknown":
                for pf in pf_list:
                    pf.ai_notes = "No zip code — cannot search"
                    pf.last_scanned = dt.utcnow()
                    pf.scan_error_count = (pf.scan_error_count or 0) + 1
                    pf.last_scan_error = "No zip code"
                continue

            logger.info(f"  Searching zip {zip_code} ({len(pf_list)} properties)...")
            api_url = "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items"
            payload = {"zipCodes": [zip_code], "maxItems": 50}

            # Retry with backoff for Apify calls
            all_results = None
            for attempt in range(3):
                try:
                    apify_calls += 1
                    resp = req.post(api_url, params={"token": APIFY_API_KEY}, json=payload,
                                    headers={"Content-Type": "application/json"}, timeout=120)
                    if resp.status_code == 429:
                        wait = 2 ** (attempt + 1)
                        logger.warning(f"  Apify rate limited (429), waiting {wait}s")
                        time.sleep(wait)
                        continue
                    if resp.status_code not in (200, 201):
                        logger.error(f"  Apify error {resp.status_code} for zip {zip_code}: {resp.text[:200]}")
                        for pf in pf_list:
                            pf.last_scanned = dt.utcnow()
                            pf.scan_error_count = (pf.scan_error_count or 0) + 1
                            pf.last_scan_error = f"Apify HTTP {resp.status_code}"
                        break
                    all_results = resp.json()
                    break
                except req.exceptions.Timeout:
                    logger.warning(f"  Apify timeout attempt {attempt+1}/3 for zip {zip_code}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        for pf in pf_list:
                            pf.last_scanned = dt.utcnow()
                            pf.scan_error_count = (pf.scan_error_count or 0) + 1
                            pf.last_scan_error = "Apify timeout after 3 attempts"
                except req.exceptions.ConnectionError as e:
                    logger.warning(f"  Apify connection error attempt {attempt+1}/3: {e}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        for pf in pf_list:
                            pf.last_scanned = dt.utcnow()
                            pf.scan_error_count = (pf.scan_error_count or 0) + 1
                            pf.last_scan_error = f"Connection error: {str(e)[:100]}"

            if all_results is None:
                db.commit()
                continue

            logger.info(f"  Got {len(all_results)} Zillow listings for {zip_code}")

            for pf in pf_list:
                try:
                    _scan_single_property(pf, all_results, zip_code, new_on_market)
                    # Clear error tracking on success
                    pf.scan_error_count = 0
                    pf.last_scan_error = None
                    scanned += 1
                except Exception as e:
                    logger.error(f"  Error processing {pf.address}: {e}")
                    pf.last_scanned = dt.utcnow()
                    pf.scan_error_count = (pf.scan_error_count or 0) + 1
                    pf.last_scan_error = f"{type(e).__name__}: {str(e)[:100]}"
                    errors += 1

            # Commit after each zip to prevent connection timeout
            db.commit()
            time.sleep(delay_seconds)

        # Save alert data before closing session
        alert_data = []
        for pf in new_on_market:
            alert_data.append({"address": pf.address, "city": pf.city, "mls_price": pf.mls_price, "estimated_value": pf.estimated_value})

        db.commit()
        db.close()

        logger.info(f"Pre-foreclosure scan complete: {scanned} scanned, {errors} errors, {apify_calls} Apify calls, {len(new_on_market)} new on market")

        # Send alert for newly listed properties
        if alert_data:
            _send_new_listing_alert(alert_data)

    except Exception as e:
        logger.error(f"Pre-foreclosure scan failed: {e}", exc_info=True)


def _scan_single_property(pf, all_results, zip_code, new_on_market):
    """Process a single pre-foreclosure property against Zillow results.

    Updates: mls_status, previous_mls_status, listed_at, is_new, mls_price,
    ai_notes, last_scanned, scan_error_count, last_scan_error.
    Does NOT auto-populate estimated_value.
    """
    from datetime import datetime as dt

    street_parts = pf.address.lower().split()
    matched = None
    for item in all_results:
        item_addr = (item.get("addressStreet") or item.get("address") or "").lower()
        if len(street_parts) >= 2 and street_parts[0] in item_addr and street_parts[1] in item_addr:
            matched = item
            break

    # Save previous status for transition detection
    prev_status = pf.mls_status
    pf.previous_mls_status = prev_status
    pf.last_scanned = dt.utcnow()

    if not matched:
        # Not found — set to Monitoring (shown as "Monitoring" in dashboard)
        pf.mls_status = "unknown"
        pf.ai_notes = f"Not found on Zillow ({len(all_results)} listings in {zip_code})"
        return

    home_info = matched.get("hdpData", {}).get("homeInfo", {})
    home_status = (home_info.get("homeStatus") or matched.get("statusType") or "").upper()
    price = home_info.get("price") or matched.get("unformattedPrice")
    zestimate = home_info.get("zestimate") or matched.get("zestimate")

    # Check if it's an auction listing (not a real MLS listing)
    status_text = (matched.get("statusText") or "").upper()
    raw_price = home_info.get("price") or matched.get("unformattedPrice")
    try:
        price_num = float(str(raw_price).replace("$", "").replace(",", "") or 0)
    except (ValueError, TypeError):
        price_num = 0
    is_auction = bool(
        "AUCTION" in status_text
        or "AUCTION" in home_status
        or "FORECLOSED" in home_status
        or (raw_price is not None and price_num == 0)
    )

    if is_auction:
        pf.mls_status = "auction"
    elif "FOR_SALE" in home_status:
        pf.mls_status = "on-market"
    elif "PENDING" in home_status or "OTHER" in home_status:
        pf.mls_status = "on-market"
    elif "FORECLOSURE" in home_status or "PRE_FORECLOSURE" in home_status:
        pf.mls_status = "pre-foreclosure"
    else:
        pf.mls_status = "unknown"

    # Update price (NOT estimated_value — user requirement)
    if price:
        try:
            pf.mls_price = float(str(price).replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            pass

    # Build notes
    notes = [f"Zillow: {home_status}"]
    if price:
        try:
            notes.append(f"${float(str(price).replace('$','').replace(',','')):,.0f}")
        except (ValueError, TypeError):
            pass
    if zestimate:
        try:
            notes.append(f"Zest: ${float(zestimate):,.0f}")
        except (ValueError, TypeError):
            pass
    pf.ai_notes = " | ".join(notes)

    # Detect Monitoring → On Market transition
    is_new_listing = (prev_status != "on-market" and pf.mls_status == "on-market")
    pf.is_new = is_new_listing
    if is_new_listing:
        pf.listed_at = dt.utcnow()
        new_on_market.append(pf)
        logger.info(f"  NEW ON MARKET: {pf.address}, {pf.city}")


def _send_new_listing_alert(alert_data):
    """Send email alert for newly listed pre-foreclosure properties."""
    try:
        from alerts import GMAIL_USER, GMAIL_PASSWORD, ALERT_EMAIL
        if not GMAIL_USER or not GMAIL_PASSWORD:
            return
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        html = "<html><body>"
        html += "<h2 style='color:#22c55e;'>Pre-Foreclosure NOW ON MARKET!</h2>"
        html += "<p>These pre-foreclosure properties just appeared on Zillow:</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse:collapse;'>"
        html += "<tr style='background:#166534;color:white;'><th>Address</th><th>Price</th><th>Value</th></tr>"
        for ad in alert_data:
            price_str = f"${ad['mls_price']:,.0f}" if ad.get('mls_price') else "N/A"
            val_str = f"${ad['estimated_value']:,.0f}" if ad.get('estimated_value') else "N/A"
            html += f"<tr><td><b>{ad['address']}, {ad['city']}</b></td><td>{price_str}</td><td>{val_str}</td></tr>"
        html += "</table></body></html>"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{len(alert_data)} Pre-Foreclosure Properties NOW ON MARKET!"
        msg["From"] = GMAIL_USER
        msg["To"] = ALERT_EMAIL or GMAIL_USER
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, ALERT_EMAIL or GMAIL_USER, msg.as_string())
        logger.info(f"Alert email sent for {len(alert_data)} new listings")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


def run_dealflow_pipeline():
    """Run the DealFlow AI scraper pipeline (Mon & Thu at 7AM PST)."""
    now_pst = datetime.now(PST)
    logger.info(f"=== DEALFLOW PIPELINE started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")
    try:
        from main import run_full_pipeline
        run_full_pipeline()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    logger.info("DealFlow Worker starting...")
    logger.info(f"Current time PST: {datetime.now(PST).strftime('%Y-%m-%d %H:%M %Z')}")

    # All times in UTC (Railway runs UTC)
    # PST = UTC - 7 (PDT = UTC - 7 during daylight saving)
    schedule.every().day.at("15:00").do(run_full)           # 8AM PST = 15:00 UTC
    schedule.every().hour.at(":00").do(run_gmail_only)      # Hourly Gmail check (9AM-6PM PST only, checked inside func)

    # Schedule DealFlow AI pipeline (Mon & Thu at 7AM PST = 14:00 UTC) — PAUSED
    # schedule.every().monday.at("14:00").do(run_dealflow_pipeline)
    # schedule.every().thursday.at("14:00").do(run_dealflow_pipeline)

    # Pre-foreclosure MLS scan — every 3 days at 2AM PST = 09:00 UTC
    # Controlled by MLS_CHECKER_ENABLED env var (default: true)
    schedule.every(3).days.at("09:00").do(run_preforeclosure_scan)

    mls_enabled = os.getenv("MLS_CHECKER_ENABLED", "true").lower() != "false"
    logger.info("Scheduled (UTC times, Railway server):")
    logger.info(f"  - Every 3 days 09:00 UTC (2AM PST): Pre-foreclosure MLS scan ({'ENABLED' if mls_enabled else 'DISABLED via env'})")
    logger.info("  - 15:00 UTC (8AM PST): Full updater run")
    logger.info("  - Hourly (9AM-6PM PST): Gmail-only counter checks")
    logger.info("  - PAUSED: Mon & Thu DealFlow AI scraper pipeline")

    # Skip initial Gmail check on startup — let the schedule handle it
    # (Previous bug: if gmail check crashes on startup, schedule loop never starts)
    # Start a tiny health server so Railway healthcheck passes
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        def log_message(self, format, *args):
            pass  # Suppress access logs

    health_port = int(os.getenv("PORT", 8080))
    health_server = HTTPServer(("0.0.0.0", health_port), HealthHandler)
    threading.Thread(target=health_server.serve_forever, daemon=True).start()
    logger.info(f"Health server on port {health_port}")

    logger.info("Worker ready. Schedule loop starting...")

    # Loop
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logger.error(f"Schedule error: {e}")
        time.sleep(30)
