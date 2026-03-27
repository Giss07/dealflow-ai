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
    """Run dealflow_updater in full mode (every 2 days)."""
    now_pst = datetime.now(PST)
    if now_pst.day % 2 == 0:
        logger.info(f"Skipping full run — runs every 2 days (next run tomorrow)")
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
    """Scan ALL pre-foreclosure properties on Zillow daily at 2AM PST."""
    now_pst = datetime.now(PST)
    logger.info(f"=== PRE-FORECLOSURE SCAN started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")
    try:
        import requests as req
        from database import init_db, get_session, PreForeclosure, preforeclosure_to_dict
        from urllib.parse import quote_plus
        from datetime import datetime as dt

        APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
        if not APIFY_API_KEY:
            logger.error("APIFY_API_KEY not set, skipping pre-foreclosure scan")
            return

        init_db()
        db = get_session()
        properties = db.query(PreForeclosure).filter(PreForeclosure.mls_status != "offer-submitted").all()
        logger.info(f"Scanning {len(properties)} pre-foreclosure properties...")

        # Group by zip code to minimize Apify calls
        by_zip = {}
        for pf in properties:
            z = pf.zip_code or "unknown"
            if z not in by_zip:
                by_zip[z] = []
            by_zip[z].append(pf)

        scanned = 0
        new_on_market = []

        for zip_code, pf_list in by_zip.items():
            if zip_code == "unknown":
                for pf in pf_list:
                    pf.ai_notes = "No zip code — cannot search"
                    pf.last_scanned = dt.utcnow()
                continue

            logger.info(f"  Searching zip {zip_code} ({len(pf_list)} properties)...")
            api_url = "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items"
            payload = {"zipCodes": [zip_code], "maxItems": 50}

            try:
                resp = req.post(api_url, params={"token": APIFY_API_KEY}, json=payload,
                                headers={"Content-Type": "application/json"}, timeout=300)
                if resp.status_code not in (200, 201):
                    logger.error(f"  Apify error {resp.status_code} for zip {zip_code}: {resp.text[:200]}")
                    for pf in pf_list:
                        pf.ai_notes = f"Apify error {resp.status_code}"
                        pf.last_scanned = dt.utcnow()
                    continue

                all_results = resp.json()
                logger.info(f"  Got {len(all_results)} Zillow listings for {zip_code}")

                for pf in pf_list:
                    street_parts = pf.address.lower().split()
                    matched = None
                    for item in all_results:
                        item_addr = (item.get("addressStreet") or item.get("address") or "").lower()
                        if len(street_parts) >= 2 and street_parts[0] in item_addr and street_parts[1] in item_addr:
                            matched = item
                            break

                    prev_status = pf.mls_status
                    pf.last_scanned = dt.utcnow()

                    if not matched:
                        pf.mls_status = "unknown"
                        pf.ai_notes = f"Not found on Zillow ({len(all_results)} listings in {zip_code})"
                    else:
                        home_info = matched.get("hdpData", {}).get("homeInfo", {})
                        home_status = (home_info.get("homeStatus") or matched.get("statusType") or "").upper()
                        price = home_info.get("price") or matched.get("unformattedPrice")
                        zestimate = home_info.get("zestimate") or matched.get("zestimate")

                        # Check if it's an auction listing (not a real MLS listing)
                        sub_type = home_info.get("listing_sub_type", {}) or {}
                        status_text = (matched.get("statusText") or "").upper()
                        raw_price = home_info.get("price") or matched.get("unformattedPrice")
                        is_auction = bool(
                            sub_type.get("is_forAuction")
                            or sub_type.get("is_foreclosure")
                            or sub_type.get("is_bankOwned")
                            or "AUCTION" in home_status
                            or "AUCTION" in status_text
                            or "FORECLOSED" in home_status
                            or (raw_price is not None and float(str(raw_price).replace("$","").replace(",","") or 0) == 0)
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

                        if price:
                            try: pf.mls_price = float(str(price).replace("$","").replace(",",""))
                            except: pass
                        if zestimate:
                            try: pf.estimated_value = float(zestimate)
                            except: pass

                        notes = [f"Zillow: {home_status}"]
                        if price: notes.append(f"${float(str(price).replace('$','').replace(',','')):,.0f}")
                        if zestimate: notes.append(f"Zest: ${float(zestimate):,.0f}")
                        pf.ai_notes = " | ".join(notes)

                    pf.is_new = (prev_status != "on-market" and pf.mls_status == "on-market")
                    if pf.is_new:
                        new_on_market.append(pf)
                        logger.info(f"  🚨 NEW ON MARKET: {pf.address}, {pf.city}")

                    scanned += 1

                # Commit after each zip to prevent connection timeout
                db.commit()

                import time
                time.sleep(3)  # Rate limit between zip codes

            except Exception as e:
                logger.error(f"  Error scanning zip {zip_code}: {e}")
                for pf in pf_list:
                    pf.ai_notes = f"Scan error: {str(e)[:100]}"
                    pf.last_scanned = dt.utcnow()

        # Save alert data before closing session
        alert_data = []
        for pf in new_on_market:
            alert_data.append({"address": pf.address, "city": pf.city, "mls_price": pf.mls_price, "estimated_value": pf.estimated_value})

        db.commit()
        db.close()

        logger.info(f"Pre-foreclosure scan complete: {scanned} scanned, {len(new_on_market)} new on market")

        # Send alert for newly listed properties
        if alert_data:
            try:
                from alerts import GMAIL_USER, GMAIL_PASSWORD, ALERT_EMAIL
                if GMAIL_USER and GMAIL_PASSWORD:
                    import smtplib
                    from email.mime.text import MIMEText
                    from email.mime.multipart import MIMEMultipart

                    html = "<html><body>"
                    html += "<h2 style='color:#22c55e;'>🚨 Pre-Foreclosure NOW ON MARKET!</h2>"
                    html += "<p>These pre-foreclosure properties just appeared on Zillow:</p>"
                    html += "<table border='1' cellpadding='8' style='border-collapse:collapse;'>"
                    html += "<tr style='background:#166534;color:white;'><th>Address</th><th>Price</th><th>Value</th></tr>"
                    for ad in alert_data:
                        price_str = f"${ad['mls_price']:,.0f}" if ad.get('mls_price') else "N/A"
                        val_str = f"${ad['estimated_value']:,.0f}" if ad.get('estimated_value') else "N/A"
                        html += f"<tr><td><b>{ad['address']}, {ad['city']}</b></td><td>{price_str}</td><td>{val_str}</td></tr>"
                    html += "</table></body></html>"

                    msg = MIMEMultipart("alternative")
                    msg["Subject"] = f"🚨 {len(alert_data)} Pre-Foreclosure Properties NOW ON MARKET!"
                    msg["From"] = GMAIL_USER
                    msg["To"] = ALERT_EMAIL or GMAIL_USER
                    msg.attach(MIMEText(html, "html"))

                    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                        server.login(GMAIL_USER, GMAIL_PASSWORD)
                        server.sendmail(GMAIL_USER, ALERT_EMAIL or GMAIL_USER, msg.as_string())
                    logger.info(f"Alert email sent for {len(alert_data)} new listings")
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")

    except Exception as e:
        logger.error(f"Pre-foreclosure scan failed: {e}", exc_info=True)


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

    # Pre-foreclosure scan daily at 2AM PST = 09:00 UTC
    schedule.every().day.at("09:00").do(run_preforeclosure_scan)

    logger.info("Scheduled (UTC times, Railway server):")
    logger.info("  - 09:00 UTC (2AM PST): Pre-foreclosure Zillow scan")
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
