"""
DealFlow Worker — Runs on Railway 24/7.
- 8AM PST: Full run (Gmail + Zillow + Alerts)
- 9AM-6PM PST hourly: Gmail-only (counter offer checks)
"""

from dotenv import load_dotenv
load_dotenv()

import os
import sys
import time
import logging
import schedule
import pytz
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PST = pytz.timezone("America/Los_Angeles")

# Add current dir to path so imports work
sys.path.insert(0, os.path.dirname(__file__))


def run_full():
    """Run dealflow_updater in full mode (every 2 days)."""
    now_pst = datetime.now(PST)
    # Only run on odd days (1st, 3rd, 5th, etc.) to achieve every-2-days
    if now_pst.day % 2 == 0:
        logger.info(f"Skipping full run — runs every 2 days (next run tomorrow)")
        return
    logger.info(f"=== FULL RUN started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")
    try:
        os.system(f"{sys.executable} {os.path.join(os.path.dirname(__file__), 'dealflow_updater.py')} full")
    except Exception as e:
        logger.error(f"Full run failed: {e}")


def run_gmail_only():
    """Run dealflow_updater in gmail_only mode (only during 9AM-6PM PST)."""
    now_pst = datetime.now(PST)  # Always use PST-aware time for the hour check
    hour = now_pst.hour
    if hour < 9 or hour > 18:
        logger.info(f"Skipping Gmail check — outside 9AM-6PM PST (currently {hour}:00)")
        return
    logger.info(f"=== GMAIL-ONLY RUN started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")
    try:
        os.system(f"{sys.executable} {os.path.join(os.path.dirname(__file__), 'dealflow_updater.py')} gmail_only")
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

                        if "FOR_SALE" in home_status:
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

    # Run Gmail check immediately on startup
    logger.info("Running initial Gmail check...")
    run_gmail_only()

    # Loop
    while True:
        schedule.run_pending()
        time.sleep(30)
