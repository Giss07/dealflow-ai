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
    now_pst = datetime.now(PST)
    hour = now_pst.hour
    if hour < 9 or hour > 18:
        logger.info(f"Skipping Gmail check — outside 9AM-6PM PST (currently {hour}:00)")
        return
    logger.info(f"=== GMAIL-ONLY RUN started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")
    try:
        os.system(f"{sys.executable} {os.path.join(os.path.dirname(__file__), 'dealflow_updater.py')} gmail_only")
    except Exception as e:
        logger.error(f"Gmail-only run failed: {e}")


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

    # Schedule dealflow_updater
    schedule.every().day.at("08:00").do(run_full)           # 8AM PST full run
    schedule.every().hour.at(":00").do(run_gmail_only)      # Hourly Gmail check (9AM-6PM only)

    # Schedule DealFlow AI pipeline (Mon & Thu at 7AM)
    schedule.every().monday.at("07:00").do(run_dealflow_pipeline)
    schedule.every().thursday.at("07:00").do(run_dealflow_pipeline)

    logger.info("Scheduled:")
    logger.info("  - 8:00 AM PST daily: Full updater run")
    logger.info("  - Hourly 9AM-6PM PST: Gmail-only counter checks")
    logger.info("  - Mon & Thu 7AM PST: DealFlow AI scraper pipeline")

    # Run Gmail check immediately on startup
    logger.info("Running initial Gmail check...")
    run_gmail_only()

    # Loop
    while True:
        schedule.run_pending()
        time.sleep(30)
