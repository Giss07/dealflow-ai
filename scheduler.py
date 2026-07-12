"""
DealFlow Scheduler — Run full pipeline at 7am PST on Monday and Thursday.
"""

import logging
import schedule
import time
import pytz
from datetime import datetime

logger = logging.getLogger(__name__)

PST = pytz.timezone("America/Los_Angeles")


def run_pipeline():
    """Import and run the main pipeline."""
    from main import run_full_pipeline
    logger.info(f"Scheduler triggered pipeline at {datetime.now(PST).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    run_full_pipeline()


def start_scheduler():
    """Start the scheduler to run at 7am PST on Monday and Thursday."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("DealFlow Scheduler started — pipeline runs Mon & Thu at 7:00 AM PST")

    schedule.every().monday.at("07:00").do(run_pipeline)
    schedule.every().thursday.at("07:00").do(run_pipeline)

    # Run immediately on first start
    logger.info("Running initial pipeline...")
    run_pipeline()

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    start_scheduler()
