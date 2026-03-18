"""
DealFlow Main — Entry point that runs the full deal-finding pipeline.
"""

from dotenv import load_dotenv
load_dotenv()

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def run_full_pipeline():
    """Run: scrape → filter → analyze photos → score → ARV → repairs → offers → alerts → save."""
    from scraper import scrape_and_normalize
    from filter import filter_listings
    from photo_analyzer import analyze_all_photos
    from scorer import score_deals
    from arv_calculator import compute_arv_for_all
    from repair_estimator import estimate_all_repairs
    from offer_calculator import calculate_all_offers
    from alerts import send_alerts
    from database import init_db, save_deals, log_pipeline_run

    started_at = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("DealFlow Pipeline Started")
    logger.info("=" * 60)

    try:
        # Initialize database
        init_db()

        # Step 1: Scrape
        logger.info("[1/8] Scraping Zillow listings...")
        listings = scrape_and_normalize()
        logger.info(f"  → {len(listings)} raw listings")

        if not listings:
            logger.warning("No listings scraped, pipeline ending")
            log_pipeline_run(started_at, datetime.utcnow(), 0, 0, 0, 0, "empty")
            return []

        # Step 2: Filter
        logger.info("[2/8] Filtering listings...")
        filtered = filter_listings(listings)
        logger.info(f"  → {len(filtered)} pass filters")

        if not filtered:
            logger.warning("No listings passed filters")
            log_pipeline_run(started_at, datetime.utcnow(), len(listings), 0, 0, 0, "no_matches")
            return []

        # Sort by price (lowest first) to prioritize best flip candidates
        filtered.sort(key=lambda x: x.get("price", 999999999))

        # Step 3: Skip photo analysis (run manually per deal via analyze_deal.py or dashboard)
        logger.info("[3/8] Skipping photo analysis (run per-deal via dashboard or analyze_deal.py)")
        for listing in filtered:
            listing["photo_grades"] = {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
            listing["overall_condition"] = "Unknown"
        analyzed = filtered

        # Step 4: Score deals
        logger.info("[4/8] Scoring deals with AI...")
        scored = score_deals(analyzed)

        # Step 5: Calculate ARV
        logger.info("[5/8] Calculating ARV from comps...")
        with_arv = compute_arv_for_all(scored)

        # Step 6: Estimate repairs
        logger.info("[6/8] Estimating repair costs...")
        with_repairs = estimate_all_repairs(with_arv)

        # Step 7: Calculate offers
        logger.info("[7/8] Calculating max offers...")
        with_offers = calculate_all_offers(with_repairs)

        # Step 8: Send alerts
        logger.info("[8/8] Sending alerts for top deals...")
        alerts_sent = send_alerts(with_offers)

        # Save to database
        logger.info("Saving deals to database...")
        save_deals(with_offers)

        # Log pipeline run
        log_pipeline_run(
            started_at, datetime.utcnow(),
            len(listings), len(filtered), len(scored), alerts_sent, "success"
        )

        logger.info("=" * 60)
        logger.info(f"Pipeline Complete!")
        logger.info(f"  Scraped: {len(listings)}")
        logger.info(f"  Filtered: {len(filtered)}")
        logger.info(f"  Scored: {len(scored)}")
        logger.info(f"  Alerts Sent: {alerts_sent}")
        logger.info("=" * 60)

        return with_offers

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        log_pipeline_run(started_at, datetime.utcnow(), 0, 0, 0, 0, "error", str(e))
        raise


def run_reprocess():
    """Reprocess existing DB deals: re-filter, re-score, re-ARV, re-repairs, re-offers."""
    from filter import filter_listings
    from scorer import score_deals
    from arv_calculator import compute_arv_for_all
    from repair_estimator import estimate_all_repairs
    from offer_calculator import calculate_all_offers
    from alerts import send_alerts
    from database import init_db, get_session, Deal, deal_to_dict, save_deals, log_pipeline_run

    started_at = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("DealFlow REPROCESS — Using existing DB data")
    logger.info("=" * 60)

    try:
        init_db()

        # Try loading from cache file first, then fall back to DB
        import json, os
        cache_path = os.path.join(os.path.dirname(__file__), "scraped_cache.json")

        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                listings = json.load(f)
            logger.info(f"Loaded {len(listings)} deals from cache file")
        else:
            session = get_session()
            deals = session.query(Deal).all()
            session.close()
            if not deals:
                logger.warning("No deals in database or cache to reprocess")
                return []
            listings = [deal_to_dict(d) for d in deals]
            logger.info(f"Loaded {len(listings)} deals from database")

        # Re-filter with updated rules
        logger.info("[1/5] Re-filtering with updated rules...")
        filtered = filter_listings(listings)
        logger.info(f"  → {len(filtered)} pass filters (was {len(listings)})")

        if not filtered:
            logger.warning("No listings passed filters")
            return []

        filtered.sort(key=lambda x: x.get("price", 999999999))

        # Default photo grades
        for listing in filtered:
            if not listing.get("photo_grades") or all(v == "Unknown" for v in listing.get("photo_grades", {}).values()):
                listing["photo_grades"] = {z: "Unknown" for z in ["Roof","HVAC","Plumbing","Interior","Kitchen","Bath","Foundation"]}
                listing["overall_condition"] = "Unknown"

        # Re-score
        logger.info("[2/5] Re-scoring deals with AI...")
        scored = score_deals(filtered)

        # Re-calculate ARV
        logger.info("[3/5] Calculating ARV from comps...")
        with_arv = compute_arv_for_all(scored)

        # Re-estimate repairs
        logger.info("[4/5] Estimating repair costs...")
        with_repairs = estimate_all_repairs(with_arv)

        # Re-calculate offers
        logger.info("[5/5] Calculating max offers...")
        with_offers = calculate_all_offers(with_repairs)

        # Send alerts
        logger.info("Sending alerts for top deals...")
        alerts_sent = send_alerts(with_offers)

        # Save updated deals
        logger.info("Saving reprocessed deals to database...")
        save_deals(with_offers)

        log_pipeline_run(
            started_at, datetime.utcnow(),
            len(listings), len(filtered), len(scored), alerts_sent, "reprocess"
        )

        logger.info("=" * 60)
        logger.info(f"Reprocess Complete!")
        logger.info(f"  Loaded: {len(listings)}")
        logger.info(f"  Filtered: {len(filtered)}")
        logger.info(f"  Scored: {len(scored)}")
        logger.info(f"  Alerts Sent: {alerts_sent}")
        logger.info("=" * 60)

        return with_offers

    except Exception as e:
        logger.error(f"Reprocess failed: {e}", exc_info=True)
        log_pipeline_run(started_at, datetime.utcnow(), 0, 0, 0, 0, "error", str(e))
        raise


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if len(sys.argv) > 1 and sys.argv[1] == "reprocess":
        run_reprocess()
    else:
        run_full_pipeline()
