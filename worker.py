"""
DealFlow Worker — Runs on Railway 24/7.
- 8AM PST: Full run (Gmail + Zillow + Alerts)
- 9AM-6PM PST hourly: Gmail-only (counter offer checks)
- Pre-foreclosure MLS scan: manual only by default (MLS_AUTO_SCAN_ENABLED=true to schedule)
"""

import sys
import os

# Force unbuffered output so Railway sees logs immediately
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ['PYTHONUNBUFFERED'] = '1'

print("Worker script starting...", flush=True)

from dotenv import load_dotenv
load_dotenv()

import json
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

# ── API cost constants ────────────────────────────────────────────────
# Apify (measured from actual billing data, 2026-05-01):
COST_ZIP_SEARCH = 0.29
COST_DETAIL_SCRAPER = 0.003
# TODO: Replace the 70% not-found assumption with actual historical
# not-found rate computed from scan_error_count + last_scanned data
# once we accumulate enough scan history.
NOT_FOUND_RATE_ESTIMATE = 0.70

# OpenWeb Ninja Real-Time Zillow Data API:
#   $0.0025 per call, direct address lookup, 1-2s response time
#   Free tier: 100 requests/month — keep dev scans to <=5 properties
COST_OPENWEB_NINJA = 0.0025


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


def estimate_scan_cost(property_ids):
    """Estimate cost for scanning a set of properties.

    Uses OpenWeb Ninja pricing when USE_OPENWEB_NINJA=true (simple: N × $0.0025),
    otherwise Apify pricing with zip/detail split.
    """
    from database import init_db, get_session, PreForeclosure
    use_openweb = os.getenv("USE_OPENWEB_NINJA", "false").lower() == "true"
    init_db()
    db = get_session()
    try:
        properties = db.query(PreForeclosure).filter(PreForeclosure.id.in_(property_ids)).all()
        count = len(properties)

        if use_openweb:
            total = count * COST_OPENWEB_NINJA
            return {
                "property_count": count,
                "provider": "openweb_ninja",
                "estimated_calls": count,
                "cost_per_call": COST_OPENWEB_NINJA,
                "total_cost": round(total, 2),
            }

        # Apify path
        has_url = sum(1 for pf in properties if pf.zillow_url)
        zips = set(pf.zip_code for pf in properties
                   if pf.zip_code and pf.zip_code != "unknown" and not pf.zillow_url)
        zip_calls = len(zips)
        zip_search_properties = count - has_url
        # TODO: Replace NOT_FOUND_RATE_ESTIMATE with actual historical rate
        # from accumulated scan data once we have enough history.
        estimated_not_found = int(zip_search_properties * NOT_FOUND_RATE_ESTIMATE)
        detail_calls = estimated_not_found + has_url

        detail_enabled = os.getenv("MLS_DETAIL_FALLBACK_ENABLED", "true").lower() != "false"
        if not detail_enabled:
            detail_calls = has_url

        zip_cost = zip_calls * COST_ZIP_SEARCH
        detail_cost = detail_calls * COST_DETAIL_SCRAPER
        total_cost = zip_cost + detail_cost

        return {
            "property_count": count,
            "provider": "apify",
            "unique_zips": zip_calls,
            "properties_with_url": has_url,
            "estimated_zip_calls": zip_calls,
            "estimated_detail_calls": detail_calls,
            "zip_cost": round(zip_cost, 2),
            "detail_cost": round(detail_cost, 2),
            "total_cost": round(total_cost, 2),
            "detail_fallback_enabled": detail_enabled,
            "not_found_rate_assumption": NOT_FOUND_RATE_ESTIMATE,
        }
    finally:
        db.close()


def _update_job_progress(job_id, db, **kwargs):
    """Update a ScanJob row with progress data. No-op if job_id is None."""
    if not job_id:
        return
    from database import ScanJob
    job = db.query(ScanJob).filter_by(id=job_id).first()
    if job:
        for k, v in kwargs.items():
            setattr(job, k, v)
        db.commit()


def check_pending_scan_jobs():
    """Check for pending scan jobs and execute the oldest one.

    Called every iteration of the worker's main loop (~30s).
    Picks up one job at a time to avoid overloading Apify.
    """
    from database import init_db, get_session, ScanJob
    from datetime import datetime as dt, timedelta
    init_db()
    db = get_session()
    try:
        # Clean up stale running jobs (exceeded expires_at)
        stale = db.query(ScanJob).filter(
            ScanJob.status == "running",
            ScanJob.expires_at < dt.utcnow()
        ).all()
        for job in stale:
            logger.warning(f"Scan job {job.id} expired — marking failed")
            job.status = "failed"
            job.error_message = "Exceeded 2 hour time limit"
            job.completed_at = dt.utcnow()
        if stale:
            db.commit()

        # Pick up oldest pending job
        job = db.query(ScanJob).filter(
            ScanJob.status == "pending"
        ).order_by(ScanJob.created_at).first()

        if not job:
            return

        logger.info(f"Picking up scan job {job.id} ({job.total} properties)")
        job.status = "running"
        job.started_at = dt.utcnow()
        db.commit()

        property_ids = json.loads(job.property_ids)
        summary = run_preforeclosure_scan(property_ids=property_ids, job_id=job.id)

        # Update job with final results
        job = db.query(ScanJob).filter_by(id=job.id).first()
        if summary.get("error"):
            job.status = "failed"
            job.error_message = summary["error"]
        else:
            job.status = "completed"
            job.scanned = summary.get("scanned", 0)
            job.new_on_market = summary.get("new_on_market", 0)
            job.errors = summary.get("errors", 0)
            job.actual_cost = summary.get("actual_cost", 0)
            job.result = json.dumps(summary)
        job.completed_at = dt.utcnow()
        db.commit()
        logger.info(f"Scan job {job.id} completed: {job.status}")

    except Exception as e:
        logger.error(f"Error processing scan job: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()


def run_preforeclosure_scan(property_ids=None, job_id=None):
    """Scan pre-foreclosure properties on Zillow for MLS status changes.

    Args:
        property_ids: List of property IDs to scan. None = scan all (auto-scheduler).
        job_id: Optional ScanJob ID to update with progress during scan.

    Groups properties by zip code to minimize Apify calls.
    Detects Monitoring -> On Market transitions and sends email + sets is_new flag.
    After zip-search pass, runs detail-scraper fallback for "not found" properties.

    Returns dict with scan results summary.
    """
    now_pst = datetime.now(PST)
    logger.info(f"=== PRE-FORECLOSURE SCAN started at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")

    delay_seconds = int(os.getenv("MLS_DELAY_SECONDS", "3"))
    detail_fallback = os.getenv("MLS_DETAIL_FALLBACK_ENABLED", "true").lower() != "false"
    use_openweb = os.getenv("USE_OPENWEB_NINJA", "false").lower() == "true"
    OPENWEB_KEY = os.getenv("OPENWEB_NINJA_API_KEY", "")

    try:
        import requests as req
        from database import init_db, get_session, PreForeclosure
        from datetime import datetime as dt

        init_db()
        db = get_session()

        if property_ids:
            properties = db.query(PreForeclosure).filter(PreForeclosure.id.in_(property_ids)).all()
        else:
            properties = db.query(PreForeclosure).filter(
                (PreForeclosure.is_archived == False) | (PreForeclosure.is_archived == None)
            ).filter(PreForeclosure.mls_status != "offer-submitted").all()

        # ── OpenWeb Ninja path (feature flag) ──
        if use_openweb and OPENWEB_KEY:
            # FREE TIER SAFEGUARD: Basic plan = 100 req/month.
            # For development, keep scans to <=5 properties.
            if len(properties) > 10:
                logger.warning(f"OpenWeb Ninja: scanning {len(properties)} properties. "
                               f"Free tier is 100 req/month — consider using MLS_BATCH_SIZE or selecting fewer properties.")

            logger.info(f"Scanning {len(properties)} properties via OpenWeb Ninja (delay={delay_seconds}s)...")
            scanned = 0
            errors = 0
            apify_calls = 0  # reused field name for consistency in summary
            actual_cost = 0.0
            new_on_market = []

            for i, pf in enumerate(properties):
                try:
                    found = _scan_via_openweb_ninja(pf, OPENWEB_KEY, delay_seconds, new_on_market)
                    apify_calls += 1
                    actual_cost += COST_OPENWEB_NINJA
                    if found:
                        pf.scan_error_count = 0
                        pf.last_scan_error = None
                    scanned += 1
                except Exception as e:
                    logger.error(f"  Error processing {pf.address}: {e}")
                    pf.last_scanned = dt.utcnow()
                    pf.scan_error_count = (pf.scan_error_count or 0) + 1
                    pf.last_scan_error = f"{type(e).__name__}: {str(e)[:100]}"
                    errors += 1

                # Commit + update job progress every property
                db.commit()
                _update_job_progress(job_id, db, scanned=scanned, errors=errors,
                                     actual_cost=round(actual_cost, 4),
                                     new_on_market=len(new_on_market))
                if i < len(properties) - 1:
                    time.sleep(delay_seconds)

            alert_data = [{"address": pf.address, "city": pf.city,
                           "mls_price": pf.mls_price, "estimated_value": pf.estimated_value}
                          for pf in new_on_market]
            db.commit()
            db.close()

            summary = {
                "scanned": scanned, "errors": errors,
                "apify_calls": apify_calls, "actual_cost": round(actual_cost, 4),
                "new_on_market": len(new_on_market), "provider": "openweb_ninja",
            }
            logger.info(f"Pre-foreclosure scan complete: {summary}")
            if alert_data:
                _send_new_listing_alert(alert_data)
            return summary

        # ── Apify path (existing) ──
        APIFY_API_KEY = os.getenv("APIFY_API_KEY", "")
        if not APIFY_API_KEY:
            logger.error("No API key configured (APIFY_API_KEY or OPENWEB_NINJA_API_KEY)")
            return {"error": "No API key configured"}

        logger.info(f"Scanning {len(properties)} pre-foreclosure properties via Apify (delay={delay_seconds}s, detail_fallback={detail_fallback})...")

        # Group by zip code to minimize Apify calls
        by_zip = {}
        for pf in properties:
            z = pf.zip_code or "unknown"
            by_zip.setdefault(z, []).append(pf)

        scanned = 0
        errors = 0
        apify_calls = 0
        actual_cost = 0.0
        new_on_market = []
        not_found = []  # Properties that need detail-scraper fallback

        for zip_code, pf_list in by_zip.items():
            if zip_code == "unknown":
                for pf in pf_list:
                    pf.ai_notes = "No zip code — cannot search"
                    pf.last_scanned = dt.utcnow()
                    pf.scan_error_count = (pf.scan_error_count or 0) + 1
                    pf.last_scan_error = "No zip code"
                    errors += 1
                db.commit()
                continue

            # Separate properties with zillow_url (skip zip search, go to detail)
            url_props = [pf for pf in pf_list if pf.zillow_url]
            zip_props = [pf for pf in pf_list if not pf.zillow_url]

            # Properties with URLs go directly to the detail fallback list
            not_found.extend(url_props)

            if not zip_props:
                continue

            logger.info(f"  Searching zip {zip_code} ({len(zip_props)} properties)...")
            api_url = "https://api.apify.com/v2/acts/maxcopell~zillow-zip-search/run-sync-get-dataset-items"
            payload = {"zipCodes": [zip_code], "maxItems": 50}

            # Retry with backoff for Apify calls
            all_results = None
            for attempt in range(3):
                try:
                    apify_calls += 1
                    actual_cost += COST_ZIP_SEARCH
                    resp = req.post(api_url, params={"token": APIFY_API_KEY}, json=payload,
                                    headers={"Content-Type": "application/json"}, timeout=120)
                    if resp.status_code == 429:
                        wait = 2 ** (attempt + 1)
                        logger.warning(f"  Apify rate limited (429), waiting {wait}s")
                        time.sleep(wait)
                        continue
                    if resp.status_code not in (200, 201):
                        logger.error(f"  Apify error {resp.status_code} for zip {zip_code}: {resp.text[:200]}")
                        for pf in zip_props:
                            pf.last_scanned = dt.utcnow()
                            pf.scan_error_count = (pf.scan_error_count or 0) + 1
                            pf.last_scan_error = f"Apify HTTP {resp.status_code}"
                            errors += 1
                        break
                    all_results = resp.json()
                    break
                except req.exceptions.Timeout:
                    logger.warning(f"  Apify timeout attempt {attempt+1}/3 for zip {zip_code}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        for pf in zip_props:
                            pf.last_scanned = dt.utcnow()
                            pf.scan_error_count = (pf.scan_error_count or 0) + 1
                            pf.last_scan_error = "Apify timeout after 3 attempts"
                            errors += 1
                except req.exceptions.ConnectionError as e:
                    logger.warning(f"  Apify connection error attempt {attempt+1}/3: {e}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        for pf in zip_props:
                            pf.last_scanned = dt.utcnow()
                            pf.scan_error_count = (pf.scan_error_count or 0) + 1
                            pf.last_scan_error = f"Connection error: {str(e)[:100]}"
                            errors += 1

            if all_results is None:
                db.commit()
                continue

            logger.info(f"  Got {len(all_results)} Zillow listings for {zip_code}")

            for pf in zip_props:
                try:
                    found = _scan_single_property(pf, all_results, zip_code, new_on_market)
                    if found:
                        # Matched in zip search — clear error tracking
                        pf.scan_error_count = 0
                        pf.last_scan_error = None
                    else:
                        # Not found in zip search — queue for detail fallback
                        not_found.append(pf)
                    scanned += 1
                except Exception as e:
                    logger.error(f"  Error processing {pf.address}: {e}")
                    pf.last_scanned = dt.utcnow()
                    pf.scan_error_count = (pf.scan_error_count or 0) + 1
                    pf.last_scan_error = f"{type(e).__name__}: {str(e)[:100]}"
                    errors += 1

            db.commit()
            _update_job_progress(job_id, db, scanned=scanned, errors=errors,
                                 actual_cost=round(actual_cost, 2),
                                 new_on_market=len(new_on_market))
            time.sleep(delay_seconds)

        # ── Pass 2: Detail-scraper fallback for "not found" properties ──
        if not_found and detail_fallback:
            logger.info(f"  Detail fallback: {len(not_found)} properties to check...")
            detail_results = _detail_fallback_pass(
                not_found, db, APIFY_API_KEY, delay_seconds, new_on_market
            )
            apify_calls += detail_results["calls"]
            actual_cost += detail_results["calls"] * COST_DETAIL_SCRAPER
            scanned += detail_results["scanned"]
            errors += detail_results["errors"]
        elif not_found:
            logger.info(f"  Detail fallback disabled, {len(not_found)} properties unchecked")
            # Still update last_scanned for these — don't increment error count
            from datetime import datetime as dt
            for pf in not_found:
                pf.last_scanned = dt.utcnow()
                # Don't touch scan_error_count — clean not-found is not an error
            db.commit()

        # Save alert data before closing session
        alert_data = []
        for pf in new_on_market:
            alert_data.append({"address": pf.address, "city": pf.city,
                               "mls_price": pf.mls_price, "estimated_value": pf.estimated_value})

        db.commit()
        db.close()

        summary = {
            "scanned": scanned,
            "errors": errors,
            "apify_calls": apify_calls,
            "actual_cost": round(actual_cost, 2),
            "new_on_market": len(new_on_market),
            "detail_fallback_checked": len(not_found) if detail_fallback else 0,
        }
        logger.info(f"Pre-foreclosure scan complete: {summary}")

        # Send alert for newly listed properties
        if alert_data:
            _send_new_listing_alert(alert_data)

        return summary

    except Exception as e:
        logger.error(f"Pre-foreclosure scan failed: {e}", exc_info=True)
        return {"error": str(e)}


def _scan_single_property(pf, all_results, zip_code, new_on_market):
    """Process a single pre-foreclosure property against Zillow zip-search results.

    Returns True if matched, False if not found (needs detail fallback).
    Does NOT auto-populate estimated_value.
    Does NOT increment scan_error_count on clean not-found.
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
        # Not found in zip search — don't update status yet, detail fallback will handle it
        pf.is_new = False  # Clear stale new flag
        return False

    _apply_zillow_match(pf, matched, prev_status, new_on_market)
    return True


def _parse_date_safe(value):
    """Parse a date string to datetime. Returns None if unparseable.

    Handles: "2026-05-01", "2026-05-01T12:00:00", "05/01/2026", epoch ints.
    All date fields from OpenWeb Ninja should go through this — store NULL
    rather than unparseable strings.
    """
    if not value:
        return None
    from datetime import datetime as dt
    if isinstance(value, (int, float)):
        try:
            return dt.utcfromtimestamp(value / 1000 if value > 1e12 else value)
        except (ValueError, OSError):
            return None
    value = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
                "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return dt.strptime(value, fmt)
        except ValueError:
            continue
    logger.warning(f"Could not parse date: {value!r}")
    return None


def _scan_via_openweb_ninja(pf, api_key, delay_seconds, new_on_market):
    """Scan a single property via OpenWeb Ninja Real-Time Zillow Data API.

    Direct address lookup — no zip grouping, no address matching needed.
    Populates MLS status, foreclosure data, and property details.
    Returns True if data found, False if not found.
    Does NOT increment scan_error_count on clean not-found.

    FREE TIER WARNING: Basic plan has only 100 requests/month.
    Keep dev/test scans to <=5 properties to preserve quota.
    """
    import requests as req
    from datetime import datetime as dt

    full_addr = f"{pf.address}, {pf.city}, {pf.state or 'CA'} {pf.zip_code or ''}".strip()
    logger.info(f"  OpenWeb [{pf.id}] {full_addr}")

    prev_status = pf.mls_status
    pf.previous_mls_status = prev_status
    pf.last_scanned = dt.utcnow()

    # Retry with backoff
    result_data = None
    lookup_error = None
    for attempt in range(3):
        try:
            resp = req.get(
                "https://api.openwebninja.com/realtime-zillow-data/property-details-address",
                params={"address": full_addr},
                headers={"x-api-key": api_key},
                timeout=30,
            )
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning(f"  OpenWeb rate limited (429), waiting {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                # Property not found — clean not-found, not an error
                break
            if resp.status_code not in (200, 201):
                lookup_error = f"OpenWeb HTTP {resp.status_code}"
                break

            body = resp.json()
            data = body.get("data") or body
            if not data or not data.get("zpid"):
                # Empty/invalid response — treat as not found
                break
            result_data = data
            break

        except req.exceptions.Timeout:
            lookup_error = "OpenWeb timeout"
            logger.warning(f"  OpenWeb timeout attempt {attempt+1}/3 for {pf.address}")
            if attempt < 2:
                time.sleep(2 ** attempt)
        except req.exceptions.ConnectionError as e:
            lookup_error = f"Connection error: {str(e)[:80]}"
            if attempt < 2:
                time.sleep(2 ** attempt)
        except (ValueError, KeyError) as e:
            lookup_error = f"Response parse error: {str(e)[:80]}"
            break

    if lookup_error:
        # Real failure — increment error count
        pf.scan_error_count = (pf.scan_error_count or 0) + 1
        pf.last_scan_error = lookup_error
        pf.ai_notes = f"OpenWeb lookup failed: {lookup_error}"
        logger.error(f"  OpenWeb FAILED for {pf.address}: {lookup_error}")
        return False

    if not result_data:
        # Clean not-found — property not on Zillow. NOT an error.
        pf.mls_status = "unknown"
        pf.ai_notes = "Not found on Zillow (OpenWeb Ninja)"
        pf.is_new = False
        return False

    # ── Map response fields to PreForeclosure model ──

    # MLS status
    home_status = (result_data.get("homeStatus") or "").upper()
    if "FOR_SALE" in home_status:
        pf.mls_status = "on-market"
    elif "PENDING" in home_status:
        pf.mls_status = "on-market"
    elif "SOLD" in home_status or "RECENTLY_SOLD" in home_status:
        pf.mls_status = "unknown"
    elif "OFF_MARKET" in home_status:
        pf.mls_status = "unknown"
    elif "FORECLOSURE" in home_status or "PRE_FORECLOSURE" in home_status:
        pf.mls_status = "pre-foreclosure"
    else:
        pf.mls_status = "unknown"

    # Price
    price = result_data.get("price")
    if price:
        try:
            pf.mls_price = float(price)
        except (ValueError, TypeError):
            pass

    # Zillow URL (auto-populate from hdpUrl)
    hdp_url = result_data.get("hdpUrl")
    if hdp_url and not pf.zillow_url:
        pf.zillow_url = f"https://www.zillow.com{hdp_url}" if hdp_url.startswith("/") else hdp_url

    # Foreclosure data
    pf.foreclosing_bank = result_data.get("foreclosingBank") or pf.foreclosing_bank
    pf.foreclosure_default_description = result_data.get("foreclosureDefaultDescription") or pf.foreclosure_default_description
    pf.foreclosure_default_filing_date = _parse_date_safe(result_data.get("foreclosureDefaultFilingDate")) or pf.foreclosure_default_filing_date
    pf.foreclosure_auction_filing_date = _parse_date_safe(result_data.get("foreclosureAuctionFilingDate")) or pf.foreclosure_auction_filing_date
    pf.foreclosure_auction_city = result_data.get("foreclosureAuctionCity") or pf.foreclosure_auction_city
    pf.foreclosure_auction_location = result_data.get("foreclosureAuctionLocation") or pf.foreclosure_auction_location
    pf.foreclosure_auction_time = _parse_date_safe(result_data.get("foreclosureAuctionTime")) or pf.foreclosure_auction_time
    pf.foreclosure_unpaid_balance = _safe_float(result_data.get("foreclosureUnpaidBalance")) or pf.foreclosure_unpaid_balance
    pf.foreclosure_past_due_balance = _safe_float(result_data.get("foreclosurePastDueBalance")) or pf.foreclosure_past_due_balance
    pf.foreclosure_loan_amount = _safe_float(result_data.get("foreclosureLoanAmount")) or pf.foreclosure_loan_amount
    pf.foreclosure_loan_originator = result_data.get("foreclosureLoanOriginator") or pf.foreclosure_loan_originator
    pf.foreclosure_loan_date = _parse_date_safe(result_data.get("foreclosureLoanDate")) or pf.foreclosure_loan_date
    pf.foreclosure_judicial_type = result_data.get("foreclosureJudicialType") or pf.foreclosure_judicial_type

    # Property/listing data
    pf.last_sold_price = _safe_float(result_data.get("lastSoldPrice")) or pf.last_sold_price
    pf.year_built = result_data.get("yearBuilt") if isinstance(result_data.get("yearBuilt"), int) else pf.year_built
    pf.listing_type_dimension = result_data.get("listingTypeDimension") or pf.listing_type_dimension
    pf.price_change = _safe_float(result_data.get("priceChange")) or pf.price_change
    pf.price_change_date = _parse_date_safe(result_data.get("priceChangeDateString")) or pf.price_change_date
    pf.days_on_zillow = result_data.get("daysOnZillow") if isinstance(result_data.get("daysOnZillow"), int) else pf.days_on_zillow

    # Build notes
    notes = [f"Zillow: {home_status}"]
    if price:
        notes.append(f"${float(price):,.0f}")
    zest = result_data.get("zestimate")
    if zest:
        notes.append(f"Zest: ${float(zest):,.0f}")
    if result_data.get("foreclosingBank"):
        notes.append(f"Bank: {result_data['foreclosingBank']}")
    pf.ai_notes = " | ".join(notes)

    # Clear error tracking on success
    pf.scan_error_count = 0
    pf.last_scan_error = None

    # Detect Monitoring -> On Market transition
    is_new_listing = (prev_status != "on-market" and pf.mls_status == "on-market")
    pf.is_new = is_new_listing
    if is_new_listing:
        pf.listed_at = dt.utcnow()
        new_on_market.append(pf)
        logger.info(f"  NEW ON MARKET: {pf.address}, {pf.city}")

    return True


def _safe_float(value):
    """Convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _apply_zillow_match(pf, matched, prev_status, new_on_market):
    """Apply a Zillow match result to a PreForeclosure record.

    Shared by both zip-search and detail-scraper paths.
    """
    from datetime import datetime as dt

    home_info = matched.get("hdpData", {}).get("homeInfo", {})
    home_status = (home_info.get("homeStatus") or matched.get("homeStatus")
                   or matched.get("statusType") or "").upper()
    price = home_info.get("price") or matched.get("price") or matched.get("unformattedPrice")
    zestimate = home_info.get("zestimate") or matched.get("zestimate")

    # Check if it's an auction listing
    status_text = (matched.get("statusText") or "").upper()
    raw_price = price
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
    elif "FOR_SALE" in home_status or "ACTIVE" in home_status:
        pf.mls_status = "on-market"
    elif "PENDING" in home_status or "OTHER" in home_status or "UNDER_CONTRACT" in home_status:
        pf.mls_status = "on-market"
    elif "FORECLOSURE" in home_status or "PRE_FORECLOSURE" in home_status:
        pf.mls_status = "pre-foreclosure"
    elif "SOLD" in home_status or "RECENTLY_SOLD" in home_status:
        pf.mls_status = "unknown"
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

    # Detect Monitoring -> On Market transition
    is_new_listing = (prev_status != "on-market" and pf.mls_status == "on-market")
    pf.is_new = is_new_listing
    if is_new_listing:
        pf.listed_at = dt.utcnow()
        new_on_market.append(pf)
        logger.info(f"  NEW ON MARKET: {pf.address}, {pf.city}")


def _detail_fallback_pass(properties, db, apify_api_key, delay_seconds, new_on_market):
    """Second pass: use zillow-detail-scraper for properties not found in zip search.

    Uses zillow_url if set, otherwise builds a search URL from the address.
    Only increments scan_error_count on actual lookup failures, NOT on clean not-found.
    """
    import requests as req
    import re
    from datetime import datetime as dt

    DETAIL_API = "https://api.apify.com/v2/acts/maxcopell~zillow-detail-scraper/run-sync-get-dataset-items"
    calls = 0
    scanned = 0
    errors = 0

    for i, pf in enumerate(properties):
        # Build the lookup input
        # If zillow_url is set, use startUrls (direct property page).
        # Otherwise use the addresses field — the actor resolves the
        # address to a ZPID internally. The old /homes/_rb/ search URL
        # format no longer works with this actor.
        use_url = bool(pf.zillow_url)
        if use_url:
            logger.info(f"  Detail [{i+1}/{len(properties)}] {pf.address} (using saved URL)")
        else:
            lookup_addr = f"{pf.address}, {pf.city}, {pf.state or 'CA'} {pf.zip_code or ''}"
            logger.info(f"  Detail [{i+1}/{len(properties)}] {pf.address} -> addresses lookup")

        prev_status = pf.mls_status
        pf.previous_mls_status = prev_status
        pf.last_scanned = dt.utcnow()

        # Retry with backoff
        result_item = None
        lookup_error = None
        for attempt in range(3):
            try:
                calls += 1
                if use_url:
                    payload = {
                        "startUrls": [{"url": pf.zillow_url}],
                        "maxItems": 1,
                        "proxyConfiguration": {
                            "useApifyProxy": True,
                            "apifyProxyGroups": ["BUYPROXIES94952"]
                        }
                    }
                else:
                    payload = {
                        "addresses": [lookup_addr],
                        "maxItems": 1,
                        "proxyConfiguration": {
                            "useApifyProxy": True,
                            "apifyProxyGroups": ["BUYPROXIES94952"]
                        }
                    }
                resp = req.post(DETAIL_API, params={"token": apify_api_key},
                                json=payload, headers={"Content-Type": "application/json"},
                                timeout=120)

                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"  Detail scraper rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue

                if resp.status_code not in (200, 201):
                    lookup_error = f"Detail scraper HTTP {resp.status_code}"
                    break

                data = resp.json()
                if data and isinstance(data, list) and len(data) > 0:
                    result_item = data[0]
                break

            except req.exceptions.Timeout:
                lookup_error = "Detail scraper timeout"
                logger.warning(f"  Detail timeout attempt {attempt+1}/3 for {pf.address}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
            except req.exceptions.ConnectionError as e:
                lookup_error = f"Connection error: {str(e)[:80]}"
                if attempt < 2:
                    time.sleep(2 ** attempt)

        if lookup_error:
            # Actual failure — increment error count
            pf.scan_error_count = (pf.scan_error_count or 0) + 1
            pf.last_scan_error = lookup_error
            pf.mls_status = "unknown"
            pf.ai_notes = f"Detail lookup failed: {lookup_error}"
            errors += 1
        elif result_item:
            # Found via detail scraper
            _apply_zillow_match(pf, result_item, prev_status, new_on_market)
            pf.scan_error_count = 0
            pf.last_scan_error = None
            scanned += 1
        else:
            # Clean not-found: scraper ran fine but property isn't listed
            # Do NOT increment scan_error_count — this is normal for pre-foreclosures
            pf.mls_status = "unknown"
            pf.ai_notes = "Not found on Zillow (zip search + detail scraper)"
            pf.is_new = False
            scanned += 1

        # Commit in batches of 10
        if (i + 1) % 10 == 0:
            db.commit()

        time.sleep(delay_seconds)

    db.commit()
    logger.info(f"  Detail fallback complete: {scanned} scanned, {errors} errors, {calls} Apify calls")
    return {"scanned": scanned, "errors": errors, "calls": calls}


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

    # Pre-foreclosure MLS scan — disabled by default, manual-only via dashboard
    # Set MLS_AUTO_SCAN_ENABLED=true to enable automatic every-3-day scanning
    mls_auto = os.getenv("MLS_AUTO_SCAN_ENABLED", "false").lower() == "true"
    if mls_auto:
        schedule.every(3).days.at("09:00").do(run_preforeclosure_scan)

    logger.info("Scheduled (UTC times, Railway server):")
    logger.info(f"  - Pre-foreclosure MLS scan: {'ENABLED every 3 days 09:00 UTC' if mls_auto else 'MANUAL ONLY (MLS_AUTO_SCAN_ENABLED=false)'}")
    logger.info("  - 15:00 UTC (8AM PST): Full updater run")
    logger.info("  - Hourly (9AM-6PM PST): Gmail-only counter checks")
    logger.info("  - PAUSED: Mon & Thu DealFlow AI scraper pipeline")

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
        try:
            check_pending_scan_jobs()
        except Exception as e:
            logger.error(f"Scan job check error: {e}")
        time.sleep(5)
