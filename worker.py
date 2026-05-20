"""
DealFlow Worker — Runs on Railway 24/7.
- 8AM PST: Full run (Gmail + Zillow + Alerts)
- 6AM-7PM PT hourly: Gmail-only (counter offer checks)
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
# OpenWeb Ninja Real-Time Zillow Data API:
#   $0.0025 per call, direct address lookup, 1-2s response time
#   Pro tier: 10,000 requests/month
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
    """Run dealflow_updater in gmail_only mode (only during 6AM-7PM PT)."""
    now_pst = datetime.now(PST)
    hour = now_pst.hour
    if hour < 6 or hour > 19:
        logger.info(f"Skipping Gmail check — outside 6AM-7PM PT (currently {hour}:00)")
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
    """Estimate cost for scanning a set of properties via OpenWeb Ninja."""
    from database import init_db, get_session, PreForeclosure
    init_db()
    db = get_session()
    try:
        count = db.query(PreForeclosure).filter(PreForeclosure.id.in_(property_ids)).count()
        total = count * COST_OPENWEB_NINJA
        return {
            "property_count": count,
            "provider": "openweb_ninja",
            "estimated_calls": count,
            "cost_per_call": COST_OPENWEB_NINJA,
            "total_cost": round(total, 2),
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

        pickup_delay = (dt.utcnow() - job.created_at).total_seconds() if job.created_at else 0
        logger.info(f"[SCAN_JOB_PICKUP] id={job.id} created_at={job.created_at.isoformat() if job.created_at else '?'} pickup_at={dt.utcnow().isoformat()} pickup_delay_seconds={pickup_delay:.1f}")
        job.status = "running"
        job.started_at = dt.utcnow()
        db.commit()

        property_ids = json.loads(job.property_ids)
        summary = run_preforeclosure_scan(property_ids=property_ids, job_id=job.id)

        # Mark job complete FIRST — so polling sees completed immediately
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

        # Send alerts AFTER job is marked complete — don't block UX
        alert_data = summary.get("_alert_data", [])
        if alert_data:
            try:
                _send_new_listing_alert(alert_data)
            except Exception as e:
                logger.error(f"Alert email failed (job already marked complete): {e}")

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

    delay_seconds = int(os.getenv("MLS_DELAY_SECONDS", "1"))
    OPENWEB_KEY = os.getenv("OPENWEB_NINJA_API_KEY", "")

    if not OPENWEB_KEY:
        logger.error("OPENWEB_NINJA_API_KEY not set — cannot scan")
        return {"error": "OPENWEB_NINJA_API_KEY not configured"}

    try:
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

        logger.info(f"Scanning {len(properties)} properties via OpenWeb Ninja (delay={delay_seconds}s)...")
        scanned = 0
        errors = 0
        api_calls = 0
        actual_cost = 0.0
        new_on_market = []

        progress_interval = max(5, len(properties) // 4)  # Update progress at most every 5 properties
        for i, pf in enumerate(properties):
            try:
                found = _scan_via_openweb_ninja(pf, OPENWEB_KEY, delay_seconds, new_on_market)
                api_calls += 1
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

            # Batch commit every 5 properties or at the end
            if (i + 1) % 5 == 0 or i == len(properties) - 1:
                db.commit()

            # Progress update only at 25% intervals or at the end
            if (i + 1) % progress_interval == 0 or i == len(properties) - 1:
                _update_job_progress(job_id, db, scanned=scanned, errors=errors,
                                     actual_cost=round(actual_cost, 4),
                                     new_on_market=len(new_on_market))

            if i < len(properties) - 1:
                time.sleep(delay_seconds)

        # Collect alert data before returning — caller will send alerts AFTER marking job complete
        alert_data = [{"address": pf.address, "city": pf.city,
                       "mls_price": pf.mls_price, "estimated_value": pf.estimated_value}
                      for pf in new_on_market]

        summary = {
            "scanned": scanned, "errors": errors,
            "api_calls": api_calls, "actual_cost": round(actual_cost, 4),
            "new_on_market": len(new_on_market), "provider": "openweb_ninja",
            "_alert_data": alert_data,  # Passed to caller for post-completion alerting
        }
        from datetime import datetime as dt2
        duration = (dt2.utcnow() - now_pst.astimezone(pytz.utc).replace(tzinfo=None)).total_seconds()
        logger.info(f"[SCAN_JOB_COMPLETE] job_id={job_id} duration_seconds={duration:.1f} scanned={scanned} errors={errors} new_on_market={len(new_on_market)}")
        return summary

    except Exception as e:
        logger.error(f"Pre-foreclosure scan failed: {e}", exc_info=True)
        return {"error": str(e)}


def _extract_unit(address):
    """Extract unit/apt number from an address. Returns (street, unit) tuple.

    Examples:
        "2001 Club Center Dr APT 8126" → ("2001 club center dr", "8126")
        "123 Main St" → ("123 main st", None)
        "456 Oak Ave Unit 5B" → ("456 oak ave", "5b")
    """
    if not address:
        return ("", None)
    import re
    addr = address.strip().lower()
    m = re.search(r'\s*(?:apt|unit|ste|suite|#)\s*\.?\s*(\S+)\s*$', addr, re.IGNORECASE)
    if m:
        unit = m.group(1)
        street = addr[:m.start()].strip()
        return (street, unit)
    return (addr, None)


def _verify_address_match(queried, returned):
    """Verify API-returned address matches what we queried.

    Returns (accept, unit_verified) tuple:
      (True, True)  — clean match, unit verified or not applicable
      (True, False) — accepted but unit unverified (query had no unit, response had one)
      (False, False) — rejected (different units)
    """
    q_street, q_unit = _extract_unit(queried.split(',')[0] if ',' in queried else queried)
    r_street, r_unit = _extract_unit(returned)

    if q_unit and r_unit and q_unit != r_unit:
        logger.warning(f"[ADDRESS_MISMATCH] Queried '{queried}' but API returned '{returned}' — different unit, skipping")
        return (False, False)
    if not q_unit and r_unit:
        logger.info(f"[UNIT_UNVERIFIED] Queried '{queried}' (no unit), API returned '{returned}' (unit {r_unit}) — accepting, needs manual verification")
        return (True, False)
    return (True, True)


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
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
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
                if resp.status_code == 200 and body.get("status") == "OK" and not body.get("error"):
                    logger.warning(f"[API_EMPTY_RESPONSE] address='{full_addr}' request_id='{body.get('request_id', 'unknown')}' — endpoint returned empty data, treating as unknown")
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

    # Verify returned address matches queried address (multi-unit protection)
    returned_addr = result_data.get("streetAddress") or result_data.get("address") or ""
    accept, unit_verified = _verify_address_match(full_addr, returned_addr)
    if not accept:
        pf.mls_status = "unknown"
        pf.ai_notes = f"Address mismatch: queried '{pf.address}', API returned '{returned_addr}'"
        pf.is_new = False
        return False
    pf.unit_verified = unit_verified

    # ── Map response fields to PreForeclosure model ──

    # MLS status — safe-default logic:
    # "on-market" ONLY when agent-listed MLS, NOT auction.
    # Default to "pre-foreclosure" or "auction" when ambiguous.
    home_status = (result_data.get("homeStatus") or "").upper()
    listing_type = (result_data.get("listingTypeDimension") or "").lower()
    listing_source = (result_data.get("listingDataSource") or "").lower()
    is_agent_listed = "by agent" in listing_type
    is_auction_source = "auction" in listing_source

    # Check for contingent/under-contract FIRST (overrides homeStatus)
    contingent = (result_data.get("contingentListingType") or "").upper()
    is_pending = ("PENDING" in home_status or "UNDER_CONTRACT" in home_status
                  or "CONTINGENT" in contingent or "UNDER_CONTRACT" in contingent)

    if "SOLD" in home_status or "RECENTLY_SOLD" in home_status:
        pf.mls_status = "unknown"
    elif is_pending:
        pf.mls_status = "pending"
    elif "FOR_SALE" in home_status:
        if is_agent_listed and not is_auction_source:
            pf.mls_status = "on-market"
        else:
            pf.mls_status = "auction"
    elif "FORECLOSURE" in home_status or "PRE_FORECLOSURE" in home_status:
        pf.mls_status = "pre-foreclosure"
    elif "OFF_MARKET" in home_status or "OTHER" in home_status:
        pf.mls_status = "unknown"
    else:
        pf.mls_status = "pre-foreclosure"  # Default: safer than on-market

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


def check_upcoming_auctions():
    """Daily cron: send digest email for properties with auctions in next 7 days.

    Priority filter:
      - mute: always excluded
      - watch: always included (bypass MLS check)
      - auto: included only if MLS-verified (last_scanned set + listing_type_dimension contains 'by Agent')

    Runs at 8AM Pacific (15:00 UTC). Skips properties already notified.
    """
    now_pst = datetime.now(PST)
    logger.info(f"=== CHECK UPCOMING AUCTIONS at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")

    try:
        from database import init_db, get_session, PreForeclosure
        from notifications import send_auction_digest, log_notification, was_notification_sent
        from datetime import datetime as dt, timedelta

        init_db()
        db = get_session()

        now = dt.utcnow()
        window_end = now + timedelta(days=7)

        # Get all Auction-stage properties with auction in next 7 days
        candidates = db.query(PreForeclosure).filter(
            PreForeclosure.foreclosure_stage == "Auction",
            PreForeclosure.foreclosure_auction_time > now,
            PreForeclosure.foreclosure_auction_time <= window_end,
            (PreForeclosure.is_archived == False) | (PreForeclosure.is_archived == None),
        ).all()

        total_in_window = len(candidates)

        # Apply priority filter
        muted = 0
        skipped_non_mls = 0
        already_notified = 0
        watch_count = 0
        auto_mls_count = 0
        qualify = []

        for pf in candidates:
            priority = pf.notification_priority or "auto"

            if priority == "mute":
                muted += 1
                continue

            if priority == "watch":
                # Check if already notified
                if was_notification_sent(db, pf.id, "auction_warning_7d"):
                    already_notified += 1
                    continue
                watch_count += 1
                qualify.append(pf)
            else:
                # auto: must be MLS-verified
                listing_type = (pf.listing_type_dimension or "").lower()
                if pf.last_scanned and "by agent" in listing_type:
                    if was_notification_sent(db, pf.id, "auction_warning_7d"):
                        already_notified += 1
                        continue
                    auto_mls_count += 1
                    qualify.append(pf)
                else:
                    skipped_non_mls += 1

        logger.info(f"  Found {total_in_window} properties with auction in next 7 days")
        logger.info(f"  After priority filter: {len(qualify)} qualify ({watch_count} watched, {auto_mls_count} auto+MLS)")
        logger.info(f"  Muted: {muted}, skipped non-MLS: {skipped_non_mls}, already notified: {already_notified}")

        if not qualify:
            logger.info("  No properties qualify for digest — skipping email")
            db.close()
            return

        # Sort by auction time ascending (soonest first)
        qualify.sort(key=lambda pf: pf.foreclosure_auction_time)

        # Send digest
        subject = f"Auction alert: {len(qualify)} propert{'y' if len(qualify)==1 else 'ies'} with auctions in next 7 days"
        sent = send_auction_digest(qualify)

        # Log notification for each property
        for pf in qualify:
            log_notification(db, pf.id, "auction_warning_7d", subject, sent,
                             None if sent else "Digest email send failed")

        logger.info(f"  Sent digest with {len(qualify)} properties: {'OK' if sent else 'FAILED'}")
        db.close()

    except Exception as e:
        logger.error(f"check_upcoming_auctions failed: {e}", exc_info=True)


def rescan_nod_properties():
    """Every-3-day cron: rescan NOD properties via OpenWeb Ninja to detect new auction dates.

    When foreclosureAuctionTime newly appears in API response:
      - Sets foreclosure_stage='Auction'
      - Sends immediate auction_scheduled email
      - Logs notification

    Config: RESCAN_BATCH_LIMIT env var (default 500).
    Requires OPENWEB_NINJA_API_KEY set.
    """
    now_pst = datetime.now(PST)
    logger.info(f"=== RESCAN NOD PROPERTIES at {now_pst.strftime('%Y-%m-%d %H:%M %Z')} ===")

    OPENWEB_KEY = os.getenv("OPENWEB_NINJA_API_KEY", "")
    if not OPENWEB_KEY:
        logger.error("OPENWEB_NINJA_API_KEY not set — aborting NOD rescan")
        return

    batch_limit = int(os.getenv("RESCAN_BATCH_LIMIT", "500"))

    try:
        from database import init_db, get_session, PreForeclosure
        from notifications import send_auction_scheduled, log_notification
        from datetime import datetime as dt, timedelta

        init_db()
        db = get_session()

        # Properties: NOD stage, not manually overridden, not scanned in last 3 days
        cutoff = dt.utcnow() - timedelta(days=3)
        properties = db.query(PreForeclosure).filter(
            PreForeclosure.foreclosure_stage == "NOD",
            (PreForeclosure.foreclosure_stage_manual_override == False) | (PreForeclosure.foreclosure_stage_manual_override == None),
            (PreForeclosure.is_archived == False) | (PreForeclosure.is_archived == None),
        ).filter(
            (PreForeclosure.last_scanned == None) | (PreForeclosure.last_scanned < cutoff)
        ).limit(batch_limit).all()

        logger.info(f"  Found {len(properties)} NOD properties to rescan (limit={batch_limit})")

        scanned = 0
        moved_to_auction = 0
        errors = 0
        rate_limited = 0

        for i, pf in enumerate(properties):
            prev_auction_time = pf.foreclosure_auction_time

            try:
                new_on_market = []  # Not used for NOD rescan but required by function signature
                found = _scan_via_openweb_ninja(pf, OPENWEB_KEY, 0, new_on_market)

                if found:
                    pf.scan_error_count = 0
                    pf.last_scan_error = None

                    # Detect new auction date: was NULL, now set
                    if pf.foreclosure_auction_time and not prev_auction_time:
                        pf.foreclosure_stage = "Auction"
                        moved_to_auction += 1
                        logger.info(f"  NOD -> AUCTION: {pf.address} (auction: {pf.foreclosure_auction_time})")

                        # Send immediate notification (regardless of priority — rare event)
                        subject = f"New auction scheduled: {pf.address}"
                        sent = send_auction_scheduled(pf)
                        log_notification(db, pf.id, "new_auction_scheduled", subject, sent,
                                         None if sent else "Email send failed")

                scanned += 1
            except Exception as e:
                logger.error(f"  Error scanning {pf.address}: {e}")
                pf.last_scanned = dt.utcnow()
                pf.scan_error_count = (pf.scan_error_count or 0) + 1
                pf.last_scan_error = f"{type(e).__name__}: {str(e)[:100]}"
                errors += 1

            # Commit every 10 properties
            if (i + 1) % 10 == 0:
                db.commit()

            # Rate limit: 200ms between calls
            time.sleep(0.2)

        db.commit()
        db.close()

        logger.info(f"  NOD rescan complete: {scanned} scanned, {moved_to_auction} moved to Auction, {errors} errors, {rate_limited} rate-limited")

    except Exception as e:
        logger.error(f"rescan_nod_properties failed: {e}", exc_info=True)


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
    # PST = UTC - 8 (PDT = UTC - 7 during daylight saving)
    schedule.every().day.at("15:00").do(run_full)           # 8AM PST = 15:00 UTC
    schedule.every().hour.at(":00").do(run_gmail_only)      # Hourly Gmail check (6AM-7PM PT only, checked inside func)

    # Schedule DealFlow AI pipeline (Mon & Thu at 7AM PST = 14:00 UTC) — PAUSED
    # schedule.every().monday.at("14:00").do(run_dealflow_pipeline)
    # schedule.every().thursday.at("14:00").do(run_dealflow_pipeline)

    # Pre-foreclosure MLS scan — disabled by default, manual-only via dashboard
    # Set MLS_AUTO_SCAN_ENABLED=true to enable automatic every-3-day scanning
    mls_auto = os.getenv("MLS_AUTO_SCAN_ENABLED", "false").lower() == "true"
    if mls_auto:
        schedule.every(3).days.at("09:00").do(run_preforeclosure_scan)

    # Auction notification digest — daily at 8AM Pacific = 15:00 UTC
    schedule.every().day.at("15:00").do(check_upcoming_auctions)

    # Auto-rescan disabled May 7 2026 — use manual batch scanning until OpenWeb Ninja Pro upgrade.
    # Function rescan_nod_properties() still exists and is callable via /admin/run-cron/rescan-nod-properties.
    # To re-enable: uncomment the line below.
    # schedule.every(3).days.at("13:00").do(rescan_nod_properties)

    logger.info("Scheduled (UTC times, Railway server):")
    logger.info(f"  - Pre-foreclosure MLS scan: {'ENABLED every 3 days 09:00 UTC' if mls_auto else 'MANUAL ONLY (MLS_AUTO_SCAN_ENABLED=false)'}")
    logger.info("  - 15:00 UTC (8AM PST): Full updater + check_upcoming_auctions")
    logger.info("  - DISABLED: rescan_nod_properties (manual only via /admin/run-cron)")
    logger.info("  - Hourly (6AM-7PM PT): Gmail-only counter checks")
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
