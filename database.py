"""
DealFlow Database — SQLite locally, auto-migrate to Postgres on Railway.
"""

import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Auto-detect Railway Postgres or fall back to SQLite
DATABASE_URL = os.getenv("DATABASE_URL")


def get_engine():
    """Create SQLAlchemy engine — Postgres on Railway, SQLite locally."""
    from sqlalchemy import create_engine

    if DATABASE_URL:
        # Railway Postgres
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        logger.info("Using PostgreSQL database")
        return create_engine(url)
    else:
        db_path = os.path.join(os.path.dirname(__file__), "dealflow.db")
        logger.info(f"Using SQLite database: {db_path}")
        return create_engine(f"sqlite:///{db_path}")


def get_session():
    """Get a database session."""
    from sqlalchemy.orm import sessionmaker
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


# --- Models ---
from sqlalchemy import Column, Integer, Float, String, Text, DateTime, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Deal(Base):
    __tablename__ = "deals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    zpid = Column(String(50), unique=True, nullable=True)
    address = Column(String(255))
    city = Column(String(100))
    state = Column(String(10), default="CA")
    zip_code = Column(String(10))
    price = Column(Float)
    bedrooms = Column(Integer)
    bathrooms = Column(Float)
    sqft = Column(Float)
    lot_sqft = Column(Float)
    year_built = Column(Integer)
    description = Column(Text)
    home_type = Column(String(50))
    listing_url = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    days_on_zillow = Column(Integer)
    status = Column(String(50))

    # Analysis results
    score = Column(Integer)
    score_reasoning = Column(Text)
    overall_condition = Column(String(20))
    photo_grades = Column(Text)  # JSON
    arv = Column(Float)
    comp_count = Column(Integer)
    comps = Column(Text)  # JSON
    privy_url = Column(Text)

    # Repair estimates
    repairs_mid = Column(Float)
    repairs_worst = Column(Float)
    repair_breakdown = Column(Text)  # JSON

    # Offer analysis
    max_offer = Column(Float)
    max_offer_worst = Column(Float)
    estimated_profit = Column(Float)
    roi_pct = Column(Float)
    offer_analysis = Column(Text)  # JSON

    # Meta
    has_deal_keywords = Column(Boolean, default=False)
    matched_keywords = Column(Text)  # JSON
    date_found = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    alert_sent = Column(Boolean, default=False)

    # Archive & Offer tracking
    is_archived = Column(Boolean, default=False)
    offer_amount = Column(Float)
    offer_date = Column(String(50))
    offer_notes = Column(Text)
    offer_status = Column(String(20))  # Pending, Accepted, Rejected, Countered

    # Comps & ARV justification
    user_comps = Column(Text)  # JSON list of comp addresses
    arv_justification = Column(Text)

    # Hidden (not interested, but not archived)
    is_hidden = Column(Boolean, default=False)

    # Source tracking
    source = Column(String(20), default="zillow")  # zillow or manual


class PreForeclosure(Base):
    __tablename__ = "preforeclosures"

    id = Column(Integer, primary_key=True, autoincrement=True)
    address = Column(String(255))
    city = Column(String(100))
    state = Column(String(10), default="CA")
    zip_code = Column(String(10))
    property_type = Column(String(50), default="SFR")
    source_list = Column(String(50), default="pre-foreclosure")
    estimated_value = Column(Float)
    auction_date = Column(String(50))
    notes = Column(Text)
    mls_status = Column(String(50), default="unknown")
    mls_price = Column(Float)
    ai_notes = Column(Text)
    last_scanned = Column(DateTime)
    is_new = Column(Boolean, default=False)
    is_archived = Column(Boolean, default=False)
    linked_deal_id = Column(Integer)  # ID of Deal created from this property
    date_added = Column(DateTime, default=datetime.utcnow)

    # MLS monitoring fields (added for mls-monitoring feature)
    listed_at = Column(DateTime)              # Set when Monitoring → On Market
    previous_mls_status = Column(String(50))  # Previous status for transition detection
    zillow_url = Column(Text)                 # Manual Zillow URL override / auto-populated from scan
    scan_error_count = Column(Integer, default=0)
    last_scan_error = Column(Text)            # Last error message (why scan failed)

    # Foreclosure data (from OpenWeb Ninja / Zillow)
    foreclosing_bank = Column(Text)
    foreclosure_default_description = Column(Text)
    foreclosure_default_filing_date = Column(DateTime)
    foreclosure_auction_filing_date = Column(DateTime)
    foreclosure_auction_city = Column(Text)
    foreclosure_auction_location = Column(Text)
    foreclosure_auction_time = Column(DateTime)
    foreclosure_unpaid_balance = Column(Float)
    foreclosure_past_due_balance = Column(Float)
    foreclosure_loan_amount = Column(Float)
    foreclosure_loan_originator = Column(Text)
    foreclosure_loan_date = Column(DateTime)
    foreclosure_judicial_type = Column(Text)

    # Property/listing data (from OpenWeb Ninja / Zillow)
    last_sold_price = Column(Float)
    year_built = Column(Integer)
    listing_type_dimension = Column(Text)
    price_change = Column(Float)
    price_change_date = Column(DateTime)
    days_on_zillow = Column(Integer)


def preforeclosure_to_dict(pf):
    return {
        "id": pf.id,
        "address": pf.address,
        "city": pf.city,
        "state": pf.state,
        "zip_code": pf.zip_code,
        "property_type": pf.property_type,
        "source_list": pf.source_list,
        "estimated_value": pf.estimated_value,
        "auction_date": pf.auction_date,
        "notes": pf.notes,
        "mls_status": pf.mls_status,
        "mls_price": pf.mls_price,
        "ai_notes": pf.ai_notes,
        "last_scanned": pf.last_scanned.isoformat() if pf.last_scanned else None,
        "is_new": pf.is_new or False,
        "is_archived": pf.is_archived or False,
        "linked_deal_id": pf.linked_deal_id,
        "date_added": pf.date_added.isoformat() if pf.date_added else None,
        "listed_at": pf.listed_at.isoformat() if pf.listed_at else None,
        "previous_mls_status": pf.previous_mls_status,
        "zillow_url": pf.zillow_url,
        "scan_error_count": pf.scan_error_count or 0,
        "last_scan_error": pf.last_scan_error,
        # Foreclosure data
        "foreclosing_bank": pf.foreclosing_bank,
        "foreclosure_default_description": pf.foreclosure_default_description,
        "foreclosure_default_filing_date": pf.foreclosure_default_filing_date.isoformat() if pf.foreclosure_default_filing_date else None,
        "foreclosure_auction_filing_date": pf.foreclosure_auction_filing_date.isoformat() if pf.foreclosure_auction_filing_date else None,
        "foreclosure_auction_city": pf.foreclosure_auction_city,
        "foreclosure_auction_location": pf.foreclosure_auction_location,
        "foreclosure_auction_time": pf.foreclosure_auction_time.isoformat() if pf.foreclosure_auction_time else None,
        "foreclosure_unpaid_balance": pf.foreclosure_unpaid_balance,
        "foreclosure_past_due_balance": pf.foreclosure_past_due_balance,
        "foreclosure_loan_amount": pf.foreclosure_loan_amount,
        "foreclosure_loan_originator": pf.foreclosure_loan_originator,
        "foreclosure_loan_date": pf.foreclosure_loan_date.isoformat() if pf.foreclosure_loan_date else None,
        "foreclosure_judicial_type": pf.foreclosure_judicial_type,
        # Property/listing data
        "last_sold_price": pf.last_sold_price,
        "year_built": pf.year_built,
        "listing_type_dimension": pf.listing_type_dimension,
        "price_change": pf.price_change,
        "price_change_date": pf.price_change_date.isoformat() if pf.price_change_date else None,
        "days_on_zillow": pf.days_on_zillow,
    }


class ScanJob(Base):
    __tablename__ = "scan_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    property_ids = Column(Text)       # JSON array of IDs
    total = Column(Integer, default=0)
    scanned = Column(Integer, default=0)
    new_on_market = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    actual_cost = Column(Float, default=0)
    result = Column(Text)             # Final summary JSON
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    expires_at = Column(DateTime)     # Max runtime (created + 2hr)


def scan_job_to_dict(job):
    return {
        "id": job.id,
        "status": job.status,
        "total": job.total or 0,
        "scanned": job.scanned or 0,
        "new_on_market": job.new_on_market or 0,
        "errors": job.errors or 0,
        "actual_cost": job.actual_cost or 0,
        "result": json.loads(job.result) if job.result else None,
        "error_message": job.error_message,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    listings_scraped = Column(Integer, default=0)
    listings_filtered = Column(Integer, default=0)
    listings_scored = Column(Integer, default=0)
    alerts_sent = Column(Integer, default=0)
    status = Column(String(20), default="running")
    error = Column(Text)


def init_db():
    """Create all tables."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("Database initialized")


def save_deal(listing, session=None):
    """Save or update a deal in the database."""
    close_session = False
    if session is None:
        session = get_session()
        close_session = True

    try:
        # Check if deal exists by zpid or address
        existing = None
        if listing.get("zpid"):
            existing = session.query(Deal).filter_by(zpid=listing["zpid"]).first()
        if not existing and listing.get("address"):
            existing = session.query(Deal).filter_by(
                address=listing["address"],
                zip_code=listing.get("zip_code")
            ).first()

        is_update = existing is not None
        if existing:
            deal = existing
        else:
            deal = Deal()
            session.add(deal)

        # On update from scraper: only update volatile fields, preserve user data
        if is_update and listing.get("_scraper_update"):
            deal.price = listing.get("price") or deal.price
            deal.days_on_zillow = listing.get("days_on_zillow") if listing.get("days_on_zillow") is not None else deal.days_on_zillow
            deal.status = listing.get("status") or deal.status
            deal.last_updated = datetime.utcnow()
        else:
            # Full update — map all listing fields
            deal.zpid = listing.get("zpid")
            deal.address = listing.get("address")
            deal.city = listing.get("city")
            deal.state = listing.get("state", "CA")
            deal.zip_code = listing.get("zip_code")
            deal.price = listing.get("price")
            deal.bedrooms = listing.get("bedrooms")
            deal.bathrooms = listing.get("bathrooms")
            deal.sqft = listing.get("sqft")
            deal.lot_sqft = listing.get("lot_sqft")
            deal.year_built = listing.get("year_built")
            deal.description = listing.get("description")
            deal.home_type = listing.get("home_type")
            deal.listing_url = listing.get("listing_url")
            deal.latitude = listing.get("latitude")
            deal.longitude = listing.get("longitude")
            deal.days_on_zillow = listing.get("days_on_zillow")
            deal.status = listing.get("status")

            deal.score = listing.get("score")
            deal.score_reasoning = listing.get("score_reasoning")
            deal.overall_condition = listing.get("overall_condition")
            deal.photo_grades = json.dumps(listing.get("photo_grades")) if listing.get("photo_grades") else None
            deal.arv = listing.get("arv")
            deal.comp_count = listing.get("comp_count")
            deal.comps = json.dumps(listing.get("comps")) if listing.get("comps") else None
            deal.privy_url = listing.get("privy_url")

            repair_est = listing.get("repair_estimate", {})
            deal.repairs_mid = repair_est.get("total_mid")
            deal.repairs_worst = repair_est.get("total_worst")
            deal.repair_breakdown = json.dumps(repair_est.get("breakdown")) if repair_est.get("breakdown") else None

            offer = listing.get("offer_analysis", {})
            deal.max_offer = offer.get("max_offer")
            deal.max_offer_worst = offer.get("max_offer_worst")
            deal.estimated_profit = offer.get("estimated_profit")
            deal.roi_pct = offer.get("roi_pct")
            deal.offer_analysis = json.dumps(offer) if offer else None

            deal.has_deal_keywords = listing.get("has_deal_keywords", False)
            deal.matched_keywords = json.dumps(listing.get("matched_keywords")) if listing.get("matched_keywords") else None
            deal.last_updated = datetime.utcnow()

        session.commit()
        return deal

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to save deal: {e}")
        raise
    finally:
        if close_session:
            session.close()


def save_deals(listings):
    """Save all deals to database."""
    session = get_session()
    saved = 0
    try:
        for listing in listings:
            try:
                save_deal(listing, session)
                saved += 1
            except Exception as e:
                logger.warning(f"Failed to save {listing.get('address')}: {e}")
        logger.info(f"Saved {saved}/{len(listings)} deals to database")
    finally:
        session.close()
    return saved


def get_all_deals(session=None):
    """Get all deals from database."""
    close_session = False
    if session is None:
        session = get_session()
        close_session = True
    try:
        deals = session.query(Deal).order_by(Deal.score.desc().nullslast()).all()
        return deals
    finally:
        if close_session:
            session.close()


def deal_to_dict(deal):
    """Convert a Deal ORM object to a dictionary."""
    return {
        "id": deal.id,
        "zpid": deal.zpid,
        "address": deal.address,
        "city": deal.city,
        "state": deal.state,
        "zip_code": deal.zip_code,
        "price": deal.price,
        "bedrooms": deal.bedrooms,
        "bathrooms": deal.bathrooms,
        "sqft": deal.sqft,
        "lot_sqft": deal.lot_sqft,
        "year_built": deal.year_built,
        "description": deal.description,
        "home_type": deal.home_type,
        "listing_url": deal.listing_url,
        "latitude": deal.latitude,
        "longitude": deal.longitude,
        "days_on_zillow": deal.days_on_zillow,
        "status": deal.status,
        "score": deal.score,
        "score_reasoning": deal.score_reasoning,
        "overall_condition": deal.overall_condition,
        "photo_grades": json.loads(deal.photo_grades) if deal.photo_grades else {},
        "arv": deal.arv,
        "comp_count": deal.comp_count,
        "comps": json.loads(deal.comps) if deal.comps else [],
        "privy_url": deal.privy_url,
        "repairs_mid": deal.repairs_mid,
        "repairs_worst": deal.repairs_worst,
        "repair_breakdown": json.loads(deal.repair_breakdown) if deal.repair_breakdown else {},
        "max_offer": deal.max_offer,
        "max_offer_worst": deal.max_offer_worst,
        "estimated_profit": deal.estimated_profit,
        "roi_pct": deal.roi_pct,
        "offer_analysis": json.loads(deal.offer_analysis) if deal.offer_analysis else {},
        "has_deal_keywords": deal.has_deal_keywords,
        "matched_keywords": json.loads(deal.matched_keywords) if deal.matched_keywords else [],
        "date_found": deal.date_found.isoformat() if deal.date_found else None,
        "last_updated": deal.last_updated.isoformat() if deal.last_updated else None,
        "alert_sent": deal.alert_sent,
        "is_archived": deal.is_archived or False,
        "offer_amount": deal.offer_amount,
        "offer_date": deal.offer_date,
        "offer_notes": deal.offer_notes,
        "offer_status": deal.offer_status,
        "user_comps": json.loads(deal.user_comps) if deal.user_comps else [],
        "arv_justification": deal.arv_justification,
        "is_hidden": deal.is_hidden or False,
        "source": deal.source or "zillow",
    }


def log_pipeline_run(started_at, finished_at, scraped, filtered, scored, alerts, status, error=None):
    """Log a pipeline run."""
    session = get_session()
    try:
        run = PipelineRun(
            started_at=started_at,
            finished_at=finished_at,
            listings_scraped=scraped,
            listings_filtered=filtered,
            listings_scored=scored,
            alerts_sent=alerts,
            status=status,
            error=error,
        )
        session.add(run)
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to log pipeline run: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    print("Database initialized successfully")
