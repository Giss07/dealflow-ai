"""
Migrate local SQLite data to Railway Postgres.
Preserves user-edited fields (ARV, repairs, offers, archive) on Railway.
"""

from dotenv import load_dotenv
load_dotenv()

import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, Deal, PipelineRun

# Local SQLite
local_engine = create_engine("sqlite:///dealflow.db")
LocalSession = sessionmaker(bind=local_engine)

# Railway Postgres
PG_URL = "postgresql://postgres:yhqkGnYLXNIDxeiupjowAXIKWSnDJgpD@interchange.proxy.rlwy.net:42167/railway"
pg_engine = create_engine(PG_URL)
PgSession = sessionmaker(bind=pg_engine)

# Create tables on Postgres
print("Creating tables on Railway Postgres...")
Base.metadata.create_all(pg_engine)

# Load local deals
local = LocalSession()
local_deals = local.query(Deal).all()
print(f"Local deals: {len(local_deals)}")

# Load existing Railway deals to preserve user edits
pg = PgSession()
pg_deals = {d.zpid: d for d in pg.query(Deal).all() if d.zpid}
pg_deals_addr = {}
for d in pg.query(Deal).all():
    key = (d.address or "").strip().lower()
    if key:
        pg_deals_addr[key] = d
print(f"Existing Railway deals: {len(pg_deals)}")

# User-editable fields to preserve from Railway
USER_FIELDS = ["is_archived", "offer_amount", "offer_date", "offer_notes", "offer_status"]

migrated = 0
updated = 0
preserved = 0

# Clear Railway and re-insert, preserving user edits
pg.query(Deal).delete()
pg.commit()

print(f"Migrating {len(local_deals)} deals...")
for d in local_deals:
    new_deal = Deal()
    for col in Deal.__table__.columns:
        if col.name == 'id':
            continue
        setattr(new_deal, col.name, getattr(d, col.name, None))

    # Check if this deal existed on Railway with user edits
    existing = pg_deals.get(d.zpid) or pg_deals_addr.get((d.address or "").strip().lower())
    if existing:
        for field in USER_FIELDS:
            old_val = getattr(existing, field, None)
            if old_val:
                setattr(new_deal, field, old_val)
                preserved += 1
        # Preserve user-edited ARV (if different from Zestimate/price*1.25)
        if existing.offer_amount:
            # If user submitted an offer, they likely also set custom ARV/repairs
            if existing.arv != getattr(d, 'arv', None):
                new_deal.arv = existing.arv
                new_deal.max_offer = existing.max_offer
                new_deal.estimated_profit = existing.estimated_profit
                new_deal.roi_pct = existing.roi_pct
                new_deal.offer_analysis = existing.offer_analysis
                new_deal.repairs_mid = existing.repairs_mid
                new_deal.repairs_worst = existing.repairs_worst

    pg.add(new_deal)
    migrated += 1
    if migrated % 200 == 0:
        pg.commit()
        print(f"  {migrated}/{len(local_deals)}...")

pg.commit()
local.close()

count = pg.query(Deal).count()
pg.close()

print(f"\nDONE. Railway Postgres now has {count} deals.")
if preserved:
    print(f"Preserved {preserved} user-edited fields from Railway.")
