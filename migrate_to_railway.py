"""
Migrate local SQLite data to Railway Postgres.
"""

from dotenv import load_dotenv
load_dotenv()

import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, Deal, PipelineRun, deal_to_dict

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
deals = local.query(Deal).all()
print(f"Local deals: {len(deals)}")

# Insert into Postgres
pg = PgSession()

# Clear existing data on Postgres
existing = pg.query(Deal).count()
if existing:
    print(f"Clearing {existing} existing deals on Postgres...")
    pg.query(Deal).delete()
    pg.commit()

print(f"Migrating {len(deals)} deals to Railway Postgres...")
migrated = 0
for d in deals:
    new_deal = Deal()
    for col in Deal.__table__.columns:
        if col.name == 'id':
            continue
        val = getattr(d, col.name, None)
        setattr(new_deal, col.name, val)
    pg.add(new_deal)
    migrated += 1
    if migrated % 200 == 0:
        pg.commit()
        print(f"  {migrated}/{len(deals)}...")

pg.commit()
local.close()

# Verify
count = pg.query(Deal).count()
pg.close()

print(f"\nDONE. Railway Postgres now has {count} deals.")
