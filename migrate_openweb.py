"""
Migration: Add foreclosure + property data columns to preforeclosures table.

Run locally:  python migrate_openweb.py
Run on Railway: DATABASE_URL="postgresql://..." python migrate_openweb.py

Adds 19 columns for OpenWeb Ninja / Zillow enrichment data.
Safe to re-run — checks if columns exist before adding.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from database import get_engine
from sqlalchemy import text

COLUMNS = [
    # Foreclosure data
    ("foreclosing_bank", "TEXT"),
    ("foreclosure_default_description", "TEXT"),
    ("foreclosure_default_filing_date", "TIMESTAMP"),
    ("foreclosure_auction_filing_date", "TIMESTAMP"),
    ("foreclosure_auction_city", "TEXT"),
    ("foreclosure_auction_location", "TEXT"),
    ("foreclosure_auction_time", "TIMESTAMP"),
    ("foreclosure_unpaid_balance", "FLOAT"),
    ("foreclosure_past_due_balance", "FLOAT"),
    ("foreclosure_loan_amount", "FLOAT"),
    ("foreclosure_loan_originator", "TEXT"),
    ("foreclosure_loan_date", "TIMESTAMP"),
    ("foreclosure_judicial_type", "TEXT"),
    # Property/listing data
    ("last_sold_price", "FLOAT"),
    ("year_built", "INTEGER"),
    ("listing_type_dimension", "TEXT"),
    ("price_change", "FLOAT"),
    ("price_change_date", "TIMESTAMP"),
    ("days_on_zillow", "INTEGER"),
]


def migrate():
    engine = get_engine()
    with engine.connect() as conn:
        for col_name, col_type in COLUMNS:
            try:
                conn.execute(text(f"ALTER TABLE preforeclosures ADD COLUMN {col_name} {col_type}"))
                conn.commit()
                print(f"  Added column: {col_name} {col_type}")
            except Exception as e:
                conn.rollback()
                if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                    print(f"  Column {col_name} already exists — skipping")
                else:
                    print(f"  Error adding {col_name}: {e}")

    print(f"\nMigration complete. {len(COLUMNS)} columns checked.")


if __name__ == "__main__":
    print("Running OpenWeb Ninja migration (19 new columns)...")
    migrate()
