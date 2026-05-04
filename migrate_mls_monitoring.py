"""
Migration: Add MLS monitoring columns to preforeclosures table.

Run on Railway:
  python migrate_mls_monitoring.py

Adds 5 columns:
  - listed_at TIMESTAMP
  - previous_mls_status VARCHAR(50)
  - zillow_url TEXT
  - scan_error_count INTEGER DEFAULT 0
  - last_scan_error TEXT

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
    ("listed_at", "TIMESTAMP"),
    ("previous_mls_status", "VARCHAR(50)"),
    ("zillow_url", "TEXT"),
    ("scan_error_count", "INTEGER DEFAULT 0"),
    ("last_scan_error", "TEXT"),
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

    print("\nMigration complete.")


if __name__ == "__main__":
    print("Running MLS monitoring migration...")
    migrate()
