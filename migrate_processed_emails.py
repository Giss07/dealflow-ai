"""
Migration: Create processed_emails table for the Gmail scanner dedup ledger.

The scanner now searches a rolling date window (read + unread) instead of the
old UNSEEN-only gate, and dedupes against this ledger of handled Message-IDs.

Run on Railway:
  DATABASE_URL="..." python migrate_processed_emails.py

Safe to re-run — checks if table exists before creating. (The scanner also
self-creates this table on first use, so this migration is optional.)
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from database import get_engine, ProcessedEmail
from sqlalchemy import inspect


def migrate():
    engine = get_engine()
    inspector = inspect(engine)
    if "processed_emails" in inspector.get_table_names():
        print("  Table processed_emails already exists — skipping")
    else:
        ProcessedEmail.__table__.create(engine)
        print("  Created table: processed_emails")
    print("\nMigration complete.")


if __name__ == "__main__":
    print("Running processed_emails migration...")
    migrate()
