"""
Migration: Create scan_jobs table for async MLS scanning.

Run on Railway:
  DATABASE_URL="..." python migrate_scan_jobs.py

Safe to re-run — checks if table exists before creating.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from database import get_engine, Base, ScanJob
from sqlalchemy import inspect


def migrate():
    engine = get_engine()
    inspector = inspect(engine)
    if "scan_jobs" in inspector.get_table_names():
        print("  Table scan_jobs already exists — skipping")
    else:
        ScanJob.__table__.create(engine)
        print("  Created table: scan_jobs")
    print("\nMigration complete.")


if __name__ == "__main__":
    print("Running scan_jobs migration...")
    migrate()
