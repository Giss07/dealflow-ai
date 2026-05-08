"""
Migration: Add archive reason tracking to preforeclosures.

Run locally:  python migrate_archive_reasons.py
Run on Railway: DATABASE_URL="postgresql://..." python migrate_archive_reasons.py

Adds 3 columns:
  - archive_reason VARCHAR(30)
  - archive_notes TEXT
  - archived_at TIMESTAMP

Safe to re-run — checks column existence before adding.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from database import get_engine
from sqlalchemy import text

COLUMNS = [
    ("archive_reason", "VARCHAR(30)"),
    ("archive_notes", "TEXT"),
    ("archived_at", "TIMESTAMP"),
]


def migrate():
    engine = get_engine()
    is_postgres = "postgresql" in str(engine.url)

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

        if is_postgres:
            try:
                conn.execute(text("""
                    ALTER TABLE preforeclosures
                    ADD CONSTRAINT chk_archive_reason
                    CHECK (archive_reason IN ('already_sold', 'no_equity', 'not_real_lead'))
                """))
                conn.commit()
                print("  Added CHECK constraint: chk_archive_reason")
            except Exception as e:
                conn.rollback()
                if "already exists" in str(e).lower():
                    print("  CHECK constraint already exists — skipping")
                else:
                    print(f"  Error adding CHECK constraint: {e}")

    print("\nMigration complete.")


if __name__ == "__main__":
    print("Running archive reasons migration...")
    migrate()
