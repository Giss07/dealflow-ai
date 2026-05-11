"""
Migration: Add unit_verified column to preforeclosures.
Safe to re-run.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()
from database import get_engine
from sqlalchemy import text

def migrate():
    engine = get_engine()
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE preforeclosures ADD COLUMN unit_verified BOOLEAN DEFAULT TRUE"))
            conn.commit()
            print("  Added column: unit_verified BOOLEAN DEFAULT TRUE")
        except Exception as e:
            conn.rollback()
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                print("  Column unit_verified already exists — skipping")
            else:
                print(f"  Error: {e}")
    print("Migration complete.")

if __name__ == "__main__":
    print("Running unit_verified migration...")
    migrate()
