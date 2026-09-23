"""
Migration: Builder Zones tables — new_builds + builder_zone_matches.

Run locally:    python migrate_new_builds.py
Run on Railway: DATABASE_URL="postgresql://..." python migrate_new_builds.py

Creates both tables (and their indexes) if absent. Safe to re-run — uses
checkfirst=True, so an existing table is left untouched.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from database import get_engine, NewBuild, BuilderZoneMatch


def migrate():
    engine = get_engine()
    for model in (NewBuild, BuilderZoneMatch):
        existed = engine.dialect.has_table(engine.connect(), model.__tablename__)
        model.__table__.create(bind=engine, checkfirst=True)
        if existed:
            print(f"  Table {model.__tablename__} already exists — skipping")
        else:
            print(f"  Created table: {model.__tablename__}")
    print("\nMigration complete.")


if __name__ == "__main__":
    print("Running Builder Zones migration (new_builds, builder_zone_matches)...")
    migrate()
