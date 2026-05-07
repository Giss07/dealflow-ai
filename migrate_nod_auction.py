"""
Migration: Add foreclosure stage tracking and sent_notifications table.

Run locally:  python migrate_nod_auction.py
Run on Railway: DATABASE_URL="postgresql://..." python migrate_nod_auction.py

Changes:
  1. Add foreclosure_stage VARCHAR(20) DEFAULT 'NOD' to preforeclosures
  2. Add foreclosure_stage_manual_override BOOLEAN DEFAULT FALSE to preforeclosures
  3. Backfill: SET foreclosure_stage='Auction' WHERE foreclosure_auction_time IS NOT NULL
  4. Create sent_notifications table
  5. Add UNIQUE constraint on (property_id, notification_type) — Postgres only

Safe to re-run — checks existence before adding.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from database import get_engine, Base, SentNotification
from sqlalchemy import text, inspect


def migrate():
    engine = get_engine()
    is_postgres = "postgresql" in str(engine.url)

    with engine.connect() as conn:
        # ── Step 1: Add foreclosure_stage column ──
        try:
            conn.execute(text("ALTER TABLE preforeclosures ADD COLUMN foreclosure_stage VARCHAR(20) DEFAULT 'NOD'"))
            conn.commit()
            print("  Added column: foreclosure_stage VARCHAR(20) DEFAULT 'NOD'")
        except Exception as e:
            conn.rollback()
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                print("  Column foreclosure_stage already exists — skipping")
            else:
                print(f"  Error adding foreclosure_stage: {e}")

        # ── Step 2: Add foreclosure_stage_manual_override column ──
        try:
            conn.execute(text("ALTER TABLE preforeclosures ADD COLUMN foreclosure_stage_manual_override BOOLEAN DEFAULT FALSE"))
            conn.commit()
            print("  Added column: foreclosure_stage_manual_override BOOLEAN DEFAULT FALSE")
        except Exception as e:
            conn.rollback()
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                print("  Column foreclosure_stage_manual_override already exists — skipping")
            else:
                print(f"  Error adding foreclosure_stage_manual_override: {e}")

        # ── Step 2b: Add notification_priority column ──
        try:
            conn.execute(text("ALTER TABLE preforeclosures ADD COLUMN notification_priority VARCHAR(20) DEFAULT 'auto'"))
            conn.commit()
            print("  Added column: notification_priority VARCHAR(20) DEFAULT 'auto'")
        except Exception as e:
            conn.rollback()
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                print("  Column notification_priority already exists — skipping")
            else:
                print(f"  Error adding notification_priority: {e}")

        # ── Step 3: Add CHECK constraints (Postgres only) ──
        if is_postgres:
            try:
                conn.execute(text("""
                    ALTER TABLE preforeclosures
                    ADD CONSTRAINT chk_foreclosure_stage
                    CHECK (foreclosure_stage IN ('NOD', 'Auction', 'Sold', 'REO'))
                """))
                conn.commit()
                print("  Added CHECK constraint: chk_foreclosure_stage")
            except Exception as e:
                conn.rollback()
                if "already exists" in str(e).lower():
                    print("  CHECK constraint already exists — skipping")
                else:
                    print(f"  Error adding CHECK constraint: {e}")

            try:
                conn.execute(text("""
                    ALTER TABLE preforeclosures
                    ADD CONSTRAINT chk_notification_priority
                    CHECK (notification_priority IN ('auto', 'watch', 'mute'))
                """))
                conn.commit()
                print("  Added CHECK constraint: chk_notification_priority")
            except Exception as e:
                conn.rollback()
                if "already exists" in str(e).lower():
                    print("  CHECK constraint already exists — skipping")
                else:
                    print(f"  Error adding CHECK constraint: {e}")

        # ── Step 4: Backfill foreclosure_stage='Auction' ──
        try:
            result = conn.execute(text("""
                UPDATE preforeclosures
                SET foreclosure_stage = 'Auction'
                WHERE foreclosure_auction_time IS NOT NULL
                AND (foreclosure_stage IS NULL OR foreclosure_stage = 'NOD')
                AND (foreclosure_stage_manual_override IS NULL OR foreclosure_stage_manual_override = FALSE)
            """))
            conn.commit()
            count = result.rowcount
            print(f"  Backfill: {count} properties set to foreclosure_stage='Auction'")
        except Exception as e:
            conn.rollback()
            print(f"  Error during backfill: {e}")

    # ── Step 5: Create sent_notifications table ──
    inspector = inspect(engine)
    if "sent_notifications" in inspector.get_table_names():
        print("  Table sent_notifications already exists — skipping")
    else:
        SentNotification.__table__.create(engine)
        print("  Created table: sent_notifications")

    # ── Step 6: Add UNIQUE constraint on sent_notifications (Postgres only) ──
    if is_postgres:
        with engine.connect() as conn:
            try:
                conn.execute(text("""
                    ALTER TABLE sent_notifications
                    ADD CONSTRAINT uq_property_notification_type
                    UNIQUE (property_id, notification_type)
                """))
                conn.commit()
                print("  Added UNIQUE constraint: uq_property_notification_type")
            except Exception as e:
                conn.rollback()
                if "already exists" in str(e).lower():
                    print("  UNIQUE constraint already exists — skipping")
                else:
                    print(f"  Error adding UNIQUE constraint: {e}")

    print("\nMigration complete.")


if __name__ == "__main__":
    print("Running NOD/Auction migration...")
    migrate()
