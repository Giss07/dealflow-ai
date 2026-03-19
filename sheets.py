"""
DealFlow Google Sheets integration — via Apps Script webhook.
No OAuth, no tokens, no credentials needed. Works everywhere.
"""

import os
import json
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

SHEETS_WEBHOOK = os.getenv(
    "SHEETS_WEBHOOK_URL",
    "https://script.google.com/macros/s/AKfycbxtXGxX-U914JhfZrO9JaFXSd9OZQAVMRGb2Hgso3jio-SmbNYt2Yab5Vwy-xkAzf5B/exec"
)


def write_offer_to_sheet(deal_dict, offer_amount, offer_date, offer_status, offer_notes):
    """Write an offer to Google Sheet via Apps Script webhook."""
    payload = {
        "date": offer_date or datetime.now().strftime("%Y-%m-%d"),
        "address": deal_dict.get("address", ""),
        "city": deal_dict.get("city", ""),
        "state": deal_dict.get("state", "CA"),
        "zip": deal_dict.get("zip_code", ""),
        "offer_amount": offer_amount,
        "price": deal_dict.get("price", ""),
        "arv": deal_dict.get("arv", ""),
        "profit": deal_dict.get("estimated_profit", ""),
        "status": offer_status or "Pending",
        "notes": offer_notes or "",
    }

    try:
        # Apps Script redirects on POST — need to follow redirects
        resp = requests.post(
            SHEETS_WEBHOOK,
            json=payload,
            timeout=30,
            allow_redirects=True,
            headers={"Content-Type": "application/json"},
        )
        # Apps Script may return 302 → 200, or direct 200
        if resp.status_code == 200:
            logger.info(f"Wrote offer to Google Sheet: {deal_dict.get('address')}")
            return True
        else:
            logger.error(f"Sheet webhook returned {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"Sheet webhook failed: {e}")
        return False


def update_offer_status_in_sheet(address, new_status):
    """Update offer status — sends as new row (Apps Script doesn't support updates easily)."""
    payload = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "address": address,
        "status": new_status,
        "notes": f"Status updated to {new_status}",
    }

    try:
        resp = requests.post(SHEETS_WEBHOOK, json=payload, timeout=15)
        if resp.status_code == 200:
            logger.info(f"Updated sheet status for {address}: {new_status}")
            return True
        else:
            logger.error(f"Sheet webhook returned {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"Sheet webhook failed: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing Google Sheets webhook...")
    result = write_offer_to_sheet(
        {"address": "TEST - 123 Main St", "city": "Fontana", "state": "CA",
         "zip_code": "92335", "price": 450000, "arv": 550000, "estimated_profit": 65000},
        offer_amount=380000,
        offer_date="2026-03-18",
        offer_status="Pending",
        offer_notes="Test offer from DealFlow"
    )
    print(f"Result: {'Success' if result else 'Failed'}")
    print("Check your Google Sheet Offers tab!")
