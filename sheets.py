"""
DealFlow Google Sheets integration — OAuth2 (edits show as user's Gmail).
One-time auth: python3 sheets.py auth
"""

import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

SPREADSHEET_ID = "1GMp9LbZLgY_uaTjiDQ9cTcy4I1QxOqLsNZWORwkUMCY"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CLIENT_SECRET = os.path.join(os.path.dirname(__file__), "client_secret.json")
# On Railway, client_secret.json won't exist but we don't need it
# (only needed for initial auth flow, not for token refresh)
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token.json")
OFFERS_SHEET = "Offers"  # Tab name for offers


def get_credentials():
    """Get OAuth2 credentials from file or GOOGLE_TOKEN_JSON env var."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    creds = None

    # Try token file first (local), then env var (Railway)
    if os.path.exists(TOKEN_FILE):
        logger.info("Loading Google credentials from token.json")
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    elif os.getenv("GOOGLE_TOKEN_JSON"):
        logger.info("Loading Google credentials from GOOGLE_TOKEN_JSON env var")
        token_data = json.loads(os.getenv("GOOGLE_TOKEN_JSON"))
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    else:
        logger.warning("No Google credentials found (no token.json or GOOGLE_TOKEN_JSON)")
        return None

    # Always try to refresh if we have a refresh token
    if creds and creds.refresh_token:
        try:
            if not creds.valid or creds.expired:
                logger.info("Refreshing expired Google token...")
                creds.refresh(Request())
                logger.info("Google token refreshed successfully")
                # Save refreshed token locally if possible
                try:
                    with open(TOKEN_FILE, "w") as f:
                        f.write(creds.to_json())
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Failed to refresh Google token: {e}")
            # Try using the creds anyway — they might still work
            pass

    if not creds:
        logger.error("No Google credentials available")
        return None

    return creds


def run_auth_flow():
    """One-time OAuth2 flow — opens browser for Google login."""
    from google_auth_oauthlib.flow import InstalledAppFlow

    print("Opening browser for Google OAuth2 login...")
    print("Sign in with gescobarrei@gmail.com")
    print()

    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET, SCOPES)
    creds = flow.run_local_server(port=8888)

    with open(TOKEN_FILE, "w") as f:
        f.write(creds.to_json())

    print(f"\nAuthorized! Token saved to {TOKEN_FILE}")
    print(f"Edits will show as your Gmail in Google Sheets version history.")
    return creds


def get_sheets_service():
    """Get Google Sheets API service."""
    from googleapiclient.discovery import build

    creds = get_credentials()
    if not creds:
        logger.error("No OAuth2 token. Run: python3 sheets.py auth")
        return None

    return build("sheets", "v4", credentials=creds)


def ensure_offers_tab(service):
    """Create Offers tab if it doesn't exist."""
    try:
        meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        tabs = [s["properties"]["title"] for s in meta.get("sheets", [])]

        if OFFERS_SHEET not in tabs:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={
                    "requests": [{
                        "addSheet": {
                            "properties": {"title": OFFERS_SHEET}
                        }
                    }]
                }
            ).execute()
            # Add headers
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{OFFERS_SHEET}!A1:K1",
                valueInputOption="RAW",
                body={
                    "values": [[
                        "Date Submitted", "Address", "City", "State", "Zip",
                        "Offer Amount", "Listing Price", "ARV", "Est. Profit",
                        "Status", "Notes"
                    ]]
                }
            ).execute()
            logger.info(f"Created '{OFFERS_SHEET}' tab with headers")

    except Exception as e:
        logger.error(f"Failed to ensure offers tab: {e}")


def write_offer_to_sheet(deal_dict, offer_amount, offer_date, offer_status, offer_notes):
    """Write an offer to the Google Sheet Offers tab."""
    service = get_sheets_service()
    if not service:
        logger.warning("Google Sheets not connected — skipping sheet write")
        return False

    ensure_offers_tab(service)

    row = [
        offer_date or datetime.now().strftime("%Y-%m-%d"),
        deal_dict.get("address", ""),
        deal_dict.get("city", ""),
        deal_dict.get("state", "CA"),
        deal_dict.get("zip_code", ""),
        offer_amount,
        deal_dict.get("price", ""),
        deal_dict.get("arv", ""),
        deal_dict.get("estimated_profit", ""),
        offer_status or "Pending",
        offer_notes or "",
    ]

    try:
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{OFFERS_SHEET}!A:K",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
        logger.info(f"Wrote offer to Google Sheet: {deal_dict.get('address')}")
        return True
    except Exception as e:
        logger.error(f"Failed to write to sheet: {e}")
        return False


def update_offer_status_in_sheet(address, new_status):
    """Update offer status in sheet by finding the address row."""
    service = get_sheets_service()
    if not service:
        return False

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{OFFERS_SHEET}!A:K",
        ).execute()
        rows = result.get("values", [])

        for i, row in enumerate(rows):
            if len(row) > 1 and row[1].strip().lower() == address.strip().lower():
                # Update status column (J = column 10)
                service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{OFFERS_SHEET}!J{i+1}",
                    valueInputOption="RAW",
                    body={"values": [[new_status]]},
                ).execute()
                logger.info(f"Updated sheet status for {address}: {new_status}")
                return True

        logger.warning(f"Address not found in sheet: {address}")
        return False
    except Exception as e:
        logger.error(f"Failed to update sheet: {e}")
        return False


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) > 1 and sys.argv[1] == "auth":
        run_auth_flow()
    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        service = get_sheets_service()
        if service:
            print("Google Sheets connected!")
            ensure_offers_tab(service)
            print(f"Offers tab ready in spreadsheet {SPREADSHEET_ID}")
        else:
            print("Not connected. Run: python3 sheets.py auth")
    else:
        print("Usage:")
        print("  python3 sheets.py auth   — One-time Google login")
        print("  python3 sheets.py test   — Test connection")
