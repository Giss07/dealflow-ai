"""
DealFlow - Zillow Status Updater + Counter Alert System
--------------------------------------------------------
This script does 3 things:
1. Monitors Christian's Gmail (unmatched.dealflow@gmail.com) for counter offer emails
   - If address matches your Google Sheet → updates counter price + date
   - Saves previous counter info to Notes column so history is never lost
   - If no match → ignores the email completely
2. Checks Zillow for current status of all properties:
   - Active on Zillow + was Pending in sheet → changes to Resubmit + sends alert
   - Sold on Zillow → changes to STP
3. Sends alerts for:
   - HOT ALERT: Counter at or below your offer price
   - CLOSE DEAL ALERT: Counter within $30k of your offer price
   - BACK ON MARKET: Previously Pending, now back for sale

SCHEDULE (via crontab):
  • Every morning at 9AM PST  → Full run (Gmail + Zillow + Alerts)
  • Every hour 9AM–8PM PST    → Gmail-only (counters only, skip Zillow)

CRONTAB SETUP:
  crontab -e  →  add these two lines:

  # Full run at 9AM PST daily
  0 9 * * * TZ=America/Los_Angeles /usr/bin/python3 /Users/Gissel/Desktop/DealFlow/dealflow_updater.py full >> /Users/Gissel/Desktop/DealFlow/dealflow_log.txt 2>&1

  # Gmail-only check every hour from 10AM–8PM PST (skips 9AM since full run covers it)
  0 10-20 * * * TZ=America/Los_Angeles /usr/bin/python3 /Users/Gissel/Desktop/DealFlow/dealflow_updater.py gmail_only >> /Users/Gissel/Desktop/DealFlow/dealflow_log.txt 2>&1

MANUAL USAGE:
  python3 dealflow_updater.py           → full run
  python3 dealflow_updater.py full      → full run (Gmail + Zillow + Alerts)
  python3 dealflow_updater.py gmail_only → Gmail-only (no Zillow)
  python3 dealflow_updater.py test      → run all connection checks, print PASS/FAIL report
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
import gspread
import sys
from google.oauth2.service_account import Credentials
import requests
import smtplib
import imaplib
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import re
from datetime import datetime
import pytz

# ============================================================
# CONFIGURATION
# ============================================================

SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dealflow-sheets-b59dc0c02384.json'))
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '')


def _load_credentials(scopes):
    """Load Google service-account credentials.

    Prefers GOOGLE_SERVICE_ACCOUNT_JSON env (Railway) so secrets stay out of the image;
    falls back to SERVICE_ACCOUNT_FILE on disk for local development.
    """
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        import json
        return Credentials.from_service_account_info(json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), scopes=scopes)
    return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)

YOUR_GMAIL = os.getenv('DEALFLOW_ALERTS_GMAIL', '')
YOUR_APP_PASSWORD = os.getenv('DEALFLOW_ALERTS_PASSWORD', '')

CHRISTIAN_GMAIL = os.getenv('CHRISTIAN_GMAIL', '')
CHRISTIAN_APP_PASSWORD = os.getenv('CHRISTIAN_APP_PASSWORD', '')

ALERT_EMAILS = os.getenv('ALERT_EMAILS', '').split(',') if os.getenv('ALERT_EMAILS') else []

CLOSE_DEAL_THRESHOLD = 30000

OPENWEB_NINJA_API_KEY = os.getenv('OPENWEB_NINJA_API_KEY', '')

PST = pytz.timezone('America/Los_Angeles')

# ============================================================
# SCHEDULE LOGIC
# ============================================================

def get_run_mode_from_schedule():
    """
    Determines run mode based on current PST time:
      - 9:00 AM exactly          → 'full'  (Gmail + Zillow + Alerts)
      - 10:00 AM – 8:00 PM       → 'gmail_only'
      - Outside those hours      → 'skip'
    Only used when no CLI argument is passed.
    """
    now_pst = datetime.now(PST)
    hour = now_pst.hour
    minute = now_pst.minute

    if hour == 9 and minute < 30:
        return 'full'
    elif 10 <= hour <= 20:
        return 'gmail_only'
    else:
        return 'skip'

# ============================================================
# GOOGLE SHEETS
# ============================================================

def connect_to_sheet():
    print("Connecting to Google Sheet...")
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = _load_credentials(scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1
    print("Connected!")
    return sheet

def get_zillow_urls_from_sheet():
    """
    Fetches the hyperlink URLs from column C (Address) using the Sheets API.
    Returns a dict of {address_text: zillow_url}
    """
    try:
        from googleapiclient.discovery import build
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds = _load_credentials(scopes)
        service = build('sheets', 'v4', credentials=creds)
        result = service.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID,
            ranges=['Properties_Offer_Tracker_Template!C:C'],
            fields='sheets/data/rowData/values/hyperlink,sheets/data/rowData/values/formattedValue'
        ).execute()
        url_map = {}
        rows = result['sheets'][0]['data'][0].get('rowData', [])
        for row in rows:
            values = row.get('values', [{}])
            if values:
                cell = values[0]
                text = cell.get('formattedValue', '').strip()
                url = cell.get('hyperlink', '')
                if text and url and 'zillow.com' in url:
                    url_map[text.lower()] = url
        print(f"  Loaded {len(url_map)} Zillow URLs from sheet")
        return url_map
    except Exception as e:
        print(f"  Could not load Zillow URLs: {e}")
        return {}

# ============================================================
# HELPER
# ============================================================

def clean_price(price_str):
    if not price_str:
        return None
    try:
        cleaned = str(price_str).replace('$', '').replace(',', '').replace(' ', '').strip()
        if cleaned and cleaned != 'nan':
            return int(float(cleaned))
        return None
    except:
        return None

# ============================================================
# EMAIL READER - Christian's Gmail
# ============================================================

def get_all_addresses(records):
    addresses = []
    for record in records:
        addr = record.get('Address', '').strip()
        if addr:
            addresses.append(addr.lower())
    return addresses

def normalize_address(addr):
    """Normalize address for matching: lowercase, strip unit variations,
    expand street-type abbreviations to canonical full words.
    Without expansion, 'Pl' vs 'Place' silently breaks substring matching.
    """
    a = addr.lower().strip()
    # Normalize unit indicators
    a = re.sub(r'\bapt\b\.?', '#', a)
    a = re.sub(r'\bunit\b\.?', '#', a)
    a = re.sub(r'\bste\b\.?', '#', a)
    a = re.sub(r'\bsuite\b\.?', '#', a)
    # Expand street-type abbreviations to canonical full words so that
    # email '4th Pl' matches sheet '4th Place' (and vice versa).
    for abbrev, full in (
        ('st', 'street'), ('ave', 'avenue'), ('pl', 'place'),
        ('dr', 'drive'), ('ln', 'lane'), ('rd', 'road'),
        ('blvd', 'boulevard'), ('ct', 'court'), ('cir', 'circle'),
        ('hwy', 'highway'), ('pkwy', 'parkway'), ('ter', 'terrace'),
    ):
        a = re.sub(rf'\b{abbrev}\b\.?', full, a)
    return a


def find_matching_address(email_text, sheet_addresses):
    email_lower = email_text.lower()
    email_normalized = normalize_address(email_text)

    # First try: look for "Address: <street>" label in HUD emails
    hud_match = re.search(r'address:\s*([^\n\r]+)', email_lower)
    if hud_match:
        hud_addr = normalize_address(hud_match.group(1).strip())
        for address in sheet_addresses:
            parts = normalize_address(address.split(',')[0].strip())
            if parts and len(parts) > 5 and parts in hud_addr:
                return address

    # Second try: exact match with normalization
    for address in sheet_addresses:
        parts = normalize_address(address.split(',')[0].strip())
        if parts and len(parts) > 5 and parts in email_normalized:
            return address

    # Third try: match on street number + street name only (ignore unit)
    for address in sheet_addresses:
        street = address.split(',')[0].strip().lower()
        # Get just the street number and name, drop unit
        street_core = re.sub(r'\s*(apt|unit|ste|suite|#)\s*\S*$', '', street, flags=re.IGNORECASE).strip()
        if street_core and len(street_core) > 5 and street_core in email_lower:
            return address

    return None

# Phrases that signal the email is describing SOMEONE ELSE's bid being accepted
# (not yours). Anything in this list short-circuits detect_acceptance — the
# email then falls through to counter/rejection detection.
REJECTION_OVERRIDE_PHRASES = [
    # narrow negation of "your bid" outcome
    "not accepted", "not accept", "cannot accept", "won't accept",
    # competing offer was accepted
    "accepted another", "accept another", "accepting another",
    "accepted a different", "accept a different", "accepting a different",
    # seller chose someone else
    "going with another", "went with another",
    "chose another", "choosing another",
    "selected another", "selecting another",
    # HUD/agent describing the WINNING competing offer (third-person passive)
    "offer accepted was", "bid accepted was",
    "the offer accepted", "the bid accepted",
    "offer that was accepted", "bid that was accepted",
    "offer accepted included", "bid accepted included",
    "offer we accepted", "bid we accepted",
    "another offer was accepted", "another bid was accepted",
    # HUD agent boilerplate when revealing competing terms
    "allowed me to disclose",
    # explicit framings
    "decided to go with",
    "decided to accept another",
]

# HIGH confidence — explicitly addresses YOUR bid; we are confident this is a real win
HIGH_CONFIDENCE_ACCEPTANCE_KEYWORDS = [
    "accepted your offer", "accepted your bid",
    "your bid has been accepted",
    "your offer has been accepted",
    "your bid has been provisionally accepted",
    "your bid has been selected",
    "you have been selected",
]

# LIKELY confidence — positive language but doesn't explicitly name your bid.
# HUD's template subject "Bid Acceptance Notification" appears on every HUD
# email (wins, counters, AND rejections), so these keywords can fire on
# competitor wins. Matches here produce a hedged "LIKELY ACCEPTED — VERIFY"
# alert and leave the sheet Status column UNCHANGED.
LIKELY_ACCEPTANCE_KEYWORDS = [
    "bid acceptance", "bid accepted", "bid has been accepted",
    "provisionally accepted", "bid has been provisionally accepted",
    "offer accepted", "offer has been accepted",
    "congratulations", "winning bid", "winning bidder",
    "proceed to closing", "proceed with closing",
    "under contract", "executed contract",
    "bid acceptance notification",
]


def detect_acceptance(email_text):
    """Check if email contains acceptance language.

    Returns (keyword, context, confidence) where confidence is "high" or
    "likely". Returns (None, None, None) if not an acceptance.

    HIGH keywords explicitly name your bid ("accepted your offer"); LIKELY
    keywords use positive but ambiguous language that could describe a
    competing bid being accepted. Override phrases short-circuit detection
    even if an acceptance keyword would otherwise match.
    """
    import re as _re
    text_lower = email_text.lower()
    clean_text = _re.sub(r'\s+', ' ', text_lower).strip()

    # Short-circuit if the email is actually describing a competing offer
    # being accepted — see REJECTION_OVERRIDE_PHRASES for the full pattern set.
    for rej in REJECTION_OVERRIDE_PHRASES:
        if rej in clean_text:
            return None, None, None

    # HIGH confidence first — explicit "your bid/offer" wording
    for kw in HIGH_CONFIDENCE_ACCEPTANCE_KEYWORDS:
        pos = clean_text.find(kw)
        if pos >= 0:
            start = max(0, pos - 60)
            end = min(len(clean_text), pos + len(kw) + 90)
            context = clean_text[start:end].strip()
            if start > 0:
                space = context.find(' ')
                if space > 0 and space < 15:
                    context = context[space + 1:]
            return kw, context, "high"

    # LIKELY confidence — positive but ambiguous (alert will be hedged)
    for kw in LIKELY_ACCEPTANCE_KEYWORDS:
        pos = clean_text.find(kw)
        if pos >= 0:
            start = max(0, pos - 60)
            end = min(len(clean_text), pos + len(kw) + 90)
            context = clean_text[start:end].strip()
            if start > 0:
                space = context.find(' ')
                if space > 0 and space < 15:
                    context = context[space + 1:]
            return kw, context, "likely"

    return None, None, None


REJECTION_KEYWORDS = [
    "won't consider", "will not consider", "not considering",
    "rejected", "reject", "declined", "decline",
    "not accepted", "not accept", "cannot accept",
    "resubmit", "re-submit", "submit again",
    "view the property first", "view first", "see the property first",
    "not entertaining", "no longer available", "off the market",
    "too low", "offer is too", "below asking",
    "not interested", "pass on", "passing on",
    "do not wish to", "does not wish to",
    "lower than the lowest", "lower than the highest",
    "highest and best", "best and final",
    "multiple offers", "come up",
    # competing offer accepted — routes "another offer" emails to rejection
    # path after detect_acceptance's override blocks them
    "accepted another offer", "accepted another bid",
    "accept another offer", "accept another bid",
    "accepted a different offer", "accepted a different bid",
    "going with another", "went with another",
    "chose another", "selected another",
    "decided to go with another",
]


def detect_rejection(email_text):
    """Check if email contains rejection language. Returns (keyword, context) or (None, None)."""
    text_lower = email_text.lower()
    # Clean up whitespace for better context extraction
    import re as _re
    clean_text = _re.sub(r'\s+', ' ', text_lower).strip()
    for kw in REJECTION_KEYWORDS:
        pos = clean_text.find(kw)
        if pos >= 0:
            # Extract ~150 chars of context around the keyword
            start = max(0, pos - 60)
            end = min(len(clean_text), pos + len(kw) + 90)
            context = clean_text[start:end].strip()
            # Clean up leading/trailing partial words
            if start > 0:
                space = context.find(' ')
                if space > 0 and space < 15:
                    context = context[space + 1:]
            if end < len(clean_text):
                space = context.rfind(' ')
                if space > len(context) - 15:
                    context = context[:space]
            return kw, context
    return None, None


NOT_COUNTER_PHRASES = [
    "lower than", "below", "higher than", "above",
    "less than", "more than", "short of", "away from",
    "lower than the lowest", "lower than the highest",
    "not enough", "insufficient", "come up", "increase",
    "multiple offers", "highest and best", "best and final",
]


def extract_counter_price(email_text):
    text_lower = email_text.lower()
    clean = re.sub(r'\s+', ' ', text_lower).strip()

    # First check: if the email contains "not a counter" language,
    # the dollar amounts are feedback, not counter prices
    for phrase in NOT_COUNTER_PHRASES:
        if phrase in clean:
            # Check if a dollar amount is near this phrase (within 50 chars)
            pos = clean.find(phrase)
            nearby = clean[max(0, pos-50):pos+len(phrase)+50]
            if re.search(r'\$[\s]*([\d,]+)', nearby) or re.search(r'([\d,]{4,})', nearby):
                return None  # Dollar amount is part of feedback, not a counter

    patterns = [
        # HUD format: "minimum acceptable net to HUD offer amount for this property as 209,000.00"
        r'minimum acceptable net to hud offer amount for[\s\S]*?as\s*([\d,]+)',
        r'minimum acceptable[\s\S]*?as\s*([\d,]+)',
        # Explicit counter offer formats only
        r'counter\s*(?:offer)?\s*(?:price)?\s*(?:of|at|is|:)?\s*\$[\s]*([\d,]+)',
        r'counter\s*(?:offer)?\s*(?:price)?\s*(?:of|at|is|:)?\s*([\d,]{6,})',
        r'\$[\s]*([\d,]+)[\s]*counter',
        r'(?:seller|owner)\s*(?:is\s*)?counter(?:ing|ed)?\s*(?:at|with)?\s*\$[\s]*([\d,]+)',
        r'counter(?:ed)?\s*(?:at|with)\s*\$[\s]*([\d,]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            price_str = match.group(1).replace(',', '').split('.')[0]
            try:
                price = int(price_str)
                if 50000 < price < 5000000:
                    return price
            except:
                continue
    return None

def read_christian_emails(sheet, records):
    print("\n--- Checking Christian's Gmail for Counter Emails ---")
    sheet_addresses = get_all_addresses(records)
    headers = sheet.row_values(1)
    try:
        counter_price_col = headers.index('Counter Price') + 1
        counter_date_col = headers.index('Counter Date') + 1
        status_col = headers.index('Status (/Accepted/Rejected/Counter)') + 1
        notes_col = headers.index('Notes') + 1
    except ValueError as e:
        print(f"Column not found: {e}")
        return []

    alerts = []
    try:
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(CHRISTIAN_GMAIL, CHRISTIAN_APP_PASSWORD)
        mail.select('inbox')
        status, messages = mail.search(None, 'UNSEEN')
        email_ids = messages[0].split()
        print(f"Found {len(email_ids)} unread emails in Christian's inbox")

        for email_id in email_ids:
            try:
                status, msg_data = mail.fetch(email_id, '(RFC822)')
                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)
                body = ''
                html_body = ''
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == 'text/plain':
                            body += part.get_payload(decode=True).decode('utf-8', errors='ignore')
                        elif part.get_content_type() == 'text/html' and not body:
                            html_body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                else:
                    ct = msg.get_content_type()
                    payload = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
                    if ct == 'text/html':
                        html_body = payload
                    else:
                        body = payload

                # If no plain text, strip HTML tags
                if not body and html_body:
                    body = re.sub(r'<[^>]+>', ' ', html_body)
                    body = re.sub(r'&nbsp;', ' ', body)
                    body = re.sub(r'\s+', ' ', body).strip()

                subject = msg.get('Subject', '')
                sender = msg.get('From', '').lower()
                full_text = subject + ' ' + body

                # Skip our own alert emails being forwarded back
                own_alert_signatures = [
                    'back on market', 'hot alert', 'close deal alert',
                    'dealflow', 'offer rejected', 'deal alert'
                ]
                subject_lower = subject.lower()
                is_own_alert = any(sig in subject_lower for sig in own_alert_signatures)
                if is_own_alert:
                    print(f"  Skipping forwarded alert email: {subject[:60]}")
                    continue

                # HUD/bid-specific: extract address from email body or subject
                hud_address_match = None
                clean_subject = re.sub(r'^(re:|fw:|fwd:)\s*', '', subject.lower(), flags=re.IGNORECASE).strip()
                is_hud = ('hud' in clean_subject and any(kw in clean_subject for kw in ['counter', 'bid'])) \
                    or 'bid acceptance' in clean_subject \
                    or 'bid notification' in clean_subject

                # Also extract address from subject format: "case#; 4001 Terra Granada Dr #1A"
                subject_addr_match = re.search(r'[\d-]+;\s*(\d+\s+[A-Za-z].*?)$', clean_subject)
                if subject_addr_match:
                    hud_address_match = subject_addr_match.group(1).strip()
                    print(f"  Subject address extracted: {hud_address_match}")
                if is_hud:
                    addr_match = re.search(r'address[:\s]+(\d+[^,\n]+,\s*[^,\n]+,\s*[A-Z]{2}\s*\d{5})', body, re.IGNORECASE) or \
                                 re.search(r'property[:\s]+(\d+[^,\n]+,\s*[^,\n]+,\s*[A-Z]{2}\s*\d{5})', body, re.IGNORECASE) or \
                                 re.search(r'(\d+\s+[A-Za-z]+(?:\s+[A-Za-z]+){0,3}(?:\s+(?:Ave|St|Dr|Ln|Rd|Blvd|Way|Ct|Pl|Cir))[^,]*,\s*[^,]+,\s*[A-Z]{2}\s*\d{5})', body, re.IGNORECASE)
                    if addr_match:
                        hud_address_match = addr_match.group(1).strip().lower()
                        print(f"  HUD email — extracted address: {hud_address_match}")

                matched_address = find_matching_address(full_text, sheet_addresses)

                # If normal matching failed but HUD address was extracted, try fuzzy match
                if not matched_address and hud_address_match:
                    for sheet_addr in sheet_addresses:
                        # Match on street number + street name
                        hud_street = hud_address_match.split(',')[0].strip()
                        if hud_street in sheet_addr or sheet_addr in hud_street:
                            matched_address = sheet_addr
                            print(f"  HUD fuzzy match: {matched_address}")
                            break

                if matched_address:
                    print(f"  Match found: {matched_address}")

                    # Check acceptance FIRST — before counter price extraction
                    # (HUD acceptance emails contain dollar amounts that confuse the counter parser)
                    accept_kw, accept_context, accept_confidence = detect_acceptance(full_text)
                    if accept_kw:
                        emoji = "🎉" if accept_confidence == "high" else "⚠️"
                        label = "ACCEPTANCE" if accept_confidence == "high" else "LIKELY ACCEPTANCE (VERIFY)"
                        print(f"  {emoji} {label} detected: '{accept_kw}' (confidence={accept_confidence})")
                        for i, record in enumerate(records):
                            if record.get('Address', '').strip().lower() == matched_address:
                                row_num = i + 2
                                current_status = record.get('Status (/Accepted/Rejected/Counter)', '')
                                existing_notes = record.get('Notes', '') or ''
                                try:
                                    alert_sent_col = headers.index('Alert Sent') + 1
                                except ValueError:
                                    alert_sent_col = None

                                if accept_confidence == "high":
                                    # HIGH: write Status to Accepted, note in Notes, alert
                                    if current_status not in ['STP', 'Accepted']:
                                        sheet.update_cell(row_num, status_col, 'Accepted')
                                        accept_note = f"[ACCEPTED: {accept_context} — {datetime.now().strftime('%m/%d/%Y')}]"
                                        new_notes = f"{existing_notes} | {accept_note}" if existing_notes else accept_note
                                        sheet.update_cell(row_num, notes_col, new_notes)
                                        print(f"  Sheet updated to Accepted for row {row_num} (HIGH confidence)")
                                        alerts.append({
                                            'type': 'ACCEPTED',
                                            'address': record.get('Address'),
                                            'purchase_price': clean_price(record.get('Purchase Contract Price', '')),
                                            'counter_price': None, 'difference': None,
                                            'row': row_num, 'alert_col': alert_sent_col,
                                            'reason': accept_context,
                                            'confidence': 'high',
                                        })
                                else:
                                    # LIKELY: do NOT touch Status (false confidence is what burned us on Heaton).
                                    # Only annotate Notes + send amber alert. Dedup: skip if already flagged
                                    # or if the user has manually set a definitive status.
                                    if "[LIKELY ACCEPTED — VERIFY" in existing_notes:
                                        print(f"  Skipping LIKELY alert for row {row_num} — already flagged in Notes")
                                    elif current_status in ['Accepted', 'Rejected', 'STP']:
                                        print(f"  Skipping LIKELY alert for row {row_num} — definitive status already set ({current_status!r})")
                                    else:
                                        likely_note = f"[LIKELY ACCEPTED — VERIFY: {accept_context} — {datetime.now().strftime('%m/%d/%Y')}]"
                                        new_notes = f"{existing_notes} | {likely_note}" if existing_notes else likely_note
                                        sheet.update_cell(row_num, notes_col, new_notes)
                                        print(f"  Notes flagged LIKELY ACCEPTED for row {row_num} (status unchanged: {current_status!r})")
                                        alerts.append({
                                            'type': 'ACCEPTED',
                                            'address': record.get('Address'),
                                            'purchase_price': clean_price(record.get('Purchase Contract Price', '')),
                                            'counter_price': None, 'difference': None,
                                            'row': row_num, 'alert_col': alert_sent_col,
                                            'reason': accept_context,
                                            'confidence': 'likely',
                                            'preserved_status': current_status,
                                        })
                                break
                        # Skip to next email — don't process as counter
                        continue

                    counter_price = extract_counter_price(full_text)
                    counter_date = datetime.now().strftime('%m/%d/%Y')

                    if counter_price:
                        print(f"  Counter price: ${counter_price:,}")
                        for i, record in enumerate(records):
                            if record.get('Address', '').strip().lower() == matched_address:
                                row_num = i + 2

                                # Check if there's already a counter price - save to notes
                                existing_counter = record.get('Counter Price', '')
                                existing_date = record.get('Counter Date', '')
                                existing_notes = record.get('Notes', '')

                                if existing_counter and clean_price(existing_counter):
                                    # Save previous counter to notes
                                    prev_price = clean_price(existing_counter)
                                    prev_price_fmt = f"${prev_price:,}" if prev_price else existing_counter
                                    prev_date_fmt = existing_date if existing_date else "unknown date"
                                    history_note = f"[Previous counter: {prev_price_fmt} on {prev_date_fmt} — updated {datetime.now().strftime('%m/%d/%Y')}]"
                                    if existing_notes:
                                        new_notes = f"{existing_notes} | {history_note}"
                                    else:
                                        new_notes = history_note
                                    sheet.update_cell(row_num, notes_col, new_notes)
                                    print(f"  Previous counter saved to Notes!")

                                # Update with new counter price and date
                                sheet.update_cell(row_num, counter_price_col, f"${counter_price:,}")
                                sheet.update_cell(row_num, counter_date_col, counter_date)
                                sheet.update_cell(row_num, status_col, 'Counter')
                                print(f"  Sheet updated for row {row_num}!")

                                # Check alert thresholds
                                purchase_price = clean_price(record.get('Purchase Contract Price', ''))
                                try:
                                    alert_sent_col = headers.index('Alert Sent') + 1
                                except ValueError:
                                    alert_sent_col = None
                                if purchase_price:
                                    diff = counter_price - purchase_price
                                    if diff <= 0:
                                        alerts.append({'type': 'HOT', 'address': record.get('Address'), 'purchase_price': purchase_price, 'counter_price': counter_price, 'difference': diff, 'row': row_num, 'alert_col': alert_sent_col})
                                    elif diff <= CLOSE_DEAL_THRESHOLD:
                                        alerts.append({'type': 'CLOSE', 'address': record.get('Address'), 'purchase_price': purchase_price, 'counter_price': counter_price, 'difference': diff, 'row': row_num, 'alert_col': alert_sent_col})
                                break
                    else:
                        # Check if it's a rejection
                            rejection_kw, rejection_context = detect_rejection(full_text)
                            if rejection_kw:
                                print(f"  REJECTION detected: '{rejection_kw}'")
                                for i, record in enumerate(records):
                                    if record.get('Address', '').strip().lower() == matched_address:
                                        row_num = i + 2
                                        current_status = record.get('Status (/Accepted/Rejected/Counter)', '')
                                        if current_status not in ['Rejected', 'STP', 'Accepted']:
                                            sheet.update_cell(row_num, status_col, 'Rejected')
                                            existing_notes = record.get('Notes', '')
                                            rejection_note = f"[Rejected: {rejection_context} — {datetime.now().strftime('%m/%d/%Y')}]"
                                            new_notes = f"{existing_notes} | {rejection_note}" if existing_notes else rejection_note
                                            sheet.update_cell(row_num, notes_col, new_notes)
                                            print(f"  Sheet updated to Rejected for row {row_num}!")
                                            try:
                                                alert_sent_col = headers.index('Alert Sent') + 1
                                            except ValueError:
                                                alert_sent_col = None
                                            alerts.append({
                                                'type': 'REJECTED',
                                                'address': record.get('Address'),
                                                'purchase_price': clean_price(record.get('Purchase Contract Price', '')),
                                                'counter_price': None,
                                                'difference': None,
                                                'row': row_num,
                                                'alert_col': alert_sent_col,
                                                'reason': rejection_context
                                            })
                                        break
                            else:
                                print(f"  Could not extract price from email")
                else:
                    print(f"  No match - ignoring email")
            except Exception as e:
                print(f"  Error processing email: {e}")
                continue

        mail.logout()
    except Exception as e:
        print(f"Error connecting to Christian's Gmail: {e}")

    return alerts

# ============================================================
# ARCHIVE STP — WRITE SOLD PRICE
# ============================================================

def write_sold_price_to_archive(address, sold_price):
    try:
        from google.oauth2.service_account import Credentials
        import gspread
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = _load_credentials(scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        stp_sheet = spreadsheet.worksheet('Archive_STP')
        records = stp_sheet.get_all_records()
        for i, record in enumerate(records):
            if record.get('Address', '').strip().lower() == address.lower():
                stp_sheet.update_cell(i + 2, 17, f"${sold_price:,}")
                # Write STP date to column R (18)
                from datetime import datetime as dt
                stp_sheet.update_cell(i + 2, 18, dt.now().strftime('%m/%d/%Y'))
                print(f"  → Sold price ${sold_price:,} + STP Date written to Archive_STP row {i+2}")
                return True
        print(f"  → Address not found in Archive_STP: {address}")
        return False
    except Exception as e:
        print(f"  → Could not write to Archive_STP: {e}")
        return False

# ============================================================
# ============================================================
# ZILLOW STATUS CHECKER — powered by OpenWeb Ninja
# ============================================================

def _check_zillow_status_owin(address, zillow_url=None):
    """Check Zillow status via OpenWeb Ninja (address-based lookup).

    Returns: 'active' | 'pending' | 'sold' | None
    Fails loudly if API key is missing or rate-limited — no Apify fallback.
    """
    try:
        OWIN_KEY = OPENWEB_NINJA_API_KEY
        if not OWIN_KEY:
            print(f"  [OpenWeb] ERROR: OPENWEB_NINJA_API_KEY not set — cannot check status")
            return None

        print(f"  [OpenWeb] Looking up: {address}")

        resp = requests.get(
            "https://api.openwebninja.com/realtime-zillow-data/property-details-address",
            params={"address": address},
            headers={"x-api-key": OWIN_KEY},
            timeout=30,
        )

        if resp.status_code == 404:
            print(f"  [OpenWeb] Not found on Zillow")
            return None
        if resp.status_code == 429:
            print(f"  [OpenWeb] ERROR: Rate limited (429) — skipping property")
            return None
        if resp.status_code not in (200, 201):
            print(f"  [OpenWeb] HTTP {resp.status_code}: {resp.text[:200]}")
            return None

        body = resp.json()
        data = body.get("data") or body
        if not data or not data.get("zpid"):
            if resp.status_code == 200 and body.get("status") == "OK" and not body.get("error"):
                print(f"  [API_EMPTY_RESPONSE] address='{address}' request_id='{body.get('request_id', 'unknown')}' — endpoint returned empty data, treating as unknown")
            else:
                print(f"  [OpenWeb] No results returned")
            return None

        # Verify returned address matches queried address (multi-unit protection)
        returned_addr = data.get("streetAddress") or data.get("address") or ""
        from worker import _verify_address_match
        accept, unit_verified = _verify_address_match(address, returned_addr)
        if not accept:
            print(f"  [OpenWeb] Address mismatch — skipping (queried '{address}', got '{returned_addr}')")
            return None

        home_status = str(data.get("homeStatus", "")).upper()
        print(f"  [OpenWeb] homeStatus={home_status}")

        # Check contingentListingType FIRST
        contingent = str(data.get("contingentListingType", "") or "").upper()
        if "UNDER_CONTRACT" in contingent or "CONTINGENT" in contingent:
            print(f"  [OpenWeb] contingentListingType={contingent} — treating as pending")
            return "pending"

        if "SOLD" in home_status or "RECENTLY_SOLD" in home_status or "FORECLOSED" in home_status or "CLOSED" in home_status:
            return "sold"
        elif "PENDING" in home_status or "UNDER_CONTRACT" in home_status:
            return "pending"
        elif "FOR_SALE" in home_status or "ACTIVE" in home_status:
            sub_type = data.get("listing_sub_type", {}) or {}
            if sub_type.get("is_pending", False):
                print(f"  [OpenWeb] listing_sub_type.is_pending=True — treating as pending")
                return "pending"
            return "active"
        elif home_status == "OTHER":
            print(f"  [OpenWeb] Status OTHER = likely Under Contract, treating as pending")
            return "pending"
        else:
            print(f"  [OpenWeb] Unrecognized status: {home_status}")
            return None

    except Exception as e:
        print(f"  [OpenWeb] Error: {e}")
        return None


# ============================================================
# COUNTER PRICE ALERTS FROM EXISTING SHEET DATA
# ============================================================

def check_existing_counter_alerts(records, sheet, headers):
    alerts = []

    # Get or create Alert Sent column
    if 'Alert Sent' not in headers:
        sheet.update_cell(1, 17, 'Alert Sent')
        headers.append('Alert Sent')
        print("  Added 'Alert Sent' column to sheet")

    alert_sent_col = headers.index('Alert Sent') + 1

    for i, record in enumerate(records):
        address = record.get('Address', '').strip()
        if not address:
            continue

        # Skip if already alerted
        already_alerted = record.get('Alert Sent', '').strip()
        if already_alerted == 'Yes':
            continue

        purchase_price = clean_price(record.get('Purchase Contract Price'))
        counter_price = clean_price(record.get('Counter Price'))
        status = record.get('Status (/Accepted/Rejected/Counter)', '')
        if not purchase_price or not counter_price:
            continue
        if 'counter' not in str(status).lower():
            continue
        diff = counter_price - purchase_price
        if diff <= 0:
            alerts.append({'type': 'HOT', 'address': address, 'purchase_price': purchase_price, 'counter_price': counter_price, 'difference': diff, 'row': i + 2, 'alert_col': alert_sent_col})
        elif diff <= CLOSE_DEAL_THRESHOLD:
            alerts.append({'type': 'CLOSE', 'address': address, 'purchase_price': purchase_price, 'counter_price': counter_price, 'difference': diff, 'row': i + 2, 'alert_col': alert_sent_col})
    return alerts

# ============================================================
# EMAIL SENDER
# ============================================================

def send_email(subject, html_body, property_label=None, alert_type=None):
    """Send an alert via Resend HTTP API.

    Replaces Gmail SMTP, which failed with OSError ENETUNREACH from
    Railway (outbound SMTP egress blocked or IPv6 route pothole — 3
    confirmed silent failures in 3 days on 2026-06-24/25/26). Resend
    uses HTTPS port 443 which Railway always permits.

    Retry + sentinel logic lives in email_sender.send_via_resend.
    Failures write [RESEND_ALERT_FAILED] to stderr, which the worker
    surfaces in Railway logs via _surface_stderr_sentinels (even when
    the subprocess exits 0).

    Also writes to SentNotification audit log.

    Args:
        subject: email subject line
        html_body: HTML content
        property_label: address or identifier for the audit log
        alert_type: 'hot' / 'close' / 'accepted' / 'likely_accepted' /
                    'rejected' / 'back_on_market' — for audit log
    """
    from email_sender import send_via_resend
    sent_ok, error_msg = send_via_resend(ALERT_EMAILS, subject, html_body)

    # Audit log — write to Postgres SentNotification (best-effort; never raises)
    try:
        _audit_log_alert(property_label, alert_type, subject, sent_ok, error_msg)
    except Exception as e:
        sys.stderr.write(f"[AUDIT_LOG_FAILED] {type(e).__name__}: {str(e)[:200]}\n")
        sys.stderr.flush()

    return sent_ok


def _audit_log_alert(property_label, alert_type, subject, sent_ok, error_msg):
    """Write outbound-alert attempt to SentNotification table for audit.

    property_id is left NULL — dealflow_updater works off sheet addresses,
    not PreForeclosure IDs. The property_label goes into email_subject so
    "did the Coral St alert fire today?" is a single grep. notification_type
    keeps the 'gmail_alert_' prefix for backward-compat with existing audit
    rows; semantically these are "outbound user alerts" regardless of the
    underlying provider.
    """
    if not alert_type:
        return  # Skip audit for one-off email_sent flows that didn't pass a type
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from database import get_session, SentNotification
    from datetime import datetime as _dt
    db = get_session()
    try:
        full_subject = f"[{property_label or 'unknown'}] {subject}"[:500]
        db.add(SentNotification(
            property_id=None,
            notification_type=f"gmail_alert_{alert_type}",
            sent_at=_dt.utcnow(),
            email_subject=full_subject,
            email_status="sent" if sent_ok else "failed",
            error_message=error_msg,
        ))
        db.commit()
    finally:
        db.close()

def send_alerts(alerts, back_on_market=[]):
    hot = [a for a in alerts if a['type'] == 'HOT']
    if hot:
        html = "<html><body>"
        html += "<h2 style='color:red;'>🚨 HOT ALERT — Counter At or Below Your Offer!</h2>"
        html += "<p>Move fast on these properties!</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;'>"
        html += "<tr style='background:#ff4444;color:white;'><th>Address</th><th>Your Offer</th><th>Their Counter</th><th>Difference</th><th>Offer to Net Counter (6%)</th></tr>"
        for a in hot:
            offer_to_net = int(a['counter_price'] / 0.94)
            html += f"<tr><td><b>{a['address']}</b></td><td>${a['purchase_price']:,}</td><td>${a['counter_price']:,}</td><td style='color:green;'><b>${abs(a['difference']):,} BELOW your offer!</b></td><td style='color:blue;'><b>${offer_to_net:,}</b></td></tr>"
        html += "</table></body></html>"
        send_email("🚨 HOT ALERT - Counter At or Below Your Offer Price!", html,
                   property_label=", ".join(a['address'] for a in hot[:3]), alert_type='hot')

    close = [a for a in alerts if a['type'] == 'CLOSE']
    if close:
        html = "<html><body>"
        html += "<h2 style='color:orange;'>⚠️ CLOSE DEAL ALERT — Counter Within $30k of Your Offer!</h2>"
        html += "<p>Worth negotiating on these properties!</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;'>"
        html += "<tr style='background:#ff9900;color:white;'><th>Address</th><th>Your Offer</th><th>Their Counter</th><th>Gap</th><th>Offer to Net Counter (6%)</th></tr>"
        for a in close:
            offer_to_net = int(a['counter_price'] / 0.94)
            html += f"<tr><td><b>{a['address']}</b></td><td>${a['purchase_price']:,}</td><td>${a['counter_price']:,}</td><td style='color:orange;'><b>${a['difference']:,} apart</b></td><td style='color:blue;'><b>${offer_to_net:,}</b></td></tr>"
        html += "</table></body></html>"
        send_email("⚠️ CLOSE DEAL ALERT - Counter Within $30k of Your Offer!", html,
                   property_label=", ".join(a['address'] for a in close[:3]), alert_type='close')

    # Split ACCEPTED alerts by confidence — HIGH gets the confident "DEAL WON"
    # email; LIKELY gets a hedged "VERIFY" email so the user knows to check
    # the original message before treating as a real win (Status is unchanged
    # in the sheet for LIKELY — only Notes is flagged).
    high_conf = [a for a in alerts if a['type'] == 'ACCEPTED' and a.get('confidence') != 'likely']
    likely = [a for a in alerts if a['type'] == 'ACCEPTED' and a.get('confidence') == 'likely']

    if high_conf:
        html = "<html><body>"
        html += "<h2 style='color:#22c55e;'>🎉 BID ACCEPTED — DEAL WON!</h2>"
        html += "<p style='font-size:16px;'>Congratulations! The following bid(s) have been accepted:</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;'>"
        html += "<tr style='background:#166534;color:white;'><th>Address</th><th>Your Offer</th><th>Details</th></tr>"
        for a in high_conf:
            offer_str = f"${a['purchase_price']:,}" if a.get('purchase_price') else "N/A"
            html += f"<tr><td><b>{a['address']}</b></td><td>{offer_str}</td><td>{a.get('reason', '')[:150]}</td></tr>"
        html += "</table>"
        html += "<p style='color:#22c55e;font-weight:bold;font-size:18px;'>⚡ TAKE IMMEDIATE ACTION — Proceed to closing!</p>"
        html += "</body></html>"
        send_email("🎉 BID ACCEPTED — " + ", ".join(a['address'] for a in high_conf[:3]), html,
                   property_label=", ".join(a['address'] for a in high_conf[:3]), alert_type='accepted')

    if likely:
        html = "<html><body>"
        html += "<h2 style='color:#f59e0b;'>⚠️ LIKELY ACCEPTED — VERIFY</h2>"
        html += "<p style='font-size:14px;'>The following email(s) matched positive acceptance language but did NOT explicitly name your bid. The keyword may be describing a competing offer being accepted. <b>Open the actual email in Christian's Gmail and confirm before treating as a win.</b></p>"
        html += "<table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;'>"
        html += "<tr style='background:#92400e;color:white;'><th>Address</th><th>Your Offer</th><th>Status (unchanged)</th><th>Matched context (verify against email)</th></tr>"
        for a in likely:
            offer_str = f"${a['purchase_price']:,}" if a.get('purchase_price') else "N/A"
            preserved = a.get('preserved_status') or '(unknown)'
            html += f"<tr><td><b>{a['address']}</b></td><td>{offer_str}</td><td><i>{preserved}</i></td><td style='font-style:italic;color:#475569;'>{a.get('reason', '')[:200]}</td></tr>"
        html += "</table>"
        html += "<p style='color:#92400e;font-weight:bold;font-size:14px;'>Sheet Status was LEFT UNCHANGED. The Notes column has been tagged [LIKELY ACCEPTED — VERIFY] so you can review. After verifying the actual email, manually update Status to Accepted (real win) or Rejected (false positive).</p>"
        html += "</body></html>"
        send_email("⚠️ LIKELY ACCEPTED (VERIFY) — " + ", ".join(a['address'] for a in likely[:3]), html,
                   property_label=", ".join(a['address'] for a in likely[:3]), alert_type='likely_accepted')

    rejected = [a for a in alerts if a['type'] == 'REJECTED']
    if rejected:
        html = "<html><body>"
        html += "<h2 style='color:#dc2626;'>❌ OFFER REJECTED</h2>"
        html += "<p>The following offers were rejected by the seller. Consider resubmitting with better terms or moving on.</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;'>"
        html += "<tr style='background:#dc2626;color:white;'><th>Address</th><th>Your Offer</th><th>Reason</th></tr>"
        for a in rejected:
            offer_str = f"${a['purchase_price']:,}" if a.get('purchase_price') else "N/A"
            html += f"<tr><td><b>{a['address']}</b></td><td>{offer_str}</td><td>{a.get('reason', 'Unknown')}</td></tr>"
        html += "</table></body></html>"
        send_email("❌ Offer Rejected — " + ", ".join(a['address'] for a in rejected[:3]), html,
                   property_label=", ".join(a['address'] for a in rejected[:3]), alert_type='rejected')

    if back_on_market:
        hud = [a for a in back_on_market if str(a.get('lead_source','')).upper() == 'HUD']
        other = [a for a in back_on_market if str(a.get('lead_source','')).upper() != 'HUD']
        html = "<html><body>"
        html += "<h2 style='color:blue;'>🔄 BACK ON MARKET — Previously Pending Properties!</h2>"
        html += "<p>These properties fell out of escrow and are back for sale. Consider resubmitting!</p>"
        if hud:
            html += "<h3 style='color:red;background:#fff3cd;padding:8px;'>🚨 HUD PROPERTIES — RESUBMIT IMMEDIATELY!</h3>"
            html += "<table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;margin-bottom:20px;'>"
            html += "<tr style='background:#ff4444;color:white;'><th>Address</th><th>Your Offer</th><th>ARV</th><th>Status</th></tr>"
            for a in hud:
                html += f"<tr style='background:#fff3cd;'><td><b>🏠 {a['address']}</b></td><td>${a['purchase_price']:,}</td><td>{a['arv']}</td><td style='color:red;'><b>⚡ HUD - RESUBMIT NOW!</b></td></tr>"
            html += "</table>"
        if other:
            html += "<h3 style='color:blue;'>Other Properties Back on Market</h3>"
            html += "<table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;'>"
            html += "<tr style='background:#1a73e8;color:white;'><th>Address</th><th>Your Offer</th><th>ARV</th><th>Status</th></tr>"
            for a in other:
                html += f"<tr><td><b>{a['address']}</b></td><td>${a['purchase_price']:,}</td><td>{a['arv']}</td><td style='color:blue;'><b>Verify & Consider Resubmitting</b></td></tr>"
            html += "</table>"
        html += "</body></html>"
        subject = "🚨 HUD BACK ON MARKET!" if hud else "🔄 BACK ON MARKET - Previously Pending Properties!"
        send_email(subject, html,
                   property_label=", ".join(a['address'] for a in back_on_market[:3]), alert_type='back_on_market')

# ============================================================
# GMAIL-ONLY RUN (counters only, no Zillow)
# ============================================================

def run_gmail_only(sheet, records, headers):
    print("\n[MODE: Gmail-Only — Skipping Zillow]")

    # Step 1: Read Christian's Gmail
    email_alerts = read_christian_emails(sheet, records)
    records = sheet.get_all_records()

    # Step 2: Check existing counter alerts
    print("\n--- Checking Existing Counter Prices ---")
    existing_alerts = check_existing_counter_alerts(records, sheet, headers)

    # Combine and deduplicate
    all_alerts = email_alerts + existing_alerts
    seen = set()
    unique_alerts = []
    for a in all_alerts:
        if a['address'] not in seen:
            seen.add(a['address'])
            unique_alerts.append(a)

    # Send alerts
    if unique_alerts:
        print(f"\nSending {len(unique_alerts)} alert(s)...")
        send_alerts(unique_alerts)
        for a in unique_alerts:
            if 'row' in a and 'alert_col' in a and a['alert_col']:
                sheet.update_cell(a['row'], a['alert_col'], 'Yes')
                print(f"  Marked Alert Sent for: {a['address']}")
    else:
        print("\nNo new alerts.")

# ============================================================
# FULL RUN (Gmail + Zillow + Alerts)
# ============================================================

def run_full(sheet, records, headers, status_col):
    print("\n[MODE: Full Run — Gmail + Zillow + Alerts]")

    # Step 1: Read Christian's Gmail
    email_alerts = read_christian_emails(sheet, records)
    records = sheet.get_all_records()

    # Step 2: Check existing counter alerts
    print("\n--- Checking Existing Counter Prices ---")
    existing_alerts = check_existing_counter_alerts(records, sheet, headers)

    # Combine and deduplicate alerts
    all_alerts = email_alerts + existing_alerts
    seen = set()
    unique_alerts = []
    for a in all_alerts:
        if a['address'] not in seen:
            seen.add(a['address'])
            unique_alerts.append(a)

    # Step 3: Check Zillow status
    print("\n--- Checking Zillow Status ---")
    updated_count = 0
    back_on_market = []

    # Load Zillow URLs from sheet hyperlinks
    zillow_url_map = get_zillow_urls_from_sheet()

    for i, record in enumerate(records):
        address = record.get('Address', '').strip()
        if not address or len(address) < 10:
            continue
        current_status = record.get('Status (/Accepted/Rejected/Counter)', '')
        if current_status in ['Accepted', 'Rejected', 'Closed', 'STP']:
            continue

        # Get Zillow URL from hyperlink map
        zillow_url = zillow_url_map.get(address.lower())
        if zillow_url and '?' in zillow_url:
            zillow_url = zillow_url.split('?')[0]

        print(f"[{i+1}/{len(records)}] {address}")
        zillow_status = _check_zillow_status_owin(address, zillow_url=zillow_url)

        if zillow_status == 'sold':
            if current_status not in ['STP', 'Closed', 'Accepted', 'Rejected']:
                print(f"  → Sold! Updating to STP")
                sheet.update_cell(i + 2, status_col, 'STP')
                updated_count += 1
                # Write STP date to main sheet
                if 'STP Date' in headers:
                    stp_date_col = headers.index('STP Date') + 1
                    sheet.update_cell(i + 2, stp_date_col, datetime.now().strftime('%m/%d/%Y'))
                # Write to Archive_STP
                write_sold_price_to_archive(address, None)
        elif zillow_status == 'active':
            if current_status in ['Pending', 'Resubmit']:
                # Skip if alert already sent for this property
                already_alerted = record.get('Alert Sent', '').strip()
                if already_alerted == 'Yes':
                    print(f"  → Active but alert already sent — skipping")
                    continue
                print(f"  → Zillow shows Active — sending alert to verify manually")
                updated_count += 1
                purchase_price = clean_price(record.get('Purchase Contract Price', ''))
                alert_sent_col = headers.index('Alert Sent') + 1 if 'Alert Sent' in headers else None
                back_on_market.append({
                    'address': address,
                    'purchase_price': purchase_price or 0,
                    'arv': record.get('ARV', 'N/A'),
                        'lead_source': record.get('Lead Source', ''),
                    'row': i + 2,
                    'alert_col': alert_sent_col
                })
            else:
                print(f"  → Active (no change needed)")
        elif zillow_status == 'pending':
            if current_status == 'Sent':
                print(f"  → Now Pending on Zillow, was Sent → updating to Pending")
                sheet.update_cell(i + 2, status_col, 'Pending')
                updated_count += 1
            elif current_status == 'Pending':
                print(f"  → Still Pending (no change)")
            else:
                print(f"  → Pending on Zillow, status is {current_status} (no change)")
        else:
            print(f"  → Could not determine status")

        time.sleep(2)

    # Send all alerts
    if unique_alerts or back_on_market:
        print(f"\nSending alerts...")
        send_alerts(unique_alerts, back_on_market)
        for a in unique_alerts + back_on_market:
            if 'row' in a and 'alert_col' in a and a['alert_col']:
                sheet.update_cell(a['row'], a['alert_col'], 'Yes')
                print(f"  Marked Alert Sent for: {a['address']}")
    else:
        print("\nNo alerts to send.")

    print(f"\nDone! Updated {updated_count} properties.")

# ============================================================
# TEST MODE — pass/fail report, zero sheet writes
# ============================================================

def run_test():
    results = []

    def check(label, fn):
        try:
            result = fn()
            status = 'PASS' if result else 'FAIL'
            detail = result if isinstance(result, str) else ''
        except Exception as e:
            status = 'FAIL'
            detail = str(e)
        results.append((label, status, detail))
        icon = '✅' if status == 'PASS' else '❌'
        print(f"  {icon}  {label:<40} {status}  {detail}")

    print("\n" + "="*60)
    print("DealFlow Updater — TEST MODE")
    print("Read-only. Nothing will be written to the sheet.")
    print("="*60 + "\n")

    # ── 1. Google Sheet connection ──────────────────────────
    print("[ 1 / 4 ] Google Sheet")
    sheet_obj = [None]
    def test_sheet():
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = _load_credentials(scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).sheet1
        sheet_obj[0] = sheet
        records = sheet.get_all_records()
        headers = sheet.row_values(1)
        required = ['Address', 'Status (/Accepted/Rejected/Counter)',
                    'Counter Price', 'Counter Date', 'Notes']
        missing = [c for c in required if c not in headers]
        if missing:
            return f"Connected but missing columns: {missing}"
        return f"Connected — {len(records)} rows, all required columns found"
    check("Sheet connection + columns", test_sheet)

    # ── 2. Christian's Gmail (IMAP read) ───────────────────
    print("\n[ 2 / 4 ] Christian's Gmail (unmatched.dealflow@gmail.com)")
    def test_christian_gmail():
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(CHRISTIAN_GMAIL, CHRISTIAN_APP_PASSWORD)
        mail.select('inbox')
        status, messages = mail.search(None, 'UNSEEN')
        count = len(messages[0].split()) if messages[0] else 0
        mail.logout()
        return f"Login OK — {count} unread email(s) in inbox"
    check("IMAP login + inbox access", test_christian_gmail)

    # ── 3. Your Gmail SMTP (outbound alerts) ───────────────
    print("\n[ 3 / 4 ] Your Gmail SMTP (dealflow.alerts@gmail.com)")
    def test_smtp():
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(YOUR_GMAIL, YOUR_APP_PASSWORD)
        return "SMTP login OK — outbound email ready"
    check("SMTP login (outbound alerts)", test_smtp)

    # ── 4. OpenWeb Ninja Zillow lookup ──────────────────────
    print("\n[ 4 / 4 ] OpenWeb Ninja Zillow API")
    def test_owin():
        key = OPENWEB_NINJA_API_KEY
        if not key:
            raise Exception("OPENWEB_NINJA_API_KEY not set")
        r = requests.get(
            "https://api.openwebninja.com/realtime-zillow-data/property-details-address",
            params={"address": "3524 E Elgin St, Gilbert, AZ 85295"},
            headers={"x-api-key": key},
            timeout=30,
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"HTTP {r.status_code}: {r.text[:200]}")
        data = r.json().get("data") or r.json()
        if not data or not data.get("zpid"):
            raise Exception("No data returned")
        return f"OK — zpid={data.get('zpid')} homeStatus={data.get('homeStatus')}"
    check("Zillow lookup via OpenWeb Ninja", test_owin)

    # ── Summary ────────────────────────────────────────────
    passed = sum(1 for _, s, _ in results if s == 'PASS')
    total  = len(results)
    print("\n" + "="*60)
    print(f"  Result: {passed}/{total} checks passed")
    if passed == total:
        print("  All systems go — safe to run full mode.")
    else:
        print("  Fix the failing checks above before running full mode.")
    print("="*60 + "\n")



# ============================================================
# REFRESH STP REPORT
# ============================================================

def run_refresh_report():
    print("\n[MODE: Refresh STP Report]")
    from google.oauth2.service_account import Credentials
    import gspread

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = _load_credentials(scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    stp_report = spreadsheet.worksheet('STP Report')
    stp_report.clear()
    stp_sheet = spreadsheet.worksheet('Archive_STP')
    records = stp_sheet.get_all_records()
    print(f"Found {len(records)} STP records")

    def parse_price(val):
        if not val:
            return None
        try:
            return int(float(str(val).replace('$','').replace(',','')))
        except:
            return None

    def parse_month(date_str):
        if not date_str:
            return None
        date_str = str(date_str).strip().split(' ')[0]
        for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y']:
            try:
                return datetime.strptime(date_str, fmt)
            except:
                continue
        return None

    months = {}
    all_props = [r for r in records if r.get('Status (/Accepted/Rejected/Counter)','').strip() == 'STP']
    for r in all_props:
        date_str = r.get('STP Date', '') or r.get('Sent Date', '') or r.get('Date Entered', '')
        dt = parse_month(str(date_str))
        if dt:
            key = dt.strftime('%Y-%m')
            label = dt.strftime('%B %Y')
            if key not in months:
                months[key] = label

    months_sorted = sorted(months.items())
    print(f"Found months: {[m[1] for m in months_sorted]}")

    all_data = []
    all_data.append(['STP REPORT — Deal Performance Analysis', '', '', '', '', '', ''])
    all_data.append([f'Last updated: {datetime.now().strftime("%m/%d/%Y %I:%M %p")}', '', '', '', '', '', ''])
    all_data.append(['', '', '', '', '', '', ''])

    total_stp = len(all_props)
    offers = [parse_price(r.get('Purchase Contract Price')) for r in all_props]
    sold_prices = [parse_price(r.get('Sold Price')) for r in all_props]
    offers_valid = [p for p in offers if p]
    sold_valid = [p for p in sold_prices if p]
    avg_offer = int(sum(offers_valid)/len(offers_valid)) if offers_valid else 0
    avg_sold = int(sum(sold_valid)/len(sold_valid)) if sold_valid else 0
    avg_gap = int(avg_sold - avg_offer) if avg_offer and avg_sold else 0

    hud = len([r for r in all_props if str(r.get('Lead Source','')).upper() == 'HUD'])
    reo = len([r for r in all_props if str(r.get('Lead Source','')).upper() == 'REO'])
    other = len([r for r in all_props if str(r.get('Lead Source','')).upper() not in ['HUD','REO']])

    all_data.append(['📊 OVERALL STATS', '', '', '', '', '', ''])
    all_data.append(['Total STPs', total_stp, '', 'Lead Source Breakdown', '', '', ''])
    all_data.append(['Avg Offer Price', f'${avg_offer:,}' if avg_offer else 'N/A', '', 'HUD', hud, '', ''])
    all_data.append(['Avg Sold Price', f'${avg_sold:,}' if avg_sold else 'N/A', '', 'REO', reo, '', ''])
    all_data.append(['Avg Gap', f'${avg_gap:,}' if avg_gap else 'N/A (no sold prices yet)', '', 'Other', other, '', ''])
    all_data.append(['', '', '', '', '', '', ''])

    for month_key, month_label in months_sorted:
        month_records = []
        for r in all_props:
            date_str = r.get('STP Date', '') or r.get('Sent Date', '') or r.get('Date Entered', '')
            dt = parse_month(str(date_str))
            if dt and dt.strftime('%Y-%m') == month_key:
                month_records.append(r)

        if not month_records:
            continue

        all_data.append([f'📅 {month_label.upper()}', '', '', '', '', '', ''])
        all_data.append(['Address', 'Lead Source', 'Your Offer', 'Sold Price', 'Difference', '% Gap', 'Notes'])

        month_offers = []
        month_sold = []

        for r in month_records:
            address = r.get('Address', '')
            lead = r.get('Lead Source', '')
            offer = parse_price(r.get('Purchase Contract Price'))
            sold = parse_price(r.get('Sold Price'))
            notes = r.get('Notes', '')

            offer_str = f'${offer:,}' if offer else 'N/A'
            if sold:
                sold_str = f'${sold:,}'
                diff = sold - offer if offer else None
                diff_str = f'${diff:,}' if diff is not None else 'N/A'
                pct = f'{((diff/offer)*100):.1f}%' if offer and diff is not None else 'N/A'
                month_offers.append(offer)
                month_sold.append(sold)
            else:
                sold_str = 'Sold price pending'
                diff_str = 'N/A'
                pct = 'N/A'
                if offer:
                    month_offers.append(offer)

            all_data.append([address, lead, offer_str, sold_str, diff_str, pct, notes[:50] if notes else ''])

        m_avg_offer = int(sum(month_offers)/len(month_offers)) if month_offers else 0
        m_avg_sold = int(sum(month_sold)/len(month_sold)) if month_sold else 0
        m_avg_gap = m_avg_sold - m_avg_offer if m_avg_offer and m_avg_sold else 0
        all_data.append([f'Month Total: {len(month_records)} STPs', '',
                         f'Avg: ${m_avg_offer:,}' if m_avg_offer else '',
                         f'Avg: ${m_avg_sold:,}' if m_avg_sold else 'No sold prices yet',
                         f'Avg gap: ${m_avg_gap:,}' if m_avg_gap else '', '', ''])
        all_data.append(['', '', '', '', '', '', ''])

    stp_report.update(values=all_data, range_name='A1')

    # Apply formatting
    requests = [
        {"updateDimensionProperties": {"range": {"sheetId": stp_report.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 280}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": stp_report.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 100}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": stp_report.id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 3}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": stp_report.id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": 4}, "properties": {"pixelSize": 150}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": stp_report.id, "dimension": "COLUMNS", "startIndex": 4, "endIndex": 5}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": stp_report.id, "dimension": "COLUMNS", "startIndex": 5, "endIndex": 6}, "properties": {"pixelSize": 80}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": stp_report.id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 7}, "properties": {"pixelSize": 300}, "fields": "pixelSize"}},
        {"repeatCell": {"range": {"sheetId": stp_report.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.1, "green": 0.2, "blue": 0.5}, "textFormat": {"bold": True, "fontSize": 16, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}}}, "fields": "userEnteredFormat"}},
        {"repeatCell": {"range": {"sheetId": stp_report.id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2}, "textFormat": {"bold": True, "fontSize": 12, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}}}, "fields": "userEnteredFormat"}},
        {"updateSheetProperties": {"properties": {"sheetId": stp_report.id, "gridProperties": {"frozenRowCount": 1}}, "fields": "gridProperties.frozenRowCount"}},
    ]

    all_values = stp_report.get_all_values()
    for i, row in enumerate(all_values):
        if row and row[0].startswith('📅'):
            requests.append({"repeatCell": {"range": {"sheetId": stp_report.id, "startRowIndex": i, "endRowIndex": i+1, "startColumnIndex": 0, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.1, "green": 0.4, "blue": 0.7}, "textFormat": {"bold": True, "fontSize": 12, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}}}, "fields": "userEnteredFormat"}})
        elif row and row[0] == 'Address':
            requests.append({"repeatCell": {"range": {"sheetId": stp_report.id, "startRowIndex": i, "endRowIndex": i+1, "startColumnIndex": 0, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.7, "green": 0.85, "blue": 1.0}, "textFormat": {"bold": True}}}, "fields": "userEnteredFormat"}})
        elif row and row[0].startswith('Month Total'):
            requests.append({"repeatCell": {"range": {"sheetId": stp_report.id, "startRowIndex": i, "endRowIndex": i+1, "startColumnIndex": 0, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}, "textFormat": {"bold": True, "italic": True}}}, "fields": "userEnteredFormat"}})

    spreadsheet.batch_update({"requests": requests})
    print(f"\n✅ STP Report refreshed with {len(all_data)} rows and formatting applied!")

# ============================================================
# MAIN
# ============================================================

def main():
    # Determine mode: CLI arg takes priority, else schedule logic
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        mode = get_run_mode_from_schedule()

    now_pst = datetime.now(PST)
    print("\n" + "="*60)
    print(f"DealFlow Updater — {now_pst.strftime('%Y-%m-%d %I:%M %p %Z')}")
    print(f"Run mode: {mode.upper()}")
    print("="*60 + "\n")

    if mode == 'test':
        run_test()
        return

    if mode == 'skip':
        print("Outside scheduled hours (9AM–8PM PST). Nothing to do.")
        return

    sheet = connect_to_sheet()
    records = sheet.get_all_records()
    print(f"Found {len(records)} properties in sheet.")

    headers = sheet.row_values(1)
    try:
        status_col = headers.index('Status (/Accepted/Rejected/Counter)') + 1
    except ValueError:
        print("Could not find Status column!")
        return

    if mode == 'gmail_only':
        run_gmail_only(sheet, records, headers)
    elif mode == 'refresh_report':
        run_refresh_report()
        return
    else:
        # 'full' or any unrecognized arg defaults to full
        run_full(sheet, records, headers, status_col)

    print("\n" + "="*60)
    print("All done!")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
