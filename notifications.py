"""
DealFlow Auction Notifications — Email alerts for foreclosure auction dates.

Uses existing Gmail SMTP credentials from alerts.py.
Logs all send attempts to sent_notifications table.
"""

import os
import time
import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "gescobarrei@gmail.com")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://dealflow-ai.up.railway.app")


def _send_email(subject, body_html, body_text):
    """Send an email via Resend HTTP API.

    Was SMTP_SSL to smtp.gmail.com — replaced because outbound SMTP
    fails from Railway with OSError ENETUNREACH (3 confirmed silent
    failures 2026-06-24/25/26). Resend uses HTTPS port 443 which
    Railway always permits.

    Returns True if sent, False on failure. Does NOT log to
    sent_notifications (caller handles that via log_notification).
    """
    from email_sender import send_via_resend
    sent_ok, error = send_via_resend([ALERT_EMAIL], subject, body_html, body_text)
    if sent_ok:
        logger.info(f"Email sent: {subject}")
    else:
        logger.error(f"Email failed: {subject} — {error}")
    return sent_ok


def log_notification(db, property_id, notification_type, subject, sent_ok, error_msg=None):
    """Log a notification attempt to the sent_notifications table.

    Reusable by worker.py crons and any code that sends notifications.
    Respects the UNIQUE(property_id, notification_type) constraint —
    skips insert if a record already exists for that pair.
    """
    from database import SentNotification
    try:
        existing = db.query(SentNotification).filter_by(
            property_id=property_id, notification_type=notification_type
        ).first()
        if existing:
            # Update existing record (e.g., retry after failure)
            existing.sent_at = datetime.utcnow()
            existing.email_subject = subject
            existing.email_status = "sent" if sent_ok else "failed"
            existing.error_message = error_msg
        else:
            db.add(SentNotification(
                property_id=property_id,
                notification_type=notification_type,
                sent_at=datetime.utcnow(),
                email_subject=subject,
                email_status="sent" if sent_ok else "failed",
                error_message=error_msg,
            ))
        db.commit()
    except Exception as e:
        logger.error(f"Failed to log notification for property {property_id}: {e}")
        db.rollback()


def was_notification_sent(db, property_id, notification_type):
    """Check if a notification was already successfully sent for this property+type.

    Used by crons to avoid duplicate emails.
    """
    from database import SentNotification
    existing = db.query(SentNotification).filter_by(
        property_id=property_id,
        notification_type=notification_type,
        email_status="sent",
    ).first()
    return existing is not None


def _days_until(auction_time):
    """Calculate days until auction from a datetime. Negative = past."""
    if not auction_time:
        return None
    now = datetime.utcnow()
    delta = auction_time - now
    return delta.days


def _format_date(dt_val):
    """Format a datetime for email display."""
    if not dt_val:
        return "N/A"
    return dt_val.strftime("%B %d, %Y at %I:%M %p")


def _format_money(val):
    """Format a number as currency."""
    if val is None:
        return "N/A"
    return f"${val:,.0f}"


def _property_dashboard_link(pf):
    """Build a link to the property in the dashboard."""
    return f"{DASHBOARD_URL}/#preforeclosure"


# ── Template A: Auction Scheduled ──────────────────────────────────────

def send_auction_scheduled(pf):
    """Send immediate email when an auction date is first detected.

    Args:
        pf: PreForeclosure ORM object with foreclosure_auction_time set.
    Returns:
        True if sent, False on failure or skip.
    """
    if not pf.foreclosure_auction_time:
        logger.warning(f"send_auction_scheduled: no auction time for {pf.address} — skipping")
        return False

    days = _days_until(pf.foreclosure_auction_time)
    days_text = f"{days} days from now" if days and days > 0 else "DATE HAS PASSED" if days is not None else "unknown"
    address = f"{pf.address}, {pf.city or ''}, {pf.state or 'CA'} {pf.zip_code or ''}"
    subject = f"New auction scheduled: {pf.address}"
    link = _property_dashboard_link(pf)

    body_html = f"""
    <html>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;margin:0 auto;">
        <div style="background:#1e293b;color:white;padding:20px;border-radius:12px 12px 0 0;">
            <h2 style="margin:0;font-size:18px;">New Auction Scheduled</h2>
        </div>
        <div style="padding:20px;background:white;border:1px solid #e2e8f0;border-top:none;">
            <h3 style="color:#1e293b;margin:0 0 12px;">{address}</h3>
            <table style="width:100%;border-collapse:collapse;">
                <tr style="border-bottom:1px solid #f1f5f9;">
                    <td style="padding:8px 0;color:#64748b;">Auction Date</td>
                    <td style="padding:8px 0;text-align:right;font-weight:600;">{_format_date(pf.foreclosure_auction_time)}</td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9;">
                    <td style="padding:8px 0;color:#64748b;">Days Until Auction</td>
                    <td style="padding:8px 0;text-align:right;font-weight:600;color:{'#dc2626' if days is None or days < 14 else '#f59e0b' if days < 30 else '#22c55e'};">{days_text}</td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9;">
                    <td style="padding:8px 0;color:#64748b;">Foreclosing Bank</td>
                    <td style="padding:8px 0;text-align:right;font-weight:600;">{pf.foreclosing_bank or 'N/A'}</td>
                </tr>
                <tr style="border-bottom:1px solid #f1f5f9;">
                    <td style="padding:8px 0;color:#64748b;">Auction Location</td>
                    <td style="padding:8px 0;text-align:right;font-weight:600;">{pf.foreclosure_auction_location or 'N/A'}{(', ' + pf.foreclosure_auction_city) if pf.foreclosure_auction_city else ''}</td>
                </tr>
                <tr>
                    <td style="padding:8px 0;color:#64748b;">Unpaid Balance</td>
                    <td style="padding:8px 0;text-align:right;font-weight:600;">{_format_money(pf.foreclosure_unpaid_balance)}</td>
                </tr>
            </table>
            <div style="margin-top:16px;text-align:center;">
                <a href="{link}" style="display:inline-block;background:#3b82f6;color:white;padding:10px 24px;border-radius:8px;text-decoration:none;font-weight:600;">View in Dashboard</a>
            </div>
        </div>
        <div style="background:#f1f5f9;padding:12px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px;text-align:center;color:#94a3b8;font-size:12px;">
            DealFlow AI — Pre-Foreclosure Monitor
        </div>
    </body>
    </html>
    """

    body_text = (
        f"New Auction Scheduled\n\n"
        f"Property: {address}\n"
        f"Auction Date: {_format_date(pf.foreclosure_auction_time)}\n"
        f"Days Until Auction: {days_text}\n"
        f"Foreclosing Bank: {pf.foreclosing_bank or 'N/A'}\n"
        f"Location: {pf.foreclosure_auction_location or 'N/A'}"
        f"{(', ' + pf.foreclosure_auction_city) if pf.foreclosure_auction_city else ''}\n"
        f"Unpaid Balance: {_format_money(pf.foreclosure_unpaid_balance)}\n\n"
        f"View in Dashboard: {link}\n"
    )

    return _send_email(subject, body_html, body_text)


# ── Template B: Auction Digest ─────────────────────────────────────────

def send_auction_digest(properties):
    """Send daily digest of properties with auctions in the next 7 days.

    Args:
        properties: list of PreForeclosure ORM objects, sorted by auction time ASC.
    Returns:
        True if sent, False on failure. Returns False if list is empty (no email sent).
    """
    if not properties:
        return False

    count = len(properties)
    subject = f"Auction alert: {count} propert{'y' if count == 1 else 'ies'} with auctions in next 7 days"
    link = _property_dashboard_link(properties[0])

    # Build HTML rows
    rows_html = ""
    rows_text = ""
    for i, pf in enumerate(properties, 1):
        days = _days_until(pf.foreclosure_auction_time)
        days_str = f"{days}d" if days is not None else "?"
        address = f"{pf.address}, {pf.city or ''}"
        color = "#dc2626" if days is None or days < 14 else "#f59e0b" if days < 30 else "#22c55e"

        rows_html += f"""
        <tr style="border-bottom:1px solid #f1f5f9;">
            <td style="padding:10px 8px;font-weight:500;">{address}</td>
            <td style="padding:10px 8px;text-align:center;color:{color};font-weight:700;">{days_str}</td>
            <td style="padding:10px 8px;text-align:right;font-size:13px;">{_format_date(pf.foreclosure_auction_time)}</td>
            <td style="padding:10px 8px;text-align:right;font-size:13px;color:#64748b;">{pf.foreclosing_bank or 'N/A'}</td>
        </tr>
        """
        rows_text += f"  {i}. {address} — {days_str} — {_format_date(pf.foreclosure_auction_time)} — {pf.foreclosing_bank or 'N/A'}\n"

    body_html = f"""
    <html>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:700px;margin:0 auto;">
        <div style="background:#991b1b;color:white;padding:20px;border-radius:12px 12px 0 0;">
            <h2 style="margin:0;font-size:18px;">Auction Alert: {count} Upcoming</h2>
            <p style="margin:4px 0 0;opacity:0.8;font-size:13px;">Properties with auctions in the next 7 days</p>
        </div>
        <div style="padding:16px;background:white;border:1px solid #e2e8f0;border-top:none;">
            <table style="width:100%;border-collapse:collapse;font-size:14px;">
                <tr style="background:#f8fafc;font-size:12px;color:#64748b;text-transform:uppercase;">
                    <th style="padding:8px;text-align:left;">Property</th>
                    <th style="padding:8px;text-align:center;">Days</th>
                    <th style="padding:8px;text-align:right;">Auction Date</th>
                    <th style="padding:8px;text-align:right;">Bank</th>
                </tr>
                {rows_html}
            </table>
            <div style="margin-top:16px;text-align:center;">
                <a href="{link}" style="display:inline-block;background:#dc2626;color:white;padding:10px 24px;border-radius:8px;text-decoration:none;font-weight:600;">View All in Dashboard</a>
            </div>
        </div>
        <div style="background:#f1f5f9;padding:12px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px;text-align:center;color:#94a3b8;font-size:12px;">
            DealFlow AI — Pre-Foreclosure Monitor
        </div>
    </body>
    </html>
    """

    body_text = (
        f"Auction Alert: {count} Upcoming\n"
        f"Properties with auctions in the next 7 days:\n\n"
        f"{rows_text}\n"
        f"View in Dashboard: {link}\n"
    )

    return _send_email(subject, body_html, body_text)
