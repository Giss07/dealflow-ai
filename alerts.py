"""
DealFlow Email Alerts — Send alerts for high-scoring NEW deals via Gmail SMTP.
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "gescobarrei@gmail.com")
SCORE_THRESHOLD = 80


def build_zillow_url(listing):
    """Build Zillow search URL for the property."""
    address = listing.get("address", "")
    city = listing.get("city", "")
    state = listing.get("state", "CA")
    zip_code = listing.get("zip_code", "")
    full = f"{address}, {city}, {state} {zip_code}"
    return f"https://www.zillow.com/homes/{quote_plus(full)}_rb/"


def fmt(val):
    """Format number as currency."""
    if val is None:
        return "N/A"
    return f"${val:,.0f}"


def build_alert_email(listing):
    """Build HTML email for a high-scoring deal."""
    address = f"{listing.get('address', 'N/A')}, {listing.get('city', '')}, {listing.get('state', 'CA')} {listing.get('zip_code', '')}"
    score = listing.get("score", 0)
    price = listing.get("price")
    arv = listing.get("arv")
    days = listing.get("days_on_zillow")
    reasoning = listing.get("score_reasoning", "")
    zillow_url = build_zillow_url(listing)
    privy_url = listing.get("privy_url", "#")

    # Get offer analysis
    offer = listing.get("offer_analysis", {})
    if isinstance(offer, str):
        import json
        try:
            offer = json.loads(offer)
        except Exception:
            offer = {}

    max_offer = offer.get("max_offer") or listing.get("max_offer")
    profit = offer.get("estimated_profit") or listing.get("estimated_profit")

    # Repairs
    repairs_mid = listing.get("repairs_mid", 0) or 0
    repairs_worst = listing.get("repairs_worst", 0) or 0
    repairs = round((repairs_mid + repairs_worst) / 2) if (repairs_mid or repairs_worst) else None

    # Score color
    if score >= 80:
        score_color = "#22c55e"
        score_bg = "#166534"
    elif score >= 60:
        score_color = "#fbbf24"
        score_bg = "#854d0e"
    else:
        score_color = "#fca5a5"
        score_bg = "#991b1b"

    html = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 600px; margin: 0 auto; background: #f8fafc;">
        <div style="background: #1e293b; color: white; padding: 24px; border-radius: 12px 12px 0 0;">
            <h1 style="margin: 0; font-size: 22px;">DealFlow Alert</h1>
            <p style="margin: 5px 0 0; opacity: 0.7; font-size: 14px;">New high-scoring deal found</p>
        </div>

        <div style="padding: 24px; background: white; border: 1px solid #e2e8f0; border-top: none;">
            <div style="display: inline-block; background: {score_bg}; color: {score_color}; padding: 8px 20px; border-radius: 20px; font-size: 22px; font-weight: bold; margin-bottom: 16px;">
                {score}/100
            </div>

            <h2 style="color: #1e293b; margin: 0 0 16px 0; font-size: 18px;">{address}</h2>

            <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                <tr style="border-bottom: 1px solid #f1f5f9;">
                    <td style="padding: 10px 0; color: #64748b; font-size: 14px;">Price</td>
                    <td style="padding: 10px 0; text-align: right; font-weight: 600; font-size: 14px;">{fmt(price)}</td>
                </tr>
                <tr style="border-bottom: 1px solid #f1f5f9;">
                    <td style="padding: 10px 0; color: #64748b; font-size: 14px;">ARV</td>
                    <td style="padding: 10px 0; text-align: right; font-weight: 600; font-size: 14px;">{fmt(arv)}</td>
                </tr>
                <tr style="border-bottom: 1px solid #f1f5f9;">
                    <td style="padding: 10px 0; color: #64748b; font-size: 14px;">Est. Repairs</td>
                    <td style="padding: 10px 0; text-align: right; font-weight: 600; font-size: 14px;">{fmt(repairs)}</td>
                </tr>
                <tr style="border-bottom: 1px solid #f1f5f9;">
                    <td style="padding: 10px 0; color: #64748b; font-size: 14px;">Max Offer</td>
                    <td style="padding: 10px 0; text-align: right; font-weight: 600; color: #3b82f6; font-size: 14px;">{fmt(max_offer)}</td>
                </tr>
                <tr style="border-bottom: 1px solid #f1f5f9;">
                    <td style="padding: 10px 0; color: #64748b; font-size: 14px;">Est. Profit</td>
                    <td style="padding: 10px 0; text-align: right; font-weight: 600; color: #22c55e; font-size: 14px;">{fmt(profit)}</td>
                </tr>
                <tr>
                    <td style="padding: 10px 0; color: #64748b; font-size: 14px;">Days on Market</td>
                    <td style="padding: 10px 0; text-align: right; font-weight: 600; font-size: 14px;">{days if days is not None else 'N/A'}</td>
                </tr>
            </table>

            <p style="color: #64748b; font-size: 13px; margin-bottom: 20px; padding: 10px; background: #f8fafc; border-radius: 6px;">
                {reasoning}
            </p>

            <div style="text-align: center;">
                <a href="{zillow_url}" style="display: inline-block; background: #3b82f6; color: white; padding: 12px 28px; border-radius: 8px; text-decoration: none; font-weight: 600; font-size: 14px; margin-right: 8px;">View on Zillow</a>
                <a href="{privy_url}" style="display: inline-block; background: #8b5cf6; color: white; padding: 12px 28px; border-radius: 8px; text-decoration: none; font-weight: 600; font-size: 14px;">Open in Privy</a>
            </div>
        </div>

        <div style="background: #f1f5f9; padding: 16px; border: 1px solid #e2e8f0; border-top: none; border-radius: 0 0 12px 12px; text-align: center; color: #94a3b8; font-size: 12px;">
            DealFlow AI — Inland Empire Deal Finder
        </div>
    </body>
    </html>
    """
    return html


def send_alert(listing):
    """Send email alert for a single deal."""
    if not GMAIL_USER or not GMAIL_PASSWORD:
        logger.warning("GMAIL_USER or GMAIL_PASSWORD not set, skipping email alert")
        return False

    address = listing.get("address", "Unknown")
    score = listing.get("score", 0)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"New Deal Alert: {address} — Score {score}/100"
    msg["From"] = GMAIL_USER
    msg["To"] = ALERT_EMAIL

    html = build_alert_email(listing)
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, ALERT_EMAIL, msg.as_string())
        logger.info(f"Alert sent for {address} (score {score})")
        return True
    except Exception as e:
        logger.error(f"Failed to send alert for {address}: {e}")
        return False


def send_alerts_for_new_deals(listings):
    """Send alerts ONLY for new deals scoring 80+. Checks alert_sent flag."""
    high_scoring = [l for l in listings
                    if l.get("score", 0) >= SCORE_THRESHOLD
                    and not l.get("alert_sent")]

    if not high_scoring:
        logger.info(f"No new deals scoring {SCORE_THRESHOLD}+")
        return 0

    logger.info(f"Sending alerts for {len(high_scoring)} new high-scoring deals")
    sent = 0
    for listing in high_scoring:
        if send_alert(listing):
            listing["alert_sent"] = True
            sent += 1

    logger.info(f"Sent {sent}/{len(high_scoring)} alerts")
    return sent


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Test
    test = {
        "address": "163 N Center St", "city": "Redlands", "state": "CA", "zip_code": "92373",
        "score": 85, "score_reasoning": "Strong ARV margin (40%). Low $/sqft ($150). Stale listing (194 days)",
        "price": 188000, "arv": 310000, "max_offer": 200000,
        "estimated_profit": 31000, "repairs_mid": 50000, "repairs_worst": 70000,
        "days_on_zillow": 194, "privy_url": "#",
        "offer_analysis": {"max_offer": 200000, "estimated_profit": 31000},
    }
    send_alert(test)
