"""
DealFlow Email Alerts — Send deal alerts for high-scoring properties.
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
ALERT_RECIPIENT = "gescobarrei@gmail.com"
SCORE_THRESHOLD_MIN = 80
SCORE_THRESHOLD_MAX = 100


def build_alert_email(listing):
    """Build HTML email for a high-scoring deal."""
    analysis = listing.get("offer_analysis", {})
    address = f"{listing.get('address', 'N/A')}, {listing.get('city', '')}, {listing.get('state', 'CA')} {listing.get('zip_code', '')}"

    score = listing.get("score", 0)
    arv = analysis.get("arv", "N/A")
    max_offer = analysis.get("max_offer", "N/A")
    repairs_mid = analysis.get("repairs_mid", "N/A")
    repairs_worst = analysis.get("repairs_worst", "N/A")
    profit = analysis.get("estimated_profit", "N/A")
    roi = analysis.get("roi_pct", "N/A")
    privy_url = listing.get("privy_url", "#")
    listing_url = listing.get("listing_url", "#")

    # Format currency values
    def fmt(val):
        if isinstance(val, (int, float)):
            return f"${val:,.0f}"
        return str(val)

    # Score color
    if score >= 80:
        score_color = "#22c55e"
    elif score >= 60:
        score_color = "#eab308"
    else:
        score_color = "#ef4444"

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: #1e293b; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
            <h1 style="margin: 0;">🏠 DealFlow Alert</h1>
            <p style="margin: 5px 0 0; opacity: 0.8;">High-scoring deal found!</p>
        </div>

        <div style="padding: 20px; border: 1px solid #e2e8f0; border-top: none;">
            <h2 style="color: #1e293b; margin-top: 0;">{address}</h2>

            <div style="display: inline-block; background: {score_color}; color: white; padding: 8px 16px; border-radius: 20px; font-size: 24px; font-weight: bold;">
                Score: {score}/100
            </div>

            <table style="width: 100%; margin-top: 20px; border-collapse: collapse;">
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 8px; font-weight: bold;">ARV</td>
                    <td style="padding: 8px; text-align: right;">{fmt(arv)}</td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 8px; font-weight: bold;">Max Offer</td>
                    <td style="padding: 8px; text-align: right;">{fmt(max_offer)}</td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 8px; font-weight: bold;">Repairs (Mid)</td>
                    <td style="padding: 8px; text-align: right;">{fmt(repairs_mid)}</td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 8px; font-weight: bold;">Repairs (Worst)</td>
                    <td style="padding: 8px; text-align: right;">{fmt(repairs_worst)}</td>
                </tr>
                <tr style="border-bottom: 1px solid #e2e8f0;">
                    <td style="padding: 8px; font-weight: bold;">Est. Profit</td>
                    <td style="padding: 8px; text-align: right; color: #22c55e; font-weight: bold;">{fmt(profit)}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; font-weight: bold;">ROI</td>
                    <td style="padding: 8px; text-align: right;">{roi}%</td>
                </tr>
            </table>

            <div style="margin-top: 20px;">
                <a href="{listing_url}" style="display: inline-block; background: #3b82f6; color: white; padding: 12px 24px; border-radius: 6px; text-decoration: none; margin-right: 10px;">View on Zillow</a>
                <a href="{privy_url}" style="display: inline-block; background: #8b5cf6; color: white; padding: 12px 24px; border-radius: 6px; text-decoration: none;">Open in Privy</a>
            </div>

            <p style="margin-top: 20px; color: #64748b; font-size: 12px;">
                {listing.get('score_reasoning', '')}
            </p>
        </div>

        <div style="background: #f8fafc; padding: 15px; border: 1px solid #e2e8f0; border-top: none; border-radius: 0 0 8px 8px; text-align: center; color: #94a3b8; font-size: 12px;">
            DealFlow AI — Inland Empire Deal Finder
        </div>
    </body>
    </html>
    """
    return html


def send_alert(listing):
    """Send email alert for a single deal."""
    if not GMAIL_USER or not GMAIL_PASSWORD:
        logger.warning("GMAIL_USER or GMAIL_PASSWORD not set, skipping email")
        return False

    address = listing.get("address", "Unknown Property")
    score = listing.get("score", 0)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🏠 DealFlow Alert: Score {score}/100 — {address}"
    msg["From"] = GMAIL_USER
    msg["To"] = ALERT_RECIPIENT

    html = build_alert_email(listing)
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, ALERT_RECIPIENT, msg.as_string())
        logger.info(f"Alert sent for {address} (score {score})")
        return True
    except Exception as e:
        logger.error(f"Failed to send alert for {address}: {e}")
        return False


def send_alerts(listings):
    """Send alerts for all listings scoring 80-100."""
    high_scoring = [l for l in listings
                    if SCORE_THRESHOLD_MIN <= l.get("score", 0) <= SCORE_THRESHOLD_MAX]

    if not high_scoring:
        logger.info("No deals scoring 80-100, no alerts to send")
        return 0

    logger.info(f"Sending alerts for {len(high_scoring)} high-scoring deals")
    sent = 0
    for listing in high_scoring:
        if send_alert(listing):
            sent += 1

    logger.info(f"Sent {sent}/{len(high_scoring)} alerts")
    return sent


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test = {
        "address": "123 Main St", "city": "Fontana", "state": "CA", "zip_code": "92335",
        "score": 85, "score_reasoning": "Strong fixer opportunity in growing area.",
        "listing_url": "https://zillow.com/test",
        "privy_url": "https://app.privy.pro/dashboard?search_text=123+Main+St%2C+Fontana%2C+CA&street_number=123&street=123+Main+St&city=Fontana&zip=92335&state=CA&include_detached=true&include_active=true&date_range=6_month&spread_type=arv",
        "offer_analysis": {
            "arv": 650000, "max_offer": 420000, "repairs_mid": 75000,
            "repairs_worst": 120000, "estimated_profit": 65000, "roi_pct": 10.0,
        },
    }
    send_alert(test)
