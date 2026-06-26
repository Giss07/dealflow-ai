"""Shared email-sending helper using Resend's HTTP API.

Replaces Gmail SMTP, which fails with OSError ENETUNREACH from Railway
containers — outbound SMTP egress is either blocked or hits an IPv6
routing pothole. Resend uses HTTPS port 443 which Railway always permits.

Sender defaults to Resend's sandbox onboarding@resend.dev — set
ALERT_FROM_EMAIL env var to override once a verified domain is set up.

Failure mode: writes [RESEND_ALERT_FAILED] sentinel to stderr. The
worker subprocess wrapper (worker.py _surface_stderr_sentinels) logs
these at ERROR level even when the subprocess exits 0, so silent
delivery failures cannot recur.
"""
import os
import sys
import time
import logging

logger = logging.getLogger(__name__)

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "onboarding@resend.dev")


def send_via_resend(to_addrs, subject, html_body, plain_body=None):
    """Send an email via Resend's HTTP API.

    Retries once (2 attempts total) with 5-second backoff between.
    Returns (success: bool, error_message: Optional[str]).

    Args:
        to_addrs: list of recipient email addresses (str also accepted)
        subject: email subject line
        html_body: HTML content
        plain_body: optional plain-text alternative
    """
    if not RESEND_API_KEY:
        msg = "RESEND_API_KEY not set in env"
        sys.stderr.write(f"[RESEND_ALERT_FAILED] {msg}\n")
        sys.stderr.flush()
        return False, msg

    if isinstance(to_addrs, str):
        to_addrs = [to_addrs]
    # filter out empty strings (defensive — comma-split env vars sometimes carry blanks)
    to_addrs = [a.strip() for a in to_addrs if a and a.strip()]
    if not to_addrs:
        msg = "no recipient addresses"
        sys.stderr.write(f"[RESEND_ALERT_FAILED] {msg}\n")
        sys.stderr.flush()
        return False, msg

    payload = {
        "from": ALERT_FROM_EMAIL,
        "to": to_addrs,
        "subject": subject,
        "html": html_body,
    }
    if plain_body:
        payload["text"] = plain_body

    import requests  # already in requirements.txt
    last_error = None
    for attempt in (1, 2):
        try:
            r = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=15,
            )
            if 200 <= r.status_code < 300:
                resend_id = (r.json() or {}).get("id", "?")
                print(f"  Resend sent (attempt {attempt}, id={resend_id}): {subject}")
                return True, None
            last_error = f"HTTP {r.status_code}: {r.text[:200]}"
            sys.stderr.write(f"[RESEND_ALERT_FAILED] attempt {attempt}/2 for {subject!r}: {last_error}\n")
            sys.stderr.flush()
        except Exception as e:
            last_error = f"{type(e).__name__}: {str(e)[:200]}"
            sys.stderr.write(f"[RESEND_ALERT_FAILED] attempt {attempt}/2 for {subject!r}: {last_error}\n")
            sys.stderr.flush()
        if attempt == 1:
            time.sleep(5)
    return False, last_error
