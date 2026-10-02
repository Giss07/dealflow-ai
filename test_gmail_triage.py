"""
Regression test for the Gmail scanner's header-triage pass.

The 2026-10-01 stall: read_christian_emails fetched the full RFC822 of every
message in the lookback window and only then checked the dedup ledger, so each
30-minute run re-downloaded dozens of already-processed emails, hit the
worker's subprocess timeout, and was killed before reaching the newest
messages. Eight HUD counter notices went unprocessed for hours.

The first fix (headers instead of bodies) was not enough, and the second
measurement is the important one: every IMAP command to Gmail from the Railway
container costs ~10s, so a per-message header pass over 53 messages still took
533s and still blew the budget. The cost is ROUND TRIPS, not bytes — hence the
batched fetch, and hence the round-trip assertion below.

This test stubs IMAP and the sheet (no network, no Google, no DB) and asserts
that an already-ledgered message is skipped on its headers alone — never
fetched in full.

    python test_gmail_triage.py
"""

import os
import sys
import hashlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("EMAIL_LOOKBACK_DAYS", "2")

FAILS = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{(' — ' + str(detail)) if detail else ''}")
    if not cond:
        FAILS.append(label)


def _msg_bytes(subject, sender, date, msg_id, body):
    return (
        f"Subject: {subject}\r\nFrom: {sender}\r\nDate: {date}\r\n"
        f"Message-ID: {msg_id}\r\nContent-Type: text/plain\r\n\r\n{body}\r\n"
    ).encode()


def _hdr_bytes(subject, sender, date, msg_id):
    hdr = f"Message-ID: {msg_id}\r\nSubject: {subject}\r\nDate: {date}\r\nFrom: {sender}\r\n\r\n"
    if not msg_id:
        hdr = f"Subject: {subject}\r\nDate: {date}\r\nFrom: {sender}\r\n\r\n"
    return hdr.encode()


class FakeIMAP:
    """Minimal IMAP stand-in that records which fetches were full-body."""

    def __init__(self, messages):
        self.messages = messages          # {id: dict(subject, sender, date, msg_id, body)}
        self.full_fetches = []
        self.header_fetches = []
        self.round_trips = 0
        self.logged_out = False

    def login(self, user, pw):
        return "OK", [b"logged in"]

    def select(self, mailbox, readonly=False):
        return "OK", [str(len(self.messages)).encode()]

    def search(self, charset, *criteria):
        return "OK", [b" ".join(k.encode() for k in self.messages)]

    def fetch(self, eid, spec):
        raw = eid.decode() if isinstance(eid, bytes) else str(eid)
        keys = [k for k in raw.split(",") if k]
        self.round_trips += 1
        if "PEEK" in spec:
            # Batched header fetch — one response per requested message, each
            # prefixed with its sequence number (what imaplib really returns).
            self.header_fetches.extend(keys)
            out = []
            for k in keys:
                m = self.messages[k]
                payload = _hdr_bytes(m["subject"], m["sender"], m["date"], m["msg_id"])
                out.append((f"{k} (BODY[HEADER.FIELDS (MESSAGE-ID SUBJECT DATE FROM)] "
                            f"{{{len(payload)}}}".encode(), payload))
                out.append(b")")
            return "OK", out
        assert len(keys) == 1, "full fetches stay per-message"
        m = self.messages[keys[0]]
        self.full_fetches.append(keys[0])
        payload = _msg_bytes(m["subject"], m["sender"], m["date"], m["msg_id"], m["body"])
        return "OK", [(b"1 (RFC822 {%d}" % len(payload), payload), b")"]

    def logout(self):
        self.logged_out = True


class FakeSheet:
    HEADERS = ["Address", "Purchase Contract Price", "Counter Price", "Counter Date",
               "Status (/Accepted/Rejected/Counter)", "Notes", "Alert Sent", "HUD Case #"]

    def __init__(self, records=None):
        self.updates = []
        self.records = records or []

    def row_values(self, n):
        return list(self.HEADERS)

    def update_cell(self, row, col, value):
        self.updates.append((row, col, value))

    def get_all_records(self):
        return list(self.records)

    def writes_to(self, column_name):
        col = self.HEADERS.index(column_name) + 1
        return [(r, v) for (r, c, v) in self.updates if c == col]


def run():
    import dealflow_updater as du

    KNOWN = "<known-already-processed@mail.gmail.com>"
    NEW = "<brand-new-counter-notice@mail.gmail.com>"
    NO_ID = ""

    messages = {
        "1": dict(subject="Fwd: P260 - HUD - Bid Counter Offer Notice - 111-111111",
                  sender="Natalie Serna <natalie@example.com>", date="Wed, 30 Sep 2026 11:00:00 -0700",
                  msg_id=KNOWN, body="Address: 123 Old St, Riverside, CA 92503 counter offer of $400,000"),
        "2": dict(subject="Fwd: P260 - HUD - Bid Counter Offer Notice - 222-222222",
                  sender="Natalie Serna <natalie@example.com>", date="Thu, 1 Oct 2026 10:00:00 -0700",
                  msg_id=NEW, body="Address: 456 New Ave, Riverside, CA 92503 counter offer of $500,000"),
        "3": dict(subject="Fwd: P260 - HUD - Bid Counter Offer Notice - 333-333333",
                  sender="Natalie Serna <natalie@example.com>", date="Thu, 1 Oct 2026 10:05:00 -0700",
                  msg_id=NO_ID, body="Address: 789 Third Rd, Riverside, CA 92503 counter offer of $600,000"),
    }
    fake = FakeIMAP(messages)

    # Ledger: message 1 is already processed. Message 3 has no Message-ID, so the
    # scanner must build the same synthetic id from headers in both passes.
    synthetic_3 = "synthetic:" + hashlib.md5(
        "|".join([messages["3"]["subject"], messages["3"]["date"], messages["3"]["sender"]]).encode("utf-8", "ignore")
    ).hexdigest()
    ledger = {KNOWN, synthetic_3}
    marked = []

    orig_imap = du.imaplib.IMAP4_SSL
    du.imaplib.IMAP4_SSL = lambda host, **kw: fake
    import database
    orig_load, orig_mark = database.load_processed_message_ids, database.mark_email_processed
    database.load_processed_message_ids = lambda since_days=7: set(ledger)
    database.mark_email_processed = lambda mid, subject=None: marked.append(mid)
    orig_creds = (du.CHRISTIAN_GMAIL, du.CHRISTIAN_APP_PASSWORD)
    du.CHRISTIAN_GMAIL, du.CHRISTIAN_APP_PASSWORD = "fake@example.com", "fake-pw"

    try:
        print("\n=== Header triage ===")
        du.read_christian_emails(FakeSheet(), [])

        check("every message triaged on headers first", sorted(fake.header_fetches) == ["1", "2", "3"],
              fake.header_fetches)
        # The 2026-10-01 killer was round trips, not bytes: ~10s per IMAP command
        # against Gmail from Railway. 3 messages must cost ONE header fetch, not three.
        check("headers come back in a single batched round trip",
              fake.round_trips == 2, f"{fake.round_trips} fetch commands (1 batched header + 1 full body)")
        check("ledgered message never fetched in full", "1" not in fake.full_fetches,
              f"full fetches: {fake.full_fetches}")
        check("synthetic-id message also skipped on headers", "3" not in fake.full_fetches,
              f"full fetches: {fake.full_fetches}")
        check("only the new message is downloaded in full", fake.full_fetches == ["2"],
              fake.full_fetches)
        check("connection closed cleanly", fake.logged_out)
        # 840677b deleted the _remember helper, so every call raised NameError
        # into the loop's except and NOTHING was ever ledgered — every run
        # reprocessed the same mail and re-alerted. Assert the write happens.
        check("the processed message is written to the ledger", marked == [NEW],
              f"ledger writes: {marked}")
    finally:
        du.imaplib.IMAP4_SSL = orig_imap
        database.load_processed_message_ids, database.mark_email_processed = orig_load, orig_mark
        du.CHRISTIAN_GMAIL, du.CHRISTIAN_APP_PASSWORD = orig_creds

    # Header peek failing must never drop an email — it falls through to a full fetch.
    print("\n=== Header peek failure falls through ===")
    class BrokenPeek(FakeIMAP):
        def fetch(self, eid, spec):
            if "PEEK" in spec:
                raise RuntimeError("simulated IMAP header hiccup")
            return super().fetch(eid, spec)

    broken = BrokenPeek({"1": messages["1"]})
    du.imaplib.IMAP4_SSL = lambda host, **kw: broken
    database.load_processed_message_ids = lambda since_days=7: set(ledger)
    database.mark_email_processed = lambda mid, subject=None: None
    du.CHRISTIAN_GMAIL, du.CHRISTIAN_APP_PASSWORD = "fake@example.com", "fake-pw"
    try:
        du.read_christian_emails(FakeSheet(), [])
        check("peek failure still fetches the message in full", broken.full_fetches == ["1"],
              broken.full_fetches)
    finally:
        du.imaplib.IMAP4_SSL = orig_imap
        database.load_processed_message_ids, database.mark_email_processed = orig_load, orig_mark
        du.CHRISTIAN_GMAIL, du.CHRISTIAN_APP_PASSWORD = orig_creds


GURNSEY = "116 Gurnsey Ave, Red Bluff, CA 96080"


def _counter_email(mid, amount):
    return dict(
        subject=f"Fwd: P260 - HUD - Bid Counter Offer Notice - 043-746923",
        sender="Natalie Serna <natalie@example.com>",
        date="Fri, 2 Oct 2026 10:22:09 -0700",
        msg_id=mid,
        body=(f"Address: {GURNSEY} The minimum acceptable net to HUD offer "
              f"amount for this property as {amount:,}.00"),
    )


def _record(counter_price, alert_sent):
    return {
        "Address": GURNSEY,
        "Purchase Contract Price": 190000,
        "Counter Price": counter_price,
        "Counter Date": "10/01/2026",
        "Status (/Accepted/Rejected/Counter)": "Counter",
        "Notes": "",
        "Alert Sent": alert_sent,
        "HUD Case #": "043-746923",
    }


def _run_counter_case(email_amount, row_counter, alert_sent, ledger=None, mid=None):
    """Process one counter email against one row.

    Returns (alerts, sheet, ledger_writes, imap). `ledger` seeds the dedup
    ledger so a second run over the same message can be simulated.
    """
    import dealflow_updater as du
    import database

    mid = mid or "<counter-notice-1@mail.gmail.com>"
    msgs = {"1": _counter_email(mid, email_amount)}
    fake = FakeIMAP(msgs)
    records = [_record(row_counter, alert_sent)]
    sheet = FakeSheet(records)
    seeded = set(ledger or ())
    written = []

    orig_imap = du.imaplib.IMAP4_SSL
    orig_load, orig_mark = database.load_processed_message_ids, database.mark_email_processed
    orig_creds = (du.CHRISTIAN_GMAIL, du.CHRISTIAN_APP_PASSWORD)
    du.imaplib.IMAP4_SSL = lambda host, **kw: fake
    database.load_processed_message_ids = lambda since_days=7: set(seeded)
    database.mark_email_processed = lambda m, subject=None: written.append(m)
    du.CHRISTIAN_GMAIL, du.CHRISTIAN_APP_PASSWORD = "fake@example.com", "fake-pw"
    try:
        alerts = du.read_christian_emails(sheet, records)
    finally:
        du.imaplib.IMAP4_SSL = orig_imap
        database.load_processed_message_ids, database.mark_email_processed = orig_load, orig_mark
        du.CHRISTIAN_GMAIL, du.CHRISTIAN_APP_PASSWORD = orig_creds
    return alerts, sheet, written, fake


def test_alert_once_per_email():
    print("\n=== One alert per counter EMAIL ===")

    # The rule: every counter email within $30k alerts, even when it repeats a
    # price already alerted on. $190k offer vs $217.5k counter = $27.5k gap.
    alerts, sheet, written, _imap = _run_counter_case(217500, 217500.0, "Yes")
    check("a re-forwarded identical counter DOES alert",
          len(alerts) == 1 and alerts[0]["type"] == "CLOSE",
          [(a["type"], a["difference"]) for a in alerts])
    check("that email is recorded in the ledger", written == ["<counter-notice-1@mail.gmail.com>"],
          written)
    check("no 'previous counter' note when the amount did not change",
          sheet.writes_to("Notes") == [], sheet.writes_to("Notes"))

    # The SAME email on a later run must not alert again — the ledger is what
    # stops the every-30-minutes repeats.
    alerts2, _sheet2, written2, imap2 = _run_counter_case(
        217500, 217500.0, "Yes", ledger={"<counter-notice-1@mail.gmail.com>"})
    check("the same email never alerts twice", alerts2 == [],
          [(a["type"], a["address"]) for a in alerts2])
    check("an already-ledgered email is not even downloaded", imap2.full_fetches == [],
          imap2.full_fetches)
    check("and it is not re-written to the ledger", written2 == [], written2)

    # A different email for the same property (tomorrow's re-forward) alerts again.
    alerts3, _s3, _w3, _i3 = _run_counter_case(
        217500, 217500.0, "Yes", ledger={"<counter-notice-1@mail.gmail.com>"},
        mid="<counter-notice-2@mail.gmail.com>")
    check("tomorrow's re-forward of the same price alerts again",
          len(alerts3) == 1 and alerts3[0]["type"] == "CLOSE",
          [(a["type"], a["counter_price"]) for a in alerts3])

    # Threshold unchanged: a counter beyond $30k still stays silent.
    alerts4, _s4, _w4, _i4 = _run_counter_case(260000, 217500.0, "")
    check("counter beyond the $30k rule still does not alert", alerts4 == [],
          [(a["type"], a["difference"]) for a in alerts4])

    # An un-alerted row still behaves the same way.
    alerts5, _s5, _w5, _i5 = _run_counter_case(217500, 217500.0, "")
    check("an un-alerted row alerts as before",
          len(alerts5) == 1 and alerts5[0]["type"] == "CLOSE",
          [(a["type"], a["difference"]) for a in alerts5])


def test_failed_send_recovers():
    print("\n=== A failed send stays retryable ===")
    import dealflow_updater as du

    alert = {"type": "CLOSE", "address": GURNSEY, "purchase_price": 190000,
             "counter_price": 217500, "difference": 27500,
             "row": 2, "alert_col": FakeSheet.HEADERS.index("Alert Sent") + 1}

    def _run(delivered):
        """run_gmail_only with the email scan and the sender stubbed out."""
        sheet = FakeSheet([_record(217500.0, "Yes")])
        orig_read = du.read_christian_emails
        orig_existing = du.check_existing_counter_alerts
        orig_send = du.send_alerts
        du.read_christian_emails = lambda s, r: [dict(alert)]
        du.check_existing_counter_alerts = lambda r, s, h: []
        du.send_alerts = lambda alerts, back_on_market=[]: set(delivered)
        try:
            du.run_gmail_only(sheet, sheet.get_all_records(), list(FakeSheet.HEADERS))
        finally:
            du.read_christian_emails = orig_read
            du.check_existing_counter_alerts = orig_existing
            du.send_alerts = orig_send
        return sheet

    # Send FAILS on a row already marked 'Yes' from an earlier counter round.
    # The stale 'Yes' used to stand, and with the email already in the dedup
    # ledger nothing ever retried it — the alert was lost.
    sheet = _run(delivered=[])
    check("failed send clears the stale 'Yes'",
          sheet.writes_to("Alert Sent") == [(2, "")], sheet.writes_to("Alert Sent"))

    # With the flag cleared, the sheet-scan recovery path re-queues the row.
    records = [_record(217500.0, "")]
    requeued = du.check_existing_counter_alerts(records, FakeSheet(records),
                                                list(FakeSheet.HEADERS))
    check("the cleared row is re-queued by the recovery path",
          len(requeued) == 1 and requeued[0]["type"] == "CLOSE",
          [(a["type"], a["difference"]) for a in requeued])

    # A successful send still stamps 'Yes' and nothing is cleared.
    sheet2 = _run(delivered=[GURNSEY])
    check("successful send stamps 'Yes'",
          sheet2.writes_to("Alert Sent") == [(2, "Yes")], sheet2.writes_to("Alert Sent"))

    # And a row still marked 'Yes' is not re-queued, so no repeats.
    records2 = [_record(217500.0, "Yes")]
    check("a delivered row is not re-queued",
          du.check_existing_counter_alerts(records2, FakeSheet(records2),
                                           list(FakeSheet.HEADERS)) == [])


if __name__ == "__main__":
    run()
    test_alert_once_per_email()
    test_failed_send_recovers()
    print(f"\n=== {'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)} ===")
    sys.exit(1 if FAILS else 0)
