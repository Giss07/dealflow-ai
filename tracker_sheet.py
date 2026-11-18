"""
The offer tracker sheet, for the Distressed Deals page: find a property and
append a new row for the agent.

The tracker is the spreadsheet's FIRST tab — the one dealflow_updater's
counter-offer scanner reads as `sheet1` — plus Archive_STP and Archive_Closed
for the "already there" check. Rows are only ever appended after the last
address, never inserted, because the scanner updates rows by number.
"""

import logging
import re
import threading
import time
from datetime import date

logger = logging.getLogger(__name__)

ARCHIVE_TABS = ("Archive_STP", "Archive_Closed")
INDEX_TTL = 60                       # seconds the address index is reused
_index_lock = threading.Lock()
_write_lock = threading.Lock()
_index = {"at": 0.0, "rows": None}  # rows: [(tab, row_number, address)]

# Fixed values, matching the owner's existing rows exactly ("10 Days " has a trailing space).
LEAD_SOURCE = "Other"
CLOSING = "10 Days "
INSPECTION = "3 Days "
EMD = 0.01


def _session():
    import dealflow_updater as du
    from google.auth.transport.requests import AuthorizedSession
    creds = du._load_credentials(["https://www.googleapis.com/auth/spreadsheets"])
    base = f"https://sheets.googleapis.com/v4/spreadsheets/{du.SPREADSHEET_ID}"
    return AuthorizedSession(creds), base


def _check(resp):
    if resp.status_code >= 400:
        raise RuntimeError(f"Sheets API {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def _tabs(s, base):
    meta = _check(s.get(base, params={"fields": "sheets.properties(sheetId,title,index,gridProperties)"}))
    return sorted((x["properties"] for x in meta["sheets"]), key=lambda p: p["index"])


# ── Address identity ──────────────────────────────────────────────────

def address_key(address):
    """(street, zip, city) for comparing addresses across formats."""
    from dealflow_updater import normalize_address
    a = str(address or "").replace("\n", ", ").strip().lower()
    parts = [p.strip() for p in a.split(",") if p.strip()]
    street = normalize_address(parts[0]) if parts else ""
    street = re.sub(r"\s*(#|apt|unit|ste|suite)\s*\S+$", "", street).strip()
    zips = re.findall(r"\b(\d{5})(?:-\d{4})?\b", a)
    city = parts[1] if len(parts) > 2 and not re.fullmatch(r"[a-z]{2}( \d{5})?", parts[1]) else ""
    return street, (zips[-1] if zips else ""), city


def same_property(a, b):
    """Same street (whole words, either one may carry extra words such as the
    city run into it) and no ZIP / city disagreement."""
    sa, za, ca = address_key(a)
    sb, zb, cb = address_key(b)
    if len(sa) <= 5 or len(sb) <= 5:
        return False
    def within(x, y):
        return bool(re.search(r"(?<![0-9a-z])" + re.escape(x) + r"(?![0-9a-z])", y))
    if not (sa == sb or within(sa, sb) or within(sb, sa)):
        return False
    if za and zb:
        return za == zb
    if ca and cb:
        return ca == cb or ca in sb or cb in sa
    return True


# ── Reading ───────────────────────────────────────────────────────────

def _load_rows():
    s, base = _session()
    tabs = _tabs(s, base)
    names = [tabs[0]["title"]] + [t["title"] for t in tabs if t["title"] in ARCHIVE_TABS]
    got = _check(s.get(f"{base}/values:batchGet",
                       params={"ranges": [f"'{n}'!C2:C" for n in names], "majorDimension": "COLUMNS"}))
    rows = []
    for name, vr in zip(names, got.get("valueRanges", [])):
        col = (vr.get("values") or [[]])[0]
        rows += [(name, i + 2, v) for i, v in enumerate(col) if str(v).strip()]
    return tabs[0]["title"], rows


def tracker_index(force=False):
    """[(tab, row, address)] for the tracker and archive tabs, cached INDEX_TTL seconds."""
    with _index_lock:
        if force or _index["rows"] is None or time.time() - _index["at"] > INDEX_TTL:
            _index["tracker_tab"], _index["rows"] = _load_rows()
            _index["at"] = time.time()
        return _index["tracker_tab"], _index["rows"]


def find_property(address, force=False):
    """{'tab', 'row', 'where': 'tracker'|'archive', 'address'} or None."""
    tracker_tab, rows = tracker_index(force)
    hit = None
    for tab, row, existing in rows:
        if same_property(address, existing):
            where = "tracker" if tab == tracker_tab else "archive"
            if hit is None or (where == "tracker" and hit["where"] == "archive"):
                hit = {"tab": tab, "row": row, "where": where, "address": existing}
    return hit


# ── Writing ───────────────────────────────────────────────────────────

def _serial(d):
    return (d - date(1899, 12, 30)).days


def add_property(address, zillow_url, arv, offer, rehab, notes, today=None):
    """Append one row for the agent. Returns {'status': 'added', 'row': n} or,
    when the property is already in the tracker or an archive tab,
    {'status': 'exists', ...find_property()...}."""
    with _write_lock:
        existing = find_property(address, force=True)        # never trust a stale cache for writes
        if existing:
            return {"status": "exists", **existing}
        s, base = _session()
        tab = _tabs(s, base)[0]
        sheet_id, title = tab["sheetId"], tab["title"]
        cols = tab["gridProperties"]["columnCount"]
        col_c = _check(s.get(f"{base}/values/'{title}'!C1:C",
                             params={"majorDimension": "COLUMNS"})).get("values", [[]])[0]
        last = max(i for i, v in enumerate(col_c) if str(v).strip()) + 1     # 1-based row of the last address
        target = last + 1
        requests = []
        if target > tab["gridProperties"]["rowCount"]:
            requests.append({"appendDimension": {"sheetId": sheet_id, "dimension": "ROWS", "length": 50}})
        src = {"sheetId": sheet_id, "startRowIndex": last - 1, "endRowIndex": last, "startColumnIndex": 0, "endColumnIndex": cols}
        dst = {"sheetId": sheet_id, "startRowIndex": target - 1, "endRowIndex": target, "startColumnIndex": 0, "endColumnIndex": cols}
        for paste in ("PASTE_FORMAT", "PASTE_DATA_VALIDATION"):
            requests.append({"copyPaste": {"source": src, "destination": dst, "pasteType": paste}})

        def cell(col, value=None, link=None):
            v = {}
            if isinstance(value, (int, float)):
                v["userEnteredValue"] = {"numberValue": value}
            elif value is not None:
                v["userEnteredValue"] = {"stringValue": value}
            fields = "userEnteredValue"
            if link:
                v["userEnteredFormat"] = {"textFormat": {"link": {"uri": link}}}
                fields += ",userEnteredFormat.textFormat.link"
            return {"updateCells": {"rows": [{"values": [v]}], "fields": fields,
                                    "range": {"sheetId": sheet_id, "startRowIndex": target - 1, "endRowIndex": target,
                                              "startColumnIndex": col, "endColumnIndex": col + 1}}}

        today = today or date.today()
        requests += [
            cell(0, _serial(today)), cell(1, LEAD_SOURCE), cell(2, address, link=zillow_url or None),
            cell(3, round(arv)), cell(4, round(offer)), cell(5, round(rehab)),
            cell(6, CLOSING), cell(7, INSPECTION), cell(8, EMD),
            cell(9), cell(10),                       # Sent Date, Status: blank for the agent
            cell(13, notes),
        ]
        # J and K were just given the previous row's formats; make sure they are empty.
        requests[-3]["updateCells"]["fields"] = requests[-2]["updateCells"]["fields"] = "userEnteredValue"
        _check(s.post(f"{base}:batchUpdate", json={"requests": requests}))
        with _index_lock:
            _index["rows"] = None                    # next read sees the new row
        logger.info(f"[tracker] added row {target}: {address}")
        return {"status": "added", "row": target, "tab": title}
