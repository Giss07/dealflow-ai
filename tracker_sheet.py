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


COMP_COLS = (14, 15, 16)          # O Anchor Comp, P Comp #2, Q Comp #3 (the owner's headers)


def comp_cell_text(address):
    """Street only, like the owner's manual comp cells ("1438 S Church Avenue")."""
    return str(address or "").replace("\n", ", ").split(",")[0].strip()


def _cell(sheet_id, row, col, value=None, link=None):
    """One updateCells request. value None + no link clears the cell's value."""
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
                            "range": {"sheetId": sheet_id, "startRowIndex": row - 1, "endRowIndex": row,
                                      "startColumnIndex": col, "endColumnIndex": col + 1}}}


def _comp_cells(sheet_id, row, comps):
    """O/P/Q: [(address, zillow_url), ...] → up to 3 linked street-only cells."""
    return [_cell(sheet_id, row, col, comp_cell_text(addr), link=url or None)
            for col, (addr, url) in zip(COMP_COLS, comps[:3])]


def add_property(address, zillow_url, arv, offer, rehab, comps=(), today=None):
    """Append one row for the agent. Returns {'status': 'added', 'row': n} or,
    when the property is already in the tracker or an archive tab,
    {'status': 'exists', ...find_property()...}.

    comps: [(address, zillow_url)] in O, P, Q order — Anchor Comp first.
    Notes (N) and Sent Date / Status (J, K) are never written: J carries the
    agent's per-row protections, and a new row's J and K start empty."""
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

        today = today or date.today()
        requests += [
            _cell(sheet_id, target, 0, _serial(today)), _cell(sheet_id, target, 1, LEAD_SOURCE),
            _cell(sheet_id, target, 2, address, link=zillow_url or None),
            _cell(sheet_id, target, 3, round(arv)), _cell(sheet_id, target, 4, round(offer)),
            _cell(sheet_id, target, 5, round(rehab)),
            _cell(sheet_id, target, 6, CLOSING), _cell(sheet_id, target, 7, INSPECTION), _cell(sheet_id, target, 8, EMD),
        ] + _comp_cells(sheet_id, target, list(comps))
        _check(s.post(f"{base}:batchUpdate", json={"requests": requests}))
        with _index_lock:
            _index["rows"] = None                    # next read sees the new row
        logger.info(f"[tracker] added row {target}: {address}")
        return {"status": "added", "row": target, "tab": title}


def update_property_row(row, expect_address, arv, offer, rehab, comps, clear_notes=False):
    """Rewrite D/E/F and O/P/Q of an existing tracker row (and optionally clear
    N), after checking column C still holds `expect_address`. Nothing else in
    the row is touched."""
    with _write_lock:
        s, base = _session()
        tab = _tabs(s, base)[0]
        sheet_id, title = tab["sheetId"], tab["title"]
        current = (_check(s.get(f"{base}/values/'{title}'!C{row}")).get("values") or [[""]])[0][0]
        if not same_property(current, expect_address):
            raise RuntimeError(f"row {row} holds {current!r}, not {expect_address!r} — nothing written")
        requests = [_cell(sheet_id, row, 3, round(arv)), _cell(sheet_id, row, 4, round(offer)),
                    _cell(sheet_id, row, 5, round(rehab))] + _comp_cells(sheet_id, row, list(comps))
        if clear_notes:
            requests.append(_cell(sheet_id, row, 13))
        _check(s.post(f"{base}:batchUpdate", json={"requests": requests}))
        logger.info(f"[tracker] updated row {row}: {expect_address}")
        return {"status": "updated", "row": row, "tab": title}
