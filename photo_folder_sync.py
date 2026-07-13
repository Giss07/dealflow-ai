"""
Incrementally sync per-address photo folders for Properties_Offer_Tracker_Template.

For each row where column C (Address) is non-blank and column S (Photos) is blank:
  1. Look for an existing subfolder in the "Property Photos" parent with the same name.
  2. If none, create one, share it with the user as Editor, and add anyone-with-link Viewer.
  3. Write =HYPERLINK(folder_url, "View photos") into column S.

Idempotent — safe to re-run. Reuses existing folders by name to avoid duplicates.

Usage:
    python3 photo_folder_sync.py           # do the sync
    python3 photo_folder_sync.py --dry-run # show what would happen, no writes

Run manually after adding rows, or wire into scheduler.py for periodic sync.
"""
import os
import sys
import time
import warnings
warnings.filterwarnings("ignore")

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Reuse the same env-first cred loader as dealflow_updater so this works on
# Railway (GOOGLE_SERVICE_ACCOUNT_JSON env var) as well as locally (file fallback).
from dealflow_updater import _load_credentials

SHEET_ID    = "1GMp9LbZLgY_uaTjiDQ9cTcy4I1QxOqLsNZWORwkUMCY"
TAB_NAME    = "Properties_Offer_Tracker_Template"
PHOTOS_COL  = "S"        # header "Photos"
ADDRESS_COL = "C"        # header "Address"
PARENT_ID   = "1ga1DXdGpw9vyG6CjWn8c10eKkhKZYPV8"   # "Property Photos" folder
USER_EMAIL  = "gescobarrei@gmail.com"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _clients():
    creds = _load_credentials(SCOPES)
    return (
        build("sheets", "v4", credentials=creds, cache_discovery=False),
        build("drive", "v3", credentials=creds, cache_discovery=False),
    )


def _find_existing_folder(drive, name):
    # Escape single quotes for the query
    safe = name.replace("'", r"\'")
    q = (
        f"name = '{safe}' and "
        f"'{PARENT_ID}' in parents and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"trashed = false"
    )
    r = drive.files().list(q=q, fields="files(id,webViewLink)", pageSize=1).execute()
    files = r.get("files", [])
    return files[0] if files else None


def _create_and_share(drive, name):
    sub = drive.files().create(
        body={"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [PARENT_ID]},
        fields="id,webViewLink",
    ).execute()
    fid = sub["id"]
    drive.permissions().create(
        fileId=fid,
        body={"type": "user", "role": "writer", "emailAddress": USER_EMAIL},
        sendNotificationEmail=False,
        fields="id",
    ).execute()
    drive.permissions().create(
        fileId=fid,
        body={"type": "anyone", "role": "reader"},
        fields="id",
    ).execute()
    return sub


def sync(dry_run=False):
    sheets, drive = _clients()

    # Pull columns C and S in one range so rows align
    resp = sheets.spreadsheets().values().batchGet(
        spreadsheetId=SHEET_ID,
        ranges=[f"{TAB_NAME}!{ADDRESS_COL}2:{ADDRESS_COL}", f"{TAB_NAME}!{PHOTOS_COL}2:{PHOTOS_COL}"],
        valueRenderOption="FORMULA",   # keep formulas so we can tell 'has hyperlink' from blank
    ).execute()
    col_c = resp["valueRanges"][0].get("values", [])
    col_s = resp["valueRanges"][1].get("values", [])

    todo = []
    for i, addr_row in enumerate(col_c):
        row_num = i + 2  # data starts at row 2
        addr = (addr_row[0] if addr_row else "").strip()
        if not addr:
            continue
        s_val = (col_s[i][0] if i < len(col_s) and col_s[i] else "").strip()
        if s_val:
            continue  # already has something in the Photos column
        todo.append((row_num, addr))

    if not todo:
        print(f"Nothing to do — every row with an address in {ADDRESS_COL} already has a value in {PHOTOS_COL}.")
        return

    print(f"Rows needing folders: {len(todo)}")
    for row_num, addr in todo[:5]:
        print(f"  row {row_num}: {addr!r}")
    if len(todo) > 5:
        print(f"  ...and {len(todo) - 5} more")

    if dry_run:
        print("\n--dry-run: not creating or writing anything.")
        return

    formula_updates = []
    reused = created = failed = 0
    failures = []

    for idx, (row_num, addr) in enumerate(todo, start=1):
        try:
            existing = _find_existing_folder(drive, addr)
            if existing:
                url = existing["webViewLink"]
                reused += 1
                tag = "REUSED"
            else:
                sub = _create_and_share(drive, addr)
                url = sub["webViewLink"]
                created += 1
                tag = "CREATED"

            formula_updates.append({
                "range": f"{TAB_NAME}!{PHOTOS_COL}{row_num}",
                "values": [[f'=HYPERLINK("{url}", "View photos")']],
            })
            print(f"[{idx}/{len(todo)}] row {row_num} {tag}: {addr!r}")
        except HttpError as e:
            failed += 1
            failures.append((row_num, addr, f"{e.resp.status} {str(e)[:180]}"))
            print(f"[{idx}/{len(todo)}] row {row_num} FAIL: {addr!r} — {e.resp.status}")

        if idx % 30 == 0:
            time.sleep(1)

    if formula_updates:
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": formula_updates},
        ).execute()

    print(f"\n=== SYNC DONE ===")
    print(f"Created: {created}  Reused: {reused}  Failed: {failed}")
    if failures:
        for row_num, addr, err in failures:
            print(f"  row {row_num}: {addr!r} — {err}")


if __name__ == "__main__":
    sync(dry_run=("--dry-run" in sys.argv))
