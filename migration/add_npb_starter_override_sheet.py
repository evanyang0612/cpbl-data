"""Create the 先發指定 tab that tells the scrapers who really started a game.

One row per game a club opened with somebody other than its starter. The rule
the row triggers lives in `baseball/npb_starter_overrides.py`: everyone ahead of
the named pitcher is folded into his line, so an opener's inning lands on the
pitcher the club actually started. A game with no row behaves exactly as it
always has, which is why this tab only ever holds exceptions.

    uv run python migration/add_npb_starter_override_sheet.py --dry-run
    uv run python migration/add_npb_starter_override_sheet.py

Safe to re-run: an existing tab keeps its rows, and seeding skips a game that
is already designated.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from baseball import npb_starter_overrides as overrides  # noqa: E402
from baseball.sheets import GoogleSheetsClient  # noqa: E402

# The workbook 賽錄 and 先發明細 live in, so the designation sits beside the
# record it corrects rather than in a second place to remember.
SPREADSHEET_KEY = "1X2oaXk6DJLkx1MPVjc0lgLNtqa88X5qdNdKuKyikrbg"

COLUMN_WIDTHS = (110, 110, 140, 320)

# The game that prompted the tab: 西武 opened with 森脇 亮介 for an inning and
# handed the next seven to 平良 海馬.
SEED = [["2026-09-15", "西武", "平良 海馬", "森脇 亮介 開局 1 局"]]


def worksheet(spreadsheet, *, create: bool):
    try:
        return spreadsheet.worksheet(overrides.SHEET_NAME)
    except Exception:  # gspread WorksheetNotFound
        if not create:
            return None
        sheet = spreadsheet.add_worksheet(
            title=overrides.SHEET_NAME, rows=200, cols=len(overrides.HEADERS))
        sheet.update([overrides.HEADERS], "A1", value_input_option="USER_ENTERED")
        sheet.freeze(rows=1)
        sheet.format("A1:D1", {"textFormat": {"bold": True}})
        # 日期 is written by hand and must survive being read back, so it stays
        # text rather than becoming whatever locale Sheets would guess at.
        sheet.format("A2:A", {"numberFormat": {"type": "TEXT"}})
        for index, width in enumerate(COLUMN_WIDTHS):
            spreadsheet.batch_update({"requests": [{"updateDimensionProperties": {
                "range": {"sheetId": sheet.id, "dimension": "COLUMNS",
                          "startIndex": index, "endIndex": index + 1},
                "properties": {"pixelSize": width}, "fields": "pixelSize"}}]})
        print(f"[先發指定] created in {spreadsheet.title}")
        return sheet


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="say what would be written, and write nothing")
    parser.add_argument("--no-seed", action="store_true",
                        help="create the tab empty")
    args = parser.parse_args()

    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), ".env"), override=False)

    spreadsheet = GoogleSheetsClient().spreadsheet(SPREADSHEET_KEY)
    if args.dry_run:
        sheet = worksheet(spreadsheet, create=False)
        existing = overrides.parse(sheet.get_all_values()) if sheet else {}
        print(f"[先發指定] {'exists' if sheet else 'would be created'} in "
              f"{spreadsheet.title}, {len(existing)} designation(s)")
        for row in ([] if args.no_seed else SEED):
            key = (overrides.normalise_date(row[0]), overrides.normalise_team(row[1]))
            print(f"  {'skip ' if key in existing else 'seed '}{row}")
        return

    sheet = worksheet(spreadsheet, create=True)
    if args.no_seed:
        return
    existing = overrides.parse(sheet.get_all_values())
    fresh = [row for row in SEED
             if (overrides.normalise_date(row[0]),
                 overrides.normalise_team(row[1])) not in existing]
    if not fresh:
        print("[先發指定] every seed row is already designated")
        return
    sheet.append_rows(fresh, value_input_option="USER_ENTERED", table_range="A:D")
    print(f"[先發指定] seeded {len(fresh)} row(s)")


if __name__ == "__main__":
    main()
