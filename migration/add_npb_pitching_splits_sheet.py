"""Build the 投手主客 tab: each team's starter / bullpen / total ERA, home and away.

Reads 分析表紀錄 — which carries both the starting pitcher's line and the
team's whole-game line for each side of every game — and writes one tab
summarising the season by team, segment and venue, ranked inside each league.

The tab is rewritten whole on every run rather than appended to, so it can be
re-run after any day's games without leaving a stale row behind.

    uv run python migration/add_npb_pitching_splits_sheet.py --dry-run
    uv run python migration/add_npb_pitching_splits_sheet.py
"""

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from baseball.npb_pitching_splits import HEADERS, build_sheet  # noqa: E402
from baseball.sheets import GoogleSheetsClient  # noqa: E402

# 分析表紀錄 lives in the record workbook; the tab is written into the analysis
# workbook, beside the other hand-built tables that read the season this way.
SOURCE_SPREADSHEET_KEY = "1XBATQ-ZQVE7saISTw_EYEXg3qFFAn5aeLDPdGI1_8Rg"
SOURCE_SHEET = "分析表紀錄"
TARGET_SPREADSHEET_KEY = "1X2oaXk6DJLkx1MPVjc0lgLNtqa88X5qdNdKuKyikrbg"
TARGET_SHEET = "投手主客"

# 分析表紀錄 carries two header rows above the games.
FIRST_GAME_ROW = 2


def _season(rows: list[list]) -> str:
    dates = sorted({str(row[1]).strip() for row in rows if len(row) > 1 and row[1]})
    return dates[0].split("/")[0] if dates else ""


def _worksheet(spreadsheet, title: str, *, rows: int, cols: int):
    """The tab, created on first run and reused after."""
    try:
        return spreadsheet.worksheet(title)
    except Exception:  # gspread WorksheetNotFound
        print(f"[splits] creating {title}")
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def _emphasise(sheet, values: list[list]) -> None:
    """Bold the title and every section's own title and header row.

    Three tables stacked in one tab read as one long list without it, and the
    header repeats often enough that a reader scrolling past loses which
    segment they are in.
    """
    bold = {"textFormat": {"bold": True}}
    last_column = chr(ord("A") + len(HEADERS) - 1)
    ranges = ["A1"]
    for index, row in enumerate(values, start=1):
        first = str(row[0]) if row else ""
        if first.startswith("【") or first == HEADERS[0]:
            ranges.append(f"A{index}:{last_column}{index}")
    sheet.batch_format([{"range": name, "format": bold} for name in ranges])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarise each team's pitching by segment and venue.")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the table instead of writing it")
    args = parser.parse_args()

    client = GoogleSheetsClient()
    source = client.worksheet(SOURCE_SPREADSHEET_KEY, SOURCE_SHEET)
    games = [row for row in source.get_all_values()[FIRST_GAME_ROW:] if any(row)]
    print(f"[splits] read {len(games)} row(s) from {SOURCE_SHEET}")

    values = build_sheet(
        games,
        updated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        season=_season(games),
    )

    if args.dry_run:
        for row in values:
            print("\t".join(str(cell) for cell in row))
        return 0

    spreadsheet = client.spreadsheet(TARGET_SPREADSHEET_KEY)
    sheet = _worksheet(spreadsheet, TARGET_SHEET,
                       rows=len(values) + 10, cols=len(HEADERS) + 2)
    sheet.clear()
    sheet.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
    _emphasise(sheet, values)
    print(f"[splits] wrote {len(values)} row(s) to {TARGET_SHEET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
