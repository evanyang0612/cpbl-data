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

from baseball.npb_pitching_splits import (  # noqa: E402
    HEADERS,
    ROW_DATA,
    ROW_HEADER,
    ROW_INFO,
    ROW_NOTE,
    ROW_SECTION,
    ROW_TITLE,
    build_sheet,
    row_roles,
)
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


# The palette 球審排名v2 already uses in this workbook: a dark title band, a
# mid-navy header, two barely-tinted rows for the notes, and a green-to-red
# gradient down the column being ranked. Reusing it keeps the workbook reading
# as one thing rather than as a pile of tabs by different hands.
NAVY_DARK = {"red": 0.047, "green": 0.137, "blue": 0.220}
NAVY_MID = {"red": 0.098, "green": 0.239, "blue": 0.357}
BLUE_PALE = {"red": 0.910, "green": 0.937, "blue": 0.969}
BLUE_FAINT = {"red": 0.976, "green": 0.988, "blue": 1.0}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
TEXT_HEAD = {"red": 0.098, "green": 0.169, "blue": 0.247}
TEXT_BODY = {"red": 0.118, "green": 0.149, "blue": 0.176}
TEXT_NOTE = {"red": 0.149, "green": 0.200, "blue": 0.278}
BORDER_OUT = {"red": 0.800, "green": 0.839, "blue": 0.878}
BORDER_IN = {"red": 0.898, "green": 0.918, "blue": 0.937}
GRAD_GOOD = {"red": 0.800, "green": 0.929, "blue": 0.827}
GRAD_MID = {"red": 1.000, "green": 0.949, "blue": 0.698}
GRAD_BAD = {"red": 0.957, "green": 0.776, "blue": 0.757}

# One colour per league, so the eye lands on the block it wants before it
# reads a single number. The chip in the first column carries the name; the
# rest of the row takes the palest wash of the same hue, faint enough that the
# ERA gradient still reads over it.
LEAGUE_CHIP = {"央聯": {"red": 0.180, "green": 0.361, "blue": 0.541},
               "洋聯": {"red": 0.180, "green": 0.490, "blue": 0.420}}
LEAGUE_TINT = {"央聯": {"red": 0.953, "green": 0.969, "blue": 0.988},
               "洋聯": {"red": 0.949, "green": 0.976, "blue": 0.969}}

COLUMN_WIDTHS = [62, 104, 68, 68, 52, 78, 78, 52, 78, 78, 52, 74]

# 局數 / ERA / 名次 repeat three times across a row, then the gap.
INNINGS_COLUMNS = (2, 5, 8)
ERA_COLUMNS = (3, 6, 9)
RANK_COLUMNS = (4, 7, 10)
GAP_COLUMN = 11


def _border(color):
    return {"style": "SOLID", "width": 1, "color": color}


def _cell_format(**fields):
    return {"userEnteredFormat": fields}


def _band(sheet_id, row, *, background, color, bold=False, size=11,
          align="CENTER"):
    return {"repeatCell": {
        "range": {"sheetId": sheet_id, "startRowIndex": row, "endRowIndex": row + 1,
                  "startColumnIndex": 0, "endColumnIndex": len(HEADERS)},
        "cell": _cell_format(
            backgroundColor=background,
            horizontalAlignment=align,
            textFormat={"foregroundColor": color, "bold": bold, "fontSize": size},
        ),
        "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)",
    }}


def _gradient(sheet_id, first_row, last_row, column, *, diverging=False):
    """Green where the number is good, red where it is not.

    ERA runs green at the lowest and red at the highest. The home-minus-away
    gap is read around zero instead: green means the staff is better at home,
    red means it is better on the road, and a team near even stays pale.
    """
    midpoint = ({"color": WHITE, "type": "NUMBER", "value": "0"} if diverging
                else {"color": GRAD_MID, "type": "PERCENTILE", "value": "50"})
    return {"addConditionalFormatRule": {"rule": {
        "ranges": [{"sheetId": sheet_id, "startRowIndex": first_row,
                    "endRowIndex": last_row + 1,
                    "startColumnIndex": column, "endColumnIndex": column + 1}],
        "gradientRule": {"minpoint": {"color": GRAD_GOOD, "type": "MIN"},
                         "midpoint": midpoint,
                         "maxpoint": {"color": GRAD_BAD, "type": "MAX"}},
    }, "index": 0}}


def _format(spreadsheet, sheet, values: list[list]) -> None:
    """Paint the tab in the workbook's own house style.

    Rewritten from scratch every run: `clear()` empties the cells but leaves
    formats, merges and conditional rules behind, and a tab that grows or
    shrinks by a row would otherwise keep painting last week's layout.
    """
    sheet_id = sheet.id
    width = len(HEADERS)
    roles = row_roles(values)
    requests: list[dict] = []

    existing = spreadsheet.fetch_sheet_metadata(
        {"fields": "sheets(properties(sheetId),conditionalFormats)"})
    for meta in existing["sheets"]:
        if meta["properties"]["sheetId"] != sheet_id:
            continue
        for index in reversed(range(len(meta.get("conditionalFormats", [])))):
            requests.append({"deleteConditionalFormatRule":
                             {"sheetId": sheet_id, "index": index}})

    whole = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": len(values),
             "startColumnIndex": 0, "endColumnIndex": width}
    requests += [
        {"unmergeCells": {"range": whole}},
        {"repeatCell": {
            "range": whole,
            "cell": _cell_format(
                backgroundColor=WHITE,
                horizontalAlignment="CENTER",
                verticalAlignment="MIDDLE",
                textFormat={"foregroundColor": TEXT_BODY, "bold": False,
                            "fontSize": 11},
                borders={side: _border(BORDER_IN)
                         for side in ("top", "bottom", "left", "right")},
            ),
            "fields": ("userEnteredFormat(backgroundColor,horizontalAlignment,"
                       "verticalAlignment,textFormat,borders)"),
        }},
        {"updateSheetProperties": {
            "properties": {"sheetId": sheet_id,
                           "gridProperties": {"frozenRowCount": 3,
                                              "columnCount": width}},
            "fields": "gridProperties(frozenRowCount,columnCount)"}},
    ]
    for index, pixels in enumerate(COLUMN_WIDTHS):
        requests.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": index, "endIndex": index + 1},
            "properties": {"pixelSize": pixels}, "fields": "pixelSize"}})

    banners = {ROW_TITLE: (NAVY_DARK, WHITE, True, 15),
               ROW_INFO: (BLUE_PALE, TEXT_HEAD, True, 10),
               ROW_NOTE: (BLUE_FAINT, TEXT_NOTE, False, 10),
               ROW_SECTION: (NAVY_MID, WHITE, True, 12),
               ROW_HEADER: (BLUE_PALE, TEXT_HEAD, True, 11)}
    for row, role in enumerate(roles):
        if role not in banners:
            continue
        background, color, bold, size = banners[role]
        align = "LEFT" if role in (ROW_TITLE, ROW_INFO, ROW_NOTE, ROW_SECTION) else "CENTER"
        requests.append(_band(sheet_id, row, background=background, color=color,
                              bold=bold, size=size, align=align))
        if role != ROW_HEADER:
            # One banner across the tab; a merged strip reads as a heading and
            # an unmerged one reads as a row with eleven empty cells.
            requests.append({"mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": row,
                          "endRowIndex": row + 1, "startColumnIndex": 0,
                          "endColumnIndex": width},
                "mergeType": "MERGE_ROWS"}})

    data_rows = [row for row, role in enumerate(roles) if role == ROW_DATA]

    # Painted in runs rather than row by row: the six blocks are contiguous, so
    # one request each keeps the batch small enough to stay one round trip.
    runs: list[tuple[str, int, int]] = []
    for row in data_rows:
        league = str(values[row][0])
        if runs and runs[-1][0] == league and runs[-1][2] == row - 1:
            runs[-1] = (league, runs[-1][1], row)
        else:
            runs.append((league, row, row))
    for league, first, last in runs:
        if league not in LEAGUE_TINT:
            continue
        requests.append({"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": first,
                      "endRowIndex": last + 1, "startColumnIndex": 0,
                      "endColumnIndex": width},
            "cell": _cell_format(backgroundColor=LEAGUE_TINT[league]),
            "fields": "userEnteredFormat.backgroundColor"}})
        requests.append({"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": first,
                      "endRowIndex": last + 1, "startColumnIndex": 0,
                      "endColumnIndex": 1},
            "cell": _cell_format(
                backgroundColor=LEAGUE_CHIP[league],
                textFormat={"foregroundColor": WHITE, "bold": True,
                            "fontSize": 11}),
            "fields": "userEnteredFormat(backgroundColor,textFormat)"}})
        # The team is the row's name, so it carries the weight the rest does not.
        requests.append({"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": first,
                      "endRowIndex": last + 1, "startColumnIndex": 1,
                      "endColumnIndex": 2},
            "cell": _cell_format(
                textFormat={"foregroundColor": TEXT_HEAD, "bold": True,
                            "fontSize": 11}),
            "fields": "userEnteredFormat.textFormat"}})

    for columns, pattern in ((INNINGS_COLUMNS, "0.0"), (ERA_COLUMNS, "0.00"),
                             (RANK_COLUMNS, "0"),
                             ((GAP_COLUMN,), "+0.00;-0.00;0.00")):
        for column in columns:
            requests.append({"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": min(data_rows),
                          "endRowIndex": max(data_rows) + 1,
                          "startColumnIndex": column, "endColumnIndex": column + 1},
                "cell": _cell_format(numberFormat={"type": "NUMBER",
                                                   "pattern": pattern}),
                "fields": "userEnteredFormat.numberFormat"}})

    # The gradient is read down one section at a time: a bullpen ERA means
    # nothing against a rotation's, and the three tables sit in one column.
    sections = []
    for row, role in enumerate(roles):
        if role == ROW_SECTION:
            sections.append([])
        elif role == ROW_DATA and sections:
            sections[-1].append(row)
    for rows in sections:
        for column in ERA_COLUMNS:
            requests.append(_gradient(sheet_id, min(rows), max(rows), column))
        requests.append(_gradient(sheet_id, min(rows), max(rows), GAP_COLUMN,
                                  diverging=True))

    # Outer edge last, so it sits over the light grid drawn across everything.
    requests.append({"updateBorders": {
        "range": {"sheetId": sheet_id, "startRowIndex": min(data_rows) - 1,
                  "endRowIndex": max(data_rows) + 1, "startColumnIndex": 0,
                  "endColumnIndex": width},
        "left": _border(BORDER_OUT), "right": _border(BORDER_OUT)}})

    spreadsheet.batch_update({"requests": requests})


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
    _format(spreadsheet, sheet, values)
    print(f"[splits] wrote {len(values)} row(s) to {TARGET_SHEET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
