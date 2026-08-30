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
    COLUMN_GROUPS,
    FROZEN_ROWS,
    GROUP_STARTS,
    HEADERS,
    LEAGUE_WIDTH,
    LEAGUE_STARTS,
    ROW_DATA,
    ROW_HEADER,
    ROW_INFO,
    ROW_GROUP,
    ROW_LEAGUE,
    ROW_NOTE,
    ROW_SECTION,
    ROW_TITLE,
    TOTAL_WIDTH,
    build_sheet,
    league_blocks,
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
LEAGUE_CHIP = {"央聯": {"red": 0.180, "green": 0.490, "blue": 0.420},
               "洋聯": {"red": 0.180, "green": 0.361, "blue": 0.541}}
LEAGUE_TINT = {"央聯": {"red": 0.949, "green": 0.976, "blue": 0.969},
               "洋聯": {"red": 0.953, "green": 0.969, "blue": 0.988}}

# One league's widths, repeated on both halves with the spacer between them.
LEAGUE_COLUMN_WIDTHS = [92, 56, 58, 44, 56, 58, 44, 56, 58, 44, 62]
SPACER_WIDTH = 22

# 局數 / ERA / 名次 repeat three times across a league's half, then the gap.
INNINGS_COLUMNS = (1, 4, 7)
ERA_COLUMNS = (2, 5, 8)
RANK_COLUMNS = (3, 6, 9)
GAP_COLUMN = 10

# The line drawn between 全場, 主場 and 客場. Heavier than the grid, so the
# three splits read as three blocks rather than as nine columns in a row.
SPLIT_BORDER = {"red": 0.663, "green": 0.714, "blue": 0.769}


def _border(color):
    return {"style": "SOLID", "width": 1, "color": color}


def _cell_format(**fields):
    return {"userEnteredFormat": fields}


def _band(sheet_id, row, *, background, color, bold=False, size=11,
          align="CENTER", first_column=0, last_column=None):
    return {"repeatCell": {
        "range": {"sheetId": sheet_id, "startRowIndex": row, "endRowIndex": row + 1,
                  "startColumnIndex": first_column,
                  "endColumnIndex": TOTAL_WIDTH if last_column is None else last_column},
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
    width = TOTAL_WIDTH
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
        # The whole sheet, not just the new width: last run's banners may span
        # a column this one no longer has, and a partial range is refused.
        {"unmergeCells": {"range": {"sheetId": sheet_id}}},
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
                           "gridProperties": {"frozenRowCount": FROZEN_ROWS,
                                              "columnCount": width}},
            "fields": "gridProperties(frozenRowCount,columnCount)"}},
    ]
    widths = list(LEAGUE_COLUMN_WIDTHS) + [SPACER_WIDTH] + list(LEAGUE_COLUMN_WIDTHS)
    for index, pixels in enumerate(widths):
        requests.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": index, "endIndex": index + 1},
            "properties": {"pixelSize": pixels}, "fields": "pixelSize"}})

    banners = {ROW_TITLE: (NAVY_DARK, WHITE, True, 15),
               ROW_INFO: (BLUE_PALE, TEXT_HEAD, True, 10),
               ROW_NOTE: (BLUE_FAINT, TEXT_NOTE, False, 10),
               ROW_SECTION: (NAVY_MID, WHITE, True, 12),
               ROW_LEAGUE: (NAVY_MID, WHITE, True, 11),
               ROW_GROUP: (BLUE_PALE, TEXT_HEAD, True, 11),
               ROW_HEADER: (BLUE_PALE, TEXT_HEAD, True, 10)}
    for row, role in enumerate(roles):
        if role not in banners:
            continue
        background, color, bold, size = banners[role]
        align = ("LEFT" if role in (ROW_TITLE, ROW_INFO, ROW_NOTE, ROW_SECTION,
                                    ROW_LEAGUE) else "CENTER")
        if role == ROW_LEAGUE:
            # Two bands, one per half, each in its own league's colour.
            for league, start in LEAGUE_STARTS.items():
                requests.append(_band(sheet_id, row, background=LEAGUE_CHIP[league],
                                      color=color, bold=bold, size=size,
                                      align=align, first_column=start,
                                      last_column=start + LEAGUE_WIDTH))
            continue
        requests.append(_band(sheet_id, row, background=background, color=color,
                              bold=bold, size=size, align=align))
        if role not in (ROW_HEADER, ROW_GROUP):
            # One banner across the tab; a merged strip reads as a heading and
            # an unmerged one reads as a row with eleven empty cells.
            requests.append({"mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": row,
                          "endRowIndex": row + 1, "startColumnIndex": 0,
                          "endColumnIndex": width},
                "mergeType": "MERGE_ROWS"}})

    # 全場 / 主場 / 客場 span their three columns; 球隊 and 主-客 have no
    # sub-columns, so they take both header rows instead of leaving a hole.
    group_row, header_row = roles.index(ROW_GROUP), roles.index(ROW_HEADER)
    for start in LEAGUE_STARTS.values():
        for offset, (_label, span) in zip(GROUP_STARTS, COLUMN_GROUPS):
            first = start + offset
            requests.append({"mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": group_row,
                          "endRowIndex": (header_row + 1) if span == 1
                          else group_row + 1,
                          "startColumnIndex": first,
                          "endColumnIndex": first + span},
                "mergeType": "MERGE_ALL"}})

    data_rows = [row for row, role in enumerate(roles) if role == ROW_DATA]

    # Each block takes the palest wash of its league's colour — faint enough
    # that the ERA gradient still reads over it, strong enough that the two
    # tables never run together.
    blocks = league_blocks(values)
    for league, start, first, last in blocks:
        requests.append({"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": first,
                      "endRowIndex": last + 1, "startColumnIndex": start,
                      "endColumnIndex": start + LEAGUE_WIDTH},
            "cell": _cell_format(backgroundColor=LEAGUE_TINT[league]),
            "fields": "userEnteredFormat.backgroundColor"}})
        # The team is the row's name, so it carries the weight the rest does not.
        requests.append({"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": first,
                      "endRowIndex": last + 1, "startColumnIndex": start,
                      "endColumnIndex": start + 1},
            "cell": _cell_format(
                horizontalAlignment="LEFT",
                textFormat={"foregroundColor": TEXT_HEAD, "bold": True,
                            "fontSize": 11}),
            "fields": "userEnteredFormat(horizontalAlignment,textFormat)"}})

    for columns, pattern in ((INNINGS_COLUMNS, "0.0"), (ERA_COLUMNS, "0.00"),
                             (RANK_COLUMNS, "0"),
                             ((GAP_COLUMN,), "+0.00;-0.00;0.00")):
        for offset in columns:
            for start in LEAGUE_STARTS.values():
                column = start + offset
                requests.append({"repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": min(data_rows),
                              "endRowIndex": max(data_rows) + 1,
                              "startColumnIndex": column,
                              "endColumnIndex": column + 1},
                    "cell": _cell_format(numberFormat={"type": "NUMBER",
                                                       "pattern": pattern}),
                    "fields": "userEnteredFormat.numberFormat"}})

    # The gradient is read down one league block at a time: a bullpen ERA
    # means nothing against a rotation's, and a team is ranked against its own
    # league rather than against all twelve.
    for _, start, first, last in blocks:
        for offset in ERA_COLUMNS:
            requests.append(_gradient(sheet_id, first, last, start + offset))
        requests.append(_gradient(sheet_id, first, last, start + GAP_COLUMN,
                                  diverging=True))

    # Fences last, so they sit over the light grid drawn across everything: one
    # down each league's outer edge, one before every split inside it.
    for start in LEAGUE_STARTS.values():
        for offset in GROUP_STARTS[1:]:
            requests.append({"updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": group_row,
                          "endRowIndex": max(data_rows) + 1,
                          "startColumnIndex": start + offset,
                          "endColumnIndex": start + offset + 1},
                "left": _border(SPLIT_BORDER)}})
        requests.append({"updateBorders": {
            "range": {"sheetId": sheet_id, "startRowIndex": group_row,
                      "endRowIndex": max(data_rows) + 1,
                      "startColumnIndex": start,
                      "endColumnIndex": start + LEAGUE_WIDTH},
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
    # Before the values, not after: writing into a merged range keeps only the
    # top-left cell and drops the rest, so last run's banners would silently
    # eat this run's header row.
    spreadsheet.batch_update(
        {"requests": [{"unmergeCells": {"range": {"sheetId": sheet.id}}}]})
    sheet.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
    _format(spreadsheet, sheet, values)
    print(f"[splits] wrote {len(values)} row(s) to {TARGET_SHEET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
