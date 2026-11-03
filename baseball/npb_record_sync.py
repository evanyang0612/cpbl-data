"""Mirror NPB's 分析表紀錄 into the プロ野球データ分析 workbook, one tab per season.

The two sheets are the same 83-column game log — verified header by header, the
only difference being that 分析表紀錄 leaves column A blank where 紀錄總表 carries
a pre-filled 編號. So this is a sheet-to-sheet copy, not a scrape: everything
紀錄總表 needs (用球数, 三振, 四死, 併打, 盜壘, 壘打数, and 被壘打, which no Yahoo box
score exposes) is already in the source.

Each season's games live in their own tab, 「2026・紀錄總表」, 「2027・紀錄總表」…
紀錄總表 itself holds no games any more: its A4:CE is one formula showing the
season picked in B1. Every analysis tab in the workbook — some 140,000
references to '紀錄總表'!X4:X993 — keeps reading 紀錄總表, so switching B1
switches the whole workbook to that season without touching a formula. The
formula names each year tab directly rather than going through INDIRECT, which
would recalculate all of those references on every edit anywhere.

The first season of a tab is made by copying 紀錄總表 as it stands, so nothing
typed into it by hand is lost; a later season copies the previous season's tab
and empties B–CE, keeping 編號, the formatting and the per-row formulas. When a
new season's tab appears, B1 moves to it.

Matching is by (日期, 客場球隊, 主場球隊) rather than by position, so a game NPB
publishes late, or one the weekly audit corrects after the fact, lands in the
right row instead of shifting everything below it.

Only columns B–CE of a year tab are written. Column A's 編號 and the per-row
formulas in CF–CK / CZ / DA are left alone; they are filled down to row 993.
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from baseball.sheets import GoogleSheetsClient  # noqa: E402

TARGET_KEY = "1FTn1L5hi3TAHSf9JjoiEpcBYUlJ46cV49cxldfL3Wzc"
TARGET_SHEET = "紀錄總表"
SOURCE_SHEET = "分析表紀錄"
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"

SOURCE_HEADER_ROW = 2   # 分析表紀錄: group header on row 1, columns on row 2
TARGET_FIRST_ROW = 4    # 紀錄總表: group header row 2, columns row 3, data from 4
TARGET_LAST_ROW = 993   # every analysis tab reads '紀錄總表'!X4:X993
LAST_COLUMN = "CE"      # 83 columns, A–CE
COLUMN_COUNT = 83
YEAR_CELL = "B1"        # the season 紀錄總表 shows

# (date, away team, home team) — enough to identify a game, and all three sit in
# the first fourteen columns of both sheets.
KEY_COLUMNS = (1, 8, 11)   # B 日期, I 客場球隊, L 主場球隊

_YEAR_TAB = re.compile(r"^(\d{4})・" + TARGET_SHEET + "$")


def year_tab(year):
    return f"{year}・{TARGET_SHEET}"


def tab_year(title):
    match = _YEAR_TAB.match(title)
    return int(match.group(1)) if match else None


def row_year(row):
    match = re.match(r"\s*(\d{4})/\d{1,2}/\d{1,2}", str(row[1])) if len(row) > 1 else None
    return int(match.group(1)) if match else None


def view_formula(years):
    years = sorted(years)
    ranges = ",".join(
        f"'{year_tab(y)}'!A{TARGET_FIRST_ROW}:{LAST_COLUMN}{TARGET_LAST_ROW}" for y in years)
    return (f"=ARRAYFORMULA(CHOOSE(MATCH(VALUE(${YEAR_CELL[0]}${YEAR_CELL[1:]}),"
            f"{{{','.join(map(str, years))}}},0),{ranges}))")


def layout_actions(*, view_years, tab_years, source_years):
    """The year tabs to make, in order.

    ("adopt", year, None) copies 紀錄總表 as it stands into that year's tab; it
    happens once, when no year tab exists yet. ("new", year, from_year) copies
    from_year's tab and empties it.
    """
    actions = []
    tab_years = set(tab_years)
    if not tab_years:
        if len(view_years) > 1:
            raise RuntimeError(
                f"{TARGET_SHEET} 混了 {sorted(view_years)} 好幾季，無法判斷要轉成哪一年的分頁")
        if not view_years and not source_years:
            return []
        year = next(iter(view_years)) if view_years else min(source_years)
        actions.append(("adopt", year, None))
        tab_years.add(year)
    for year in sorted(set(source_years) - tab_years):
        actions.append(("new", year, max(tab_years)))
        tab_years.add(year)
    return actions


def _norm(value):
    return re.sub(r"\s+", "", str(value if value is not None else "")).strip()


def _row_key(row):
    return tuple(_norm(row[i]) if i < len(row) else "" for i in KEY_COLUMNS)


def _padded(row):
    """Columns B–CE of one row, padded so short rows do not shift on write."""
    return [(row[i] if i < len(row) else "") for i in range(1, COLUMN_COUNT)]


def plan_rows(source, target):
    """(appends, corrections) as (sheet row, source row) for one year tab.

    target is the tab's rows from TARGET_FIRST_ROW down.
    """
    seen = {}
    last_filled = TARGET_FIRST_ROW - 1
    for offset, row in enumerate(target):
        if len(row) > 1 and _norm(row[1]):
            seen[_row_key(row)] = (TARGET_FIRST_ROW + offset, row)
            last_filled = TARGET_FIRST_ROW + offset

    appends, corrections = [], []
    next_row = last_filled + 1
    for row in source:
        key = _row_key(row)
        if key in seen:
            row_number, existing = seen[key]
            if _padded(row) != _padded(existing):
                corrections.append((row_number, row))
            continue
        appends.append((next_row, row))
        next_row += 1
    if next_row - 1 > TARGET_LAST_ROW:
        raise RuntimeError(
            f"這一季要寫到第 {next_row - 1} 列，超過分析分頁讀到的第 {TARGET_LAST_ROW} 列")
    return appends, corrections


class RecordSync:
    def __init__(self, client, *, source_key):
        self.http = client.http_client
        self.source_key = source_key

    # --- Sheets API ------------------------------------------------------

    def _values(self, key, a1, render="UNFORMATTED_VALUE"):
        response = self.http.request(
            "get", f"{SHEETS_API}/{key}/values/{a1}",
            params={"valueRenderOption": render,
                    # Dates come back as "2026/3/27" so a USER_ENTERED write puts
                    # a real date back, matching the rows already there.
                    "dateTimeRenderOption": "FORMATTED_STRING"},
        )
        return response.json().get("values", [])

    def _batch(self, requests):
        return self.http.request(
            "post", f"{SHEETS_API}/{TARGET_KEY}:batchUpdate",
            json={"requests": requests}).json()

    def _write(self, data):
        written = 0
        for start in range(0, len(data), 200):
            response = self.http.request(
                "post", f"{SHEETS_API}/{TARGET_KEY}/values:batchUpdate",
                json={"valueInputOption": "USER_ENTERED", "data": data[start:start + 200]})
            written += response.json().get("totalUpdatedCells", 0)
        return written

    def _clear(self, a1):
        self.http.request("post", f"{SHEETS_API}/{TARGET_KEY}/values:batchClear",
                          json={"ranges": [a1]})

    def tabs(self):
        response = self.http.request(
            "get", f"{SHEETS_API}/{TARGET_KEY}",
            params={"fields": "sheets(properties(title,sheetId,index))"}).json()
        return {s["properties"]["title"]: s["properties"] for s in response["sheets"]}

    # --- reading ---------------------------------------------------------

    def source_rows(self):
        return [r for r in self._values(
            self.source_key, f"'{SOURCE_SHEET}'!A{SOURCE_HEADER_ROW + 1}:{LAST_COLUMN}")
            if len(r) > 1 and _norm(r[1])]

    def tab_rows(self, tab):
        return self._values(
            TARGET_KEY,
            f"'{tab}'!A{TARGET_FIRST_ROW}:{LAST_COLUMN}{TARGET_LAST_ROW}")

    def check_headers(self, tab):
        source = self._values(self.source_key,
                              f"'{SOURCE_SHEET}'!A{SOURCE_HEADER_ROW}:{LAST_COLUMN}{SOURCE_HEADER_ROW}")
        target = self._values(TARGET_KEY,
                              f"'{tab}'!A{TARGET_FIRST_ROW - 1}:{LAST_COLUMN}{TARGET_FIRST_ROW - 1}")
        if not source or not target:
            raise RuntimeError("could not read either header row")
        mismatched = [
            (i, _norm(source[0][i] if i < len(source[0]) else ""),
             _norm(target[0][i] if i < len(target[0]) else ""))
            # Column A is 編號 on the target and blank on the source, by design.
            for i in range(1, COLUMN_COUNT)
            if _norm(source[0][i] if i < len(source[0]) else "")
            != _norm(target[0][i] if i < len(target[0]) else "")
        ]
        if mismatched:
            raise RuntimeError(f"headers have drifted apart: {mismatched[:5]}")

    # --- layout ----------------------------------------------------------

    def make_tab(self, action):
        kind, year, from_year = action
        tabs = self.tabs()
        source = TARGET_SHEET if kind == "adopt" else year_tab(from_year)
        if kind == "adopt":
            first = self._values(TARGET_KEY, f"'{TARGET_SHEET}'!A{TARGET_FIRST_ROW}", "FORMULA")
            if first and first[0] and str(first[0][0]).startswith("="):
                raise RuntimeError(
                    f"{TARGET_SHEET} 已經是切換年度的公式，但找不到任何年度分頁")
        self._batch([{"duplicateSheet": {
            "sourceSheetId": tabs[source]["sheetId"],
            "insertSheetIndex": tabs[TARGET_SHEET]["index"] + 1,
            "newSheetName": year_tab(year),
        }}])
        if kind == "new":
            self._clear(f"'{year_tab(year)}'!B{TARGET_FIRST_ROW}:{LAST_COLUMN}{TARGET_LAST_ROW}")

    def point_view(self, years, select=None):
        """Make 紀錄總表 show the year tabs, switching to `select` if given."""
        tabs = self.tabs()
        sheet_id = tabs[TARGET_SHEET]["sheetId"]
        formula = view_formula(years)
        current = self._values(TARGET_KEY, f"'{TARGET_SHEET}'!A{TARGET_FIRST_ROW}", "FORMULA")
        shown = self._values(TARGET_KEY, f"'{TARGET_SHEET}'!{YEAR_CELL}")
        shown = shown[0][0] if shown and shown[0] else ""
        changed = not current or current[0][0] != formula
        if changed:
            self._clear(f"'{TARGET_SHEET}'!A{TARGET_FIRST_ROW}:{LAST_COLUMN}{TARGET_LAST_ROW}")
        if select is None and str(shown) not in {str(y) for y in years}:
            select = max(years)
        data = []
        if select is not None:
            data.append({"range": f"'{TARGET_SHEET}'!{YEAR_CELL}", "values": [[select]]})
        if changed:
            data.append({"range": f"'{TARGET_SHEET}'!A{TARGET_FIRST_ROW}", "values": [[formula]]})
        if data:
            self._write(data)
        if changed:
            col = ord(YEAR_CELL[0]) - ord("A")
            row = int(YEAR_CELL[1:]) - 1
            cell = {"sheetId": sheet_id, "startRowIndex": row, "endRowIndex": row + 1,
                    "startColumnIndex": col, "endColumnIndex": col + 1}
            self._batch([
                {"setDataValidation": {"range": cell, "rule": {
                    "condition": {"type": "ONE_OF_LIST", "values": [
                        {"userEnteredValue": str(y)} for y in sorted(years, reverse=True)]},
                    "showCustomUi": True, "strict": True}}},
                # Column B is formatted as dates; the year must read as 2026.
                {"repeatCell": {"range": cell,
                                "cell": {"userEnteredFormat": {
                                    "numberFormat": {"type": "NUMBER", "pattern": "0"},
                                    "textFormat": {"bold": True}}},
                                "fields": "userEnteredFormat(numberFormat,textFormat.bold)"}},
            ])
        return changed, select

    def apply(self, tab, updates):
        return self._write([
            {"range": f"'{tab}'!B{row}:{LAST_COLUMN}{row}", "values": [_padded(values)]}
            for row, values in updates])


def main(dry_run=False):
    import npb

    client = GoogleSheetsClient().client
    sync = RecordSync(client, source_key=npb.NPB_SPREADSHEET_KEY)

    by_year = {}
    for row in sync.source_rows():
        year = row_year(row)
        if year is None:
            raise RuntimeError(f"{SOURCE_SHEET} 有一列日期看不懂：{row[:2]}")
        by_year.setdefault(year, []).append(row)

    tab_years = {y for title in sync.tabs() if (y := tab_year(title))}
    view_rows = sync.tab_rows(TARGET_SHEET) if not tab_years else []
    view_years = {y for r in view_rows if (y := row_year(r))}
    actions = layout_actions(view_years=view_years, tab_years=tab_years,
                             source_years=set(by_year))

    for kind, year, from_year in actions:
        how = (f"把目前的 {TARGET_SHEET} 複製成" if kind == "adopt"
               else f"從 {year_tab(from_year)} 複製一份空白的")
        print(f"{how}「{year_tab(year)}」")
        if not dry_run:
            sync.make_tab((kind, year, from_year))
    years = tab_years | {year for _, year, _ in actions}
    if not years:
        print("沒有任何比賽，無須寫入")
        return 0
    if not dry_run:
        newest = max(year for _, year, _ in actions) if actions else None
        changed, shown = sync.point_view(years, select=newest)
        if changed:
            print(f"{TARGET_SHEET} 改為依 {YEAR_CELL} 切換年度（{sorted(years)}）")
        if shown is not None:
            print(f"{TARGET_SHEET} 顯示 {shown} 年")

    adopted = {year for kind, year, _ in actions if kind == "adopt"}
    fresh = {year for kind, year, _ in actions if kind == "new"}
    total = 0
    for year in sorted(by_year):
        tab = year_tab(year)
        # A dry run makes no tabs: an adopted year is 紀錄總表 as it stands, a
        # new one starts empty, and both share 紀錄總表's header.
        if dry_run and year in adopted:
            target, header_tab = view_rows, TARGET_SHEET
        elif dry_run and year in fresh:
            target, header_tab = [], TARGET_SHEET
        else:
            target, header_tab = sync.tab_rows(tab), tab
        sync.check_headers(header_tab)
        appends, corrections = plan_rows(by_year[year], target)
        print(f"{tab}：新增 {len(appends)} 場、更正 {len(corrections)} 場")
        for row, values in (appends[:3] + corrections[:3]):
            print(f"   第{row}列  {values[1]}  {values[8]} @ {values[11]}")
        if not dry_run and (appends or corrections):
            total += sync.apply(tab, appends + corrections)
    if dry_run:
        print("(dry-run，沒有寫入)")
    elif total:
        print(f"寫入 {total} 格")
    else:
        print("已是最新，無須寫入")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="只列出差異，不寫入")
    raise SystemExit(main(dry_run=parser.parse_args().dry_run))
