"""Mirror NPB's 分析表紀錄 into the プロ野球データ分析 workbook's 紀錄總表.

The two sheets are the same 83-column game log — verified header by header, the
only difference being that 分析表紀錄 leaves column A blank where 紀錄總表 carries
a pre-filled 編號. So this is a sheet-to-sheet copy, not a scrape: everything
紀錄總表 needs (用球数, 三振, 四死, 併打, 盜壘, 壘打数, and 被壘打, which no Yahoo box
score exposes) is already in the source.

Matching is by (日期, 客場球隊, 主場球隊) rather than by position, so a game NPB
publishes late, or one the weekly audit corrects after the fact, lands in the
right row instead of shifting everything below it.

Only columns B–CE are written. Column A's 編號 and the per-row formulas in
CF–CK / CZ / DA belong to 紀錄總表 and are left alone; they are already filled
down well past the end of the season.
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

SOURCE_HEADER_ROW = 2   # 分析表紀錄: group header on row 1, columns on row 2
TARGET_FIRST_ROW = 4    # 紀錄總表: group header row 2, columns row 3, data from 4
LAST_COLUMN = "CE"      # 83 columns, A–CE
COLUMN_COUNT = 83

# (date, away team, home team) — enough to identify a game, and all three sit in
# the first fourteen columns of both sheets.
KEY_COLUMNS = (1, 8, 11)   # B 日期, I 客場球隊, L 主場球隊


def _norm(value):
    return re.sub(r"\s+", "", str(value if value is not None else "")).strip()


def _row_key(row):
    return tuple(_norm(row[i]) if i < len(row) else "" for i in KEY_COLUMNS)


def _padded(row):
    """Columns B–CE of one row, padded so short rows do not shift on write."""
    return [(row[i] if i < len(row) else "") for i in range(1, COLUMN_COUNT)]


class RecordSync:
    def __init__(self, client, *, source_key):
        self.client = client
        self.source_key = source_key

    def _values(self, key, a1):
        response = self.client.http_client.request(
            "get",
            f"https://sheets.googleapis.com/v4/spreadsheets/{key}/values/{a1}",
            params={"valueRenderOption": "UNFORMATTED_VALUE",
                    # Dates come back as "2026/3/27" so a USER_ENTERED write puts
                    # a real date back, matching the rows already there.
                    "dateTimeRenderOption": "FORMATTED_STRING"},
        )
        return response.json().get("values", [])

    def check_headers(self):
        source = self._values(self.source_key,
                              f"'{SOURCE_SHEET}'!A{SOURCE_HEADER_ROW}:{LAST_COLUMN}{SOURCE_HEADER_ROW}")
        target = self._values(TARGET_KEY,
                              f"'{TARGET_SHEET}'!A{TARGET_FIRST_ROW - 1}:{LAST_COLUMN}{TARGET_FIRST_ROW - 1}")
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

    def plan(self):
        source = [r for r in self._values(
            self.source_key, f"'{SOURCE_SHEET}'!A{SOURCE_HEADER_ROW + 1}:{LAST_COLUMN}")
            if len(r) > 1 and _norm(r[1])]
        target = self._values(
            TARGET_KEY, f"'{TARGET_SHEET}'!A{TARGET_FIRST_ROW}:{LAST_COLUMN}")

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
        return appends, corrections

    def apply(self, updates):
        data = [{"range": f"'{TARGET_SHEET}'!B{row}:{LAST_COLUMN}{row}",
                 "values": [_padded(values)]} for row, values in updates]
        written = 0
        for start in range(0, len(data), 200):
            response = self.client.http_client.request(
                "post",
                f"https://sheets.googleapis.com/v4/spreadsheets/{TARGET_KEY}/values:batchUpdate",
                json={"valueInputOption": "USER_ENTERED", "data": data[start:start + 200]},
            )
            written += response.json().get("totalUpdatedCells", 0)
        return written


def main(dry_run=False):
    import npb

    client = GoogleSheetsClient().client
    sync = RecordSync(client, source_key=npb.NPB_SPREADSHEET_KEY)
    sync.check_headers()
    appends, corrections = sync.plan()

    print(f"新增 {len(appends)} 場、更正 {len(corrections)} 場")
    for row, values in (appends[:3] + corrections[:3]):
        print(f"   第{row}列  {values[1]}  {values[8]} @ {values[11]}")
    if dry_run:
        print("(dry-run，沒有寫入)")
        return 0
    if not appends and not corrections:
        print("已是最新，無須寫入")
        return 0
    written = sync.apply(appends + corrections)
    print(f"寫入 {written} 格")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="只列出差異，不寫入")
    raise SystemExit(main(dry_run=parser.parse_args().dry_run))
