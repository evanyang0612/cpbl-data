"""One-off: rewrite legacy team codes in 紀錄 onto the canonical one.

MLB Stats API began abbreviating the Athletics as ATH in 2025; 紀錄 holds OAK for
2017-2024 and ATH from 2025, which silently splits every aggregation keyed on a team
label (MLB勝敗表 shows 0 wins for OAK the moment its window reaches 2025).

`migration/update_mlb_record.py` now maps the API's code through
`canonical_team_code()` on the way in, so this script only has to fix the rows that
were written before that. It touches nothing but 客隊隊伍 (C) and 主隊隊伍 (R), dumps
every change to a JSON backup first, and can be re-run safely — a second pass finds
nothing to do.

    uv run python migration/normalize_mlb_team_codes.py --dry-run
    uv run python migration/normalize_mlb_team_codes.py
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/Users/evansmac/cpbl")

from dotenv import load_dotenv

load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")

from migration.update_mlb_record import (  # noqa: E402
    SPREADSHEET_KEY,
    WORKSHEET_NAME,
    TEAM_CODE_ALIASES,
    canonical_team_code,
)

AWAY_TEAM_COL_0IDX = 2   # 紀錄!C 客隊隊伍
HOME_TEAM_COL_0IDX = 17  # 紀錄!R 主隊隊伍
FIRST_DATA_ROW = 2
BACKUP_DIR = Path("/Users/evansmac/cpbl/.cache")


def col_letter(idx0: int) -> str:
    idx = idx0 + 1
    letters = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def plan_row_updates(rows: list[list], *, first_row: int, away_col: int,
                     home_col: int) -> list[dict]:
    """Every cell needing a rewrite, as {row, column, old, new}."""
    updates = []
    for offset, row in enumerate(rows):
        for col_idx in (away_col, home_col):
            if col_idx >= len(row):
                continue
            value = str(row[col_idx])
            canonical = canonical_team_code(value)
            if canonical != value:
                updates.append({
                    "row": first_row + offset,
                    "column": col_letter(col_idx),
                    "old": value,
                    "new": canonical,
                })
    return updates


def main(dry_run: bool = False) -> None:
    from baseball.sheets import GoogleSheetsClient

    client = GoogleSheetsClient()
    ws = client.worksheet(SPREADSHEET_KEY, WORKSHEET_NAME)
    last_col = col_letter(HOME_TEAM_COL_0IDX)
    rows = ws.get(f"A{FIRST_DATA_ROW}:{last_col}", value_render_option="FORMATTED_VALUE")

    updates = plan_row_updates(rows, first_row=FIRST_DATA_ROW,
                               away_col=AWAY_TEAM_COL_0IDX,
                               home_col=HOME_TEAM_COL_0IDX)
    counts: dict[str, int] = {}
    for u in updates:
        counts[f'{u["old"]} -> {u["new"]}'] = counts.get(f'{u["old"]} -> {u["new"]}', 0) + 1
    print(f"{WORKSHEET_NAME}: {len(rows)} data rows scanned, "
          f"{len(updates)} cell(s) to rewrite")
    for label, n in sorted(counts.items()):
        print(f"  {label}: {n}")
    if updates:
        first, last = updates[0], updates[-1]
        print(f"  rows {first['row']}..{last['row']} "
              f"(dates {rows[first['row'] - FIRST_DATA_ROW][0]} .. "
              f"{rows[last['row'] - FIRST_DATA_ROW][0]})")
    if not updates or dry_run:
        return

    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = BACKUP_DIR / f"mlb_team_code_backfill_{stamp}.json"
    backup.write_text(json.dumps({
        "spreadsheet": SPREADSHEET_KEY,
        "worksheet": WORKSHEET_NAME,
        "aliases": TEAM_CODE_ALIASES,
        "updates": updates,
    }, ensure_ascii=False, indent=2))
    print(f"backup written: {backup}")

    ws.spreadsheet.values_batch_update({
        "valueInputOption": "RAW",
        "data": [{"range": f"'{WORKSHEET_NAME}'!{u['column']}{u['row']}",
                  "values": [[u["new"]]]} for u in updates],
    })
    print(f"rewrote {len(updates)} cell(s)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would change, write nothing")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
