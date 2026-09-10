"""Lay the cached npb.jp box scores out as a 先發明細 tab.

One row per starting pitcher per game, from 2016 on. This is the table the
"does a starter's recent form predict his next outing" question needs and
賽錄 could never answer: 賽錄 carries a starter's innings and earned runs, and
nothing about how he got there.

The relief appearances are in the cache too — every box score was parsed whole
— but they are not written here. A rolling bullpen-fatigue view is a different
question with a different shape, and 40,000 rows nobody reads would only make
this tab slower to open.

    python migration/write_npb_starter_log.py --dry-run
    python migration/write_npb_starter_log.py
"""

import argparse
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), ".env"))

from baseball.sheets import GoogleSheetsClient  # noqa: E402

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), ".cache", "npb_box")

# The analysis workbook, where 賽錄 and 過盤率紀錄 already live — everything a
# join would reach for is on the same side of the API.
SPREADSHEET_KEY = "1X2oaXk6DJLkx1MPVjc0lgLNtqa88X5qdNdKuKyikrbg"
SHEET_NAME = "先發明細"

COLUMNS = [
    "日期", "GameId", "球隊", "對手", "主客", "投手", "選手ID", "勝敗",
    "投球回", "投球數", "打者", "安打", "本壘打", "四球", "死球", "三振",
    "失分", "自責",
]


def starter_rows(record: dict) -> list[list]:
    """The two starters' rows for one game, or none at all.

    A game is written whole or not written: a rainout has no starters, and a
    record with only one side would read as a game the other team never
    pitched in.
    """
    if record.get("cancelled"):
        return []
    away_pitchers = record.get("away_pitchers") or []
    home_pitchers = record.get("home_pitchers") or []
    if not away_pitchers or not home_pitchers:
        return []

    rows = []
    for pitchers, team, opponent, side in (
        (away_pitchers, record["away"], record["home"], "客"),
        (home_pitchers, record["home"], record["away"], "主"),
    ):
        p = pitchers[0]
        rows.append([
            record["date"], record["game_code"], team, opponent, side,
            p["name"], p.get("player_id") or "", p.get("result", ""),
            # `.1`/`.2` notation cannot be summed, and every rolling window
            # downstream is a sum.
            p["outs"] / 3,
            p["pitches"], p["batters"], p["hits"], p["hr"],
            p["bb"], p["hbp"], p["so"], p["runs"], p["er"],
        ])
    return rows


def build_rows(records) -> list[list]:
    """Every starter's row, oldest game first."""
    ordered = sorted(records, key=lambda r: (r.get("date", ""),
                                             r.get("game_code", "")))
    return [row for record in ordered for row in starter_rows(record)]


def load_cache(cache_dir: str = CACHE_DIR) -> list[dict]:
    records = []
    for path in sorted(glob.glob(os.path.join(cache_dir, "*.json"))):
        with open(path, encoding="utf-8") as fh:
            records.append(json.load(fh))
    return records


def write_sheet(rows: list[list]) -> None:
    client = GoogleSheetsClient().client
    spreadsheet = client.open_by_key(SPREADSHEET_KEY)
    try:
        sheet = spreadsheet.worksheet(SHEET_NAME)
        sheet.clear()
    except Exception:
        sheet = spreadsheet.add_worksheet(
            title=SHEET_NAME, rows=len(rows) + 10, cols=len(COLUMNS))
    if sheet.row_count < len(rows) + 1:
        sheet.resize(rows=len(rows) + 1, cols=len(COLUMNS))
    sheet.update(values=[COLUMNS] + rows, range_name="A1")
    sheet.freeze(rows=1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    records = load_cache()
    rows = build_rows(records)
    cancelled = sum(1 for r in records if r.get("cancelled"))
    dates = [r[0] for r in rows]
    print(f"快取比賽 {len(records)} 場(中止 {cancelled})")
    print(f"先發列 {len(rows)}  範圍 {dates[0] if dates else '-'} ~ "
          f"{dates[-1] if dates else '-'}")
    if args.dry_run:
        for row in rows[:3]:
            print("   ", row)
        return
    write_sheet(rows)
    print(f"已寫入 {SHEET_NAME}")


if __name__ == "__main__":
    main()
