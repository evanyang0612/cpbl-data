"""Keep a daily record of the weather each game was played in.

`npb_starters` already reads Yahoo's pinpoint forecast for every game on the
card — temperature, rain, and a wind direction resolved against the park's
bearing — and then throws it away once the Telegram broadcast has gone out.

That reading cannot be recovered later. Yahoo's forecast covers today and
nothing else: a request for yesterday returns a page with no forecast at all,
and npb.jp's box scores carry no weather of any kind. So a day not recorded on
the day is a day lost for good, which is the whole reason this exists.

    python -m baseball.npb_weather --dry-run
    python -m baseball.npb_weather                # what the sweep runs
    python -m baseball.npb_weather --date 2026-09-10
"""

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from baseball import npb_starters as ns
from baseball.sheets import GoogleSheetsClient

# The analysis workbook, beside 賽錄 and 先發明細, so a join never crosses
# workbooks.
SPREADSHEET_KEY = "1X2oaXk6DJLkx1MPVjc0lgLNtqa88X5qdNdKuKyikrbg"
SHEET_NAME = "天氣"

COLUMNS = ["日期", "球場", "屋頂", "天候", "氣溫", "降水mm",
           "風向", "風速", "對球場", "抓取時間"]

JST = ZoneInfo("Asia/Tokyo")


def _number(value):
    """A float where Yahoo gave one, and a blank where it gave nothing.

    A missing reading must not become a zero: 0mm of rain is a dry evening,
    and a blank is a forecast we never saw.
    """
    if value in (None, ""):
        return ""
    try:
        return float(value)
    except (TypeError, ValueError):
        return ""


def weather_rows(slate, game_date: str, captured_at: str) -> list[list]:
    """One row per game on the card.

    A Slate keys weather by team, and both sides of a game share one forecast,
    so the rows are collapsed on the venue.
    """
    rows = {}
    for weather in slate.weather.values():
        venue = weather.venue or ""
        if venue in rows:
            continue
        compass, _, speed = (weather.wind or "").partition(" ")
        rows[venue] = [
            game_date,
            venue,
            "有" if ns.is_roofed(venue) or ns.is_sheltered(venue) else "",
            weather.condition or "",
            _number(weather.temp_c),
            _number(weather.rain_mm),
            compass,
            _number(speed),
            # A compass point says nothing until it is read against the park;
            # wind_effect is the same reduction the broadcast already shows.
            ns.wind_effect(compass, ns.park_bearing(venue)) or "",
            captured_at,
        ]
    return sorted(rows.values(), key=lambda r: r[1])


def merge_rows(existing: list[list], fresh: list[list]) -> list[list]:
    """Existing rows with today's readings written over their own games.

    The sweep runs every half hour, so each game is read a dozen times a day.
    Only the newest reading is kept — it is the one closest to first pitch, and
    a dozen near-identical rows would make the tab harder to join against.
    """
    by_game = {(row[0], row[1]): row for row in existing}
    for row in fresh:
        by_game[(row[0], row[1])] = row
    return sorted(by_game.values(), key=lambda r: (r[0], r[1]))


def _open_sheet(client):
    spreadsheet = client.open_by_key(SPREADSHEET_KEY)
    try:
        return spreadsheet.worksheet(SHEET_NAME)
    except Exception:
        sheet = spreadsheet.add_worksheet(
            title=SHEET_NAME, rows=1000, cols=len(COLUMNS))
        sheet.update(values=[COLUMNS], range_name="A1")
        sheet.freeze(rows=1)
        return sheet


def run(game_date: str | None = None, *, dry_run: bool = False) -> list[list]:
    now = datetime.now(JST)
    game_date = game_date or now.strftime("%Y-%m-%d")
    captured_at = now.strftime("%Y-%m-%d %H:%M")

    slate = ns.fetch_slate(game_date)
    fresh = weather_rows(slate, game_date, captured_at)
    if not fresh:
        # No card today, or the forecast has not been posted yet. Either way
        # there is nothing to write and nothing to warn about.
        print(f"{game_date}: 沒有可記錄的天氣")
        return []

    for row in fresh:
        print("   ", row)
    if dry_run:
        return fresh

    sheet = _open_sheet(GoogleSheetsClient().client)
    values = sheet.get_all_values()
    existing = [r for r in values[1:] if len(r) >= 2 and r[0].strip()]
    merged = merge_rows(existing, fresh)
    if sheet.row_count < len(merged) + 1:
        sheet.resize(rows=len(merged) + 50, cols=len(COLUMNS))
    sheet.update(values=[COLUMNS] + merged, range_name="A1")
    print(f"{game_date}: 寫入 {len(fresh)} 場,{SHEET_NAME} 共 {len(merged)} 列")
    return fresh


def main():
    # The workflow supplies credentials through the environment; a local run
    # reads them from .env the way every other entry point here does.
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD,預設今天(JST)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
