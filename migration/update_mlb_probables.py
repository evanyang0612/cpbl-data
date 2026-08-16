"""Fill 設定 with the day's announced starters, and refresh the 先発 dropdown lists.

Two chores that were being done by hand:

1. **設定** — MLB Stats API publishes probable pitchers for today and tomorrow
   (`schedule?hydrate=probablePitcher`), so the 15 matchup blocks can be filled in
   before anyone opens the sheet. Game 1 gets both teams and both starters; game 2 and
   3 get starters only, because their 隊伍 cells are `=B4` / `=B6` mirrors of game 1 and
   a series is the same two clubs. A block whose pairing does not repeat the next day
   (the series ended) is left blank rather than filled with the wrong matchup.

2. **AL-P / NL-P** — the per-team lists behind the 先発 dropdowns, which had been kept
   by hand and still held 2019 rosters. They are rebuilt from 紀錄's own starter
   columns, so only pitchers who have actually started for that club appear — no
   relievers — ordered by most recent start.

    uv run python migration/update_mlb_probables.py --dry-run
    uv run python migration/update_mlb_probables.py
    uv run python migration/update_mlb_probables.py --skip-rosters
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from baseball.mlb_teams import canonical_team_code  # noqa: E402
from migration.add_mlb_matchup_sheets import (  # noqa: E402
    MATCHUP_COUNT,
    MLB_SPREADSHEET_KEY,
    SETTING_BANDS,
    SETTING_TITLE,
    col_letter,
    setting_block_col,
    setting_pitcher_cell,
    setting_team_cell,
)

MLB_API = "https://statsapi.mlb.com/api/v1/schedule"
REQUEST_TIMEOUT = (10, 60)
BALLPARK_TZ = ZoneInfo("America/New_York")  # MLB's schedule day is the US date

RECORD_TITLE = "紀錄"
ROSTER_SHEETS = ("AL-P", "NL-P")
ROSTER_FIRST_ROW = 2
ROSTER_LIMIT = 29  # rows A2:A30 behind each dropdown

DATE_COL, AWAY_TEAM_COL, AWAY_SP_COL = "A", "C", "D"
HOME_TEAM_COL, HOME_SP_COL = "R", "AE"


# ===================================================================================
# pure helpers
# ===================================================================================
def schedule_matchups(payload: dict) -> list[dict]:
    """Regular-season games for a date, in first-pitch order, with both probables."""
    games = []
    for date in payload.get("dates", []):
        for game in date.get("games", []):
            if game.get("gameType") != "R":
                continue
            state = (game.get("status") or {}).get("detailedState", "")
            if any(word in state for word in ("Postponed", "Cancelled", "Suspended")):
                continue
            teams = game.get("teams", {})
            away, home = teams.get("away", {}), teams.get("home", {})
            games.append({
                "start": game.get("gameDate", ""),
                "away": canonical_team_code(
                    (away.get("team") or {}).get("abbreviation", "")),
                "home": canonical_team_code(
                    (home.get("team") or {}).get("abbreviation", "")),
                "away_starter": (away.get("probablePitcher") or {}).get("fullName", ""),
                "home_starter": (home.get("probablePitcher") or {}).get("fullName", ""),
            })
    return sorted(games, key=lambda g: g["start"])


def setting_writes(days: list[list[dict]], *, report_overflow: bool = False):
    """Cells to write into 設定 for up to three days of games.

    `days[0]` fills game 1 of every block (teams and starters); `days[1]` and `days[2]`
    fill only the starter cells of games 2 and 3, and only where the block's pairing
    repeats — a mirrored 隊伍 cell must never be overwritten with a literal.
    """
    writes: list[tuple[str, str]] = []
    first_day = days[0] if days else []

    for block in range(MATCHUP_COUNT):
        game = first_day[block] if block < len(first_day) else None
        away = game["away"] if game else ""
        home = game["home"] if game else ""
        writes += [
            (setting_team_cell(block, 0, 0), away),
            (setting_pitcher_cell(block, 0, 0), game["away_starter"] if game else ""),
            (setting_team_cell(block, 0, 1), home),
            (setting_pitcher_cell(block, 0, 1), game["home_starter"] if game else ""),
        ]
        for band in range(1, SETTING_BANDS):
            later = days[band] if band < len(days) else []
            match = next((g for g in later
                          if game and g["away"] == away and g["home"] == home), None)
            writes += [
                (setting_pitcher_cell(block, band, 0),
                 match["away_starter"] if match else ""),
                (setting_pitcher_cell(block, band, 1),
                 match["home_starter"] if match else ""),
            ]
    overflow = first_day[MATCHUP_COUNT:]
    return (writes, overflow) if report_overflow else writes


def starters_by_team(rows, limit: int = ROSTER_LIMIT) -> dict[str, list[str]]:
    """Each club's starting pitchers, most recent start first.

    Built from 紀錄's 客隊先發 / 主隊先發 columns, so a reliever cannot get in.
    """
    seen: dict[str, list[str]] = {}
    for row in reversed(rows):
        row = list(row) + [""] * (5 - len(row))
        _, away_team, away_sp, home_team, home_sp = row[:5]
        for team, pitcher in ((away_team, away_sp), (home_team, home_sp)):
            team, pitcher = str(team).strip(), str(pitcher).strip()
            if not team or not pitcher:
                continue
            names = seen.setdefault(team, [])
            if pitcher not in names and len(names) < limit:
                names.append(pitcher)
    return seen


# ===================================================================================
# sheet I/O
# ===================================================================================
def fetch_day(session: requests.Session, day) -> list[dict]:
    response = session.get(MLB_API, params={
        "sportId": 1, "date": day.strftime("%Y-%m-%d"),
        # `team` is what carries `abbreviation`; without it the codes come back blank
        "hydrate": "probablePitcher,team",
    }, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return schedule_matchups(response.json())


def fill_setting(ss, days: list[list[dict]], *, dry_run: bool) -> None:
    writes, overflow = setting_writes(days, report_overflow=True)
    filled = sum(1 for _, value in writes if value)
    later = [sum(1 for game in day if game["away_starter"] and game["home_starter"])
             for day in days[1:]]
    print(f"{SETTING_TITLE}: {len(days[0])} game(s) today, "
          f"{filled}/{len(writes)} cell(s) filled; "
          f"next days announced: {later}")
    for block, game in enumerate(days[0][:MATCHUP_COUNT]):
        column = col_letter(setting_block_col(block))
        print(f"  {column}: {game['away']} {game['away_starter'] or '(未定)'} @ "
              f"{game['home']} {game['home_starter'] or '(未定)'}")
    for game in overflow:
        print(f"  ! no column left for {game['away']} @ {game['home']} "
              f"(doubleheader?) — fill it in by hand")
    if dry_run:
        return
    ss.values_batch_update({
        "valueInputOption": "USER_ENTERED",
        "data": [{"range": f"'{SETTING_TITLE}'!{a1}", "values": [[value]]}
                 for a1, value in writes],
    })


def refresh_rosters(ss, *, dry_run: bool) -> None:
    record = ss.worksheet(RECORD_TITLE)
    last_row = len(record.col_values(1))
    ranges = [f"{col}{2}:{col}{last_row}" for col in
              (DATE_COL, AWAY_TEAM_COL, AWAY_SP_COL, HOME_TEAM_COL, HOME_SP_COL)]
    columns = record.batch_get(ranges, value_render_option="FORMATTED_VALUE")
    rows = [
        [cell[0] if cell else "" for cell in cells]
        for cells in zip(*[col + [[]] * (last_row - len(col)) for col in columns])
    ]
    starters = starters_by_team(rows)

    updates, summary = [], []
    for title in ROSTER_SHEETS:
        ws = ss.worksheet(title)
        header = ws.row_values(1)
        for index, team in enumerate(header):
            team = team.strip()
            names = starters.get(team)
            if not team or not names:
                continue
            column = col_letter(index)
            padded = names + [""] * (ROSTER_LIMIT - len(names))
            updates.append({
                "range": f"'{title}'!{column}{ROSTER_FIRST_ROW}:"
                         f"{column}{ROSTER_FIRST_ROW + ROSTER_LIMIT - 1}",
                "values": [[name] for name in padded],
            })
            summary.append(f"{team}={len(names)}")
    print(f"rosters: {len(updates)} team column(s) rebuilt from {RECORD_TITLE} "
          f"starters — {', '.join(summary[:6])}…")
    if updates and not dry_run:
        ss.values_batch_update({"valueInputOption": "RAW", "data": updates})


def main(dry_run: bool = False, skip_rosters: bool = False,
         days_ahead: int = SETTING_BANDS) -> None:
    from baseball.sheets import GoogleSheetsClient

    session = requests.Session()
    today = datetime.now(BALLPARK_TZ).date()
    days = [fetch_day(session, today + timedelta(days=offset))
            for offset in range(days_ahead)]

    ss = GoogleSheetsClient().spreadsheet(MLB_SPREADSHEET_KEY)
    fill_setting(ss, days, dry_run=dry_run)
    if not skip_rosters:
        refresh_rosters(ss, dry_run=dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="print what would be written, change nothing")
    parser.add_argument("--skip-rosters", action="store_true",
                        help="only fill 設定; leave the AL-P / NL-P lists alone")
    args = parser.parse_args()
    main(dry_run=args.dry_run, skip_rosters=args.skip_rosters)
