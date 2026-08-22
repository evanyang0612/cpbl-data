"""Backfill MLB regular-season games into the 紀錄 worksheet.

The target worksheet stores raw box-score fields in A:AO and formula-driven
fields in AP:BD. This script writes only A:AO; it copies formulas down from
the last dated row only when the rows it is about to fill do not already have
them.

That condition is the whole point. 紀錄 is meant to be kept the way NPB keeps
賽錄: rows are pre-built as numbered placeholders with their formulas already
in place, and the daily run does nothing but drop values into them. Every
separate write to this workbook starts a recalculation that the next call has
to wait out -- about eight minutes at 23k rows, because ~30k SUMPRODUCT and
COUNTIFS cells across twenty sheets each scan whole columns of it. Adding
rows, pasting formulas and writing values used to be three such waits a day;
against pre-built rows it is one.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import requests
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from baseball.mlb_teams import TEAM_CODE_ALIASES, canonical_team_code  # noqa: F401
from cpbl import _sheets_client


SPREADSHEET_KEY = "11FV70TXVAxLTwYH6pLj7HwK1qq-fIa61QrePRCC8YUM"
WORKSHEET_NAME = "紀錄"
MLB_API = "https://statsapi.mlb.com/api"
REQUEST_TIMEOUT = (10, 60)
CACHE_DIR = Path(tempfile.gettempdir())
RAW_COLUMN_COUNT = 41
RAW_END_COLUMN = "AO"
FORMULA_START_COL_0IDX = 41
FORMULA_END_COL_0IDX = 56
FORMULA_FIRST_COLUMN = "AP"
MAX_CONCURRENT = 8



def _get_json(session: requests.Session, url: str, **params: Any) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 5:
                break
            sleep_for = min(2**attempt, 20)
            print(
                f"Request failed ({attempt}/5), retrying in {sleep_for}s: {url}",
                flush=True,
            )
            time.sleep(sleep_for)
    raise RuntimeError(f"Failed to fetch {url}") from last_error


def _with_retries(label: str, fn: Any) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            return fn()
        except (APIError, requests.RequestException) as exc:
            last_error = exc
            if attempt == 5:
                break
            sleep_for = min(2**attempt, 20)
            print(
                f"{label} failed ({attempt}/5), retrying in {sleep_for}s",
                flush=True,
            )
            time.sleep(sleep_for)
    raise RuntimeError(f"{label} failed after retries") from last_error


def _division_abbrev(team: dict[str, Any]) -> str:
    league = team.get("league", {}).get("name", "")
    division = team.get("division", {}).get("name", "")
    league_code = "AL" if "American" in league else "NL"
    if "East" in division:
        return f"{league_code}E"
    if "Central" in division:
        return f"{league_code}C"
    if "West" in division:
        return f"{league_code}W"
    return league_code


def _inning_runs(linescore: dict[str, Any], side: str) -> list[Any]:
    runs = []
    for inning in linescore.get("innings", [])[:9]:
        runs.append(inning.get(side, {}).get("runs", ""))
    return runs + [""] * (9 - len(runs))


def _starter(box_team: dict[str, Any]) -> tuple[int | None, dict[str, Any]]:
    players = box_team.get("players", {})
    for pitcher_id in box_team.get("pitchers", []):
        player = players.get(f"ID{pitcher_id}", {})
        pitching = player.get("stats", {}).get("pitching", {})
        if int(pitching.get("gamesStarted") or 0) == 1:
            return pitcher_id, player
    if box_team.get("pitchers"):
        pitcher_id = box_team["pitchers"][0]
        return pitcher_id, players.get(f"ID{pitcher_id}", {})
    return None, {}


def _home_plate_umpire(boxscore: dict[str, Any]) -> str:
    for official in boxscore.get("officials", []):
        if official.get("officialType") == "Home Plate":
            return official.get("official", {}).get("fullName", "")
    return ""


def _pitch_hand(game_data: dict[str, Any], pitcher_id: int | None) -> str:
    if not pitcher_id:
        return ""
    player = game_data.get("players", {}).get(f"ID{pitcher_id}", {})
    return player.get("pitchHand", {}).get("code", "")


def _row_from_game(
    session: requests.Session,
    game: dict[str, Any],
) -> list[Any]:
    game_pk = int(game["gamePk"])
    feed = _get_json(session, f"{MLB_API}/v1.1/game/{game_pk}/feed/live")
    return row_from_feed(feed, game)


def row_from_feed(feed: dict[str, Any], game: dict[str, Any]) -> list[Any]:
    """Build one A:AO 紀錄 row from an already-fetched live feed.

    Split out so 近十場 can build the same rows from the same feed it already
    pulls for batting stats, instead of reading them back out of 紀錄.
    """
    game_pk = int(game["gamePk"])
    game_data = feed["gameData"]
    live_data = feed["liveData"]
    boxscore = live_data["boxscore"]
    linescore = live_data["linescore"]

    away_team = game["teams"]["away"]["team"]
    home_team = game["teams"]["home"]["team"]
    away_box = boxscore["teams"]["away"]
    home_box = boxscore["teams"]["home"]
    away_starter_id, away_starter = _starter(away_box)
    home_starter_id, home_starter = _starter(home_box)

    away_pitching = away_starter.get("stats", {}).get("pitching", {})
    home_pitching = home_starter.get("stats", {}).get("pitching", {})
    away_line = linescore["teams"]["away"]
    home_line = linescore["teams"]["home"]

    official_date = datetime.strptime(game["officialDate"], "%Y-%m-%d").date()
    venue = game_data.get("venue", {})
    row: list[Any] = [
        f"{official_date.year}/{official_date.month}/{official_date.day}",
        game_pk,
        canonical_team_code(away_team.get("abbreviation", "")),
        away_starter.get("person", {}).get("fullName", ""),
        _pitch_hand(game_data, away_starter_id),
        *_inning_runs(linescore, "away"),
        away_line.get("runs", ""),
        away_line.get("hits", ""),
        away_line.get("errors", ""),
        canonical_team_code(home_team.get("abbreviation", "")),
        *_inning_runs(linescore, "home"),
        home_line.get("runs", ""),
        home_line.get("hits", ""),
        home_line.get("errors", ""),
        home_starter.get("person", {}).get("fullName", ""),
        _pitch_hand(game_data, home_starter_id),
        venue.get("name", game.get("venue", {}).get("name", "")),
        venue.get("id", game.get("venue", {}).get("id", "")),
        away_pitching.get("inningsPitched", ""),
        home_pitching.get("inningsPitched", ""),
        _home_plate_umpire(boxscore),
        _division_abbrev(away_team),
        _division_abbrev(home_team),
        away_pitching.get("earnedRuns", ""),
        home_pitching.get("earnedRuns", ""),
    ]
    if len(row) != RAW_COLUMN_COUNT:
        raise RuntimeError(
            f"Expected {RAW_COLUMN_COUNT} raw columns for game {game_pk}, got {len(row)}"
        )
    return row


def _final_regular_games(
    session: requests.Session, start_date: str, end_date: str
) -> list[dict[str, Any]]:
    data = _get_json(
        session,
        f"{MLB_API}/v1/schedule",
        sportId=1,
        gameType="R",
        startDate=start_date,
        endDate=end_date,
        hydrate="team,venue",
    )
    games: list[dict[str, Any]] = []
    for day in data.get("dates", []):
        for game in day.get("games", []):
            status = game.get("status", {})
            if status.get("statusCode") != "F" or status.get("detailedState") != "Final":
                continue
            if game.get("gameType") != "R":
                continue
            games.append(game)
    return sorted(games, key=lambda g: (g["officialDate"], int(g["gamePk"])))


def placeholder_formulas_ready(
    formula_cells: list[list[Any]], start_row: int, end_row: int
) -> bool:
    """True when every row in start_row..end_row already carries a formula.

    紀錄 keeps pre-built rows whose AP:BD formulas are already in place, the
    way NPB's 賽錄 keeps numbered placeholders. Pasting formulas over rows
    that already have them buys nothing and costs a full recalculation of the
    workbook -- around eight minutes at this size -- because every later call
    queues behind it.
    """
    needed = end_row - start_row + 1
    if len(formula_cells) < needed:
        return False
    return all(
        str(row[0] if row else "").startswith("=")
        for row in formula_cells[:needed]
    )


DATE_COL_0IDX = 0
# Sheets counts days from 1899-12-30, so an UNFORMATTED_VALUE read of a date
# cell comes back as 46196 rather than 2026/6/23.
SHEETS_EPOCH = date(1899, 12, 30)


def _as_serial(value: Any) -> float | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return float((datetime.strptime(text, "%Y/%m/%d").date() - SHEETS_EPOCH).days)
    except ValueError:
        return None


def _same_cell(api_value: Any, sheet_value: Any, column: int) -> bool:
    if column == DATE_COL_0IDX:
        api_serial, sheet_serial = _as_serial(api_value), _as_serial(sheet_value)
        if api_serial is not None and sheet_serial is not None:
            return api_serial == sheet_serial
    try:
        return float(str(api_value).strip()) == float(str(sheet_value).strip())
    except ValueError:
        return str(api_value).strip() == str(sheet_value).strip()


def revised_cells(api_row: list[Any], sheet_row: list[Any]) -> dict[int, Any]:
    """Columns where the API now disagrees with what 紀錄 holds.

    A game already in 紀錄 is never looked at again, so an official scorer's
    later ruling on a hit, an error or an earned run never reaches the sheet.
    Measured over sixty days: 94 stale values across 756 games, every one of
    them in those fields and none anywhere else.

    A blank from the API is missing data rather than a correction, so it never
    clears a cell that already holds something.
    """
    padded = list(sheet_row) + [""] * (RAW_COLUMN_COUNT - len(sheet_row))
    revised: dict[int, Any] = {}
    for column in range(RAW_COLUMN_COUNT):
        api_value = api_row[column]
        if str(api_value).strip() == "":
            continue
        if not _same_cell(api_value, padded[column], column):
            revised[column] = api_value
    return revised


def _last_dated_row(date_values: list[str]) -> int:
    for index in range(len(date_values), 0, -1):
        if str(date_values[index - 1]).strip():
            return index
    return 1


def _protected_range_blocks_edit(
    worksheet: Any, start_row: int, end_row: int, start_col: int, end_col: int
) -> bool:
    metadata = _with_retries(
        "fetch sheet metadata", lambda: worksheet.spreadsheet.fetch_sheet_metadata()
    )
    sheet = next(
        item
        for item in metadata.get("sheets", [])
        if item.get("properties", {}).get("sheetId") == worksheet.id
    )
    for protected in sheet.get("protectedRanges", []):
        protected_range = protected.get("range", {})
        if protected_range.get("sheetId") != worksheet.id:
            continue
        protected_start_row = protected_range.get("startRowIndex", 0)
        protected_end_row = protected_range.get("endRowIndex", worksheet.row_count)
        protected_start_col = protected_range.get("startColumnIndex", 0)
        protected_end_col = protected_range.get("endColumnIndex", worksheet.col_count)
        rows_overlap = start_row < protected_end_row and end_row > protected_start_row
        cols_overlap = start_col < protected_end_col and end_col > protected_start_col
        if rows_overlap and cols_overlap and not protected.get("warningOnly"):
            return True
    return False


def _cache_path(start_date: str, end_date: str, count: int) -> Path:
    safe_start = start_date.replace("-", "")
    safe_end = end_date.replace("-", "")
    return CACHE_DIR / f"mlb_record_rows_{safe_start}_{safe_end}_{count}.json"


def _pending_revisions(
    session: requests.Session,
    worksheet: Any,
    games: list[dict[str, Any]],
    row_by_game_pk: dict[str, int],
) -> dict[int, dict[int, Any]]:
    """Cells in games already written that the API has since corrected.

    The feeds are fetched, and the sheet rows read, before anything is
    written, so this costs no recalculation wait. What it finds is merged
    into the one write the run makes.
    """
    rows_wanted = sorted(
        row_by_game_pk[str(game["gamePk"])]
        for game in games
        if str(game["gamePk"]) in row_by_game_pk
    )
    if not rows_wanted:
        return {}

    first, last = rows_wanted[0], rows_wanted[-1]
    sheet_rows = _with_retries(
        f"read rows {first}:{last} to check for revisions",
        lambda: worksheet.get(
            f"A{first}:{RAW_END_COLUMN}{last}",
            value_render_option="UNFORMATTED_VALUE",
        ),
    )

    feeds = _feeds_for_games(session, games)
    revisions: dict[int, dict[int, Any]] = {}
    for game in games:
        game_pk = str(game["gamePk"])
        row = row_by_game_pk.get(game_pk)
        feed = feeds.get(game_pk)
        if row is None or feed is None:
            continue
        offset = row - first
        sheet_row = sheet_rows[offset] if offset < len(sheet_rows) else []
        changed = revised_cells(row_from_feed(feed, game), sheet_row)
        if changed:
            revisions[row] = changed

    if revisions:
        cells = sum(len(columns) for columns in revisions.values())
        print(
            f"{cells} cell(s) revised by the API across {len(revisions)} game(s).",
            flush=True,
        )
    return revisions


def _feeds_for_games(
    session: requests.Session, games: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Fetch each game's live feed once, a few at a time."""
    game_pks = sorted({str(game["gamePk"]) for game in games}, key=int)
    feeds: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {
            pool.submit(
                _get_json, session, f"{MLB_API}/v1.1/game/{game_pk}/feed/live"
            ): game_pk
            for game_pk in game_pks
        }
        for future in as_completed(futures):
            feeds[futures[future]] = future.result()
    return feeds


def update_record_sheet(start_date: str, end_date: str, dry_run: bool = False) -> int:
    session = requests.Session()
    worksheet = _sheets_client.worksheet(SPREADSHEET_KEY, WORKSHEET_NAME)
    record_keys = _with_retries("read record keys", lambda: worksheet.get("A:B"))
    date_values = [row[0] if row else "" for row in record_keys]
    game_pk_values = [row[1] if len(row) > 1 else "" for row in record_keys]
    last_row = _last_dated_row(date_values)
    existing_game_pks = {
        str(value).strip() for value in game_pk_values[1:] if str(value).strip()
    }

    row_by_game_pk = {
        str(value).strip(): index
        for index, value in enumerate(game_pk_values[1:], start=2)
        if str(value).strip()
    }

    schedule = _final_regular_games(session, start_date, end_date)
    games = [
        game for game in schedule if str(game["gamePk"]) not in existing_game_pks
    ]
    already_written = [
        game for game in schedule if str(game["gamePk"]) in existing_game_pks
    ]
    print(f"Found {len(games)} missing final regular-season game(s).", flush=True)

    revisions = _pending_revisions(session, worksheet, already_written, row_by_game_pk)

    if dry_run or (not games and not revisions):
        for game in games[:10]:
            print(game["officialDate"], game["gamePk"])
        if len(games) > 10:
            print(f"... {len(games) - 10} more")
        return len(games)

    spreadsheet = worksheet.spreadsheet
    data: list[dict[str, Any]] = []
    rows: list[list[Any]] = []

    if games:
        start_row = last_row + 1
        end_row = start_row + len(games) - 1
        if _protected_range_blocks_edit(
            worksheet, start_row - 1, end_row, 0, FORMULA_END_COL_0IDX
        ):
            raise RuntimeError(
                f"{WORKSHEET_NAME} has a protected range covering rows "
                f"{start_row}:{end_row}. Grant edit access to the service account "
                "or remove/unprotect that range before running this backfill."
            )

        cache_path = _cache_path(start_date, end_date, len(games))
        if cache_path.exists():
            rows = json.loads(cache_path.read_text())
            if any(len(row) != RAW_COLUMN_COUNT for row in rows):
                print(
                    f"Ignoring stale cache with old raw column count: {cache_path}",
                    flush=True,
                )
                rows = []
            else:
                print(f"Loaded {len(rows)} cached row(s) from {cache_path}", flush=True)
        if not rows:
            for index, game in enumerate(games, start=1):
                rows.append(_row_from_game(session, game))
                if index % 100 == 0 or index == len(games):
                    print(f"Built {index}/{len(games)} rows", flush=True)
                time.sleep(0.05)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(rows, ensure_ascii=False))
            print(f"Cached {len(rows)} row(s) to {cache_path}", flush=True)

        if worksheet.row_count < end_row:
            rows_to_add = end_row - worksheet.row_count
            _with_retries("add_rows", lambda: worksheet.add_rows(rows_to_add))
            print(f"Added {rows_to_add} worksheet row(s).", flush=True)
            formulas_ready = False
        else:
            formula_range = (
                f"'{WORKSHEET_NAME}'!{FORMULA_FIRST_COLUMN}{start_row}"
                f":{FORMULA_FIRST_COLUMN}{end_row}"
            )
            cells = _with_retries(
                "read placeholder formulas",
                lambda: spreadsheet.values_batch_get(
                    [formula_range], params={"valueRenderOption": "FORMULA"}
                )["valueRanges"][0].get("values", []),
            )
            formulas_ready = placeholder_formulas_ready(cells, start_row, end_row)

        if formulas_ready:
            print(
                f"Rows {start_row}:{end_row} already carry their formulas; "
                "writing values only.",
                flush=True,
            )
        else:
            _with_retries(
                "copy formulas",
                lambda: spreadsheet.batch_update(
                    {
                        "requests": [
                            {
                                "copyPaste": {
                                    "source": {
                                        "sheetId": worksheet.id,
                                        "startRowIndex": last_row - 1,
                                        "endRowIndex": last_row,
                                        "startColumnIndex": FORMULA_START_COL_0IDX,
                                        "endColumnIndex": FORMULA_END_COL_0IDX,
                                    },
                                    "destination": {
                                        "sheetId": worksheet.id,
                                        "startRowIndex": start_row - 1,
                                        "endRowIndex": end_row,
                                        "startColumnIndex": FORMULA_START_COL_0IDX,
                                        "endColumnIndex": FORMULA_END_COL_0IDX,
                                    },
                                    "pasteType": "PASTE_FORMULA",
                                }
                            }
                        ]
                    }
                ),
            )

        for offset in range(0, len(rows), 500):
            chunk = rows[offset : offset + 500]
            chunk_start = start_row + offset
            data.append(
                {
                    "range": (
                        f"'{WORKSHEET_NAME}'!A{chunk_start}"
                        f":{RAW_END_COLUMN}{chunk_start + len(chunk) - 1}"
                    ),
                    "values": chunk,
                }
            )

    for row in sorted(revisions):
        for column, value in sorted(revisions[row].items()):
            letter = rowcol_to_a1(1, column + 1).rstrip("1")
            data.append(
                {
                    "range": f"'{WORKSHEET_NAME}'!{letter}{row}",
                    "values": [[value]],
                }
            )

    # One request, not one per chunk: each separate write starts its own
    # recalculation, and the next call waits the whole of it out.
    _with_retries(
        f"write {len(data)} range(s)",
        lambda: spreadsheet.values_batch_update(
            {"valueInputOption": "USER_ENTERED", "data": data}
        ),
    )
    print(
        f"Wrote {len(rows)} new row(s) and "
        f"{sum(len(c) for c in revisions.values())} revised cell(s).",
        flush=True,
    )


    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument(
        "--recent-days",
        type=int,
        help=(
            "Update the last N calendar dates instead of passing explicit dates. "
            "Useful for daily runs; gamePk dedupe prevents duplicate rows."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.recent_days:
        end = date.today()
        start = end - timedelta(days=args.recent_days - 1)
        args.start_date = start.isoformat()
        args.end_date = end.isoformat()
    count = update_record_sheet(args.start_date, args.end_date, dry_run=args.dry_run)
    print(f"Done. {'Would write' if args.dry_run else 'Wrote'} {count} row(s).")


if __name__ == "__main__":
    main()
