"""Build MLB 近十場 sheets from the MLB 紀錄 worksheet."""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from gspread.exceptions import APIError, WorksheetNotFound

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from baseball.mlb_teams import canonical_team_code
from cpbl import _sheets_client

SPREADSHEET_KEY = "11FV70TXVAxLTwYH6pLj7HwK1qq-fIa61QrePRCC8YUM"
RECORD_SHEET_NAME = "紀錄"
SHEET_PREFIX = "MLB近十場"
MLB_API = "https://statsapi.mlb.com/api"
REQUEST_TIMEOUT = (10, 45)

GAMES_COUNT = 10
BLOCK_COLS = [2, 17, 32]
TOP_HEADER_ROW = 3
TOP_GAME_START = 4
TOP_AVG5_ROW = 15
BOTTOM_HEADER_ROW = 16
BOTTOM_GAME_START = 17
BOTTOM_AVG5_ROW = 28

HOME_RUN_LOOKBACK_GAMES = 6
HOME_RUN_EVENT_ROWS = 20
TOP_HR_HEADER_ROW = 30
TOP_HR_END_ROW = TOP_HR_HEADER_ROW + HOME_RUN_EVENT_ROWS
BOTTOM_HR_HEADER_ROW = TOP_HR_END_ROW + 2
BOTTOM_HR_END_ROW = BOTTOM_HR_HEADER_ROW + HOME_RUN_EVENT_ROWS

DEFAULT_FONT = "#202124"
SCORE_WIN_FONT = "#d93025"
SCORE_LOSS_FONT = "#188038"
SCORE_TIE_FONT = "#5f6368"
HITS_10_PLUS_FONT = "#d93025"
OPPOSITE_FIELD_FONT = "#ff0000"
BAT_SIDE_R_FONT = "#1155cc"
BAT_SIDE_L_FONT = "#cc0000"
BAT_SIDE_S_FONT = "#bf9000"
HOT_RATE_FONT = "#ff0000"
COLD_RATE_FONT = "#38761d"
HOT_AVG_THRESHOLD = 0.280
COLD_AVG_THRESHOLD = 0.200
HOT_OBP_THRESHOLD = 0.330
COLD_OBP_THRESHOLD = 0.250

DIRECTION_PATTERNS = [
    (re.compile(r"left[- ]center field", re.IGNORECASE), "左中本"),
    (re.compile(r"right[- ]center field", re.IGNORECASE), "右中本"),
    (re.compile(r"left field", re.IGNORECASE), "左本"),
    (re.compile(r"right field", re.IGNORECASE), "右本"),
    (re.compile(r"center field", re.IGNORECASE), "中本"),
]

TEAM_ORDER = [
    "NYY",
    "BOS",
    "TOR",
    "TB",
    "BAL",
    "CLE",
    "DET",
    "KC",
    "MIN",
    "CWS",
    "HOU",
    "TEX",
    "SEA",
    "LAA",
    "OAK",
    "ATL",
    "NYM",
    "PHI",
    "MIA",
    "WSH",
    "CHC",
    "MIL",
    "STL",
    "CIN",
    "PIT",
    "LAD",
    "SD",
    "SF",
    "AZ",
    "COL",
]

TEAM_COLORS = {
    "NYY": ("#132448", "#ffffff"),
    "BOS": ("#bd3039", "#ffffff"),
    "TOR": ("#134a8e", "#ffffff"),
    "TB": ("#092c5c", "#ffffff"),
    "BAL": ("#df4601", "#000000"),
    "CLE": ("#00385d", "#ffffff"),
    "DET": ("#0c2340", "#ffffff"),
    "KC": ("#004687", "#ffffff"),
    "MIN": ("#002b5c", "#ffffff"),
    "CWS": ("#27251f", "#ffffff"),
    "HOU": ("#eb6e1f", "#002d62"),
    "TEX": ("#003278", "#ffffff"),
    "SEA": ("#005c5c", "#ffffff"),
    "LAA": ("#ba0021", "#ffffff"),
    # 紀錄 stores the Athletics as OAK for every season, including 2025 onwards when
    # the API switched to ATH — see canonical_team_code() in baseball/mlb_teams.py.
    "OAK": ("#003831", "#efb21e"),
    "ATL": ("#13274f", "#ffffff"),
    "NYM": ("#ff5910", "#002d72"),
    "PHI": ("#e81828", "#ffffff"),
    "MIA": ("#00a3e0", "#000000"),
    "WSH": ("#ab0003", "#ffffff"),
    "CHC": ("#0e3386", "#ffffff"),
    "MIL": ("#12284b", "#ffc52f"),
    "STL": ("#c41e3a", "#ffffff"),
    "CIN": ("#c6011f", "#ffffff"),
    "PIT": ("#fdb827", "#27251f"),
    "LAD": ("#005a9c", "#ffffff"),
    "SD": ("#2f241d", "#ffc425"),
    "SF": ("#fd5a1e", "#27251f"),
    "AZ": ("#a71930", "#ffffff"),
    "COL": ("#33006f", "#ffffff"),
}

MLB_VENUE_ID_DISPLAY_NAMES = {
    1: "Angels",
    2: "Orioles",
    3: "Red Sox",
    4: "White Sox",
    5: "Guardians",
    7: "Royals",
    12: "Rays",
    14: "Blue Jays",
    15: "D-backs",
    17: "Cubs",
    19: "Rockies",
    22: "Dodgers",
    31: "Pirates",
    32: "Brewers",
    680: "Mariners",
    2392: "Astros",
    2394: "Tigers",
    2395: "Giants",
    2529: "Athletics",
    2602: "Reds",
    2680: "Padres",
    2681: "Phillies",
    2889: "Cardinals",
    3289: "Mets",
    3309: "Nationals",
    3312: "Twins",
    3313: "Yankees",
    4169: "Marlins",
    4705: "Braves",
    5325: "Rangers",
}

MLB_VENUE_NAME_DISPLAY_NAMES = {
    "Oriole Park at Camden Yards": "Orioles",
    "UNIQLO Field at Dodger Stadium": "Dodgers",
}


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
            print(f"{label} failed ({attempt}/5), retrying in {sleep_for}s", flush=True)
            time.sleep(sleep_for)
    raise RuntimeError(f"{label} failed after retries") from last_error


def _get_json(session: requests.Session, url: str, **params: Any) -> dict[str, Any]:
    return _with_retries(
        f"fetch {url}",
        lambda: _raise_for_json(
            session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        ),
    )


def _raise_for_json(response: requests.Response) -> dict[str, Any]:
    response.raise_for_status()
    return response.json()


def _col_to_letter(col: int) -> str:
    result = ""
    while col > 0:
        col, rem = divmod(col - 1, 26)
        result = chr(65 + rem) + result
    return result


def _hex_to_rgb(hex_color: str) -> dict[str, float]:
    h = hex_color.lstrip("#")
    return {
        "red": int(h[0:2], 16) / 255,
        "green": int(h[2:4], 16) / 255,
        "blue": int(h[4:6], 16) / 255,
    }


def _to_int(value: Any) -> int:
    try:
        if value in ("", None):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        if value in ("", None):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _rate_text(value: float | None) -> str:
    if value is None:
        return ""
    text = f"{value:.3f}"
    return text[1:] if text.startswith("0.") else text


def _batting_average(game: dict[str, Any]) -> float | None:
    at_bats = _to_float(game.get("打數"))
    hits = _to_float(game.get("安打"))
    if not at_bats:
        return None
    return hits / at_bats


def _on_base_percentage(game: dict[str, Any]) -> float | None:
    hits = _to_float(game.get("安打"))
    on_base = _to_float(game.get("四死"))
    at_bats = _to_float(game.get("打數"))
    sac_flies = _to_float(game.get("犧飛"))
    denominator = at_bats + on_base + sac_flies
    if not denominator:
        return None
    return (hits + on_base) / denominator


def _aggregate_batting_average(games: list[dict[str, Any]]) -> float | None:
    at_bats = sum(_to_float(g.get("打數")) for g in games)
    hits = sum(_to_float(g.get("安打")) for g in games)
    if not at_bats:
        return None
    return hits / at_bats


def _aggregate_on_base_percentage(games: list[dict[str, Any]]) -> float | None:
    hits = sum(_to_float(g.get("安打")) for g in games)
    on_base = sum(_to_float(g.get("四死")) for g in games)
    at_bats = sum(_to_float(g.get("打數")) for g in games)
    sac_flies = sum(_to_float(g.get("犧飛")) for g in games)
    denominator = at_bats + on_base + sac_flies
    if not denominator:
        return None
    return (hits + on_base) / denominator


def _direction_from_description(description: str) -> str:
    for pattern, label in DIRECTION_PATTERNS:
        if pattern.search(description or ""):
            return label
    return ""


def _display_venue_name(venue: str, venue_id: Any = None) -> str:
    venue_id_int = _to_int(venue_id)
    if venue_id_int in MLB_VENUE_ID_DISPLAY_NAMES:
        return MLB_VENUE_ID_DISPLAY_NAMES[venue_id_int]
    return MLB_VENUE_NAME_DISPLAY_NAMES.get(venue, venue)


def _to_date_text(value: Any) -> str:
    if isinstance(value, (int, float)):
        dt = date(1899, 12, 30) + timedelta(days=int(value))
        return f"{dt.year}/{dt.month}/{dt.day}"
    text = _to_text(value)
    if "-" in text:
        dt = datetime.strptime(text, "%Y-%m-%d").date()
        return f"{dt.year}/{dt.month}/{dt.day}"
    return text


def _display_date(value: str) -> str:
    dt = datetime.strptime(value, "%Y/%m/%d")
    return f"{dt.month}/{dt.day}"


def _record_to_team_games(row: list[str]) -> list[tuple[str, dict[str, Any]]]:
    padded = row + [""] * (41 - len(row))
    game_id = _to_text(padded[1])
    if not game_id:
        return []
    away = _to_text(padded[2])
    home = _to_text(padded[17])
    if not away or not home:
        return []

    date_value = _to_date_text(padded[0])
    venue = _display_venue_name(_to_text(padded[32]), padded[33])
    away_game = {
        "日期": date_value,
        "賽事編號": game_id,
        "隊伍": away,
        "對戰球隊": home,
        "對戰先發": _to_text(padded[30]),
        "球場": venue,
        "実分": _to_int(padded[40]),
        "得分": _to_int(padded[14]),
        "失分": _to_int(padded[27]),
        "実失": _to_int(padded[39]),
        "安打": _to_int(padded[15]),
        "主客": "客",
    }
    home_game = {
        "日期": date_value,
        "賽事編號": game_id,
        "隊伍": home,
        "對戰球隊": away,
        "對戰先發": _to_text(padded[3]),
        "球場": venue,
        "実分": _to_int(padded[39]),
        "得分": _to_int(padded[27]),
        "失分": _to_int(padded[14]),
        "実失": _to_int(padded[40]),
        "安打": _to_int(padded[28]),
        "主客": "主",
    }
    return [(away, away_game), (home, home_game)]


def _read_team_games() -> dict[str, list[dict[str, Any]]]:
    started = time.time()
    worksheet = _sheets_client.worksheet(SPREADSHEET_KEY, RECORD_SHEET_NAME)
    rows = _with_retries(
        "read record raw columns",
        lambda: worksheet.get("A2:AO", value_render_option="UNFORMATTED_VALUE"),
    )
    print(
        f"Read {len(rows)} {RECORD_SHEET_NAME} row(s) in {time.time() - started:.1f}s",
        flush=True,
    )
    games_by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for team, game in _record_to_team_games(row):
            games_by_team[team].append(game)
    for team in games_by_team:
        games_by_team[team].sort(
            key=lambda g: (
                datetime.strptime(g["日期"], "%Y/%m/%d"),
                int(g["賽事編號"]),
            )
        )
        games_by_team[team] = games_by_team[team][-GAMES_COUNT:]
    return games_by_team


def _home_run_events_by_team(
    feed: dict[str, Any], game_data: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    abbrev_by_side = {
        side: canonical_team_code(game_data["teams"][side].get("abbreviation", ""))
        for side in ("away", "home")
    }
    events: dict[str, list[dict[str, Any]]] = {
        abbrev_by_side["away"]: [],
        abbrev_by_side["home"]: [],
    }
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in plays:
        if play.get("result", {}).get("eventType") != "home_run":
            continue
        batting_side = "away" if play.get("about", {}).get("isTopInning") else "home"
        batter_abbrev = abbrev_by_side[batting_side]
        matchup = play.get("matchup", {})
        events[batter_abbrev].append(
            {
                "打者": matchup.get("batter", {}).get("fullName", ""),
                "左右打": matchup.get("batSide", {}).get("code", ""),
                "方向": _direction_from_description(
                    play.get("result", {}).get("description", "")
                ),
                "投手": matchup.get("pitcher", {}).get("fullName", ""),
            }
        )
    return events


def _extra_base_hits(batting: dict[str, Any]) -> int:
    """Doubles + triples + home runs, which is what NPB's 長打 column counts."""
    return sum(_to_int(batting.get(key))
               for key in ("doubles", "triples", "homeRuns"))


def _team_stats_from_feed(
    session: requests.Session, game_id: str
) -> dict[str, Any]:
    feed = _get_json(session, f"{MLB_API}/v1.1/game/{game_id}/feed/live")
    game_data = feed["gameData"]
    boxscore = feed["liveData"]["boxscore"]
    team_stats: dict[str, dict[str, int]] = {}
    for side in ("away", "home"):
        team = boxscore["teams"][side]
        abbrev = canonical_team_code(
            game_data["teams"][side].get("abbreviation", "")
        )
        batting = team.get("teamStats", {}).get("batting", {})
        team_stats[abbrev] = {
            "三振": _to_int(batting.get("strikeOuts")),
            "四死": _to_int(batting.get("baseOnBalls"))
            + _to_int(batting.get("hitByPitch")),
            "長打": _extra_base_hits(batting),
            "打數": _to_int(batting.get("atBats")),
            "犧飛": _to_int(batting.get("sacFlies")),
        }
    venue = game_data.get("venue", {})
    return {
        "team_stats": team_stats,
        "venue_id": venue.get("id"),
        "venue_name": venue.get("name", ""),
        "hr_events": _home_run_events_by_team(feed, game_data),
    }


def _enrich_batting_stats(games_by_team: dict[str, list[dict[str, Any]]]) -> None:
    session = requests.Session()
    game_ids = sorted(
        {g["賽事編號"] for games in games_by_team.values() for g in games}
    )
    game_cache: dict[str, dict[str, Any]] = {}
    started = time.time()
    for index, game_id in enumerate(game_ids, start=1):
        game_cache[game_id] = _team_stats_from_feed(session, game_id)
        if index == 1 or index % 25 == 0 or index == len(game_ids):
            print(
                f"Fetched batting stats {index}/{len(game_ids)} "
                f"({time.time() - started:.1f}s)",
                flush=True,
            )
        time.sleep(0.03)
    for games in games_by_team.values():
        for game in games:
            cached = game_cache.get(game["賽事編號"], {})
            game.update(cached.get("team_stats", {}).get(game["隊伍"], {}))
            game["全壘打明細"] = cached.get("hr_events", {}).get(game["隊伍"], [])
            venue = _display_venue_name(
                _to_text(cached.get("venue_name")),
                cached.get("venue_id"),
            )
            if venue:
                game["球場"] = venue


def _avg_row(label: str, games: list[dict[str, Any]]) -> list[Any]:
    if not games:
        return ["", "", label, "平 均"] + [""] * 10
    n = len(games)

    def avg(key: str) -> float:
        return round(sum(_to_float(g.get(key)) for g in games) / n, 1)

    return [
        "",
        "",
        label,
        "平 均",
        avg("実分"),
        avg("得分"),
        avg("失分"),
        avg("実失"),
        avg("安打"),
        avg("三振"),
        avg("四死"),
        avg("長打"),
        _rate_text(_aggregate_batting_average(games)),
        _rate_text(_aggregate_on_base_percentage(games)),
    ]


def _build_block_values(team: str, games: list[dict[str, Any]]) -> list[list[Any]]:
    rows: list[list[Any]] = [
        [
            team,
            "球 隊",
            "對 戰",
            "球 場",
            "実 点",
            "得 点",
            "失 点",
            "実 失",
            "安 打",
            "三 振",
            "四 死",
            "長 打",
            "打 率",
            "上 率",
        ]
    ]
    sorted_games = games[-GAMES_COUNT:]
    for index in range(GAMES_COUNT):
        if index >= len(sorted_games):
            rows.append([""] * 14)
            continue
        game = sorted_games[index]
        rows.append(
            [
                _display_date(game["日期"]),
                game.get("對戰球隊", ""),
                game.get("對戰先發", ""),
                game.get("球場", ""),
                game.get("実分", 0),
                game.get("得分", 0),
                game.get("失分", 0),
                game.get("実失", 0),
                game.get("安打", 0),
                game.get("三振", ""),
                game.get("四死", ""),
                game.get("長打", ""),
                _rate_text(_batting_average(game)),
                _rate_text(_on_base_percentage(game)),
            ]
        )
    rows.append(_avg_row("近十場", sorted_games))
    rows.append(_avg_row("近五場", sorted_games[-5:]))
    return rows


def _next_matchups() -> list[tuple[str, str]]:
    session = requests.Session()
    today = date.today()
    for offset in range(0, 8):
        target = today + timedelta(days=offset)
        data = _get_json(
            session,
            f"{MLB_API}/v1/schedule",
            sportId=1,
            gameType="R",
            startDate=target.isoformat(),
            endDate=target.isoformat(),
            hydrate="team",
        )
        matchups = _matchups_from_schedule(data)
        if matchups:
            return matchups
    return []


def _matchups_from_schedule(data: dict[str, Any]) -> list[tuple[str, str]]:
    """Today's regular-season pairings, in first-pitch order.

    Codes go through canonical_team_code() because the schedule feed and 紀錄 can
    disagree — the API says ATH where 紀錄 stores OAK, and a block headed with a
    code 紀錄 does not use finds no games to list under it.
    """
    games = [
        game
        for day in data.get("dates", [])
        for game in day.get("games", [])
        if game.get("gameType") == "R"
    ]
    games.sort(key=lambda g: g.get("gameDate", ""))
    matchups = [
        (
            canonical_team_code(game["teams"]["away"]["team"].get("abbreviation", "")),
            canonical_team_code(game["teams"]["home"]["team"].get("abbreviation", "")),
        )
        for game in games
    ]
    return [(a, h) for a, h in matchups if a and h]


def _fallback_matchups() -> list[tuple[str, str]]:
    return [
        (TEAM_ORDER[index], TEAM_ORDER[index + 1])
        for index in range(0, len(TEAM_ORDER), 2)
    ]


def _ensure_worksheet(title: str):
    spreadsheet = _sheets_client.spreadsheet(SPREADSHEET_KEY)
    needed_rows = BOTTOM_HR_END_ROW
    needed_cols = max(BLOCK_COLS) + 13
    try:
        worksheet = spreadsheet.worksheet(title)
    except WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=title, rows=needed_rows, cols=needed_cols
        )
    if worksheet.row_count < needed_rows or worksheet.col_count < needed_cols:
        worksheet.resize(
            rows=max(worksheet.row_count, needed_rows),
            cols=max(worksheet.col_count, needed_cols),
        )
    return worksheet


def _number_format_clear_request(
    sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int
) -> dict:
    """Clear any lingering number format (e.g. DATE) on cells whose column now
    holds a different field than it did under an earlier, narrower layout."""
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row - 1,
                "endRowIndex": end_row,
                "startColumnIndex": start_col - 1,
                "endColumnIndex": end_col,
            },
            "cell": {},
            "fields": "userEnteredFormat.numberFormat",
        }
    }


def _hide_gridlines_request(sheet_id: int) -> dict:
    """Match NPB's 近十場, which reads as free-standing blocks rather than a grid."""
    return {
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"hideGridlines": True},
            },
            "fields": "gridProperties.hideGridlines",
        }
    }


def _hide_top_rows_request(sheet_id: int) -> dict:
    """Rows 1-2 hold nothing — NPB squeezes them to 2px, we take them out of view."""
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": 0,
                "endIndex": HIDDEN_TOP_ROWS,
            },
            "properties": {"hiddenByUser": True},
            "fields": "hiddenByUser",
        }
    }


def _base_font_request(
    sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int
) -> dict:
    """One typeface for the whole area, as NPB's 近十場 uses.

    Issued before anything else touches the text so the few cells that want a
    smaller size — a long pitcher name, the home-run 打位 / 方向 — still win.
    """
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row - 1,
                "endRowIndex": end_row,
                "startColumnIndex": start_col - 1,
                "endColumnIndex": end_col,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "fontFamily": BLOCK_FONT,
                        "fontSize": BLOCK_FONT_SIZE,
                        "bold": True,
                    }
                }
            },
            "fields": (
                "userEnteredFormat.textFormat.fontFamily,"
                "userEnteredFormat.textFormat.fontSize,"
                "userEnteredFormat.textFormat.bold"
            ),
        }
    }


def _score_bracket_requests(
    sheet_id: int, game_start_row: int, col_start: int
) -> list[dict]:
    """Dashed rules either side of 得点 / 失点, as NPB's 近十場a draws them.

    Only the ten game rows are bracketed — the 近十場 / 近五場 averages below them
    are left open, which is what makes the pair read as a running tally.
    """
    dashed = {"style": "DASHED", "width": 1, "color": _hex_to_rgb("#000000")}
    none = {"style": "NONE"}
    area = {
        "sheetId": sheet_id,
        "startRowIndex": game_start_row - 1,
        "endRowIndex": game_start_row - 1 + GAMES_COUNT,
        "startColumnIndex": col_start + 4,
        "endColumnIndex": col_start + 6,
    }
    return [
        # clear first: a bracket drawn under an earlier column layout would
        # otherwise stay put next to whatever now sits there
        {"updateBorders": {
            "range": {**area, "startColumnIndex": col_start - 1,
                      "endColumnIndex": col_start + 13},
            "top": none, "bottom": none, "left": none, "right": none,
            "innerHorizontal": none, "innerVertical": none,
        }},
        {"updateBorders": {"range": area, "left": dashed, "right": dashed}},
    ]


def _appearance_reset_request(
    sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int
) -> dict:
    """Drop every fill and font colour across the block area before repainting.

    The colour requests below only ever set the cells they care about, so anything
    left over from an earlier layout stays put: after the blocks moved from
    B/O/AB to B/Q/AF, gap column AE kept an old header fill and the 對戰 column
    inherited the win/loss colours of the column that used to sit there.
    """
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row - 1,
                "endRowIndex": end_row,
                "startColumnIndex": start_col - 1,
                "endColumnIndex": end_col,
            },
            "cell": {},
            "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)",
        }
    }


def _font_color_request(
    sheet_id: int, row_0idx: int, col_0idx: int, hex_color: str
) -> dict:
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row_0idx,
                "endRowIndex": row_0idx + 1,
                "startColumnIndex": col_0idx,
                "endColumnIndex": col_0idx + 1,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"foregroundColor": _hex_to_rgb(hex_color)}
                }
            },
            "fields": "userEnteredFormat.textFormat.foregroundColor",
        }
    }


def _header_format_request(
    sheet_id: int, team: str, header_row: int, col_start: int
) -> dict:
    fill, font = TEAM_COLORS.get(team, ("#3c4043", "#ffffff"))
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": header_row - 1,
                "endRowIndex": header_row,
                "startColumnIndex": col_start - 1,
                "endColumnIndex": col_start + 13,
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": _hex_to_rgb(fill),
                    "textFormat": {
                        # the family and size are restated because this request
                        # replaces the whole textFormat — omitting them would drop
                        # the header row back to the default typeface
                        "fontFamily": BLOCK_FONT,
                        "fontSize": BLOCK_FONT_SIZE,
                        "bold": True,
                        "foregroundColor": _hex_to_rgb(font),
                    },
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }
    }


def _pitcher_font_size(name: str) -> int:
    n = len(str(name).replace(" ", ""))
    if n > 16:
        return 6
    if n > 12:
        return 8
    return 10


def _pitcher_font_requests(
    sheet_id: int, games: list[dict[str, Any]], game_start_row: int, col_start: int
) -> list[dict]:
    requests = []
    pitcher_col = col_start + 1
    for index in range(GAMES_COUNT):
        name = games[index].get("對戰先發", "") if index < len(games) else ""
        row_0idx = game_start_row - 1 + index
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_0idx,
                        "endRowIndex": row_0idx + 1,
                        "startColumnIndex": pitcher_col,
                        "endColumnIndex": pitcher_col + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"fontSize": _pitcher_font_size(name)}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat.fontSize",
                }
            }
        )
    return requests


def _rate_colors(games: list[dict[str, Any]]) -> tuple[str, str]:
    batting_avg = _aggregate_batting_average(games)
    obp = _aggregate_on_base_percentage(games)
    avg_color = DEFAULT_FONT
    obp_color = DEFAULT_FONT
    if batting_avg is not None and batting_avg >= HOT_AVG_THRESHOLD:
        avg_color = HOT_RATE_FONT
    elif batting_avg is not None and batting_avg <= COLD_AVG_THRESHOLD:
        avg_color = COLD_RATE_FONT
    if obp is not None and obp >= HOT_OBP_THRESHOLD:
        obp_color = HOT_RATE_FONT
    elif obp is not None and obp <= COLD_OBP_THRESHOLD:
        obp_color = COLD_RATE_FONT
    return avg_color, obp_color


def _game_font_color_requests(
    sheet_id: int, games: list[dict[str, Any]], game_start_row: int, col_start: int
) -> list[dict]:
    requests = []
    runs_col = col_start + 4
    allowed_col = col_start + 5
    hits_col = col_start + 7
    avg_col = col_start + 11
    obp_col = col_start + 12
    for index in range(GAMES_COUNT):
        row_0idx = game_start_row - 1 + index
        runs_color = DEFAULT_FONT
        allowed_color = DEFAULT_FONT
        hits_color = DEFAULT_FONT
        avg_color = DEFAULT_FONT
        obp_color = DEFAULT_FONT
        if index < len(games):
            game = games[index]
            runs = _to_float(game.get("得分"))
            allowed = _to_float(game.get("失分"))
            if runs > allowed:
                runs_color = SCORE_WIN_FONT
            elif allowed > runs:
                allowed_color = SCORE_LOSS_FONT
            else:
                runs_color = SCORE_TIE_FONT
                allowed_color = SCORE_TIE_FONT
            if _to_float(game.get("安打")) >= 10:
                hits_color = HITS_10_PLUS_FONT
            avg_color, obp_color = _rate_colors([game])
        requests.append(_font_color_request(sheet_id, row_0idx, runs_col, runs_color))
        requests.append(
            _font_color_request(sheet_id, row_0idx, allowed_col, allowed_color)
        )
        requests.append(_font_color_request(sheet_id, row_0idx, hits_col, hits_color))
        requests.append(_font_color_request(sheet_id, row_0idx, avg_col, avg_color))
        requests.append(_font_color_request(sheet_id, row_0idx, obp_col, obp_color))

    for row_offset, avg_games in (
        (GAMES_COUNT, games),
        (GAMES_COUNT + 1, games[-5:]),
    ):
        row_0idx = game_start_row - 1 + row_offset
        avg_color, obp_color = _rate_colors(avg_games)
        requests.append(_font_color_request(sheet_id, row_0idx, avg_col, avg_color))
        requests.append(_font_color_request(sheet_id, row_0idx, obp_col, obp_color))
    return requests


def _recent_home_runs(games: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Home-run log across the last few games, newest last.

    A game with no home run still yields one placeholder row (``_no_hr``) that
    keeps its date, opponent, venue and starter while leaving the batting
    cells blank, so every recent game is accounted for.
    """
    events: list[dict[str, Any]] = []
    for game in games[-HOME_RUN_LOOKBACK_GAMES:]:
        game_date = game.get("日期", "")
        venue = game.get("球場", "")
        opponent = game.get("對戰球隊", "")
        details = game.get("全壘打明細", [])
        if details:
            for index, event in enumerate(details):
                enriched = dict(event)
                first = index == 0
                enriched["_日期"] = game_date if first else ""
                enriched["_球場"] = venue if first else ""
                enriched["_對戰"] = opponent if first else ""
                events.append(enriched)
        else:
            events.append(
                {
                    "_日期": game_date,
                    "_球場": venue,
                    "_對戰": opponent,
                    "打者": "─" * 12,
                    "投手": game.get("對戰先發", ""),
                    "_no_hr": True,
                }
            )
    return events[-HOME_RUN_EVENT_ROWS:]


def _home_run_event_row_count(games: list[dict[str, Any]]) -> int:
    return max(1, len(_recent_home_runs(games)))


def _home_run_layout(
    matchups: list[tuple[str, str]], games_by_team: dict[str, list[dict[str, Any]]]
) -> dict[str, int]:
    top_event_rows = max(
        (_home_run_event_row_count(games_by_team.get(away, [])) for away, _ in matchups),
        default=1,
    )
    bottom_event_rows = max(
        (_home_run_event_row_count(games_by_team.get(home, [])) for _, home in matchups),
        default=1,
    )
    top_header = TOP_HR_HEADER_ROW
    top_end = top_header + top_event_rows
    bottom_header = top_end + 2
    bottom_end = bottom_header + bottom_event_rows
    return {
        "top_header": top_header,
        "top_event_rows": top_event_rows,
        "top_end": top_end,
        "bottom_header": bottom_header,
        "bottom_event_rows": bottom_event_rows,
        "bottom_end": bottom_end,
    }


def _is_opposite_field_home_run(side: str, direction: str) -> bool:
    """Whether a home run should be flagged red, as NPB's 近十場 flags them.

    Dead centre always counts; otherwise it is opposite field when the ball left
    towards the batter's own side — 左本 for a left-handed hitter, 右本 for a
    right-handed one. MLB's feed already resolves a switch hitter to the side he
    actually batted from, so no pitcher hand is needed.
    """
    if direction.startswith("中"):
        return True
    stance = {"L": "左", "R": "右"}.get((side or "").upper()[:1], "")
    if not stance or not direction:
        return False
    return direction.startswith(stance)


def _hr_font_request(
    sheet_id: int, row_0idx: int, col_0idx: int, color: str, font_size: int
) -> dict:
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row_0idx,
                "endRowIndex": row_0idx + 1,
                "startColumnIndex": col_0idx,
                "endColumnIndex": col_0idx + 1,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "foregroundColor": _hex_to_rgb(color),
                        "fontSize": font_size,
                    }
                }
            },
            "fields": (
                "userEnteredFormat.textFormat.foregroundColor,"
                "userEnteredFormat.textFormat.fontSize"
            ),
        }
    }


def _hr_event_font_requests(
    sheet_id: int,
    games: list[dict[str, Any]],
    header_row: int,
    col_start: int,
    event_rows: int,
    col_offset: int,
    font_size: int,
    colour_of,
) -> list[dict]:
    """One request per event slot, empty ones included.

    Emitting every slot is what resets a colour left behind by yesterday's events
    when a row now holds a different home run, or none at all.
    """
    events = _recent_home_runs(games)
    requests = []
    for index in range(event_rows):
        event = events[index] if index < len(events) else None
        colour = colour_of(event) if event else DEFAULT_FONT
        requests.append(
            _hr_font_request(sheet_id, header_row + index,
                             col_start - 1 + col_offset, colour, font_size)
        )
    return requests


def _hr_bat_side_font_requests(
    sheet_id: int, games: list[dict[str, Any]], header_row: int,
    col_start: int, event_rows: int
) -> list[dict]:
    """打位: R blue, L red, switch hitters yellow — NPB's palette."""
    colours = {"R": BAT_SIDE_R_FONT, "L": BAT_SIDE_L_FONT, "S": BAT_SIDE_S_FONT}

    def colour_of(event):
        return colours.get((event.get("左右打") or "").upper()[:1], DEFAULT_FONT)

    return _hr_event_font_requests(sheet_id, games, header_row, col_start,
                                   event_rows, HR_BAT_SIDE_COL, 9, colour_of)


def _hr_direction_font_requests(
    sheet_id: int, games: list[dict[str, Any]], header_row: int,
    col_start: int, event_rows: int
) -> list[dict]:
    """方向: red for an opposite-field or dead-centre home run."""
    def colour_of(event):
        opposite = _is_opposite_field_home_run(
            event.get("左右打", ""), event.get("方向", "")
        )
        return OPPOSITE_FIELD_FONT if opposite else DEFAULT_FONT

    return _hr_event_font_requests(sheet_id, games, header_row, col_start,
                                   event_rows, HR_DIRECTION_SPAN[0], 9,
                                   colour_of)


def _hr_opponent_font_requests(
    sheet_id: int, games: list[dict[str, Any]], header_row: int,
    col_start: int, event_rows: int
) -> list[dict]:
    """球隊: the opponent's own colour, so the column reads at a glance."""
    def colour_of(event):
        fill, _ = TEAM_COLORS.get(event.get("_對戰", ""), (DEFAULT_FONT, ""))
        return fill

    return _hr_event_font_requests(sheet_id, games, header_row, col_start,
                                   event_rows, HR_OPPONENT_COL, 10,
                                   colour_of)


# Where each field sits inside the 14 columns a block owns, given their widths
# (40, 40, 130, 90, 35 x8, 55, 55). The game block above needs a wide 球場 column at
# +3, which in this block would be spent on a single L/R, so 打者 takes both wide
# columns and everything after it shifts along:
#   +1 日期 | +2..4 打者 | +5 打位 | +6..7 方向 | +8..12 投手 | +13 球隊
# 球場 is not shown: every club has one home park, so the opponent code already says
# where the game was played.
HR_DATE_COL = 1
HR_BATTER_SPAN = (2, 5)      # 255px — "Christian Encarnacion-Strand" with room to spare
HR_BAT_SIDE_COL = 5          # 35px is plenty for one letter
HR_DIRECTION_SPAN = (6, 8)   # 70px — 左中本 no longer collides with 投手
HR_PITCHER_SPAN = (8, 13)    # 195px — "Jordan Montgomery" no longer runs to the edge
HR_OPPONENT_COL = 13
HR_DASH_SPAN = (2, 8)        # a no-home-run row runs its dash across 打者→方向


def _hr_unmerge_request(
    sheet_id: int, header_row: int, end_row: int, col_start: int
) -> dict:
    """Drop existing merges before re-merging, so a re-run is not an error."""
    return {
        "unmergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": header_row - 1,
                "endRowIndex": end_row,
                "startColumnIndex": col_start - 1,
                "endColumnIndex": col_start + 13,
            }
        }
    }


def _hr_merge_requests(
    sheet_id: int,
    games: list[dict[str, Any]],
    header_row: int,
    col_start: int,
    event_rows: int,
) -> list[dict]:
    """Give 投手 and 球場 the width NPB gives them.

    Both hold names far longer than one 35px column, and the cell to the right is
    occupied, so without the merge Sheets clips them rather than letting them run on.
    """
    def merge(row_0idx: int, span: tuple[int, int]) -> dict:
        return {
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_0idx,
                    "endRowIndex": row_0idx + 1,
                    "startColumnIndex": col_start - 1 + span[0],
                    "endColumnIndex": col_start - 1 + span[1],
                },
                "mergeType": "MERGE_ALL",
            }
        }

    events = _recent_home_runs(games)
    requests = [_hr_unmerge_request(sheet_id, header_row, header_row + event_rows,
                                    col_start)]
    for offset in range(event_rows + 1):
        row_0idx = header_row - 1 + offset
        requests.append(merge(row_0idx, HR_PITCHER_SPAN))
        requests.append(merge(row_0idx, HR_DIRECTION_SPAN))
        index = offset - 1
        if 0 <= index < len(events) and events[index].get("_no_hr"):
            requests.append(merge(row_0idx, HR_DASH_SPAN))
        else:
            requests.append(merge(row_0idx, HR_BATTER_SPAN))
    return requests


def _hr_row(team, date, batter, bat_side, direction, pitcher,
            opponent) -> list[Any]:
    """Lay one home-run row out across the block's 14 columns."""
    row = [""] * 14
    row[0] = team
    row[HR_DATE_COL] = date
    row[HR_BATTER_SPAN[0]] = batter
    row[HR_BAT_SIDE_COL] = bat_side
    row[HR_DIRECTION_SPAN[0]] = direction
    row[HR_PITCHER_SPAN[0]] = pitcher
    row[HR_OPPONENT_COL] = opponent
    return row


def _hr_table_values(
    team: str, games: list[dict[str, Any]], event_rows: int
) -> list[list[Any]]:
    events = _recent_home_runs(games)
    header = _hr_row(team, "日 期", "打 者", "打 位", "方 向", "投 手", "球 隊")
    rows = [header]
    for index in range(event_rows):
        if index >= len(events):
            rows.append([""] * 14)
            continue
        event = events[index]
        rows.append(
            _hr_row(
                "",
                _display_date(event["_日期"]) if event["_日期"] else "",
                event.get("打者", ""),
                event.get("左右打", ""),
                event.get("方向", ""),
                event.get("投手", ""),
                event.get("_對戰", ""),
            )
        )
    return rows


def _hr_pitcher_font_requests(
    sheet_id: int,
    games: list[dict[str, Any]],
    header_row: int,
    col_start: int,
    event_rows: int,
) -> list[dict]:
    events = _recent_home_runs(games)
    pitcher_col = col_start + 4
    requests = []
    for index in range(event_rows):
        name = events[index].get("投手", "") if index < len(events) else ""
        row_0idx = header_row + index
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_0idx,
                        "endRowIndex": row_0idx + 1,
                        "startColumnIndex": pitcher_col,
                        "endColumnIndex": pitcher_col + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"fontSize": _pitcher_font_size(name)}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat.fontSize",
                }
            }
        )
    return requests


BLOCK_FONT = "Arial Black"
BLOCK_FONT_SIZE = 10
HIDDEN_TOP_ROWS = 2  # rows 1-2 carry nothing; NPB squeezes them, we hide them

COLUMN_WIDTHS = [40, 40, 130, 90, 35, 35, 35, 35, 35, 35, 35, 35, 55, 55]
GAP_COLUMN_WIDTH = 2  # NPB keeps the seam between blocks hairline-thin


def _column_width_requests(sheet_id: int) -> list[dict]:
    """Force each block's 14 columns to a sane width every run.

    Sheets inherited from the pre-existing narrower (12-column) layout had
    2px "spacer" columns marking the old gaps between blocks. Those physical
    columns now fall inside the new 14-column blocks (holding 打率/四死 for
    some blocks) and stay invisible unless explicitly re-widened here.
    """
    requests = []
    for col_start in BLOCK_COLS:
        for index, width in enumerate(COLUMN_WIDTHS):
            col = col_start + index
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col - 1,
                            "endIndex": col,
                        },
                        "properties": {"pixelSize": width},
                        "fields": "pixelSize",
                    }
                }
            )
    for col_start in BLOCK_COLS[:-1]:
        gap_col = col_start + 14
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": gap_col - 1,
                        "endIndex": gap_col,
                    },
                    "properties": {"pixelSize": GAP_COLUMN_WIDTH},
                    "fields": "pixelSize",
                }
            }
        )
    return requests


def _update_sheet(
    sheet,
    matchups: list[tuple[str, str]],
    games_by_team: dict[str, list[dict[str, Any]]],
) -> None:
    active_matchups = matchups[:3]
    layout = _home_run_layout(active_matchups, games_by_team)
    value_updates = []
    format_requests = [
        _number_format_clear_request(
            sheet.id,
            TOP_HEADER_ROW,
            BOTTOM_HR_END_ROW,
            min(BLOCK_COLS),
            max(BLOCK_COLS) + 13,
        ),
        _appearance_reset_request(
            sheet.id,
            TOP_HEADER_ROW,
            BOTTOM_HR_END_ROW,
            min(BLOCK_COLS),
            max(BLOCK_COLS) + 13,
        ),
        _hide_gridlines_request(sheet.id),
        _hide_top_rows_request(sheet.id),
        _base_font_request(
            sheet.id,
            TOP_HEADER_ROW,
            BOTTOM_HR_END_ROW,
            min(BLOCK_COLS),
            max(BLOCK_COLS) + 13,
        ),
        *_column_width_requests(sheet.id),
    ]
    for col_idx, (away, home) in enumerate(active_matchups):
        col_start = BLOCK_COLS[col_idx]
        col_end = col_start + 13
        col_start_l = _col_to_letter(col_start)
        col_end_l = _col_to_letter(col_end)

        away_games = games_by_team.get(away, [])
        value_updates.append(
            {
                "range": f"{col_start_l}{TOP_HEADER_ROW}:{col_end_l}{TOP_AVG5_ROW}",
                "values": _build_block_values(away, away_games),
            }
        )
        format_requests.append(
            _header_format_request(sheet.id, away, TOP_HEADER_ROW, col_start)
        )
        format_requests.extend(
            _score_bracket_requests(sheet.id, TOP_GAME_START, col_start)
        )
        format_requests.extend(
            _pitcher_font_requests(sheet.id, away_games, TOP_GAME_START, col_start)
        )
        format_requests.extend(
            _game_font_color_requests(sheet.id, away_games, TOP_GAME_START, col_start)
        )

        home_games = games_by_team.get(home, [])
        value_updates.append(
            {
                "range": f"{col_start_l}{BOTTOM_HEADER_ROW}:{col_end_l}{BOTTOM_AVG5_ROW}",
                "values": _build_block_values(home, home_games),
            }
        )
        format_requests.append(
            _header_format_request(sheet.id, home, BOTTOM_HEADER_ROW, col_start)
        )
        format_requests.extend(
            _score_bracket_requests(sheet.id, BOTTOM_GAME_START, col_start)
        )
        format_requests.extend(
            _pitcher_font_requests(sheet.id, home_games, BOTTOM_GAME_START, col_start)
        )
        format_requests.extend(
            _game_font_color_requests(
                sheet.id, home_games, BOTTOM_GAME_START, col_start
            )
        )

        top_hr_end_l_row = layout["top_header"] + layout["top_event_rows"]
        value_updates.append(
            {
                "range": (
                    f"{col_start_l}{layout['top_header']}:"
                    f"{col_end_l}{top_hr_end_l_row}"
                ),
                "values": _hr_table_values(
                    away, away_games, layout["top_event_rows"]
                ),
            }
        )
        format_requests.append(
            _header_format_request(sheet.id, away, layout["top_header"], col_start)
        )
        format_requests.extend(
            _hr_pitcher_font_requests(
                sheet.id,
                away_games,
                layout["top_header"],
                col_start,
                layout["top_event_rows"],
            )
        )
        format_requests.extend(
            _hr_merge_requests(
                sheet.id,
                away_games,
                layout["top_header"],
                col_start,
                layout["top_event_rows"],
            )
        )
        format_requests.extend(
            _hr_bat_side_font_requests(
                sheet.id,
                away_games,
                layout["top_header"],
                col_start,
                layout["top_event_rows"],
            )
        )
        format_requests.extend(
            _hr_direction_font_requests(
                sheet.id,
                away_games,
                layout["top_header"],
                col_start,
                layout["top_event_rows"],
            )
        )
        format_requests.extend(
            _hr_opponent_font_requests(
                sheet.id,
                away_games,
                layout["top_header"],
                col_start,
                layout["top_event_rows"],
            )
        )

        bottom_hr_end_l_row = layout["bottom_header"] + layout["bottom_event_rows"]
        value_updates.append(
            {
                "range": (
                    f"{col_start_l}{layout['bottom_header']}:"
                    f"{col_end_l}{bottom_hr_end_l_row}"
                ),
                "values": _hr_table_values(
                    home, home_games, layout["bottom_event_rows"]
                ),
            }
        )
        format_requests.append(
            _header_format_request(sheet.id, home, layout["bottom_header"], col_start)
        )
        format_requests.extend(
            _hr_pitcher_font_requests(
                sheet.id,
                home_games,
                layout["bottom_header"],
                col_start,
                layout["bottom_event_rows"],
            )
        )
        format_requests.extend(
            _hr_merge_requests(
                sheet.id,
                home_games,
                layout["bottom_header"],
                col_start,
                layout["bottom_event_rows"],
            )
        )
        format_requests.extend(
            _hr_bat_side_font_requests(
                sheet.id,
                home_games,
                layout["bottom_header"],
                col_start,
                layout["bottom_event_rows"],
            )
        )
        format_requests.extend(
            _hr_direction_font_requests(
                sheet.id,
                home_games,
                layout["bottom_header"],
                col_start,
                layout["bottom_event_rows"],
            )
        )
        format_requests.extend(
            _hr_opponent_font_requests(
                sheet.id,
                home_games,
                layout["bottom_header"],
                col_start,
                layout["bottom_event_rows"],
            )
        )

    clear_col_l = _col_to_letter(max(BLOCK_COLS) + 13)
    _with_retries(
        f"clear {sheet.title}",
        lambda: sheet.batch_clear([f"B3:{clear_col_l}{BOTTOM_HR_END_ROW}"]),
    )
    # Unmerge before writing: a value landing on a cell that some earlier layout
    # merged away is silently dropped, which is how 投手 / 球隊 / 球場 came out blank
    # the first time the home-run columns moved.
    unmerges = [r for r in format_requests if "unmergeCells" in r]
    format_requests = [r for r in format_requests if "unmergeCells" not in r]
    if unmerges:
        _with_retries(
            f"unmerge {sheet.title}",
            lambda: sheet.spreadsheet.batch_update({"requests": unmerges}),
        )
    _with_retries(
        f"write {sheet.title}",
        lambda: sheet.batch_update(value_updates, value_input_option="RAW"),
    )
    _with_retries(
        f"format {sheet.title}",
        lambda: sheet.spreadsheet.batch_update({"requests": format_requests}),
    )


def update_mlb_last10() -> None:
    games_by_team = _read_team_games()
    _enrich_batting_stats(games_by_team)
    matchups = _next_matchups() or _fallback_matchups()

    seen: set[str] = set()
    ordered_pairs: list[tuple[str, str]] = []
    for away, home in matchups:
        if away in seen or home in seen:
            continue
        seen.update([away, home])
        ordered_pairs.append((away, home))
    for team in TEAM_ORDER:
        if team not in seen:
            partner = next((t for t in TEAM_ORDER if t not in seen and t != team), "")
            if partner:
                seen.update([team, partner])
                ordered_pairs.append((team, partner))

    for page_index in range(5):
        page_pairs = ordered_pairs[page_index * 3 : page_index * 3 + 3]
        if not page_pairs:
            continue
        sheet = _ensure_worksheet(f"{SHEET_PREFIX}{page_index + 1}")
        _update_sheet(sheet, page_pairs, games_by_team)
        print(
            f"[{sheet.title}] updated {len(page_pairs)} matchup block(s).", flush=True
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    update_mlb_last10()


if __name__ == "__main__":
    main()
