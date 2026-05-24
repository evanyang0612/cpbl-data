"""Build MLB 近十場 sheets from the MLB 紀錄 worksheet."""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from gspread.exceptions import APIError, WorksheetNotFound

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cpbl import _sheets_client


SPREADSHEET_KEY = "11FV70TXVAxLTwYH6pLj7HwK1qq-fIa61QrePRCC8YUM"
RECORD_SHEET_NAME = "紀錄"
SHEET_PREFIX = "MLB近十場"
MLB_API = "https://statsapi.mlb.com/api"
REQUEST_TIMEOUT = (10, 45)

GAMES_COUNT = 10
BLOCK_COLS = [2, 15, 28]
TOP_HEADER_ROW = 3
TOP_GAME_START = 4
TOP_AVG5_ROW = 15
BOTTOM_HEADER_ROW = 16
BOTTOM_GAME_START = 17
BOTTOM_AVG5_ROW = 28

DEFAULT_FONT = "#202124"
SCORE_WIN_FONT = "#d93025"
SCORE_LOSS_FONT = "#188038"
SCORE_TIE_FONT = "#5f6368"
HITS_10_PLUS_FONT = "#d93025"

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
    "ATH",
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
    "ATH": ("#003831", "#efb21e"),
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
        lambda: _raise_for_json(session.get(url, params=params, timeout=REQUEST_TIMEOUT)),
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
    padded = row + [""] * (40 - len(row))
    game_id = _to_text(padded[1])
    if not game_id:
        return []
    away = _to_text(padded[2])
    home = _to_text(padded[17])
    if not away or not home:
        return []

    date_value = _to_date_text(padded[0])
    venue = _to_text(padded[32])
    away_game = {
        "日期": date_value,
        "賽事編號": game_id,
        "隊伍": away,
        "對戰球隊": home,
        "對戰先發": _to_text(padded[30]),
        "球場": venue,
        "実分": _to_int(padded[39]),
        "得分": _to_int(padded[14]),
        "失分": _to_int(padded[27]),
        "実失": _to_int(padded[38]),
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
        "実分": _to_int(padded[38]),
        "得分": _to_int(padded[27]),
        "失分": _to_int(padded[14]),
        "実失": _to_int(padded[39]),
        "安打": _to_int(padded[28]),
        "主客": "主",
    }
    return [(away, away_game), (home, home_game)]


def _read_team_games() -> dict[str, list[dict[str, Any]]]:
    worksheet = _sheets_client.worksheet(SPREADSHEET_KEY, RECORD_SHEET_NAME)
    rows = _with_retries(
        "read record raw columns",
        lambda: worksheet.get("A2:AN", value_render_option="UNFORMATTED_VALUE"),
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


def _team_stats_from_feed(session: requests.Session, game_id: str) -> dict[str, dict[str, int]]:
    feed = _get_json(session, f"{MLB_API}/v1.1/game/{game_id}/feed/live")
    game_data = feed["gameData"]
    boxscore = feed["liveData"]["boxscore"]
    result: dict[str, dict[str, int]] = {}
    for side in ("away", "home"):
        team = boxscore["teams"][side]
        abbrev = game_data["teams"][side].get("abbreviation")
        batting = team.get("teamStats", {}).get("batting", {})
        result[abbrev] = {
            "三振": _to_int(batting.get("strikeOuts")),
            "四死": _to_int(batting.get("baseOnBalls"))
            + _to_int(batting.get("hitByPitch")),
            "本打": _to_int(batting.get("homeRuns")),
        }
    return result


def _enrich_batting_stats(games_by_team: dict[str, list[dict[str, Any]]]) -> None:
    session = requests.Session()
    game_ids = sorted({g["賽事編號"] for games in games_by_team.values() for g in games})
    stats_cache: dict[str, dict[str, dict[str, int]]] = {}
    for index, game_id in enumerate(game_ids, start=1):
        stats_cache[game_id] = _team_stats_from_feed(session, game_id)
        if index % 25 == 0 or index == len(game_ids):
            print(f"Fetched batting stats {index}/{len(game_ids)}", flush=True)
        time.sleep(0.03)
    for games in games_by_team.values():
        for game in games:
            game.update(stats_cache.get(game["賽事編號"], {}).get(game["隊伍"], {}))


def _avg_row(label: str, games: list[dict[str, Any]]) -> list[Any]:
    if not games:
        return ["", "", label, "平 均"] + [""] * 8
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
        avg("本打"),
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
            "本 打",
        ]
    ]
    sorted_games = games[-GAMES_COUNT:]
    for index in range(GAMES_COUNT):
        if index >= len(sorted_games):
            rows.append([""] * 12)
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
                game.get("本打", ""),
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
        games = [
            game
            for day in data.get("dates", [])
            for game in day.get("games", [])
            if game.get("gameType") == "R"
        ]
        if games:
            games.sort(key=lambda g: g.get("gameDate", ""))
            matchups = [
                (
                    game["teams"]["away"]["team"].get("abbreviation", ""),
                    game["teams"]["home"]["team"].get("abbreviation", ""),
                )
                for game in games
            ]
            return [(a, h) for a, h in matchups if a and h]
    return []


def _fallback_matchups() -> list[tuple[str, str]]:
    return [
        (TEAM_ORDER[index], TEAM_ORDER[index + 1])
        for index in range(0, len(TEAM_ORDER), 2)
    ]


def _ensure_worksheet(title: str):
    spreadsheet = _sheets_client.spreadsheet(SPREADSHEET_KEY)
    try:
        return spreadsheet.worksheet(title)
    except WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=30, cols=40)


def _font_color_request(sheet_id: int, row_0idx: int, col_0idx: int, hex_color: str) -> dict:
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


def _header_format_request(sheet_id: int, team: str, header_row: int, col_start: int) -> dict:
    fill, font = TEAM_COLORS.get(team, ("#3c4043", "#ffffff"))
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": header_row - 1,
                "endRowIndex": header_row,
                "startColumnIndex": col_start - 1,
                "endColumnIndex": col_start + 11,
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": _hex_to_rgb(fill),
                    "textFormat": {
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


def _game_font_color_requests(
    sheet_id: int, games: list[dict[str, Any]], game_start_row: int, col_start: int
) -> list[dict]:
    requests = []
    runs_col = col_start + 4
    allowed_col = col_start + 5
    hits_col = col_start + 7
    for index in range(GAMES_COUNT):
        row_0idx = game_start_row - 1 + index
        runs_color = DEFAULT_FONT
        allowed_color = DEFAULT_FONT
        hits_color = DEFAULT_FONT
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
        requests.append(_font_color_request(sheet_id, row_0idx, runs_col, runs_color))
        requests.append(_font_color_request(sheet_id, row_0idx, allowed_col, allowed_color))
        requests.append(_font_color_request(sheet_id, row_0idx, hits_col, hits_color))
    return requests


def _update_sheet(sheet, matchups: list[tuple[str, str]], games_by_team: dict[str, list[dict[str, Any]]]) -> None:
    value_updates = []
    format_requests = []
    for col_idx, (away, home) in enumerate(matchups[:3]):
        col_start = BLOCK_COLS[col_idx]
        col_end = col_start + 11
        col_start_l = _col_to_letter(col_start)
        col_end_l = _col_to_letter(col_end)

        away_games = games_by_team.get(away, [])
        value_updates.append(
            {
                "range": f"{col_start_l}{TOP_HEADER_ROW}:{col_end_l}{TOP_AVG5_ROW}",
                "values": _build_block_values(away, away_games),
            }
        )
        format_requests.append(_header_format_request(sheet.id, away, TOP_HEADER_ROW, col_start))
        format_requests.extend(_pitcher_font_requests(sheet.id, away_games, TOP_GAME_START, col_start))
        format_requests.extend(_game_font_color_requests(sheet.id, away_games, TOP_GAME_START, col_start))

        home_games = games_by_team.get(home, [])
        value_updates.append(
            {
                "range": f"{col_start_l}{BOTTOM_HEADER_ROW}:{col_end_l}{BOTTOM_AVG5_ROW}",
                "values": _build_block_values(home, home_games),
            }
        )
        format_requests.append(_header_format_request(sheet.id, home, BOTTOM_HEADER_ROW, col_start))
        format_requests.extend(_pitcher_font_requests(sheet.id, home_games, BOTTOM_GAME_START, col_start))
        format_requests.extend(_game_font_color_requests(sheet.id, home_games, BOTTOM_GAME_START, col_start))

    _with_retries(
        f"clear {sheet.title}",
        lambda: sheet.batch_clear(["B3:AM28"]),
    )
    _with_retries(
        f"write {sheet.title}",
        lambda: sheet.batch_update(value_updates, value_input_option="USER_ENTERED"),
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
        print(f"[{sheet.title}] updated {len(page_pairs)} matchup block(s).", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    update_mlb_last10()


if __name__ == "__main__":
    main()
