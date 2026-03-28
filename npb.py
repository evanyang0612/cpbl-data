import json
import os
import sys
import re
import platform
import asyncio
import aiohttp
from bs4 import BeautifulSoup as bs
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
NPB_SPREADSHEET_KEY = "1C4TsCe3LSjSpp_hPrrVErAN22vaR9kuh1iZmJkJMpGk"
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")

BASE_URL = "https://baseball.yahoo.co.jp/npb/"
MAX_RETRY = 3
GAMES_COUNT = 10
MAX_CONCURRENT = 5

NPB_TEAMS = {
    "巨人": {
        "id": 1,
        "name": "巨 人",
        "fill": "ff6600",
        "font": "000000",
        "league": "央盟",
    },
    "ヤクルト": {
        "id": 2,
        "name": "燕 子",
        "fill": "00009a",
        "font": "ffffff",
        "league": "央盟",
    },
    "DeNA": {
        "id": 3,
        "name": "橫 濱",
        "fill": "003366",
        "font": "b6dde8",
        "league": "央盟",
    },
    "中日": {
        "id": 4,
        "name": "中 日",
        "fill": "002774",
        "font": "ffffff",
        "league": "央盟",
    },
    "阪神": {
        "id": 5,
        "name": "阪 神",
        "fill": "fcf600",
        "font": "000000",
        "league": "央盟",
    },
    "広島": {
        "id": 6,
        "name": "廣 島",
        "fill": "ea0000",
        "font": "ffffff",
        "league": "央盟",
    },
    "西武": {
        "id": 7,
        "name": "西 武",
        "fill": "99ccff",
        "font": "17365d",
        "league": "洋盟",
    },
    "日本ハム": {
        "id": 8,
        "name": "火 腿",
        "fill": "2b67af",
        "font": "ffffff",
        "league": "洋盟",
    },
    "ロッテ": {
        "id": 9,
        "name": "羅 德",
        "fill": "808080",
        "font": "ffffff",
        "league": "洋盟",
    },
    "オリックス": {
        "id": 11,
        "name": "歐 牛",
        "fill": "002060",
        "font": "c4bf00",
        "league": "洋盟",
    },
    "ソフトバンク": {
        "id": 12,
        "name": "軟 銀",
        "fill": "ffcc00",
        "font": "000000",
        "league": "洋盟",
    },
    "楽天": {
        "id": 376,
        "name": "樂 天",
        "fill": "800000",
        "font": "ffffff",
        "league": "洋盟",
    },
}

NPB_FIELDS = {
    "東京ドーム": "東 京",
    "バンテリンドーム": "名古屋",
    "甲子園": "甲子園",
    "神宮": "神 宮",
    "マツダスタジアム": "馬自達",
    "横浜": "横 浜",
    "ZOZOマリン": "ZOZO",
    "ベルーナドーム": "西 武",
    "みずほPayPay": "福 岡",
    "京セラD大阪": "京大阪",
    "エスコンF": "エスコン",
    "楽天モバイル": "宮 城",
}


LEAGUE_SHEETS = {
    "央盟": "近十場a",
    "洋盟": "近十場b",
}

# Block column start positions (1-indexed: B=2, O=15, AB=28)
BLOCK_COLS = [2, 15, 28]

# Row layout
TOP_HEADER_ROW = 3
TOP_GAME_START = 4
TOP_GAME_END = 13
TOP_AVG10_ROW = 14
TOP_AVG5_ROW = 15

BOTTOM_HEADER_ROW = 16
BOTTOM_GAME_START = 17
BOTTOM_GAME_END = 26
BOTTOM_AVG10_ROW = 27
BOTTOM_AVG5_ROW = 28

# Rows per block (header + 10 games + 2 avg rows = 13)
BLOCK_ROWS = 13


# --- Helpers ---


def hex_to_rgb(hex_color: str) -> dict:
    """Convert a 6-char hex color string to a Sheets API RGB dict (0.0–1.0 floats)."""
    h = hex_color.lstrip("#")
    return {
        "red": int(h[0:2], 16) / 255,
        "green": int(h[2:4], 16) / 255,
        "blue": int(h[4:6], 16) / 255,
    }


def col_to_letter(col: int) -> str:
    """Convert 1-indexed column number to letter(s). e.g. 2→B, 15→O, 28→AB"""
    result = ""
    while col > 0:
        col, rem = divmod(col - 1, 26)
        result = chr(65 + rem) + result
    return result


def get_worksheet(sheet_name: str):
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=scope
        )
    else:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    client = gspread.authorize(creds)
    return client.open_by_key(NPB_SPREADSHEET_KEY).worksheet(sheet_name)


# --- Scraping ---


async def _fetch(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    for attempt in range(MAX_RETRY + 1):
        try:
            async with session.get(url) as res:
                if res.status == 200:
                    return await res.text()
        except Exception:
            pass
        if attempt < MAX_RETRY:
            await asyncio.sleep(5)
    return None


async def get_game_info(game_id: str, session: aiohttp.ClientSession) -> Optional[dict]:
    """
    Fetch box score for a finished game. Returns a dict keyed by team display name,
    each value is a game data dict. Also includes 'teams' and 'game_id'.

    実分 = earned runs scored BY this team (ER against opponent's pitchers)
    実失 = earned runs allowed BY this team (ER this team's pitchers gave up)
    """
    html = await _fetch(session, f"{BASE_URL}game/{game_id}/stats")
    if not html:
        return None

    soup = bs(html, "html.parser")
    teams_info = soup.find_all(class_="bb-gameScoreTable__team")
    if len(teams_info) < 2:
        return None

    # teams_info[0] = away team, teams_info[1] = home team
    away_raw = teams_info[0].text.strip()
    home_raw = teams_info[1].text.strip()
    if home_raw not in NPB_TEAMS or away_raw not in NPB_TEAMS:
        return None

    home_name = NPB_TEAMS[home_raw]["name"]
    away_name = NPB_TEAMS[away_raw]["name"]

    title = soup.find("title")
    if not title:
        return None
    match = re.search(r"(\d+年\d{1,2}月\d{1,2}日)", title.text)
    if not match:
        return None
    date = datetime.strptime(match.group(1), "%Y年%m月%d日").strftime("%Y/%m/%d")

    venue_el = soup.find(class_="bb-gameRound--stadium")
    if not venue_el:
        return None
    venue_raw = venue_el.text.strip()
    field = NPB_FIELDS.get(venue_raw, venue_raw)

    game_template = {
        "日期": date,
        "球場": field,
        "對戰球隊": "",
        "對戰先發": "",
        "実分": 0,
        "失分": 0,
        "実失": 0,
        "得分": 0,
        "安打": 0,
        "三振": 0,
        "四球": 0,
        "死球": 0,
        "全壘打": 0,
    }
    result = {
        home_name: {**deepcopy(game_template), "對戰球隊": away_name},
        away_name: {**deepcopy(game_template), "對戰球隊": home_name},
        "teams": [home_name, away_name],
        "home": home_name,
        "away": away_name,
        "game_id": game_id,
    }

    # Batting stats: idx=0 → away team, idx=1 → home team
    for idx, tbl in enumerate(soup.find_all(class_="bb-statsTable")):
        key = away_name if idx == 0 else home_name
        cells = tbl.find_all(class_="bb-statsTable__data--result")
        if len(cells) < 12:
            continue
        result[key].update(
            {
                "得分": int(cells[2].text),
                "安打": int(cells[3].text),
                "三振": int(cells[5].text),
                "四球": int(cells[6].text),
                "死球": int(cells[7].text),
                "全壘打": int(cells[11].text),
            }
        )

    # Pitching stats: score table idx=0 → away pitchers (faced by home batters)
    #                              idx=1 → home pitchers (faced by away batters)
    #   - idx=0: pitcher_side=away, batter_side=home
    #     → home["実分"] += away pitcher ER  (earned runs home scored vs away pitching)
    #     → away["失分"] += away pitcher R   (total runs home scored vs away pitching)
    #   - idx=1: pitcher_side=home, batter_side=away
    #     → away["実分"] += home pitcher ER  (earned runs away scored vs home pitching)
    #     → home["失分"] += home pitcher R   (total runs away scored vs home pitching)
    for idx, score_tbl in enumerate(soup.find_all(class_="bb-scoreTable")):
        batter_key = home_name if idx == 0 else away_name
        pitcher_key = away_name if idx == 0 else home_name
        rows = score_tbl.find_all(class_="bb-scoreTable__row")
        for row_idx, row in enumerate(rows):
            if row_idx == 0:
                player_el = row.find(class_="bb-scoreTable__data--player")
                if player_el:
                    # This is the opposing starter from batter_key's perspective
                    result[batter_key]["對戰先發"] = player_el.text.strip()
            scores = row.find_all(class_="bb-scoreTable__data--score")
            if len(scores) >= 2:
                result[batter_key]["実分"] += int(
                    scores[-1].text
                )  # ER batter_key scored
                result[pitcher_key]["失分"] += int(
                    scores[-2].text
                )  # R allowed by pitcher_key

    # 実失 = earned runs this team allowed = earned runs the opposing team scored
    result[home_name]["実失"] = result[away_name]["実分"]
    result[away_name]["実失"] = result[home_name]["実分"]

    return result


async def get_last_n_game_ids(
    team_id: int, n: int, session: aiohttp.ClientSession
) -> list[str]:
    """Return up to n finished game IDs for a team, most-recent first."""
    game_ids: list[str] = []
    now = datetime.now()

    while len(game_ids) < n:
        time_key = now.strftime("%Y-%m")
        html = await _fetch(
            session, f"{BASE_URL}teams/{team_id}/schedule?month={time_key}"
        )
        if not html:
            break

        soup = bs(html, "html.parser")
        entries = list(soup.find_all(class_="bb-calendarTable__data"))
        entries.reverse()

        for data in entries:
            date_el = data.find(class_="bb-calendarTable__date")
            if date_el is None:
                continue
            try:
                entry_day = int(date_el.text)
            except ValueError:
                continue
            if entry_day != now.day:
                continue

            status = data.find(class_="bb-calendarTable__status")
            if status and status.text.strip() == "試合終了":
                m = re.search(r"npb/game/(.*?)/", status.get("href", ""))
                if m:
                    gid = m.group(1)
                    if gid not in game_ids:
                        game_ids.append(gid)

            if len(game_ids) >= n:
                break

            now -= timedelta(days=1)
            if now.strftime("%Y-%m") != time_key:
                break

    return game_ids[:n]


async def get_next_scheduled_game(
    team_id: int, session: aiohttp.ClientSession
) -> tuple[Optional[str], Optional[str]]:
    """
    Find the next upcoming (not yet finished) game for a team.
    Returns (game_id, date_str) or (None, None).
    game_id may be None when the game is scheduled but the page isn't live yet.
    """
    now = datetime.now()

    for month_offset in range(3):
        check_month = (now.replace(day=1) + timedelta(days=32 * month_offset)).replace(
            day=1
        )
        time_key = check_month.strftime("%Y-%m")
        html = await _fetch(
            session, f"{BASE_URL}teams/{team_id}/schedule?month={time_key}"
        )
        if not html:
            continue

        soup = bs(html, "html.parser")
        for data in soup.find_all(class_="bb-calendarTable__data"):
            date_el = data.find(class_="bb-calendarTable__date")
            if date_el is None:
                continue
            try:
                day = int(date_el.text)
                entry_date = check_month.replace(day=day)
            except (ValueError, TypeError):
                continue

            if entry_date.date() < now.date():
                continue

            status = data.find(class_="bb-calendarTable__status")
            if not status:
                continue
            text = status.text.strip()
            if text in ("試合終了", "中止", ""):
                continue

            # Game is upcoming — try to extract game ID (href may or may not exist)
            href = status.get("href") or ""
            # Use [^/]+ to match with or without trailing slash
            m = re.search(r"npb/game/([^/]+)", href)
            game_id = m.group(1) if m else None
            return game_id, entry_date.strftime("%Y-%m-%d")

    return None, None


async def _get_schedule_opponent(
    team_id: int, target_date: str, session: aiohttp.ClientSession
) -> Optional[str]:
    """
    Read a team's schedule page for target_date and return the opponent team key
    by scanning for npb/teams/{id}/ links in that day's calendar entry.
    """
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    time_key = dt.strftime("%Y-%m")
    html = await _fetch(session, f"{BASE_URL}teams/{team_id}/schedule?month={time_key}")
    if not html:
        return None
    soup = bs(html, "html.parser")
    for data in soup.find_all(class_="bb-calendarTable__data"):
        date_el = data.find(class_="bb-calendarTable__date")
        if not date_el:
            continue
        try:
            if int(date_el.text) != dt.day:
                continue
        except ValueError:
            continue
        for a in data.find_all("a", href=True):
            m = re.search(r"npb/teams/(\d+)", a["href"])
            if m:
                opp_id = int(m.group(1))
                for key, info in NPB_TEAMS.items():
                    if info["id"] == opp_id:
                        return key
    return None


async def get_next_matchups(
    league: str, session: aiohttp.ClientSession
) -> list[tuple[str, str]]:
    """
    Returns up to 3 (away_key, home_key) pairs for the next game day in the league.
    Home/away is determined by the team order in the game page HTML ([0]=away, [1]=home).
    During inter-league (交流戦) games, records each same-league team's home/away role
    and pairs them with another same-league team in the same role split.
    Falls back to alphabetical pairing if schedule cannot be determined.
    """
    league_teams = {k: v for k, v in NPB_TEAMS.items() if v["league"] == league}

    # Get next game ID + date for each team concurrently
    tasks = {
        key: get_next_scheduled_game(info["id"], session)
        for key, info in league_teams.items()
    }
    resolved = await asyncio.gather(*tasks.values())
    # Include teams where a date was found, even if game_id is None (pre-season / no page yet)
    team_next: dict[str, tuple[Optional[str], str]] = {
        key: (gid, d) for key, (gid, d) in zip(tasks.keys(), resolved) if d is not None
    }

    if not team_next:
        print(f"[{league}] No upcoming games found, using alphabetical order.")
        teams = list(league_teams.keys())
        return [(teams[i * 2], teams[i * 2 + 1]) for i in range(3)]

    # Find the nearest next game date
    next_date = min(d for _, d in team_next.values())
    day_games = {key: gid for key, (gid, d) in team_next.items() if d == next_date}

    if not day_games:
        teams = list(league_teams.keys())
        return [(teams[i * 2], teams[i * 2 + 1]) for i in range(3)]

    print(f"[{league}] Next game day: {next_date}, games: {day_games}")

    seen: dict[str, tuple[str, str]] = {}  # game_id -> (away_key, home_key)
    cross_roles: dict[str, str] = (
        {}
    )  # team_key -> 'away' | 'home' for inter-league teams

    # For teams that have a real game ID, fetch the game page to get teams + venue.
    # /top works for finished games; for upcoming games /top has no team/venue data,
    # but /stats does — so always try /stats as fallback when parsing fails.
    known_ids = {gid for gid in day_games.values() if gid is not None}
    for game_id in dict.fromkeys(known_ids):
        soup = None
        for path in ("stats", "top"):
            html = await _fetch(session, f"{BASE_URL}game/{game_id}/{path}")
            if not html:
                continue
            candidate = bs(html, "html.parser")
            if len(candidate.find_all(class_="bb-gameScoreTable__team")) >= 2:
                soup = candidate
                break
        if soup is None:
            continue

        teams_els = soup.find_all(class_="bb-gameScoreTable__team")
        if len(teams_els) < 2:
            continue

        t0 = teams_els[0].text.strip()  # away
        t1 = teams_els[1].text.strip()  # home

        t0_in = t0 in league_teams
        t1_in = t1 in league_teams

        if t0_in and t1_in:
            # Same-league game — page order: [0]=away, [1]=home
            seen[game_id] = (t0, t1)
        elif t0_in:
            # Inter-league: t0 is our team and is playing away
            cross_roles[t0] = "away"
        elif t1_in:
            # Inter-league: t1 is our team and is playing home
            cross_roles[t1] = "home"

    matchups = list(seen.values())
    matched = {t for pair in matchups for t in pair}
    matched.update(cross_roles.keys())

    # For teams still unmatched (no game ID yet), try reading opponent from schedule page
    no_id_teams = [
        k for k, gid in day_games.items() if gid is None and k not in matched
    ]

    if no_id_teams:
        opp_tasks = {
            key: _get_schedule_opponent(league_teams[key]["id"], next_date, session)
            for key in no_id_teams
        }
        opp_resolved = await asyncio.gather(*opp_tasks.values())
        opponents: dict[str, Optional[str]] = dict(zip(opp_tasks.keys(), opp_resolved))

        paired: set[str] = set()
        for key in no_id_teams:
            if key in paired or key in matched:
                continue
            opp = opponents.get(key)
            if opp and opp in league_teams and opp in no_id_teams and opp not in paired:
                # Same-league game, no ID yet; use lower NPB ID as home (arbitrary but stable)
                if NPB_TEAMS[key]["id"] < NPB_TEAMS[opp]["id"]:
                    home_key, away_key = key, opp
                else:
                    home_key, away_key = opp, key
                matchups.append((away_key, home_key))
                paired.update([key, opp])
                matched.update([key, opp])
            elif opp and opp not in league_teams:
                # Inter-league game, no ID yet — can't determine home/away without game page;
                # default to 'away' so the team lands on top rather than being dropped
                cross_roles[key] = "away"
                matched.add(key)

    # Pair inter-league teams: match away with home where possible
    away_cross = [k for k, r in cross_roles.items() if r == "away"]
    home_cross = [k for k, r in cross_roles.items() if r == "home"]

    while away_cross and home_cross and len(matchups) < 3:
        matchups.append((away_cross.pop(0), home_cross.pop(0)))

    # If roles are unbalanced (e.g. all-away day), pair same-role teams together
    remaining_cross = away_cross + home_cross
    for i in range(0, len(remaining_cross) - 1, 2):
        if len(matchups) >= 3:
            break
        matchups.append((remaining_cross[i], remaining_cross[i + 1]))

    # Pad to 3 if still fewer than 3 matchups (e.g. rest days)
    matched = {t for pair in matchups for t in pair}
    unmatched = [k for k in league_teams if k not in matched]
    for i in range(0, len(unmatched) - 1, 2):
        if len(matchups) >= 3:
            break
        matchups.append((unmatched[i], unmatched[i + 1]))

    # Sort columns by away team's NPB ID for a consistent, predictable left-to-right order
    matchups.sort(key=lambda pair: NPB_TEAMS[pair[0]]["id"])
    return matchups[:3]


# --- Sheet building ---


def build_block_values(team_key: str, games: list[dict]) -> list[list]:
    """
    Build 13 rows × 12 cols for one team block:
      row 0:    header (team name + 11 column labels)
      rows 1-10: game data (oldest → newest, empty rows if fewer than 10 games)
      row 11:   近十場 平均
      row 12:   近五場 平均
    """
    display_name = NPB_TEAMS[team_key]["name"]

    header = [
        display_name,
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

    # Sort by date, keep last GAMES_COUNT
    sorted_games = sorted(
        games,
        key=lambda g: datetime.strptime(g["日期"], "%Y/%m/%d"),
    )[-GAMES_COUNT:]

    rows = [header]

    for i in range(GAMES_COUNT):
        if i < len(sorted_games):
            g = sorted_games[i]
            date = datetime.strptime(g["日期"], "%Y/%m/%d")
            date_str = (
                date.strftime("%#m/%#d")
                if platform.system() == "Windows"
                else date.strftime("%-m/%-d")
            )
            row = [
                date_str,
                g.get("對戰球隊", ""),
                g.get("對戰先發", ""),
                g.get("球場", ""),
                g.get("実分", 0),
                g.get("得分", 0),
                g.get("失分", 0),
                g.get("実失", 0),
                g.get("安打", 0),
                g.get("三振", 0),
                g.get("四球", 0) + g.get("死球", 0),
                g.get("全壘打", 0),
            ]
        else:
            row = [""] * 12
        rows.append(row)

    def avg_row(label: str, game_list: list[dict]) -> list:
        if not game_list:
            return ["", "", label, "平 均"] + [""] * 8
        n = len(game_list)

        def r(v):
            return round(v / n, 1)

        return [
            "",
            "",
            label,
            "平 均",
            r(sum(g.get("実分", 0) for g in game_list)),
            r(sum(g.get("得分", 0) for g in game_list)),
            r(sum(g.get("失分", 0) for g in game_list)),
            r(sum(g.get("実失", 0) for g in game_list)),
            r(sum(g.get("安打", 0) for g in game_list)),
            r(sum(g.get("三振", 0) for g in game_list)),
            r(sum(g.get("四球", 0) + g.get("死球", 0) for g in game_list)),
            r(sum(g.get("全壘打", 0) for g in game_list)),
        ]

    rows.append(avg_row("近十場", sorted_games))
    rows.append(avg_row("近五場", sorted_games[-5:]))

    return rows  # 13 rows × 12 cols


def _pitcher_font_size(name: str) -> int:
    """12pt (default) for ≤5 chars; shrink only when name exceeds 5 chars."""
    n = len(name.replace(" ", ""))
    if n > 7:
        return 6
    if n > 5:
        return 8
    return 10


def _pitcher_font_requests(
    sheet_id: int, games: list[dict], game_start_row: int, col_start: int
) -> list[dict]:
    """
    One repeatCell request per game row that sets the pitcher cell font size.
    Also resets empty rows to default (10) so stale small fonts don't linger.
    Pitcher column = col_start + 2 (1-indexed) → col_start + 1 (0-indexed).
    """
    sorted_games = sorted(
        games, key=lambda g: datetime.strptime(g["日期"], "%Y/%m/%d")
    )[-GAMES_COUNT:]

    pitcher_col = col_start + 1  # 0-indexed (col_start is 1-indexed)
    requests = []

    for i in range(GAMES_COUNT):
        name = sorted_games[i].get("對戰先發", "") if i < len(sorted_games) else ""
        row_0idx = game_start_row - 1 + i
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


def _header_format_request(
    sheet_id: int, team_key: str, header_row: int, col_start: int
) -> dict:
    """Build a Sheets API repeatCell request to colour one header row."""
    info = NPB_TEAMS[team_key]
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": header_row - 1,  # 0-indexed, inclusive
                "endRowIndex": header_row,  # exclusive
                "startColumnIndex": col_start - 1,  # 0-indexed, inclusive
                "endColumnIndex": col_start + 11,  # exclusive (12 cols)
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": hex_to_rgb(info["fill"]),
                    "textFormat": {
                        "bold": True,
                        "foregroundColor": hex_to_rgb(info["font"]),
                    },
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }
    }


def update_league_sheet(
    sheet_name: str,
    matchups: list[tuple[str, str]],
    all_games: dict[str, list[dict]],
):
    """
    Write all 6 team blocks into one sheet.
    matchups[i] = (away_key, home_key) → away goes to top block i, home to bottom block i.
    """
    sheet = get_worksheet(sheet_name)
    value_updates = []
    format_requests = []

    for col_idx, (away_key, home_key) in enumerate(matchups[:3]):
        col_start = BLOCK_COLS[col_idx]
        col_end = col_start + 11
        col_start_l = col_to_letter(col_start)
        col_end_l = col_to_letter(col_end)

        # Top block (away team)
        away_games = all_games.get(away_key, [])
        top_values = build_block_values(away_key, away_games)
        value_updates.append(
            {
                "range": f"{col_start_l}{TOP_HEADER_ROW}:{col_end_l}{TOP_AVG5_ROW}",
                "values": top_values,
            }
        )
        format_requests.append(
            _header_format_request(sheet.id, away_key, TOP_HEADER_ROW, col_start)
        )
        format_requests.extend(
            _pitcher_font_requests(sheet.id, away_games, TOP_GAME_START, col_start)
        )

        # Bottom block (home team)
        home_games = all_games.get(home_key, [])
        bottom_values = build_block_values(home_key, home_games)
        value_updates.append(
            {
                "range": f"{col_start_l}{BOTTOM_HEADER_ROW}:{col_end_l}{BOTTOM_AVG5_ROW}",
                "values": bottom_values,
            }
        )
        format_requests.append(
            _header_format_request(sheet.id, home_key, BOTTOM_HEADER_ROW, col_start)
        )
        format_requests.extend(
            _pitcher_font_requests(sheet.id, home_games, BOTTOM_GAME_START, col_start)
        )

    # Write values first, then apply formatting in one batch API call
    sheet.batch_update(value_updates, value_input_option="USER_ENTERED")
    sheet.spreadsheet.batch_update({"requests": format_requests})
    print(f"[{sheet_name}] Updated {len(value_updates)} blocks with header colours.")


# --- Main ---


async def run_once():
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    errors = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for league, sheet_name in LEAGUE_SHEETS.items():
            league_teams = {k: v for k, v in NPB_TEAMS.items() if v["league"] == league}
            print(f"\n=== {league} ({sheet_name}) ===")

            # 1. Determine team order from next game matchups
            try:
                matchups = await get_next_matchups(league, session)
            except Exception as e:
                errors.append(f"get_next_matchups({league}): {e}")
                teams = list(league_teams.keys())
                matchups = [(teams[i * 2], teams[i * 2 + 1]) for i in range(3)]

            print(f"Matchup order: {matchups}")

            # 2. Fetch last 10 game IDs for each team
            all_game_ids: dict[str, list[str]] = {}
            for team_key, team_info in league_teams.items():
                try:
                    ids = await get_last_n_game_ids(
                        team_info["id"], GAMES_COUNT, session
                    )
                    all_game_ids[team_key] = ids
                    print(f"  {team_key}: {len(ids)} game IDs found")
                except Exception as e:
                    errors.append(f"get_last_n_game_ids({team_key}): {e}")
                    all_game_ids[team_key] = []

            # 3. Fetch game details (deduplicated across teams)
            game_cache: dict[str, dict] = {}
            unique_ids = {gid for ids in all_game_ids.values() for gid in ids}
            id_list = list(unique_ids)

            for i in range(0, len(id_list), MAX_CONCURRENT):
                batch = id_list[i : i + MAX_CONCURRENT]
                results = await asyncio.gather(
                    *[get_game_info(gid, session) for gid in batch],
                    return_exceptions=True,
                )
                for gid, result in zip(batch, results):
                    if isinstance(result, Exception):
                        errors.append(f"get_game_info({gid}): {result}")
                    elif result:
                        game_cache[gid] = result
                if i + MAX_CONCURRENT < len(id_list):
                    await asyncio.sleep(2)

            # 4. Build per-team game lists from cache
            all_games: dict[str, list[dict]] = {}
            for team_key, team_info in league_teams.items():
                team_name = team_info["name"]
                game_list = []
                for gid in all_game_ids[team_key]:
                    cached = game_cache.get(gid)
                    if cached and team_name in cached:
                        game_list.append(cached[team_name])
                all_games[team_key] = game_list
                print(f"  {team_key}: {len(game_list)} games with data")

            # 5. Write to sheet
            try:
                update_league_sheet(sheet_name, matchups, all_games)
            except Exception as e:
                errors.append(f"update_league_sheet({sheet_name}): {e}")

    if errors:
        print(f"\n[ERROR] {len(errors)} failure(s):")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)
    else:
        print("\nDone.")


if __name__ == "__main__":
    asyncio.run(run_once())
