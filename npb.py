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
from urllib.parse import urljoin

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
NPB_SPREADSHEET_KEY = "1XBATQ-ZQVE7saISTw_EYEXg3qFFAn5aeLDPdGI1_8Rg"
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")

BASE_URL = "https://baseball.yahoo.co.jp/npb/"
NPB_OFFICIAL_BASE_URL = "https://npb.jp"
MAX_RETRY = 3
GAMES_COUNT = 10
MAX_CONCURRENT = 5

SCORE_WIN_FONT = "ff0000"
SCORE_LOSS_FONT = "38761d"
SCORE_TIE_FONT = "0000ff"
HITS_10_PLUS_FONT = "e26b0a"
DEFAULT_FONT = "000000"

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

SAILU_SPREADSHEET_KEY = "1qPdgcy_4s4Dj2xKo0QJawxPRaB6u9sGM3D4avkAjJUw"
SAILU_TARGET_SPREADSHEET_KEY = "1XBATQ-ZQVE7saISTw_EYEXg3qFFAn5aeLDPdGI1_8Rg"
SAILU_SHEET_NAME = "賽錄"
EXHIBITION_SHEET_NAME = "熱身賽紀錄"
ANALYSIS_SHEET_NAME = "分析表紀錄"
HUIZI_SHEET_NAME = "彙資"
ANALYSIS_SEASON = 2026

OFFICIAL_TEAM_NAME_MAP = {
    "読売": "巨人",
    "巨人": "巨人",
    "東京ヤクルト": "ヤクルト",
    "ヤクルト": "ヤクルト",
    "横浜DeNA": "DeNA",
    "DeNA": "DeNA",
    "中日": "中日",
    "阪神": "阪神",
    "広島東洋": "広島",
    "広島": "広島",
    "埼玉西武": "西武",
    "西武": "西武",
    "北海道日本ハム": "日本ハム",
    "日本ハム": "日本ハム",
    "千葉ロッテ": "ロッテ",
    "ロッテ": "ロッテ",
    "オリックス": "オリックス",
    "福岡ソフトバンク": "ソフトバンク",
    "ソフトバンク": "ソフトバンク",
    "東北楽天": "楽天",
    "楽天": "楽天",
}
OFFICIAL_TEAM_CODE_MAP = {
    "g": "巨人",
    "t": "阪神",
    "db": "横浜",
    "s": "ヤクルト",
    "d": "中日",
    "c": "広島",
    "l": "西武",
    "f": "日本ハム",
    "m": "ロッテ",
    "b": "オリックス",
    "h": "ソフトバンク",
    "e": "楽天",
}
_OFFICIAL_PLAYBYPLAY_CACHE: dict[str, dict[tuple[str, str, str], str]] = {}

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


def _display_field_name(venue: str) -> str:
    """Format venue names for compact NPB display sheets."""
    field = NPB_FIELDS.get(venue, venue)
    return f"{field[0]} {field[1]}" if len(field) == 2 else field


ANALYSIS_FIELDS = {
    "エスコンF": "エスコンF",
    "東京ドーム": "東京ドーム",
    "神宮": "明治神宮",
    "明治神宮": "明治神宮",
    "横浜": "横浜",
    "甲子園": "甲子園",
    "マツダスタジアム": "マツダ",
    "マツダ": "マツダ",
    "バンテリンドーム": "ナゴヤドーム",
    "ナゴヤドーム": "ナゴヤドーム",
    "ZOZOマリン": "QVCマリン",
    "QVCマリン": "QVCマリン",
    "みずほPayPay": "ヤフードーム",
    "ヤフードーム": "ヤフードーム",
    "ベルーナドーム": "西武ドーム",
    "西武ドーム": "西武ドーム",
    "楽天モバイル": "Ｋスタ宮城",
    "Ｋスタ宮城": "Ｋスタ宮城",
    "京セラD大阪": "京セラドーム",
    "京セラドーム": "京セラドーム",
    "京セラD": "京セラドーム",
    "ほっと神戸": "スカイマーク",
    "ほっともっと神戸": "スカイマーク",
    "スカイマーク": "スカイマーク",
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


def get_worksheet(sheet_name: str, spreadsheet_key: str = NPB_SPREADSHEET_KEY):
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=scope
        )
    else:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_key).worksheet(sheet_name)


def is_exhibition_game_id(game_id: str) -> bool:
    """Warm-up / exhibition games currently use the 202104... game-id prefix."""
    return str(game_id).startswith("202104")


def display_team_name(team_name: str) -> str:
    """Match existing sheet naming conventions."""
    return "横浜" if team_name == "DeNA" else team_name


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


async def _fetch_once(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url) as res:
            if res.status == 200:
                return await res.text()
    except Exception:
        pass
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
    field = _display_field_name(venue_raw)

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


# --- 賽錄 scraping & update ---


def _batting_event_counts(tbl) -> dict[str, int]:
    """Count batting events that Yahoo does not expose in the team total row."""

    def _normalized(text: str) -> str:
        return (
            text.replace("２", "2")
            .replace("３", "3")
            .replace("　", "")
            .replace(" ", "")
        )

    counts = {"2B": 0, "3B": 0, "GIDP": 0, "SF": 0}
    rows = tbl.find_all("tr") or tbl.find_all(class_="bb-statsTable__row")
    for row in rows:
        if row.find(class_="bb-statsTable__head--result"):
            continue
        for cell in row.find_all(class_="bb-statsTable__data--inning"):
            text = _normalized(cell.get_text("", strip=True))
            if not text:
                continue
            if "併打" in text or "併殺" in text:
                counts["GIDP"] += 1
            if "犠飛" in text:
                counts["SF"] += 1
            if "二塁打" in text or re.search(
                r"(左中|右中|左線|右線|中越|左越|右越|左|中|右)2", text
            ):
                counts["2B"] += 1
            if "三塁打" in text or re.search(
                r"(左中|右中|左線|右線|中越|左越|右越|左|中|右)3", text
            ):
                counts["3B"] += 1
    return counts


def _parse_batting_table(tbl) -> list:
    """
    Parse Yahoo's batting table into:
    [AB, R, H, RBI, 2B, 3B, HR, GIDP, BB, HBP, K, SH, SF, SB, CS, E].

    Yahoo's current total row has no GIDP or CS. GIDP/SF/2B/3B are counted from
    per-plate-appearance text, and CS is left as 0 because it is not exposed.
    """

    cells = tbl.find_all(class_="bb-statsTable__data--result")
    events = _batting_event_counts(tbl)

    def s(i):
        try:
            return int(cells[i].text.strip())
        except Exception:
            return 0

    return [
        s(1),  # AB (打數)
        s(2),  # R (得分)
        s(3),  # H (安打)
        s(4),  # RBI (打點)
        events["2B"],
        events["3B"],
        s(11),  # HR (全壘打)
        events["GIDP"],
        s(6),  # BB (四壞球)
        s(7),  # HBP (死球)
        s(5),  # K (被三振)
        s(8),  # SH (犧牲短打)
        events["SF"],
        s(9),  # SB (盜壘)
        0,  # CS is not exposed by Yahoo's batting table.
        s(10),  # E (失誤)
    ]


def _parse_official_caught_stealing(html: str) -> dict[str, int]:
    """
    Count caught stealing from NPB.jp play-by-play text.

    The official page marks half-innings as h5 text like "7回表（楽天の攻撃）".
    We only count explicit caught-stealing keywords, avoiding baserunning outs
    that do not clearly say a steal was attempted.
    """

    soup = bs(html, "html.parser")
    progress = soup.find(id="progress")
    if not progress:
        return {"away": 0, "home": 0}

    counts = {"away": 0, "home": 0}
    side = None
    for child in progress.children:
        name = getattr(child, "name", None)
        if not name:
            continue
        text = child.get_text("", strip=True)
        if name == "h5":
            if "回表" in text:
                side = "away"
            elif "回裏" in text:
                side = "home"
            else:
                side = None
            continue
        if side and name == "table":
            counts[side] += len(re.findall(r"盗塁(?:失敗|死)", text))
    return counts


def _official_display_team(name: str) -> str:
    norm = re.sub(r"\s+", "", name)
    for official, raw in OFFICIAL_TEAM_NAME_MAP.items():
        if official in norm:
            return display_team_name(raw)
    raise ValueError(f"Unknown official team name: {name}")


async def _official_playbyplay_map(
    session: aiohttp.ClientSession, date_str: str
) -> dict[tuple[str, str, str], str]:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    month_key = f"{dt.year}-{dt.month:02d}"
    if month_key in _OFFICIAL_PLAYBYPLAY_CACHE:
        return _OFFICIAL_PLAYBYPLAY_CACHE[month_key]

    paths = [
        f"/games/{dt.year}/schedule_{dt.month:02d}_detail.html",
        f"/interleague/{dt.year}/schedule_detail.html",
        f"/climax/{dt.year}/schedule_detail.html",
        f"/nippons/{dt.year}/schedule_detail.html",
    ]
    if dt.month <= 3:
        paths.insert(0, f"/preseason/{dt.year}/schedule_detail.html")

    mapping: dict[tuple[str, str, str], str] = {}
    for path in paths:
        html = await _fetch_once(session, urljoin(NPB_OFFICIAL_BASE_URL, path))
        if not html:
            continue
        soup = bs(html, "html.parser")
        current_date = ""
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            if th and re.search(r"\d+/\d+（", th.get_text(" ", strip=True)):
                current_date = th.get_text(" ", strip=True)

            score_link = tr.find(
                "a", href=lambda h: h and f"/scores/{dt.year}/" in h
            )
            team1 = tr.find("div", class_="team1")
            team2 = tr.find("div", class_="team2")
            if not current_date or not score_link or not team1 or not team2:
                continue
            m = re.match(r"(\d{1,2})/(\d{1,2})", current_date)
            if not m:
                continue
            try:
                home = _official_display_team(team1.get_text(" ", strip=True))
                away = _official_display_team(team2.get_text(" ", strip=True))
            except ValueError:
                continue
            key = (f"{dt.year}/{int(m.group(1))}/{int(m.group(2))}", away, home)
            score_url = urljoin(NPB_OFFICIAL_BASE_URL, score_link["href"])
            mapping[key] = urljoin(score_url.rstrip("/") + "/", "playbyplay.html")

        for a in soup.find_all(
            "a", href=lambda h: h and f"/scores/{dt.year}/" in h
        ):
            href = a["href"]
            m = re.search(
                rf"/scores/{dt.year}/(\d{{2}})(\d{{2}})/([a-z]+)-([a-z]+)-\d+/",
                href,
            )
            if not m:
                continue
            home = OFFICIAL_TEAM_CODE_MAP.get(m.group(3))
            away = OFFICIAL_TEAM_CODE_MAP.get(m.group(4))
            if not home or not away:
                continue
            key = (f"{dt.year}/{int(m.group(1))}/{int(m.group(2))}", away, home)
            score_url = urljoin(NPB_OFFICIAL_BASE_URL, href)
            mapping.setdefault(
                key, urljoin(score_url.rstrip("/") + "/", "playbyplay.html")
            )

    _OFFICIAL_PLAYBYPLAY_CACHE[month_key] = mapping
    return mapping


async def _official_caught_stealing_for_game(
    session: aiohttp.ClientSession, date_str: str, away_raw: str, home_raw: str
) -> dict[str, int]:
    mapping = await _official_playbyplay_map(session, date_str)
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    key = (
        f"{dt.year}/{dt.month}/{dt.day}",
        display_team_name(away_raw),
        display_team_name(home_raw),
    )
    url = mapping.get(key)
    if not url:
        return {"away": 0, "home": 0}
    html = await _fetch_once(session, url)
    return _parse_official_caught_stealing(html or "")


async def get_sailu_game_data(
    game_id: str, session: aiohttp.ClientSession
) -> Optional[dict]:
    """
    Scrape a finished game's full box score from Yahoo Baseball and return a
    dict whose keys map directly to 賽錄 columns.  Returns None on any failure.
    """
    # Fetch both /stats and /top pages concurrently
    stats_html, top_html = await asyncio.gather(
        _fetch(session, f"{BASE_URL}game/{game_id}/stats"),
        _fetch(session, f"{BASE_URL}game/{game_id}/top"),
    )
    if not stats_html:
        return None
    soup = bs(stats_html, "html.parser")
    top_soup = bs(top_html, "html.parser") if top_html else None

    # ── Teams ──────────────────────────────────────────────────────────────
    teams_els = soup.find_all(class_="bb-gameScoreTable__team")
    if len(teams_els) < 2:
        return None
    away_raw = teams_els[0].text.strip()
    home_raw = teams_els[1].text.strip()
    if away_raw not in NPB_TEAMS or home_raw not in NPB_TEAMS:
        return None

    # ── Date ───────────────────────────────────────────────────────────────
    title = soup.find("title")
    if not title:
        return None
    m = re.search(r"(\d+年\d{1,2}月\d{1,2}日)", title.text)
    if not m:
        return None
    date_str = datetime.strptime(m.group(1), "%Y年%m月%d日").strftime("%Y-%m-%d")

    # ── Venue ──────────────────────────────────────────────────────────────
    venue_el = soup.find(class_="bb-gameRound--stadium")
    venue = venue_el.text.strip() if venue_el else ""

    # ── Game time (from /top page) ─────────────────────────────────────────
    game_time = ""
    if top_soup:
        for txt_node in top_soup.find_all(string=re.compile(r"\d{1,2}:\d{2}")):
            stripped = txt_node.strip()
            if re.match(r"^\d{1,2}:\d{2}$", stripped):
                game_time = stripped
                break

    # ── Umpire / 球審 (from /top page) ────────────────────────────────────
    umpire = ""
    if top_soup:
        judge_el = top_soup.find(class_="bb-tableLeft__head--judge")
        if judge_el:
            tr = judge_el.find_parent("tr")
            if tr:
                data_el = tr.find(class_="bb-tableLeft__data")
                if data_el:
                    umpire = data_el.text.strip()

    # ── Per-inning scores, R / H / E ───────────────────────────────────────
    away_innings: list = [""] * 12
    home_innings: list = [""] * 12
    away_r = away_h = away_e = 0
    home_r = home_h = home_e = 0

    score_table = soup.find(class_="bb-gameScoreTable")
    if score_table:
        score_rows = score_table.find_all(class_="bb-gameScoreTable__row")
        for row_idx, row in enumerate(score_rows[:2]):
            innings = away_innings if row_idx == 0 else home_innings
            # Per-inning scores are on <a> tags with class bb-gameScoreTable__score
            inning_cells = row.find_all(class_="bb-gameScoreTable__score")
            for i, cell in enumerate(inning_cells[:12]):
                raw = cell.text.strip()
                if raw in ("", "-"):
                    innings[i] = ""
                elif raw == "×":
                    innings[i] = "×"  # unplayed inning
                else:
                    innings[i] = re.sub(r"[×Xx]+$", "", raw)  # strip walk-off marker
            # R total
            total_el = row.find(class_="bb-gameScoreTable__total")
            # H total
            hits_el = row.find(class_="bb-gameScoreTable__data--hits")
            # E total
            error_el = row.find(class_="bb-gameScoreTable__data--loss")
            try:
                r_val = int(total_el.text.strip()) if total_el else 0
            except ValueError:
                r_val = 0
            try:
                h_val = int(hits_el.text.strip()) if hits_el else 0
            except ValueError:
                h_val = 0
            try:
                e_val = int(error_el.text.strip()) if error_el else 0
            except ValueError:
                e_val = 0
            if row_idx == 0:
                away_r, away_h, away_e = r_val, h_val, e_val
            else:
                home_r, home_h, home_e = r_val, h_val, e_val

    # ── Starting pitcher stats (from /stats page) ──────────────────────────
    # pitch_tables[0] = away pitchers, pitch_tables[1] = home pitchers
    away_starter = home_starter = ""
    away_ip = home_ip = ""
    away_er = home_er = 0
    away_qs = home_qs = 0

    for p_idx, ptbl in enumerate(soup.find_all(class_="bb-scoreTable")[:2]):
        rows = ptbl.find_all(class_="bb-scoreTable__row")
        if not rows:
            continue
        row = rows[0]  # starter is always first row

        # Name — strip any (右)/(左) suffix that may appear
        name_el = row.find(class_="bb-scoreTable__data--player")
        raw_name = name_el.text.strip() if name_el else ""
        name = re.sub(r"\s*[（(][右左][）)]\s*", "", raw_name).strip()

        # score cells current format: [ERA, IP, PC, Str, BF, H, HR, BB, HBP, SO, …, R, ER]
        # [0]=ERA, [1]=IP, [-2]=R, [-1]=ER (positions are format-independent)
        score_cells = row.find_all(class_="bb-scoreTable__data--score")
        ip = score_cells[1].text.strip() if len(score_cells) > 1 else ""
        try:
            er = int(score_cells[-1].text) if score_cells else 0
        except ValueError:
            er = 0

        # QS: 7+ IP & <=3 ER, or 6+ IP & <=2 ER, or 5+ IP & <=1 ER.
        try:
            ip_parts = str(ip).split(".")
            outs = int(ip_parts[0]) * 3 + (int(ip_parts[1]) if len(ip_parts) > 1 else 0)
        except Exception:
            outs = 0
        qs = (
            1
            if (
                (outs >= 21 and er <= 3)
                or (outs >= 18 and er <= 2)
                or (outs >= 15 and er <= 1)
            )
            else 0
        )

        if p_idx == 0:
            away_starter, away_ip, away_er, away_qs = name, ip, er, qs
        else:
            home_starter, home_ip, home_er, home_qs = name, ip, er, qs

    # ── Pitcher handedness (from /top page bb-splitsTable) ─────────────────
    away_hand = home_hand = ""
    if top_soup:
        for splits_tbl in top_soup.find_all(class_="bb-splitsTable"):
            for row in splits_tbl.find_all(class_="bb-splitsTable__row"):
                cells = row.find_all(["th", "td"])
                if len(cells) < 4:
                    continue
                if cells[0].text.strip() == "先発" and cells[1].text.strip() == "投":
                    pitcher_name = cells[2].text.strip()
                    handedness = cells[3].text.strip()
                    # Match to away or home starter by name
                    if (
                        away_starter
                        and pitcher_name in away_starter
                        or away_starter in pitcher_name
                    ):
                        away_hand = handedness
                    elif (
                        home_starter
                        and pitcher_name in home_starter
                        or home_starter in pitcher_name
                    ):
                        home_hand = handedness

    return {
        "賽事編號": game_id,
        "客場隊伍": away_raw,
        "客場先發": away_starter,
        "主場隊伍": home_raw,
        "主場先發": home_starter,
        "時間": game_time,
        "球場": venue,
        "主審": umpire,
        "away_innings": away_innings,
        "home_innings": home_innings,
        "客總分": away_r,
        "客安打": away_h,
        "客失誤": away_e,
        "主總": home_r,
        "主安打": home_h,
        "主失誤": home_e,
        "賽事狀態": "正常",
        "日期": date_str,
        "客隊代號": NPB_TEAMS[away_raw]["id"],
        "主隊代號": NPB_TEAMS[home_raw]["id"],
        "客投別": away_hand,
        "主投別": home_hand,
        "客投局": away_ip,
        "主投局": home_ip,
        "客責失": away_er,
        "客QS": away_qs,
        "主責失": home_er,
        "主QS": home_qs,
    }


async def get_schedule_game_data(
    game_id: str, session: aiohttp.ClientSession, *, retry: bool = True
) -> Optional[dict]:
    """
    Scrape a finished game's full box score for the 賽程 sheet.
    Extends get_sailu_game_data with full pitching stats (starter + total) and
    full batting stats per team.  Returns None on any failure.
    """
    fetch = _fetch if retry else _fetch_once
    stats_html, top_html = await asyncio.gather(
        fetch(session, f"{BASE_URL}game/{game_id}/stats"),
        fetch(session, f"{BASE_URL}game/{game_id}/top"),
    )
    if not stats_html:
        return None
    soup = bs(stats_html, "html.parser")
    top_soup = bs(top_html, "html.parser") if top_html else None

    # ── Teams ──────────────────────────────────────────────────────────────
    teams_els = soup.find_all(class_="bb-gameScoreTable__team")
    if len(teams_els) < 2:
        return None
    away_raw = teams_els[0].text.strip()
    home_raw = teams_els[1].text.strip()
    if away_raw not in NPB_TEAMS or home_raw not in NPB_TEAMS:
        return None

    away_name = NPB_TEAMS[away_raw]["name"]
    home_name = NPB_TEAMS[home_raw]["name"]

    # ── Date ───────────────────────────────────────────────────────────────
    title = soup.find("title")
    if not title:
        return None
    m = re.search(r"(\d+年\d{1,2}月\d{1,2}日)", title.text)
    if not m:
        return None
    date_str = datetime.strptime(m.group(1), "%Y年%m月%d日").strftime("%Y-%m-%d")

    # ── Venue ──────────────────────────────────────────────────────────────
    venue_el = soup.find(class_="bb-gameRound--stadium")
    venue_raw = venue_el.text.strip() if venue_el else ""
    field = _display_field_name(venue_raw)

    # ── Game time ──────────────────────────────────────────────────────────
    game_time = ""
    if top_soup:
        for txt_node in top_soup.find_all(string=re.compile(r"\d{1,2}:\d{2}")):
            stripped = txt_node.strip()
            if re.match(r"^\d{1,2}:\d{2}$", stripped):
                game_time = stripped
                break

    # ── Umpire ────────────────────────────────────────────────────────────
    umpire = ""
    if top_soup:
        judge_el = top_soup.find(class_="bb-tableLeft__head--judge")
        if judge_el:
            tr = judge_el.find_parent("tr")
            if tr:
                data_el = tr.find(class_="bb-tableLeft__data")
                if data_el:
                    umpire = data_el.text.strip()

    # ── Per-inning scores ─────────────────────────────────────────────────
    away_innings: list = [""] * 12
    home_innings: list = [""] * 12
    away_r = away_h = away_e = 0
    home_r = home_h = home_e = 0

    score_table = soup.find(class_="bb-gameScoreTable")
    if score_table:
        score_rows = score_table.find_all(class_="bb-gameScoreTable__row")
        for row_idx, row in enumerate(score_rows[:2]):
            innings = away_innings if row_idx == 0 else home_innings
            inning_cells = row.find_all(class_="bb-gameScoreTable__score")
            for i, cell in enumerate(inning_cells[:12]):
                raw = cell.text.strip()
                if raw in ("", "-"):
                    innings[i] = ""
                elif raw == "×":
                    innings[i] = "×"  # unplayed inning
                else:
                    innings[i] = re.sub(r"[×Xx]+$", "", raw)  # strip walk-off marker
            total_el = row.find(class_="bb-gameScoreTable__total")
            hits_el = row.find(class_="bb-gameScoreTable__data--hits")
            error_el = row.find(class_="bb-gameScoreTable__data--loss")

            def _si(el, default=0):
                try:
                    return int(el.text.strip()) if el else default
                except ValueError:
                    return default

            if row_idx == 0:
                away_r, away_h, away_e = _si(total_el), _si(hits_el), _si(error_el)
            else:
                home_r, home_h, home_e = _si(total_el), _si(hits_el), _si(error_el)

    # ── Pitching stats ─────────────────────────────────────────────────────
    # pitch_tables[0]=away pitchers, [1]=home pitchers
    # Yahoo Baseball current cell order (score cells only, 12 cells):
    #   ERA(0), IP(1), PC(2), BF(3), H(4), HR(5), SO(6), BB(7), HBP(8), BK(9), R(10), ER(11)
    # No Str (好球數) or WP (暴投) column — stats[3] and stats[9] stay 0.
    # Stats array order for 賽程 sheet (13 values, indices 0-12):
    #   [IP, BF, PC, Str, H, HR, BB, HBP, SO, WP, BK, R, ER]
    def _parse_pitch_block(ptbl):
        def _zero():
            return [0] * 13

        def _ip_str(outs):
            full, rem = divmod(outs, 3)
            if rem == 0:
                return str(full)
            return f"{full}.3333" if rem == 1 else f"{full}.6667"

        def _parse_outs(ip_raw):
            try:
                parts = str(ip_raw).strip().split(".")
                return int(parts[0]) * 3 + (int(parts[1]) if len(parts) > 1 else 0)
            except Exception:
                return 0

        def _safe(cell):
            try:
                return int(cell.text.strip())
            except Exception:
                return 0

        def _accumulate(stats, cells):
            """Accumulate stats from one pitcher row into stats[].
            Uses += throughout so it works for both single-pitcher (starter)
            and multi-pitcher (total) aggregation."""
            n = len(cells)
            if n >= 11:
                # Current 12-cell format: ERA(0), IP(1), PC(2), BF(3), H(4), HR(5),
                #   SO(6), BB(7), HBP(8), BK(9), R(10), ER(11)
                stats[2] += _safe(cells[2])  # PC  (投球數)
                stats[1] += _safe(cells[3])  # BF  (打席)
                stats[4] += _safe(cells[4])  # H   (被安打)
                stats[5] += _safe(cells[5])  # HR  (被HR)
                stats[8] += _safe(cells[6])  # SO  (三振)
                stats[6] += _safe(cells[7])  # BB  (四球)
                stats[7] += _safe(cells[8])  # HBP (死球)
                if n >= 12:
                    stats[10] += _safe(cells[9])  # BK  (ボーク)
            elif n >= 10:
                # Older 10-cell format: ERA, IP, BF, H, HR, BB, HBP, SO, R, ER
                stats[1] += _safe(cells[2])  # BF
                stats[4] += _safe(cells[3])  # H
                stats[5] += _safe(cells[4])  # HR
                stats[6] += _safe(cells[5])  # BB
                stats[7] += _safe(cells[6])  # HBP
                stats[8] += _safe(cells[7])  # SO
            # R and ER are always the last two cells regardless of format
            if n >= 10:
                stats[11] += _safe(cells[-2])  # R  (失分)
                stats[12] += _safe(cells[-1])  # ER (自責分)

        rows = ptbl.find_all(class_="bb-scoreTable__row")
        if not rows:
            return _zero(), _zero(), ""

        s_stats = _zero()
        t_stats = _zero()
        starter_name = ""
        total_outs = 0

        for i, row in enumerate(rows):
            cells = row.find_all(class_="bb-scoreTable__data--score")
            if len(cells) < 2:
                continue
            outs = _parse_outs(cells[1].text.strip())
            total_outs += outs

            if i == 0:
                name_el = row.find(class_="bb-scoreTable__data--player")
                if name_el:
                    starter_name = re.sub(
                        r"\s*[（(][右左][）)]\s*", "", name_el.text.strip()
                    ).strip()
                s_stats[0] = _ip_str(outs)
                _accumulate(s_stats, cells)  # starter only

            _accumulate(t_stats, cells)  # all pitchers → total

        t_stats[0] = _ip_str(total_outs)
        return s_stats, t_stats, starter_name

    away_s_pitch = [0] * 13
    away_t_pitch = [0] * 13
    home_s_pitch = [0] * 13
    home_t_pitch = [0] * 13
    away_starter = home_starter = ""

    pitch_tables = soup.find_all(class_="bb-scoreTable")[:2]
    if len(pitch_tables) >= 1:
        away_s_pitch, away_t_pitch, away_starter = _parse_pitch_block(pitch_tables[0])
    if len(pitch_tables) >= 2:
        home_s_pitch, home_t_pitch, home_starter = _parse_pitch_block(pitch_tables[1])

    # ── QS ─────────────────────────────────────────────────────────────────
    def _qs(s):
        ip_str = str(s[0])
        ip_full = int(ip_str.split(".")[0]) if ip_str and ip_str[0].isdigit() else 0
        return 1 if ip_full >= 6 and s[12] <= 3 else 0

    away_qs = _qs(away_s_pitch)
    home_qs = _qs(home_s_pitch)

    # ── Pitcher handedness ─────────────────────────────────────────────────
    away_hand = home_hand = ""
    if top_soup:
        for splits_tbl in top_soup.find_all(class_="bb-splitsTable"):
            for row in splits_tbl.find_all(class_="bb-splitsTable__row"):
                cells = row.find_all(["th", "td"])
                if len(cells) < 4:
                    continue
                if cells[0].text.strip() == "先発" and cells[1].text.strip() == "投":
                    pitcher_name = cells[2].text.strip()
                    handedness = cells[3].text.strip()
                    if away_starter and (
                        pitcher_name in away_starter or away_starter in pitcher_name
                    ):
                        away_hand = handedness
                    elif home_starter and (
                        pitcher_name in home_starter or home_starter in pitcher_name
                    ):
                        home_hand = handedness

    # ── Batting stats ──────────────────────────────────────────────────────
    # bb-statsTable[0]=away, [1]=home
    bat_tables = soup.find_all(class_="bb-statsTable")
    away_bat = _parse_batting_table(bat_tables[0]) if len(bat_tables) > 0 else [0] * 16
    home_bat = _parse_batting_table(bat_tables[1]) if len(bat_tables) > 1 else [0] * 16
    # Batting table doesn't expose fielding errors; use scoreboard totals (same as col X/AM)
    away_bat[15] = away_e
    home_bat[15] = home_e
    caught_stealing = await _official_caught_stealing_for_game(
        session, date_str, away_raw, home_raw
    )
    away_bat[14] = caught_stealing["away"]
    home_bat[14] = caught_stealing["home"]

    return {
        "賽事編號": game_id,
        "日期": date_str,
        "客隊原名": away_raw,
        "客隊": away_name,
        "客隊先發": away_starter,
        "主隊原名": home_raw,
        "主隊": home_name,
        "主隊先發": home_starter,
        "球場原名": venue_raw,
        "球場": field,
        "主審": umpire,
        "時間": game_time,
        "away_innings": away_innings,
        "home_innings": home_innings,
        "客總分": away_r,
        "客總安打": away_h,
        "客總失誤": away_e,
        "主總分": home_r,
        "主總安打": home_h,
        "主總失誤": home_e,
        "客先發投球": away_s_pitch,  # list[13]
        "客總投球": away_t_pitch,  # list[13]
        "主先發投球": home_s_pitch,  # list[13]
        "主總投球": home_t_pitch,  # list[13]
        "客投別": away_hand,
        "主投別": home_hand,
        "客打擊": away_bat,  # list[16]
        "主打擊": home_bat,  # list[16]
        "客QS": away_qs,
        "主QS": home_qs,
    }


def _schedule_row(seq: int, data: dict) -> list:
    """
    Convert a game data dict (from get_schedule_game_data) into a 125-value row
    covering columns A–DU of the 賽程 sheet.  Columns DV onwards are formula-driven
    in the sheet and are intentionally left untouched.

    Columns not available from Yahoo Baseball (投球數, 好球數, 暴投, 犯規) are 0.
    Row layout (1-indexed columns):
      A(1)         賽事編號  ← game ID goes here
      B(2)         場次
      C(3)         日期
      D–I(4–9)     teams / field / umpire
      J–U(10–21)   客1–12
      V–X(22–24)   客總分 / 客總安打 / 客總失誤
      Y–AJ(25–36)  主1–12
      AK–AM(37–39) 主總分 / 主總安打 / 主總失誤
      AN–AZ(40–52) 客先發投球 (13)
      BA–BM(53–65) 客總投球 (13)
      BN–BZ(66–78) 主先發投球 (13)
      CA–CM(79–91) 主總投球 (13)
      CN–CO(92–93) 客投左右 / 主投左右
      CP–DE(94–109) 客打擊 (16)
      DF–DU(110–125) 主打擊 (16)
    """
    ai = data["away_innings"]
    hi = data["home_innings"]

    asp = data["客先發投球"]  # list[13]
    atp = data["客總投球"]  # list[13]
    hsp = data["主先發投球"]  # list[13]
    htp = data["主總投球"]  # list[13]
    ab = data["客打擊"]  # list[16]
    hb = data["主打擊"]  # list[16]

    return [
        data["賽事編號"],  # A  賽事編號
        seq,  # B  場次
        data["日期"],  # C  日期
        data["客隊"],  # D  客隊
        data["客隊先發"],  # E  客隊先發
        data["主隊"],  # F  主隊
        data["主隊先發"],  # G  主隊先發
        data["球場"],  # H  球場
        data["主審"],  # I  主審
        *ai,  # J–U  客1–12
        data["客總分"],  # V   客總分
        data["客總安打"],  # W   客總安打
        data["客總失誤"],  # X   安總失誤
        *hi,  # Y–AJ 主1–12
        data["主總分"],  # AK  主總分
        data["主總安打"],  # AL  主總安打
        data["主總失誤"],  # AM  主總失誤
        *asp,  # AN–AZ 客先發投球 (13)
        *atp,  # BA–BM 客總投球 (13)
        *hsp,  # BN–BZ 主先發投球 (13)
        *htp,  # CA–CM 主總投球 (13)
        data["客投別"],  # CN  客投左右
        data["主投別"],  # CO  主投左右
        *ab,  # CP–DE 客打擊 (16)
        *hb,  # DF–DU 主打擊 (16)
    ]  # 125 values total — DV onwards are formula columns, left untouched


def _analysis_date(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"{dt.year}/{dt.month}/{dt.day}"


def _analysis_game_type(data: dict) -> str:
    away = data.get("客隊原名", "")
    home = data.get("主隊原名", "")
    if not away or not home:
        return "例行賽"
    return (
        "交流賽" if NPB_TEAMS[away]["league"] != NPB_TEAMS[home]["league"] else "例行賽"
    )


def _analysis_day_night(game_time: str) -> str:
    m = re.match(r"^(\d{1,2}):(\d{2})$", str(game_time or ""))
    if not m:
        return ""
    return "夜" if int(m.group(1)) >= 17 else "日"


def _analysis_team_name(team_name: str) -> str:
    return display_team_name(team_name)


def _analysis_field(data: dict) -> str:
    raw = data.get("球場原名") or data.get("球場") or ""
    return ANALYSIS_FIELDS.get(raw, "地方球場" if raw else "")


def _analysis_hand(hand: str) -> str:
    if not hand:
        return ""
    return hand if hand.endswith("投") else f"{hand}投"


def _analysis_marks(away_score: int, home_score: int) -> tuple[str, str]:
    if away_score > home_score:
        return "○", "●"
    if away_score < home_score:
        return "●", "○"
    return "△", "△"


def _analysis_innings(vals: list) -> tuple[list, str]:
    innings = ["" if str(v) in ("", "×") else v for v in vals[:9]]
    extras = []
    for v in vals[9:12]:
        if str(v).isdigit():
            extras.append(int(v))
    return innings, (sum(extras) if extras else "")


def _analysis_total_bases(batting: list) -> int:
    hits = int(batting[2] or 0)
    doubles = int(batting[4] or 0)
    triples = int(batting[5] or 0)
    homers = int(batting[6] or 0)
    return hits + doubles + triples * 2 + homers * 3


def _analysis_long_hits(batting: list) -> int:
    return int(batting[4] or 0) + int(batting[5] or 0) + int(batting[6] or 0)


def _analysis_qs(starter_pitch: list):
    ip_raw = str(starter_pitch[0] or "")
    try:
        parts = ip_raw.split(".")
        partial = 0
        if len(parts) > 1:
            frac = parts[1]
            if frac.startswith("3333"):
                partial = 1
            elif frac.startswith("6667"):
                partial = 2
            else:
                partial = int(frac[:1] or 0)
        outs = int(parts[0]) * 3 + partial
    except (TypeError, ValueError):
        outs = 0
    try:
        earned_runs = int(starter_pitch[12] or 0)
    except (TypeError, ValueError):
        earned_runs = 0

    if outs >= 21 and earned_runs <= 3:
        return "QS"
    if outs >= 18 and earned_runs <= 2:
        return "QS"
    if outs >= 15 and earned_runs <= 1:
        return "QS"
    return "x"


def _analysis_starter_block(starter_pitch: list) -> list:
    return [
        starter_pitch[0],  # 局數
        starter_pitch[1],  # 打數 / faced batters
        starter_pitch[4],  # 安打
        starter_pitch[5],  # HR
        starter_pitch[6] + starter_pitch[7],  # 四球 + 死球
        starter_pitch[11],  # 失点
        starter_pitch[12],  # 責失
        starter_pitch[4] + starter_pitch[5] * 3,  # 被壘打, minimum from H/HR
        _analysis_qs(starter_pitch),  # QS
    ]


def _analysis_team_total_block(
    opposing_pitch: list,
    opposing_batting: list,
    own_batting: list,
    score: int,
    earned_runs: int,
    errors: int,
) -> list:
    return [
        opposing_pitch[0],  # 局数
        opposing_pitch[2],  # 用球数
        opposing_batting[0],  # 打 数
        opposing_batting[2],  # 安打
        opposing_batting[6],  # HR
        opposing_batting[10],  # 三振
        opposing_batting[8] + opposing_batting[9],  # 四死
        score,  # 失点 / 得点 from this team's view
        earned_runs,
        errors,
        own_batting[7],  # 併打
        own_batting[13],  # 盜壘
        own_batting[14],  # 盜壘刺
        _analysis_total_bases(own_batting),
        _analysis_long_hits(own_batting),
    ]


def _analysis_row(seq: int, data: dict) -> list:
    away_score = int(data["客總分"])
    home_score = int(data["主總分"])
    away_mark, home_mark = _analysis_marks(away_score, home_score)
    away_innings, away_ot = _analysis_innings(data["away_innings"])
    home_innings, home_ot = _analysis_innings(data["home_innings"])

    away_bat = data["客打擊"]
    home_bat = data["主打擊"]
    away_starter_view = _analysis_starter_block(data["客先發投球"])
    home_starter_view = _analysis_starter_block(data["主先發投球"])
    away_total_view = _analysis_team_total_block(
        data["客總投球"],
        home_bat,
        away_bat,
        home_score,
        data["客總投球"][12],
        data["客總失誤"],
    )
    home_total_view = _analysis_team_total_block(
        data["主總投球"],
        away_bat,
        home_bat,
        away_score,
        data["主總投球"][12],
        data["主總失誤"],
    )

    return [
        seq,
        _analysis_date(data["日期"]),
        _analysis_day_night(data.get("時間", "")),
        _analysis_game_type(data),
        data["主審"],
        _analysis_hand(data["客投別"]),
        _analysis_hand(data["主投別"]),
        away_mark,
        _analysis_team_name(data.get("客隊原名", data["客隊"])),
        away_score,
        home_score,
        _analysis_team_name(data.get("主隊原名", data["主隊"])),
        home_mark,
        _analysis_field(data),
        *away_innings,
        away_ot,
        *home_innings,
        home_ot,
        *away_starter_view,
        *away_total_view,
        "",
        *home_starter_view,
        *home_total_view,
    ]


def _sailu_row(seq: int, data: dict) -> list:
    """
    Convert a game data dict to a 賽錄 sheet row covering columns A–AY (51 values).
    Columns AZ onwards are all formula-driven in the sheet and are left untouched.
    """
    ai = data["away_innings"]
    hi = data["home_innings"]
    return [
        seq,  # A   編號
        data["賽事編號"],  # B   賽事編號
        data["客場隊伍"],  # C   客場隊伍
        data["客場先發"],  # D   客場先發
        data["主場隊伍"],  # E   主場隊伍
        data["主場先發"],  # F   主場先發
        data["時間"],  # G   時間
        data["球場"],  # H   球場
        data["主審"],  # I   主審
        ai[0],
        ai[1],
        ai[2],
        ai[3],  # J–M  客1–4
        ai[4],
        ai[5],
        ai[6],
        ai[7],  # N–Q  客5–8
        ai[8],
        ai[9],
        ai[10],
        ai[11],  # R–U  客9–12
        data["客總分"],  # V   客總分
        data["客安打"],  # W   客安打
        data["客失誤"],  # X   客失誤
        hi[0],
        hi[1],
        hi[2],
        hi[3],  # Y–AB 主1–4
        hi[4],
        hi[5],
        hi[6],
        hi[7],  # AC–AF 主5–8
        hi[8],
        hi[9],
        hi[10],
        hi[11],  # AG–AJ 主9–12
        data["主總"],  # AK  主總
        data["主安打"],  # AL  主安打
        data["主失誤"],  # AM  主失誤
        data["賽事狀態"],  # AN  賽事狀態
        data["日期"],  # AO  日期
        data["客隊代號"],  # AP  客隊代號
        data["主隊代號"],  # AQ  主隊代號
        data["客投別"],  # AR  客投別
        data["主投別"],  # AS  主投別
        data["客投局"],  # AT  客投局
        data["主投局"],  # AU  主投局
        data["客責失"],  # AV  客責失
        data["客QS"],  # AW  客QS
        data["主責失"],  # AX  主責失
        data["主QS"],  # AY  主QS
    ]


def _sailu_formula_row(row_num: int) -> list[str]:
    """Build AZ:BT formula cells for one 賽錄 row."""
    return [
        f"=SUM(J{row_num}:L{row_num})",
        f"=SUM(Y{row_num}:AA{row_num})",
        f"=SUM(J{row_num}:N{row_num})",
        f"=SUM(Y{row_num}:AC{row_num})",
        f"=SUM(J{row_num}:O{row_num})",
        f"=SUM(Y{row_num}:AD{row_num})",
        f"=SUM(J{row_num}:P{row_num})",
        f"=SUM(Y{row_num}:AE{row_num})",
        '=IF(客總分="","",IF(客總分=主總分,"平",IF(客總分>主總分,"勝","敗")))',
        '=IF(BH{0}="","",IF(BH{0}="平","平",IF(BH{0}="勝","敗","勝")))'.format(row_num),
        '=IF(BH{0}="勝",客總分-主總分,IF(BH{0}="敗",主總分-客總分,0))'.format(row_num),
        '=IF(MOD(AT{0},1)=0,AT{0},IF(RIGHT(AT{0},1)="1",(AT{0}-0.1)+1/3,(AT{0}-0.2)+2/3))'.format(
            row_num
        ),
        '=IF(MOD(AU{0},1)=0,AU{0},IF(RIGHT(AU{0},1)="1",(AU{0}-0.1)+1/3,(AU{0}-0.2)+2/3))'.format(
            row_num
        ),
        '=IF(客總分="","",客總5+主總5)',
        '=IF(客總分="","",客總分+主總分)',
        f'=IF(J{row_num}="","",SUM(J{row_num}:R{row_num}))',
        f'=IF(J{row_num}="","",SUM(Y{row_num}:AG{row_num}))',
        f'=IF(S{row_num}="","",SUM(S{row_num}:U{row_num}))',
        f'=IF(AH{row_num}="","",SUM(AH{row_num}:AJ{row_num}))',
        '=IF(AO{0}="","",IF(AND(客先局>=5,主總7<=3,主總6<=2,主總5<=1),1,IF(AND(客先局>=5,主總6<=2,主總5<=1),1,IF(AND(客先局>=5,主總5<=1),1,""))))'.format(
            row_num
        ),
        '=IF(AO{0}="","",IF(AND(主先局>=5,客總7<=3,客總6<=2,客總5<=1),1,IF(AND(主先局>=5,客總6<=2,客總5<=1),1,IF(AND(主先局>=5,客總5<=1),1,""))))'.format(
            row_num
        ),
    ]


def _chunked(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _placeholder_rows(sheet) -> list[int]:
    col_a = sheet.col_values(1)[1:]
    col_b = sheet.col_values(2)[1:]
    return [
        i + 2
        for i, a in enumerate(col_a)
        if a and not (col_b[i] if i < len(col_b) else "")
    ]


def _ensure_target_sailu_capacity(sheet, needed_rows: int) -> list[int]:
    """Extend target 賽錄 with numbered placeholder rows and formulas if needed."""
    placeholder_rows = _placeholder_rows(sheet)
    if len(placeholder_rows) >= needed_rows:
        return placeholder_rows

    missing = needed_rows - len(placeholder_rows)
    start_row = sheet.row_count + 1
    sheet.add_rows(missing)

    prev_seq = int(sheet.acell(f"A{start_row - 1}").value)
    seq_values = [[prev_seq + offset + 1] for offset in range(missing)]
    formula_values = [
        _sailu_formula_row(row_num) for row_num in range(start_row, start_row + missing)
    ]

    for offset, chunk in enumerate(_chunked(seq_values, 200)):
        chunk_start = start_row + offset * 200
        chunk_end = chunk_start + len(chunk) - 1
        sheet.update(
            f"A{chunk_start}:A{chunk_end}",
            chunk,
            value_input_option="USER_ENTERED",
        )

    for offset, chunk in enumerate(_chunked(formula_values, 200)):
        chunk_start = start_row + offset * 200
        chunk_end = chunk_start + len(chunk) - 1
        sheet.update(
            f"AZ{chunk_start}:BT{chunk_end}",
            chunk,
            value_input_option="USER_ENTERED",
        )

    return _placeholder_rows(sheet)


def _write_regular_sailu_games(
    sheet,
    games: list[tuple[str, dict]],
    *,
    auto_extend_target: bool = False,
):
    """Write regular-season 賽錄 rows into placeholder rows, optionally extending them."""
    if not games:
        return 0, []

    placeholder_rows = (
        _ensure_target_sailu_capacity(sheet, len(games))
        if auto_extend_target
        else _placeholder_rows(sheet)
    )

    filled = 0
    for (gid, data), row_num in zip(games, placeholder_rows):
        row_values = _sailu_row(0, data)[1:]  # drop col A; keep existing sequence
        sheet.update(
            f"B{row_num}:AY{row_num}", [row_values], value_input_option="USER_ENTERED"
        )
        print(f"  [sailu] Row {row_num} ← {gid}")
        filled += 1

    return filled, games[len(placeholder_rows) :]


def _exhibition_row(data: dict) -> list[str]:
    """Convert scraped game data into 熱身賽紀錄's compact 28-column layout."""
    away_score = int(data["客總分"])
    home_score = int(data["主總"])
    if away_score > home_score:
        away_mark, home_mark = "○", "●"
    elif away_score < home_score:
        away_mark, home_mark = "●", "○"
    else:
        away_mark = home_mark = "△"

    def _cell(v: str) -> str:
        return "" if v in ("", "×") else str(v)

    def _ot_total(vals: list) -> str:
        nums = [int(v) for v in vals if str(v).isdigit()]
        return str(sum(nums)) if nums else ""

    away_innings = [_cell(v) for v in data["away_innings"][:9]]
    home_innings = [_cell(v) for v in data["home_innings"][:9]]
    away_ot = _ot_total(data["away_innings"][9:12])
    home_ot = _ot_total(data["home_innings"][9:12])
    dt = datetime.strptime(data["日期"], "%Y-%m-%d")

    return [
        f"{dt.year}/{dt.month}/{dt.day}",
        away_mark,
        display_team_name(data["客場隊伍"]),
        str(away_score),
        str(home_score),
        display_team_name(data["主場隊伍"]),
        home_mark,
        data["球場"],
        *away_innings,
        away_ot,
        *home_innings,
        home_ot,
    ]


def _exhibition_identity(data: dict) -> tuple[str, str, str]:
    return (
        data["日期"],
        display_team_name(data["客場隊伍"]),
        display_team_name(data["主場隊伍"]),
    )


def _existing_exhibition_identities(sheet) -> set[tuple[str, str, str]]:
    rows = sheet.get_all_values()[1:]
    identities: set[tuple[str, str, str]] = set()
    for row in rows:
        if len(row) < 6 or not row[0]:
            continue
        try:
            dt = datetime.strptime(row[0], "%Y/%m/%d").strftime("%Y-%m-%d")
        except ValueError:
            continue
        identities.add((dt, row[2], row[5]))
    return identities


async def update_sailu_sheet(session: aiohttp.ClientSession):
    """
    Fill finished games into 賽錄's pre-populated placeholder rows.

    The sheet pre-builds rows with formulas in columns AZ onwards and leaves
    columns B–AY blank as placeholders (column A / 編號 is already set).
    This function detects those placeholders and writes only B–AY into them,
    letting the existing formulas handle everything from AZ onwards.
    """
    print("\n=== 賽錄 update ===")
    sheet = get_worksheet(SAILU_SHEET_NAME, SAILU_SPREADSHEET_KEY)
    target_sheet = get_worksheet(SAILU_SHEET_NAME, SAILU_TARGET_SPREADSHEET_KEY)
    exhibition_sheet = get_worksheet(EXHIBITION_SHEET_NAME, SAILU_SPREADSHEET_KEY)

    # Games already recorded
    existing_ids = set(v for v in sheet.col_values(2)[1:] if v)
    target_existing_ids = set(v for v in target_sheet.col_values(2)[1:] if v)
    existing_exhibition = _existing_exhibition_identities(exhibition_sheet)
    print(
        f"[sailu] {len(_placeholder_rows(sheet))} source placeholder row(s) available."
    )
    print(
        f"[sailu] {len(_placeholder_rows(target_sheet))} target placeholder row(s) available."
    )

    # Collect recently finished game IDs across all teams (last 3 per team)
    all_ids: set[str] = set()
    tasks = {
        key: get_last_n_game_ids(info["id"], 3, session)
        for key, info in NPB_TEAMS.items()
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    for key, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            print(f"  [sailu] get_last_n_game_ids({key}): {result}")
        else:
            all_ids.update(result)

    new_ids = sorted(gid for gid in all_ids if gid not in existing_ids)
    if not new_ids:
        print("[sailu] No new games to add.")
        return []

    print(f"[sailu] {len(new_ids)} new game(s): {new_ids}")

    # Scrape full box score for each new game
    new_games: list[tuple[str, dict]] = []
    for i in range(0, len(new_ids), MAX_CONCURRENT):
        batch = new_ids[i : i + MAX_CONCURRENT]
        scraped = await asyncio.gather(
            *[get_sailu_game_data(gid, session) for gid in batch],
            return_exceptions=True,
        )
        for gid, data in zip(batch, scraped):
            if isinstance(data, Exception):
                print(f"  [sailu] get_sailu_game_data({gid}): {data}")
            elif data:
                new_games.append((gid, data))
            else:
                print(f"  [sailu] No data for {gid} (game may not be finished yet)")
        if i + MAX_CONCURRENT < len(new_ids):
            await asyncio.sleep(2)

    if not new_games:
        print("[sailu] Nothing to write.")
        return []

    new_games.sort(key=lambda x: x[0])  # sort by game ID (encodes date + sequence)

    regular_games = [
        (gid, data) for gid, data in new_games if not is_exhibition_game_id(gid)
    ]
    exhibition_games = [
        (gid, data) for gid, data in new_games if is_exhibition_game_id(gid)
    ]

    source_regular_games = [
        (gid, data) for gid, data in regular_games if gid not in existing_ids
    ]
    target_regular_games = [
        (gid, data) for gid, data in regular_games if gid not in target_existing_ids
    ]

    filled, overflow = _write_regular_sailu_games(sheet, source_regular_games)
    if overflow:
        print(
            f"[sailu] WARNING: {len(overflow)} source game(s) skipped — no placeholder rows left: "
            + str([gid for gid, _ in overflow])
            + "\n  → Add more pre-populated formula rows to 賽錄 and re-run."
        )

    target_filled, target_overflow = _write_regular_sailu_games(
        target_sheet,
        target_regular_games,
        auto_extend_target=True,
    )
    if target_overflow:
        print(
            f"[sailu-target] WARNING: {len(target_overflow)} game(s) skipped: "
            + str([gid for gid, _ in target_overflow])
        )

    exhibition_rows = []
    exhibition_written = 0
    for gid, data in exhibition_games:
        ident = _exhibition_identity(data)
        if ident in existing_exhibition:
            print(f"  [exhibition] skip existing ← {gid}")
            continue
        exhibition_rows.append(_exhibition_row(data))
        existing_exhibition.add(ident)
        exhibition_written += 1

    if exhibition_rows:
        exhibition_sheet.append_rows(
            exhibition_rows,
            value_input_option="USER_ENTERED",
            table_range="A:AB",
        )
        print(
            f"[exhibition] Appended {exhibition_written} row(s) to '{EXHIBITION_SHEET_NAME}'."
        )
    else:
        print("[exhibition] No new games to add.")

    print(
        f"[sailu] Done. Filled {filled} source row(s) and {target_filled} target row(s)."
    )
    source_written_ids = [gid for gid, _ in source_regular_games[:filled]]
    target_written_ids = [gid for gid, _ in target_regular_games[:target_filled]]
    return list(dict.fromkeys(source_written_ids + target_written_ids))


def _analysis_identity(data: dict) -> tuple[str, str, str]:
    return (
        _analysis_date(data["日期"]),
        _analysis_team_name(data.get("客隊原名", data["客隊"])),
        _analysis_team_name(data.get("主隊原名", data["主隊"])),
    )


def _analysis_identity_from_row(row: list[str]) -> tuple[str, str, str] | None:
    if len(row) < 12 or not row[1] or not row[8] or not row[11]:
        return None
    return (row[1], row[8], row[11])


def _analysis_row_year(row: list[str]) -> int | None:
    if len(row) < 2 or not row[1]:
        return None
    try:
        return datetime.strptime(row[1], "%Y/%m/%d").year
    except ValueError:
        return None


def _analysis_row_date(row: list[str]) -> datetime | None:
    if len(row) < 2 or not row[1]:
        return None
    try:
        return datetime.strptime(row[1], "%Y/%m/%d")
    except ValueError:
        return None


def _last_analysis_seq(rows: list[list[str]]) -> int:
    last_seq = 0
    for row in rows[2:]:
        if not row:
            continue
        try:
            last_seq = max(last_seq, int(row[0]))
        except (TypeError, ValueError):
            continue
    return last_seq


def _analysis_insert_index(rows: list[list[str]], date_str: str) -> int:
    """
    Return the 1-based worksheet row where a new analysis row should be inserted.
    Rows 1-2 are headers; data stays sorted by game date.
    """
    game_date = datetime.strptime(date_str, "%Y-%m-%d")
    insert_at = len(rows) + 1
    for row_num, row in enumerate(rows[2:], start=3):
        row_date = _analysis_row_date(row)
        if row_date and row_date > game_date:
            return row_num
        if row_date:
            insert_at = row_num + 1
    return insert_at


def _season_months(year: int) -> list[str]:
    today = datetime.now()
    end_month = today.month if today.year == year else 12
    return [f"{year}-{month:02d}" for month in range(1, end_month + 1)]


async def get_finished_game_ids_for_month(
    team_id: int, month: str, session: aiohttp.ClientSession
) -> set[str]:
    ids: set[str] = set()
    html = await _fetch(session, f"{BASE_URL}teams/{team_id}/schedule?month={month}")
    if not html:
        return ids
    soup = bs(html, "html.parser")
    for entry in soup.find_all(class_="bb-calendarTable__data"):
        status = entry.find(class_="bb-calendarTable__status")
        if not status or status.text.strip() != "試合終了":
            continue
        m = re.search(r"npb/game/([^/]+)", status.get("href", ""))
        if m:
            ids.add(m.group(1))
    return ids


async def get_finished_game_ids_for_season(
    year: int, session: aiohttp.ClientSession
) -> set[str]:
    all_ids: set[str] = set()
    months = _season_months(year)
    for month in months:
        tasks = [
            get_finished_game_ids_for_month(info["id"], month, session)
            for info in NPB_TEAMS.values()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                print(f"  [analysis] schedule scan {month}: {result}")
            else:
                all_ids.update(result)
        await asyncio.sleep(0.5)
    return all_ids


async def get_recent_finished_game_ids(
    session: aiohttp.ClientSession, games_per_team: int = 3
) -> set[str]:
    all_ids: set[str] = set()
    tasks = {
        key: get_last_n_game_ids(info["id"], games_per_team, session)
        for key, info in NPB_TEAMS.items()
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    for key, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            print(f"  [analysis] get_last_n_game_ids({key}): {result}")
        else:
            all_ids.update(result)
    return all_ids


def _date_key(date_value: datetime | str | None = None) -> str:
    if date_value is None:
        return datetime.now().strftime("%Y-%m-%d")
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%d")
    return date_value


def _sailu_game_ids_for_date(date_value: datetime | str | None = None) -> list[str]:
    target_key = _date_key(date_value)
    sheet = get_worksheet(SAILU_SHEET_NAME, SAILU_TARGET_SPREADSHEET_KEY)
    rows = sheet.get_all_values()
    ids: list[str] = []
    for row in rows[1:]:
        if len(row) <= 40 or row[40] != target_key:
            continue
        gid = row[1] if len(row) > 1 else ""
        if gid and gid not in ids:
            ids.append(gid)
    return ids


def _today_sailu_game_ids(today: datetime | None = None) -> list[str]:
    return _sailu_game_ids_for_date(today)


def _sailu_dates_for_game_ids(game_ids: list[str]) -> list[str]:
    if not game_ids:
        return []
    wanted = set(game_ids)
    sheet = get_worksheet(SAILU_SHEET_NAME, SAILU_TARGET_SPREADSHEET_KEY)
    rows = sheet.get_all_values()
    dates: list[str] = []
    for row in rows[1:]:
        if len(row) <= 40 or row[1] not in wanted or not row[40]:
            continue
        if row[40] not in dates:
            dates.append(row[40])
    return sorted(dates)


async def update_analysis_sheet(
    session: aiohttp.ClientSession,
    year: int = ANALYSIS_SEASON,
    *,
    game_ids: list[str] | None = None,
    target_date: datetime | str | None = None,
    full_season: bool = False,
):
    """
    Insert missing finished games into 分析表紀錄.

    The sheet does not store Yahoo game IDs, so duplicate detection uses
    (date, away team, home team), which is stable for NPB regular-season games.
    Daily runs use game IDs already written to the target date's 賽錄 rows;
    full_season=True is only for manual historical repair/backfill.
    """
    print(f"\n=== {ANALYSIS_SHEET_NAME} update ({year}) ===")
    sheet = get_worksheet(ANALYSIS_SHEET_NAME, NPB_SPREADSHEET_KEY)
    rows = sheet.get_all_values()
    season_rows = [row for row in rows[2:] if _analysis_row_year(row) == year]
    existing = {
        ident for row in season_rows if (ident := _analysis_identity_from_row(row))
    }
    last_seq = _last_analysis_seq(rows)

    if full_season:
        candidate_ids = list(
            reversed(sorted(await get_finished_game_ids_for_season(year, session)))
        )
        print(
            f"[analysis] Full-season scan found {len(candidate_ids)} finished game ID(s)."
        )
    else:
        source_ids = (
            game_ids
            if game_ids is not None
            else _sailu_game_ids_for_date(target_date)
        )
        candidate_ids = []
        for gid in source_ids:
            if gid and gid not in candidate_ids:
                candidate_ids.append(gid)
        if game_ids is not None:
            target_label = "provided game IDs"
        else:
            target_label = _date_key(target_date)
        print(
            f"[analysis] {target_label} has {len(candidate_ids)} candidate game ID(s)."
        )

    if not candidate_ids:
        print("[analysis] No candidate games found.")
        return 0

    if full_season and len(existing) >= len(candidate_ids):
        print(
            "[analysis] Sheet already has all finished games by count; "
            "skipping box-score scrape."
        )
        return 0

    new_games: list[tuple[str, dict]] = []
    for i in range(0, len(candidate_ids), MAX_CONCURRENT):
        batch = candidate_ids[i : i + MAX_CONCURRENT]
        scraped = await asyncio.gather(
            *[get_schedule_game_data(gid, session, retry=full_season) for gid in batch],
            return_exceptions=True,
        )
        for gid, data in zip(batch, scraped):
            if isinstance(data, Exception):
                print(f"  [analysis] get_schedule_game_data({gid}): {data}")
            elif data:
                if target_date and data["日期"] != _date_key(target_date):
                    continue
                ident = _analysis_identity(data)
                if ident not in existing:
                    new_games.append((gid, data))
                    existing.add(ident)
                    print(f"  [analysis] missing ← {gid} {ident}")
            else:
                print(f"  [analysis] No data for {gid}")
        if i + MAX_CONCURRENT < len(candidate_ids):
            await asyncio.sleep(2)

    if not new_games:
        print("[analysis] No new games to append.")
        return 0

    new_games.sort(key=lambda x: (x[1]["日期"], x[0]))
    inserted = 0
    for gid, data in new_games:
        row_values = _analysis_row(last_seq + inserted + 1, data)
        insert_at = _analysis_insert_index(rows, data["日期"])
        sheet.insert_row(
            row_values,
            index=insert_at,
            value_input_option="USER_ENTERED",
            inherit_from_before=True,
        )
        rows.insert(insert_at - 1, [str(v) for v in row_values])
        inserted += 1
        print(f"  [analysis] inserted row {insert_at} ← {gid}")
        await asyncio.sleep(1)

    print(f"[analysis] Inserted {inserted} row(s) into {ANALYSIS_SHEET_NAME}.")
    return inserted


def update_huizi_sheet(today: datetime | str | None = None):
    """
    Refresh 彙資 with a target date's finished games from 分析表紀錄.

    彙資 keeps the same 83-column shape and reserves rows 3-8 for the date's six
    possible NPB games.
    """
    if isinstance(today, str):
        today = datetime.strptime(today, "%Y-%m-%d")
    else:
        today = today or datetime.now()
    today_str = f"{today.year}/{today.month}/{today.day}"
    print(f"\n=== {HUIZI_SHEET_NAME} update ({today_str}) ===")

    analysis = get_worksheet(ANALYSIS_SHEET_NAME, NPB_SPREADSHEET_KEY)
    huizi = get_worksheet(HUIZI_SHEET_NAME, NPB_SPREADSHEET_KEY)
    rows = analysis.get_all_values()
    today_rows = [row[:83] for row in rows[2:] if len(row) > 1 and row[1] == today_str]

    if not today_rows:
        print(f"[huizi] No finished games for {today_str}; keeping existing data.")
        return 0

    huizi.batch_clear(["B3:CE8"])
    values = []
    for row in today_rows[:6]:
        padded = row + [""] * (83 - len(row))
        values.append(padded[1:83])

    end_row = 2 + len(values)
    huizi.update(
        range_name=f"B3:CE{end_row}",
        values=values,
        value_input_option="USER_ENTERED",
    )
    print(f"[huizi] Updated {len(values)} today game row(s).")
    return len(values)


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
                _display_field_name(g.get("球場", "")),
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
    """10pt default; shrink longer pitcher names to fit the narrow column."""
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


def _to_number(value) -> Optional[float]:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
                    "textFormat": {"foregroundColor": hex_to_rgb(hex_color)}
                }
            },
            "fields": "userEnteredFormat.textFormat.foregroundColor",
        }
    }


def _game_font_color_requests(
    sheet_id: int, games: list[dict], game_start_row: int, col_start: int
) -> list[dict]:
    """Colour game-row score and hit cells to match the CPBL 近十場 rules."""
    sorted_games = sorted(
        games, key=lambda g: datetime.strptime(g["日期"], "%Y/%m/%d")
    )[-GAMES_COUNT:]

    runs_col = col_start + 4  # 0-indexed 得点; col_start is 1-indexed
    allowed_col = col_start + 5  # 0-indexed 失点
    hits_col = col_start + 7  # 0-indexed 安打
    requests = []

    for i in range(GAMES_COUNT):
        row_0idx = game_start_row - 1 + i
        runs_color = DEFAULT_FONT
        allowed_color = DEFAULT_FONT
        hits_color = DEFAULT_FONT

        if i < len(sorted_games):
            game = sorted_games[i]
            runs = _to_number(game.get("得分"))
            allowed = _to_number(game.get("失分"))
            hits = _to_number(game.get("安打"))

            if runs is not None and allowed is not None:
                if runs > allowed:
                    runs_color = SCORE_WIN_FONT
                elif allowed > runs:
                    allowed_color = SCORE_LOSS_FONT
                else:
                    runs_color = SCORE_TIE_FONT
                    allowed_color = SCORE_TIE_FONT

            if hits is not None and hits >= 10:
                hits_color = HITS_10_PLUS_FONT

        requests.append(_font_color_request(sheet_id, row_0idx, runs_col, runs_color))
        requests.append(
            _font_color_request(sheet_id, row_0idx, allowed_col, allowed_color)
        )
        requests.append(_font_color_request(sheet_id, row_0idx, hits_col, hits_color))

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
        format_requests.extend(
            _game_font_color_requests(sheet.id, away_games, TOP_GAME_START, col_start)
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
        format_requests.extend(
            _game_font_color_requests(
                sheet.id, home_games, BOTTOM_GAME_START, col_start
            )
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

        # Update 賽錄 in the analysis spreadsheet
        new_sailu_ids = []
        try:
            new_sailu_ids = await update_sailu_sheet(session)
        except Exception as e:
            errors.append(f"update_sailu_sheet: {e}")

        # Update the newly written finished games in 分析表紀錄, then refresh 彙資.
        huizi_date = None
        try:
            analysis_game_ids = new_sailu_ids or _sailu_game_ids_for_date()
            await update_analysis_sheet(session, game_ids=analysis_game_ids)
            sailu_dates = _sailu_dates_for_game_ids(new_sailu_ids)
            if not sailu_dates and analysis_game_ids:
                sailu_dates = _sailu_dates_for_game_ids(analysis_game_ids)
            if sailu_dates:
                huizi_date = sailu_dates[-1]
        except Exception as e:
            errors.append(f"update_analysis_sheet: {e}")

        try:
            update_huizi_sheet(huizi_date)
        except Exception as e:
            errors.append(f"update_huizi_sheet: {e}")

    if errors:
        print(f"\n[ERROR] {len(errors)} failure(s):")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)
    else:
        print("\nDone.")


if __name__ == "__main__":
    asyncio.run(run_once())
