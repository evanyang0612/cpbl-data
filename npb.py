import json
import os
import sys
import re
import platform
import asyncio
import argparse
import aiohttp
import secrets
from bs4 import BeautifulSoup as bs
from copy import deepcopy
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import urljoin

import gspread
from dotenv import load_dotenv

from baseball.npb_services import (
    _NpbPredictionLogic,
    NpbAnalysisService,
    NpbHuiziService,
    NpbLeagueSheetService,
    NpbPredictionService,
    NpbRowsService,
    NpbSailuService,
    NpbUpdateService,
)
from baseball.sheets import GoogleSheetsClient

load_dotenv()

# --- Configuration ---
NPB_SPREADSHEET_KEY = "1XBATQ-ZQVE7saISTw_EYEXg3qFFAn5aeLDPdGI1_8Rg"
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
_sheets_client = GoogleSheetsClient(credentials_file=CREDENTIALS_FILE)

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
HOT_RATE_FONT = "ff0000"
COLD_RATE_FONT = "38761d"
HOT_AVG_THRESHOLD = 0.280
COLD_AVG_THRESHOLD = 0.200
HOT_OBP_THRESHOLD = 0.330
COLD_OBP_THRESHOLD = 0.250

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

HOME_TEAM_MATCHUP_ORDER = [
    "巨人",
    "中日",
    "阪神",
    "ヤクルト",
    "広島",
    "DeNA",
    "ロッテ",
    "西武",
    "ソフトバンク",
    "オリックス",
    "日本ハム",
    "楽天",
]
HOME_TEAM_MATCHUP_RANK = {team: idx for idx, team in enumerate(HOME_TEAM_MATCHUP_ORDER)}

SAILU_SPREADSHEET_KEY = "1X2oaXk6DJLkx1MPVjc0lgLNtqa88X5qdNdKuKyikrbg"
SAILU_TARGET_SPREADSHEET_KEY = "1XBATQ-ZQVE7saISTw_EYEXg3qFFAn5aeLDPdGI1_8Rg"
SAILU_SHEET_NAME = "賽錄"
EXHIBITION_SHEET_NAME = "熱身賽紀錄"
ANALYSIS_SHEET_NAME = "分析表紀錄"
HUIZI_SHEET_NAME = "彙資"
NPB_STATUS_SHEET_NAME = "NPB狀態"
NPB_STATUS_HEADERS = ["Date", "GameId", "Status", "Resolved", "UpdatedAt"]
NPB_NO_GAMES_SENTINEL = "__NO_GAMES__"
PREDICTION_SHEET_NAME = "預測紀錄"
PREDICTION_SPREADSHEET_KEY = "1-L5RvjhN3OFiXfrDUVxBa8W0EqYIaLQtxizSi-yP8b0"
ANALYSIS_SEASON = 2026
PREDICTION_STARTING_BALANCE = 0.0
PREDICTION_DEFAULT_STAKE = 10.0
PREDICTION_PROMPT_SENTINEL = "__prompt_prediction__"
PREDICTION_MARKET_ALIASES = {
    "half": "half_winner",
    "half-winner": "half_winner",
    "half_winner": "half_winner",
    "final": "final_winner",
    "winner": "final_winner",
    "final-winner": "final_winner",
    "final_winner": "final_winner",
    "half-total": "half_total",
    "half_total": "half_total",
    "final-total": "final_total",
    "total": "final_total",
    "final_total": "final_total",
    "handicap": "final_handicap",
    "final_handicap": "final_handicap",
    "final-handicap": "final_handicap",
    "讓分": "final_handicap",
    "half_handicap": "half_handicap",
    "half-handicap": "half_handicap",
    "half_handicap": "half_handicap",
    "半場讓分": "half_handicap",
}

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
_PLAYER_BAT_HAND_CACHE: dict[str, str] = {}

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
    "ハードオフ新潟": "新潟",
}


def _display_field_name(venue: str) -> str:
    """Format venue names for compact NPB display sheets."""
    return NpbLeagueSheetService(module=sys.modules[__name__]).display_field_name(venue)


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

NPB_TEAM_HOME_FIELDS = {
    "巨人": {"東京ドーム"},
    "ヤクルト": {"神宮", "明治神宮"},
    "DeNA": {"横浜"},
    "中日": {"バンテリンドーム", "ナゴヤドーム"},
    "阪神": {"甲子園"},
    "広島": {"マツダスタジアム", "マツダ"},
    "西武": {"ベルーナドーム", "西武ドーム"},
    "日本ハム": {"エスコンF"},
    "ロッテ": {"ZOZOマリン", "QVCマリン"},
    "オリックス": {"京セラD大阪", "京セラドーム", "京セラD"},
    "ソフトバンク": {"みずほPayPay", "ヤフードーム"},
    "楽天": {"楽天モバイル", "Ｋスタ宮城"},
}


LEAGUE_SHEETS = {
    "央盟": "近十場a",
    "洋盟": "近十場b",
}

# Block column start positions (1-indexed: B=2, Q=17, AF=32)
BLOCK_COLS = [2, 17, 32]

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
TOP_HR_HEADER_ROW = 30
TOP_HR_END_ROW = 38
BOTTOM_HR_HEADER_ROW = 40
BOTTOM_HR_END_ROW = 48

# Rows per block (header + 10 games + 2 avg rows = 13)
BLOCK_ROWS = 13


# --- Helpers ---


def hex_to_rgb(hex_color: str) -> dict:
    """Convert a 6-char hex color string to a Sheets API RGB dict (0.0–1.0 floats)."""
    return NpbLeagueSheetService.hex_to_rgb(hex_color)


def col_to_letter(col: int) -> str:
    """Convert 1-indexed column number to letter(s). e.g. 2→B, 15→O, 28→AB"""
    return NpbLeagueSheetService.col_to_letter(col)


def get_worksheet(sheet_name: str, spreadsheet_key: str = NPB_SPREADSHEET_KEY):
    return _sheets_client.worksheet(spreadsheet_key, sheet_name)


def get_npb_status_worksheet():
    spreadsheet = _sheets_client.spreadsheet(NPB_SPREADSHEET_KEY)
    try:
        sheet = spreadsheet.worksheet(NPB_STATUS_SHEET_NAME)
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(
            title=NPB_STATUS_SHEET_NAME, rows=1000, cols=len(NPB_STATUS_HEADERS)
        )
        sheet.update("A1", [NPB_STATUS_HEADERS], value_input_option="USER_ENTERED")
        return sheet

    values = sheet.get_all_values()
    if not values:
        sheet.update("A1", [NPB_STATUS_HEADERS], value_input_option="USER_ENTERED")
    elif values[0][: len(NPB_STATUS_HEADERS)] != NPB_STATUS_HEADERS:
        sheet.update("A1", [NPB_STATUS_HEADERS], value_input_option="USER_ENTERED")
    return sheet


def is_exhibition_game_id(game_id: str) -> bool:
    """Warm-up / exhibition games currently use the 202104... game-id prefix."""
    return str(game_id).startswith("202104")


def display_team_name(team_name: str) -> str:
    """Match existing sheet naming conventions."""
    return "横浜" if team_name == "DeNA" else team_name


def _sort_matchups_by_home_team(
    matchups: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    return sorted(
        matchups,
        key=lambda matchup: (
            HOME_TEAM_MATCHUP_RANK.get(matchup[1], len(HOME_TEAM_MATCHUP_RANK)),
            matchup[1],
            matchup[0],
        ),
    )


# --- NPB prediction ledger ---


def _prediction_now() -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).now()


def _prediction_payload(
    game_id: str,
    pick: str,
    rate: float,
    stake: float,
    market: str = "final_winner",
    line: float | None = None,
    predicted_at: str | None = None,
) -> dict:
    return _NpbPredictionLogic(module=sys.modules[__name__]).payload(
        game_id,
        pick,
        rate,
        stake,
        market=market,
        line=line,
        predicted_at=predicted_at,
    )


def build_prediction_text(
    game_id: str,
    pick: str,
    rate: float,
    stake: float = PREDICTION_DEFAULT_STAKE,
    *,
    market: str = "final_winner",
    line: float | None = None,
) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).prediction_text(
        game_id,
        pick,
        rate,
        stake,
        market=market,
        line=line,
    )


def calculate_prediction_balance(
    balance_before: float, stake: float, rate: float, outcome: str
) -> float:
    return _NpbPredictionLogic.calculate_balance(balance_before, stake, rate, outcome)


def _prediction_float(value, default: float = 0.0) -> float:
    return _NpbPredictionLogic.to_float(value, default)


def _prediction_normalize_team(value: str) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).normalize_team(value)


def _prediction_team_options() -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).team_options()


def validate_prediction_home_team(value: str) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).validate_home_team(value)


def _prediction_display_team(value: str) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).display_team(value)


def normalize_prediction_market(market: str) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).normalize_market(market)


def _prediction_side(value: str) -> str:
    return _NpbPredictionLogic.prediction_side(value)


def validate_prediction_pick(pick: str, market: str) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).validate_pick(pick, market)


def _prediction_int(value) -> int:
    return _NpbPredictionLogic.to_int(value)


def _prediction_innings_total(values: list, innings: int = 5) -> int:
    return _NpbPredictionLogic(module=sys.modules[__name__]).innings_total(
        values, innings
    )


def _prediction_winner_outcome(data: dict, pick: str, *, half: bool = False) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).winner_outcome(
        data, pick, half=half
    )


def _prediction_total_outcome(
    data: dict, pick: str, line: float, *, half: bool = False
) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).total_outcome(
        data, pick, line, half=half
    )


def prediction_outcome_for_game(
    data: dict, pick: str, market: str = "final_winner", line: float | None = None
) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).outcome_for_game(
        data, pick, market=market, line=line
    )


def _prediction_headers() -> list[str]:
    return _NpbPredictionLogic.headers()


def _prediction_has_header(row: list[str]) -> bool:
    return _NpbPredictionLogic(module=sys.modules[__name__]).has_header(row)


def _prediction_sheet():
    return _NpbPredictionLogic(module=sys.modules[__name__]).sheet()


def _prediction_rows(sheet, *, ensure_header: bool = True) -> list[list[str]]:
    return _NpbPredictionLogic(module=sys.modules[__name__]).rows(
        sheet, ensure_header=ensure_header
    )


def _last_prediction_balance(rows: list[list[str]]) -> float:
    return _NpbPredictionLogic(module=sys.modules[__name__]).last_balance(rows)


def _prediction_balance_before_formula(headers: list[str], row_num: int):
    return _NpbPredictionLogic(module=sys.modules[__name__]).balance_before_formula(
        headers, row_num
    )


def _prediction_balance_after_formula(headers: list[str], row_num: int) -> str:
    return _NpbPredictionLogic(module=sys.modules[__name__]).balance_after_formula(
        headers, row_num
    )


def _prediction_stats_from_rows(rows: list[list[str]]) -> dict:
    return _NpbPredictionLogic(module=sys.modules[__name__]).stats_from_rows(rows)


def _prediction_stats_after(
    rows: list[list[str]], outcome: str, balance_after: float
) -> dict:
    return _NpbPredictionLogic(module=sys.modules[__name__]).stats_after(
        rows, outcome, balance_after
    )


def create_npb_prediction(
    game_id: str,
    pick: str,
    rate: float,
    *,
    market: str = "final_winner",
    line: float | None = None,
    stake: float = PREDICTION_DEFAULT_STAKE,
    game_date: str = "",
    away_team: str = "",
    home_team: str = "",
    sheet=None,
    post: bool = False,
    dry_run: bool = False,
) -> dict:
    return NpbPredictionService(module=sys.modules[__name__]).create_prediction(
        game_id,
        pick,
        rate,
        market=market,
        line=line,
        stake=stake,
        game_date=game_date,
        away_team=away_team,
        home_team=home_team,
        sheet=sheet,
        post=post,
        dry_run=dry_run,
    )


def _prompt_text(label: str, *, default: str = "", required: bool = True) -> str:
    suffix = f" [{default}]" if default != "" else ""
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default != "":
            return default
        if not required:
            return ""
        print(f"{label} is required.")


def _prompt_float(label: str, *, default: float | None = None) -> float:
    suffix = f" [{default}]" if default is not None else ""
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if not value and default is not None:
            return float(default)
        try:
            return float(value)
        except ValueError:
            print(f"{label} must be a number.")


def _prompt_market(default: str = "final_winner") -> str:
    options = [
        "final_winner",
        "half_winner",
        "final_total",
        "half_total",
        "final_handicap",
        "half_handicap",
    ]
    print("Market options:")
    for idx, market in enumerate(options, start=1):
        print(f"  {idx}. {market}")
    while True:
        value = _prompt_text("Market", default=default)
        if value.isdigit() and 1 <= int(value) <= len(options):
            return options[int(value) - 1]
        try:
            return normalize_prediction_market(value)
        except ValueError as exc:
            print(exc)


def _prompt_pick(market: str, *, default: str = "") -> str:
    while True:
        value = _prompt_text(
            "Pick (team for winner, over/under for total)",
            default=default,
        )
        try:
            return validate_prediction_pick(value, market)
        except ValueError as exc:
            print(exc)


def _prompt_home_team(default: str = "") -> str:
    print(f"Home team options: {_prediction_team_options()}")
    while True:
        value = _prompt_text("Home team", default=default)
        try:
            return validate_prediction_home_team(value)
        except ValueError as exc:
            print(exc)


def _prediction_status_allows_predict(text: str) -> bool:
    status = str(text or "").strip()
    if not status:
        return False
    blocked = ("試合終了", "中止", "ノーゲーム", "延期", "試合中")
    if any(word in status for word in blocked):
        return False
    if re.search(r"\d+\s*回[表裏]", status):
        return False
    return True


def _prediction_starter_from_status(text: str) -> str:
    match = re.search(r"先発\s*[:：]\s*([^,\s　/／]+)", str(text or ""))
    return match.group(1).strip() if match else ""


def _prediction_parse_game_context(
    game_id: str,
    game_date: str,
    html: str,
) -> dict:
    soup = bs(html, "html.parser")
    teams = [
        el.get_text(" ", strip=True)
        for el in soup.find_all(class_="bb-gameScoreTable__team")
    ]
    away_team = _prediction_normalize_team(teams[0]) if len(teams) >= 1 else ""
    home_team = _prediction_normalize_team(teams[1]) if len(teams) >= 2 else ""

    starters = []
    for score_tbl in soup.find_all(class_="bb-scoreTable")[:2]:
        row = score_tbl.find(class_="bb-scoreTable__row")
        player_el = row.find(class_="bb-scoreTable__data--player") if row else None
        starters.append(player_el.get_text(" ", strip=True) if player_el else "")
    while len(starters) < 2:
        starters.append("")

    return {
        "game_id": str(game_id),
        "game_date": game_date,
        "away_team": away_team,
        "home_team": home_team,
        "away_starter": starters[0],
        "home_starter": starters[1],
    }


def _prediction_print_game_context(context: dict) -> None:
    print("Prediction game:")
    print(f"  Date: {context.get('game_date', '')}")
    print(f"  Game ID: {context.get('game_id', '')}")
    print(f"  Away: {_prediction_display_team(context.get('away_team', ''))}")
    print(f"  Home: {_prediction_display_team(context.get('home_team', ''))}")
    print(f"  Away starter: {context.get('away_starter') or 'unknown'}")
    print(f"  Home starter: {context.get('home_starter') or 'unknown'}")


async def resolve_prediction_game_by_home_team(
    home_team: str,
    session: aiohttp.ClientSession,
    *,
    today: date | None = None,
) -> dict:
    home_key = _prediction_normalize_team(home_team)
    if home_key not in NPB_TEAMS:
        raise ValueError(f"Unknown home team: {home_team}")

    start = today or datetime.now().date()
    end = start + timedelta(days=1)
    time_keys = {
        start.strftime("%Y-%m"),
        end.strftime("%Y-%m"),
    }
    candidates: list[tuple[date, str, str]] = []

    for time_key in sorted(time_keys):
        html = await _fetch(
            session,
            f"{BASE_URL}teams/{NPB_TEAMS[home_key]['id']}/schedule?month={time_key}",
        )
        if not html:
            continue
        soup = bs(html, "html.parser")
        month_base = datetime.strptime(f"{time_key}-01", "%Y-%m-%d").date()
        for data in soup.find_all(class_="bb-calendarTable__data"):
            date_el = data.find(class_="bb-calendarTable__date")
            status = data.find(class_="bb-calendarTable__status")
            if not date_el or not status:
                continue
            try:
                game_date = month_base.replace(day=int(date_el.get_text(strip=True)))
            except (TypeError, ValueError):
                continue
            if game_date < start or game_date > end:
                continue
            status_text = status.get_text(" ", strip=True)
            if not _prediction_status_allows_predict(status_text):
                continue
            href = status.get("href") or ""
            match = re.search(r"npb/game/([^/]+)", href)
            if match:
                candidates.append((game_date, match.group(1), status_text))

    for game_date, game_id, status_text in sorted(candidates):
        for path in ("stats", "top"):
            html = await _fetch(session, f"{BASE_URL}game/{game_id}/{path}")
            if not html:
                continue
            context = _prediction_parse_game_context(
                game_id, game_date.strftime("%Y-%m-%d"), html
            )
            if context["home_team"] == home_key:
                context["home_starter"] = context[
                    "home_starter"
                ] or _prediction_starter_from_status(status_text)
                return context

    raise ValueError(
        f"No unstarted today/tomorrow game found with {home_key} as home team."
    )


async def _prediction_resolve_cli_game(values: dict, *, confirm: bool = False) -> dict:
    async with aiohttp.ClientSession() as session:
        context = await resolve_prediction_game_by_home_team(
            values["home_team_lookup"], session
        )
    _prediction_print_game_context(context)
    if confirm:
        answer = input("Use this game? [Y/n]: ").strip().lower()
        if answer in {"n", "no", "不要", "否"}:
            raise ValueError("Prediction cancelled.")

    values = dict(values)
    values["game_id"] = context["game_id"]
    values["game_date"] = values["game_date"] or context["game_date"]
    values["away_team"] = values["away_team"] or context["away_team"]
    values["home_team"] = values["home_team"] or context["home_team"]
    return values


def _prediction_cli_values(args) -> dict:
    interactive = args.create_prediction == PREDICTION_PROMPT_SENTINEL
    home_team_lookup = (
        _prompt_home_team()
        if interactive
        else validate_prediction_home_team(args.create_prediction or "")
    )
    market = (
        _prompt_market(args.market or "final_winner") if interactive else args.market
    )
    market = normalize_prediction_market(market or "final_winner")
    pick = (
        _prompt_pick(market, default=args.pick or "")
        if interactive
        else (validate_prediction_pick(args.pick, market) if args.pick else "")
    )
    line = args.line
    if (
        market in {"half_total", "final_total", "final_handicap", "half_handicap"}
        and line is None
    ):
        line = _prompt_float("Line")
    rate = args.rate if args.rate is not None else _prompt_float("Rate")
    stake = (
        _prompt_float("Stake", default=PREDICTION_DEFAULT_STAKE)
        if interactive and args.stake is None
        else (args.stake if args.stake is not None else PREDICTION_DEFAULT_STAKE)
    )
    game_date = args.game_date
    away_team = args.away_team
    home_team = args.home_team
    return {
        "home_team_lookup": home_team_lookup,
        "game_id": "",
        "market": market,
        "pick": pick,
        "line": line,
        "rate": rate,
        "stake": stake,
        "game_date": game_date,
        "away_team": away_team,
        "home_team": home_team,
    }


def resolve_npb_predictions_for_game(
    game_id: str,
    data: dict,
    *,
    sheet=None,
    post: bool = False,
    dry_run: bool = False,
) -> int:
    return NpbPredictionService(module=sys.modules[__name__]).resolve_for_game(
        game_id, data, sheet=sheet, post=post, dry_run=dry_run
    )


async def update_npb_prediction_reveals(
    session: aiohttp.ClientSession,
    game_ids: list[str],
    *,
    post: bool = False,
    dry_run: bool = False,
) -> int:
    return await NpbPredictionService(
        module=sys.modules[__name__]
    ).reveal_predictions_for_games(session, game_ids, post=post, dry_run=dry_run)


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

    month_cursor = now.replace(day=1)
    for _ in range(12):
        time_key = month_cursor.strftime("%Y-%m")
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
            entry_date = month_cursor.replace(day=entry_day)
            if entry_date.date() > now.date():
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

        if len(game_ids) >= n:
            break

        month_cursor = (month_cursor - timedelta(days=1)).replace(day=1)

    return game_ids[:n]


async def get_next_scheduled_game(
    team_id: int,
    session: aiohttp.ClientSession,
    start_date: Optional[date] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    Find the next upcoming (not yet finished) game for a team.
    Returns (game_id, date_str) or (None, None).
    game_id may be None when the game is scheduled but the page isn't live yet.
    """
    now = datetime.now()
    start = start_date or now.date()

    for month_offset in range(3):
        month_base = datetime.combine(start, datetime.min.time())
        check_month = (
            month_base.replace(day=1) + timedelta(days=32 * month_offset)
        ).replace(day=1)
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

            if entry_date.date() < start:
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


def _official_team_key(name: str) -> str:
    norm = re.sub(r"\s+", "", name)
    for official, raw in OFFICIAL_TEAM_NAME_MAP.items():
        if official in norm:
            return raw
    raise ValueError(f"Unknown official team name: {name}")


async def _official_next_matchups(
    league: str, session: aiohttp.ClientSession, start_date: date
) -> list[tuple[str, str]]:
    """
    Read NPB.jp schedule rows in official display order and return matchups for
    the first league game day on or after start_date.

    Official rows render team1 as home and team2 as away. Using this as the
    primary source keeps both column order and home/away correct when Yahoo's
    per-team schedule is mixed by partially finished games.
    """
    league_teams = {k: v for k, v in NPB_TEAMS.items() if v["league"] == league}
    by_date: dict[str, list[tuple[str, str]]] = {}

    month_cursor = start_date.replace(day=1)
    for month_offset in range(3):
        dt = (
            datetime.combine(month_cursor, datetime.min.time())
            + timedelta(days=32 * month_offset)
        ).replace(day=1)
        paths = [
            f"/games/{dt.year}/schedule_{dt.month:02d}_detail.html",
            f"/interleague/{dt.year}/schedule_detail.html",
            f"/climax/{dt.year}/schedule_detail.html",
            f"/nippons/{dt.year}/schedule_detail.html",
        ]
        if dt.month <= 3:
            paths.insert(0, f"/preseason/{dt.year}/schedule_detail.html")

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

                team1 = tr.find("div", class_="team1")
                team2 = tr.find("div", class_="team2")
                if not current_date or not team1 or not team2:
                    continue
                if "予備日" in tr.get_text(" ", strip=True):
                    continue

                m = re.match(r"(\d{1,2})/(\d{1,2})", current_date)
                if not m:
                    continue
                game_date = datetime(dt.year, int(m.group(1)), int(m.group(2))).date()
                if game_date < start_date:
                    continue

                try:
                    home_key = _official_team_key(team1.get_text(" ", strip=True))
                    away_key = _official_team_key(team2.get_text(" ", strip=True))
                except ValueError:
                    continue

                if home_key not in NPB_TEAMS or away_key not in NPB_TEAMS:
                    continue
                if home_key not in league_teams and away_key not in league_teams:
                    continue

                date_key = game_date.strftime("%Y-%m-%d")
                game = (away_key, home_key)
                by_date.setdefault(date_key, [])
                if game not in by_date[date_key]:
                    by_date[date_key].append(game)

    for date_key in sorted(by_date):
        same_league: list[tuple[str, str]] = []
        cross_league: list[tuple[str, str]] = []

        for away_key, home_key in by_date[date_key]:
            away_in = away_key in league_teams
            home_in = home_key in league_teams
            if away_in and home_in:
                same_league.append((away_key, home_key))
            elif away_in or home_in:
                cross_league.append((away_key, home_key))

        matchups = _sort_matchups_by_home_team(same_league)

        if not same_league and len(cross_league) > 3:
            league_idx = list(LEAGUE_SHEETS).index(league)
            start_idx = league_idx * 3
            matchups = _sort_matchups_by_home_team(cross_league)[
                start_idx : start_idx + 3
            ]
            if matchups:
                print(
                    f"[{league}] Official next game day: {date_key}, games: {matchups}"
                )
                return matchups

        # During interleague play, keep real cross-league opponents. Prefer games
        # where this sheet's league is the away side, so the two league sheets
        # split a six-game day into three actual matchups each when schedules are
        # balanced by home/away league.
        preferred_cross = _sort_matchups_by_home_team(
            [game for game in cross_league if game[0] in league_teams]
            + [game for game in cross_league if game[0] not in league_teams]
        )
        for game in preferred_cross:
            if len(matchups) >= 3:
                break
            if game not in matchups:
                matchups.append(game)

        matched = {t for pair in matchups for t in pair}
        unmatched = [k for k in league_teams if k not in matched]
        for i in range(0, len(unmatched) - 1, 2):
            if len(matchups) >= 3:
                break
            matchups.append((unmatched[i], unmatched[i + 1]))

        if matchups:
            matchups = _sort_matchups_by_home_team(matchups)
            print(f"[{league}] Official next game day: {date_key}, games: {matchups}")
            return matchups[:3]

    return []


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


def _resolve_matchup_start_date(
    value: str | None = None, today: date | None = None
) -> date:
    """
    Resolve the first date allowed for 近十場 matchup ordering.

    Default stays tomorrow so the scheduled job keeps showing the next game day.
    Manual backfills can pass "today" or YYYY-MM-DD to keep the display anchored
    on the current day's matchups.
    """
    base = today or datetime.now().date()
    raw = (value or os.getenv("NPB_MATCHUP_DATE") or "").strip()
    if not raw:
        return base + timedelta(days=1)

    normalized = raw.lower()
    if normalized in {"today", "今天", "今日"}:
        return base
    if normalized in {"tomorrow", "明天", "明日"}:
        return base + timedelta(days=1)

    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            "matchup date must be 'today', 'tomorrow', or YYYY-MM-DD"
        ) from exc


async def get_next_matchups(
    league: str, session: aiohttp.ClientSession, start_date: date | None = None
) -> list[tuple[str, str]]:
    """
    Returns up to 3 (away_key, home_key) pairs for the next game day in the league.
    By default the display is based on tomorrow or the first later scheduled game
    day, so partial results from games in progress today cannot affect the matchup
    order. Manual backfills may pass start_date=today to keep today's matchups.
    NPB.jp official schedule order is preferred; Yahoo team/game pages are fallback.
    """
    league_teams = {k: v for k, v in NPB_TEAMS.items() if v["league"] == league}
    start = start_date or _resolve_matchup_start_date()

    official_matchups = await _official_next_matchups(league, session, start)
    if official_matchups:
        return official_matchups

    async def _resolve_team_next(start: date) -> dict[str, tuple[Optional[str], str]]:
        tasks = {
            key: get_next_scheduled_game(info["id"], session, start)
            for key, info in league_teams.items()
        }
        resolved = await asyncio.gather(*tasks.values())
        # Include teams where a date was found, even if game_id is None
        # (pre-season / no page yet).
        return {
            key: (gid, d)
            for key, (gid, d) in zip(tasks.keys(), resolved)
            if d is not None
        }

    team_next = await _resolve_team_next(start)

    if not team_next:
        print(f"[{league}] No upcoming games found, using alphabetical order.")
        teams = list(league_teams.keys())
        return [(teams[i * 2], teams[i * 2 + 1]) for i in range(3)]

    next_dates = sorted({d for _, d in team_next.values()})
    start = datetime.strptime(next_dates[0], "%Y-%m-%d").date()

    # Find the nearest next game date.
    next_date = min(d for _, d in team_next.values())
    day_games = {key: gid for key, (gid, d) in team_next.items() if d == next_date}

    if not day_games:
        teams = list(league_teams.keys())
        return [(teams[i * 2], teams[i * 2 + 1]) for i in range(3)]

    print(f"[{league}] Next game day: {next_date}, games: {day_games}")

    seen: dict[str, tuple[str, str]] = {}  # game_id -> (away_key, home_key)
    cross_games: list[tuple[str, str]] = []

    # For teams that have a real game ID, fetch the game page to get teams + venue.
    # /top works for finished games; for upcoming games /top has no team/venue data,
    # but /stats does — so always try /stats as fallback when parsing fails.
    known_ids = sorted({gid for gid in day_games.values() if gid is not None})
    for game_id in known_ids:
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
        elif t0_in or t1_in:
            # Inter-league game — keep the actual opponent and home/away order.
            cross_games.append((t0, t1))

    matchups = _sort_matchups_by_home_team(list(seen.values()))
    matched = {t for pair in matchups for t in pair}
    matched.update(t for pair in cross_games for t in pair if t in league_teams)

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
                # Inter-league game, no ID yet — can't determine home/away without
                # game page; default this league's team to away so it is retained.
                cross_games.append((key, opp))
                matched.add(key)

    if not matchups and len(cross_games) > 3:
        league_idx = list(LEAGUE_SHEETS).index(league)
        start_idx = league_idx * 3
        matchups = _sort_matchups_by_home_team(cross_games)[start_idx : start_idx + 3]

    preferred_cross = _sort_matchups_by_home_team(
        [game for game in cross_games if game[0] in league_teams]
        + [game for game in cross_games if game[0] not in league_teams]
    )
    for game in preferred_cross:
        if len(matchups) >= 3:
            break
        if game not in matchups:
            matchups.append(game)

    # Pad to 3 if still fewer than 3 matchups (e.g. rest days)
    matched = {t for pair in matchups for t in pair}
    unmatched = [k for k in league_teams if k not in matched]
    for i in range(0, len(unmatched) - 1, 2):
        if len(matchups) >= 3:
            break
        matchups.append((unmatched[i], unmatched[i + 1]))

    return _sort_matchups_by_home_team(matchups)[:3]


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


def _normalize_batting_event_text(text: str) -> str:
    return text.replace("２", "2").replace("３", "3").replace("　", "").replace(" ", "")


def _home_run_direction(text: str) -> str:
    normalized = _normalize_batting_event_text(text)
    if not re.search(r"(本塁打|本塁|本)", normalized):
        return ""
    m = re.search(r"(左中|右中|左|中|右)", normalized)
    if not m:
        return ""
    direction = m.group(1)
    if "左" in direction:
        return "左本"
    if "右" in direction:
        return "右本"
    return "中本"


def _parse_home_run_events(tbl) -> list[dict[str, str]]:
    events = []
    rows = tbl.find_all("tr") or tbl.find_all(class_="bb-statsTable__row")
    for row in rows:
        if row.find(class_="bb-statsTable__head--result"):
            continue
        player_el = row.find(class_=lambda c: c and "statsTable" in c and "player" in c)
        if not player_el:
            cells = row.find_all(["th", "td"])
            player_el = cells[0] if cells else None
        player_link = player_el.find("a", href=True) if player_el else None
        player_id = ""
        if player_link:
            player_id = _parse_player_id_from_href(player_link["href"])
        batter = player_el.get_text("", strip=True) if player_el else ""
        batter = re.sub(r"\s*[（(][左右][）)]\s*", "", batter).strip()
        batter_side = ""
        side_match = re.search(
            r"[（(]([左右])[）)]",
            player_el.get_text("", strip=True) if player_el else "",
        )
        if side_match:
            batter_side = f"{side_match.group(1)}打"
        for cell in row.find_all(class_="bb-statsTable__data--inning"):
            raw = cell.get_text("", strip=True)
            direction = _home_run_direction(raw)
            if not direction:
                continue
            events.append(
                {
                    "打者": batter,
                    "player_id": player_id,
                    "左右打": batter_side,
                    "方向": direction,
                }
            )
    return events


def _parse_player_bat_hand(html: str) -> str:
    soup = bs(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    match = re.search(r"投打\s*[右左両]投([右左両])打", text)
    if not match:
        match = re.search(r"[右左両]投([右左両])打", text)
    if not match:
        return ""
    side = match.group(1)
    return "兩打" if side == "両" else f"{side}打"


async def _player_bat_hand(player_id: str, session: aiohttp.ClientSession) -> str:
    if not player_id:
        return ""
    if player_id in _PLAYER_BAT_HAND_CACHE:
        return _PLAYER_BAT_HAND_CACHE[player_id]
    html = await _fetch_once(session, f"{BASE_URL}player/{player_id}/top")
    hand = _parse_player_bat_hand(html or "")
    _PLAYER_BAT_HAND_CACHE[player_id] = hand
    return hand


async def _enrich_home_run_batter_hands(
    events: list[dict[str, str]], session: aiohttp.ClientSession
) -> list[dict[str, str]]:
    player_ids = sorted(
        {
            event.get("player_id", "")
            for event in events
            if event.get("player_id") and not event.get("左右打")
        }
    )
    if player_ids:
        hands = await asyncio.gather(
            *[_player_bat_hand(player_id, session) for player_id in player_ids],
            return_exceptions=True,
        )
        for player_id, hand in zip(player_ids, hands):
            if isinstance(hand, Exception):
                _PLAYER_BAT_HAND_CACHE[player_id] = ""
    for event in events:
        if not event.get("左右打"):
            event["左右打"] = _PLAYER_BAT_HAND_CACHE.get(event.get("player_id", ""), "")
        event.pop("player_id", None)
    return events


def _parse_player_id_from_href(href: str) -> str:
    match = re.search(r"/npb/player/([^/]+)/", href or "")
    return match.group(1) if match else ""


def _pitcher_name_aliases(name: str) -> set[str]:
    clean = re.sub(r"\s+", " ", str(name or "").strip())
    compact = clean.replace(" ", "")
    aliases = {clean, compact}
    parts = clean.split(" ")
    if parts:
        aliases.add(parts[0])
    if len(parts) >= 2 and parts[1]:
        aliases.add(f"{parts[0]}{parts[1][0]}")
    return {alias for alias in aliases if alias}


def _parse_pitcher_name_lookup(soup) -> dict[str, str]:
    lookup: dict[str, str] = {}
    conflicts: set[str] = set()
    for score_tbl in soup.find_all(class_="bb-scoreTable"):
        for row in score_tbl.find_all(class_="bb-scoreTable__row"):
            player_el = row.find(class_="bb-scoreTable__data--player")
            if not player_el:
                continue
            full_name = player_el.get_text(" ", strip=True)
            for alias in _pitcher_name_aliases(full_name):
                if alias in lookup and lookup[alias] != full_name:
                    conflicts.add(alias)
                else:
                    lookup[alias] = full_name
    for alias in conflicts:
        lookup.pop(alias, None)
    return lookup


def _resolve_pitcher_name(name: str, lookup: dict[str, str] | None = None) -> str:
    clean = str(name or "").strip()
    if not clean or not lookup:
        return clean
    if clean in lookup:
        return lookup[clean]
    compact = clean.replace(" ", "")
    matches = {
        full_name
        for full_name in lookup.values()
        if full_name.replace(" ", "").startswith(compact)
    }
    if len(matches) == 1:
        return next(iter(matches))
    return clean


def _parse_home_run_pitcher_events(
    html: str,
    *,
    away_raw: str,
    home_raw: str,
    away_starter: str,
    home_starter: str,
    pitcher_name_lookup: dict[str, str] | None = None,
) -> dict[str, list[dict[str, str]]]:
    soup = bs(html, "html.parser")
    current_pitcher = {
        "away": _resolve_pitcher_name(away_starter, pitcher_name_lookup),
        "home": _resolve_pitcher_name(home_starter, pitcher_name_lookup),
    }
    result = {"away": [], "home": []}

    for section in soup.find_all("section", class_="bb-liveText"):
        inning_el = section.find(class_="bb-liveText__inning")
        detail_el = section.find(class_="bb-liveText__detail")
        if not inning_el or not detail_el:
            continue
        inning_text = inning_el.get_text("", strip=True)
        detail_text = detail_el.get_text("", strip=True)
        if "回表" in inning_text:
            side = "away"
            pitching_side = "home"
        elif "回裏" in inning_text:
            side = "home"
            pitching_side = "away"
        else:
            continue
        if away_raw not in detail_text and home_raw not in detail_text:
            continue

        for item in section.find_all("li", class_="bb-liveText__item"):
            batter_el = item.find(class_="bb-liveText__batter")
            batter_link = batter_el.find("a", href=True) if batter_el else None
            batter = batter_link.get_text("", strip=True) if batter_link else ""
            player_id = (
                _parse_player_id_from_href(batter_link["href"]) if batter_link else ""
            )
            summaries = item.find_all(class_="bb-liveText__summary")
            summary_texts = [s.get_text(" ", strip=True) for s in summaries]
            for summary in summaries:
                text = summary.get_text(" ", strip=True)
                change = re.search(r"投手交代:\s*([^\s]+)\s*→\s*([^\s]+)", text)
                if change:
                    current_pitcher[pitching_side] = _resolve_pitcher_name(
                        change.group(2), pitcher_name_lookup
                    )
                    continue
                mound_change = re.search(
                    r"ピッチャー\s*([^\s]+)\s*に代わって\s*([^\s]+)\s*がマウンド",
                    text,
                )
                if mound_change:
                    current_pitcher[pitching_side] = _resolve_pitcher_name(
                        mound_change.group(2), pitcher_name_lookup
                    )

            if any("ホームラン" in text for text in summary_texts):
                result[side].append(
                    {
                        "打者": batter,
                        "player_id": player_id,
                        "投手": current_pitcher[pitching_side],
                    }
                )

    return result


def _enrich_home_run_pitchers(
    events: list[dict[str, str]], pitcher_events: list[dict[str, str]]
) -> list[dict[str, str]]:
    used = set()
    for event in events:
        for idx, pitcher_event in enumerate(pitcher_events):
            if idx in used:
                continue
            same_player = event.get("player_id") and event.get(
                "player_id"
            ) == pitcher_event.get("player_id")
            same_name = event.get("打者") and event.get("打者") == pitcher_event.get(
                "打者"
            )
            if same_player or same_name:
                event["投手"] = pitcher_event.get("投手", "")
                used.add(idx)
                break
    return events


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

            score_link = tr.find("a", href=lambda h: h and f"/scores/{dt.year}/" in h)
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

        for a in soup.find_all("a", href=lambda h: h and f"/scores/{dt.year}/" in h):
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
    schedule_status = await _schedule_status_for_game(game_id, date_str, session)

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
    away_homers = _parse_home_run_events(bat_tables[0]) if len(bat_tables) > 0 else []
    home_homers = _parse_home_run_events(bat_tables[1]) if len(bat_tables) > 1 else []
    if away_homers or home_homers:
        pitcher_name_lookup = _parse_pitcher_name_lookup(soup)
        text_html = await fetch(session, f"{BASE_URL}game/{game_id}/text")
        if text_html:
            pitcher_events = _parse_home_run_pitcher_events(
                text_html,
                away_raw=away_raw,
                home_raw=home_raw,
                away_starter=away_starter,
                home_starter=home_starter,
                pitcher_name_lookup=pitcher_name_lookup,
            )
            _enrich_home_run_pitchers(away_homers, pitcher_events["away"])
            _enrich_home_run_pitchers(home_homers, pitcher_events["home"])
    await asyncio.gather(
        _enrich_home_run_batter_hands(away_homers, session),
        _enrich_home_run_batter_hands(home_homers, session),
    )
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
        "賽事狀態": schedule_status,
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
        "客全壘打明細": away_homers,
        "主全壘打明細": home_homers,
        "客QS": away_qs,
        "主QS": home_qs,
    }


async def _schedule_status_for_game(
    game_id: str, date_str: str, session: aiohttp.ClientSession
) -> str:
    for league in ("first", "second"):
        html = await _fetch_once(
            session, f"{BASE_URL}schedule/{league}/all?date={date_str}"
        )
        if not html:
            continue
        soup = bs(html, "html.parser")
        game_path = f"/npb/game/{game_id}/"
        for entry in soup.find_all(class_="bb-calendarTable__data"):
            if not entry.find("a", href=lambda href: href and game_path in href):
                continue
            status = entry.find(class_="bb-calendarTable__status")
            if status:
                return status.get_text(" ", strip=True)
        for status in soup.find_all(class_="bb-calendarTable__status"):
            href = status.get("href", "")
            if game_path in href:
                return status.get_text(" ", strip=True)
        # bb-score layout (used on some date pages)
        anchor = soup.find("a", href=lambda h: h and game_path in h)
        if anchor:
            item = anchor.find_parent(class_="bb-score__item") or anchor.find_parent(
                class_="bb-score__content"
            )
            if item:
                link_el = item.find(class_="bb-score__link")
                if link_el:
                    return link_el.get_text(" ", strip=True)
    return ""


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
    return NpbRowsService(module=sys.modules[__name__]).schedule_row(seq, data)


def _analysis_date(date_str: str) -> str:
    return NpbRowsService.analysis_date(date_str)


def _analysis_game_type(data: dict) -> str:
    return NpbRowsService(module=sys.modules[__name__]).analysis_game_type(data)


def _analysis_team_league(team_name: str) -> str | None:
    return NpbRowsService(module=sys.modules[__name__]).analysis_team_league(team_name)


def _analysis_game_type_from_teams(away_team: str, home_team: str) -> str | None:
    return NpbRowsService(module=sys.modules[__name__]).analysis_game_type_from_teams(
        away_team, home_team
    )


def _analysis_day_night(game_time: str) -> str:
    return NpbRowsService.analysis_day_night(game_time)


def _analysis_team_name(team_name: str) -> str:
    return NpbRowsService(module=sys.modules[__name__]).analysis_team_name(team_name)


def _analysis_field(data: dict) -> str:
    return NpbRowsService(module=sys.modules[__name__]).analysis_field(data)


def _analysis_hand(hand: str) -> str:
    return NpbRowsService.analysis_hand(hand)


def _analysis_marks(away_score: int, home_score: int) -> tuple[str, str]:
    return NpbRowsService.analysis_marks(away_score, home_score)


def _analysis_innings(vals: list) -> tuple[list, str]:
    return NpbRowsService.analysis_innings(vals)


def _analysis_total_bases(batting: list) -> int:
    return NpbRowsService.analysis_total_bases(batting)


def _analysis_long_hits(batting: list) -> int:
    return NpbRowsService(module=sys.modules[__name__]).analysis_long_hits(batting)


def _analysis_qs(starter_pitch: list):
    return NpbRowsService.analysis_qs(starter_pitch)


def _analysis_starter_block(starter_pitch: list) -> list:
    return NpbRowsService(module=sys.modules[__name__]).analysis_starter_block(
        starter_pitch
    )


def _analysis_team_total_block(
    opposing_pitch: list,
    opposing_batting: list,
    own_batting: list,
    score: int,
    earned_runs: int,
    errors: int,
) -> list:
    return NpbRowsService(module=sys.modules[__name__]).analysis_team_total_block(
        opposing_pitch,
        opposing_batting,
        own_batting,
        score,
        earned_runs,
        errors,
    )


def _analysis_row(seq: int, data: dict) -> list:
    return NpbRowsService(module=sys.modules[__name__]).analysis_row(seq, data)


def _sailu_row(seq: int, data: dict) -> list:
    """
    Convert a game data dict to a 賽錄 sheet row covering columns A–AY (51 values).
    Columns AZ onwards are all formula-driven in the sheet and are left untouched.
    """
    return NpbRowsService(module=sys.modules[__name__]).sailu_row(seq, data)


def _sailu_formula_row(row_num: int) -> list[str]:
    """Build AZ:BT formula cells for one 賽錄 row."""
    return NpbRowsService.sailu_formula_row(row_num)


def _chunked(seq: list, size: int):
    yield from NpbRowsService.chunked(seq, size)


def _placeholder_rows(sheet) -> list[int]:
    return NpbRowsService.placeholder_rows(sheet)


def _ensure_target_sailu_capacity(sheet, needed_rows: int) -> list[int]:
    """Extend target 賽錄 with numbered placeholder rows and formulas if needed."""
    return NpbRowsService(module=sys.modules[__name__]).ensure_target_sailu_capacity(
        sheet, needed_rows
    )


def _write_regular_sailu_games(
    sheet,
    games: list[tuple[str, dict]],
    *,
    auto_extend_target: bool = False,
):
    """Write regular-season 賽錄 rows into placeholder rows, optionally extending them."""
    return NpbRowsService(module=sys.modules[__name__]).write_regular_sailu_games(
        sheet, games, auto_extend_target=auto_extend_target
    )


def _exhibition_row(data: dict) -> list[str]:
    """Convert scraped game data into 熱身賽紀錄's compact 28-column layout."""
    return NpbRowsService(module=sys.modules[__name__]).exhibition_row(data)


def _exhibition_identity(data: dict) -> tuple[str, str, str]:
    return NpbRowsService(module=sys.modules[__name__]).exhibition_identity(data)


def _existing_exhibition_identities(sheet) -> set[tuple[str, str, str]]:
    return NpbRowsService.existing_exhibition_identities(sheet)


async def update_sailu_sheet(session: aiohttp.ClientSession):
    """
    Fill finished games into 賽錄's pre-populated placeholder rows.

    The sheet pre-builds rows with formulas in columns AZ onwards and leaves
    columns B–AY blank as placeholders (column A / 編號 is already set).
    This function detects those placeholders and writes only B–AY into them,
    letting the existing formulas handle everything from AZ onwards.
    """
    return await NpbSailuService(module=sys.modules[__name__]).update(session)


def _analysis_identity(data: dict) -> tuple[str, str, str]:
    return NpbRowsService(module=sys.modules[__name__]).analysis_identity(data)


def _analysis_identity_from_row(row: list[str]) -> tuple[str, str, str] | None:
    return NpbRowsService.analysis_identity_from_row(row)


def _analysis_row_year(row: list[str]) -> int | None:
    return NpbRowsService.analysis_row_year(row)


def _analysis_row_date(row: list[str]) -> datetime | None:
    return NpbRowsService.analysis_row_date(row)


def _last_analysis_seq(rows: list[list[str]]) -> int:
    return NpbRowsService.last_analysis_seq(rows)


def _analysis_insert_index(rows: list[list[str]], date_str: str) -> int:
    """
    Return the 1-based worksheet row where a new analysis row should be inserted.
    Rows 1-2 are headers; data stays sorted by game date.
    """
    return NpbRowsService(module=sys.modules[__name__]).analysis_insert_index(
        rows, date_str
    )


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
    return await NpbAnalysisService(module=sys.modules[__name__]).update(
        session,
        year=year,
        game_ids=game_ids,
        target_date=target_date,
        full_season=full_season,
    )


def repair_analysis_leagues(year: int = ANALYSIS_SEASON) -> int:
    """
    Backfill 分析表紀錄 column D from the stored away/home teams.

    Existing rows do not keep Yahoo game IDs, so this repair uses the same visible
    team columns that identify the row: I=away team and L=home team.
    """
    return NpbAnalysisService(module=sys.modules[__name__]).repair_leagues(year)


def update_huizi_sheet(today: datetime | str | None = None):
    """
    Refresh 彙資 with a target date's finished games from 分析表紀錄.

    彙資 keeps the same 83-column shape and reserves rows 3-8 for the date's six
    possible NPB games.
    """
    return NpbHuiziService(module=sys.modules[__name__]).update(today)


# --- Sheet building ---


def build_block_values(team_key: str, games: list[dict]) -> list[list]:
    """
    Build 13 rows × 12 cols for one team block:
      row 0:    header (team name + 11 column labels)
      rows 1-10: game data (oldest → newest, empty rows if fewer than 10 games)
      row 11:   近十場 平均
      row 12:   近五場 平均
    """
    return NpbLeagueSheetService(module=sys.modules[__name__]).build_block_values(
        team_key, games
    )


def _pitcher_font_size(name: str) -> int:
    """10pt default; shrink longer pitcher names to fit the narrow column."""
    return NpbLeagueSheetService.pitcher_font_size(name)


def _pitcher_font_requests(
    sheet_id: int, games: list[dict], game_start_row: int, col_start: int
) -> list[dict]:
    """
    One repeatCell request per game row that sets the pitcher cell font size.
    Also resets empty rows to default (10) so stale small fonts don't linger.
    Pitcher column = col_start + 2 (1-indexed) → col_start + 1 (0-indexed).
    """
    return NpbLeagueSheetService(module=sys.modules[__name__]).pitcher_font_requests(
        sheet_id, games, game_start_row, col_start
    )


def _to_number(value) -> Optional[float]:
    return NpbLeagueSheetService.to_number(value)


def _font_color_request(
    sheet_id: int, row_0idx: int, col_0idx: int, hex_color: str
) -> dict:
    return NpbLeagueSheetService(module=sys.modules[__name__]).font_color_request(
        sheet_id, row_0idx, col_0idx, hex_color
    )


def _game_font_color_requests(
    sheet_id: int, games: list[dict], game_start_row: int, col_start: int
) -> list[dict]:
    """Colour game-row score and hit cells to match the CPBL 近十場 rules."""
    return NpbLeagueSheetService(module=sys.modules[__name__]).game_font_color_requests(
        sheet_id, games, game_start_row, col_start
    )


def _header_format_request(
    sheet_id: int, team_key: str, header_row: int, col_start: int
) -> dict:
    """Build a Sheets API repeatCell request to colour one header row."""
    return NpbLeagueSheetService(module=sys.modules[__name__]).header_format_request(
        sheet_id, team_key, header_row, col_start
    )


def update_league_sheet(
    sheet_name: str,
    matchups: list[tuple[str, str]],
    all_games: dict[str, list[dict]],
):
    """
    Write all 6 team blocks into one sheet.
    matchups[i] = (away_key, home_key) → away goes to top block i, home to bottom block i.
    """
    return NpbLeagueSheetService(module=sys.modules[__name__]).update_league_sheet(
        sheet_name, matchups, all_games
    )


# --- Main ---


async def run_once(
    matchup_date: str | None = None,
    league_sheet_suffix: str | None = None,
    league_sheet_overrides: dict[str, str] | None = None,
    recent_only: bool = False,
):
    if league_sheet_suffix is None:
        league_sheet_suffix = os.getenv("NPB_LEAGUE_SHEET_SUFFIX", "")
    return await NpbUpdateService(module=sys.modules[__name__]).run_once(
        matchup_date=matchup_date,
        league_sheet_suffix=league_sheet_suffix,
        league_sheet_overrides=league_sheet_overrides,
        recent_only=recent_only,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update NPB Google Sheets.")
    parser.add_argument(
        "--matchup-date",
        help=(
            "First date to use for 近十場 matchup ordering: today, tomorrow, "
            "or YYYY-MM-DD. Defaults to tomorrow; NPB_MATCHUP_DATE is also supported."
        ),
    )
    parser.add_argument(
        "--sheet-copy-suffix",
        default=os.getenv("NPB_LEAGUE_SHEET_SUFFIX", ""),
        help=(
            "Optional suffix appended to NPB 近十場 league sheet names, e.g. "
            "' 的副本' to write 近十場a 的副本 / 近十場b 的副本."
        ),
    )
    parser.add_argument(
        "--central-recent-sheet",
        default="",
        help="Exact worksheet title for 央盟 近十場 output.",
    )
    parser.add_argument(
        "--pacific-recent-sheet",
        default="",
        help="Exact worksheet title for 洋盟 近十場 output.",
    )
    parser.add_argument(
        "--repair-analysis-leagues",
        action="store_true",
        help="Backfill 分析表紀錄 column D to 央盟/洋盟/交流戰 for existing rows.",
    )
    parser.add_argument(
        "--analysis-year",
        type=int,
        default=ANALYSIS_SEASON,
        help=f"Season year for analysis repairs. Defaults to {ANALYSIS_SEASON}.",
    )
    parser.add_argument(
        "--create-prediction",
        "--predict",
        nargs="?",
        const=PREDICTION_PROMPT_SENTINEL,
        metavar="HOME_TEAM",
        dest="create_prediction",
        help="Create an NPB prediction row by home team. Omit HOME_TEAM for prompts.",
    )
    parser.add_argument(
        "-p",
        "--pick",
        help="Team for winner markets, or over/under for total markets.",
    )
    parser.add_argument(
        "-m",
        "--market",
        default="final_winner",
        choices=[
            "half_winner",
            "final_winner",
            "half_total",
            "final_total",
            "final_handicap",
            "half_handicap",
        ],
        help="Prediction market. Defaults to final_winner.",
    )
    parser.add_argument(
        "-l",
        "--line",
        type=float,
        help="Score line for total/handicap predictions (e.g. 0.5, -1.5).",
    )
    parser.add_argument(
        "-r",
        "--rate",
        type=float,
        help="Prediction return rate, e.g. 0.92.",
    )
    parser.add_argument(
        "-s",
        "--stake",
        type=float,
        default=None,
        help=f"Stake size for the prediction. Defaults to {PREDICTION_DEFAULT_STAKE}.",
    )
    parser.add_argument(
        "--game-date", default="", help="Optional prediction game date."
    )
    parser.add_argument(
        "--away-team", default="", help="Optional prediction away team."
    )
    parser.add_argument(
        "--home-team", default="", help="Optional prediction home team."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build prediction output without writing to Google Sheets.",
    )
    parser.add_argument(
        "--recent-only",
        action="store_true",
        help="Only update NPB 近十場 sheets; skip 賽錄, 分析表, prediction reveals, and 彙資.",
    )
    args = parser.parse_args()
    recent_sheet_overrides = {}
    if args.central_recent_sheet:
        recent_sheet_overrides["央盟"] = args.central_recent_sheet
    if args.pacific_recent_sheet:
        recent_sheet_overrides["洋盟"] = args.pacific_recent_sheet

    if args.create_prediction is not None:
        try:
            values = _prediction_cli_values(args)
            values = asyncio.run(
                _prediction_resolve_cli_game(
                    values,
                    confirm=args.create_prediction == PREDICTION_PROMPT_SENTINEL,
                )
            )
        except ValueError as exc:
            parser.error(str(exc))
        if not values["game_id"]:
            parser.error("--create-prediction could not resolve a game ID")
        if not values["pick"] or values["rate"] is None:
            parser.error("--create-prediction requires --pick and --rate")
        if values["market"] in {"half_total", "final_total"} and values["line"] is None:
            parser.error("--market half_total/final_total requires --line")
        result = create_npb_prediction(
            values["game_id"],
            values["pick"],
            values["rate"],
            market=values["market"],
            line=values["line"],
            stake=values["stake"],
            game_date=values["game_date"],
            away_team=values["away_team"],
            home_team=values["home_team"],
            dry_run=args.dry_run,
        )
        print(result["prediction_text"])
    elif args.repair_analysis_leagues:
        repair_analysis_leagues(args.analysis_year)
    else:
        asyncio.run(
            NpbUpdateService(module=sys.modules[__name__]).run_once(
                matchup_date=args.matchup_date,
                league_sheet_suffix=args.sheet_copy_suffix,
                league_sheet_overrides=recent_sheet_overrides,
                recent_only=args.recent_only,
            )
        )
