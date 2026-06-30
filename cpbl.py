import json
import os
import sys
import requests
from bs4 import BeautifulSoup
import gspread
from datetime import datetime, timedelta
from dotenv import load_dotenv

from baseball.cpbl_services import (
    CpblGameSheetService,
    CpblHuiziService,
    CpblScheduleService,
    CpblStatusService,
)
from baseball.sheets import GoogleSheetsClient
from utils import send_telegram

from dotenv import load_dotenv

load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")
# --- Configuration ---
SPREADSHEET_KEY = os.getenv("SPREADSHEET_KEY")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
_sheets_client = GoogleSheetsClient(credentials_file=CREDENTIALS_FILE)
# KindCode: A = 正式賽, G = 熱身賽
WORKSHEET_MAP = {
    "A": "賽程",
    "G": "熱身賽賽程",
}
STATUS_WORKSHEET_NAME = "CPBL狀態"
STATUS_HEADERS = ["Date", "KindCode", "GameSno", "Status", "Resolved", "UpdatedAt"]
NO_GAMES_SENTINEL = "__NO_GAMES__"
SUSPENDED_STATUS_KEYWORD = "保留"
TERMINAL_STATUS_KEYWORDS = (
    "延賽",
    "取消",
    "裁定",
    "沒收",
    "無效",
    "中止",
    "No Game",
    "NO GAME",
    "NoGame",
    "NOGAME",
)

TEAM_MAP = {
    "樂天桃猿": "樂天",
    "統一7-ELEVEn獅": "統一7-ELEVEn",
    "中信兄弟": "中信兄弟",
    "味全龍": "味全",
    "富邦悍將": "富邦",
    "台鋼雄鷹": "台鋼",
}

REQUEST_TIMEOUT = (10, 30)

_habit_cache: dict = {}


def get_session():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.cpbl.com.tw/schedule",
            "Origin": "https://www.cpbl.com.tw",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
    )

    proxy_url = os.environ.get("DECODO_PROXY_URL")
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}

    return session


def get_worksheet(kind_code):
    worksheet_name = WORKSHEET_MAP.get(kind_code, "賽程")
    return _sheets_client.worksheet(SPREADSHEET_KEY, worksheet_name)


def get_spreadsheet():
    return _sheets_client.spreadsheet(SPREADSHEET_KEY)


def get_status_worksheet():
    spreadsheet = get_spreadsheet()
    try:
        sheet = spreadsheet.worksheet(STATUS_WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(
            title=STATUS_WORKSHEET_NAME, rows=1000, cols=len(STATUS_HEADERS)
        )
        sheet.update("A1", [STATUS_HEADERS], value_input_option="USER_ENTERED")
        return sheet

    values = sheet.get_all_values()
    if not values:
        sheet.update("A1", [STATUS_HEADERS], value_input_option="USER_ENTERED")
    elif values[0][: len(STATUS_HEADERS)] != STATUS_HEADERS:
        sheet.update("A1", [STATUS_HEADERS], value_input_option="USER_ENTERED")
    return sheet


def fetch_schedule(year, month, kind_code, session):
    try:
        response = session.get(
            "https://www.cpbl.com.tw/schedule", timeout=REQUEST_TIMEOUT
        )
        soup = BeautifulSoup(response.text, "html.parser")

        # 從頁面 HTML 抓 hardcoded 的 token（格式是 token1:token2）
        import re

        token_match = re.search(r"RequestVerificationToken:\s*'([^']+)'", response.text)
        token = token_match.group(1) if token_match else ""
        print(f"[token] {token}")

        calendar_str = f"{year}/{int(month):02d}/01"
        payload = {
            "calendar": calendar_str,
            "location": "",
            "kindCode": kind_code,
        }
        headers = {
            "RequestVerificationToken": token,  # 注意大小寫和冒號格式
            "x-requested-with": "XMLHttpRequest",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://www.cpbl.com.tw",
            "referer": "https://www.cpbl.com.tw/schedule",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        }

        post_response = session.post(
            "https://www.cpbl.com.tw/schedule/getgamedatas",
            data=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        print(f"[status] {post_response.status_code}")
        print(f"[response] {post_response.text[:300]}")

        result = post_response.json()
        if result.get("Success"):
            return json.loads(result.get("GameDatas", "[]"))
        return []
    except Exception as e:
        print(f"Error fetching schedule: {e}")
        raise


def is_game_recorded(game_sno, year, sheet):
    """回傳 True 如果 B 欄有相同 game_sno 且 C 欄日期包含相同年份。"""
    return CpblGameSheetService(module=sys.modules[__name__]).is_game_recorded(
        game_sno, year, sheet
    )


def is_terminal_game_status(status: str) -> bool:
    if not status:
        return False
    if status == "比賽結束":
        return True
    normalized = status.strip()
    return any(keyword in normalized for keyword in TERMINAL_STATUS_KEYWORDS)


def is_non_finished_terminal_status(status: str) -> bool:
    return bool(status) and status != "比賽結束" and is_terminal_game_status(status)


def is_suspended_game_status(status: str) -> bool:
    return bool(status) and SUSPENDED_STATUS_KEYWORD in status.strip()


def _extract_game_detail(data, game_sno):
    return CpblGameSheetService(module=sys.modules[__name__]).extract_game_detail(
        data, game_sno
    )


def _status_records(status_sheet):
    return CpblStatusService(module=sys.modules[__name__]).records(status_sheet)


def _status_records_for_date(status_sheet, date_str, kind_code):
    return CpblStatusService(module=sys.modules[__name__]).records_for_date(
        status_sheet, date_str, kind_code
    )


def _all_games_resolved_for_date(status_sheet, date_str, kind_code):
    service = CpblStatusService(module=sys.modules[__name__])
    return service.all_games_resolved_for_date(status_sheet, date_str, kind_code)


def _unresolved_game_snos_for_date(status_sheet, date_str, kind_code):
    service = CpblStatusService(module=sys.modules[__name__])
    return service.unresolved_game_snos_for_date(status_sheet, date_str, kind_code)


def _upsert_status(status_sheet, date_str, kind_code, game_sno, status, resolved):
    return CpblStatusService(module=sys.modules[__name__]).upsert(
        status_sheet, date_str, kind_code, game_sno, status, resolved
    )


def _game_date_str(game):
    return CpblGameSheetService.game_date_str(game)


def fetch_game_data(game_sno, year, kind_code, session):
    """從 box/getlive 抓取比賽的詳細資料，回傳 JSON dict 或 None。"""
    url = f"https://www.cpbl.com.tw/box/index?gameSno={game_sno}&year={year}&kindCode={kind_code}"
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            print(f"[box] HTTP {response.status_code} for game {game_sno}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        token_input = soup.find("input", {"name": "__RequestVerificationToken"})
        if not token_input:
            print(f"Token not found for game {game_sno}.")
            return None
        token = token_input.get("value")

        payload = {
            "__RequestVerificationToken": token,
            "GameSno": game_sno,
            "KindCode": kind_code,
            "Year": year,
            "SelectKindCode": kind_code,
            "SelectYear": year,
            "SelectMonth": str(datetime.now().month),
        }
        post_response = session.post(
            "https://www.cpbl.com.tw/box/getlive", data=payload, timeout=REQUEST_TIMEOUT
        )
        if post_response.status_code != 200:
            print(f"[getlive] HTTP {post_response.status_code} for game {game_sno}")
            return None

        return post_response.json()
    except Exception as e:
        print(f"Error fetching game {game_sno}: {e}")
        return None


def get_pitching_habit(acnt_id, session):
    if not acnt_id:
        return ""
    if acnt_id in _habit_cache:
        return _habit_cache[acnt_id]
    result = ""
    try:
        url = f"https://www.cpbl.com.tw/team/person?acnt={acnt_id}"
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            bt_dd = soup.find("dd", class_="b_t")
            if bt_dd:
                desc = bt_dd.find("div", class_="desc").text.strip()
                if "左投" in desc:
                    result = "左"
                elif "右投" in desc:
                    result = "右"
    except Exception as e:
        print(f"Error fetching habit for {acnt_id}: {e}")
    _habit_cache[acnt_id] = result
    return result


def _get_pitching_stats(pitching, ptype, is_starter=False):
    """從 PitchingJson 計算單邊（客/主）投球統計。回傳 (stats[13], name, acnt)。"""
    return CpblGameSheetService.pitching_stats(pitching, ptype, is_starter)


def _get_batting_stats(batting, pitching, ptype):
    """從 BattingJson + PitchingJson 計算單邊（客/主）打擊統計。回傳 stats[16]。"""
    return CpblGameSheetService.batting_stats(batting, pitching, ptype)


def process_and_update_sheet(data, game_sno, year, kind_code, session, sheet):
    """解析比賽資料並寫入對應 worksheet。回傳 True 代表成功寫入。"""
    return CpblGameSheetService(module=sys.modules[__name__]).process_and_update_sheet(
        data, game_sno, year, kind_code, session, sheet
    )


def update_huizi(year: str = None):
    """
    找出今天的比賽資料（來自 賽程 或 熱身賽賽程），
    清除 彙資 B4:DU6 後貼上最多 3 場比賽（對應 VS1/VS2/VS3）。
    """
    return CpblHuiziService(module=sys.modules[__name__]).update(year=year)


def run_once(year: str = None, kind_codes=None):
    """
    執行一次檢查：抓賽程，若比賽結束且尚未記錄就寫入 sheet。
    由 GitHub Actions cron 觸發，不需要自己維持迴圈。

    Args:
        year: 賽季年份，預設為今年
        kind_codes: 要監控的賽事種類列表，預設 ["A", "G"]（正式賽 + 熱身賽）
    """
    return CpblScheduleService(module=sys.modules[__name__]).run_once(
        year=year, kind_codes=kind_codes
    )


def main(game_sno: str, year: str, kind_code="A"):
    """
    手動跑單場比賽。

    Args:
        game_sno: 比賽編號
        year: 年份
        kind_code: "A" = 正式賽, "G" = 熱身賽
    """

    return CpblScheduleService(module=sys.modules[__name__]).update_game(
        game_sno=game_sno, year=year, kind_code=kind_code
    )


if __name__ == "__main__":
    # GitHub Actions cron 觸發時執行此入口
    try:
        CpblScheduleService(module=sys.modules[__name__]).run_once(kind_codes=["A"])
        send_telegram("CPBL schedule update completed successfully.")
    except Exception as e:
        send_telegram(f"CPBL schedule update failed: {e}")
        raise

    # 手動跑單場範例（本地測試用）：
    # main(game_sno="1", year="2025", kind_code="G")  # 熱身賽
    # main(game_sno="239", year="2025", kind_code="A")  # 正式賽
