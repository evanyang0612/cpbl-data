import json
import os
import sys
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
# --- Configuration ---
SPREADSHEET_KEY = os.getenv("SPREADSHEET_KEY")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
# KindCode: A = 正式賽, G = 熱身賽
WORKSHEET_MAP = {
    "A": "賽程",
    "G": "熱身賽賽程",
}

TEAM_MAP = {
    "樂天桃猿": "樂天",
    "統一7-ELEVEn獅": "統一7-ELEVEn",
    "中信兄弟": "中信兄弟",
    "味全龍": "味全",
    "富邦悍將": "富邦",
    "台鋼雄鷹": "台鋼",
}


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

    return session


def get_worksheet(kind_code):
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    # 優先使用環境變數（GitHub Actions），否則使用本地憑證檔
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=scope
        )
    else:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    client = gspread.authorize(creds)
    worksheet_name = WORKSHEET_MAP.get(kind_code, "賽程")
    return client.open_by_key(SPREADSHEET_KEY).worksheet(worksheet_name)


def fetch_schedule(year, month, kind_code, session):
    try:
        response = session.get("https://www.cpbl.com.tw/schedule")
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
    col_b = sheet.col_values(2)
    for idx, val in enumerate(col_b, start=1):
        if str(val) == str(game_sno):
            row_vals = sheet.row_values(idx)
            if len(row_vals) > 2 and str(year) in str(row_vals[2]):
                return True
    return False


def fetch_game_data(game_sno, year, kind_code, session):
    """從 box/getlive 抓取比賽的詳細資料，回傳 JSON dict 或 None。"""
    url = f"https://www.cpbl.com.tw/box/index?gameSno={game_sno}&year={year}&kindCode={kind_code}"
    try:
        response = session.get(url)
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
            "https://www.cpbl.com.tw/box/getlive", data=payload
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
    try:
        url = f"https://www.cpbl.com.tw/team/person?acnt={acnt_id}"
        response = session.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            bt_dd = soup.find("dd", class_="b_t")
            if bt_dd:
                desc = bt_dd.find("div", class_="desc").text.strip()
                if "左投" in desc:
                    return "左"
                elif "右投" in desc:
                    return "右"
    except Exception as e:
        print(f"Error fetching habit for {acnt_id}: {e}")
    return ""


def _get_pitching_stats(pitching, ptype, is_starter=False):
    """從 PitchingJson 計算單邊（客/主）投球統計。回傳 (stats[13], name, acnt)。"""
    stats = [0] * 13
    target_pitchers = [
        p
        for p in pitching
        if str(p.get("VisitingHomeType")) == str(ptype)
        and (not is_starter or p.get("RoleType") == "先發")
    ]
    name = (
        target_pitchers[0].get("PitcherName", "")
        if is_starter and target_pitchers
        else ""
    )
    acnt = (
        target_pitchers[0].get("PitcherAcnt", "")
        if is_starter and target_pitchers
        else ""
    )
    total_outs = 0
    for p in target_pitchers:
        total_outs += int(p.get("InningPitchedCnt", 0)) * 3 + int(
            p.get("InningPitchedDiv3Cnt", 0)
        )
        stats[1] += int(p.get("PlateAppearances", 0))
        stats[2] += int(p.get("PitchCnt", 0))
        stats[3] += int(p.get("StrikeCnt", 0))
        stats[4] += int(p.get("HittingCnt", 0))
        stats[5] += int(p.get("HomeRunCnt", 0))
        stats[6] += int(p.get("BasesONBallsCnt", 0))
        stats[7] += int(p.get("HitBYPitchCnt", 0))
        stats[8] += int(p.get("StrikeOutCnt", 0))
        stats[9] += int(p.get("WildPitchCnt", 0))
        stats[10] += int(p.get("BalkCnt", 0))
        stats[11] += int(p.get("RunCnt", 0))
        stats[12] += int(p.get("EarnedRunCnt", 0))
    stats[0] = total_outs // 3 if total_outs % 3 == 0 else round(total_outs / 3, 3)
    return stats, name, acnt


def _get_batting_stats(batting, pitching, ptype):
    """從 BattingJson + PitchingJson 計算單邊（客/主）打擊統計。回傳 stats[16]。"""
    stats = [0] * 16
    target_batters = [
        b for b in batting if str(b.get("VisitingHomeType")) == str(ptype)
    ]
    for b in target_batters:
        stats[0] += int(b.get("HitCnt", 0))  # 打數 (AB)
        stats[1] += int(b.get("ScoreCnt", 0))
        stats[2] += int(b.get("HittingCnt", 0))  # 安打 (H)
        stats[3] += int(b.get("RunBattedINCnt", 0))
        stats[4] += int(b.get("TwoBaseHitCnt", 0))
        stats[5] += int(b.get("ThreeBaseHitCnt", 0))
        stats[6] += int(b.get("HomeRunCnt", 0))
        stats[7] += int(b.get("DoublePlayBatCnt", 0))
        stats[8] += int(b.get("BasesONBallsCnt", 0))
        stats[9] += int(b.get("HitBYPitchCnt", 0))
        stats[10] += int(b.get("StrikeOutCnt", 0))
        stats[11] += int(b.get("SacrificeHitCnt", 0))
        stats[12] += int(b.get("SacrificeFlyCnt", 0))
        stats[13] += int(b.get("StealBaseOKCnt", 0))
        stats[14] += int(b.get("StealBaseFailCnt", 0))
        stats[15] += int(b.get("ErrorCnt", 0))
    # 加上 PitchingJson 的失誤
    for p in pitching:
        if str(p.get("VisitingHomeType")) == str(ptype):
            stats[15] += int(p.get("ErrorCnt", 0))
    return stats


def process_and_update_sheet(data, game_sno, year, kind_code, session, sheet):
    """解析比賽資料並寫入對應 worksheet。回傳 True 代表成功寫入。"""
    curt_game_detail = json.loads(data.get("CurtGameDetailJson", "{}"))
    game_detail_list = json.loads(data.get("GameDetailJson", "[]"))

    # 找到對應的 game_detail
    game_detail = None
    if str(curt_game_detail.get("GameSno")) == str(game_sno):
        game_detail = curt_game_detail
    else:
        for g in game_detail_list:
            if str(g.get("GameSno")) == str(game_sno):
                game_detail = g
                break

    if not game_detail:
        if game_detail_list:
            game_detail = game_detail_list[0]
            print(
                f"Warning: No exact match for GameSno {game_sno}. Using first available."
            )
        else:
            print("No game detail found.")
            return False

    # 只在比賽結束時更新
    if game_detail.get("GameStatusChi") != "比賽結束":
        print(f"Game {game_sno} ({year}) is not finished yet. Skipping.")
        return False

    # 貼上前先再確認沒有重複（double-check）
    if is_game_recorded(game_sno, year, sheet):
        print(f"Game {game_sno} ({year}) already recorded. Skipping.")
        return True

    scoreboard = json.loads(data.get("ScoreboardJson", "[]"))
    pitching = json.loads(data.get("PitchingJson", "[]"))
    batting = json.loads(data.get("BattingJson", "[]"))

    # --- 決定目標列 ---
    col_b_values = sheet.col_values(2)
    target_row = len(col_b_values) + 1
    print(f"Targeting Row {target_row} for Game {game_sno} ({kind_code})...")

    # --- 準備資料 (125 欄: A to DU) ---
    update_values = [""] * 125
    update_values[0] = game_detail.get("GameStatusChi", "")
    update_values[1] = game_sno
    update_values[2] = game_detail.get("GameDate", "").split("T")[0].replace("/", "-")
    update_values[3] = TEAM_MAP.get(
        game_detail.get("VisitingTeamName", ""), game_detail.get("VisitingTeamName", "")
    )
    update_values[5] = TEAM_MAP.get(
        game_detail.get("HomeTeamName", ""), game_detail.get("HomeTeamName", "")
    )
    update_values[7] = game_detail.get("FieldAbbe", "")
    update_values[8] = curt_game_detail.get("HeadUmpire") or game_detail.get(
        "HeadUmpire", ""
    )

    # 客隊逐局得分
    for score in scoreboard:
        if str(score.get("VisitingHomeType")) == "1":
            inning = int(float(score.get("InningSeq", 0)))
            if 1 <= inning <= 12:
                update_values[9 + inning - 1] = int(float(score.get("ScoreCnt", 0)))

    v_batting = _get_batting_stats(batting, pitching, 1)
    update_values[21] = game_detail.get("VisitingTotalScore", 0)
    update_values[22] = v_batting[2]
    update_values[23] = v_batting[15]

    # 主隊逐局得分（含 X 判斷）
    for score in scoreboard:
        if str(score.get("VisitingHomeType")) == "2":
            inning = int(float(score.get("InningSeq", 0)))
            if 1 <= inning <= 12:
                score_val = int(float(score.get("ScoreCnt", 0)))
                if inning >= 9 and game_detail.get("GameStatusChi") == "比賽結束":
                    v_total = int(game_detail.get("VisitingTotalScore", 0))
                    h_total = int(game_detail.get("HomeTotalScore", 0))
                    if h_total > v_total:
                        h_score_before = sum(
                            int(float(s2.get("ScoreCnt", 0)))
                            for s2 in scoreboard
                            if str(s2.get("VisitingHomeType")) == "2"
                            and int(float(s2.get("InningSeq", 0))) < inning
                        )
                        v_score_up_to = sum(
                            int(float(s2.get("ScoreCnt", 0)))
                            for s2 in scoreboard
                            if str(s2.get("VisitingHomeType")) == "1"
                            and int(float(s2.get("InningSeq", 0))) <= inning
                        )
                        if h_score_before > v_score_up_to:
                            score_val = "X"
                update_values[24 + inning - 1] = score_val

    h_batting = _get_batting_stats(batting, pitching, 2)
    update_values[36] = game_detail.get("HomeTotalScore", 0)
    update_values[37] = h_batting[2]
    update_values[38] = h_batting[15]

    # 投球資料
    v_starter_stats, v_starter_name, v_starter_acnt = _get_pitching_stats(pitching, 1, True)
    update_values[4] = v_starter_name
    for i in range(13):
        update_values[39 + i] = v_starter_stats[i]

    v_total_pitch, _, _ = _get_pitching_stats(pitching, 1, False)
    for i in range(13):
        update_values[52 + i] = v_total_pitch[i]

    h_starter_stats, h_starter_name, h_starter_acnt = _get_pitching_stats(pitching, 2, True)
    update_values[6] = h_starter_name
    for i in range(13):
        update_values[65 + i] = h_starter_stats[i]

    h_total_pitch, _, _ = _get_pitching_stats(pitching, 2, False)
    for i in range(13):
        update_values[78 + i] = h_total_pitch[i]

    update_values[91] = get_pitching_habit(v_starter_acnt, session)
    update_values[92] = get_pitching_habit(h_starter_acnt, session)

    # 打擊資料
    for i in range(16):
        update_values[93 + i] = v_batting[i]
    for i in range(16):
        update_values[109 + i] = h_batting[i]

    # --- 寫入 ---
    sheet.update(
        range_name=f"A{target_row}",
        values=[update_values],
        value_input_option="USER_ENTERED",
    )
    print(f"Successfully updated Row {target_row} (Game {game_sno}, {kind_code}).")
    return True


def update_huizi(year: str = None):
    """
    找出今天的比賽資料（來自 賽程 或 熱身賽賽程），
    清除 彙資 B4:DU6 後貼上最多 3 場比賽（對應 VS1/VS2/VS3）。
    """
    if year is None:
        year = str(datetime.now().year)

    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"Updating 彙資 for {today_str}...")

    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=scope
        )
    else:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_KEY)
    huizi = spreadsheet.worksheet("彙資")

    # Collect today's games from 賽程 then 熱身賽賽程
    today_games = []
    for sheet_name in WORKSHEET_MAP.values():
        sheet = spreadsheet.worksheet(sheet_name)
        col_c = sheet.col_values(3)  # column C = date
        for idx, val in enumerate(col_c, start=1):
            if today_str in str(val):
                row_data = sheet.row_values(idx)
                today_games.append(row_data[1:125])  # paste columns B through DU

    if not today_games:
        print(f"No games found for {today_str}. Keeping existing 彙資 data.")
        return

    # Only clear if we have today's data to replace with
    huizi.batch_clear(["B4:DU6"])
    print("Cleared 彙資 B4:DU6.")

    # Paste up to 3 games into rows 4-6
    for i, game_data in enumerate(today_games[:3]):
        row_num = 4 + i
        huizi.update(
            range_name=f"B{row_num}",
            values=[game_data],
            value_input_option="USER_ENTERED",
        )
        print(f"Pasted game {i + 1} into 彙資 row {row_num}.")

    print(f"彙資 updated with {min(len(today_games), 3)} game(s) for {today_str}.")


def run_once(year: str = None, kind_codes=None):
    """
    執行一次檢查：抓賽程，若比賽結束且尚未記錄就寫入 sheet。
    由 GitHub Actions cron 觸發，不需要自己維持迴圈。

    Args:
        year: 賽季年份，預設為今年
        kind_codes: 要監控的賽事種類列表，預設 ["A", "G"]（正式賽 + 熱身賽）
    """
    if year is None:
        year = str(datetime.now().year)
    if kind_codes is None:
        kind_codes = ["A", "G"]

    session = get_session()
    now = datetime.now()
    current_month = str(now.month)
    errors = []
    print(
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Run started (year={year}, kind_codes={kind_codes})"
    )

    for kind_code in kind_codes:
        try:
            games = fetch_schedule(year, current_month, kind_code, session)
        except Exception as e:
            errors.append(f"fetch_schedule({kind_code}): {e}")
            continue
        if not games:
            continue

        sheet = get_worksheet(kind_code)

        # 一次性讀取已記錄的場次編號
        col_b_cache = sheet.col_values(2)
        col_c_cache = sheet.col_values(3)
        existing_snos = {
            str(sno)
            for sno, date_val in zip(col_b_cache, col_c_cache)
            if sno and str(year) in str(date_val)
        }

        # 過濾出「過去且未記錄」的候選場次（仿照 NPB 的 new_ids 邏輯）
        candidates = []
        for game in games:
            game_sno = str(game.get("GameSno"))
            game_date_str = game.get("GameDate", "").split("T")[0]
            try:
                game_date = datetime.strptime(game_date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if game_date.date() > now.date():
                continue
            if game_sno in existing_snos:
                continue
            candidates.append(game)

        print(f"[{kind_code}] {len(candidates)} unrecorded past game(s) to check.")

        # 只對候選場次發 HTTP 請求
        for game in candidates:
            game_sno = str(game.get("GameSno"))
            print(f"Processing GameSno {game_sno} ({kind_code})...")
            try:
                data = fetch_game_data(game_sno, year, kind_code, session)
                if not data:
                    continue
                written = process_and_update_sheet(
                    data, game_sno, year, kind_code, session, sheet
                )
                if written:
                    existing_snos.add(game_sno)
            except Exception as e:
                errors.append(f"game {game_sno} ({kind_code}): {e}")
                continue

            time.sleep(2)  # 避免打 API 太快

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Run finished.")

    try:
        update_huizi(year=year)
    except Exception as e:
        errors.append(f"update_huizi: {e}")

    if errors:
        print(f"\n[ERROR] {len(errors)} failure(s) occurred:")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)


def main(game_sno: str, year: str, kind_code="A"):
    """
    手動跑單場比賽。

    Args:
        game_sno: 比賽編號
        year: 年份
        kind_code: "A" = 正式賽, "G" = 熱身賽
    """

    session = get_session()
    data = fetch_game_data(game_sno, year, kind_code, session)
    if not data:
        return

    sheet = get_worksheet(kind_code)
    process_and_update_sheet(data, game_sno, year, kind_code, session, sheet)


if __name__ == "__main__":
    # GitHub Actions cron 觸發時執行此入口
    run_once(year=str(datetime.now().year), kind_codes=["A"])

    # 手動跑單場範例（本地測試用）：
    # main(game_sno="1", year="2025", kind_code="G")  # 熱身賽
    # main(game_sno="239", year="2025", kind_code="A")  # 正式賽
