import json
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import time


def main():
    # Setup Session
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.cpbl.com.tw/",
        }
    )

    # 1. Fetch Data
    game_sno = "239"
    year = "2025"
    url = f"https://www.cpbl.com.tw/box/index?gameSno={game_sno}&year={year}&kindCode=A"

    try:
        response = session.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch page: {response.status_code}")
            return

        soup = BeautifulSoup(response.text, "html.parser")
        token_input = soup.find("input", {"name": "__RequestVerificationToken"})
        if not token_input:
            print("Token not found.")
            return
        token = token_input.get("value")

        payload = {
            "__RequestVerificationToken": token,
            "GameSno": game_sno,
            "KindCode": "A",
            "Year": year,
            "SelectKindCode": "A",
            "SelectYear": year,
            "SelectMonth": "4",
        }

        post_url = "https://www.cpbl.com.tw/box/getlive"
        post_response = session.post(post_url, data=payload)

        if post_response.status_code != 200:
            print("Failed to fetch API data.")
            return

        data = post_response.json()

        process_and_update_sheet(data, game_sno, year, session)

    except Exception as e:
        print(f"Error in main: {e}")


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


def process_and_update_sheet(data, game_sno, year, session):
    # --- Parse JSON ---
    curt_game_detail = json.loads(data.get("CurtGameDetailJson", "{}"))
    game_detail_list = json.loads(data.get("GameDetailJson", "[]"))

    # Identify the correct game detail that matches game_sno
    game_detail = None
    if str(curt_game_detail.get("GameSno")) == str(game_sno):
        game_detail = curt_game_detail
    else:
        for g in game_detail_list:
            if str(g.get("GameSno")) == str(game_sno):
                game_detail = g
                break

    if not game_detail:
        # Fallback to first item if no match found (old behavior, but safer to warn)
        if game_detail_list:
            game_detail = game_detail_list[0]
            print(
                f"Warning: No exact match for GameSno {game_sno}. Using first available game {game_detail.get('GameSno')}."
            )
        else:
            print("No game detail found.")
            return

    # Only update if the game is finished
    if game_detail.get("GameStatusChi") != "比賽結束":
        print(f"Game {game_sno} ({year}) is not finished. Skipping.")
        return

    scoreboard = json.loads(data.get("ScoreboardJson", "[]"))
    pitching = json.loads(data.get("PitchingJson", "[]"))
    batting = json.loads(data.get("BattingJson", "[]"))

    # Helpers
    def get_pitching_stats(ptype, is_starter=False):
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

        if total_outs % 3 == 0:
            stats[0] = total_outs // 3
        else:
            stats[0] = round(total_outs / 3, 3)
        return stats, name, acnt

    def get_batting_stats(ptype):
        stats = [0] * 16
        target_batters = [
            b for b in batting if str(b.get("VisitingHomeType")) == str(ptype)
        ]
        for b in target_batters:
            # Swap HitCnt and HittingCnt to match correct data (AB vs H)
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

        # Also add errors from PitchingJson
        target_pitchers = [
            p for p in pitching if str(p.get("VisitingHomeType")) == str(ptype)
        ]
        for p in target_pitchers:
            stats[15] += int(p.get("ErrorCnt", 0))

        return stats

    # --- Authenticate ---
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        "/Users/evansmac/Desktop/project-e0a5748a-0bec-4063-a99-0721295c7390.json",
        scopes=scope,
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(
        "1EQ24A5wLW80bZ6kQHE9j0qEdxK7t-QBy5bx5k1DXwVo"
    ).worksheet("賽程副本")

    # --- Determine Target Row ---
    # Fetch all values in Column B to find the actual last row of data
    col_b_values = sheet.col_values(2)

    # Check if record already exists
    for idx, val in enumerate(col_b_values, start=1):
        if str(val) == str(game_sno):
            row_vals = sheet.row_values(idx)
            if len(row_vals) > 2 and str(year) in str(row_vals[2]):
                print(f"Game {game_sno} already exists at Row {idx}. Skipping.")
                # We still proceed to update if it's the target row for comparison
                # return

    # Target row is the very next row after the last entry in Column B
    target_row = len(col_b_values) + 1
    print(f"Targeting Row {target_row} for Game {game_sno}...")

    # Team Name Mapping
    team_map = {
        "樂天桃猿": "樂天",
        "統一7-ELEVEn獅": "統一7-ELEVEn",
        "中信兄弟": "中信兄弟",
        "味全龍": "味全",
        "富邦悍將": "富邦",
        "台鋼雄鷹": "台鋼",
    }

    # --- Prepare Row Data (125 columns: A to DU) ---
    update_values = [""] * 125
    update_values[0] = game_detail.get("GameStatusChi", "")
    update_values[1] = game_sno
    # Ensure date format is strictly YYYY-MM-DD
    raw_date = game_detail.get("GameDate", "").split("T")[0]
    update_values[2] = raw_date
    update_values[3] = team_map.get(
        game_detail.get("VisitingTeamName", ""), game_detail.get("VisitingTeamName", "")
    )
    update_values[5] = team_map.get(
        game_detail.get("HomeTeamName", ""), game_detail.get("HomeTeamName", "")
    )
    update_values[7] = game_detail.get("FieldAbbe", "")
    update_values[8] = curt_game_detail.get("HeadUmpire") or game_detail.get(
        "HeadUmpire", ""
    )

    # Scoreboard Visitor
    for score in scoreboard:
        if str(score.get("VisitingHomeType")) == "1":
            inning = int(float(score.get("InningSeq", 0)))
            if 1 <= inning <= 12:
                update_values[9 + inning - 1] = int(float(score.get("ScoreCnt", 0)))

    v_batting = get_batting_stats(1)
    update_values[21] = game_detail.get("VisitingTotalScore", 0)
    update_values[22] = v_batting[2]
    update_values[23] = v_batting[15]

    # Scoreboard Home
    for score in scoreboard:
        if str(score.get("VisitingHomeType")) == "2":
            inning = int(float(score.get("InningSeq", 0)))
            if 1 <= inning <= 12:
                score_val = int(float(score.get("ScoreCnt", 0)))

                # Check for 'X' in the last inning for the home team
                if inning >= 9 and game_detail.get("GameStatusChi") == "比賽結束":
                    v_total = int(game_detail.get("VisitingTotalScore", 0))
                    h_total = int(game_detail.get("HomeTotalScore", 0))

                    if h_total > v_total:
                        # Calculate Home score before this inning
                        h_score_before = 0
                        for s2 in scoreboard:
                            if (
                                str(s2.get("VisitingHomeType")) == "2"
                                and int(float(s2.get("InningSeq", 0))) < inning
                            ):
                                h_score_before += int(float(s2.get("ScoreCnt", 0)))

                        # Calculate Visitor score up to this inning
                        v_score_up_to = 0
                        for s2 in scoreboard:
                            if (
                                str(s2.get("VisitingHomeType")) == "1"
                                and int(float(s2.get("InningSeq", 0))) <= inning
                            ):
                                v_score_up_to += int(float(s2.get("ScoreCnt", 0)))

                        if h_score_before > v_score_up_to:
                            score_val = "X"

                update_values[24 + inning - 1] = score_val

    h_batting = get_batting_stats(2)
    update_values[36] = game_detail.get("HomeTotalScore", 0)
    update_values[37] = h_batting[2]
    update_values[38] = h_batting[15]

    # Pitching Mapping
    v_starter_stats, v_starter_name, v_starter_acnt = get_pitching_stats(1, True)
    update_values[4] = v_starter_name
    for i in range(13):
        update_values[39 + i] = v_starter_stats[i]

    v_total_pitch, _, _ = get_pitching_stats(1, False)
    for i in range(13):
        update_values[52 + i] = v_total_pitch[i]

    h_starter_stats, h_starter_name, h_starter_acnt = get_pitching_stats(2, True)
    update_values[6] = h_starter_name
    for i in range(13):
        update_values[65 + i] = h_starter_stats[i]

    h_total_pitch, _, _ = get_pitching_stats(2, False)
    for i in range(13):
        update_values[78 + i] = h_total_pitch[i]

    # Fetch habits for starters
    update_values[91] = get_pitching_habit(v_starter_acnt, session)
    update_values[92] = get_pitching_habit(h_starter_acnt, session)

    # Batting Mapping
    for i in range(16):
        update_values[93 + i] = v_batting[i]
    for i in range(16):
        update_values[109 + i] = h_batting[i]

    # --- Final Update ---
    range_label = f"A{target_row}"
    # We wrap update_values in a list because update() expects a list of lists (rows)
    sheet.update(
        range_name=range_label,
        values=[update_values],
        value_input_option="USER_ENTERED",
    )
    print(f"Successfully updated Row {target_row}.")


if __name__ == "__main__":
    main()
