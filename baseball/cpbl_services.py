import json
import importlib
import time
from datetime import datetime, timedelta


class CpblModuleService:
    """Base service that lazily resolves the CPBL module dependency."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("cpbl")
        return self._module


class CpblStatusService(CpblModuleService):
    """Service for CPBL status worksheet records."""

    def records(self, status_sheet):
        module = self.module
        rows = status_sheet.get_all_values()
        if not rows:
            return []
        records = []
        for idx, row in enumerate(rows[1:], start=2):
            padded = row + [""] * (len(module.STATUS_HEADERS) - len(row))
            records.append(
                {
                    "row": idx,
                    "date": padded[0],
                    "kind_code": padded[1],
                    "game_sno": padded[2],
                    "status": padded[3],
                    "resolved": str(padded[4]).upper() == "TRUE",
                    "updated_at": padded[5],
                }
            )
        return records

    def records_for_date(self, status_sheet, date_str, kind_code):
        return [
            record
            for record in self.records(status_sheet)
            if record["date"] == date_str and record["kind_code"] == kind_code
        ]

    def all_games_resolved_for_date(self, status_sheet, date_str, kind_code):
        records = self.records_for_date(status_sheet, date_str, kind_code)
        return bool(records) and all(record["resolved"] for record in records)

    def unresolved_game_snos_for_date(self, status_sheet, date_str, kind_code):
        module = self.module
        return [
            record["game_sno"]
            for record in self.records_for_date(status_sheet, date_str, kind_code)
            if not record["resolved"] and record["game_sno"] != module.NO_GAMES_SENTINEL
        ]

    def upsert(self, status_sheet, date_str, kind_code, game_sno, status, resolved):
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = [
            date_str,
            kind_code,
            str(game_sno),
            status or "",
            "TRUE" if resolved else "FALSE",
            updated_at,
        ]
        for record in self.records(status_sheet):
            if (
                record["date"] == date_str
                and record["kind_code"] == kind_code
                and record["game_sno"] == str(game_sno)
            ):
                status_sheet.update(
                    range_name=f"A{record['row']}:F{record['row']}",
                    values=[values],
                    value_input_option="USER_ENTERED",
                )
                return

        status_sheet.append_row(values, value_input_option="USER_ENTERED")


class CpblGameSheetService(CpblModuleService):
    """Service for CPBL game box-score transformation and worksheet writes."""

    @staticmethod
    def game_date_str(game):
        return game.get("GameDate", "").split("T")[0].replace("/", "-")

    def extract_game_detail(self, data, game_sno):
        curt_game_detail = json.loads(data.get("CurtGameDetailJson", "{}"))
        game_detail_list = json.loads(data.get("GameDetailJson", "[]"))

        if str(curt_game_detail.get("GameSno")) == str(game_sno):
            return curt_game_detail

        for game_detail in game_detail_list:
            if str(game_detail.get("GameSno")) == str(game_sno):
                return game_detail

        if game_detail_list:
            print(
                f"Warning: No exact match for GameSno {game_sno}. "
                "Using first available."
            )
            return game_detail_list[0]

        return None

    @staticmethod
    def pitching_stats(pitching, ptype, is_starter=False):
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

    @staticmethod
    def batting_stats(batting, pitching, ptype):
        stats = [0] * 16
        target_batters = [
            b for b in batting if str(b.get("VisitingHomeType")) == str(ptype)
        ]
        for b in target_batters:
            stats[0] += int(b.get("HitCnt", 0))
            stats[1] += int(b.get("ScoreCnt", 0))
            stats[2] += int(b.get("HittingCnt", 0))
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
        for p in pitching:
            if str(p.get("VisitingHomeType")) == str(ptype):
                stats[15] += int(p.get("ErrorCnt", 0))
        return stats

    def is_game_recorded(self, game_sno, year, sheet):
        col_b = sheet.col_values(2)
        for idx, val in enumerate(col_b, start=1):
            if str(val) == str(game_sno):
                row_vals = sheet.row_values(idx)
                if len(row_vals) > 2 and str(year) in str(row_vals[2]):
                    return True
        return False

    def process_and_update_sheet(self, data, game_sno, year, kind_code, session, sheet):
        module = self.module
        curt_game_detail = json.loads(data.get("CurtGameDetailJson", "{}"))
        game_detail = self.extract_game_detail(data, game_sno)
        if not game_detail:
            print("No game detail found.")
            return False

        if game_detail.get("GameStatusChi") != "比賽結束":
            print(f"Game {game_sno} ({year}) is not finished yet. Skipping.")
            return False

        if self.is_game_recorded(game_sno, year, sheet):
            print(f"Game {game_sno} ({year}) already recorded. Skipping.")
            return True

        scoreboard = json.loads(data.get("ScoreboardJson", "[]"))
        pitching = json.loads(data.get("PitchingJson", "[]"))
        batting = json.loads(data.get("BattingJson", "[]"))

        col_b_values = sheet.col_values(2)
        target_row = len(col_b_values) + 1
        print(f"Targeting Row {target_row} for Game {game_sno} ({kind_code})...")

        update_values = [""] * 125
        update_values[0] = game_detail.get("GameStatusChi", "")
        update_values[1] = game_sno
        update_values[2] = self.game_date_str(game_detail)
        update_values[3] = module.TEAM_MAP.get(
            game_detail.get("VisitingTeamName", ""),
            game_detail.get("VisitingTeamName", ""),
        )
        update_values[5] = module.TEAM_MAP.get(
            game_detail.get("HomeTeamName", ""),
            game_detail.get("HomeTeamName", ""),
        )
        update_values[7] = game_detail.get("FieldAbbe", "")
        update_values[8] = curt_game_detail.get("HeadUmpire") or game_detail.get(
            "HeadUmpire", ""
        )

        for score in scoreboard:
            if str(score.get("VisitingHomeType")) == "1":
                inning = int(float(score.get("InningSeq", 0)))
                if 1 <= inning <= 12:
                    update_values[9 + inning - 1] = int(float(score.get("ScoreCnt", 0)))

        v_batting = self.batting_stats(batting, pitching, 1)
        update_values[21] = game_detail.get("VisitingTotalScore", 0)
        update_values[22] = v_batting[2]
        update_values[23] = v_batting[15]

        for score in scoreboard:
            if str(score.get("VisitingHomeType")) == "2":
                inning = int(float(score.get("InningSeq", 0)))
                if 1 <= inning <= 12:
                    score_val = int(float(score.get("ScoreCnt", 0)))
                    is_finished_ninth_or_later = (
                        inning >= 9
                        and game_detail.get("GameStatusChi") == "比賽結束"
                    )
                    if is_finished_ninth_or_later:
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

        h_batting = self.batting_stats(batting, pitching, 2)
        update_values[36] = game_detail.get("HomeTotalScore", 0)
        update_values[37] = h_batting[2]
        update_values[38] = h_batting[15]

        v_starter_stats, v_starter_name, v_starter_acnt = self.pitching_stats(
            pitching, 1, True
        )
        update_values[4] = v_starter_name
        for i in range(13):
            update_values[39 + i] = v_starter_stats[i]

        v_total_pitch, _, _ = self.pitching_stats(pitching, 1, False)
        for i in range(13):
            update_values[52 + i] = v_total_pitch[i]

        h_starter_stats, h_starter_name, h_starter_acnt = self.pitching_stats(
            pitching, 2, True
        )
        update_values[6] = h_starter_name
        for i in range(13):
            update_values[65 + i] = h_starter_stats[i]

        h_total_pitch, _, _ = self.pitching_stats(pitching, 2, False)
        for i in range(13):
            update_values[78 + i] = h_total_pitch[i]

        update_values[91] = module.get_pitching_habit(v_starter_acnt, session)
        update_values[92] = module.get_pitching_habit(h_starter_acnt, session)

        for i in range(16):
            update_values[93 + i] = v_batting[i]
        for i in range(16):
            update_values[109 + i] = h_batting[i]

        sheet.update(
            range_name=f"A{target_row}",
            values=[update_values],
            value_input_option="USER_ENTERED",
        )
        print(f"Successfully updated Row {target_row} (Game {game_sno}, {kind_code}).")
        return True

    def update_game(self, game_sno: str, year: str, kind_code="A"):
        module = self.module
        session = module.get_session()
        data = module.fetch_game_data(game_sno, year, kind_code, session)
        if not data:
            return None

        sheet = module.get_worksheet(kind_code)
        return self.process_and_update_sheet(
            data, game_sno, year, kind_code, session, sheet
        )


class CpblScheduleService:
    """Service entrypoint for CPBL schedule updates."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("cpbl")
        return self._module

    def run_once(self, year: str = None, kind_codes=None):
        module = self.module
        status_service = CpblStatusService(module=module)
        game_service = CpblGameSheetService(module=module)
        now = datetime.now() - timedelta(hours=6)
        if year is None:
            year = str(now.year)
        if kind_codes is None:
            kind_codes = ["A", "G"]

        current_month = str(now.month)
        today_str = now.strftime("%Y-%m-%d")
        errors = []
        updated_any_kind = False
        print(
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"Run started (year={year}, kind_codes={kind_codes})"
        )

        for kind_code in kind_codes:
            sheet = module.get_worksheet(kind_code)
            status_sheet = module.get_status_worksheet()

            if status_service.all_games_resolved_for_date(
                status_sheet, today_str, kind_code
            ):
                print(
                    f"[{kind_code}] {today_str} already resolved. Skipping CPBL fetch."
                )
                continue

            col_b_cache = sheet.col_values(2)
            col_c_cache = sheet.col_values(3)
            existing_snos = {
                str(sno)
                for sno, date_val in zip(col_b_cache, col_c_cache)
                if sno and str(year) in str(date_val)
            }

            unresolved_snos = status_service.unresolved_game_snos_for_date(
                status_sheet, today_str, kind_code
            )
            candidates = [
                {"GameSno": game_sno, "GameDate": today_str}
                for game_sno in unresolved_snos
            ]

            if not candidates:
                session = module.get_session()
                try:
                    games = module.fetch_schedule(
                        year, current_month, kind_code, session
                    )
                except Exception as e:
                    errors.append(f"fetch_schedule({kind_code}): {e}")
                    continue

                today_games = [
                    game
                    for game in games
                    if game_service.game_date_str(game) == today_str
                ]
                if not today_games:
                    status_service.upsert(
                        status_sheet,
                        today_str,
                        kind_code,
                        module.NO_GAMES_SENTINEL,
                        "無賽事",
                        True,
                    )
                    print(f"[{kind_code}] No games scheduled for {today_str}.")
                    continue

                for game in today_games:
                    game_sno = str(game.get("GameSno"))
                    if not game_sno:
                        continue
                    schedule_status = game.get("GameStatusChi", "")
                    resolved = (
                        game_sno in existing_snos
                        or module.is_non_finished_terminal_status(schedule_status)
                    )
                    if game_sno in existing_snos and not schedule_status:
                        schedule_status = "比賽結束"
                    status_service.upsert(
                        status_sheet,
                        today_str,
                        kind_code,
                        game_sno,
                        schedule_status,
                        resolved,
                    )

                candidates = []
                for game in today_games:
                    game_sno = str(game.get("GameSno"))
                    if not game_sno or game_sno in existing_snos:
                        continue
                    if module.is_non_finished_terminal_status(
                        game.get("GameStatusChi", "")
                    ):
                        continue
                    candidates.append(game)
            else:
                session = module.get_session()

            print(f"[{kind_code}] {len(candidates)} unresolved game(s) to check.")

            for game in candidates:
                game_sno = str(game.get("GameSno"))
                game_date_str = game_service.game_date_str(game) or today_str
                if game_sno in existing_snos:
                    status_service.upsert(
                        status_sheet,
                        game_date_str,
                        kind_code,
                        game_sno,
                        "比賽結束",
                        True,
                    )
                    continue

                print(f"Processing GameSno {game_sno} ({kind_code})...")
                try:
                    data = module.fetch_game_data(game_sno, year, kind_code, session)
                    if not data:
                        continue
                    game_detail = game_service.extract_game_detail(data, game_sno)
                    status = game_detail.get("GameStatusChi", "") if game_detail else ""
                    if status and status != "比賽結束":
                        status_service.upsert(
                            status_sheet,
                            game_date_str,
                            kind_code,
                            game_sno,
                            status,
                            module.is_terminal_game_status(status),
                        )
                        if module.is_terminal_game_status(status):
                            print(f"Game {game_sno} terminal status: {status}.")
                        else:
                            print(f"Game {game_sno} still unresolved: {status}.")
                        continue
                    written = game_service.process_and_update_sheet(
                        data, game_sno, year, kind_code, session, sheet
                    )
                    if written:
                        existing_snos.add(game_sno)
                        status_service.upsert(
                            status_sheet,
                            game_date_str,
                            kind_code,
                            game_sno,
                            "比賽結束",
                            True,
                        )
                except Exception as e:
                    errors.append(f"game {game_sno} ({kind_code}): {e}")
                    continue

                time.sleep(2)

            if status_service.all_games_resolved_for_date(
                status_sheet, today_str, kind_code
            ):
                updated_any_kind = True

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Run finished.")

        if updated_any_kind:
            try:
                CpblHuiziService(module=module).update(year=year)
            except Exception as e:
                errors.append(f"update_huizi: {e}")
        else:
            print("No newly resolved CPBL game day. Skipping 彙資 update.")

        if errors:
            print(f"\n[ERROR] {len(errors)} failure(s) occurred:")
            for err in errors:
                print(f"  - {err}")
            raise RuntimeError("; ".join(errors))

    def update_game(self, game_sno: str, year: str, kind_code="A"):
        return CpblGameSheetService(module=self.module).update_game(
            game_sno=game_sno, year=year, kind_code=kind_code
        )


class CpblHuiziService:
    """Service entrypoint for CPBL 彙資 refreshes."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("cpbl")
        return self._module

    def update(self, year: str = None):
        module = self.module
        effective_now = datetime.now() - timedelta(hours=6)
        if year is None:
            year = str(effective_now.year)

        today_str = effective_now.strftime("%Y-%m-%d")
        print(f"Updating 彙資 for {today_str}...")

        spreadsheet = module.get_spreadsheet()
        huizi = spreadsheet.worksheet("彙資")

        today_games = []
        for sheet_name in module.WORKSHEET_MAP.values():
            sheet = spreadsheet.worksheet(sheet_name)
            col_c = sheet.col_values(3)
            for idx, val in enumerate(col_c, start=1):
                if today_str in str(val):
                    row_data = sheet.row_values(idx)
                    today_games.append(row_data[1:125])

        if not today_games:
            print(f"No games found for {today_str}. Keeping existing 彙資 data.")
            return None

        huizi.batch_clear(["B4:DU6"])
        print("Cleared 彙資 B4:DU6.")

        for i, game_data in enumerate(today_games[:3]):
            row_num = 4 + i
            huizi.update(
                range_name=f"B{row_num}",
                values=[game_data],
                value_input_option="USER_ENTERED",
            )
            print(f"Pasted game {i + 1} into 彙資 row {row_num}.")

        print(
            f"彙資 updated with {min(len(today_games), 3)} "
            f"game(s) for {today_str}."
        )
        return len(today_games[:3])
