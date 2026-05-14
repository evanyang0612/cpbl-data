import importlib
import time
from datetime import datetime, timedelta


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

            if module._all_games_resolved_for_date(status_sheet, today_str, kind_code):
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

            unresolved_snos = module._unresolved_game_snos_for_date(
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
                    game for game in games if module._game_date_str(game) == today_str
                ]
                if not today_games:
                    module._upsert_status(
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
                    module._upsert_status(
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
                game_date_str = module._game_date_str(game) or today_str
                if game_sno in existing_snos:
                    module._upsert_status(
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
                    game_detail = module._extract_game_detail(data, game_sno)
                    status = game_detail.get("GameStatusChi", "") if game_detail else ""
                    if status and status != "比賽結束":
                        module._upsert_status(
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
                    written = module.process_and_update_sheet(
                        data, game_sno, year, kind_code, session, sheet
                    )
                    if written:
                        existing_snos.add(game_sno)
                        module._upsert_status(
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

            if module._all_games_resolved_for_date(status_sheet, today_str, kind_code):
                updated_any_kind = True

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Run finished.")

        if updated_any_kind:
            try:
                module.update_huizi(year=year)
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
        return self.module.main(game_sno=game_sno, year=year, kind_code=kind_code)


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
        return self.module.update_huizi(year=year)
