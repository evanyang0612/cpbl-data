import asyncio
import importlib
import sys

import aiohttp


class NpbUpdateService:
    """Service entrypoint for the full NPB sheet update job."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("npb")
        return self._module

    async def run_once(self, matchup_date: str | None = None):
        module = self.module
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        errors = []
        matchup_start_date = module._resolve_matchup_start_date(matchup_date)
        print(f"Matchup start date: {matchup_start_date:%Y-%m-%d}")

        async with aiohttp.ClientSession(headers=headers) as session:
            await NpbRecentGamesService(module=module).update(
                session, matchup_start_date=matchup_start_date, errors=errors
            )

            new_sailu_ids = []
            try:
                new_sailu_ids = await NpbSailuService(module=module).update(session)
            except Exception as e:
                errors.append(f"update_sailu_sheet: {e}")

            huizi_date = None
            try:
                analysis_game_ids = new_sailu_ids or module._sailu_game_ids_for_date()
                await NpbAnalysisService(module=module).update(
                    session, game_ids=analysis_game_ids
                )
                sailu_dates = module._sailu_dates_for_game_ids(new_sailu_ids)
                if not sailu_dates and analysis_game_ids:
                    sailu_dates = module._sailu_dates_for_game_ids(analysis_game_ids)
                if sailu_dates:
                    huizi_date = sailu_dates[-1]
                await NpbPredictionService(module=module).reveal_predictions_for_games(
                    session, analysis_game_ids
                )
            except Exception as e:
                errors.append(f"update_analysis_sheet: {e}")

            try:
                NpbHuiziService(module=module).update(huizi_date)
            except Exception as e:
                errors.append(f"update_huizi_sheet: {e}")

        if errors:
            print(f"\n[ERROR] {len(errors)} failure(s):")
            for err in errors:
                print(f"  - {err}")
            sys.exit(1)

        print("\nDone.")


class NpbRecentGamesService:
    """Service entrypoint for NPB 近十場 league sheets."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("npb")
        return self._module

    async def update(self, session, *, matchup_start_date, errors: list[str]):
        module = self.module
        for league, sheet_name in module.LEAGUE_SHEETS.items():
            league_teams = {
                k: v for k, v in module.NPB_TEAMS.items() if v["league"] == league
            }
            print(f"\n=== {league} ({sheet_name}) ===")

            try:
                matchups = await module.get_next_matchups(
                    league, session, start_date=matchup_start_date
                )
            except Exception as e:
                errors.append(f"get_next_matchups({league}): {e}")
                teams = list(league_teams.keys())
                matchups = [(teams[i * 2], teams[i * 2 + 1]) for i in range(3)]

            print(f"Matchup order: {matchups}")

            all_game_ids: dict[str, list[str]] = {}
            for team_key, team_info in league_teams.items():
                try:
                    ids = await module.get_last_n_game_ids(
                        team_info["id"], module.GAMES_COUNT, session
                    )
                    all_game_ids[team_key] = ids
                    print(f"  {team_key}: {len(ids)} game IDs found")
                except Exception as e:
                    errors.append(f"get_last_n_game_ids({team_key}): {e}")
                    all_game_ids[team_key] = []

            game_cache: dict[str, dict] = {}
            unique_ids = {gid for ids in all_game_ids.values() for gid in ids}
            id_list = list(unique_ids)

            for i in range(0, len(id_list), module.MAX_CONCURRENT):
                batch = id_list[i : i + module.MAX_CONCURRENT]
                results = await asyncio.gather(
                    *[module.get_game_info(gid, session) for gid in batch],
                    return_exceptions=True,
                )
                for gid, result in zip(batch, results):
                    if isinstance(result, Exception):
                        errors.append(f"get_game_info({gid}): {result}")
                    elif result:
                        game_cache[gid] = result
                if i + module.MAX_CONCURRENT < len(id_list):
                    await asyncio.sleep(2)

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

            try:
                module.update_league_sheet(sheet_name, matchups, all_games)
            except Exception as e:
                errors.append(f"update_league_sheet({sheet_name}): {e}")


class NpbSailuService:
    """Service entrypoint for NPB 賽錄 updates."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("npb")
        return self._module

    async def update(self, session):
        module = self.module
        print("\n=== 賽錄 update ===")
        sheet = module.get_worksheet(module.SAILU_SHEET_NAME, module.SAILU_SPREADSHEET_KEY)
        target_sheet = module.get_worksheet(
            module.SAILU_SHEET_NAME, module.SAILU_TARGET_SPREADSHEET_KEY
        )
        exhibition_sheet = module.get_worksheet(
            module.EXHIBITION_SHEET_NAME, module.SAILU_SPREADSHEET_KEY
        )

        existing_ids = set(v for v in sheet.col_values(2)[1:] if v)
        target_existing_ids = set(v for v in target_sheet.col_values(2)[1:] if v)
        existing_exhibition = module._existing_exhibition_identities(exhibition_sheet)
        print(
            f"[sailu] {len(module._placeholder_rows(sheet))} "
            "source placeholder row(s) available."
        )
        print(
            f"[sailu] {len(module._placeholder_rows(target_sheet))} "
            "target placeholder row(s) available."
        )

        all_ids: set[str] = set()
        tasks = {
            key: module.get_last_n_game_ids(info["id"], 3, session)
            for key, info in module.NPB_TEAMS.items()
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

        new_games: list[tuple[str, dict]] = []
        for i in range(0, len(new_ids), module.MAX_CONCURRENT):
            batch = new_ids[i : i + module.MAX_CONCURRENT]
            scraped = await asyncio.gather(
                *[module.get_sailu_game_data(gid, session) for gid in batch],
                return_exceptions=True,
            )
            for gid, data in zip(batch, scraped):
                if isinstance(data, Exception):
                    print(f"  [sailu] get_sailu_game_data({gid}): {data}")
                elif data:
                    new_games.append((gid, data))
                else:
                    print(f"  [sailu] No data for {gid} (game may not be finished yet)")
            if i + module.MAX_CONCURRENT < len(new_ids):
                await asyncio.sleep(2)

        if not new_games:
            print("[sailu] Nothing to write.")
            return []

        new_games.sort(key=lambda x: x[0])

        regular_games = [
            (gid, data)
            for gid, data in new_games
            if not module.is_exhibition_game_id(gid)
        ]
        exhibition_games = [
            (gid, data) for gid, data in new_games if module.is_exhibition_game_id(gid)
        ]

        source_regular_games = [
            (gid, data) for gid, data in regular_games if gid not in existing_ids
        ]
        target_regular_games = [
            (gid, data)
            for gid, data in regular_games
            if gid not in target_existing_ids
        ]

        filled, overflow = module._write_regular_sailu_games(
            sheet, source_regular_games
        )
        if overflow:
            print(
                f"[sailu] WARNING: {len(overflow)} source game(s) skipped — "
                "no placeholder rows left: "
                + str([gid for gid, _ in overflow])
                + "\n  → Add more pre-populated formula rows to 賽錄 and re-run."
            )

        target_filled, target_overflow = module._write_regular_sailu_games(
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
            ident = module._exhibition_identity(data)
            if ident in existing_exhibition:
                print(f"  [exhibition] skip existing ← {gid}")
                continue
            exhibition_rows.append(module._exhibition_row(data))
            existing_exhibition.add(ident)
            exhibition_written += 1

        if exhibition_rows:
            exhibition_sheet.append_rows(
                exhibition_rows,
                value_input_option="USER_ENTERED",
                table_range="A:AB",
            )
            print(
                f"[exhibition] Appended {exhibition_written} row(s) to "
                f"'{module.EXHIBITION_SHEET_NAME}'."
            )
        else:
            print("[exhibition] No new games to add.")

        print(
            f"[sailu] Done. Filled {filled} source row(s) and "
            f"{target_filled} target row(s)."
        )
        source_written_ids = [gid for gid, _ in source_regular_games[:filled]]
        target_written_ids = [gid for gid, _ in target_regular_games[:target_filled]]
        return list(dict.fromkeys(source_written_ids + target_written_ids))


class NpbAnalysisService:
    """Service entrypoint for NPB 分析表紀錄 updates and repairs."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("npb")
        return self._module

    async def update(
        self,
        session,
        year: int | None = None,
        *,
        game_ids: list[str] | None = None,
        target_date=None,
        full_season: bool = False,
    ):
        module = self.module
        if year is None:
            year = module.ANALYSIS_SEASON
        print(f"\n=== {module.ANALYSIS_SHEET_NAME} update ({year}) ===")
        sheet = module.get_worksheet(
            module.ANALYSIS_SHEET_NAME, module.NPB_SPREADSHEET_KEY
        )
        rows = sheet.get_all_values()
        season_rows = [
            row for row in rows[2:] if module._analysis_row_year(row) == year
        ]
        existing = {
            ident
            for row in season_rows
            if (ident := module._analysis_identity_from_row(row))
        }
        last_seq = module._last_analysis_seq(rows)

        if full_season:
            candidate_ids = list(
                reversed(
                    sorted(await module.get_finished_game_ids_for_season(year, session))
                )
            )
            print(
                f"[analysis] Full-season scan found {len(candidate_ids)} "
                "finished game ID(s)."
            )
        else:
            source_ids = (
                game_ids
                if game_ids is not None
                else module._sailu_game_ids_for_date(target_date)
            )
            candidate_ids = []
            for gid in source_ids:
                if gid and gid not in candidate_ids:
                    candidate_ids.append(gid)
            target_label = (
                "provided game IDs"
                if game_ids is not None
                else module._date_key(target_date)
            )
            print(
                f"[analysis] {target_label} has {len(candidate_ids)} "
                "candidate game ID(s)."
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
        for i in range(0, len(candidate_ids), module.MAX_CONCURRENT):
            batch = candidate_ids[i : i + module.MAX_CONCURRENT]
            scraped = await asyncio.gather(
                *[
                    module.get_schedule_game_data(gid, session, retry=full_season)
                    for gid in batch
                ],
                return_exceptions=True,
            )
            for gid, data in zip(batch, scraped):
                if isinstance(data, Exception):
                    print(f"  [analysis] get_schedule_game_data({gid}): {data}")
                elif data:
                    if target_date and data["日期"] != module._date_key(target_date):
                        continue
                    ident = module._analysis_identity(data)
                    if ident not in existing:
                        new_games.append((gid, data))
                        existing.add(ident)
                        print(f"  [analysis] missing ← {gid} {ident}")
                else:
                    print(f"  [analysis] No data for {gid}")
            if i + module.MAX_CONCURRENT < len(candidate_ids):
                await asyncio.sleep(2)

        if not new_games:
            print("[analysis] No new games to append.")
            return 0

        new_games.sort(key=lambda x: (x[1]["日期"], x[0]))
        inserted = 0
        for gid, data in new_games:
            row_values = module._analysis_row(last_seq + inserted + 1, data)
            insert_at = module._analysis_insert_index(rows, data["日期"])
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

        print(
            f"[analysis] Inserted {inserted} row(s) into "
            f"{module.ANALYSIS_SHEET_NAME}."
        )
        return inserted

    def repair_leagues(self, year: int | None = None):
        module = self.module
        if year is None:
            year = module.ANALYSIS_SEASON
        print(f"\n=== {module.ANALYSIS_SHEET_NAME} league repair ({year}) ===")
        sheet = module.get_worksheet(
            module.ANALYSIS_SHEET_NAME, module.NPB_SPREADSHEET_KEY
        )
        rows = sheet.get_all_values()
        updates = []

        for row_num, row in enumerate(rows[2:], start=3):
            if module._analysis_row_year(row) != year:
                continue
            away_team = row[8] if len(row) > 8 else ""
            home_team = row[11] if len(row) > 11 else ""
            game_type = module._analysis_game_type_from_teams(away_team, home_team)
            if not game_type:
                print(
                    f"  [analysis] row {row_num}: "
                    f"skipped unknown team {away_team}/{home_team}"
                )
                continue
            current = row[3] if len(row) > 3 else ""
            if current == game_type:
                continue
            updates.append({"range": f"D{row_num}", "values": [[game_type]]})

        if not updates:
            print("[analysis] No league cells need repair.")
            return 0

        sheet.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"[analysis] Repaired {len(updates)} league cell(s).")
        return len(updates)


class NpbHuiziService:
    """Service entrypoint for NPB 彙資 refreshes."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("npb")
        return self._module

    def update(self, today=None):
        module = self.module
        if isinstance(today, str):
            today = module.datetime.strptime(today, "%Y-%m-%d")
        else:
            today = today or module.datetime.now()
        today_str = f"{today.year}/{today.month}/{today.day}"
        print(f"\n=== {module.HUIZI_SHEET_NAME} update ({today_str}) ===")

        analysis = module.get_worksheet(
            module.ANALYSIS_SHEET_NAME, module.NPB_SPREADSHEET_KEY
        )
        huizi = module.get_worksheet(module.HUIZI_SHEET_NAME, module.NPB_SPREADSHEET_KEY)
        rows = analysis.get_all_values()
        today_rows = [
            row[:83] for row in rows[2:] if len(row) > 1 and row[1] == today_str
        ]

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


class NpbPredictionService:
    """Service entrypoint for NPB prediction ledger operations."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("npb")
        return self._module

    def create_prediction(
        self,
        game_id: str,
        pick: str,
        rate: float,
        *,
        market: str = "final_winner",
        line: float | None = None,
        stake: float | None = None,
        game_date: str = "",
        away_team: str = "",
        home_team: str = "",
        sheet=None,
        post: bool = False,
        dry_run: bool = False,
    ) -> dict:
        module = self.module
        if stake is None:
            stake = module.PREDICTION_DEFAULT_STAKE
        sheet = sheet or (None if dry_run else module._prediction_sheet())
        rows = (
            module._prediction_rows(sheet, ensure_header=not dry_run)
            if sheet
            else [module._prediction_headers()]
        )
        headers = rows[0]
        row_num = len(rows) + 1
        balance_before = module._last_prediction_balance(rows)
        prediction_id = module.secrets.token_hex(8)
        created_at = module._prediction_now()
        market = module.normalize_prediction_market(market)
        pick = module.validate_prediction_pick(pick, market)
        posts = module.build_prediction_posts(
            game_id,
            pick,
            float(rate),
            float(stake),
            market=market,
            line=line,
            predicted_at=created_at,
        )
        commitment_post_id = ""

        balance_after = module.calculate_prediction_balance(
            balance_before, stake, rate, "pending"
        )
        row = [
            prediction_id,
            game_id,
            game_date,
            away_team,
            home_team,
            market,
            pick,
            "" if line is None else float(line),
            float(rate),
            float(stake),
            "pending",
            "",
            (
                balance_before
                if dry_run
                else module._prediction_balance_before_formula(headers, row_num)
            ),
            (
                balance_after
                if dry_run
                else module._prediction_balance_after_formula(headers, row_num)
            ),
            commitment_post_id,
            "",
            posts["commitment_hash"],
            posts["salt"],
            posts["reveal_post"],
            created_at,
            "",
        ]
        if not dry_run:
            sheet.append_row(row, value_input_option="USER_ENTERED")
        return {
            "prediction_id": prediction_id,
            "balance_before": balance_before,
            "balance_after": balance_after,
            "commitment_post_id": commitment_post_id,
            **posts,
        }

    def resolve_for_game(
        self,
        game_id: str,
        data: dict,
        *,
        sheet=None,
        post: bool = False,
        dry_run: bool = False,
    ) -> int:
        module = self.module
        sheet = sheet or module._prediction_sheet()
        rows = module._prediction_rows(sheet)
        headers = rows[0]
        index = {name: idx for idx, name in enumerate(headers)}
        required = {
            "game_id",
            "market",
            "pick",
            "line",
            "rate",
            "stake",
            "status",
            "outcome",
            "balance_before",
            "balance_after",
            "commitment_post_id",
            "reveal_post_id",
            "reveal_post",
            "salt",
            "created_at",
            "resolved_at",
        }
        missing = required - set(index)
        if missing:
            raise ValueError(
                f"{module.PREDICTION_SHEET_NAME} missing columns: {sorted(missing)}"
            )

        resolved = 0
        running_balance = module.PREDICTION_STARTING_BALANCE
        for row_num, row in enumerate(rows[1:], start=2):
            row = row + [""] * (len(headers) - len(row))
            if (
                row[index["game_id"]] != str(game_id)
                or row[index["status"]] != "pending"
            ):
                if row[index["status"]] == "pending":
                    continue
                balance_after = module._prediction_float(
                    row[index["balance_after"]], None
                )
                if balance_after is None:
                    rate = module._prediction_float(row[index["rate"]])
                    stake = module._prediction_float(
                        row[index["stake"]], module.PREDICTION_DEFAULT_STAKE
                    )
                    outcome = row[index["outcome"]] or row[index["status"]]
                    try:
                        balance_after = module.calculate_prediction_balance(
                            running_balance, stake, rate, outcome
                        )
                    except ValueError:
                        balance_after = running_balance
                running_balance = balance_after
                continue

            market = row[index["market"]] or "final_winner"
            pick = row[index["pick"]]
            line = row[index["line"]] or None
            rate = module._prediction_float(row[index["rate"]])
            stake = module._prediction_float(
                row[index["stake"]], module.PREDICTION_DEFAULT_STAKE
            )
            outcome = module.prediction_outcome_for_game(
                data, pick, market=market, line=line
            )
            balance_before = running_balance
            balance_after = module.calculate_prediction_balance(
                balance_before, stake, rate, outcome
            )
            stats = module._prediction_stats_after(rows, outcome, balance_after)
            payload = module._prediction_payload(
                game_id,
                pick,
                rate,
                stake,
                market=market,
                line=line,
                predicted_at=row[index["created_at"]] or None,
            )
            reveal_text = module.prediction_reveal_text(payload, row[index["salt"]])
            reveal_post = module.build_prediction_reveal_post(
                game_id,
                reveal_text,
                outcome,
                stats,
            )
            reveal_post_id = ""

            running_balance = balance_after
            resolved_at = module._prediction_now()
            if not dry_run:
                updates = [
                    ("status", "resolved"),
                    ("outcome", outcome),
                    (
                        "balance_before",
                        module._prediction_balance_before_formula(headers, row_num),
                    ),
                    (
                        "balance_after",
                        module._prediction_balance_after_formula(headers, row_num),
                    ),
                    ("reveal_post_id", reveal_post_id),
                    ("reveal_post", reveal_post),
                    ("resolved_at", resolved_at),
                ]
                backfill_values = {
                    "game_date": data.get("日期", ""),
                    "away_team": data.get("客隊原名") or data.get("客隊", ""),
                    "home_team": data.get("主隊原名") or data.get("主隊", ""),
                }
                for col_name, value in backfill_values.items():
                    if col_name in index and not row[index[col_name]] and value:
                        updates.append((col_name, value))
                for col_name, value in updates:
                    sheet.update_cell(row_num, index[col_name] + 1, value)
                    row[index[col_name]] = str(value)
                rows[row_num - 1] = row
            resolved += 1
        return resolved

    def pending_game_ids(self, sheet) -> list[str]:
        module = self.module
        rows = module._prediction_rows(sheet)
        headers = rows[0]
        index = {name: idx for idx, name in enumerate(headers)}
        if "game_id" not in index or "status" not in index:
            return []

        ids: list[str] = []
        for row in rows[1:]:
            row = row + [""] * (len(headers) - len(row))
            gid = str(row[index["game_id"]]).strip()
            if gid and row[index["status"]] == "pending" and gid not in ids:
                ids.append(gid)
        return ids

    async def reveal_predictions_for_games(self, session, game_ids: list[str], **kwargs):
        module = self.module
        post = kwargs.get("post", False)
        dry_run = kwargs.get("dry_run", False)
        total = 0
        sheet = module._prediction_sheet()
        candidate_ids = list(
            dict.fromkeys([*(game_ids or []), *self.pending_game_ids(sheet)])
        )
        if not candidate_ids:
            return 0
        for gid in candidate_ids:
            data = await module.get_schedule_game_data(gid, session, retry=False)
            if not data:
                continue
            total += self.resolve_for_game(
                gid, data, sheet=sheet, post=post, dry_run=dry_run
            )
        if total:
            print(f"[prediction] Revealed {total} prediction(s).")
        return total
