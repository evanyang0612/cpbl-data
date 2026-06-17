import asyncio
import importlib
import os
import platform
import re
import sys
from datetime import datetime, timedelta

import aiohttp


class NpbModuleService:
    """Base service that lazily resolves the NPB module dependency."""

    def __init__(self, module=None):
        self._module = module

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("npb")
        return self._module


class NpbStatusService(NpbModuleService):
    """Service for NPB status worksheet records."""

    TERMINAL_NON_FINISHED = ("中止", "ノーゲーム", "延期")

    def effective_date_str(self) -> str:
        """Date the 賽錄 scrape should process.

        Defaults to now minus 6 hours so late-night JST games still count as the
        same calendar day. A manual backfill can override the date with the
        NPB_STATUS_DATE env var: 'today', 'yesterday', or YYYY-MM-DD.
        """
        override = (os.getenv("NPB_STATUS_DATE") or "").strip()
        if override:
            normalized = override.lower()
            if normalized in {"today", "今天", "今日"}:
                return datetime.now().strftime("%Y-%m-%d")
            if normalized in {"yesterday", "昨天", "昨日"}:
                return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                return datetime.strptime(override, "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError as exc:
                raise ValueError(
                    "NPB_STATUS_DATE must be 'today', 'yesterday', or YYYY-MM-DD"
                ) from exc
        return (datetime.now() - timedelta(hours=6)).strftime("%Y-%m-%d")

    def records(self, status_sheet):
        module = self.module
        rows = status_sheet.get_all_values()
        if not rows:
            return []
        records = []
        for idx, row in enumerate(rows[1:], start=2):
            padded = row + [""] * (len(module.NPB_STATUS_HEADERS) - len(row))
            records.append(
                {
                    "row": idx,
                    "date": padded[0],
                    "game_id": padded[1],
                    "status": padded[2],
                    "resolved": str(padded[3]).upper() == "TRUE",
                    "updated_at": padded[4],
                }
            )
        return records

    def records_for_date(self, status_sheet, date_str):
        return [
            record
            for record in self.records(status_sheet)
            if record["date"] == date_str
        ]

    def all_games_resolved_for_date(self, status_sheet, date_str):
        date_records = self.records_for_date(status_sheet, date_str)
        records = [
            record
            for record in date_records
            if record["game_id"] != self.module.NPB_NO_GAMES_SENTINEL
        ]
        if not records:
            return any(
                record["game_id"] == self.module.NPB_NO_GAMES_SENTINEL
                and record["resolved"]
                for record in date_records
            )
        return bool(records) and all(record["resolved"] for record in records)

    def finished_unresolved_game_ids_for_date(self, status_sheet, date_str):
        module = self.module
        return [
            record["game_id"]
            for record in self.records_for_date(status_sheet, date_str)
            if not record["resolved"]
            and record["game_id"] != module.NPB_NO_GAMES_SENTINEL
            and record["status"] == "試合終了"
        ]

    def upsert(self, status_sheet, date_str, game_id, status, resolved):
        module = self.module
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = [
            date_str,
            str(game_id),
            status or "",
            "TRUE" if resolved else "FALSE",
            updated_at,
        ]
        for record in self.records(status_sheet):
            if record["date"] == date_str and record["game_id"] == str(game_id):
                status_sheet.update(
                    range_name=f"A{record['row']}:E{record['row']}",
                    values=[values],
                    value_input_option="USER_ENTERED",
                )
                return

        status_sheet.append_row(values, value_input_option="USER_ENTERED")

    def resolved_from_status(self, game_id: str, status: str, existing_ids: set[str]):
        if game_id in existing_ids:
            return True
        return any(word in str(status or "") for word in self.TERMINAL_NON_FINISHED)

    async def sync_team_schedule_statuses(
        self, status_sheet, date_str: str, existing_ids: set[str], session
    ):
        module = self.module
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        month = dt.strftime("%Y-%m")

        async def fetch_team_status(team_id):
            html = await module._fetch(
                session, f"{module.BASE_URL}teams/{team_id}/schedule?month={month}"
            )
            if not html:
                return None
            soup = module.bs(html, "html.parser")
            statuses = []
            for entry in soup.find_all(class_="bb-calendarTable__data"):
                date_el = entry.find(class_="bb-calendarTable__date")
                if date_el is None:
                    continue
                try:
                    if int(date_el.text) != dt.day:
                        continue
                except ValueError:
                    continue
                status_el = entry.find(class_="bb-calendarTable__status")
                if status_el is None:
                    continue
                status_text = status_el.get_text(" ", strip=True)
                match = re.search(r"npb/game/([^/]+)", status_el.get("href", ""))
                if match:
                    statuses.append((match.group(1), status_text))
            return statuses

        results = await asyncio.gather(
            *[fetch_team_status(info["id"]) for info in module.NPB_TEAMS.values()],
            return_exceptions=True,
        )

        found: dict[str, str] = {}
        fetched_count = 0
        for result in results:
            if isinstance(result, Exception):
                print(f"  [npb-status] schedule status scan: {result}")
                continue
            if result is None:
                continue
            fetched_count += 1
            for game_id, status in result:
                if game_id not in found or status == "試合終了":
                    found[game_id] = status

        if not found:
            if fetched_count == len(module.NPB_TEAMS):
                self.upsert(
                    status_sheet,
                    date_str,
                    module.NPB_NO_GAMES_SENTINEL,
                    "無賽事",
                    True,
                )
            return found

        for game_id, status in sorted(found.items()):
            self.upsert(
                status_sheet,
                date_str,
                game_id,
                status,
                self.resolved_from_status(game_id, status, existing_ids),
            )
        return found


class _NpbPredictionLogic(NpbModuleService):
    """Internal prediction calculations used by NpbPredictionService."""

    def now(self) -> str:
        return datetime.now().replace(microsecond=0).isoformat()

    def payload(
        self,
        game_id: str,
        pick: str,
        rate: float,
        stake: float,
        market: str = "final_winner",
        line: float | None = None,
        predicted_at: str | None = None,
    ) -> dict:
        payload = {
            "g": str(game_id),
            "m": self.normalize_market(market),
            "p": str(pick),
            "r": float(rate),
            "s": float(stake),
            "t": predicted_at or self.now(),
        }
        if line is not None:
            payload["l"] = float(line)
        return payload

    def prediction_text(
        self,
        game_id: str,
        pick: str,
        rate: float,
        stake: float | None = None,
        *,
        market: str = "final_winner",
        line: float | None = None,
    ) -> str:
        module = self.module
        if stake is None:
            stake = module.PREDICTION_DEFAULT_STAKE
        market = self.normalize_market(market)
        line_text = "" if line is None else f"\nLine: {float(line)}"
        return (
            f"NPB prediction\n"
            f"Game {game_id}\n"
            f"Market: {market}\n"
            f"Pick: {pick}{line_text}\n"
            f"Rate: {float(rate)}\n"
            f"Stake: {float(stake)}"
        )

    @staticmethod
    def calculate_balance(
        balance_before: float, stake: float, rate: float, outcome: str
    ):
        outcome = str(outcome).lower()
        if outcome == "win":
            return round(float(balance_before) + float(stake) * float(rate), 4)
        if outcome == "loss":
            return round(float(balance_before) - float(stake), 4)
        if outcome in {"push", "void", "pending"}:
            return round(float(balance_before), 4)
        raise ValueError(f"Unsupported prediction outcome: {outcome}")

    @staticmethod
    def to_float(value, default: float = 0.0) -> float:
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return default

    def normalize_team(self, value: str) -> str:
        module = self.module
        text = str(value or "").replace(" ", "")
        aliases = {"横浜": "DeNA", "橫濱": "DeNA"}
        text = aliases.get(text, text)
        for raw, info in module.NPB_TEAMS.items():
            candidates = {
                raw.replace(" ", ""),
                module.display_team_name(raw).replace(" ", ""),
                str(info["name"]).replace(" ", ""),
            }
            if text in candidates:
                return raw
        return text

    def team_options(self) -> str:
        return ", ".join(self.module.NPB_TEAMS.keys())

    def validate_home_team(self, value: str) -> str:
        team = self.normalize_team(value)
        if team in self.module.NPB_TEAMS:
            return team
        raise ValueError(f"Home team must be one of: {self.team_options()}.")

    def display_team(self, value: str) -> str:
        module = self.module
        team = self.normalize_team(value)
        if team in module.NPB_TEAMS:
            return f"{team} ({module.NPB_TEAMS[team]['name']})"
        return str(value or "")

    def normalize_market(self, market: str) -> str:
        module = self.module
        normalized = str(market or "final_winner").strip().lower().replace(" ", "_")
        if normalized in module.PREDICTION_MARKET_ALIASES:
            return module.PREDICTION_MARKET_ALIASES[normalized]
        raise ValueError(f"Unsupported prediction market: {market}")

    @staticmethod
    def prediction_side(value: str) -> str:
        normalized = str(value or "").strip().lower()
        aliases = {
            "over": "over",
            "greater": "over",
            "gt": "over",
            ">": "over",
            "大": "over",
            "under": "under",
            "less": "under",
            "lt": "under",
            "<": "under",
            "小": "under",
        }
        if normalized in aliases:
            return aliases[normalized]
        raise ValueError("Total predictions require pick over/under or greater/less.")

    def validate_pick(self, pick: str, market: str) -> str:
        market = self.normalize_market(market)
        if market in {"final_total", "half_total"}:
            return self.prediction_side(pick)

        team = self.normalize_team(pick)
        if team in self.module.NPB_TEAMS:
            return team
        raise ValueError(
            f"Winner/handicap predictions require a valid NPB team: {self.team_options()}."
        )

    @staticmethod
    def to_int(value) -> int:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return 0

    def innings_total(self, values: list, innings: int = 5) -> int:
        return sum(self.to_int(v) for v in values[:innings])

    def winner_outcome(self, data: dict, pick: str, *, half: bool = False) -> str:
        if half:
            away_score = self.innings_total(data.get("away_innings", []), 5)
            home_score = self.innings_total(data.get("home_innings", []), 5)
        else:
            away_score = int(data["客總分"])
            home_score = int(data["主總分"])
        if away_score == home_score:
            return "push"

        away = self.normalize_team(data.get("客隊原名") or data.get("客隊"))
        home = self.normalize_team(data.get("主隊原名") or data.get("主隊"))
        winner = away if away_score > home_score else home
        picked = self.normalize_team(pick)
        return "win" if picked == winner else "loss"

    def total_outcome(
        self, data: dict, pick: str, line: float, *, half: bool = False
    ) -> str:
        if half:
            total = self.innings_total(data.get("away_innings", []), 5)
            total += self.innings_total(data.get("home_innings", []), 5)
        else:
            total = int(data["客總分"]) + int(data["主總分"])

        line = float(line)
        if total == line:
            return "push"
        side = self.prediction_side(pick)
        if side == "over":
            return "win" if total > line else "loss"
        return "win" if total < line else "loss"

    def handicap_outcome(
        self, data: dict, pick: str, line: float, *, half: bool = False
    ) -> str:
        if half:
            away_score = self.innings_total(data.get("away_innings", []), 5)
            home_score = self.innings_total(data.get("home_innings", []), 5)
        else:
            away_score = int(data["客總分"])
            home_score = int(data["主總分"])

        away = self.normalize_team(data.get("客隊原名") or data.get("客隊"))
        home = self.normalize_team(data.get("主隊原名") or data.get("主隊"))
        picked = self.normalize_team(pick)

        if picked == away:
            adjusted = away_score + float(line)
            opponent = home_score
        elif picked == home:
            adjusted = home_score + float(line)
            opponent = away_score
        else:
            raise ValueError(f"Pick '{pick}' is not in this game.")

        if adjusted > opponent:
            return "win"
        if adjusted < opponent:
            return "loss"
        return "push"

    def outcome_for_game(
        self,
        data: dict,
        pick: str,
        market: str = "final_winner",
        line: float | None = None,
    ) -> str:
        market = self.normalize_market(market)
        if market == "final_winner":
            return self.winner_outcome(data, pick, half=False)
        if market == "half_winner":
            return self.winner_outcome(data, pick, half=True)
        if market == "final_total":
            if line is None:
                raise ValueError("final_total predictions require a line.")
            return self.total_outcome(data, pick, line, half=False)
        if market == "half_total":
            if line is None:
                raise ValueError("half_total predictions require a line.")
            return self.total_outcome(data, pick, line, half=True)
        if market == "final_handicap":
            if line is None:
                raise ValueError("final_handicap predictions require a line.")
            return self.handicap_outcome(data, pick, line, half=False)
        if market == "half_handicap":
            if line is None:
                raise ValueError("half_handicap predictions require a line.")
            return self.handicap_outcome(data, pick, line, half=True)
        raise ValueError(f"Unsupported prediction market: {market}")

    @staticmethod
    def headers() -> list[str]:
        return [
            "prediction_id",
            "game_id",
            "game_date",
            "market",
            "pick",
            "line",
            "rate",
            "stake",
            "status",
            "outcome",
            "balance_before",
            "balance_after",
            "created_at",
            "resolved_at",
        ]

    def has_header(self, row: list[str]) -> bool:
        normalized = [str(value).strip() for value in row]
        return all(header in normalized for header in self.headers())

    def sheet(self):
        module = self.module
        if not hasattr(module, "_sheets_client") and hasattr(
            module, "_prediction_sheet"
        ):
            return module._prediction_sheet()
        spreadsheet = module._sheets_client.spreadsheet(
            module.PREDICTION_SPREADSHEET_KEY
        )
        try:
            return spreadsheet.worksheet(module.PREDICTION_SHEET_NAME)
        except module.gspread.exceptions.WorksheetNotFound:
            return spreadsheet.get_worksheet(0)

    def rows(self, sheet, *, ensure_header: bool = True) -> list[list[str]]:
        module = self.module
        if not hasattr(module, "PREDICTION_SHEET_NAME") and hasattr(
            module, "_prediction_rows"
        ):
            return module._prediction_rows(sheet)
        rows = sheet.get_all_values()
        headers = self.headers()
        if not rows:
            if ensure_header:
                sheet.append_row(headers, value_input_option="USER_ENTERED")
            return [headers]
        if not self.has_header(rows[0]):
            if ensure_header:
                sheet.insert_row(headers, index=1, value_input_option="USER_ENTERED")
            return [headers] + rows
        return rows

    def last_balance(self, rows: list[list[str]]) -> float:
        module = self.module
        headers = rows[0] if rows else self.headers()
        try:
            balance_idx = headers.index("balance_after")
        except ValueError:
            return module.PREDICTION_STARTING_BALANCE
        for row in reversed(rows[1:]):
            if len(row) > balance_idx and str(row[balance_idx]).strip():
                return self.to_float(
                    row[balance_idx], module.PREDICTION_STARTING_BALANCE
                )
        return module.PREDICTION_STARTING_BALANCE

    def balance_before_formula(self, headers: list[str], row_num: int):
        if row_num <= 2:
            return self.module.PREDICTION_STARTING_BALANCE
        balance_after_col = self.module.col_to_letter(
            headers.index("balance_after") + 1
        )
        return f"={balance_after_col}{row_num - 1}"

    def balance_after_formula(self, headers: list[str], row_num: int) -> str:
        module = self.module
        outcome_col = module.col_to_letter(headers.index("outcome") + 1)
        rate_col = module.col_to_letter(headers.index("rate") + 1)
        stake_col = module.col_to_letter(headers.index("stake") + 1)
        status_col = module.col_to_letter(headers.index("status") + 1)
        balance_before_col = module.col_to_letter(headers.index("balance_before") + 1)
        outcome_ref = f"{outcome_col}{row_num}"
        rate_ref = f"{rate_col}{row_num}"
        stake_ref = f"{stake_col}{row_num}"
        status_ref = f"{status_col}{row_num}"
        before_ref = f"{balance_before_col}{row_num}"
        return (
            f'=IF({outcome_ref}="win",{before_ref}+{stake_ref}*{rate_ref},'
            f'IF({outcome_ref}="loss",{before_ref}-{stake_ref},'
            f'IF(OR({outcome_ref}="push",{outcome_ref}="void",'
            f'{status_ref}="pending",{outcome_ref}=""),{before_ref},{before_ref})))'
        )

    def stats_from_rows(self, rows: list[list[str]]) -> dict:
        headers = rows[0] if rows else self.headers()
        index = {name: idx for idx, name in enumerate(headers)}
        wins = losses = pushes = 0
        for row in rows[1:]:
            row = row + [""] * (len(headers) - len(row))
            if row[index.get("status", -1)] != "resolved":
                continue
            outcome = str(row[index.get("outcome", -1)]).lower()
            if outcome == "win":
                wins += 1
            elif outcome == "loss":
                losses += 1
            elif outcome == "push":
                pushes += 1

        graded = wins + losses
        win_rate = round((wins / graded) * 100, 1) if graded else 0.0
        return {
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "graded": graded,
            "win_rate": win_rate,
            "balance": self.last_balance(rows),
        }

    def stats_after(self, rows: list[list[str]], outcome: str, balance_after: float):
        stats = self.stats_from_rows(rows)
        outcome = str(outcome).lower()
        if outcome == "win":
            stats["wins"] += 1
        elif outcome == "loss":
            stats["losses"] += 1
        elif outcome == "push":
            stats["pushes"] += 1
        stats["graded"] = stats["wins"] + stats["losses"]
        stats["win_rate"] = (
            round((stats["wins"] / stats["graded"]) * 100, 1)
            if stats["graded"]
            else 0.0
        )
        stats["balance"] = round(float(balance_after), 4)
        return stats


class NpbRowsService(NpbModuleService):
    """Build rows and identities for NPB 賽錄, 熱身賽紀錄, and 分析表紀錄."""

    def schedule_row(self, seq: int, data: dict) -> list:
        ai = data["away_innings"]
        hi = data["home_innings"]
        return [
            data["賽事編號"],
            seq,
            data["日期"],
            data["客隊"],
            data["客隊先發"],
            data["主隊"],
            data["主隊先發"],
            data["球場"],
            data["主審"],
            *ai,
            data["客總分"],
            data["客總安打"],
            data["客總失誤"],
            *hi,
            data["主總分"],
            data["主總安打"],
            data["主總失誤"],
            *data["客先發投球"],
            *data["客總投球"],
            *data["主先發投球"],
            *data["主總投球"],
            data["客投別"],
            data["主投別"],
            *data["客打擊"],
            *data["主打擊"],
        ]

    @staticmethod
    def analysis_date(date_str: str) -> str:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{dt.year}/{dt.month}/{dt.day}"

    def analysis_team_league(self, team_name: str) -> str | None:
        module = self.module
        normalized = str(team_name or "").replace(" ", "")
        if not normalized:
            return None

        aliases = {"横浜": "DeNA", "橫濱": "DeNA"}
        raw = aliases.get(normalized, normalized)
        if raw in module.NPB_TEAMS:
            return module.NPB_TEAMS[raw]["league"]

        for key, info in module.NPB_TEAMS.items():
            if normalized == str(info["name"]).replace(" ", ""):
                return info["league"]
            if normalized == module.display_team_name(key).replace(" ", ""):
                return info["league"]
        return None

    def analysis_game_type(self, data: dict) -> str:
        away = data.get("客隊原名", "")
        home = data.get("主隊原名", "")
        if not away or not home:
            return "例行賽"
        away_league = self.analysis_team_league(away)
        home_league = self.analysis_team_league(home)
        if not away_league or not home_league:
            return "例行賽"
        if away_league != home_league:
            return "交流戰"
        return home_league

    def analysis_game_type_from_teams(
        self, away_team: str, home_team: str
    ) -> str | None:
        away_league = self.analysis_team_league(away_team)
        home_league = self.analysis_team_league(home_team)
        if not home_league:
            return None
        if away_league and away_league != home_league:
            return "交流戰"
        return home_league

    @staticmethod
    def analysis_day_night(game_time: str) -> str:
        m = re.match(r"^(\d{1,2}):(\d{2})$", str(game_time or ""))
        if not m:
            return ""
        return "夜" if int(m.group(1)) >= 17 else "日"

    def analysis_team_name(self, team_name: str) -> str:
        return self.module.display_team_name(team_name)

    def analysis_field(self, data: dict) -> str:
        module = self.module
        raw = data.get("球場原名") or data.get("球場") or ""
        if not raw:
            return ""

        home = data.get("主隊原名") or data.get("主場隊伍") or ""
        home_fields = module.NPB_TEAM_HOME_FIELDS.get(home)
        if home_fields is not None and raw not in home_fields:
            return "地方球場"

        return module.ANALYSIS_FIELDS.get(raw, "地方球場")

    @staticmethod
    def analysis_hand(hand: str) -> str:
        if not hand:
            return ""
        return hand if hand.endswith("投") else f"{hand}投"

    @staticmethod
    def analysis_marks(away_score: int, home_score: int) -> tuple[str, str]:
        if away_score > home_score:
            return "○", "●"
        if away_score < home_score:
            return "●", "○"
        return "△", "△"

    @staticmethod
    def analysis_innings(vals: list) -> tuple[list, str]:
        innings = ["" if str(v) in ("", "×") else v for v in vals[:9]]
        extras = []
        for v in vals[9:12]:
            if str(v).isdigit():
                extras.append(int(v))
        return innings, (sum(extras) if extras else "")

    @staticmethod
    def analysis_total_bases(batting: list) -> int:
        hits = int(batting[2] or 0)
        doubles = int(batting[4] or 0)
        triples = int(batting[5] or 0)
        homers = int(batting[6] or 0)
        return hits + doubles + triples * 2 + homers * 3

    def analysis_long_hits(self, batting: list) -> int:
        return int(batting[4] or 0) + int(batting[5] or 0) + int(batting[6] or 0)

    @staticmethod
    def analysis_qs(starter_pitch: list):
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

    def analysis_starter_block(self, starter_pitch: list) -> list:
        return [
            starter_pitch[0],
            starter_pitch[1],
            starter_pitch[4],
            starter_pitch[5],
            starter_pitch[6] + starter_pitch[7],
            starter_pitch[11],
            starter_pitch[12],
            starter_pitch[4] + starter_pitch[5] * 3,
            self.analysis_qs(starter_pitch),
        ]

    def analysis_team_total_block(
        self,
        opposing_pitch: list,
        opposing_batting: list,
        own_batting: list,
        score: int,
        earned_runs: int,
        errors: int,
    ) -> list:
        return [
            opposing_pitch[0],
            opposing_pitch[2],
            opposing_batting[0],
            opposing_batting[2],
            opposing_batting[6],
            opposing_batting[10],
            opposing_batting[8] + opposing_batting[9],
            score,
            earned_runs,
            errors,
            own_batting[7],
            own_batting[13],
            own_batting[14],
            self.analysis_total_bases(own_batting),
            self.analysis_long_hits(own_batting),
        ]

    def analysis_row(self, seq: int, data: dict) -> list:
        away_score = int(data["客總分"])
        home_score = int(data["主總分"])
        away_mark, home_mark = self.analysis_marks(away_score, home_score)
        away_innings, away_ot = self.analysis_innings(data["away_innings"])
        home_innings, home_ot = self.analysis_innings(data["home_innings"])

        away_bat = data["客打擊"]
        home_bat = data["主打擊"]
        away_starter_view = self.analysis_starter_block(data["客先發投球"])
        home_starter_view = self.analysis_starter_block(data["主先發投球"])
        away_total_view = self.analysis_team_total_block(
            data["客總投球"],
            home_bat,
            away_bat,
            home_score,
            data["客總投球"][12],
            data["客總失誤"],
        )
        home_total_view = self.analysis_team_total_block(
            data["主總投球"],
            away_bat,
            home_bat,
            away_score,
            data["主總投球"][12],
            data["主總失誤"],
        )

        return [
            seq,
            self.analysis_date(data["日期"]),
            self.analysis_day_night(data.get("時間", "")),
            self.analysis_game_type(data),
            data["主審"],
            self.analysis_hand(data["客投別"]),
            self.analysis_hand(data["主投別"]),
            away_mark,
            self.analysis_team_name(data.get("客隊原名", data["客隊"])),
            away_score,
            home_score,
            self.analysis_team_name(data.get("主隊原名", data["主隊"])),
            home_mark,
            self.analysis_field(data),
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

    def sailu_row(self, seq: int, data: dict) -> list:
        module = self.module
        ai = data["away_innings"]
        hi = data["home_innings"]
        away_raw = data.get("客場隊伍") or data.get("客隊原名")
        home_raw = data.get("主場隊伍") or data.get("主隊原名")
        away_starter = data.get("客場先發") or data.get("客隊先發", "")
        home_starter = data.get("主場先發") or data.get("主隊先發", "")
        venue = data.get("球場原名") or data.get("球場", "")
        away_hits = data.get("客安打", data.get("客總安打", 0))
        away_errors = data.get("客失誤", data.get("客總失誤", 0))
        home_score = data.get("主總", data.get("主總分", 0))
        home_hits = data.get("主安打", data.get("主總安打", 0))
        home_errors = data.get("主失誤", data.get("主總失誤", 0))
        game_status = data.get("賽事狀態", "")
        if "客場隊伍" not in data and game_status == "試合終了":
            game_status = "正常"
        away_code = data.get("客隊代號") or module.NPB_TEAMS[away_raw]["id"]
        home_code = data.get("主隊代號") or module.NPB_TEAMS[home_raw]["id"]
        away_ip = data.get("客投局")
        if away_ip is None:
            away_ip = data.get("客先發投球", [""])[0]
        home_ip = data.get("主投局")
        if home_ip is None:
            home_ip = data.get("主先發投球", [""])[0]
        away_er = data.get("客責失")
        if away_er is None:
            away_er = data.get("客先發投球", [0] * 13)[12]
        home_er = data.get("主責失")
        if home_er is None:
            home_er = data.get("主先發投球", [0] * 13)[12]

        return [
            seq,
            data["賽事編號"],
            away_raw,
            away_starter,
            home_raw,
            home_starter,
            data["時間"],
            venue,
            data["主審"],
            *ai[:4],
            *ai[4:8],
            *ai[8:12],
            data["客總分"],
            away_hits,
            away_errors,
            *hi[:4],
            *hi[4:8],
            *hi[8:12],
            home_score,
            home_hits,
            home_errors,
            game_status,
            data["日期"],
            away_code,
            home_code,
            data["客投別"],
            data["主投別"],
            away_ip,
            home_ip,
            away_er,
            data["客QS"],
            home_er,
            data["主QS"],
        ]

    @staticmethod
    def sailu_formula_row(row_num: int) -> list[str]:
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
            '=IF(BH{0}="","",IF(BH{0}="平","平",IF(BH{0}="勝","敗","勝")))'.format(
                row_num
            ),
            '=IF(BH{0}="勝",客總分-主總分,IF(BH{0}="敗",主總分-客總分,0))'.format(
                row_num
            ),
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

    @staticmethod
    def chunked(seq: list, size: int):
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    @staticmethod
    def placeholder_rows(sheet) -> list[int]:
        col_a = sheet.col_values(1)[1:]
        col_b = sheet.col_values(2)[1:]
        return [
            i + 2
            for i, a in enumerate(col_a)
            if a and not (col_b[i] if i < len(col_b) else "")
        ]

    def ensure_target_sailu_capacity(self, sheet, needed_rows: int) -> list[int]:
        placeholder_rows = self.placeholder_rows(sheet)
        if len(placeholder_rows) >= needed_rows:
            return placeholder_rows

        missing = needed_rows - len(placeholder_rows)
        start_row = sheet.row_count + 1
        sheet.add_rows(missing)

        prev_seq = int(sheet.acell(f"A{start_row - 1}").value)
        seq_values = [[prev_seq + offset + 1] for offset in range(missing)]
        formula_values = [
            self.sailu_formula_row(row_num)
            for row_num in range(start_row, start_row + missing)
        ]

        for offset, chunk in enumerate(self.chunked(seq_values, 200)):
            chunk_start = start_row + offset * 200
            chunk_end = chunk_start + len(chunk) - 1
            sheet.update(
                f"A{chunk_start}:A{chunk_end}",
                chunk,
                value_input_option="USER_ENTERED",
            )

        for offset, chunk in enumerate(self.chunked(formula_values, 200)):
            chunk_start = start_row + offset * 200
            chunk_end = chunk_start + len(chunk) - 1
            sheet.update(
                f"AZ{chunk_start}:BT{chunk_end}",
                chunk,
                value_input_option="USER_ENTERED",
            )

        return self.placeholder_rows(sheet)

    def write_regular_sailu_games(
        self,
        sheet,
        games: list[tuple[str, dict]],
        *,
        auto_extend_target: bool = False,
    ):
        if not games:
            return 0, []

        placeholder_rows = (
            self.ensure_target_sailu_capacity(sheet, len(games))
            if auto_extend_target
            else self.placeholder_rows(sheet)
        )

        filled = 0
        for (gid, data), row_num in zip(games, placeholder_rows):
            row_values = self.sailu_row(0, data)[1:]
            sheet.update(
                f"B{row_num}:AY{row_num}",
                [row_values],
                value_input_option="USER_ENTERED",
            )
            print(f"  [sailu] Row {row_num} ← {gid}")
            filled += 1

        return filled, games[len(placeholder_rows) :]

    def exhibition_row(self, data: dict) -> list[str]:
        module = self.module
        away_raw = data.get("客場隊伍") or data.get("客隊原名")
        home_raw = data.get("主場隊伍") or data.get("主隊原名")
        away_score = int(data.get("客總分", data.get("客總", 0)))
        home_score = int(data.get("主總", data.get("主總分", 0)))
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
            module.display_team_name(away_raw),
            str(away_score),
            str(home_score),
            module.display_team_name(home_raw),
            home_mark,
            data["球場"],
            *away_innings,
            away_ot,
            *home_innings,
            home_ot,
        ]

    def exhibition_identity(self, data: dict) -> tuple[str, str, str]:
        module = self.module
        away_raw = data.get("客場隊伍") or data.get("客隊原名")
        home_raw = data.get("主場隊伍") or data.get("主隊原名")
        return (
            data["日期"],
            module.display_team_name(away_raw),
            module.display_team_name(home_raw),
        )

    @staticmethod
    def existing_exhibition_identities(sheet) -> set[tuple[str, str, str]]:
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

    def analysis_identity(self, data: dict) -> tuple[str, str, str]:
        return (
            self.analysis_date(data["日期"]),
            self.analysis_team_name(data.get("客隊原名", data["客隊"])),
            self.analysis_team_name(data.get("主隊原名", data["主隊"])),
        )

    @staticmethod
    def analysis_identity_from_row(row: list[str]) -> tuple[str, str, str] | None:
        if len(row) < 12 or not row[1] or not row[8] or not row[11]:
            return None
        return (row[1], row[8], row[11])

    @staticmethod
    def analysis_row_year(row: list[str]) -> int | None:
        if len(row) < 2 or not row[1]:
            return None
        try:
            return datetime.strptime(row[1], "%Y/%m/%d").year
        except ValueError:
            return None

    @staticmethod
    def analysis_row_date(row: list[str]) -> datetime | None:
        if len(row) < 2 or not row[1]:
            return None
        try:
            return datetime.strptime(row[1], "%Y/%m/%d")
        except ValueError:
            return None

    @staticmethod
    def last_analysis_seq(rows: list[list[str]]) -> int:
        last_seq = 0
        for row in rows[2:]:
            if not row:
                continue
            try:
                last_seq = max(last_seq, int(row[0]))
            except (TypeError, ValueError):
                continue
        return last_seq

    def analysis_insert_index(self, rows: list[list[str]], date_str: str) -> int:
        game_date = datetime.strptime(date_str, "%Y-%m-%d")
        insert_at = len(rows) + 1
        for row_num, row in enumerate(rows[2:], start=3):
            row_date = self.analysis_row_date(row)
            if row_date and row_date > game_date:
                return row_num
            if row_date:
                insert_at = row_num + 1
        return insert_at


class NpbLeagueSheetService(NpbModuleService):
    """Build and write NPB 近十場 league sheets."""

    @staticmethod
    def hex_to_rgb(hex_color: str) -> dict:
        h = hex_color.lstrip("#")
        return {
            "red": int(h[0:2], 16) / 255,
            "green": int(h[2:4], 16) / 255,
            "blue": int(h[4:6], 16) / 255,
        }

    @staticmethod
    def col_to_letter(col: int) -> str:
        result = ""
        while col > 0:
            col, rem = divmod(col - 1, 26)
            result = chr(65 + rem) + result
        return result

    def display_field_name(self, venue: str) -> str:
        module = self.module
        field = module.NPB_FIELDS.get(venue, venue)
        return f"{field[0]} {field[1]}" if len(field) == 2 else field

    def build_block_values(self, team_key: str, games: list[dict]) -> list[list]:
        module = self.module
        display_name = module.NPB_TEAMS[team_key]["name"]
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

        sorted_games = sorted(
            games,
            key=lambda g: datetime.strptime(g["日期"], "%Y/%m/%d"),
        )[-module.GAMES_COUNT :]

        rows = [header]
        for i in range(module.GAMES_COUNT):
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
                    self.display_field_name(g.get("球場", "")),
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

        rows.append(self.avg_row("近十場", sorted_games))
        rows.append(self.avg_row("近五場", sorted_games[-5:]))
        return rows

    @staticmethod
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

    @staticmethod
    def pitcher_font_size(name: str) -> int:
        n = len(name.replace(" ", ""))
        if n > 7:
            return 6
        if n > 5:
            return 8
        return 10

    def pitcher_font_requests(
        self, sheet_id: int, games: list[dict], game_start_row: int, col_start: int
    ) -> list[dict]:
        module = self.module
        sorted_games = sorted(
            games, key=lambda g: datetime.strptime(g["日期"], "%Y/%m/%d")
        )[-module.GAMES_COUNT :]
        pitcher_col = col_start + 1
        requests = []

        for i in range(module.GAMES_COUNT):
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
                                "textFormat": {"fontSize": self.pitcher_font_size(name)}
                            }
                        },
                        "fields": "userEnteredFormat.textFormat.fontSize",
                    }
                }
            )
        return requests

    @staticmethod
    def to_number(value):
        if value in ("", None):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def font_color_request(
        self, sheet_id: int, row_0idx: int, col_0idx: int, hex_color: str
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
                        "textFormat": {"foregroundColor": self.hex_to_rgb(hex_color)}
                    }
                },
                "fields": "userEnteredFormat.textFormat.foregroundColor",
            }
        }

    def game_font_color_requests(
        self, sheet_id: int, games: list[dict], game_start_row: int, col_start: int
    ) -> list[dict]:
        module = self.module
        sorted_games = sorted(
            games, key=lambda g: datetime.strptime(g["日期"], "%Y/%m/%d")
        )[-module.GAMES_COUNT :]
        runs_col = col_start + 4
        allowed_col = col_start + 5
        hits_col = col_start + 7
        requests = []

        for i in range(module.GAMES_COUNT):
            row_0idx = game_start_row - 1 + i
            runs_color = module.DEFAULT_FONT
            allowed_color = module.DEFAULT_FONT
            hits_color = module.DEFAULT_FONT

            if i < len(sorted_games):
                game = sorted_games[i]
                runs = self.to_number(game.get("得分"))
                allowed = self.to_number(game.get("失分"))
                hits = self.to_number(game.get("安打"))

                if runs is not None and allowed is not None:
                    if runs > allowed:
                        runs_color = module.SCORE_WIN_FONT
                    elif allowed > runs:
                        allowed_color = module.SCORE_LOSS_FONT
                    else:
                        runs_color = module.SCORE_TIE_FONT
                        allowed_color = module.SCORE_TIE_FONT

                if hits is not None and hits >= 10:
                    hits_color = module.HITS_10_PLUS_FONT

            requests.append(
                self.font_color_request(sheet_id, row_0idx, runs_col, runs_color)
            )
            requests.append(
                self.font_color_request(sheet_id, row_0idx, allowed_col, allowed_color)
            )
            requests.append(
                self.font_color_request(sheet_id, row_0idx, hits_col, hits_color)
            )

        return requests

    def header_format_request(
        self, sheet_id: int, team_key: str, header_row: int, col_start: int
    ) -> dict:
        info = self.module.NPB_TEAMS[team_key]
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
                        "backgroundColor": self.hex_to_rgb(info["fill"]),
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": self.hex_to_rgb(info["font"]),
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        }

    def update_league_sheet(
        self,
        sheet_name: str,
        matchups: list[tuple[str, str]],
        all_games: dict[str, list[dict]],
    ):
        module = self.module
        sheet = module.get_worksheet(sheet_name)
        value_updates = []
        format_requests = []

        for col_idx, (away_key, home_key) in enumerate(matchups[:3]):
            col_start = module.BLOCK_COLS[col_idx]
            col_end = col_start + 11
            col_start_l = self.col_to_letter(col_start)
            col_end_l = self.col_to_letter(col_end)

            away_games = all_games.get(away_key, [])
            top_values = self.build_block_values(away_key, away_games)
            value_updates.append(
                {
                    "range": (
                        f"{col_start_l}{module.TOP_HEADER_ROW}:"
                        f"{col_end_l}{module.TOP_AVG5_ROW}"
                    ),
                    "values": top_values,
                }
            )
            format_requests.append(
                self.header_format_request(
                    sheet.id, away_key, module.TOP_HEADER_ROW, col_start
                )
            )
            format_requests.extend(
                self.pitcher_font_requests(
                    sheet.id, away_games, module.TOP_GAME_START, col_start
                )
            )
            format_requests.extend(
                self.game_font_color_requests(
                    sheet.id, away_games, module.TOP_GAME_START, col_start
                )
            )

            home_games = all_games.get(home_key, [])
            bottom_values = self.build_block_values(home_key, home_games)
            value_updates.append(
                {
                    "range": (
                        f"{col_start_l}{module.BOTTOM_HEADER_ROW}:"
                        f"{col_end_l}{module.BOTTOM_AVG5_ROW}"
                    ),
                    "values": bottom_values,
                }
            )
            format_requests.append(
                self.header_format_request(
                    sheet.id, home_key, module.BOTTOM_HEADER_ROW, col_start
                )
            )
            format_requests.extend(
                self.pitcher_font_requests(
                    sheet.id, home_games, module.BOTTOM_GAME_START, col_start
                )
            )
            format_requests.extend(
                self.game_font_color_requests(
                    sheet.id, home_games, module.BOTTOM_GAME_START, col_start
                )
            )

        sheet.batch_update(value_updates, value_input_option="USER_ENTERED")
        sheet.spreadsheet.batch_update({"requests": format_requests})
        print(
            f"[{sheet_name}] Updated {len(value_updates)} "
            "blocks with header colours."
        )


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

            sailu_service = NpbSailuService(module=module)
            new_sailu_ids = []
            try:
                new_sailu_ids = await sailu_service.update(session)
            except Exception as e:
                errors.append(f"update_sailu_sheet: {e}")

            huizi_date = None
            try:
                analysis_game_ids = new_sailu_ids or module._sailu_game_ids_for_date(
                    sailu_service.status_date
                )
                await NpbAnalysisService(module=module).update(
                    session,
                    game_ids=analysis_game_ids,
                    scraped_games=sailu_service.written_regular_game_data,
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

            await asyncio.sleep(5)
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
        league_sheet_service = NpbLeagueSheetService(module=module)
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

            team_keys = list(
                dict.fromkeys(
                    key
                    for matchup in matchups
                    for key in matchup
                    if key in module.NPB_TEAMS
                )
            )
            if not team_keys:
                team_keys = list(league_teams.keys())

            all_game_ids: dict[str, list[str]] = {}
            for team_key in team_keys:
                team_info = module.NPB_TEAMS[team_key]
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
            for team_key in team_keys:
                team_info = module.NPB_TEAMS[team_key]
                team_name = team_info["name"]
                game_list = []
                for gid in all_game_ids[team_key]:
                    cached = game_cache.get(gid)
                    if cached and team_name in cached:
                        game_list.append(cached[team_name])
                all_games[team_key] = game_list
                print(f"  {team_key}: {len(game_list)} games with data")

            try:
                league_sheet_service.update_league_sheet(
                    sheet_name, matchups, all_games
                )
            except Exception as e:
                errors.append(f"update_league_sheet({sheet_name}): {e}")


class NpbSailuService:
    """Service entrypoint for NPB 賽錄 updates."""

    def __init__(self, module=None):
        self._module = module
        self.written_regular_game_data: list[tuple[str, dict]] = []
        self.status_date: str | None = None

    @property
    def module(self):
        if self._module is None:
            self._module = importlib.import_module("npb")
        return self._module

    async def update(self, session):
        module = self.module
        rows_service = NpbRowsService(module=module)
        self.written_regular_game_data = []
        print("\n=== 賽錄 update ===")
        sheet = module.get_worksheet(
            module.SAILU_SHEET_NAME, module.SAILU_SPREADSHEET_KEY
        )
        target_sheet = module.get_worksheet(
            module.SAILU_SHEET_NAME, module.SAILU_TARGET_SPREADSHEET_KEY
        )
        exhibition_sheet = module.get_worksheet(
            module.EXHIBITION_SHEET_NAME, module.SAILU_SPREADSHEET_KEY
        )

        existing_ids = set(v for v in sheet.col_values(2)[1:] if v)
        status_service = NpbStatusService(module=module)
        status_sheet = module.get_npb_status_worksheet()
        status_date = status_service.effective_date_str()
        self.status_date = status_date
        if status_service.all_games_resolved_for_date(status_sheet, status_date):
            print(f"[sailu] {status_date} already resolved. Skipping 賽錄 scrape.")
            return []

        target_existing_ids = set(v for v in target_sheet.col_values(2)[1:] if v)
        existing_exhibition = rows_service.existing_exhibition_identities(
            exhibition_sheet
        )
        print(
            f"[sailu] {len(rows_service.placeholder_rows(sheet))} "
            "source placeholder row(s) available."
        )
        print(
            f"[sailu] {len(rows_service.placeholder_rows(target_sheet))} "
            "target placeholder row(s) available."
        )

        await status_service.sync_team_schedule_statuses(
            status_sheet, status_date, existing_ids, session
        )
        if status_service.all_games_resolved_for_date(status_sheet, status_date):
            print(f"[sailu] {status_date} already resolved after status sync.")
            return []

        new_ids = sorted(
            gid
            for gid in status_service.finished_unresolved_game_ids_for_date(
                status_sheet, status_date
            )
            if gid not in existing_ids
        )
        if not new_ids:
            print("[sailu] No new games to add.")
            return []

        print(f"[sailu] {len(new_ids)} new game(s): {new_ids}")

        new_games: list[tuple[str, dict]] = []
        for i in range(0, len(new_ids), module.MAX_CONCURRENT):
            batch = new_ids[i : i + module.MAX_CONCURRENT]
            scraped = await asyncio.gather(
                *[
                    module.get_schedule_game_data(gid, session, retry=False)
                    for gid in batch
                ],
                return_exceptions=True,
            )
            for gid, data in zip(batch, scraped):
                if isinstance(data, Exception):
                    print(f"  [sailu] get_schedule_game_data({gid}): {data}")
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
            if not module.is_exhibition_game(data)
        ]
        exhibition_games = [
            (gid, data) for gid, data in new_games if module.is_exhibition_game(data)
        ]

        source_regular_games = [
            (gid, data) for gid, data in regular_games if gid not in existing_ids
        ]
        target_regular_games = [
            (gid, data) for gid, data in regular_games if gid not in target_existing_ids
        ]

        filled, overflow = rows_service.write_regular_sailu_games(
            sheet, source_regular_games
        )
        if overflow:
            print(
                f"[sailu] WARNING: {len(overflow)} source game(s) skipped — "
                "no placeholder rows left: "
                + str([gid for gid, _ in overflow])
                + "\n  → Add more pre-populated formula rows to 賽錄 and re-run."
            )

        target_filled, target_overflow = rows_service.write_regular_sailu_games(
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
            ident = rows_service.exhibition_identity(data)
            if ident in existing_exhibition:
                print(f"  [exhibition] skip existing ← {gid}")
                continue
            exhibition_rows.append(rows_service.exhibition_row(data))
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
        written_ids = list(dict.fromkeys(source_written_ids + target_written_ids))
        self.written_regular_game_data = [
            (gid, data) for gid, data in regular_games if gid in written_ids
        ]
        for gid, data in regular_games:
            if gid in source_written_ids or gid in existing_ids:
                status_service.upsert(
                    status_sheet,
                    data["日期"],
                    gid,
                    "試合終了",
                    True,
                )
        return written_ids


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
        scraped_games: list[tuple[str, dict]] | None = None,
    ):
        module = self.module
        rows_service = NpbRowsService(module=module)
        if year is None:
            year = module.ANALYSIS_SEASON
        print(f"\n=== {module.ANALYSIS_SHEET_NAME} update ({year}) ===")
        sheet = module.get_worksheet(
            module.ANALYSIS_SHEET_NAME, module.NPB_SPREADSHEET_KEY
        )
        rows = sheet.get_all_values()
        season_rows = [
            row for row in rows[2:] if rows_service.analysis_row_year(row) == year
        ]
        existing = {
            ident
            for row in season_rows
            if (ident := rows_service.analysis_identity_from_row(row))
        }
        last_seq = rows_service.last_analysis_seq(rows)

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
        scraped_by_id = {
            gid: data
            for gid, data in (scraped_games or [])
            if gid in candidate_ids and data
        }

        def add_if_missing(gid: str, data: dict):
            if target_date and data["日期"] != module._date_key(target_date):
                return
            ident = rows_service.analysis_identity(data)
            if ident not in existing:
                new_games.append((gid, data))
                existing.add(ident)
                print(f"  [analysis] missing ← {gid} {ident}")

        for gid in candidate_ids:
            data = scraped_by_id.get(gid)
            if data:
                add_if_missing(gid, data)

        missing_candidate_ids = [
            gid for gid in candidate_ids if gid not in scraped_by_id
        ]
        if scraped_by_id:
            print(
                f"[analysis] Reusing {len(scraped_by_id)} scraped game(s) "
                "from 賽錄 update."
            )

        if missing_candidate_ids:
            for i in range(0, len(missing_candidate_ids), module.MAX_CONCURRENT):
                batch = missing_candidate_ids[i : i + module.MAX_CONCURRENT]
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
                        add_if_missing(gid, data)
                    else:
                        print(f"  [analysis] No data for {gid}")
                if i + module.MAX_CONCURRENT < len(missing_candidate_ids):
                    await asyncio.sleep(2)

        if not new_games:
            print("[analysis] No new games to append.")
            return 0

        new_games.sort(key=lambda x: (x[1]["日期"], x[0]))
        inserted = 0
        for gid, data in new_games:
            row_values = rows_service.analysis_row(last_seq + inserted + 1, data)
            insert_at = rows_service.analysis_insert_index(rows, data["日期"])
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
        rows_service = NpbRowsService(module=module)
        if year is None:
            year = module.ANALYSIS_SEASON
        print(f"\n=== {module.ANALYSIS_SHEET_NAME} league repair ({year}) ===")
        sheet = module.get_worksheet(
            module.ANALYSIS_SHEET_NAME, module.NPB_SPREADSHEET_KEY
        )
        rows = sheet.get_all_values()
        updates = []

        for row_num, row in enumerate(rows[2:], start=3):
            if rows_service.analysis_row_year(row) != year:
                continue
            away_team = row[8] if len(row) > 8 else ""
            home_team = row[11] if len(row) > 11 else ""
            game_type = rows_service.analysis_game_type_from_teams(away_team, home_team)
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
        huizi = module.get_worksheet(
            module.HUIZI_SHEET_NAME, module.NPB_SPREADSHEET_KEY
        )
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
        logic = _NpbPredictionLogic(module=module)
        if stake is None:
            stake = module.PREDICTION_DEFAULT_STAKE
        sheet = sheet or (None if dry_run else logic.sheet())
        rows = (
            logic.rows(sheet, ensure_header=not dry_run) if sheet else [logic.headers()]
        )
        headers = rows[0]
        row_num = len(rows) + 1
        balance_before = logic.last_balance(rows)
        prediction_id = module.secrets.token_hex(8)
        created_at = logic.now()
        market = logic.normalize_market(market)
        pick = logic.validate_pick(pick, market)
        prediction_text = logic.prediction_text(
            game_id,
            pick,
            float(rate),
            float(stake),
            market=market,
            line=line,
        )

        balance_after = logic.calculate_balance(balance_before, stake, rate, "pending")
        values = {
            "prediction_id": prediction_id,
            "game_id": game_id,
            "game_date": game_date,
            "market": market,
            "pick": pick,
            "line": "" if line is None else float(line),
            "rate": float(rate),
            "stake": float(stake),
            "status": "pending",
            "outcome": "",
            "balance_before": (
                balance_before
                if dry_run
                else logic.balance_before_formula(headers, row_num)
            ),
            "balance_after": (
                balance_after
                if dry_run
                else logic.balance_after_formula(headers, row_num)
            ),
            "created_at": created_at,
            "resolved_at": "",
        }
        row = [values.get(header, "") for header in headers]
        if not dry_run:
            sheet.append_row(row, value_input_option="USER_ENTERED")
        return {
            "prediction_id": prediction_id,
            "balance_before": balance_before,
            "balance_after": balance_after,
            "prediction_text": prediction_text,
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
        logic = _NpbPredictionLogic(module=module)
        game_status = str(data.get("賽事狀態", "")).strip()
        if game_status != "試合終了":
            print(
                f"[prediction] Game {game_id} is not finished "
                f"({game_status or 'unknown'}); keeping prediction pending."
            )
            return 0
        sheet = sheet or logic.sheet()
        rows = logic.rows(sheet)
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
                balance_after = logic.to_float(row[index["balance_after"]], None)
                if balance_after is None:
                    rate = logic.to_float(row[index["rate"]])
                    stake = logic.to_float(
                        row[index["stake"]], module.PREDICTION_DEFAULT_STAKE
                    )
                    outcome = row[index["outcome"]] or row[index["status"]]
                    try:
                        balance_after = logic.calculate_balance(
                            running_balance, stake, rate, outcome
                        )
                    except ValueError:
                        balance_after = running_balance
                running_balance = balance_after
                continue

            market = row[index["market"]] or "final_winner"
            pick = row[index["pick"]]
            line = row[index["line"]] or None
            rate = logic.to_float(row[index["rate"]])
            stake = logic.to_float(row[index["stake"]], module.PREDICTION_DEFAULT_STAKE)
            outcome = logic.outcome_for_game(data, pick, market=market, line=line)
            balance_before = running_balance
            balance_after = logic.calculate_balance(
                balance_before, stake, rate, outcome
            )
            running_balance = balance_after
            resolved_at = logic.now()
            if not dry_run:
                updates = [
                    ("status", "resolved"),
                    ("outcome", outcome),
                    (
                        "balance_before",
                        logic.balance_before_formula(headers, row_num),
                    ),
                    (
                        "balance_after",
                        logic.balance_after_formula(headers, row_num),
                    ),
                    ("resolved_at", resolved_at),
                ]
                backfill_values = {
                    "game_date": data.get("日期", ""),
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
        logic = _NpbPredictionLogic(module=module)
        rows = logic.rows(sheet)
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

    async def reveal_predictions_for_games(
        self, session, game_ids: list[str], **kwargs
    ):
        module = self.module
        post = kwargs.get("post", False)
        dry_run = kwargs.get("dry_run", False)
        total = 0
        sheet = _NpbPredictionLogic(module=module).sheet()
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
