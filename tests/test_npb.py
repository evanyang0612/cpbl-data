"""
Unit tests for NPB data-transformation logic.
Covers: hex_to_rgb, col_to_letter, _pitcher_font_size, build_block_values,
        get_game_info, get_next_scheduled_game, _get_schedule_opponent,
        get_last_n_game_ids.
"""

import asyncio
from argparse import Namespace
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from bs4 import BeautifulSoup as bs

import npb
from baseball.npb_services import (
    NpbAnalysisService,
    NpbLeagueSheetService,
    NpbPredictionService,
    NpbRecentGamesService,
    NpbRowsService,
    NpbStatusService,
)
from npb import (
    DEFAULT_FONT,
    HITS_10_PLUS_FONT,
    HOT_RATE_FONT,
    COLD_RATE_FONT,
    OPPOSITE_FIELD_FONT,
    SCORE_LOSS_FONT,
    SCORE_TIE_FONT,
    SCORE_WIN_FONT,
    _enrich_switch_hitter_pitcher_throws,
    _game_font_color_requests,
    _home_run_direction_font_requests,
    _parse_pitcher_id_lookup,
    _parse_player_throw_hand,
    _get_schedule_opponent,
    _header_format_request,
    _official_next_matchups,
    _analysis_game_type_from_teams,
    _analysis_row,
    _analysis_team_league,
    _parse_official_caught_stealing,
    _parse_batting_table,
    _parse_home_run_events,
    _parse_home_run_pitcher_events,
    _parse_player_bat_hand,
    _parse_pitcher_name_lookup,
    _resolve_matchup_start_date,
    _schedule_status_for_game,
    _pitcher_font_requests,
    _pitcher_font_size,
    _prediction_cli_values,
    build_block_values,
    build_prediction_text,
    calculate_prediction_balance,
    col_to_letter,
    create_npb_prediction,
    get_game_info,
    get_last_n_game_ids,
    get_next_scheduled_game,
    hex_to_rgb,
    is_exhibition_game,
    is_exhibition_game_id,
    _parse_game_kind,
    prediction_outcome_for_game,
    resolve_prediction_game_by_home_team,
    resolve_npb_predictions_for_game,
    GAMES_COUNT,
    NPB_TEAMS,
    PREDICTION_PROMPT_SENTINEL,
)

PREDICTION_HEADERS = [
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


# ---------------------------------------------------------------------------
# _resolve_matchup_start_date
# ---------------------------------------------------------------------------


class TestResolveMatchupStartDate:
    def test_default_is_tomorrow(self):
        assert _resolve_matchup_start_date(today=date(2026, 5, 10)) == date(2026, 5, 11)

    def test_today_aliases_keep_today(self):
        assert _resolve_matchup_start_date("today", date(2026, 5, 10)) == date(
            2026, 5, 10
        )
        assert _resolve_matchup_start_date("今天", date(2026, 5, 10)) == date(
            2026, 5, 10
        )

    def test_explicit_date(self):
        assert _resolve_matchup_start_date("2026-05-12") == date(2026, 5, 12)

    def test_invalid_date_raises_clear_error(self):
        with pytest.raises(ValueError, match="today.*tomorrow.*YYYY-MM-DD"):
            _resolve_matchup_start_date("05/12/2026")


# ---------------------------------------------------------------------------
# _official_next_matchups
# ---------------------------------------------------------------------------


class TestOfficialNextMatchups:
    def _run(self, coro):
        return asyncio.run(coro)

    def test_interleague_keeps_real_cross_league_matchups(self):
        html = """
        <table>
          <tr><th>5/26（火）</th></tr>
          <tr><td><div class="team1">巨人</div><div class="team2">ソフトバンク</div></td></tr>
          <tr><td><div class="team1">ヤクルト</div><div class="team2">西武</div></td></tr>
          <tr><td><div class="team1">横浜DeNA</div><div class="team2">オリックス</div></td></tr>
          <tr><td><div class="team1">中日</div><div class="team2">楽天</div></td></tr>
          <tr><td><div class="team1">阪神</div><div class="team2">日本ハム</div></td></tr>
          <tr><td><div class="team1">広島</div><div class="team2">ロッテ</div></td></tr>
        </table>
        """
        with patch("npb._fetch_once", new=AsyncMock(return_value=html)):
            central = self._run(
                _official_next_matchups("央盟", AsyncMock(), date(2026, 5, 24))
            )
            pacific = self._run(
                _official_next_matchups("洋盟", AsyncMock(), date(2026, 5, 24))
            )

        assert central == [
            ("ソフトバンク", "巨人"),
            ("楽天", "中日"),
            ("日本ハム", "阪神"),
        ]
        assert pacific == [
            ("西武", "ヤクルト"),
            ("ロッテ", "広島"),
            ("オリックス", "DeNA"),
        ]

    def test_sorts_matchups_by_home_team_rank(self):
        html = """
        <table>
          <tr><th>5/31（日）</th></tr>
          <tr><td><div class="team1">横浜DeNA</div><div class="team2">巨人</div></td></tr>
          <tr><td><div class="team1">中日</div><div class="team2">広島</div></td></tr>
          <tr><td><div class="team1">阪神</div><div class="team2">ヤクルト</div></td></tr>
        </table>
        """
        with patch("npb._fetch_once", new=AsyncMock(return_value=html)):
            matchups = self._run(
                _official_next_matchups("央盟", AsyncMock(), date(2026, 5, 30))
            )

        assert matchups == [
            ("広島", "中日"),
            ("ヤクルト", "阪神"),
            ("巨人", "DeNA"),
        ]

    def test_skips_reserve_days_for_next_official_matchups(self):
        html = """
        <table>
          <tr><th>6/1（月）</th></tr>
          <tr><td><div class="team1">楽天</div><span>(予備日)</span><div class="team2">ヤクルト</div></td></tr>
          <tr><td><div class="team1">ロッテ</div><span>(予備日)</span><div class="team2">阪神</div></td></tr>
          <tr><th>6/2（火）</th></tr>
          <tr><td><div class="team1">巨人</div><div class="team2">オリックス</div></td></tr>
          <tr><td><div class="team1">ヤクルト</div><div class="team2">ロッテ</div></td></tr>
          <tr><td><div class="team1">横浜DeNA</div><div class="team2">楽天</div></td></tr>
          <tr><td><div class="team1">中日</div><div class="team2">ソフトバンク</div></td></tr>
          <tr><td><div class="team1">阪神</div><div class="team2">西武</div></td></tr>
          <tr><td><div class="team1">広島</div><div class="team2">日本ハム</div></td></tr>
        </table>
        """
        with patch("npb._fetch_once", new=AsyncMock(return_value=html)):
            central = self._run(
                _official_next_matchups("央盟", AsyncMock(), date(2026, 6, 1))
            )
            pacific = self._run(
                _official_next_matchups("洋盟", AsyncMock(), date(2026, 6, 1))
            )

        assert central == [
            ("オリックス", "巨人"),
            ("ソフトバンク", "中日"),
            ("西武", "阪神"),
        ]
        assert pacific == [
            ("ロッテ", "ヤクルト"),
            ("日本ハム", "広島"),
            ("楽天", "DeNA"),
        ]


class TestNpbRecentGamesService:
    def test_fetches_recent_games_for_cross_league_matchup_teams(self):
        calls = []

        async def get_next_matchups(league, session, start_date):
            assert league == "央盟"
            return [("阪神", "オリックス")]

        async def get_last_n_game_ids(team_id, n, session):
            calls.append(team_id)
            return [f"game-{team_id}"]

        async def get_game_info(game_id, session):
            return {
                "阪 神": _make_game(
                    "2026/05/26",
                    "歐 牛",
                    "投手",
                    "甲子園",
                    1,
                    2,
                    3,
                    3,
                    4,
                    5,
                    1,
                    0,
                    0,
                ),
                "歐 牛": _make_game(
                    "2026/05/26",
                    "阪 神",
                    "投手",
                    "京大阪",
                    3,
                    3,
                    2,
                    1,
                    6,
                    4,
                    2,
                    0,
                    1,
                ),
            }

        module = Namespace(
            LEAGUE_SHEETS={"央盟": "近十場a"},
            NPB_TEAMS=NPB_TEAMS,
            GAMES_COUNT=10,
            MAX_CONCURRENT=10,
            get_next_matchups=get_next_matchups,
            get_last_n_game_ids=get_last_n_game_ids,
            get_game_info=get_game_info,
        )

        captured = {}

        def capture_update(self, sheet_name, matchups, all_games):
            captured["sheet_name"] = sheet_name
            captured["matchups"] = matchups
            captured["all_games"] = all_games

        with patch.object(
            NpbLeagueSheetService, "update_league_sheet", new=capture_update
        ):
            asyncio.run(
                NpbRecentGamesService(module=module).update(
                    AsyncMock(), matchup_start_date=date(2026, 5, 26), errors=[]
                )
            )

        assert calls == [NPB_TEAMS["阪神"]["id"], NPB_TEAMS["オリックス"]["id"]]
        assert captured["matchups"] == [("阪神", "オリックス")]
        assert set(captured["all_games"]) == {"阪神", "オリックス"}


# ---------------------------------------------------------------------------
# hex_to_rgb
# ---------------------------------------------------------------------------


class TestHexToRgb:
    def test_black(self):
        assert hex_to_rgb("000000") == {"red": 0.0, "green": 0.0, "blue": 0.0}

    def test_white(self):
        rgb = hex_to_rgb("ffffff")
        assert rgb["red"] == pytest.approx(1.0)
        assert rgb["green"] == pytest.approx(1.0)
        assert rgb["blue"] == pytest.approx(1.0)

    def test_pure_red(self):
        rgb = hex_to_rgb("ff0000")
        assert rgb["red"] == pytest.approx(1.0)
        assert rgb["green"] == 0.0
        assert rgb["blue"] == 0.0

    def test_mid_value(self):
        rgb = hex_to_rgb("80ff40")
        assert rgb["red"] == pytest.approx(0x80 / 255)
        assert rgb["green"] == pytest.approx(1.0)
        assert rgb["blue"] == pytest.approx(0x40 / 255)

    def test_with_hash_prefix(self):
        # hex_to_rgb uses lstrip("#") so it should handle both forms
        assert hex_to_rgb("#ff0000") == hex_to_rgb("ff0000")


# ---------------------------------------------------------------------------
# col_to_letter
# ---------------------------------------------------------------------------


class TestColToLetter:
    def test_single_letters(self):
        assert col_to_letter(1) == "A"
        assert col_to_letter(2) == "B"
        assert col_to_letter(26) == "Z"

    def test_double_letters(self):
        assert col_to_letter(27) == "AA"
        assert col_to_letter(28) == "AB"
        assert col_to_letter(52) == "AZ"
        assert col_to_letter(53) == "BA"

    def test_block_col_positions(self):
        # BLOCK_COLS = [2, 17, 32] → B, Q, AF
        assert col_to_letter(2) == "B"
        assert col_to_letter(17) == "Q"
        assert col_to_letter(32) == "AF"


# ---------------------------------------------------------------------------
# _pitcher_font_size
# ---------------------------------------------------------------------------


class TestPitcherFontSize:
    def test_short_name_default(self):
        assert _pitcher_font_size("田中") == 10  # 2 chars
        assert _pitcher_font_size("山本由伸") == 10  # 4 chars
        assert _pitcher_font_size("大谷翔平") == 10  # 4 chars

    def test_medium_name(self):
        assert _pitcher_font_size("バウアー") == 10  # 4 chars -> 10pt
        assert (
            _pitcher_font_size("グラスナー") == 10
        )  # 5 chars -> still 10pt (threshold is >5)
        assert _pitcher_font_size("マルティネス") == 8  # 6 chars -> 8pt

    def test_long_name(self):
        assert _pitcher_font_size("バルガスジュニア") == 6  # 8 chars -> 6pt

    def test_spaces_ignored(self):
        # "田 中" stripped = "田中" = 2 chars -> 10pt
        assert _pitcher_font_size("田 中") == 10

    def test_empty_string(self):
        assert _pitcher_font_size("") == 10


# ---------------------------------------------------------------------------
# build_block_values
# ---------------------------------------------------------------------------


def _make_game(
    date,
    opponent,
    starter,
    field,
    earned,
    runs,
    allowed,
    earned_allowed,
    hits,
    so,
    bb,
    hbp,
    hr,
    ab=None,
    sf=0,
    doubles=0,
    triples=0,
):
    game = {
        "日期": date,
        "對戰球隊": opponent,
        "對戰先發": starter,
        "球場": field,
        "実分": earned,
        "得分": runs,
        "失分": allowed,
        "実失": earned_allowed,
        "安打": hits,
        "三振": so,
        "四球": bb,
        "死球": hbp,
        "全壘打": hr,
        "長打": doubles + triples + hr,
    }
    if ab is not None:
        game["打數"] = ab
        game["犧飛"] = sf
    return game


SAMPLE_GAMES = [
    _make_game("2025/03/28", "橫 濱", "濱田", "横 浜", 3, 4, 2, 2, 8, 7, 3, 0, 1),
    _make_game("2025/03/29", "橫 濱", "田中", "横 浜", 1, 1, 5, 5, 5, 6, 2, 1, 0),
    _make_game("2025/03/30", "燕 子", "山本", "神 宮", 5, 6, 1, 1, 12, 8, 1, 0, 2),
    _make_game("2025/04/01", "燕 子", "原", "東 京", 2, 2, 3, 3, 7, 9, 4, 0, 0),
    _make_game("2025/04/02", "中 日", "小澤", "名古屋", 0, 0, 2, 2, 3, 5, 1, 0, 0),
]


def _make_schedule_game_data(game_id="game-1"):
    return {
        "賽事編號": game_id,
        "日期": "2026-06-04",
        "賽事狀態": "試合終了",
        "客隊原名": "阪神",
        "客隊": "阪 神",
        "客隊先發": "才木",
        "主隊原名": "巨人",
        "主隊": "巨 人",
        "主隊先發": "戸郷",
        "球場原名": "東京ドーム",
        "球場": "東 京",
        "主審": "山本",
        "時間": "18:00",
        "away_innings": [0, 0, 1, 1, 0, 0, 0, 0, 0, "", "", ""],
        "home_innings": [1, 1, 0, 0, 2, 0, 0, 0, "×", "", "", ""],
        "客總分": 2,
        "客總安打": 7,
        "客總失誤": 1,
        "主總分": 4,
        "主總安打": 8,
        "主總失誤": 0,
        "客先發投球": ["6", 24, 91, 0, 5, 1, 2, 0, 6, 0, 0, 3, 3],
        "客總投球": ["8", 33, 132, 0, 8, 1, 3, 0, 8, 0, 0, 4, 4],
        "主先發投球": ["6", 23, 88, 0, 6, 0, 1, 0, 5, 0, 0, 2, 2],
        "主總投球": ["9", 35, 121, 0, 7, 0, 2, 0, 7, 0, 0, 2, 2],
        "客投別": "右",
        "主投別": "右",
        "客打擊": [35, 2, 7, 2, 1, 0, 0, 2, 6, 1, 1, 0, 0, 1, 0, 1],
        "主打擊": [34, 4, 8, 4, 2, 0, 1, 3, 8, 2, 0, 0, 0, 0, 0, 0],
        "客QS": 1,
        "主QS": 1,
    }


class FakePredictionSheet:
    def __init__(self, rows=None):
        self.rows = [list(row) for row in (rows or [])]
        self.appended = []
        self.updated_cells = []

    def get_all_values(self):
        return [list(row) for row in self.rows]

    def append_row(self, row, value_input_option=None):
        self.rows.append(list(row))
        self.appended.append(list(row))

    def insert_row(self, row, index=1, value_input_option=None, **kwargs):
        self.rows.insert(index - 1, list(row))
        self.appended.append(list(row))

    def update(self, range_name=None, values=None, value_input_option=None):
        start = str(range_name or "A1").split(":")[0]
        row_num = int("".join(ch for ch in start if ch.isdigit()) or "1")
        while len(self.rows) < row_num:
            self.rows.append([])
        self.rows[row_num - 1] = list(values[0])

    def update_cell(self, row, col, value):
        while len(self.rows) < row:
            self.rows.append([])
        while len(self.rows[row - 1]) < col:
            self.rows[row - 1].append("")
        self.rows[row - 1][col - 1] = value
        self.updated_cells.append((row, col, value))


def _game_round_soup(label: str):
    return bs(
        f'<div class="bb-gameRound">{label}</div>',
        "html.parser",
    )


class TestExhibitionClassification:
    def test_parse_game_kind_reads_leading_label(self):
        assert (
            _parse_game_kind(_game_round_soup("オープン戦 1回戦 3月6日（金） 14:00 甲子園"))
            == "オープン戦"
        )
        assert (
            _parse_game_kind(_game_round_soup("セ・パ交流戦 3回戦 6月16日（火） 18:00 甲子園"))
            == "セ・パ交流戦"
        )
        assert (
            _parse_game_kind(_game_round_soup("セ・リーグ 2回戦 4月1日（水） 18:00 東京ドーム"))
            == "セ・リーグ"
        )

    def test_parse_game_kind_missing_header(self):
        assert _parse_game_kind(bs("<div></div>", "html.parser")) == ""

    def test_make_up_interleague_game_is_regular(self):
        # 2021044685: the 6/16 make-up game wrongly classified as exhibition by
        # the legacy id-prefix heuristic. The label says it is a regular game.
        data = {"賽事編號": "2021044685", "比賽種類": "セ・パ交流戦"}
        assert is_exhibition_game(data) is False
        assert is_exhibition_game_id("2021044685") is True  # legacy bug preserved

    def test_open_game_is_exhibition(self):
        data = {"賽事編號": "2021040043", "比賽種類": "オープン戦"}
        assert is_exhibition_game(data) is True

    def test_falls_back_to_id_prefix_without_label(self):
        assert is_exhibition_game({"賽事編號": "2021040043"}) is True
        assert is_exhibition_game({"賽事編號": "2026051201"}) is False


class TestNpbRowsService:
    def test_sailu_row_accepts_schedule_game_data_shape(self):
        row = NpbRowsService(module=Namespace(NPB_TEAMS=NPB_TEAMS)).sailu_row(
            0, _make_schedule_game_data("game-1")
        )

        assert row[1] == "game-1"
        assert row[2] == "阪神"
        assert row[3] == "才木"
        assert row[4] == "巨人"
        assert row[5] == "戸郷"
        assert row[7] == "東京ドーム"
        assert row[22] == 7
        assert row[36] == 4
        assert row[39] == "正常"
        assert row[41] == NPB_TEAMS["阪神"]["id"]
        assert row[42] == NPB_TEAMS["巨人"]["id"]
        assert row[45] == "6"
        assert row[47] == 3

    def test_exhibition_row_accepts_schedule_game_data_shape(self):
        data = _make_schedule_game_data("2021044685")
        service = NpbRowsService(
            module=Namespace(
                NPB_TEAMS=NPB_TEAMS,
                display_team_name=lambda name: "横浜" if name == "DeNA" else name,
            )
        )

        row = service.exhibition_row(data)
        assert row[0] == "2026/6/4"
        assert row[1] == "●"  # away (阪神) lost 2-4
        assert row[2] == "阪神"
        assert row[3] == "2"
        assert row[4] == "4"
        assert row[5] == "巨人"
        assert row[6] == "○"

        assert service.exhibition_identity(data) == ("2026-06-04", "阪神", "巨人")


class TestEffectiveDateStr:
    def test_defaults_to_now_minus_six_hours(self, monkeypatch):
        monkeypatch.delenv("NPB_STATUS_DATE", raising=False)
        expected = (datetime.now() - timedelta(hours=6)).strftime("%Y-%m-%d")
        assert NpbStatusService().effective_date_str() == expected

    def test_override_with_explicit_date(self, monkeypatch):
        monkeypatch.setenv("NPB_STATUS_DATE", "2026-06-16")
        assert NpbStatusService().effective_date_str() == "2026-06-16"

    def test_override_with_yesterday(self, monkeypatch):
        monkeypatch.setenv("NPB_STATUS_DATE", "yesterday")
        expected = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        assert NpbStatusService().effective_date_str() == expected

    def test_override_invalid_raises(self, monkeypatch):
        monkeypatch.setenv("NPB_STATUS_DATE", "not-a-date")
        with pytest.raises(ValueError):
            NpbStatusService().effective_date_str()


class TestNpbStatusService:
    def test_tracks_resolved_status_by_date(self):
        sheet = FakePredictionSheet(
            [
                ["Date", "GameId", "Status", "Resolved", "UpdatedAt"],
                ["2026-06-04", "game-1", "試合終了", "TRUE", "now"],
                ["2026-06-04", "game-2", "試合終了", "FALSE", "now"],
                ["2026-06-05", "game-3", "見どころ", "FALSE", "now"],
            ]
        )
        service = NpbStatusService(
            module=Namespace(
                NPB_STATUS_HEADERS=[
                    "Date",
                    "GameId",
                    "Status",
                    "Resolved",
                    "UpdatedAt",
                ],
                NPB_NO_GAMES_SENTINEL="__NO_GAMES__",
            )
        )

        assert not service.all_games_resolved_for_date(sheet, "2026-06-04")
        assert service.finished_unresolved_game_ids_for_date(sheet, "2026-06-04") == [
            "game-2"
        ]

    def test_upsert_updates_existing_status_record(self):
        sheet = FakePredictionSheet(
            [
                ["Date", "GameId", "Status", "Resolved", "UpdatedAt"],
                ["2026-06-04", "game-1", "見どころ", "FALSE", "old"],
            ]
        )
        service = NpbStatusService(
            module=Namespace(
                NPB_STATUS_HEADERS=[
                    "Date",
                    "GameId",
                    "Status",
                    "Resolved",
                    "UpdatedAt",
                ],
                NPB_NO_GAMES_SENTINEL="__NO_GAMES__",
            )
        )

        service.upsert(sheet, "2026-06-04", "game-1", "試合終了", True)

        assert sheet.rows[1][0:4] == ["2026-06-04", "game-1", "試合終了", "TRUE"]

    def test_no_games_sentinel_counts_as_resolved_day(self):
        sheet = FakePredictionSheet(
            [
                ["Date", "GameId", "Status", "Resolved", "UpdatedAt"],
                ["2026-06-04", "__NO_GAMES__", "無賽事", "TRUE", "now"],
            ]
        )
        service = NpbStatusService(
            module=Namespace(
                NPB_STATUS_HEADERS=[
                    "Date",
                    "GameId",
                    "Status",
                    "Resolved",
                    "UpdatedAt",
                ],
                NPB_NO_GAMES_SENTINEL="__NO_GAMES__",
            )
        )

        assert service.all_games_resolved_for_date(sheet, "2026-06-04")


class TestNpbAnalysisService:
    def test_reuses_scraped_games_without_refetching(self):
        sheet = FakePredictionSheet([["seq"], ["header"]])
        fetched = []

        class FakeModule:
            ANALYSIS_SEASON = 2026
            ANALYSIS_SHEET_NAME = "分析表紀錄"
            NPB_SPREADSHEET_KEY = "spreadsheet"
            NPB_TEAMS = NPB_TEAMS
            ANALYSIS_FIELDS = {}
            NPB_TEAM_HOME_FIELDS = {}

            @staticmethod
            def get_worksheet(sheet_name, spreadsheet_key):
                return sheet

            @staticmethod
            def display_team_name(team_name):
                return NPB_TEAMS[team_name]["name"]

            @staticmethod
            def _date_key(target_date=None):
                return "2026-06-04"

            @staticmethod
            async def get_schedule_game_data(gid, session, retry=True):
                fetched.append((gid, retry))
                return None

        inserted = asyncio.run(
            NpbAnalysisService(module=FakeModule).update(
                AsyncMock(),
                game_ids=["game-1"],
                scraped_games=[("game-1", _make_schedule_game_data("game-1"))],
            )
        )

        assert inserted == 1
        assert fetched == []
        assert sheet.rows[2][1] == "2026/6/4"


def _balance_after_formula(row_num):
    return (
        f'=IF(J{row_num}="win",K{row_num}+H{row_num}*G{row_num},'
        f'IF(J{row_num}="loss",K{row_num}-H{row_num},'
        f'IF(OR(J{row_num}="push",J{row_num}="void",'
        f'I{row_num}="pending",J{row_num}=""),K{row_num},K{row_num})))'
    )


# ---------------------------------------------------------------------------
# NPB prediction ledger
# ---------------------------------------------------------------------------


class TestPredictionLedger:
    def test_build_prediction_text_summarizes_pick_without_hash_fields(self):
        result = build_prediction_text(
            "2021038658",
            "巨人",
            0.92,
            10,
        )

        assert result == (
            "NPB prediction\n"
            "Game 2021038658\n"
            "Market: final_winner\n"
            "Pick: 巨人\n"
            "Rate: 0.92\n"
            "Stake: 10.0"
        )
        assert "SHA-256" not in result
        assert "salt" not in result

    def test_balance_math_matches_stake_rate_example(self):
        assert calculate_prediction_balance(0, 10, 0.92, "win") == 9.2
        assert calculate_prediction_balance(0, 10, 0.92, "loss") == -10
        assert calculate_prediction_balance(0, 10, 0.92, "push") == 0

    def test_prediction_outcome_compares_pick_to_final_winner(self):
        data = {
            "賽事狀態": "試合終了",
            "客隊原名": "阪神",
            "客隊": "阪 神",
            "主隊原名": "巨人",
            "主隊": "巨 人",
            "客總分": 2,
            "主總分": 4,
            "away_innings": [0, 0, 1, 1, 0, "", "", "", "", "", "", ""],
            "home_innings": [1, 1, 0, 0, 2, "", "", "", "", "", "", ""],
        }

        assert prediction_outcome_for_game(data, "巨 人") == "win"
        assert prediction_outcome_for_game(data, "阪神") == "loss"
        assert prediction_outcome_for_game(data, "巨人", market="half_winner") == "win"
        assert (
            prediction_outcome_for_game(data, "over", market="half_total", line=5.5)
            == "win"
        )
        assert (
            prediction_outcome_for_game(data, "under", market="final_total", line=6.5)
            == "win"
        )

    def test_prediction_outcome_handicap_final(self):
        # 客 2, 主 4
        data = {
            "賽事狀態": "試合終了",
            "客隊原名": "阪神",
            "客隊": "阪神",
            "主隊原名": "巨人",
            "主隊": "巨人",
            "客總分": 2,
            "主總分": 4,
            "away_innings": [0, 0, 1, 1, 0, "", "", "", "", "", "", ""],
            "home_innings": [1, 1, 0, 0, 2, "", "", "", "", "", "", ""],
        }
        # 主隊 +0.5: 4+0.5=4.5 > 2 → win
        assert (
            prediction_outcome_for_game(data, "巨人", market="final_handicap", line=0.5)
            == "win"
        )
        # 客隊 +0.5: 2+0.5=2.5 < 4 → loss
        assert (
            prediction_outcome_for_game(data, "阪神", market="final_handicap", line=0.5)
            == "loss"
        )
        # 主隊 -0.5: 4-0.5=3.5 > 2 → win
        assert (
            prediction_outcome_for_game(
                data, "巨人", market="final_handicap", line=-0.5
            )
            == "win"
        )
        # 主隊 -3.5: 4-3.5=0.5 < 2 → loss
        assert (
            prediction_outcome_for_game(
                data, "巨人", market="final_handicap", line=-3.5
            )
            == "loss"
        )
        # push: 客隊 +2: 2+2=4 == 4 → push
        assert (
            prediction_outcome_for_game(data, "阪神", market="final_handicap", line=2)
            == "push"
        )

    def test_prediction_outcome_handicap_half(self):
        # 前5局: 客 0+0+1+1+0=2, 主 1+1+0+0+2=4
        data = {
            "賽事狀態": "試合終了",
            "客隊原名": "阪神",
            "客隊": "阪神",
            "主隊原名": "巨人",
            "主隊": "巨人",
            "客總分": 2,
            "主總分": 4,
            "away_innings": [0, 0, 1, 1, 0, "", "", "", "", "", "", ""],
            "home_innings": [1, 1, 0, 0, 2, "", "", "", "", "", "", ""],
        }
        # 主隊 +0.5: 4+0.5=4.5 > 2 → win
        assert (
            prediction_outcome_for_game(data, "巨人", market="half_handicap", line=0.5)
            == "win"
        )
        # 客隊 +0.5: 2+0.5=2.5 < 4 → loss
        assert (
            prediction_outcome_for_game(data, "阪神", market="half_handicap", line=0.5)
            == "loss"
        )

    def test_prediction_outcome_handicap_requires_line(self):
        data = {
            "客隊原名": "阪神",
            "客隊": "阪神",
            "主隊原名": "巨人",
            "主隊": "巨人",
            "客總分": 2,
            "主總分": 4,
            "away_innings": [],
            "home_innings": [],
        }
        with pytest.raises(ValueError, match="line"):
            prediction_outcome_for_game(data, "巨人", market="final_handicap")
        with pytest.raises(ValueError, match="line"):
            prediction_outcome_for_game(data, "巨人", market="half_handicap")

    def test_create_prediction_records_pending_row_without_external_posts(self):
        sheet = FakePredictionSheet([])

        result = create_npb_prediction(
            "2021038658",
            "巨人",
            0.92,
            stake=10,
            game_date="2026-05-11",
            away_team="阪神",
            home_team="巨人",
            sheet=sheet,
            post=False,
        )

        assert len(sheet.rows) == 2
        headers = sheet.rows[0]
        row = sheet.rows[1]
        assert row[headers.index("game_id")] == "2021038658"
        assert row[headers.index("status")] == "pending"
        assert row[headers.index("balance_before")] == 0.0
        assert row[headers.index("balance_after")] == _balance_after_formula(2)
        assert "SHA-256" not in result["prediction_text"]

    def test_create_prediction_inserts_missing_header_above_existing_rows(self):
        sheet = FakePredictionSheet(
            [
                [
                    "p1",
                    "2021038658",
                    "2026-05-11",
                    "final_winner",
                    "巨人",
                    "",
                    "0.92",
                    "10",
                    "pending",
                    "",
                    "0",
                    "0",
                    "2026-05-11T12:00:00",
                    "",
                ],
            ]
        )

        create_npb_prediction(
            "2021038659",
            "阪神",
            0.92,
            stake=10,
            sheet=sheet,
            post=False,
        )

        headers = sheet.rows[0]
        assert headers == PREDICTION_HEADERS
        assert sheet.rows[1][0] == "p1"

    def test_prediction_cli_prompts_for_missing_values(self, monkeypatch):
        answers = iter(
            [
                "巨人",
                "3",
                "under",
                "8.5",
                "0.92",
                "",
            ]
        )
        monkeypatch.setattr("builtins.input", lambda _: next(answers))
        args = Namespace(
            create_prediction=PREDICTION_PROMPT_SENTINEL,
            market="final_winner",
            pick=None,
            line=None,
            rate=None,
            stake=None,
            game_date="",
            away_team="",
            home_team="",
        )

        values = _prediction_cli_values(args)

        assert values == {
            "home_team_lookup": "巨人",
            "game_id": "",
            "market": "final_total",
            "pick": "under",
            "line": 8.5,
            "rate": 0.92,
            "stake": 10.0,
            "game_date": "",
            "away_team": "",
            "home_team": "",
        }

    def test_prediction_cli_reprompts_invalid_pick(self, monkeypatch):
        answers = iter(
            [
                "巨人",
                "1",
                "not-a-team",
                "巨人",
                "0.92",
                "",
            ]
        )
        monkeypatch.setattr("builtins.input", lambda _: next(answers))
        args = Namespace(
            create_prediction=PREDICTION_PROMPT_SENTINEL,
            market="final_winner",
            pick=None,
            line=None,
            rate=None,
            stake=None,
            game_date="",
            away_team="",
            home_team="",
        )

        values = _prediction_cli_values(args)

        assert values["pick"] == "巨人"

    def test_prediction_cli_reprompts_invalid_home_team(self, monkeypatch, capsys):
        answers = iter(
            [
                "not-a-team",
                "巨人",
                "1",
                "巨人",
                "0.92",
                "",
            ]
        )
        monkeypatch.setattr("builtins.input", lambda _: next(answers))
        args = Namespace(
            create_prediction=PREDICTION_PROMPT_SENTINEL,
            market="final_winner",
            pick=None,
            line=None,
            rate=None,
            stake=None,
            game_date="",
            away_team="",
            home_team="",
        )

        values = _prediction_cli_values(args)

        output = capsys.readouterr().out
        assert "Home team options:" in output
        assert "Home team must be one of:" in output
        assert values["home_team_lookup"] == "巨人"

    def test_resolve_prediction_updates_outcome_and_balance(self):
        headers = PREDICTION_HEADERS
        sheet = FakePredictionSheet(
            [
                headers,
                [
                    "p1",
                    "2021038658",
                    "2026-05-11",
                    "final_winner",
                    "巨人",
                    "",
                    "0.92",
                    "10",
                    "pending",
                    "",
                    "0",
                    "0",
                    "2026-05-11T12:00:00",
                    "",
                ],
            ]
        )
        data = {
            "賽事狀態": "試合終了",
            "客隊原名": "阪神",
            "客隊": "阪 神",
            "主隊原名": "巨人",
            "主隊": "巨 人",
            "客總分": 2,
            "主總分": 4,
            "away_innings": [0, 0, 1, 1, 0, "", "", "", "", "", "", ""],
            "home_innings": [1, 1, 0, 0, 2, "", "", "", "", "", "", ""],
        }

        assert (
            resolve_npb_predictions_for_game(
                "2021038658", data, sheet=sheet, post=False
            )
            == 1
        )

        row = sheet.rows[1]
        assert row[headers.index("status")] == "resolved"
        assert row[headers.index("outcome")] == "win"
        assert row[headers.index("balance_before")] == 0.0
        assert row[headers.index("balance_after")] == _balance_after_formula(2)

    def test_resolve_prediction_keeps_in_progress_game_pending(self):
        headers = PREDICTION_HEADERS
        sheet = FakePredictionSheet(
            [
                headers,
                [
                    "p1",
                    "2021038658",
                    "2026-05-17",
                    "final_winner",
                    "巨人",
                    "",
                    "0.92",
                    "10",
                    "pending",
                    "",
                    "0",
                    "0",
                    "2026-05-17T12:00:00",
                    "",
                ],
            ]
        )
        data = {
            "賽事狀態": "5回表",
            "客隊原名": "阪神",
            "客隊": "阪 神",
            "主隊原名": "巨人",
            "主隊": "巨 人",
            "客總分": 2,
            "主總分": 4,
        }

        assert (
            resolve_npb_predictions_for_game(
                "2021038658", data, sheet=sheet, post=False
            )
            == 0
        )

        row = sheet.rows[1]
        assert row[headers.index("status")] == "pending"
        assert row[headers.index("outcome")] == ""
        assert row[headers.index("resolved_at")] == ""

    def test_create_prediction_balance_formula_references_previous_row(self):
        headers = PREDICTION_HEADERS
        sheet = FakePredictionSheet(
            [
                headers,
                [
                    "p1",
                    "2021038658",
                    "2026-05-11",
                    "final_winner",
                    "巨人",
                    "",
                    "0.92",
                    "10",
                    "resolved",
                    "win",
                    "0",
                    "9.2",
                    "2026-05-11T12:00:00",
                    "2026-05-11T15:00:00",
                ],
            ]
        )

        create_npb_prediction(
            "2021038659",
            "阪神",
            0.92,
            stake=10,
            sheet=sheet,
            post=False,
        )

        row = sheet.rows[2]
        assert row[headers.index("balance_before")] == "=L2"
        assert row[headers.index("balance_after")] == _balance_after_formula(3)

    def test_resolve_prediction_backfills_empty_game_date(self):
        headers = PREDICTION_HEADERS
        sheet = FakePredictionSheet(
            [
                headers,
                [
                    "p1",
                    "2021038658",
                    "",
                    "final_winner",
                    "巨人",
                    "",
                    "0.92",
                    "10",
                    "pending",
                    "",
                    "0",
                    "0",
                    "2026-05-11T12:00:00",
                    "",
                ],
            ]
        )
        data = {
            "日期": "2026-05-11",
            "賽事狀態": "試合終了",
            "客隊原名": "阪神",
            "客隊": "阪 神",
            "主隊原名": "巨人",
            "主隊": "巨 人",
            "客總分": 2,
            "主總分": 4,
            "away_innings": [0, 0, 1, 1, 0, "", "", "", "", "", "", ""],
            "home_innings": [1, 1, 0, 0, 2, "", "", "", "", "", "", ""],
        }

        resolve_npb_predictions_for_game("2021038658", data, sheet=sheet, post=False)

        row = sheet.rows[1]
        assert row[headers.index("game_date")] == "2026-05-11"

    def test_reveal_predictions_scans_pending_ids_when_no_game_ids_provided(self):
        headers = PREDICTION_HEADERS
        sheet = FakePredictionSheet(
            [
                headers,
                [
                    "p1",
                    "pending-game",
                    "",
                    "final_winner",
                    "巨人",
                    "",
                    "0.92",
                    "10",
                    "pending",
                    "",
                    "0",
                    "0",
                    "2026-05-11T12:00:00",
                    "",
                ],
            ]
        )
        fetched = []

        class FakeModule:
            @staticmethod
            def _prediction_sheet():
                return sheet

            @staticmethod
            def _prediction_rows(prediction_sheet):
                return prediction_sheet.get_all_values()

            @staticmethod
            async def get_schedule_game_data(gid, _session, retry=False):
                fetched.append((gid, retry))
                return None

        resolved = asyncio.run(
            NpbPredictionService(module=FakeModule).reveal_predictions_for_games(
                AsyncMock(), []
            )
        )

        assert resolved == 0
        assert fetched == [("pending-game", False)]

    def test_reveal_predictions_skips_unfinished_schedule_status(self):
        headers = PREDICTION_HEADERS
        sheet = FakePredictionSheet(
            [
                headers,
                [
                    "p1",
                    "live-game",
                    "2026-05-17",
                    "final_winner",
                    "巨人",
                    "",
                    "0.92",
                    "10",
                    "pending",
                    "",
                    "0",
                    "0",
                    "2026-05-17T12:00:00",
                    "",
                ],
            ]
        )

        class FakeModule:
            @staticmethod
            def _prediction_sheet():
                return sheet

            @staticmethod
            def _prediction_rows(prediction_sheet):
                return prediction_sheet.get_all_values()

            @staticmethod
            async def get_schedule_game_data(gid, _session, retry=False):
                return {
                    "賽事編號": gid,
                    "賽事狀態": "5回表",
                    "客隊": "阪 神",
                    "主隊": "巨 人",
                    "客總分": 2,
                    "主總分": 4,
                }

        resolved = asyncio.run(
            NpbPredictionService(module=FakeModule).reveal_predictions_for_games(
                AsyncMock(), []
            )
        )

        assert resolved == 0
        assert sheet.rows[1][headers.index("status")] == "pending"
        assert sheet.rows[1][headers.index("resolved_at")] == ""

    def test_reveal_predictions_keeps_unknown_schedule_status_pending(self):
        headers = PREDICTION_HEADERS
        sheet = FakePredictionSheet(
            [
                headers,
                [
                    "p1",
                    "finished-game",
                    "2026-05-17",
                    "final_winner",
                    "巨人",
                    "",
                    "0.92",
                    "10",
                    "pending",
                    "",
                    "0",
                    "0",
                    "2026-05-17T12:00:00",
                    "",
                ],
            ]
        )

        class FakeModule:
            PREDICTION_DEFAULT_STAKE = 10.0
            PREDICTION_STARTING_BALANCE = 0.0

            @staticmethod
            def col_to_letter(col_num):
                return col_to_letter(col_num)

            @staticmethod
            def display_team_name(team_name):
                return NPB_TEAMS[team_name]["name"]

            @staticmethod
            def _prediction_sheet():
                return sheet

            @staticmethod
            def _prediction_rows(prediction_sheet):
                return prediction_sheet.get_all_values()

            @staticmethod
            async def get_schedule_game_data(gid, _session, retry=False):
                return {
                    "賽事編號": gid,
                    "賽事狀態": "",
                    "客隊原名": "阪神",
                    "客隊": "阪 神",
                    "主隊原名": "巨人",
                    "主隊": "巨 人",
                    "客總分": 2,
                    "主總分": 4,
                }

        FakeModule.NPB_TEAMS = NPB_TEAMS
        FakeModule.PREDICTION_MARKET_ALIASES = {
            "final_winner": "final_winner",
        }

        resolved = asyncio.run(
            NpbPredictionService(module=FakeModule).reveal_predictions_for_games(
                AsyncMock(), []
            )
        )

        assert resolved == 0
        assert sheet.rows[1][headers.index("status")] == "pending"
        assert sheet.rows[1][headers.index("outcome")] == ""


class TestScheduleStatusForGame:
    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_session(self):
        return AsyncMock()

    def test_finds_status_when_game_link_is_not_on_status_element(self):
        html = _cal_html(
            {
                "day": "20",
                "status": "試合終了",
                "links": ["/npb/game/2021038881/top"],
            }
        )
        with patch("npb._fetch_once", new=AsyncMock(return_value=html)):
            result = self._run(
                _schedule_status_for_game(
                    "2021038881", "2026-05-20", self._mock_session()
                )
            )

        assert result == "試合終了"


class TestBuildBlockValues:
    def test_returns_13_rows(self):
        rows = build_block_values("巨人", SAMPLE_GAMES)
        assert len(rows) == 13

    def test_each_row_has_14_cols(self):
        rows = build_block_values("巨人", SAMPLE_GAMES)
        for row in rows:
            assert len(row) == 14

    def test_header_row_team_name(self):
        rows = build_block_values("巨人", SAMPLE_GAMES)
        assert rows[0][0] == "巨 人"

    def test_games_sorted_oldest_first(self):
        # Oldest game is 2025/03/28 → should appear in row 1 (index 1)
        rows = build_block_values("巨人", SAMPLE_GAMES)
        assert "3/28" in rows[1][0] or "28" in str(rows[1][0])

    def test_empty_rows_when_fewer_than_10_games(self):
        rows = build_block_values("巨人", SAMPLE_GAMES)
        # 5 games → rows 6-10 (index 6-10) should be all empty strings
        for i in range(len(SAMPLE_GAMES) + 1, GAMES_COUNT + 1):
            assert rows[i] == [""] * 14

    def test_bb_plus_hbp_combined(self):
        # Game at index 1 (2025/03/28): bb=3, hbp=0 → col index 10 = 3
        rows = build_block_values("巨人", SAMPLE_GAMES)
        assert rows[1][10] == 3  # bb+hbp

    def test_header_uses_long_hits_label(self):
        rows = build_block_values("巨人", SAMPLE_GAMES)
        assert rows[0][11] == "長 打"

    def test_long_hits_column_sums_doubles_triples_homers(self):
        # 2 doubles + 1 triple + 3 HR → col index 11 = 6
        game = _make_game(
            "2025/04/01",
            "燕 子",
            "投手",
            "東 京",
            0,
            4,
            3,
            0,
            9,
            0,
            0,
            0,
            3,
            ab=30,
            doubles=2,
            triples=1,
        )
        rows = build_block_values("巨人", [game])
        assert rows[1][11] == 6

    def test_game_rows_include_avg_and_obp(self):
        game = _make_game(
            "2025/04/01",
            "燕 子",
            "投手",
            "東 京",
            0,
            4,
            3,
            0,
            9,
            0,
            3,
            1,
            1,
            ab=30,
            sf=1,
        )
        rows = build_block_values("巨人", [game])
        assert rows[1][12] == ".300"
        assert rows[1][13] == ".371"

    def test_rate_values_keep_three_decimal_places(self):
        game = _make_game(
            "2025/04/01",
            "燕 子",
            "投手",
            "東 京",
            0,
            4,
            3,
            0,
            4,
            0,
            0,
            0,
            0,
            ab=20,
        )
        rows = build_block_values("巨人", [game])
        assert rows[1][12] == ".200"
        assert rows[1][13] == ".200"

    def test_avg_rows_use_aggregate_avg_and_obp(self):
        games = [
            _make_game(
                "2025/04/01",
                "燕 子",
                "投手",
                "東 京",
                0,
                4,
                3,
                0,
                9,
                0,
                3,
                1,
                1,
                ab=30,
                sf=1,
            ),
            _make_game(
                "2025/04/02",
                "燕 子",
                "投手",
                "東 京",
                0,
                2,
                3,
                0,
                6,
                0,
                2,
                0,
                0,
                ab=30,
                sf=0,
            ),
        ]
        rows = build_block_values("巨人", games)
        assert rows[11][12] == ".250"
        assert rows[11][13] == ".313"

    def test_home_run_rows_format_date_and_pitcher(self):
        service = NpbLeagueSheetService(module=npb)
        game = _make_game(
            "2026/06/21",
            "中 日",
            "柳",
            "東 京",
            0,
            2,
            0,
            0,
            6,
            0,
            0,
            0,
            1,
        )
        game["全壘打明細"] = [
            {
                "日期": "2026/06/21",
                "打者": "泉口 友汰",
                "左右打": "左打",
                "方向": "右本",
                "投手": "柳",
                "對戰球隊": "中 日",
            }
        ]

        rows = service.recent_home_run_rows("巨人", [game])

        assert rows[0][:8] == [
            "巨 人",
            "日 期",
            "打 者",
            "左 右",
            "方 向",
            "投 手",
            "",
            "對 戰",
        ]
        assert rows[1][:8] == [
            "",
            "6/21",
            "泉口 友汰",
            "左",
            "右本",
            "柳",
            "",
            "中 日",
        ]

    def test_home_run_rows_shorten_switch_hitter_side(self):
        service = NpbLeagueSheetService(module=npb)
        game = _make_game(
            "2026/06/21",
            "中 日",
            "柳",
            "東 京",
            0,
            2,
            0,
            0,
            6,
            0,
            0,
            0,
            1,
        )
        game["全壘打明細"] = [
            {
                "日期": "2026/06/21",
                "打者": "ヒュンメル",
                "左右打": "兩打",
                "方向": "右本",
                "投手": "柳",
                "對戰球隊": "中 日",
            }
        ]

        rows = service.recent_home_run_rows("巨人", [game])

        assert rows[1][3] == "両"

    def test_two_character_local_field_gets_spaced(self):
        games = [
            _make_game(
                "2025/04/03",
                "燕 子",
                "山本",
                "長野",
                0,
                1,
                2,
                2,
                5,
                6,
                1,
                0,
                0,
            )
        ]
        rows = build_block_values("巨人", games)
        assert rows[1][3] == "長 野"

    def test_avg10_row(self):
        # Row index 11 = 近十場 average (only 5 games available)
        rows = build_block_values("巨人", SAMPLE_GAMES)
        avg_row = rows[11]
        assert avg_row[2] == "近十場"
        assert avg_row[3] == "平 均"
        n = len(SAMPLE_GAMES)
        expected_runs = round(sum(g["得分"] for g in SAMPLE_GAMES) / n, 1)
        assert avg_row[5] == expected_runs

    def test_avg5_row(self):
        rows = build_block_values("巨人", SAMPLE_GAMES)
        avg_row = rows[12]
        assert avg_row[2] == "近五場"
        # With only 5 games, 近五場 == 近十場
        assert avg_row[5] == rows[11][5]

    def test_avg5_uses_last_5_when_more_than_5_games(self):
        games_10 = [
            _make_game(
                f"2025/0{3 if i < 9 else 4}/{i+20 if i < 9 else i-8:02d}",
                "燕 子",
                "投手",
                "東 京",
                i,
                i,
                0,
                0,
                i,
                0,
                0,
                0,
                0,
            )
            for i in range(10)
        ]
        rows = build_block_values("巨人", games_10)
        avg10 = rows[11]
        avg5 = rows[12]
        # The last 5 games have 得分 = 5,6,7,8,9 → avg = 7.0
        assert avg5[5] == 7.0
        # All 10 games have 得分 = 0..9 → avg = 4.5
        assert avg10[5] == 4.5

    def test_no_games_avg_rows_empty(self):
        rows = build_block_values("巨人", [])
        assert rows[11] == ["", "", "近十場", "平 均"] + [""] * 10
        assert rows[12] == ["", "", "近五場", "平 均"] + [""] * 10

    def test_more_than_10_games_keeps_last_10(self):
        games_12 = [
            _make_game(
                f"2025/0{3 if i < 9 else 4}/{i+20 if i < 9 else i-8:02d}",
                "燕 子",
                "投手",
                "東 京",
                0,
                i,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            )
            for i in range(12)
        ]
        rows = build_block_values("巨人", games_12)
        assert len(rows) == 13  # still 13 rows
        # Only 10 game rows should be non-empty (rows 1-10)
        non_empty = sum(1 for r in rows[1:11] if r != [""] * 14)
        assert non_empty == 10


# ---------------------------------------------------------------------------
# _analysis_row
# ---------------------------------------------------------------------------


class TestAnalysisRow:
    def _data(
        self,
        home_raw="巨人",
        home_display="巨 人",
        venue_raw="東京ドーム",
        venue_display="東 京",
    ):
        return {
            "日期": "2026-03-27",
            "時間": "18:00",
            "主審": "市川貴",
            "客投別": "右",
            "主投別": "左",
            "客隊原名": "阪神",
            "客隊": "阪 神",
            "主隊原名": home_raw,
            "主隊": home_display,
            "球場原名": venue_raw,
            "球場": venue_display,
            "客總分": 1,
            "主總分": 3,
            "客總失誤": 2,
            "主總失誤": 4,
            "away_innings": [0, 0, 0, 1, 0, 0, 0, 0, 0, "", "", ""],
            "home_innings": [2, 0, 0, 1, 0, 0, 0, 0, "×", "", "", ""],
            "客先發投球": ["6", 22, 101, 0, 3, 0, 2, 1, 6, 0, 0, 3, 2],
            "客總投球": ["8", 31, 140, 0, 7, 1, 4, 1, 9, 0, 0, 3, 2],
            "主先發投球": ["7", 24, 95, 0, 4, 1, 0, 0, 8, 0, 0, 1, 1],
            "主總投球": ["9", 33, 128, 0, 5, 1, 1, 0, 10, 0, 0, 1, 1],
            "客打擊": [33, 1, 5, 1, 1, 0, 1, 0, 1, 0, 10, 0, 0, 0, 0, 2],
            "主打擊": [31, 3, 7, 3, 2, 0, 1, 0, 4, 1, 9, 0, 0, 1, 0, 4],
        }

    def test_pitching_blocks_use_same_side_pitchers(self):
        data = self._data()

        row = _analysis_row(1, data)

        assert row[34:43] == ["6", 22, 3, 0, 3, 3, 2, 3, "QS"]
        assert row[43:58] == ["8", 140, 31, 7, 1, 9, 5, 3, 2, 2, 0, 0, 0, 9, 2]
        assert row[59:68] == ["7", 24, 4, 1, 0, 1, 1, 7, "QS"]
        assert row[68:83] == ["9", 128, 33, 5, 1, 10, 1, 1, 1, 4, 0, 1, 0, 12, 3]

    def test_analysis_field_uses_home_team_primary_stadium(self):
        row = _analysis_row(
            1,
            self._data(
                home_raw="オリックス",
                home_display="歐 牛",
                venue_raw="京セラD大阪",
                venue_display="京大阪",
            ),
        )

        assert row[13] == "京セラドーム"

    def test_analysis_field_uses_orix_kobe_home_stadium(self):
        row = _analysis_row(
            1,
            self._data(
                home_raw="オリックス",
                home_display="歐 牛",
                venue_raw="ほっともっとフィールド神戸",
                venue_display="ほっともっとフィールド神戸",
            ),
        )

        assert row[13] == "スカイマーク"

    def test_analysis_field_marks_non_primary_home_stadium_as_local(self):
        row = _analysis_row(
            1,
            self._data(
                home_raw="阪神",
                home_display="阪 神",
                venue_raw="京セラD大阪",
                venue_display="京大阪",
            ),
        )

        assert row[13] == "地方球場"

    def test_analysis_game_type_uses_home_league_for_same_league_game(self):
        row = _analysis_row(1, self._data())

        assert row[3] == "央盟"

    def test_analysis_game_type_marks_cross_league_game_as_interleague(self):
        row = _analysis_row(
            1,
            self._data(
                home_raw="オリックス",
                home_display="歐 牛",
                venue_raw="京セラD大阪",
                venue_display="京大阪",
            ),
        )

        assert row[3] == "交流戰"


class TestAnalysisLeagueRepairHelpers:
    def test_analysis_team_league_accepts_raw_and_sheet_names(self):
        assert _analysis_team_league("巨人") == "央盟"
        assert _analysis_team_league("巨 人") == "央盟"
        assert _analysis_team_league("横浜") == "央盟"
        assert _analysis_team_league("歐 牛") == "洋盟"

    def test_analysis_game_type_from_existing_row_teams(self):
        assert _analysis_game_type_from_teams("阪神", "巨人") == "央盟"
        assert _analysis_game_type_from_teams("楽天", "歐 牛") == "洋盟"
        assert _analysis_game_type_from_teams("阪神", "歐 牛") == "交流戰"


# ---------------------------------------------------------------------------
# _parse_batting_table
# ---------------------------------------------------------------------------


class TestParseBattingTable:
    def test_yahoo_current_total_row_and_event_counts(self):
        html = """
        <table class="bb-statsTable">
          <tr>
            <td class="bb-statsTable__data--inning">右2</td>
            <td class="bb-statsTable__data--inning">遊併打</td>
            <td class="bb-statsTable__data--inning">中3</td>
            <td class="bb-statsTable__data--inning">右犠飛</td>
          </tr>
          <tr>
            <th class="bb-statsTable__head--result">合計</th>
            <td class="bb-statsTable__data--result"></td>
            <td class="bb-statsTable__data--result">32</td>
            <td class="bb-statsTable__data--result">7</td>
            <td class="bb-statsTable__data--result">9</td>
            <td class="bb-statsTable__data--result">7</td>
            <td class="bb-statsTable__data--result">10</td>
            <td class="bb-statsTable__data--result">6</td>
            <td class="bb-statsTable__data--result">0</td>
            <td class="bb-statsTable__data--result">1</td>
            <td class="bb-statsTable__data--result">1</td>
            <td class="bb-statsTable__data--result">2</td>
            <td class="bb-statsTable__data--result">3</td>
            <td class="bb-statsTable__data--result"></td>
          </tr>
        </table>
        """

        stats = _parse_batting_table(bs(html, "html.parser").find("table"))

        assert stats == [32, 7, 9, 7, 1, 1, 3, 1, 6, 0, 10, 1, 1, 1, 0, 2]


class TestParseHomeRunEvents:
    def test_parses_homer_batter_player_id_and_direction(self):
        html = """
        <table class="bb-statsTable">
          <tr class="bb-statsTable__row">
            <td class="bb-statsTable__data bb-statsTable__data--bat">(遊)</td>
            <td class="bb-statsTable__data bb-statsTable__data--player">
              <a href="/npb/player/1750321/top">泉口 友汰</a>
            </td>
            <td class="bb-statsTable__data bb-statsTable__data--inning">右本</td>
          </tr>
        </table>
        """

        events = _parse_home_run_events(bs(html, "html.parser").find("table"))

        assert events == [
            {
                "打者": "泉口 友汰",
                "player_id": "1750321",
                "左右打": "",
                "方向": "右本",
            }
        ]

    def test_parses_center_gap_home_run_directions(self):
        html = """
        <table class="bb-statsTable">
          <tr class="bb-statsTable__row">
            <td class="bb-statsTable__data bb-statsTable__data--bat">(遊)</td>
            <td class="bb-statsTable__data bb-statsTable__data--player">
              <a href="/npb/player/1750321/top">泉口 友汰</a>
            </td>
            <td class="bb-statsTable__data bb-statsTable__data--inning">右中本</td>
            <td class="bb-statsTable__data bb-statsTable__data--inning">左中本</td>
          </tr>
        </table>
        """

        events = _parse_home_run_events(bs(html, "html.parser").find("table"))

        assert [event["方向"] for event in events] == ["右中本", "左中本"]

    def test_parses_player_bat_hand_from_profile(self):
        html = """
        <dl>
          <dt>投打</dt>
          <dd>右投左打</dd>
        </dl>
        """

        assert _parse_player_bat_hand(html) == "左打"

    def test_parses_homer_pitcher_from_text_page(self):
        html = """
        <section class="bb-liveText">
          <header class="bb-liveText__head">
            <h1 class="bb-liveText__inning">6回表</h1>
            <p class="bb-liveText__detail">中日の攻撃</p>
          </header>
          <ol>
            <li class="bb-liveText__item">
              <div class="bb-liveText__text">
                <p class="bb-liveText__batter">
                  <a class="bb-liveText__player" href="/npb/player/1600085/top">細川 成也</a>
                </p>
                <p class="bb-liveText__summary">
                  投手交代: 井上 → 船迫
                </p>
                <p class="bb-liveText__summary bb-liveText__summary--point">
                  レフトスタンドへのホームラン
                </p>
              </div>
            </li>
          </ol>
        </section>
        """

        events = _parse_home_run_pitcher_events(
            html,
            away_raw="中日",
            home_raw="巨人",
            away_starter="柳",
            home_starter="井上",
        )

        assert events["away"] == [
            {"打者": "細川 成也", "player_id": "1600085", "投手": "船迫"}
        ]
        assert events["home"] == []

    def test_parses_between_innings_pitcher_change_before_homer(self):
        html = """
        <section class="bb-liveText">
          <header class="bb-liveText__head">
            <h1 class="bb-liveText__inning">9回裏</h1>
            <p class="bb-liveText__detail">中日の攻撃</p>
          </header>
          <ol>
            <li class="bb-liveText__item">
              <div class="bb-liveText__text">
                <p class="bb-liveText__batter">
                  <a class="bb-liveText__player" href="/npb/player/1561854/top">サノー</a>
                </p>
                <p class="bb-liveText__summary bb-liveText__summary--change">
                  ピッチャー レイノルズ に代わって 山﨑 がマウンドにあがる
                </p>
                <p class="bb-liveText__summary bb-liveText__summary--point">
                  右中間へのホームラン 中 4-6 デ
                </p>
              </div>
            </li>
          </ol>
        </section>
        """

        events = _parse_home_run_pitcher_events(
            html,
            away_raw="DeNA",
            home_raw="中日",
            away_starter="東",
            home_starter="櫻井",
        )

        assert events["home"] == [
            {"打者": "サノー", "player_id": "1561854", "投手": "山﨑"}
        ]

    def test_expands_short_relief_pitcher_names_from_score_table(self):
        stats_html = """
        <table class="bb-scoreTable">
          <tr class="bb-scoreTable__row">
            <td class="bb-scoreTable__data--player">東 克樹</td>
          </tr>
          <tr class="bb-scoreTable__row">
            <td class="bb-scoreTable__data--player">中川 虎大</td>
          </tr>
          <tr class="bb-scoreTable__row">
            <td class="bb-scoreTable__data--player">山﨑 康晃</td>
          </tr>
        </table>
        """
        text_html = """
        <section class="bb-liveText">
          <header class="bb-liveText__head">
            <h1 class="bb-liveText__inning">9回裏</h1>
            <p class="bb-liveText__detail">中日の攻撃</p>
          </header>
          <ol>
            <li class="bb-liveText__item">
              <div class="bb-liveText__text">
                <p class="bb-liveText__batter">
                  <a class="bb-liveText__player" href="/npb/player/1561854/top">サノー</a>
                </p>
                <p class="bb-liveText__summary bb-liveText__summary--change">
                  ピッチャー 東 に代わって 中川虎 がマウンドにあがる
                </p>
                <p class="bb-liveText__summary bb-liveText__summary--point">
                  右中間へのホームラン 中 3-6 デ
                </p>
              </div>
            </li>
            <li class="bb-liveText__item">
              <div class="bb-liveText__text">
                <p class="bb-liveText__batter">
                  <a class="bb-liveText__player" href="/npb/player/1561854/top">サノー</a>
                </p>
                <p class="bb-liveText__summary bb-liveText__summary--change">
                  ピッチャー 中川虎 に代わって 山﨑 がマウンドにあがる
                </p>
                <p class="bb-liveText__summary bb-liveText__summary--point">
                  ライトスタンドへのホームラン 中 4-6 デ
                </p>
              </div>
            </li>
          </ol>
        </section>
        """
        lookup = _parse_pitcher_name_lookup(bs(stats_html, "html.parser"))

        events = _parse_home_run_pitcher_events(
            text_html,
            away_raw="DeNA",
            home_raw="中日",
            away_starter="東",
            home_starter="櫻井",
            pitcher_name_lookup=lookup,
        )

        assert events["home"] == [
            {"打者": "サノー", "player_id": "1561854", "投手": "中川 虎大"},
            {"打者": "サノー", "player_id": "1561854", "投手": "山﨑 康晃"},
        ]

    def test_ambiguous_pitcher_surname_stays_short(self):
        stats_html = """
        <table class="bb-scoreTable">
          <tr class="bb-scoreTable__row">
            <td class="bb-scoreTable__data--player">田中 太郎</td>
          </tr>
          <tr class="bb-scoreTable__row">
            <td class="bb-scoreTable__data--player">田中 次郎</td>
          </tr>
        </table>
        """

        lookup = _parse_pitcher_name_lookup(bs(stats_html, "html.parser"))

        assert lookup.get("田中") is None


class TestParsePlayerThrowHand:
    def test_parses_right_thrower(self):
        assert _parse_player_throw_hand("<dl><dd>右投左打</dd></dl>") == "右投"

    def test_parses_left_thrower(self):
        assert _parse_player_throw_hand("<dl><dd>左投左打</dd></dl>") == "左投"

    def test_parses_switch_thrower(self):
        assert _parse_player_throw_hand("<dl><dd>両投両打</dd></dl>") == "兩投"

    def test_returns_empty_when_absent(self):
        assert _parse_player_throw_hand("<dl><dd>身長</dd></dl>") == ""


class TestParsePitcherIdLookup:
    def test_maps_pitcher_name_aliases_to_player_id(self):
        html = """
        <table class="bb-scoreTable">
          <tr class="bb-scoreTable__row">
            <td class="bb-scoreTable__data--player">
              <a href="/npb/player/1900123/top">齋藤 綱記</a>
            </td>
          </tr>
        </table>
        """

        lookup = _parse_pitcher_id_lookup(bs(html, "html.parser"))

        assert lookup["齋藤 綱記"] == "1900123"
        assert lookup["齋藤綱記"] == "1900123"

    def test_skips_rows_without_links(self):
        html = """
        <table class="bb-scoreTable">
          <tr class="bb-scoreTable__row">
            <td class="bb-scoreTable__data--player">齋藤 綱記</td>
          </tr>
        </table>
        """

        assert _parse_pitcher_id_lookup(bs(html, "html.parser")) == {}


class TestEnrichSwitchHitterPitcherThrows:
    def test_resolves_pitcher_throw_hand_for_switch_hitters_only(self):
        npb._PLAYER_THROW_HAND_CACHE["1900123"] = "左投"
        events = [
            {"打者": "ヒュンメル", "左右打": "兩打", "投手": "齋藤 綱記"},
            {"打者": "泉口 友汰", "左右打": "右打", "投手": "柳"},
        ]

        asyncio.run(
            _enrich_switch_hitter_pitcher_throws(
                events, {"齋藤 綱記": "1900123"}, session=None
            )
        )

        assert events[0]["投手投"] == "左投"
        assert "投手投" not in events[1]

    def test_leaves_throw_empty_when_pitcher_id_missing(self):
        events = [{"打者": "ヒュンメル", "左右打": "兩打", "投手": "謎の投手"}]

        asyncio.run(_enrich_switch_hitter_pitcher_throws(events, {}, session=None))

        assert events[0]["投手投"] == ""


class TestHomeRunDirectionFontRequests:
    @staticmethod
    def _color(request: dict) -> dict:
        return request["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"][
            "foregroundColor"
        ]

    def _requests_for(self, events: list[dict]) -> list[dict]:
        game = {
            "日期": "2026/06/21",
            "全壘打明細": [{"日期": "2026/06/21", **e} for e in events],
        }
        return _home_run_direction_font_requests(1, [game], 30, 2)

    def test_marks_opposite_field_home_runs_red(self):
        events = [
            {"打者": "A", "左右打": "右打", "方向": "右本"},  # opposite → red
            {"打者": "B", "左右打": "右打", "方向": "右中本"},  # opposite → red
            {"打者": "C", "左右打": "右打", "方向": "左本"},  # pull → default
            {"打者": "D", "左右打": "左打", "方向": "左本"},  # opposite → red
            {"打者": "E", "左右打": "左打", "方向": "左中本"},  # opposite → red
            {"打者": "F", "左右打": "左打", "方向": "右本"},  # pull → default
            {"打者": "G", "左右打": "右打", "方向": "中本"},  # center → default
        ]

        requests = self._requests_for(events)
        red = NpbLeagueSheetService.hex_to_rgb(OPPOSITE_FIELD_FONT)
        default = NpbLeagueSheetService.hex_to_rgb(DEFAULT_FONT)

        assert [self._color(r) for r in requests[:7]] == [
            red,
            red,
            default,
            red,
            red,
            default,
            default,
        ]

    def test_targets_direction_column_and_event_rows(self):
        requests = self._requests_for([{"打者": "A", "左右打": "右打", "方向": "右本"}])

        assert len(requests) == 8  # all slots emitted so stale red is reset
        first = requests[0]["repeatCell"]["range"]
        assert first["startColumnIndex"] == 5  # 方向 → col_start(2) + 3
        assert first["startRowIndex"] == 30
        assert requests[1]["repeatCell"]["range"]["startRowIndex"] == 31

    def test_switch_hitter_uses_pitcher_throwing_hand(self):
        events = [
            # vs left pitcher → bats right → 右 is opposite field → red
            {"打者": "S1", "左右打": "兩打", "方向": "右本", "投手投": "左投"},
            {"打者": "S2", "左右打": "兩打", "方向": "左本", "投手投": "左投"},
            # vs right pitcher → bats left → 左 is opposite field → red
            {"打者": "S3", "左右打": "兩打", "方向": "左本", "投手投": "右投"},
            {"打者": "S4", "左右打": "兩打", "方向": "右本", "投手投": "右投"},
            # unknown pitcher hand → cannot decide → default
            {"打者": "S5", "左右打": "兩打", "方向": "右本", "投手投": ""},
        ]

        requests = self._requests_for(events)
        red = NpbLeagueSheetService.hex_to_rgb(OPPOSITE_FIELD_FONT)
        default = NpbLeagueSheetService.hex_to_rgb(DEFAULT_FONT)

        assert [self._color(r) for r in requests[:5]] == [
            red,
            default,
            red,
            default,
            default,
        ]


# ---------------------------------------------------------------------------
# _parse_official_caught_stealing
# ---------------------------------------------------------------------------


class TestParseOfficialCaughtStealing:
    def test_counts_explicit_steal_failures_by_half_inning(self):
        html = """
        <div id="progress">
          <h5>7回表（楽天の攻撃）</h5>
          <table>
            <tr>
              <td>0アウト</td><td>1塁</td>
              <td class="w2">（走者・山﨑）二塁盗塁失敗</td>
            </tr>
          </table>
          <table><tr><td>牽制死</td></tr></table>
          <h5>7回裏（日本ハムの攻撃）</h5>
          <table><tr><td>（走者・五十幡）二塁盗塁死</td></tr></table>
        </div>
        """

        assert _parse_official_caught_stealing(html) == {"away": 1, "home": 1}

    def test_ignores_non_steal_baserunning_outs(self):
        html = """
        <div id="progress">
          <h5>1回表（楽天の攻撃）</h5>
          <table><tr><td>（走者・山﨑）牽制死</td></tr></table>
          <table><tr><td>（走者・山﨑）本塁タッチアウト</td></tr></table>
        </div>
        """

        assert _parse_official_caught_stealing(html) == {"away": 0, "home": 0}


# ---------------------------------------------------------------------------
# _pitcher_font_requests
# ---------------------------------------------------------------------------


class TestPitcherFontRequests:
    def test_returns_games_count_requests(self):
        reqs = _pitcher_font_requests(
            sheet_id=0, games=SAMPLE_GAMES, game_start_row=4, col_start=2
        )
        assert len(reqs) == GAMES_COUNT

    def test_each_request_is_repeat_cell(self):
        reqs = _pitcher_font_requests(
            sheet_id=0, games=[], game_start_row=4, col_start=2
        )
        for req in reqs:
            assert "repeatCell" in req

    def test_pitcher_column_is_col_start_plus_one(self):
        # col_start=2 (1-indexed) → pitcher_col = 3 (0-indexed)
        reqs = _pitcher_font_requests(
            sheet_id=0, games=[], game_start_row=4, col_start=2
        )
        for req in reqs:
            rng = req["repeatCell"]["range"]
            assert rng["startColumnIndex"] == 3
            assert rng["endColumnIndex"] == 4

    def test_row_indices_are_sequential(self):
        reqs = _pitcher_font_requests(
            sheet_id=0, games=[], game_start_row=4, col_start=2
        )
        for i, req in enumerate(reqs):
            rng = req["repeatCell"]["range"]
            assert rng["startRowIndex"] == 3 + i  # game_start_row - 1 + i
            assert rng["endRowIndex"] == 4 + i

    def test_sheet_id_passed_through(self):
        reqs = _pitcher_font_requests(
            sheet_id=99, games=[], game_start_row=4, col_start=2
        )
        for req in reqs:
            assert req["repeatCell"]["range"]["sheetId"] == 99

    def test_empty_game_rows_use_default_font_size(self):
        reqs = _pitcher_font_requests(
            sheet_id=0, games=[], game_start_row=4, col_start=2
        )
        for req in reqs:
            assert (
                req["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"]["fontSize"]
                == 10
            )

    def test_long_pitcher_name_gets_smaller_font(self):
        # "マルティネス" = 6 chars -> 8pt
        game = _make_game(
            "2025/03/28", "橫 濱", "マルティネス", "横 浜", 0, 0, 0, 0, 0, 0, 0, 0, 0
        )
        reqs = _pitcher_font_requests(
            sheet_id=0, games=[game], game_start_row=4, col_start=2
        )
        assert (
            reqs[0]["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"]["fontSize"]
            == 8
        )

    def test_short_pitcher_name_gets_default_font(self):
        game = _make_game(
            "2025/03/28", "橫 濱", "田中", "横 浜", 0, 0, 0, 0, 0, 0, 0, 0, 0
        )
        reqs = _pitcher_font_requests(
            sheet_id=0, games=[game], game_start_row=4, col_start=2
        )
        assert (
            reqs[0]["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"]["fontSize"]
            == 10
        )


# ---------------------------------------------------------------------------
# _header_format_request
# ---------------------------------------------------------------------------


class TestHeaderFormatRequest:
    def test_returns_repeat_cell_request(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        assert "repeatCell" in req

    def test_range_uses_zero_indexed_header_row(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        rng = req["repeatCell"]["range"]
        assert rng["startRowIndex"] == 2  # header_row - 1
        assert rng["endRowIndex"] == 3

    def test_range_spans_14_columns(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        rng = req["repeatCell"]["range"]
        assert rng["startColumnIndex"] == 1  # col_start - 1
        assert rng["endColumnIndex"] == 15  # col_start + 13

    def test_sheet_id_passed_through(self):
        req = _header_format_request(
            sheet_id=42, team_key="巨人", header_row=3, col_start=2
        )
        assert req["repeatCell"]["range"]["sheetId"] == 42

    def test_background_color_matches_team_fill(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        bg = req["repeatCell"]["cell"]["userEnteredFormat"]["backgroundColor"]
        assert bg == hex_to_rgb(NPB_TEAMS["巨人"]["fill"])

    def test_font_color_matches_team_font(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        fg = req["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"][
            "foregroundColor"
        ]
        assert fg == hex_to_rgb(NPB_TEAMS["巨人"]["font"])

    def test_does_not_touch_non_colour_text_format(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        assert (
            "bold" not in req["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"]
        )

    def test_different_teams_have_different_colors(self):
        req_giants = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        req_tigers = _header_format_request(
            sheet_id=0, team_key="阪神", header_row=3, col_start=2
        )
        bg_giants = req_giants["repeatCell"]["cell"]["userEnteredFormat"][
            "backgroundColor"
        ]
        bg_tigers = req_tigers["repeatCell"]["cell"]["userEnteredFormat"][
            "backgroundColor"
        ]
        assert bg_giants != bg_tigers

    def test_fields_value_is_set(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        assert (
            req["repeatCell"]["fields"]
            == "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.foregroundColor"
        )


class TestConditionalFormatDeleteRequests:
    def test_deletes_rules_in_reverse_index_order(self):
        class FakeSpreadsheet:
            def fetch_sheet_metadata(self, params):
                assert params == {
                    "fields": "sheets(properties(sheetId),conditionalFormats)"
                }
                return {
                    "sheets": [
                        {
                            "properties": {"sheetId": 99},
                            "conditionalFormats": [{}, {}, {}],
                        }
                    ]
                }

        requests = NpbLeagueSheetService.conditional_format_delete_requests(
            FakeSpreadsheet(), 99
        )

        assert [
            request["deleteConditionalFormatRule"]["index"] for request in requests
        ] == [2, 1, 0]


class TestLeagueSheetLayoutClear:
    def test_layout_clear_range_covers_old_and_new_blocks(self):
        service = NpbLeagueSheetService(module=npb)

        assert service.layout_clear_range() == "B3:AS48"

    def test_home_run_pitcher_merge_requests_cover_pitcher_cells_only(self):
        service = NpbLeagueSheetService(module=npb)

        requests = service.home_run_pitcher_merge_requests(
            sheet_id=99,
            start_row=npb.TOP_HR_HEADER_ROW,
            end_row=npb.TOP_HR_END_ROW,
            col_start=2,
        )

        assert len(requests) == 9
        first = requests[0]["mergeCells"]
        assert first["mergeType"] == "MERGE_ALL"
        assert first["range"] == {
            "sheetId": 99,
            "startRowIndex": 29,
            "endRowIndex": 30,
            "startColumnIndex": 6,
            "endColumnIndex": 8,
        }

    def test_home_run_pitcher_unmerge_requests_cover_pitcher_cells_only(self):
        service = NpbLeagueSheetService(module=npb)

        requests = service.home_run_pitcher_unmerge_requests(
            sheet_id=99,
            start_row=npb.TOP_HR_HEADER_ROW,
            end_row=npb.TOP_HR_END_ROW,
            col_start=2,
        )

        assert len(requests) == 9
        assert requests[0]["unmergeCells"]["range"] == {
            "sheetId": 99,
            "startRowIndex": 29,
            "endRowIndex": 30,
            "startColumnIndex": 6,
            "endColumnIndex": 8,
        }


# ---------------------------------------------------------------------------
# _game_font_color_requests
# ---------------------------------------------------------------------------


class TestGameFontColorRequests:
    def _foreground_hex(self, request):
        color = request["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"][
            "foregroundColor"
        ]

        def channel(name):
            return round(color.get(name, 0) * 255)

        return f"{channel('red'):02x}{channel('green'):02x}{channel('blue'):02x}"

    def test_runs_more_than_allowed_colours_runs_red(self):
        game = _make_game(
            "2025/04/01", "燕 子", "投手", "東 京", 0, 5, 3, 0, 9, 0, 0, 0, 0
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[0]) == SCORE_WIN_FONT
        assert self._foreground_hex(reqs[1]) == DEFAULT_FONT

    def test_allowed_more_than_runs_colours_allowed_green(self):
        game = _make_game(
            "2025/04/01", "燕 子", "投手", "東 京", 0, 2, 6, 0, 9, 0, 0, 0, 0
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[0]) == DEFAULT_FONT
        assert self._foreground_hex(reqs[1]) == SCORE_LOSS_FONT

    def test_tie_colours_both_score_cells_blue(self):
        game = _make_game(
            "2025/04/01", "燕 子", "投手", "東 京", 0, 4, 4, 0, 9, 0, 0, 0, 0
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[0]) == SCORE_TIE_FONT
        assert self._foreground_hex(reqs[1]) == SCORE_TIE_FONT

    def test_hits_10_or_more_colours_hits_orange_brown(self):
        game = _make_game(
            "2025/04/01", "燕 子", "投手", "東 京", 0, 4, 4, 0, 10, 0, 0, 0, 0
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[2]) == HITS_10_PLUS_FONT

    def test_hits_under_10_reset_to_default(self):
        game = _make_game(
            "2025/04/01", "燕 子", "投手", "東 京", 0, 4, 4, 0, 9, 0, 0, 0, 0
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[2]) == DEFAULT_FONT

    def test_batting_average_280_or_more_colours_red(self):
        # 6 hits / 20 AB = .300 ≥ .280
        game = _make_game(
            "2025/04/01",
            "燕 子",
            "投手",
            "東 京",
            0,
            4,
            4,
            0,
            6,
            0,
            0,
            0,
            0,
            ab=20,
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[3]) == HOT_RATE_FONT

    def test_batting_average_200_or_less_colours_green(self):
        # 4 hits / 20 AB = .200 ≤ .200
        game = _make_game(
            "2025/04/01",
            "燕 子",
            "投手",
            "東 京",
            0,
            4,
            4,
            0,
            4,
            0,
            0,
            0,
            0,
            ab=20,
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[3]) == COLD_RATE_FONT

    def test_on_base_percentage_330_or_more_colours_red(self):
        # (5 H + 4 BB) / (20 AB + 4 BB) = 9/24 = .375 ≥ .330
        game = _make_game(
            "2025/04/01",
            "燕 子",
            "投手",
            "東 京",
            0,
            4,
            4,
            0,
            5,
            0,
            4,
            0,
            0,
            ab=20,
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[4]) == HOT_RATE_FONT

    def test_on_base_percentage_250_or_less_colours_green(self):
        # (4 H + 1 BB) / (20 AB + 1 BB + 1 SF) = 5/22 = .227 ≤ .250
        game = _make_game(
            "2025/04/01",
            "燕 子",
            "投手",
            "東 京",
            0,
            4,
            4,
            0,
            4,
            0,
            1,
            0,
            0,
            ab=20,
            sf=1,
        )
        reqs = _game_font_color_requests(0, [game], game_start_row=4, col_start=2)
        assert self._foreground_hex(reqs[4]) == COLD_RATE_FONT

    def test_average_rows_apply_rate_colours(self):
        # avg = 6/20 = .300 ≥ .280; obp = (6+3)/(20+3) = .391 ≥ .330
        games = [
            _make_game(
                "2025/04/01",
                "燕 子",
                "投手",
                "東 京",
                0,
                4,
                3,
                0,
                6,
                0,
                3,
                0,
                0,
                ab=20,
            )
        ]

        reqs = _game_font_color_requests(0, games, game_start_row=4, col_start=2)

        assert self._foreground_hex(reqs[-4]) == HOT_RATE_FONT
        assert self._foreground_hex(reqs[-3]) == HOT_RATE_FONT
        assert self._foreground_hex(reqs[-2]) == HOT_RATE_FONT
        assert self._foreground_hex(reqs[-1]) == HOT_RATE_FONT
        assert reqs[-4]["repeatCell"]["range"]["startRowIndex"] == 13
        assert reqs[-2]["repeatCell"]["range"]["startRowIndex"] == 14


# ---------------------------------------------------------------------------
# HTML fixtures for async scraping tests
# ---------------------------------------------------------------------------

# ヤクルト (away) vs 巨人 (home) on 2025-04-01 at 東京ドーム
# Away batting: 得分=3 安打=8 三振=7 四球=2 死球=1 全壘打=1
# Home batting: 得分=5 安打=10 三振=9 四球=3 死球=0 全壘打=2
# Score tbl 0 (away pitchers, batter=巨人):  pitcher=山本由伸, R=5, ER=4
#   → 巨人 実分+=4, ヤクルト 失分+=5
# Score tbl 1 (home pitchers, batter=ヤクルト): pitcher=菅野智之, R=3, ER=2
#   → ヤクルト 実分+=2, 巨人 失分+=3
# 実失 cross-assigned: 巨人実失=2, ヤクルト実失=4
VALID_GAME_HTML = """
<html>
<head><title>プロ野球 2025年4月1日 ヤクルト vs 巨人</title></head>
<body>
  <span class="bb-gameScoreTable__team">ヤクルト</span>
  <span class="bb-gameScoreTable__team">巨人</span>
  <span class="bb-gameRound--stadium">東京ドーム</span>
  <table class="bb-statsTable">
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">3</td>
    <td class="bb-statsTable__data--result">8</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">7</td>
    <td class="bb-statsTable__data--result">2</td>
    <td class="bb-statsTable__data--result">1</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">1</td>
  </table>
  <table class="bb-statsTable">
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">5</td>
    <td class="bb-statsTable__data--result">10</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">9</td>
    <td class="bb-statsTable__data--result">3</td>
    <td class="bb-statsTable__data--result">0</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">x</td>
    <td class="bb-statsTable__data--result">2</td>
  </table>
  <div class="bb-scoreTable">
    <div class="bb-scoreTable__row">
      <span class="bb-scoreTable__data--player">山本由伸</span>
      <span class="bb-scoreTable__data--score">5</span>
      <span class="bb-scoreTable__data--score">4</span>
    </div>
  </div>
  <div class="bb-scoreTable">
    <div class="bb-scoreTable__row">
      <span class="bb-scoreTable__data--player">菅野智之</span>
      <span class="bb-scoreTable__data--score">3</span>
      <span class="bb-scoreTable__data--score">2</span>
    </div>
  </div>
</body>
</html>
"""

NO_VENUE_GAME_HTML = """
<html>
<head><title>2025年4月1日 ヤクルト vs 巨人</title></head>
<body>
  <span class="bb-gameScoreTable__team">ヤクルト</span>
  <span class="bb-gameScoreTable__team">巨人</span>
</body>
</html>
"""

UNKNOWN_TEAM_GAME_HTML = """
<html>
<head><title>2025年4月1日</title></head>
<body>
  <span class="bb-gameScoreTable__team">UnknownFC</span>
  <span class="bb-gameScoreTable__team">巨人</span>
  <span class="bb-gameRound--stadium">東京ドーム</span>
</body>
</html>
"""


def _cal_html(*entries):
    """Build a minimal calendar schedule page.

    Each entry dict supports keys: day, status (text), href, links (list of hrefs).
    """
    parts = ["<html><body>"]
    for e in entries:
        day = e.get("day", "")
        status_text = e.get("status", "")
        href = e.get("href", "")
        links = e.get("links", [])
        parts.append('<div class="bb-calendarTable__data">')
        parts.append(f'  <span class="bb-calendarTable__date">{day}</span>')
        if href:
            parts.append(
                f'  <a class="bb-calendarTable__status" href="{href}">'
                f"{status_text}</a>"
            )
        else:
            parts.append(
                f'  <span class="bb-calendarTable__status">{status_text}</span>'
            )
        for link_href in links:
            parts.append(f'  <a href="{link_href}">team</a>')
        parts.append("</div>")
    parts.append("</body></html>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# get_game_info
# ---------------------------------------------------------------------------


class TestGetGameInfo:
    """Tests for get_game_info — mocks _fetch to avoid network calls."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_session(self):
        return AsyncMock()

    def test_returns_none_when_fetch_fails(self):
        with patch("npb._fetch", new=AsyncMock(return_value=None)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result is None

    def test_returns_none_when_fewer_than_two_team_elements(self):
        html = (
            "<html><body>"
            "<span class='bb-gameScoreTable__team'>巨人</span>"
            "</body></html>"
        )
        with patch("npb._fetch", new=AsyncMock(return_value=html)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result is None

    def test_returns_none_when_team_not_in_npb_teams(self):
        with patch("npb._fetch", new=AsyncMock(return_value=UNKNOWN_TEAM_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result is None

    def test_returns_none_when_no_venue_element(self):
        with patch("npb._fetch", new=AsyncMock(return_value=NO_VENUE_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result is None

    def test_returns_none_when_title_has_no_date(self):
        html = (
            "<html><head><title>ヤクルト vs 巨人 (no date)</title></head>"
            "<body>"
            "<span class='bb-gameScoreTable__team'>ヤクルト</span>"
            "<span class='bb-gameScoreTable__team'>巨人</span>"
            "<span class='bb-gameRound--stadium'>東京ドーム</span>"
            "</body></html>"
        )
        with patch("npb._fetch", new=AsyncMock(return_value=html)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result is None

    def test_valid_game_result_keys(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result is not None
        assert "teams" in result
        assert "home" in result
        assert "away" in result
        assert "game_id" in result

    def test_valid_game_teams_identified(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result["away"] == "燕 子"
        assert result["home"] == "巨 人"

    def test_valid_game_date_parsed(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result["巨 人"]["日期"] == "2025/04/01"

    def test_valid_game_venue_mapped(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result["巨 人"]["球場"] == "東 京"

    def test_valid_game_batting_stats_away(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        away = result["燕 子"]
        assert away["得分"] == 3
        assert away["安打"] == 8
        assert away["三振"] == 7
        assert away["四球"] == 2
        assert away["死球"] == 1
        assert away["全壘打"] == 1

    def test_valid_game_batting_stats_home(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        home = result["巨 人"]
        assert home["得分"] == 5
        assert home["安打"] == 10
        assert home["三振"] == 9
        assert home["四球"] == 3
        assert home["死球"] == 0
        assert home["全壘打"] == 2

    def test_valid_game_pitcher_names(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result["巨 人"]["對戰先發"] == "山本由伸"
        assert result["燕 子"]["對戰先發"] == "菅野智之"

    def test_valid_game_earned_runs(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        home = result["巨 人"]
        away = result["燕 子"]
        assert home["実分"] == 4
        assert home["失分"] == 3
        assert home["実失"] == 2
        assert away["実分"] == 2
        assert away["失分"] == 5
        assert away["実失"] == 4

    def test_opponent_team_names_set(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result["巨 人"]["對戰球隊"] == "燕 子"
        assert result["燕 子"]["對戰球隊"] == "巨 人"

    def test_game_id_stored_in_result(self):
        with patch("npb._fetch", new=AsyncMock(return_value=VALID_GAME_HTML)):
            result = self._run(get_game_info("g001", self._mock_session()))
        assert result["game_id"] == "g001"


# ---------------------------------------------------------------------------
# get_next_scheduled_game
# ---------------------------------------------------------------------------


class TestGetNextScheduledGame:
    """Tests for get_next_scheduled_game — mocks _fetch and datetime.now."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_session(self):
        return AsyncMock()

    def _patch_now(self, dt):
        return patch(
            "npb.datetime",
            **{
                "now.return_value": dt,
                "strptime.side_effect": datetime.strptime,
                "combine.side_effect": datetime.combine,
                "min": datetime.min,
            },
        )

    def test_returns_none_none_when_fetch_fails(self):
        with patch("npb._fetch", new=AsyncMock(return_value=None)):
            game_id, date = self._run(get_next_scheduled_game(1, self._mock_session()))
        assert game_id is None
        assert date is None

    def test_returns_upcoming_game_id_and_date(self):
        fake_now = datetime(2026, 3, 26)
        html = _cal_html(
            {"day": "25", "status": "試合終了", "href": "/npb/game/old01/top"},
            {"day": "27", "status": "先発：菅野", "href": "/npb/game/2026032701/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                game_id, date = self._run(
                    get_next_scheduled_game(1, self._mock_session())
                )
        assert game_id == "2026032701"
        assert date == "2026-03-27"

    def test_skips_finished_games(self):
        fake_now = datetime(2026, 3, 26)
        html = _cal_html(
            {"day": "26", "status": "試合終了", "href": "/npb/game/finished/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                game_id, date = self._run(
                    get_next_scheduled_game(1, self._mock_session())
                )
        assert game_id is None
        assert date is None

    def test_skips_cancelled_games_and_finds_next(self):
        fake_now = datetime(2026, 3, 26)
        html = _cal_html(
            {"day": "27", "status": "中止"},
            {"day": "28", "status": "12:00", "href": "/npb/game/2026032801/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                game_id, date = self._run(
                    get_next_scheduled_game(1, self._mock_session())
                )
        assert game_id == "2026032801"
        assert date == "2026-03-28"

    def test_returns_none_game_id_when_no_href(self):
        fake_now = datetime(2026, 3, 26)
        html = _cal_html({"day": "27", "status": "13:00"})
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                game_id, date = self._run(
                    get_next_scheduled_game(1, self._mock_session())
                )
        assert game_id is None
        assert date == "2026-03-27"

    def test_skips_past_dates(self):
        fake_now = datetime(2026, 3, 26)
        html = _cal_html(
            {"day": "24", "status": "先発", "href": "/npb/game/past01/top"},
            {"day": "25", "status": "先発", "href": "/npb/game/past02/top"},
            {"day": "27", "status": "先発", "href": "/npb/game/future01/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                game_id, _ = self._run(get_next_scheduled_game(1, self._mock_session()))
        assert game_id == "future01"


# ---------------------------------------------------------------------------
# resolve_prediction_game_by_home_team
# ---------------------------------------------------------------------------


class TestResolvePredictionGameByHomeTeam:
    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_session(self):
        return AsyncMock()

    def test_resolves_today_home_team_game_and_starters(self):
        schedule_html = _cal_html(
            {
                "day": "12",
                "status": "18:00",
                "href": "/npb/game/2026051201/top",
            }
        )
        game_html = """
        <html><body>
          <span class="bb-gameScoreTable__team">ヤクルト</span>
          <span class="bb-gameScoreTable__team">巨人</span>
          <div class="bb-scoreTable"><div class="bb-scoreTable__row">
            <span class="bb-scoreTable__data--player">小川泰弘</span>
          </div></div>
          <div class="bb-scoreTable"><div class="bb-scoreTable__row">
            <span class="bb-scoreTable__data--player">戸郷翔征</span>
          </div></div>
        </body></html>
        """

        async def fake_fetch(_session, url):
            if "teams/1/schedule" in url:
                return schedule_html
            if "game/2026051201/" in url:
                return game_html
            return None

        with patch("npb._fetch", new=fake_fetch):
            result = self._run(
                resolve_prediction_game_by_home_team(
                    "巨人", self._mock_session(), today=date(2026, 5, 12)
                )
            )

        assert result == {
            "game_id": "2026051201",
            "game_date": "2026-05-12",
            "away_team": "ヤクルト",
            "home_team": "巨人",
            "away_starter": "小川泰弘",
            "home_starter": "戸郷翔征",
        }

    def test_rejects_when_requested_team_is_not_home(self):
        schedule_html = _cal_html(
            {
                "day": "13",
                "status": "18:00",
                "href": "/npb/game/2026051301/top",
            }
        )
        game_html = """
        <html><body>
          <span class="bb-gameScoreTable__team">巨人</span>
          <span class="bb-gameScoreTable__team">ヤクルト</span>
        </body></html>
        """

        async def fake_fetch(_session, url):
            if "teams/1/schedule" in url:
                return schedule_html
            if "game/2026051301/" in url:
                return game_html
            return None

        with patch("npb._fetch", new=fake_fetch):
            with pytest.raises(ValueError, match="No unstarted today/tomorrow"):
                self._run(
                    resolve_prediction_game_by_home_team(
                        "巨人", self._mock_session(), today=date(2026, 5, 12)
                    )
                )

    def test_skips_finished_and_in_progress_schedule_entries(self):
        schedule_html = _cal_html(
            {
                "day": "12",
                "status": "3回表",
                "href": "/npb/game/live01/top",
            },
            {
                "day": "13",
                "status": "試合終了",
                "href": "/npb/game/done01/top",
            },
        )

        async def fake_fetch(_session, url):
            if "teams/1/schedule" in url:
                return schedule_html
            return "<html><body></body></html>"

        with patch("npb._fetch", new=fake_fetch):
            with pytest.raises(ValueError, match="No unstarted today/tomorrow"):
                self._run(
                    resolve_prediction_game_by_home_team(
                        "巨人", self._mock_session(), today=date(2026, 5, 12)
                    )
                )


# ---------------------------------------------------------------------------
# _get_schedule_opponent
# ---------------------------------------------------------------------------


class TestGetScheduleOpponent:
    """Tests for _get_schedule_opponent — mocks _fetch."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_session(self):
        return AsyncMock()

    def test_returns_none_when_fetch_fails(self):
        with patch("npb._fetch", new=AsyncMock(return_value=None)):
            result = self._run(
                _get_schedule_opponent(1, "2026-03-26", self._mock_session())
            )
        assert result is None

    def test_returns_opponent_key_by_team_id(self):
        html = _cal_html({"day": "26", "links": ["/npb/teams/1/schedule"]})
        with patch("npb._fetch", new=AsyncMock(return_value=html)):
            result = self._run(
                _get_schedule_opponent(2, "2026-03-26", self._mock_session())
            )
        assert result == "巨人"

    def test_returns_none_when_day_does_not_match(self):
        html = _cal_html({"day": "25", "links": ["/npb/teams/1/schedule"]})
        with patch("npb._fetch", new=AsyncMock(return_value=html)):
            result = self._run(
                _get_schedule_opponent(2, "2026-03-26", self._mock_session())
            )
        assert result is None

    def test_returns_none_when_no_team_link(self):
        html = _cal_html({"day": "26", "status": "試合終了"})
        with patch("npb._fetch", new=AsyncMock(return_value=html)):
            result = self._run(
                _get_schedule_opponent(2, "2026-03-26", self._mock_session())
            )
        assert result is None

    def test_recognises_all_npb_team_ids(self):
        """Every team ID in NPB_TEAMS resolves to the correct key."""
        for expected_key, info in NPB_TEAMS.items():
            tid = info["id"]
            html = _cal_html({"day": "1", "links": [f"/npb/teams/{tid}/schedule"]})
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                result = self._run(
                    _get_schedule_opponent(999, "2026-04-01", self._mock_session())
                )
            assert result == expected_key, f"Expected {expected_key} for id={tid}"


# ---------------------------------------------------------------------------
# get_last_n_game_ids
# ---------------------------------------------------------------------------


class TestGetLastNGameIds:
    """Tests for get_last_n_game_ids — mocks _fetch and datetime.now."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_session(self):
        return AsyncMock()

    def _patch_now(self, dt):
        return patch(
            "npb.datetime",
            **{"now.return_value": dt, "strptime.side_effect": datetime.strptime},
        )

    def test_returns_empty_list_when_fetch_fails(self):
        with patch("npb._fetch", new=AsyncMock(return_value=None)):
            result = self._run(get_last_n_game_ids(1, 3, self._mock_session()))
        assert result == []

    def test_returns_game_id_for_completed_game(self):
        fake_now = datetime(2026, 3, 26)
        html = _cal_html(
            {"day": "26", "status": "試合終了", "href": "/npb/game/2026032601/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                result = self._run(get_last_n_game_ids(1, 1, self._mock_session()))
        assert result == ["2026032601"]

    def test_skips_non_completed_entries(self):
        # Use day=1 so decrementing crosses a month boundary, which breaks
        # the inner for-loop and lets the next _fetch=None exit the while-loop.
        fake_now = datetime(2026, 3, 1)
        html = _cal_html(
            {"day": "1", "status": "先発：投手", "href": "/npb/game/2026030101/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(side_effect=[html, None])):
                result = self._run(get_last_n_game_ids(1, 1, self._mock_session()))
        assert result == []

    def test_collects_multiple_game_ids_across_days(self):
        fake_now = datetime(2026, 3, 26)
        # Entries for days 25 and 26; reversed → day 26 processed first
        html = _cal_html(
            {"day": "25", "status": "試合終了", "href": "/npb/game/2026032501/top"},
            {"day": "26", "status": "試合終了", "href": "/npb/game/2026032601/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                result = self._run(get_last_n_game_ids(1, 2, self._mock_session()))
        assert "2026032601" in result
        assert "2026032501" in result
        assert len(result) == 2

    def test_does_not_return_duplicate_game_ids(self):
        # Days 2 and 1 both carry the same game ID.  After day=1 is processed
        # the decrement crosses to February, breaking the inner loop.  The
        # subsequent _fetch=None exits the outer while-loop.
        fake_now = datetime(2026, 3, 2)
        html = _cal_html(
            {"day": "1", "status": "試合終了", "href": "/npb/game/2026030201/top"},
            {"day": "2", "status": "試合終了", "href": "/npb/game/2026030201/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(side_effect=[html, None])):
                result = self._run(get_last_n_game_ids(1, 5, self._mock_session()))
        assert result.count("2026030201") == 1

    def test_respects_n_limit(self):
        fake_now = datetime(2026, 3, 26)
        html = _cal_html(
            {"day": "26", "status": "試合終了", "href": "/npb/game/2026032601/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                result = self._run(get_last_n_game_ids(1, 1, self._mock_session()))
        assert len(result) <= 1
