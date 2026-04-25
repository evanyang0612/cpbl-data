"""
Unit tests for NPB data-transformation logic.
Covers: hex_to_rgb, col_to_letter, _pitcher_font_size, build_block_values,
        get_game_info, get_next_scheduled_game, _get_schedule_opponent,
        get_last_n_game_ids.
"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from npb import (
    _get_schedule_opponent,
    _header_format_request,
    _pitcher_font_requests,
    _pitcher_font_size,
    build_block_values,
    col_to_letter,
    get_game_info,
    get_last_n_game_ids,
    get_next_scheduled_game,
    hex_to_rgb,
    GAMES_COUNT,
    NPB_TEAMS,
)


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
        # BLOCK_COLS = [2, 15, 28] → B, O, AB
        assert col_to_letter(2) == "B"
        assert col_to_letter(15) == "O"
        assert col_to_letter(28) == "AB"


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
):
    return {
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
    }


SAMPLE_GAMES = [
    _make_game("2025/03/28", "橫 濱", "濱田", "横 浜", 3, 4, 2, 2, 8, 7, 3, 0, 1),
    _make_game("2025/03/29", "橫 濱", "田中", "横 浜", 1, 1, 5, 5, 5, 6, 2, 1, 0),
    _make_game("2025/03/30", "燕 子", "山本", "神 宮", 5, 6, 1, 1, 12, 8, 1, 0, 2),
    _make_game("2025/04/01", "燕 子", "原", "東 京", 2, 2, 3, 3, 7, 9, 4, 0, 0),
    _make_game("2025/04/02", "中 日", "小澤", "名古屋", 0, 0, 2, 2, 3, 5, 1, 0, 0),
]


class TestBuildBlockValues:
    def test_returns_13_rows(self):
        rows = build_block_values("巨人", SAMPLE_GAMES)
        assert len(rows) == 13

    def test_each_row_has_12_cols(self):
        rows = build_block_values("巨人", SAMPLE_GAMES)
        for row in rows:
            assert len(row) == 12

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
            assert rows[i] == [""] * 12

    def test_bb_plus_hbp_combined(self):
        # Game at index 1 (2025/03/28): bb=3, hbp=0 → col index 10 = 3
        rows = build_block_values("巨人", SAMPLE_GAMES)
        assert rows[1][10] == 3  # bb+hbp

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
        assert rows[11] == ["", "", "近十場", "平 均"] + [""] * 8
        assert rows[12] == ["", "", "近五場", "平 均"] + [""] * 8

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
        non_empty = sum(1 for r in rows[1:11] if r != [""] * 12)
        assert non_empty == 10


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

    def test_range_spans_12_columns(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        rng = req["repeatCell"]["range"]
        assert rng["startColumnIndex"] == 1  # col_start - 1
        assert rng["endColumnIndex"] == 13  # col_start + 11

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

    def test_text_is_bold(self):
        req = _header_format_request(
            sheet_id=0, team_key="巨人", header_row=3, col_start=2
        )
        assert (
            req["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"]["bold"] is True
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
            == "userEnteredFormat(backgroundColor,textFormat)"
        )


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
            **{"now.return_value": dt, "strptime.side_effect": datetime.strptime},
        )

    def test_returns_none_none_when_fetch_fails(self):
        with patch("npb._fetch", new=AsyncMock(return_value=None)):
            game_id, date = self._run(
                get_next_scheduled_game(1, self._mock_session())
            )
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
                game_id, _ = self._run(
                    get_next_scheduled_game(1, self._mock_session())
                )
        assert game_id == "future01"


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
        html = _cal_html(
            {"day": "26", "links": ["/npb/teams/1/schedule"]}
        )
        with patch("npb._fetch", new=AsyncMock(return_value=html)):
            result = self._run(
                _get_schedule_opponent(2, "2026-03-26", self._mock_session())
            )
        assert result == "巨人"

    def test_returns_none_when_day_does_not_match(self):
        html = _cal_html(
            {"day": "25", "links": ["/npb/teams/1/schedule"]}
        )
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
            {"day": "26", "status": "試合終了",
             "href": "/npb/game/2026032601/top"},
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
            {"day": "1", "status": "先発：投手",
             "href": "/npb/game/2026030101/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(side_effect=[html, None])):
                result = self._run(get_last_n_game_ids(1, 1, self._mock_session()))
        assert result == []

    def test_collects_multiple_game_ids_across_days(self):
        fake_now = datetime(2026, 3, 26)
        # Entries for days 25 and 26; reversed → day 26 processed first
        html = _cal_html(
            {"day": "25", "status": "試合終了",
             "href": "/npb/game/2026032501/top"},
            {"day": "26", "status": "試合終了",
             "href": "/npb/game/2026032601/top"},
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
            {"day": "1", "status": "試合終了",
             "href": "/npb/game/2026030201/top"},
            {"day": "2", "status": "試合終了",
             "href": "/npb/game/2026030201/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(side_effect=[html, None])):
                result = self._run(get_last_n_game_ids(1, 5, self._mock_session()))
        assert result.count("2026030201") == 1

    def test_respects_n_limit(self):
        fake_now = datetime(2026, 3, 26)
        html = _cal_html(
            {"day": "26", "status": "試合終了",
             "href": "/npb/game/2026032601/top"},
        )
        with self._patch_now(fake_now):
            with patch("npb._fetch", new=AsyncMock(return_value=html)):
                result = self._run(get_last_n_game_ids(1, 1, self._mock_session()))
        assert len(result) <= 1
