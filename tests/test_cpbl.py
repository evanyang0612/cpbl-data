"""
Unit tests for CPBL data-transformation logic.
Covers: _get_pitching_stats, _get_batting_stats, process_and_update_sheet.
"""

import json
from unittest.mock import MagicMock
from cpbl import (
    _get_pitching_stats,
    _get_batting_stats,
    _extract_stats_game,
    _stats_game_to_legacy_payload,
    get_stats_pitching_habit,
    process_and_update_sheet,
    is_game_recorded,
)


# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------


def _pitcher(
    ptype,
    role,
    name,
    innings,
    thirds,
    pa,
    pitches,
    strikes,
    hits,
    hr,
    bb,
    hbp,
    so,
    wp,
    bk,
    r,
    er,
    err=0,
    acnt="",
):
    return {
        "VisitingHomeType": str(ptype),
        "RoleType": role,
        "PitcherName": name,
        "PitcherAcnt": acnt,
        "InningPitchedCnt": str(innings),
        "InningPitchedDiv3Cnt": str(thirds),
        "PlateAppearances": str(pa),
        "PitchCnt": str(pitches),
        "StrikeCnt": str(strikes),
        "HittingCnt": str(hits),
        "HomeRunCnt": str(hr),
        "BasesONBallsCnt": str(bb),
        "HitBYPitchCnt": str(hbp),
        "StrikeOutCnt": str(so),
        "WildPitchCnt": str(wp),
        "BalkCnt": str(bk),
        "RunCnt": str(r),
        "EarnedRunCnt": str(er),
        "ErrorCnt": str(err),
    }


def _batter(
    ptype, ab, r, h, rbi, double, triple, hr, gdp, bb, hbp, so, sh, sf, sb, cs, err=0
):
    return {
        "VisitingHomeType": str(ptype),
        "HitCnt": str(ab),
        "ScoreCnt": str(r),
        "HittingCnt": str(h),
        "RunBattedINCnt": str(rbi),
        "TwoBaseHitCnt": str(double),
        "ThreeBaseHitCnt": str(triple),
        "HomeRunCnt": str(hr),
        "DoublePlayBatCnt": str(gdp),
        "BasesONBallsCnt": str(bb),
        "HitBYPitchCnt": str(hbp),
        "StrikeOutCnt": str(so),
        "SacrificeHitCnt": str(sh),
        "SacrificeFlyCnt": str(sf),
        "StealBaseOKCnt": str(sb),
        "StealBaseFailCnt": str(cs),
        "ErrorCnt": str(err),
    }


PITCHING = [
    # Away (type=1): starter 6.0 IP + reliever 2.2 IP = 8.2 IP total
    _pitcher(1, "先發", "王投手", 6, 0, 22, 88, 60, 5, 1, 3, 1, 7, 0, 0, 2, 2),
    _pitcher(1, "救援", "李救援", 2, 2, 8, 28, 18, 2, 0, 1, 0, 2, 1, 0, 0, 0),
    # Home (type=2): starter 9.0 IP
    _pitcher(2, "先發", "陳先發", 9, 0, 30, 95, 65, 4, 0, 2, 0, 9, 0, 0, 3, 3, err=1),
]

BATTING = [
    # Away batters (type=1)
    _batter(1, 4, 1, 2, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0),
    _batter(1, 3, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0),
    # Home batters (type=2)
    _batter(2, 4, 2, 3, 2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0),
    _batter(2, 3, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0),
]


def _stats_html(game):
    payload = 'b:' + json.dumps(
        [["$", "x", None, {"dehydratedState": {"queries": [{"state": {"data": {"data": {"game": game}}}}]}}]],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return (
        "<html><body><script>"
        f"self.__next_f.push([1,{json.dumps(payload, ensure_ascii=False)}])"
        "</script></body></html>"
    )


STATS_GAME = {
    "gameId": "2026-A-81",
    "gameStatus": "FINISHED",
    "gameSno": 81,
    "preExeDate": "2026-05-05T18:35:00",
    "field": {"abbe": "大巨蛋"},
    "visiting": {
        "team": {"name": "中信兄弟"},
        "score": 3,
        "hittingCnt": 11,
        "errorCnt": 0,
        "inningScore": [
            {"seq": 1, "score": "1"},
            {"seq": 2, "score": "0"},
            {"seq": 9, "score": "2"},
        ],
        "hitters": [
            {
                "hitCnt": 6,
                "scoreCnt": 1,
                "hittingCnt": 3,
                "runBattedInCnt": 1,
                "twoBaseHitCnt": 0,
                "threeBaseHitCnt": 0,
                "homeRunCnt": 0,
                "doublePlayBatCnt": 0,
                "basesOnBallsCnt": 0,
                "hitByPitchCnt": 0,
                "strikeOutCnt": 1,
                "sacrificeHitCnt": 0,
                "sacrificeFlyCnt": 0,
                "stealBaseOkCnt": 1,
                "stealBaseFailCnt": 0,
            }
        ],
        "pitchers": [
            {
                "roleType": "先發",
                "pitcherName": "鄭浩均",
                "pitcherAcnt": "0000002345",
                "inningPitchedCnt": 6,
                "inningPitchedDiv3Cnt": 0,
                "plateAppearances": 24,
                "pitchCnt": 101,
                "strikeCnt": 65,
                "hittingCnt": 5,
                "homeRunCnt": 1,
                "basesOnBallsCnt": 0,
                "hitByPitchCnt": 2,
                "strikeOutCnt": 5,
                "wildPitchCnt": 0,
                "balkCnt": 0,
                "runCnt": 3,
                "earnedRunCnt": 3,
            }
        ],
    },
    "home": {
        "team": {"name": "味全龍"},
        "score": 5,
        "hittingCnt": 8,
        "errorCnt": 1,
        "inningScore": [
            {"seq": 1, "score": "1"},
            {"seq": 3, "score": "2"},
            {"seq": 11, "score": "X"},
        ],
        "hitters": [
            {
                "hitCnt": 4,
                "scoreCnt": 2,
                "hittingCnt": 4,
                "runBattedInCnt": 1,
                "twoBaseHitCnt": 1,
                "threeBaseHitCnt": 0,
                "homeRunCnt": 1,
                "doublePlayBatCnt": 0,
                "basesOnBallsCnt": 1,
                "hitByPitchCnt": 0,
                "strikeOutCnt": 0,
                "sacrificeHitCnt": 0,
                "sacrificeFlyCnt": 0,
                "stealBaseOkCnt": 0,
                "stealBaseFailCnt": 0,
            }
        ],
        "pitchers": [
            {
                "roleType": "先發",
                "pitcherName": "蔣銲",
                "pitcherAcnt": "0000005555",
                "inningPitchedCnt": 6,
                "inningPitchedDiv3Cnt": 0,
                "plateAppearances": 22,
                "pitchCnt": 94,
                "strikeCnt": 60,
                "hittingCnt": 5,
                "homeRunCnt": 0,
                "basesOnBallsCnt": 2,
                "hitByPitchCnt": 0,
                "strikeOutCnt": 4,
                "wildPitchCnt": 0,
                "balkCnt": 0,
                "runCnt": 1,
                "earnedRunCnt": 1,
            }
        ],
    },
}


# ---------------------------------------------------------------------------
# _get_pitching_stats
# ---------------------------------------------------------------------------


class TestGetPitchingStats:
    def test_starter_name_and_acnt(self):
        _, name, acnt = _get_pitching_stats(PITCHING, 1, is_starter=True)
        assert name == "王投手"
        assert acnt == ""

    def test_starter_ip_exact_innings(self):
        stats, _, _ = _get_pitching_stats(PITCHING, 1, is_starter=True)
        assert stats[0] == 6  # 6.0 IP

    def test_total_ip_with_thirds(self):
        # Away total: 6.0 + 2.2 = 8.2 IP  (6*3+0 + 2*3+2 = 18+8 = 26 outs)
        # 26 // 3 = 8 remainder 2 → round(26/3, 3) = 8.667
        stats, _, _ = _get_pitching_stats(PITCHING, 1, is_starter=False)
        assert stats[0] == round(26 / 3, 3)

    def test_stats_sum_across_pitchers(self):
        # Away total: PA=22+8=30, pitches=88+28=116, SO=7+2=9, R=2+0=2
        stats, _, _ = _get_pitching_stats(PITCHING, 1, is_starter=False)
        assert stats[1] == 30  # PA
        assert stats[2] == 116  # pitches
        assert stats[8] == 9  # SO
        assert stats[11] == 2  # R
        assert stats[12] == 2  # ER

    def test_home_starter_ip(self):
        # Home starter: 9.0 IP (9*3 = 27 outs, divisible → 9)
        stats, name, _ = _get_pitching_stats(PITCHING, 2, is_starter=True)
        assert stats[0] == 9
        assert name == "陳先發"

    def test_no_pitchers_returns_zeros(self):
        stats, name, acnt = _get_pitching_stats([], 1, is_starter=True)
        assert stats == [0] * 13
        assert name == ""
        assert acnt == ""

    def test_wrong_ptype_returns_zeros(self):
        stats, name, _ = _get_pitching_stats(PITCHING, 9, is_starter=False)
        assert stats[0] == 0
        assert name == ""


# ---------------------------------------------------------------------------
# _get_batting_stats
# ---------------------------------------------------------------------------


class TestGetBattingStats:
    def test_away_batting(self):
        stats = _get_batting_stats(BATTING, PITCHING, 1)
        assert stats[0] == 7  # AB: 4+3
        assert stats[2] == 3  # H: 2+1
        assert stats[4] == 1  # 2B
        assert stats[13] == 1  # SB

    def test_home_batting(self):
        stats = _get_batting_stats(BATTING, PITCHING, 2)
        assert stats[0] == 7  # AB: 4+3
        assert stats[2] == 4  # H: 3+1
        assert stats[6] == 1  # HR

    def test_errors_include_pitching_errors(self):
        # Home pitcher (type=2) has ErrorCnt=1 → added to home batting stats[15]
        stats = _get_batting_stats(BATTING, PITCHING, 2)
        assert stats[15] == 1


# ---------------------------------------------------------------------------
# stats.cpbl.com.tw adapter
# ---------------------------------------------------------------------------


class TestStatsCpblAdapter:
    def test_extracts_game_from_next_flight_payload(self):
        html = _stats_html(STATS_GAME)
        game = _extract_stats_game(html, "2026-A-81")
        assert game["gameId"] == "2026-A-81"
        assert game["visiting"]["team"]["name"] == "中信兄弟"

    def test_converts_stats_game_to_legacy_payload(self):
        payload = _stats_game_to_legacy_payload(STATS_GAME)
        detail = json.loads(payload["GameDetailJson"])[0]
        pitching = json.loads(payload["PitchingJson"])
        batting = json.loads(payload["BattingJson"])

        assert detail["GameSno"] == "81"
        assert detail["GameStatusChi"] == "比賽結束"
        assert detail["GameDate"] == "2026-05-05T18:35:00"
        assert detail["FieldAbbe"] == "大巨蛋"
        assert pitching[0]["PitcherName"] == "鄭浩均"
        assert pitching[0]["HitBYPitchCnt"] == "2"
        assert batting[0]["RunBattedINCnt"] == "1"

    def test_process_writes_stats_payload_and_preserves_x(self):
        payload = _stats_game_to_legacy_payload(STATS_GAME)
        sheet = _make_sheet()
        result = process_and_update_sheet(payload, "81", "2026", "A", None, sheet)

        assert result is True
        values = sheet.update.call_args[1]["values"][0]
        assert values[1] == "81"
        assert values[2] == "2026-05-05"
        assert values[3] == "中信兄弟"
        assert values[5] == "味全"
        assert values[7] == "大巨蛋"
        assert values[21] == 3
        assert values[22] == 3
        assert values[36] == 5
        assert values[38] == 1
        assert values[34] == 2
        assert values[91] == ""

    def test_stats_pitching_habit_from_player_meta_description(self):
        response = MagicMock()
        response.status_code = 200
        response.text = (
            '<html><head><meta name="description" '
            'content="中信兄弟 | 投手 | 投打習慣: R | 191cm | 105kg"/>'
            "</head></html>"
        )
        session = MagicMock()
        session.get.return_value = response

        assert get_stats_pitching_habit("0000002345", session) == "右"


# ---------------------------------------------------------------------------
# process_and_update_sheet
# ---------------------------------------------------------------------------


def _make_sheet(existing_snos=None):
    """Return a mock gspread Worksheet. existing_snos: list of game_sno already recorded."""
    sheet = MagicMock()
    col_b = [""] + (existing_snos or [])  # row 1 = header placeholder
    sheet.col_values.return_value = col_b
    sheet.row_values.side_effect = lambda idx: (
        [col_b[idx - 1]] * 3 if idx <= len(col_b) else []
    )
    return sheet


def _make_data(game_detail, scoreboard, pitching=None, batting=None, curt=None):
    return {
        "CurtGameDetailJson": json.dumps(curt or {}),
        "GameDetailJson": json.dumps([game_detail]),
        "ScoreboardJson": json.dumps(scoreboard),
        "PitchingJson": json.dumps(pitching or []),
        "BattingJson": json.dumps(batting or []),
    }


GAME_DETAIL_FINISHED = {
    "GameSno": "42",
    "GameStatusChi": "比賽結束",
    "GameDate": "2025-04-15T00:00:00",
    "VisitingTeamName": "樂天桃猿",
    "HomeTeamName": "中信兄弟",
    "FieldAbbe": "天母",
    "HeadUmpire": "裁判甲",
    "VisitingTotalScore": 4,
    "HomeTotalScore": 2,
}

# Away wins 4-2: innings away=[2,0,0,1,1,0,0,0,0], home=[1,0,1,0,0,0,0,0,0]
SCOREBOARD_AWAY_WIN = [
    {"VisitingHomeType": "1", "InningSeq": str(i + 1), "ScoreCnt": str(s)}
    for i, s in enumerate([2, 0, 0, 1, 1, 0, 0, 0, 0])
] + [
    {"VisitingHomeType": "2", "InningSeq": str(i + 1), "ScoreCnt": str(s)}
    for i, s in enumerate([1, 0, 1, 0, 0, 0, 0, 0, 0])
]


class TestProcessAndUpdateSheet:
    def test_game_not_finished_returns_false(self):
        detail = {**GAME_DETAIL_FINISHED, "GameStatusChi": "比賽中"}
        data = _make_data(detail, [])
        sheet = _make_sheet()
        result = process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        assert result is False
        sheet.update.assert_not_called()

    def test_already_recorded_skips_update(self):
        # Simulate game 42 already in sheet col B
        sheet = MagicMock()
        sheet.col_values.return_value = ["", "42"]
        sheet.row_values.return_value = ["", "42", "2025-04-15"]
        data = _make_data(GAME_DETAIL_FINISHED, SCOREBOARD_AWAY_WIN)
        result = process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        assert result is True
        sheet.update.assert_not_called()

    def test_no_game_detail_returns_false(self):
        data = {
            "CurtGameDetailJson": "{}",
            "GameDetailJson": "[]",
            "ScoreboardJson": "[]",
            "PitchingJson": "[]",
            "BattingJson": "[]",
        }
        sheet = _make_sheet()
        result = process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        assert result is False

    def test_successful_write_returns_true(self):
        data = _make_data(GAME_DETAIL_FINISHED, SCOREBOARD_AWAY_WIN, PITCHING, BATTING)
        sheet = _make_sheet()
        result = process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        assert result is True
        sheet.update.assert_called_once()

    def test_column_layout(self):
        """Spot-check that key columns land in the right positions."""
        data = _make_data(GAME_DETAIL_FINISHED, SCOREBOARD_AWAY_WIN, PITCHING, BATTING)
        sheet = _make_sheet()
        process_and_update_sheet(data, "42", "2025", "A", None, sheet)

        call_kwargs = sheet.update.call_args
        values = call_kwargs[1]["values"][0]

        assert values[0] == "比賽結束"  # col A: status
        assert values[1] == "42"  # col B: game_sno
        assert values[2] == "2025-04-15"  # col C: date
        assert values[3] == "樂天"  # col D: away team (mapped)
        assert values[5] == "中信兄弟"  # col F: home team (mapped)
        assert values[7] == "天母"  # col H: field
        assert values[8] == "裁判甲"  # col I: umpire
        assert values[21] == 4  # col V: away total score
        assert values[36] == 2  # col AK: home total score

    def test_inning_scores_away(self):
        """Away inning scores land at cols J-R (index 9-17)."""
        data = _make_data(GAME_DETAIL_FINISHED, SCOREBOARD_AWAY_WIN, PITCHING, BATTING)
        sheet = _make_sheet()
        process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        values = sheet.update.call_args[1]["values"][0]
        assert values[9] == 2  # inning 1
        assert values[10] == 0  # inning 2
        assert values[13] == 1  # inning 5

    def test_inning_scores_home(self):
        """Home inning scores land at cols Y-AG (index 24-32)."""
        data = _make_data(GAME_DETAIL_FINISHED, SCOREBOARD_AWAY_WIN, PITCHING, BATTING)
        sheet = _make_sheet()
        process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        values = sheet.update.call_args[1]["values"][0]
        assert values[24] == 1  # home inning 1
        assert values[25] == 0  # home inning 2
        assert values[26] == 1  # home inning 3


class TestWalkoffX:
    """Home team walk-off: bottom of 9th should show 'X'."""

    # Home wins 3-2 without needing to bat in the 9th
    # Away: 0,1,0,0,0,1,0,0,0 = 2
    # Home: 1,0,1,1,0,0,0,0   = 3  (no bottom-9th entry from CPBL, but let's include 0)
    GAME_DETAIL = {
        **GAME_DETAIL_FINISHED,
        "VisitingTotalScore": 2,
        "HomeTotalScore": 3,
    }
    SCOREBOARD = [
        {"VisitingHomeType": "1", "InningSeq": str(i + 1), "ScoreCnt": str(s)}
        for i, s in enumerate([0, 1, 0, 0, 0, 1, 0, 0, 0])
    ] + [
        {"VisitingHomeType": "2", "InningSeq": str(i + 1), "ScoreCnt": str(s)}
        for i, s in enumerate([1, 0, 1, 1, 0, 0, 0, 0, 0])
    ]

    def test_walkoff_ninth_shows_x(self):
        data = _make_data(self.GAME_DETAIL, self.SCOREBOARD)
        sheet = _make_sheet()
        process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        values = sheet.update.call_args[1]["values"][0]
        # Home inning 9 = index 24+8 = 32
        assert values[32] == "X"

    def test_regular_inning_not_replaced(self):
        data = _make_data(self.GAME_DETAIL, self.SCOREBOARD)
        sheet = _make_sheet()
        process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        values = sheet.update.call_args[1]["values"][0]
        # Home inning 1 (index 24) should be 1, not X
        assert values[24] == 1


# ---------------------------------------------------------------------------
# is_game_recorded
# ---------------------------------------------------------------------------


class TestIsGameRecorded:
    def _make_sheet(self, col_b, rows_by_idx):
        sheet = MagicMock()
        sheet.col_values.return_value = col_b
        sheet.row_values.side_effect = lambda idx: rows_by_idx.get(idx, [])
        return sheet

    def test_found_matching_game_and_year(self):
        sheet = self._make_sheet(["", "42"], {2: ["", "42", "2025-04-15"]})
        assert is_game_recorded("42", "2025", sheet) is True

    def test_not_found_in_col(self):
        sheet = self._make_sheet(["", "10", "20"], {})
        assert is_game_recorded("42", "2025", sheet) is False

    def test_found_but_different_year(self):
        sheet = self._make_sheet(["", "42"], {2: ["", "42", "2024-04-15"]})
        assert is_game_recorded("42", "2025", sheet) is False

    def test_empty_sheet(self):
        sheet = self._make_sheet([], {})
        assert is_game_recorded("42", "2025", sheet) is False

    def test_row_too_short_returns_false(self):
        # row has fewer than 3 values → year check never reached
        sheet = self._make_sheet(["", "42"], {2: ["42"]})
        assert is_game_recorded("42", "2025", sheet) is False


# ---------------------------------------------------------------------------
# process_and_update_sheet — extra branches
# ---------------------------------------------------------------------------


class TestProcessCurtGameDetailMatch:
    """CurtGameDetailJson contains the matching game SNO (line 255 branch)."""

    def test_curt_match_is_used_and_writes(self):
        curt = {**GAME_DETAIL_FINISHED, "GameSno": "42"}
        data = {
            "CurtGameDetailJson": json.dumps(curt),
            "GameDetailJson": json.dumps([]),
            "ScoreboardJson": json.dumps(SCOREBOARD_AWAY_WIN),
            "PitchingJson": json.dumps([]),
            "BattingJson": json.dumps([]),
        }
        sheet = _make_sheet()
        result = process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        assert result is True
        sheet.update.assert_called_once()

    def test_curt_match_status_written(self):
        curt = {**GAME_DETAIL_FINISHED, "GameSno": "42"}
        data = {
            "CurtGameDetailJson": json.dumps(curt),
            "GameDetailJson": json.dumps([]),
            "ScoreboardJson": json.dumps(SCOREBOARD_AWAY_WIN),
            "PitchingJson": json.dumps([]),
            "BattingJson": json.dumps([]),
        }
        sheet = _make_sheet()
        process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        values = sheet.update.call_args[1]["values"][0]
        assert values[0] == "比賽結束"


class TestProcessFallbackToFirstGame:
    """When no game matches game_sno, falls back to first entry in GameDetailJson."""

    def test_fallback_writes_first_game(self):
        # Data contains SNO "99", but we request "42"
        detail_99 = {**GAME_DETAIL_FINISHED, "GameSno": "99"}
        data = {
            "CurtGameDetailJson": json.dumps({}),
            "GameDetailJson": json.dumps([detail_99]),
            "ScoreboardJson": json.dumps(SCOREBOARD_AWAY_WIN),
            "PitchingJson": json.dumps([]),
            "BattingJson": json.dumps([]),
        }
        sheet = _make_sheet()
        result = process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        # Falls back to first game (detail_99) which is "比賽結束", so writes
        assert result is True
        sheet.update.assert_called_once()

    def test_fallback_not_finished_returns_false(self):
        detail = {**GAME_DETAIL_FINISHED, "GameSno": "99", "GameStatusChi": "比賽中"}
        data = {
            "CurtGameDetailJson": json.dumps({}),
            "GameDetailJson": json.dumps([detail]),
            "ScoreboardJson": json.dumps([]),
            "PitchingJson": json.dumps([]),
            "BattingJson": json.dumps([]),
        }
        sheet = _make_sheet()
        result = process_and_update_sheet(data, "42", "2025", "A", None, sheet)
        assert result is False
