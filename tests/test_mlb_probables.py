"""Tests for the daily 設定 / 投手清單 filler.

Two jobs, both keyed on the same MLB Stats API schedule feed:
  * write today's announced starters into 設定, and tomorrow's into the second game
  * refresh the AL-P / NL-P dropdown lists, which had been hand-kept since 2019
"""

from migration.update_mlb_probables import (
    schedule_matchups,
    setting_writes,
    starters_by_team,
)


def _game(away, home, away_sp, home_sp, start, *, status="Scheduled", kind="R"):
    def side(code, pitcher):
        team = {"team": {"abbreviation": code}}
        if pitcher:
            team["probablePitcher"] = {"fullName": pitcher}
        return team

    return {
        "gameDate": start,
        "gameType": kind,
        "status": {"detailedState": status},
        "teams": {"away": side(away, away_sp), "home": side(home, home_sp)},
    }


class TestScheduleMatchups:
    def test_reads_both_probables_and_sorts_by_first_pitch(self):
        payload = {"dates": [{"games": [
            _game("NYY", "TOR", "Cam Schlittler", "Braydon Fisher", "2026-08-15T23:07:00Z"),
            _game("CWS", "DET", "Anthony Kay", "Troy Melton", "2026-08-15T17:10:00Z"),
        ]}]}
        games = schedule_matchups(payload)
        assert [g["away"] for g in games] == ["CWS", "NYY"]
        assert games[0]["away_starter"] == "Anthony Kay"
        assert games[0]["home_starter"] == "Troy Melton"

    def test_the_athletics_arrive_under_the_stored_code(self):
        payload = {"dates": [{"games": [
            _game("ATH", "SEA", "Jeffrey Springs", "Logan Gilbert", "2026-08-15T20:10:00Z"),
        ]}]}
        assert schedule_matchups(payload)[0]["away"] == "OAK"

    def test_unannounced_starters_come_back_blank_not_missing(self):
        payload = {"dates": [{"games": [
            _game("SD", "LAD", None, "Yoshinobu Yamamoto", "2026-08-15T20:10:00Z"),
        ]}]}
        game = schedule_matchups(payload)[0]
        assert game["away_starter"] == ""
        assert game["home_starter"] == "Yoshinobu Yamamoto"

    def test_postponed_and_exhibition_games_are_dropped(self):
        payload = {"dates": [{"games": [
            _game("NYM", "PHI", "A", "B", "2026-08-15T22:00:00Z", status="Postponed"),
            _game("BOS", "TB", "C", "D", "2026-08-15T22:00:00Z", kind="S"),
            _game("KC", "MIN", "E", "F", "2026-08-15T22:00:00Z"),
        ]}]}
        assert [g["away"] for g in schedule_matchups(payload)] == ["KC"]


class TestSettingWrites:
    def setup_method(self):
        self.today = [
            {"away": "NYY", "home": "TOR", "away_starter": "Cam Schlittler",
             "home_starter": "Braydon Fisher"},
            {"away": "CWS", "home": "DET", "away_starter": "Anthony Kay",
             "home_starter": "Troy Melton"},
        ]

    def test_first_game_carries_teams_and_starters(self):
        writes = dict(setting_writes([self.today, [], []]))
        assert writes["B4"] == "NYY"
        assert writes["C4"] == "Cam Schlittler"
        assert writes["B6"] == "TOR"
        assert writes["C6"] == "Braydon Fisher"
        # second matchup lands in the next block
        assert writes["I4"] == "CWS"
        assert writes["J6"] == "Troy Melton"

    def test_later_games_write_only_starters_so_the_mirror_survives(self):
        tomorrow = [{"away": "NYY", "home": "TOR", "away_starter": "Max Fried",
                     "home_starter": "Kevin Gausman"}]
        writes = dict(setting_writes([self.today, tomorrow, []]))
        assert writes["C11"] == "Max Fried"
        assert writes["C13"] == "Kevin Gausman"
        # B11 / B13 stay as the =B4 / =B6 formulas the sheet already holds
        assert "B11" not in writes and "B13" not in writes

    def test_a_new_series_tomorrow_leaves_that_game_blank(self):
        tomorrow = [{"away": "BOS", "home": "TOR", "away_starter": "Garrett Crochet",
                     "home_starter": "Chris Bassitt"}]
        writes = dict(setting_writes([self.today, tomorrow, []]))
        # TOR hosts someone else, so the pairing does not match column 1
        assert writes["C11"] == "" and writes["C13"] == ""

    def test_unused_columns_are_cleared_rather_than_left_stale(self):
        writes = dict(setting_writes([self.today, [], []]))
        assert writes["P4"] == "" and writes["Q4"] == ""   # third block, no game
        assert writes["P6"] == "" and writes["Q6"] == ""

    def test_more_games_than_columns_is_reported_not_squeezed(self):
        many = [{"away": f"T{i}", "home": f"H{i}", "away_starter": "x",
                 "home_starter": "y"} for i in range(17)]
        writes, overflow = setting_writes([many, [], []], report_overflow=True)
        assert len(overflow) == 2
        assert all(not a1.startswith("DD") for a1, _ in writes)


class TestStartersByTeam:
    def test_only_pitchers_who_actually_started_appear(self):
        # rows are (date, away_team, away_starter, home_team, home_starter)
        rows = [
            ("2026/4/1", "NYY", "Gerrit Cole", "TOR", "Kevin Gausman"),
            ("2026/4/2", "NYY", "Max Fried", "TOR", "Chris Bassitt"),
            ("2026/4/3", "TOR", "Kevin Gausman", "NYY", "Carlos Rodon"),
        ]
        starters = starters_by_team(rows)
        assert starters["NYY"] == ["Carlos Rodon", "Max Fried", "Gerrit Cole"]
        assert starters["TOR"] == ["Kevin Gausman", "Chris Bassitt"]

    def test_most_recent_start_wins_and_names_are_not_repeated(self):
        rows = [
            ("2026/4/1", "NYY", "Gerrit Cole", "TOR", "x"),
            ("2026/4/8", "NYY", "Max Fried", "TOR", "x"),
            ("2026/4/9", "NYY", "Gerrit Cole", "TOR", "x"),
        ]
        assert starters_by_team(rows)["NYY"] == ["Gerrit Cole", "Max Fried"]

    def test_list_is_capped_to_what_the_column_holds(self):
        rows = [("2026/4/1", "NYY", f"P{i}", "TOR", "x") for i in range(40)]
        assert len(starters_by_team(rows, limit=29)["NYY"]) == 29

    def test_blank_cells_are_skipped(self):
        rows = [("2026/4/1", "", "", "TOR", "Kevin Gausman"),
                ("2026/4/2", "NYY", "", "", "")]
        assert starters_by_team(rows) == {"TOR": ["Kevin Gausman"]}
