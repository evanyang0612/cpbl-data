"""Tests for the canonical MLB team code used in 紀錄.

MLB Stats API started abbreviating the Athletics as ATH in 2025, when the club
dropped "Oakland"; 紀錄 (and every sheet that aggregates by team label) was built on
OAK. One canonical code keeps those aggregations whole across the 2024/2025 boundary.
"""

from migration.normalize_mlb_team_codes import plan_row_updates
from baseball.mlb_teams import TEAM_CODE_ALIASES, canonical_team_code


class TestCanonicalTeamCode:
    def test_athletics_collapse_onto_the_historic_code(self):
        assert canonical_team_code("ATH") == "OAK"
        assert TEAM_CODE_ALIASES == {"ATH": "OAK"}

    def test_every_other_code_is_passed_through(self):
        for code in ("OAK", "NYY", "AZ", "CWS", "WSH", "TB", "SD", "SF", "KC"):
            assert canonical_team_code(code) == code

    def test_blank_and_unknown_values_survive_untouched(self):
        assert canonical_team_code("") == ""
        assert canonical_team_code("ZZZ") == "ZZZ"

    def test_whitespace_is_not_silently_swallowed(self):
        # a padded cell is a data problem worth seeing, not one to paper over
        assert canonical_team_code(" ATH") == " ATH"


class TestPlanRowUpdates:
    def test_only_rows_holding_an_alias_are_touched(self):
        rows = [
            ["2024/9/1", "745000", "OAK", "", "", "SEA"],
            ["2025/4/1", "780000", "ATH", "", "", "SEA"],
            ["2026/5/2", "800000", "NYY", "", "", "ATH"],
        ]
        updates = plan_row_updates(rows, first_row=2, away_col=2, home_col=5)
        assert updates == [
            {"row": 3, "column": "C", "old": "ATH", "new": "OAK"},
            {"row": 4, "column": "F", "old": "ATH", "new": "OAK"},
        ]

    def test_a_row_with_both_sides_aliased_yields_two_updates(self):
        rows = [["2025/6/1", "790000", "ATH", "", "", "ATH"]]
        updates = plan_row_updates(rows, first_row=10, away_col=2, home_col=5)
        assert [u["row"] for u in updates] == [10, 10]
        assert [u["column"] for u in updates] == ["C", "F"]

    def test_short_rows_do_not_blow_up(self):
        updates = plan_row_updates([["2025/6/1"]], first_row=2, away_col=2, home_col=5)
        assert updates == []

    def test_nothing_to_do_when_already_canonical(self):
        rows = [["2024/9/1", "745000", "OAK", "", "", "SEA"]]
        assert plan_row_updates(rows, first_row=2, away_col=2, home_col=5) == []
