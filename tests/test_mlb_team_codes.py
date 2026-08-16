"""Tests for the canonical MLB team code used in 紀錄.

MLB Stats API started abbreviating the Athletics as ATH in 2025, when the club
dropped "Oakland"; 紀錄 (and every sheet that aggregates by team label) was built on
OAK. One canonical code keeps those aggregations whole across the 2024/2025 boundary.
"""

import pytest

from migration.normalize_mlb_team_codes import (
    TARGETS,
    plan_row_updates,
    resolve_columns,
)
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
        updates = plan_row_updates(rows, first_row=2, columns=[2, 5])
        assert updates == [
            {"row": 3, "column": "C", "old": "ATH", "new": "OAK"},
            {"row": 4, "column": "F", "old": "ATH", "new": "OAK"},
        ]

    def test_a_row_with_both_sides_aliased_yields_two_updates(self):
        rows = [["2025/6/1", "790000", "ATH", "", "", "ATH"]]
        updates = plan_row_updates(rows, first_row=10, columns=[2, 5])
        assert [u["row"] for u in updates] == [10, 10]
        assert [u["column"] for u in updates] == ["C", "F"]

    def test_short_rows_do_not_blow_up(self):
        updates = plan_row_updates([["2025/6/1"]], first_row=2, columns=[2, 5])
        assert updates == []

    def test_nothing_to_do_when_already_canonical(self):
        rows = [["2024/9/1", "745000", "OAK", "", "", "SEA"]]
        assert plan_row_updates(rows, first_row=2, columns=[2, 5]) == []


class TestResolveColumns:
    def test_odds_columns_are_found_by_header_name(self):
        # 盤口's layout has shifted before, so the columns are looked up, not hardcoded
        header = ["captured_at", "snapshot_type", "event_id", "game_date", "start_et",
                  "league", "home_team", "away_team", "home_norm", "away_norm",
                  "home_abbr", "away_abbr"]
        assert resolve_columns(header, ["home_abbr", "away_abbr"]) == [10, 11]

    def test_a_missing_column_is_reported_rather_than_guessed(self):
        with pytest.raises(KeyError, match="away_abbr"):
            resolve_columns(["home_abbr"], ["home_abbr", "away_abbr"])


class TestTargets:
    def test_both_sheets_holding_team_codes_are_covered(self):
        titles = [t.title for t in TARGETS]
        assert titles == ["紀錄", "盤口"]
        # 紀錄's team columns are fixed; 盤口's are resolved from its header row
        record, odds = TARGETS
        assert record.columns == [2, 17] and record.header_names is None
        assert odds.header_names == ["home_abbr", "away_abbr"] and odds.columns is None
