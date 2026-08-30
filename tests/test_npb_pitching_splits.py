"""Unit tests for the team pitching split (baseball/npb_pitching_splits.py).

Covers: reading a 分析表紀錄 row into two sides, deriving the bullpen from the
team line, ERA over thirds of an inning, and ranking within a league.
"""

import pytest

from baseball.npb_pitching_splits import (
    BULLPEN,
    HEADERS,
    LEAGUES,
    ROW_DATA,
    ROW_HEADER,
    ROW_SECTION,
    ROW_TITLE,
    STARTER,
    TEAM,
    accumulate,
    build_sheet,
    era,
    game_sides,
    rank_within,
    row_roles,
)


def _row(away="阪神", home="巨人", *, away_starter=(6, 3), away_team=(8, 3),
         home_starter=(6, 1), home_team=(9, 1)):
    """One 分析表紀錄 row, filled only where the split reads it."""
    row = [""] * 83
    row[1] = "2026/3/27"
    row[8], row[11] = away, home
    row[34], row[40] = away_starter
    row[43], row[51] = away_team
    row[59], row[65] = home_starter
    row[68], row[76] = home_team
    return row


# --- Reading a game -------------------------------------------------------


def test_a_game_yields_one_side_per_team():
    away, home = game_sides(_row())

    assert (away["team"], away["venue"]) == ("阪神", "客")
    assert (home["team"], home["venue"]) == ("巨人", "主")


def test_the_bullpen_is_what_the_starter_did_not_pitch():
    """NPB publishes no relief total; 分析表紀錄 has the starter's line and the
    team's, and the difference is the bullpen."""
    away, _ = game_sides(_row(away_starter=(6, 3), away_team=(8, 5)))

    assert away[STARTER] == (6.0, 3.0)
    assert away[BULLPEN] == (2.0, 2.0)
    assert away[TEAM] == (8.0, 5.0)


def test_a_complete_game_leaves_the_bullpen_untouched():
    _, home = game_sides(_row(home_starter=(9, 2), home_team=(9, 2)))

    assert home[BULLPEN] == (0.0, 0.0)


def test_thirds_of_an_inning_survive_the_subtraction():
    """分析表紀錄 writes innings as decimal thirds — 5.3333 is 5⅓."""
    away, _ = game_sides(_row(away_starter=(5.3333, 2), away_team=(8, 4)))

    innings, runs = away[BULLPEN]
    assert innings == pytest.approx(2.6667, abs=1e-3)
    assert runs == 2.0


def test_a_row_without_teams_is_not_a_game():
    assert game_sides([""] * 83) is None


# --- ERA ------------------------------------------------------------------


def test_era_is_earned_runs_over_nine_innings():
    assert era(9.0, 3.0) == pytest.approx(3.0)
    assert era(6.6667, 2.0) == pytest.approx(2.70, abs=0.01)


def test_no_innings_has_no_era_rather_than_a_zero():
    """A bullpen that has not pitched is not a perfect bullpen."""
    assert era(0.0, 0.0) is None


# --- Aggregation ----------------------------------------------------------


def test_innings_and_runs_accumulate_by_team_segment_and_venue():
    rows = [_row(away_starter=(6, 3), away_team=(8, 3)),
            _row(away_starter=(5, 2), away_team=(8, 4))]

    totals = accumulate(rows)

    assert totals[("阪神", STARTER, "客")] == (11.0, 5.0)
    assert totals[("阪神", BULLPEN, "客")] == (5.0, 2.0)


# --- Ranking --------------------------------------------------------------


def test_the_lowest_era_ranks_first():
    assert rank_within({"A": 3.50, "B": 2.10, "C": 4.00}) == {"B": 1, "A": 2, "C": 3}


def test_teams_level_on_era_share_a_rank_and_the_next_is_skipped():
    ranks = rank_within({"A": 3.00, "B": 3.00, "C": 4.00})

    assert ranks["A"] == ranks["B"] == 1
    assert ranks["C"] == 3


def test_a_team_with_no_innings_is_not_ranked():
    assert rank_within({"A": 3.00, "B": None}) == {"A": 1}


# --- Sheet payload --------------------------------------------------------


def test_the_two_leagues_are_kept_apart():
    assert LEAGUES["巨人"] == "央聯" and LEAGUES["西武"] == "洋聯"
    assert len([t for t, lg in LEAGUES.items() if lg == "央聯"]) == 6
    assert len([t for t, lg in LEAGUES.items() if lg == "洋聯"]) == 6


def test_the_sheet_carries_three_sections_and_both_leagues():
    values = build_sheet([_row()], updated_at="2026-08-30 15:00")
    text = "\n".join("\t".join(str(cell) for cell in row) for row in values)

    assert "【先發投手】" in text and "【中繼投手】" in text and "【投手總計】" in text
    assert "央聯" in text and "洋聯" in text
    assert "2026-08-30 15:00" in text


def test_every_team_appears_in_every_section_even_with_no_games():
    """A missing team reads as a data problem; a blank ERA reads as no innings."""
    values = build_sheet([_row()], updated_at="")
    teams = [row[1] for row in values if len(row) > 1 and row[1] in LEAGUES]

    assert len(teams) == 36                      # 12 teams x 3 sections
    assert teams.count("オリックス") == 3


# --- Row roles, which is what the formatter paints against ----------------


def test_every_row_is_classified_for_the_formatter():
    values = build_sheet([_row()], updated_at="")

    roles = row_roles(values)

    assert len(roles) == len(values)
    assert roles[0] == ROW_TITLE
    assert roles.count(ROW_SECTION) == 3
    assert roles.count(ROW_HEADER) == 3
    assert roles.count(ROW_DATA) == 36          # 12 teams x 3 sections


def test_a_header_row_is_not_mistaken_for_data():
    """Both start in column A with a short string; only one of them is 聯盟."""
    values = build_sheet([_row()], updated_at="")
    header = next(i for i, role in enumerate(row_roles(values)) if role == ROW_HEADER)

    assert values[header] == HEADERS
    assert row_roles(values)[header + 1] == ROW_DATA
