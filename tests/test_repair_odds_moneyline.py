"""Unit tests for the 盤口 moneyline-side repair."""

import json

from migration import repair_odds_moneyline_sides as repair


def _ladder(rows):
    return json.dumps([{"hdp": h, "home": p} for h, p in rows], ensure_ascii=False)


# 2026-07-18 ソフトバンク @ ロッテ, ladder already repaired by the first pass.
# Receiving 1.5 costs 1.925 and laying it costs 4.58, so the home side's
# outright price has to sit between them — 1.529 does not, 2.39 does.
LADDER = [(-1.5, 4.58), (1.5, 1.925), (2.0, 1.724)]


def test_a_row_whose_moneyline_fits_the_bracket_is_left_alone():
    assert repair.plan_row(_ladder(LADDER), "2.39", "1.529", row_number=2) is None


def test_a_row_the_swap_fixes_is_planned():
    plan = repair.plan_row(_ladder(LADDER), "1.529", "2.39", row_number=2)
    assert plan is not None
    assert (plan["ml_home"], plan["ml_away"]) == ("2.39", "1.529")
    assert plan["row"] == 2


def test_a_row_the_swap_does_not_fix_is_never_touched():
    """Both orientations wrong means something else is going on, and guessing
    would bury it. Reported, not rewritten."""
    plan = repair.plan_row(_ladder(LADDER), "9.9", "8.8", row_number=2)
    assert plan is None


def test_the_swap_is_idempotent():
    once = repair.plan_row(_ladder(LADDER), "1.529", "2.39", row_number=2)
    assert repair.plan_row(_ladder(LADDER), once["ml_home"], once["ml_away"],
                           row_number=2) is None


def test_rows_without_both_prices_are_skipped():
    """Half markets carry no moneyline at all."""
    assert repair.plan_row(_ladder(LADDER), "", "", row_number=2) is None
    assert repair.plan_row(_ladder(LADDER), "1.529", "", row_number=2) is None
    assert repair.plan_row("[]", "1.529", "2.39", row_number=2) is None


def test_the_softbank_game_is_planned_the_way_we_verified_by_hand():
    """2026-08-22: bettingiscool has the home side at 1.454 and we stored 2.9."""
    ladder = _ladder([(-2.0, 2.18), (-1.5, 1.917), (1.5, 1.259)])
    plan = repair.plan_row(ladder, "2.9", "1.454", row_number=9)
    assert (plan["ml_home"], plan["ml_away"]) == ("1.454", "2.9")


def test_classify_counts_what_it_cannot_fix():
    rows = [
        (_ladder(LADDER), "2.39", "1.529"),      # sound
        (_ladder(LADDER), "1.529", "2.39"),      # swap fixes it
        (_ladder(LADDER), "9.9", "8.8"),         # neither orientation works
        (_ladder(LADDER), "", ""),               # nothing to compare
    ]
    plans, tally = repair.classify(rows)
    assert len(plans) == 1
    assert tally == {"scanned": 4, "sound": 1, "swapped": 1,
                     "unresolved": 1, "skipped": 1}
