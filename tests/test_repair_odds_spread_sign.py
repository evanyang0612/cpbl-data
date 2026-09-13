"""Unit tests for the 盤口 handicap-sign repair (migration/repair_odds_spread_sign.py)."""

import json

from migration import repair_odds_spread_sign as repair


def _ladder(pairs):
    """[(hdp, home price)] -> the JSON the sheet stores in all_spreads."""
    return json.dumps([{"hdp": h, "home": p, "away": round(3.2 - p, 3)}
                       for h, p in pairs], ensure_ascii=False)


# Taken from the sheet: 2026-08-22 オリックス @ ソフトバンク, written before the
# parser was fixed. Negating it reproduces bettingiscool's own record of the
# same board to within five seconds of line drift (-2.0 2.19, -1.5 1.925,
# +1.5 1.263).
BROKEN = [(-1.5, 1.259), (1.5, 1.917), (2.0, 2.18)]
# 2026-09-13 巨人 @ DeNA, written after the fix.
SOUND = [(-2.0, 3.75), (-1.5, 2.83), (1.5, 1.523)]


def test_a_ladder_that_rises_with_the_handicap_is_broken():
    """Receiving runs can only ever make a price shorter. A ladder that gets
    longer as the team receives more is physically impossible, so this needs
    no reference to dates or to the moneyline."""
    assert repair.needs_flip(_ladder(BROKEN)) is True


def test_a_ladder_that_falls_with_the_handicap_is_left_alone():
    assert repair.needs_flip(_ladder(SOUND)) is False


def test_one_rung_cannot_be_judged_and_is_never_touched():
    """372 rows carry a single rung; there is no direction to read off one
    point, and guessing would be worse than leaving them."""
    assert repair.needs_flip(_ladder([(1.5, 1.9)])) is False
    assert repair.needs_flip("[]") is False
    assert repair.needs_flip("") is False


def test_a_flat_ladder_is_left_alone():
    assert repair.needs_flip(_ladder([(-1.5, 1.9), (1.5, 1.9)])) is False


def test_flipping_negates_the_handicap_and_leaves_every_price_alone():
    before = json.loads(_ladder(BROKEN))
    after = json.loads(repair.flip_ladder(_ladder(BROKEN)))
    assert [r["hdp"] for r in after] == [1.5, -1.5, -2.0]
    assert [r["home"] for r in after] == [r["home"] for r in before]
    assert [r["away"] for r in after] == [r["away"] for r in before]


def test_flipping_is_idempotent_because_the_result_is_no_longer_broken():
    """A second pass over a repaired sheet must find nothing to do."""
    once = repair.flip_ladder(_ladder(BROKEN))
    assert repair.needs_flip(once) is False


def test_the_main_line_column_is_negated_to_match():
    # Sheets stores 4.0 as "4", so the rewrite matches its own formatting.
    row = repair.plan_row(_ladder(BROKEN), "2.0", row_number=7)
    assert row["spread_hdp"] == "-2"
    assert json.loads(row["all_spreads"])[0]["hdp"] == 1.5
    assert row["row"] == 7


def test_a_sound_row_yields_no_plan():
    assert repair.plan_row(_ladder(SOUND), "-1.5", row_number=7) is None


def test_the_repaired_ladder_matches_the_independent_record():
    """bettingiscool captured the same board five seconds later; after the
    flip the two agree rung for rung."""
    theirs = {-2.0: 2.19, -1.5: 1.925, 1.5: 1.263}
    ours = {r["hdp"]: r["home"]
            for r in json.loads(repair.flip_ladder(_ladder(BROKEN)))}
    assert ours.keys() == theirs.keys()
    for hdp, price in theirs.items():
        assert abs(ours[hdp] - price) < 0.02, hdp
