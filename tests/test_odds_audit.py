"""Unit tests for the 盤口 coherence audit (baseball/odds_audit.py)."""

import json

from baseball import odds_audit as audit


def _ladder(rows):
    return json.dumps([{"hdp": h, "home": ho} for h, ho in rows], ensure_ascii=False)


# 2026-08-22 オリックス @ ソフトバンク as the sheet now holds it, repaired.
SOFTBANK = [(-2.0, 2.18), (-1.5, 1.917), (1.5, 1.259)]
# 2026-09-13 巨人 @ DeNA, a pick'em game written after the parser was fixed.
DENA = [(-2.0, 3.75), (-1.5, 2.83), (1.5, 1.523)]


def test_a_moneyline_inside_the_bracket_is_sound():
    """DeNA: receiving 1.5 costs 1.523, laying it costs 2.83, and the
    moneyline sits between them at 1.909. Nothing to report."""
    assert audit.check_row(_ladder(DENA), "1.909") is None


def test_a_moneyline_longer_than_the_laying_price_is_flagged():
    """ソフトバンク: the ladder says laying 1.5 costs 1.917, so winning
    outright cannot cost 2.9 — laying runs is the harder bet."""
    bad = audit.check_row(_ladder(SOFTBANK), "2.9")
    assert bad is not None
    assert bad["why"] == "讓分卻比錢線便宜"
    assert bad["hdp"] == -2.0


def test_a_moneyline_shorter_than_the_receiving_price_is_flagged():
    """The mirror: receiving 1.5 costs 1.259, so the outright price cannot be
    shorter than that."""
    bad = audit.check_row(_ladder(SOFTBANK), "1.10")
    assert bad is not None
    assert bad["why"] == "受讓卻比錢線貴"


def test_the_real_softbank_moneyline_is_the_contradiction_we_found_by_hand():
    assert audit.check_row(_ladder(SOFTBANK), "1.454") is None   # the true price
    assert audit.check_row(_ladder(SOFTBANK), "2.9") is not None  # what we stored


def test_a_rung_just_past_the_moneyline_is_within_tolerance():
    """The ladder and the moneyline are not always the same instant, so a
    hair's breadth either way is not a contradiction."""
    assert audit.check_row(_ladder([(1.5, 1.92)]), "1.90") is None
    assert audit.check_row(_ladder([(1.5, 2.40)]), "1.90") is not None


def test_rows_that_cannot_speak_twice_are_skipped():
    assert audit.check_row(_ladder(SOFTBANK), "") is None
    assert audit.check_row("[]", "1.8") is None
    assert audit.check_row("not json", "1.8") is None


def test_every_rung_is_checked_not_just_the_main_line():
    """A single bad rung is enough; the alternates are where a flipped sign
    shows up most clearly."""
    ladder = _ladder([(-1.5, 1.917), (2.5, 3.90)])
    assert audit.check_row(ladder, "1.80")["hdp"] == 2.5


# --- guarding the scraper -------------------------------------------------

def _snapshot(**over):
    snap = {"period": "final", "ml_home": 1.909, "ml_away": 1.98,
            "all_spreads": [{"hdp": h, "home": p} for h, p in DENA],
            "away_norm": "巨人", "home_norm": "DeNA", "game_date": "2026-09-13"}
    snap.update(over)
    return snap


def test_a_coherent_snapshot_passes():
    assert audit.incoherence(_snapshot()) is None


def test_a_ladder_running_backwards_is_refused():
    """The pre-#79 bug, caught at the moment of parsing instead of five weeks
    later: receiving runs cannot cost more than laying them."""
    backwards = [{"hdp": -h, "home": p} for h, p in DENA]
    reason = audit.incoherence(_snapshot(all_spreads=backwards))
    assert reason is not None and "階梯" in reason


def test_a_moneyline_outside_the_bracket_is_refused():
    reason = audit.incoherence(_snapshot(ml_home=2.9, ml_away=1.454))
    assert reason is not None and "錢線" in reason


def test_a_half_snapshot_is_judged_on_its_ladder_alone():
    """1st-5-innings carries no moneyline, so only the ladder can be checked."""
    assert audit.incoherence(
        _snapshot(period="half", ml_home=None, ml_away=None)) is None
    backwards = [{"hdp": -h, "home": p} for h, p in DENA]
    assert audit.incoherence(
        _snapshot(period="half", ml_home=None, ml_away=None,
                  all_spreads=backwards)) is not None


def test_a_ladder_too_short_to_judge_is_allowed_through():
    assert audit.incoherence(_snapshot(all_spreads=[{"hdp": 1.5, "home": 1.5}],
                                       ml_home=None, ml_away=None)) is None
    assert audit.incoherence(_snapshot(all_spreads=[], ml_home=None,
                                       ml_away=None)) is None
