"""Tests for migration/repair_odds_moneyline_from_pinnacle.py."""

import importlib.util
import pathlib

_PATH = pathlib.Path(__file__).resolve().parent.parent / "migration" / \
    "repair_odds_moneyline_from_pinnacle.py"
_spec = importlib.util.spec_from_file_location("repair_ml_pinnacle", _PATH)
repair = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(repair)


def test_exact_mirror_of_pinnacle_is_swapped():
    # 2026-08-19 ヤクルト @ 阪神: Pinnacle had 阪神 1.628, the sheet 2.4
    assert repair.verdict("2.4", "1.628", 1.628, 2.4) == "swap"


def test_matching_pinnacle_is_left_alone():
    assert repair.verdict("1.628", "2.4", 1.63, 2.39) == "ok"


def test_a_near_even_game_cannot_tell_the_sides_apart():
    assert repair.verdict("1.95", "1.92", 1.92, 1.95) == "ambiguous"


def test_prices_that_match_neither_way_are_not_touched():
    # the line moved between the two clocks: leave it, report it
    assert repair.verdict("2.10", "1.80", 1.55, 2.60) == "unmatched"


def test_blank_prices_are_skipped():
    assert repair.verdict("", "1.80", 1.55, 2.60) == "skip"
    assert repair.verdict("2.10", "1.80", None, 2.60) == "skip"


def test_only_rows_captured_before_the_parsing_fix_are_candidates():
    # 0faface (#79) merged 2026-08-25 20:26:37 JST; later rows were read correctly
    assert repair.before_fix("2026-08-25 20:26:36")
    assert not repair.before_fix("2026-08-25 20:26:37")
    assert not repair.before_fix("2026-09-12 14:00:00")
