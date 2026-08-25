"""Unit tests for the Taiwanese Asian-line converter (baseball/asian_lines.py).

The fixtures below are built backwards from a known margin distribution with no
bookmaker margin, so every expected water level can be derived by hand.
"""

import pytest

from baseball import asian_lines as al

# A home team whose run-margin distribution is fixed by construction:
#
#   P(home wins by 2+)  = 0.40      P(home wins by exactly 1) = 0.15
#   P(away wins by 1)   = 0.15      P(away wins by 2+)        = 0.30
#
# so the cumulative curve is G(2) = 0.40, G(1) = G(0) = 0.55, G(-1) = 0.70.
FAIR_SPREADS = [
    # home -1.5: covers iff margin >= 2 -> 0.40
    {"hdp": -1.5, "home": 1 / 0.40, "away": 1 / 0.60},
    # home +1.5: covers iff margin >= -1 -> 0.70
    {"hdp": 1.5, "home": 1 / 0.70, "away": 1 / 0.30},
]
FAIR_ML_HOME, FAIR_ML_AWAY = 1 / 0.55, 1 / 0.45

# Totals: H(8) = 0.45, P(total == 7) = 0.12, so H(7) = 0.57.
FAIR_TOTALS = [
    {"line": 7.5, "over": 1 / 0.45, "under": 1 / 0.55},
    {"line": 6.5, "over": 1 / 0.57, "under": 1 / 0.43},
]


def test_devig_removes_the_overround_proportionally():
    assert al.devig(2.0, 2.0) == pytest.approx((0.5, 0.5))
    # A 5% overround splits in proportion to the raw implied probabilities.
    p_home, p_away = al.devig(1.5, 3.0)
    assert p_home == pytest.approx(2 / 3)
    assert p_home + p_away == pytest.approx(1.0)


def test_margin_curve_reads_half_lines_and_the_moneyline():
    curve = al.margin_curve(FAIR_SPREADS, FAIR_ML_HOME, FAIR_ML_AWAY)
    assert curve.at(2) == pytest.approx(0.40)
    assert curve.at(1) == pytest.approx(0.55)
    assert curve.at(-1) == pytest.approx(0.70)


def test_margin_curve_solves_whole_lines_against_a_known_neighbour():
    """A whole handicap pushes on the exact margin, so it only pins a ratio.

    home -1.0 wins iff margin >= 2 (0.40), pushes on exactly 1, and loses
    otherwise (1 - G(1) = 0.45), so its no-push price is 0.40 / 0.85. Given
    G(1) from the moneyline, that ratio is enough to recover G(2).
    """
    curve = al.margin_curve(
        [{"hdp": -1.0, "home": 0.85 / 0.40, "away": 0.85 / 0.45}],
        FAIR_ML_HOME, FAIR_ML_AWAY,
    )
    assert curve.at(2) == pytest.approx(0.40)


def test_home_laying_one_run_prices_the_exact_margin_case():
    """X = [P(lose) - P(win)] / P(push) = (0.45 - 0.40) / 0.15."""
    curve = al.margin_curve(FAIR_SPREADS, FAIR_ML_HOME, FAIR_ML_AWAY)
    assert al.water(curve, "home", 1) == pytest.approx(1 / 3)


def test_away_laying_one_run_uses_the_mirrored_curve():
    """The away dog laying a run: (0.55 - 0.30) / 0.15."""
    curve = al.margin_curve(FAIR_SPREADS, FAIR_ML_HOME, FAIR_ML_AWAY)
    assert al.water(curve, "away", 1) == pytest.approx(5 / 3)


def test_quote_lays_the_favourite_and_rounds_to_the_nearest_five():
    curve = al.margin_curve(FAIR_SPREADS, FAIR_ML_HOME, FAIR_ML_AWAY)
    quote = al.handicap_quote(curve)
    assert quote.side == "home"      # G(1) = 0.55
    assert quote.line == 1
    assert quote.water == pytest.approx(1 / 3)
    assert quote.text() == "1+35"


def test_quote_falls_back_to_pk_when_the_favourite_is_barely_favoured():
    """A coinflip cannot lay a whole run: the water would exceed a full unit."""
    # G(2) = 0.36, G(1) = 0.51: water = (0.49 - 0.36) / 0.15 = 0.87.
    even = [
        {"hdp": -1.5, "home": 1 / 0.36, "away": 1 / 0.64},
        {"hdp": 1.5, "home": 1 / 0.66, "away": 1 / 0.34},
    ]
    curve = al.margin_curve(even, 1 / 0.51, 1 / 0.49)
    quote = al.handicap_quote(curve, pk_above=0.85)
    assert quote.line == 0
    assert quote.text() == "PK"


def test_heavy_favourite_moves_up_the_ladder():
    """Laying one run is far too cheap for a big favourite to be quoted at.

    G(1) = 0.72, G(2) = 0.58, G(3) = 0.49: at a one-run line the water is
    (0.28 - 0.58) / 0.14 = -2.14, which no book posts, while two runs costs
    (0.42 - 0.49) / 0.09 = -0.78 and is perfectly postable. A half line is not
    reached for while a whole number still works.
    """
    curve = al.margin_curve(
        [{"hdp": -1.5, "home": 1 / 0.58, "away": 1 / 0.42},
         {"hdp": -2.5, "home": 1 / 0.49, "away": 1 / 0.51}],
        1 / 0.72, 1 / 0.28,
    )
    assert al.water(curve, "home", 1) == pytest.approx(-30 / 14)
    quote = al.handicap_quote(curve)
    assert quote.line == 2
    assert quote.text() == "2-80"


def test_a_whole_line_carries_water_when_no_half_line_is_even():
    """Same shape of game, but with 2.5 clearly lopsided the line has to be a
    whole number and absorb the difference into the push."""
    curve = al.margin_curve(
        [{"hdp": -1.5, "home": 1 / 0.58, "away": 1 / 0.42},
         {"hdp": -2.5, "home": 1 / 0.40, "away": 1 / 0.60}],
        1 / 0.72, 1 / 0.28,
    )
    quote = al.handicap_quote(curve)
    assert quote.line == 2
    assert quote.water == pytest.approx((0.42 - 0.40) / 0.18)


def test_pick_em_market_quotes_pk_without_a_favourite_run_line():
    """A dead-even moneyline gets no run line quoted at all, only the dog's.

    PS3838 stops posting the negative side of the ladder when there is no
    favourite, so G(2) is simply absent — but the answer is still PK.
    """
    curve = al.margin_curve(
        [{"hdp": 1.5, "home": 1 / 0.68, "away": 1 / 0.32},
         {"hdp": 1.0, "home": 0.83 / 0.50, "away": 0.83 / 0.33}],
        1.961, 1.961,
    )
    assert curve.at(2) is None
    quote = al.handicap_quote(curve)
    assert quote is not None and quote.text() == "PK"


def test_unpriceable_ladder_on_a_real_favourite_returns_nothing():
    """Not knowing the line is different from knowing it is a pick'em."""
    curve = al.margin_curve([{"hdp": 1.5, "home": 1 / 0.80, "away": 1 / 0.20}],
                            1 / 0.62, 1 / 0.38)
    assert al.handicap_quote(curve) is None


def test_total_quote_prices_the_whole_number_from_the_over_side():
    """X = [P(under) - P(over)] / P(push) = (0.43 - 0.45) / 0.12."""
    curve = al.total_curve(FAIR_TOTALS)
    quote = al.total_quote(curve)
    assert quote.line == 7
    assert quote.water == pytest.approx(-1 / 6)
    assert quote.text() == "7-15"


def test_total_quote_picks_the_whole_number_with_the_flattest_water():
    """Two candidate whole numbers: the book posts the one nearest to fair."""
    curve = al.total_curve([
        {"line": 7.5, "over": 1 / 0.30, "under": 1 / 0.70},
        {"line": 6.5, "over": 1 / 0.44, "under": 1 / 0.56},
        {"line": 5.5, "over": 1 / 0.62, "under": 1 / 0.38},
    ])
    # Line 6 water = (0.38 - 0.44) / 0.18 = -0.33; line 7 = (0.56 - 0.30)/0.14 = +1.86.
    assert al.total_quote(curve).line == 6


def test_water_formatting_matches_the_handwritten_sheet():
    assert al.format_water(0.70) == "+70"
    assert al.format_water(-0.30) == "-30"
    assert al.format_water(0.0) == "平"   # a line needing no compensation
    assert al.format_water(0.462) == "+45"   # rounds to the nearest five


def test_curve_returns_none_when_a_line_cannot_be_derived():
    """Only a -1.5 line: nothing pins G(-1), so the away quote is unavailable."""
    curve = al.margin_curve([FAIR_SPREADS[0]], FAIR_ML_HOME, FAIR_ML_AWAY)
    assert curve.at(-1) is None
    assert al.water(curve, "away", 1) is None


def test_a_half_line_is_posted_when_the_whole_numbers_are_badly_placed():
    """PS3838 priced 6.5 at 1.925 / 1.925 — dead even — while both 6 and 7
    needed a full unit of water. The sheet writes that game 6.5.

    A half line cannot push, so it carries no water at all; it is only
    postable when the two sides are near even, which is exactly the case a
    heavy whole-number water is pointing at.
    """
    curve = al.total_curve([
        {"line": 7.0, "over": 2.16, "under": 1.719},
        {"line": 6.5, "over": 1.925, "under": 1.925},
        {"line": 6.0, "over": 1.751, "under": 2.12},
    ])
    quote = al.total_quote(curve)
    assert quote.line == 6.5
    assert quote.water is None
    assert quote.text() == "6.5"


def test_a_whole_line_still_wins_when_it_sits_nearer_the_middle():
    """Half lines are not automatically better — the flattest candidate wins."""
    curve = al.total_curve(FAIR_TOTALS)   # H(8) = .45, H(7) = .57
    quote = al.total_quote(curve)
    assert quote.line == 7
    assert quote.text() == "7-15"


def test_a_handicap_can_be_posted_as_a_half_run():
    """G(2) = 0.50 makes "lay 1.5" an even bet, so it needs no water."""
    curve = al.margin_curve(
        [{"hdp": -1.5, "home": 2.0, "away": 2.0},
         {"hdp": -2.5, "home": 1 / 0.30, "away": 1 / 0.70}],
        1 / 0.66, 1 / 0.34,
    )
    quote = al.handicap_quote(curve)
    assert quote.side == "home"
    assert quote.line == 1.5
    assert quote.text() == "1.5"
