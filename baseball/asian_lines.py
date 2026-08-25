"""Turn PS3838's decimal odds into the whole-number Asian line used in Taiwan.

The two are not different spellings of the same price. Decimal (and American,
Hong Kong, Malay, Indonesian) odds all pay a fixed multiple when the bet wins;
the local line instead runs the handicap at **even money** and settles the one
outcome that lands exactly on the number at a fraction of the stake. Backing a
team at ``1+70``:

    wins by 2 or more -> collect the full stake
    wins by exactly 1 -> collect 70% of it
    wins by 1 less, or loses -> pay the full stake

So the quoted ``+70`` is not a price, it is the settlement on the push case,
and ``1-30`` is the same market with the exact-margin case going 30% *against*
the layer. Converting therefore needs the probability of every margin, not just
the probability of winning — which is why we read PS3838's whole spread ladder
rather than a single line.

Given ``G(k) = P(home margin >= k)``, a side laying ``L`` runs wins when the
margin clears ``L``, loses when it falls short, and lands on the number with
probability ``G(L) - G(L+1)``. The water that makes the bet fair is

    X = [P(lose) - P(win)] / P(land on the number)

Totals work the same way with ``H(k) = P(total runs >= k)`` and the over in
place of the favourite.

Every input probability is de-vigged first, so what comes out is the fair line;
the book's own commission is charged separately and is not modelled here.
"""

from dataclasses import dataclass

# Half-run lines pin a point on the curve outright; whole-run lines only pin a
# ratio, and are solved against a neighbour that is already known.
_MAX_SOLVE_PASSES = 4

# A coinflip laying a whole run needs the entire stake back on the push case
# (X = 1). Above this the handicap is more than the edge can carry and the book
# posts a pick'em instead.
PK_ABOVE = 0.85

# How close the de-vigged moneyline has to sit to even before a game counts as
# a pick'em on the moneyline alone.
EVEN_WITHIN = 0.01

# A half line cannot push, so there is no settlement to compensate either side
# with — it is only postable while the two sides are near even.
HALF_LINE_WITHIN = 0.06

# Books post whole numbers by preference and reach for a half line only when no
# whole number can carry the game: Evan's sheet of 2026-08-25 accepted +75 but
# wrote 6.5 where both 6 and 7 needed a full unit.
MAX_WATER = 0.90


def devig(price_a: float, price_b: float) -> tuple[float, float]:
    """Two decimal prices -> the fair probabilities they imply.

    The overround is removed proportionally (the multiplicative method), which
    is standard for the near-even two-way markets we read here.
    """
    raw_a, raw_b = 1.0 / price_a, 1.0 / price_b
    total = raw_a + raw_b
    return raw_a / total, raw_b / total


def _is_whole(line: float) -> bool:
    return abs(line - round(line)) < 1e-9


@dataclass
class Curve:
    """A cumulative distribution sampled at the integers the feed pins down.

    ``points[k]`` is ``P(outcome >= k)``. Gaps are real: a game whose ladder
    stops at -1.5 says nothing about the away team's own run line, and the
    caller gets ``None`` rather than an extrapolation.
    """

    points: dict[int, float]

    def at(self, k: int) -> float | None:
        return self.points.get(k)


def _solve_whole_lines(points: dict[int, float], constraints: list[tuple[int, float]]) -> None:
    """Fill gaps from whole-number lines, which only fix a conditional price.

    A line at ``k`` pushes on ``outcome == k``, so its de-vigged price is
    ``G(k+1) / (G(k+1) + 1 - G(k))``. Knowing either neighbour yields the other.
    """
    for _ in range(_MAX_SOLVE_PASSES):
        progress = False
        for k, ratio in constraints:
            if not 0.0 < ratio < 1.0:
                continue
            below, above = points.get(k), points.get(k + 1)
            if below is not None and above is None:
                points[k + 1] = ratio * (1.0 - below) / (1.0 - ratio)
                progress = True
            elif above is not None and below is None:
                points[k] = 1.0 - above * (1.0 - ratio) / ratio
                progress = True
        if not progress:
            return


def margin_curve(spreads: list[dict], ml_home: float | None = None,
                 ml_away: float | None = None) -> Curve:
    """Build ``P(home margin >= k)`` from the run-line ladder and moneyline.

    ``spreads`` are the rows stored in the ``盤口`` sheet's ``all_spreads``:
    ``hdp`` is the home team's handicap (negative = home gives runs) and
    ``home``/``away`` are that side's decimal prices.
    """
    points: dict[int, float] = {}
    constraints: list[tuple[int, float]] = []
    for row in spreads:
        hdp, home, away = row.get("hdp"), row.get("home"), row.get("away")
        if hdp is None or not home or not away:
            continue
        p_home, _ = devig(home, away)
        if _is_whole(hdp):
            # Home lays -hdp runs and pushes when the margin lands on it.
            constraints.append((int(round(-hdp)), p_home))
        else:
            # Home covers iff the margin clears the half run: margin >= -hdp+0.5.
            points[int(-hdp + 0.5)] = p_home
    if ml_home and ml_away:
        points.setdefault(1, devig(ml_home, ml_away)[0])
    _solve_whole_lines(points, constraints)
    # Draws are rare enough in baseball that P(margin >= 0) collapses onto
    # P(home wins) unless the ladder actually quoted a +0.5 line.
    if 0 not in points and 1 in points:
        points[0] = points[1]
    return Curve(points)


def total_curve(totals: list[dict]) -> Curve:
    """Build ``P(total runs >= k)`` from the over/under ladder."""
    points: dict[int, float] = {}
    constraints: list[tuple[int, float]] = []
    for row in totals:
        line, over, under = row.get("line"), row.get("over"), row.get("under")
        if line is None or not over or not under:
            continue
        p_over, _ = devig(over, under)
        if _is_whole(line):
            constraints.append((int(round(line)), p_over))
        else:
            points[int(line + 0.5)] = p_over
    _solve_whole_lines(points, constraints)
    return Curve(points)


def _imbalance(at_line: float | None, past_line: float | None) -> float | None:
    """How lopsided a line is before any compensation is applied.

    ``|P(above) - P(below)|``, and it measures whole and half lines on the same
    scale — which is what lets them be compared. A whole line can absorb its
    imbalance into the push settlement; a half line has no push, so its
    imbalance is simply how unfair it would be to post.
    """
    if at_line is None or past_line is None:
        return None
    return abs((1.0 - at_line) - past_line)


def _half_imbalance(above: float | None,
                    within: float = HALF_LINE_WITHIN) -> float | None:
    """A half line resolves every outcome, so it is fair only when even.

    Returns ``None`` once the two sides are far enough apart that no book would
    post the line bare: with no push there is nothing to settle the difference
    into.
    """
    if above is None:
        return None
    score = abs(2.0 * above - 1.0)
    return score if score <= within else None


def _water_from(at_line: float | None, past_line: float | None) -> float | None:
    """Fair settlement on the push case, from the laying side's own curve."""
    if at_line is None or past_line is None:
        return None
    push = at_line - past_line
    if push <= 1e-6:
        return None
    return ((1.0 - at_line) - past_line) / push


def water(curve: Curve, side: str, line: int) -> float | None:
    """Water for ``side`` laying ``line`` runs, or ``None`` if underivable.

    The away team's curve is the home curve read backwards:
    ``P(away margin >= k) == P(home margin <= -k) == 1 - G(1 - k)``.
    """
    if line <= 0:
        return None
    if side == "home":
        return _water_from(curve.at(line), curve.at(line + 1))
    below, above = curve.at(1 - line), curve.at(-line)
    if below is None or above is None:
        return None
    return _water_from(1.0 - below, 1.0 - above)


def format_water(value: float | None, *, step: int = 5) -> str:
    """Render water the way the sheet writes it: ``+70``, ``-30``, ``平``.

    Zero water is written ``平``, not ``PK``. The two are different claims: a
    pick'em has no handicap at all, whereas ``8平`` is a real line that happens
    to need no compensation on the push.
    """
    if value is None:
        return ""
    points = int(round(value * 100 / step)) * step
    if points == 0:
        return "平"
    return f"{points:+d}"


@dataclass
class Quote:
    """One posted line: who lays it, how much, and the water on the push.

    A half line carries no water at all — nothing can land exactly on it, so
    there is no push to settle and the number is written bare.
    """

    side: str            # "home" / "away" for handicaps, "over" for totals
    line: float
    water: float | None

    def text(self) -> str:
        if self.line == 0:
            return "PK"
        if not _is_whole(self.line):
            return f"{self.line:g}"
        return f"{self.line}{format_water(self.water)}"


def _prefer_whole(whole: "Quote | None", half) -> "Quote | None":
    """Post the whole number unless it needs more water than a book would show.

    Whole numbers are the norm; a half line is what a game falls back to when
    no whole number sits near enough the middle to be worth quoting.
    """
    if whole is not None and abs(whole.water) <= MAX_WATER:
        return whole
    if half is not None:
        return half[0]
    return whole


def _half_line_prob(curve: Curve, side: str, line: float) -> float | None:
    """P(``side`` covers) at a half-run handicap, which never pushes."""
    above = int(line + 0.5)
    if side == "home":
        return curve.at(above)
    below = curve.at(1 - above)
    return None if below is None else 1.0 - below


def handicap_quote(curve: Curve, *, pk_above: float = PK_ABOVE,
                   even_within: float = EVEN_WITHIN,
                   max_line: int = 3) -> Quote | None:
    """Post the run line a local book would: the one closest to even money.

    Most baseball games are quoted at a single run, but the choice of line is
    really the same one the totals face — the book posts whichever whole number
    needs the least compensation. A heavy favourite laying only one run would
    have to pay multiples of the stake back on the exact-margin case, so the
    line moves up instead.

    Two cases fall off the bottom of that ladder. A favourite too slight to
    carry even one run needs more than a full unit of water, and reverts to a
    pick'em. A dead-even game gets there by another road: with no favourite to
    lay runs, PS3838 quotes only the ladder above the line, so ``G(2)`` never
    appears and no water can be computed at all — the moneyline alone is enough
    to call it.
    """
    win = curve.at(1)
    if win is None:
        return None
    # A level moneyline is a pick'em outright: no handicap at all, and the tie
    # pushes. That is a different claim from laying half a run, so it is
    # settled before any line is considered.
    if abs(win - 0.5) <= even_within + 1e-9:
        return Quote(side="home" if win >= 0.5 else "away", line=0, water=None)
    side = "home" if win >= 0.5 else "away"
    whole, half = None, None
    for line in range(1, max_line + 1):
        value = water(curve, side, line)
        if value is not None and (whole is None or abs(value) < abs(whole.water)):
            whole = Quote(side=side, line=line, water=value)
        score = _half_imbalance(_half_line_prob(curve, side, line - 0.5))
        if score is not None and (half is None or score < half[1]):
            half = (Quote(side=side, line=line - 0.5, water=None), score)
    best = _prefer_whole(whole, half)
    if best is None:
        return None
    if best.line == 1 and best.water is not None and best.water > pk_above:
        return Quote(side=side, line=0, water=None)
    return best


def total_quote(curve: Curve) -> Quote | None:
    """Post the whole-number total whose water sits closest to even.

    The ladder usually supports two candidates; the book picks the one that
    needs the least compensation, which is what keeps the posted number near
    the middle of the run distribution.
    """
    whole, half = None, None
    for line in sorted(curve.points):
        value = _water_from(curve.at(line), curve.at(line + 1))
        if value is not None and (whole is None or abs(value) < abs(whole.water)):
            whole = Quote(side="over", line=line, water=value)
        # The half line just below this point: the over covers iff the total
        # reaches it, so nothing can land on the number.
        score = _half_imbalance(curve.at(line))
        if score is not None and (half is None or score < half[1]):
            half = (Quote(side="over", line=line - 0.5, water=None), score)
    return _prefer_whole(whole, half)
