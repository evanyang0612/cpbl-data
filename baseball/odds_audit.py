"""Check a 盤口 row against itself: does its handicap agree with its moneyline?

A row carries two readings of the same game. They are not independent, and the
relation between them is an ordering, not an estimate::

    home price receiving runs  <  home moneyline  <  home price laying runs

Receiving runs can only shorten a price and laying them can only lengthen it,
so a moneyline outside that bracket is a contradiction inside one row — which
is how the pre-#79 handicap bug survived five weeks unnoticed.

Stating it as an ordering is what makes it usable. An earlier version of this
audit interpolated the ladder for the handicap at which the two sides would
price even, and compared its sign to the moneyline's; across a three-run gap
between rungs that estimate is worth about half a run, which flagged hundreds
of perfectly sound pick'em games. The bracket needs no interpolation, no dead
band, and uses every rung rather than two.

    uv run python -m baseball.odds_audit
    uv run python -m baseball.odds_audit --league npb
"""

import argparse
import json

# A price may sit this far outside the bracket without meaning anything: the
# ladder and the moneyline are not always captured in the same instant, and a
# rung right beside the moneyline can round past it.
SLACK = 0.05


def _rungs(all_spreads) -> list[dict]:
    if isinstance(all_spreads, list):
        ladder = all_spreads
    else:
        try:
            ladder = json.loads(all_spreads or "[]")
        except (TypeError, ValueError):
            return []
    out = []
    for r in ladder:
        if not isinstance(r, dict):
            continue
        try:
            out.append({"hdp": float(r["hdp"]), "home": float(r["home"])})
        except (KeyError, TypeError, ValueError):
            continue
    return sorted(out, key=lambda r: r["hdp"])


def _price(value):
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


def check_row(all_spreads, ml_home, ml_away=None) -> dict | None:
    """The rung that contradicts the moneyline, or None.

    ``ml_away`` is accepted and ignored: the bracket is entirely about the home
    side, and passing both reads better at the call site.
    """
    home = _price(ml_home)
    rungs = _rungs(all_spreads)
    if home is None or not rungs:
        return None
    for r in rungs:
        if r["hdp"] > 0 and r["home"] > home + SLACK:
            return {"hdp": r["hdp"], "ladder": r["home"], "moneyline": home,
                    "why": "受讓卻比錢線貴"}
        if r["hdp"] < 0 and r["home"] < home - SLACK:
            return {"hdp": r["hdp"], "ladder": r["home"], "moneyline": home,
                    "why": "讓分卻比錢線便宜"}
    return None


def ladder_runs_backwards(all_spreads) -> bool:
    """True when the home price rises as the home team receives more runs.

    Impossible in a real board, so it is the parse that is wrong.
    """
    rungs = _rungs(all_spreads)
    if len(rungs) < 2:
        return False
    prices = [r["home"] for r in rungs]      # _rungs sorts by handicap
    up = sum(1 for a, b in zip(prices, prices[1:]) if b > a)
    down = sum(1 for a, b in zip(prices, prices[1:]) if b < a)
    return up > down


def incoherence(snapshot: dict) -> str | None:
    """Why this snapshot cannot be a real board, or None if it can be.

    Two invariants, both true of any correctly-read row whatever the feed's
    field order happens to be that day — which is the point. The scraper reads
    PS3838's positional arrays by index, and an index that silently means
    something else produces numbers that are all in range and all wrong. These
    catch that at the moment of parsing rather than five weeks later.
    """
    ladder = snapshot.get("all_spreads")
    if ladder_runs_backwards(ladder):
        rungs = _rungs(ladder)
        return (f"讓分階梯方向相反（受讓越多賠越貴）："
                f"{[(r['hdp'], r['home']) for r in rungs]}")
    bad = check_row(ladder, snapshot.get("ml_home"))
    if bad:
        return (f"錢線落在階梯括號外：主隊錢線 {bad['moneyline']}，"
                f"但 hdp={bad['hdp']} 時主隊賠 {bad['ladder']}（{bad['why']}）")
    return None


def audit_sheet(values: list[list]) -> tuple[list[dict], dict]:
    """(disagreements, tally) for one 盤口 worksheet's raw values."""
    header, rows = values[0], values[1:]
    idx = {name: header.index(name) for name in
           ("all_spreads", "ml_home", "ml_away", "game_date", "captured_at",
            "home_norm", "away_norm", "period")
           if name in header}
    flagged, tally = [], {"scanned": 0, "compared": 0, "disagree": 0}
    for offset, row in enumerate(rows):
        tally["scanned"] += 1
        def cell(name):
            i = idx.get(name)
            return row[i] if i is not None and i < len(row) else ""
        if not _rungs(cell("all_spreads")) or _price(cell("ml_home")) is None:
            continue
        tally["compared"] += 1
        bad = check_row(cell("all_spreads"), cell("ml_home"))
        if bad:
            tally["disagree"] += 1
            flagged.append({
                "row": offset + 2, "game_date": cell("game_date"),
                "captured_at": cell("captured_at"), "period": cell("period"),
                "game": f'{cell("away_norm")}@{cell("home_norm")}',
                "ml": f'{cell("ml_home")}/{cell("ml_away")}', **bad,
            })
    return flagged, tally


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", choices=["npb", "mlb", "cpbl"],
                        help="just this one (default: all three)")
    parser.add_argument("--show", type=int, default=10,
                        help="how many disagreements to print (default: 10)")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    from baseball import pinnacle_odds as po
    from baseball.sheets import GoogleSheetsClient

    specs = [po.LEAGUES[args.league]] if args.league else [po.NPB, po.MLB, po.CPBL]
    client = GoogleSheetsClient()
    for spec in specs:
        values = client.spreadsheet(spec.spreadsheet_key()).worksheet("盤口").get_all_values()
        if len(values) < 2:
            print(f"{spec.key.upper():<5} 盤口 是空的")
            continue
        flagged, tally = audit_sheet(values)
        print(f"{spec.key.upper():<5} {tally['scanned']:>6} 列  "
              f"兩邊都讀得出 {tally['compared']:>6}  矛盾 {tally['disagree']:>5}")
        by_date = {}
        for f in flagged:
            by_date[f["game_date"]] = by_date.get(f["game_date"], 0) + 1
        for f in flagged[:args.show]:
            print(f"      列{f['row']:<6}{f['game_date']} {f['game']:<20}"
                  f"{f['period']:<6} ML主={f['moneyline']:<7}"
                  f"hdp={f['hdp']:<6}主賠={f['ladder']:<7}{f['why']}")
        if len(flagged) > args.show:
            print(f"      …另外 {len(flagged) - args.show} 列")
        if by_date:
            ds = sorted(by_date)
            print(f"      日期範圍 {ds[0]} ~ {ds[-1]}（{len(by_date)} 天）")


if __name__ == "__main__":
    main()
