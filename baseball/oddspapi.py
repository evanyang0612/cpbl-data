"""Find out what OddsPapi actually carries, before building anything on it.

OddsPapi is the one provider that gives historical odds away on the free tier,
at per-change resolution rather than fixed snapshots — which would fill the
2026-03 → 2026-07 hole in ``盤口`` for nothing, and fill it finer than either
the PS3838 scraper or The Odds API can.

Two things stop us writing a parser for it straight away:

* Its odds payload is entirely numeric ids — ``markets["10286"].outcomes["10287"]``
  — and the published docs map none of them for baseball. Only the ``/markets``
  catalogue says what an id means.
* The docs never say where a handicap or a total's line is carried, nor whether
  ``participant1`` is the home side. Guessing either one produces a parser that
  looks right and silently prices the wrong team.

So this module answers those questions from the live API instead of assuming:
it walks sports → tournaments → one fixture → that fixture's odds, resolves
every id it can against the catalogue, and prints what is left unmapped. The
run is small enough for the free tier, writes nothing, and its output is what
the writer half gets built against.

    ODDSPAPI_KEY=... uv run python -m baseball.oddspapi discover
    ODDSPAPI_KEY=... uv run python -m baseball.oddspapi discover --league cpbl
"""

import argparse
import json
import os

import requests

API_BASE = os.getenv("ODDSPAPI_BASE", "https://api.oddspapi.io/v4").rstrip("/")
BOOKMAKER = "pinnacle"

# What each league is called in someone else's catalogue is a guess until it is
# looked up, so match on any of these rather than one exact string.
LEAGUE_NEEDLES = {
    "npb": ("npb", "nippon", "japan"),
    "cpbl": ("cpbl", "chinese professional", "taiwan", "chinese taipei"),
    "mlb": ("mlb", "major league baseball"),
    "kbo": ("kbo", "korea"),
}


def _get(path: str, *, api_key: str, session=None, **params) -> object:
    getter = session or requests
    resp = getter.get(f"{API_BASE}/{path.lstrip('/')}",
                      params=params, headers={"x-api-key": api_key}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def find_sport(sports: list[dict], wanted: str = "baseball") -> dict | None:
    """The sport row for ``wanted``, matched on slug or name."""
    needle = wanted.lower()
    for sport in sports or []:
        haystack = f"{sport.get('sportSlug', '')} {sport.get('sportName', '')}".lower()
        if needle in haystack:
            return sport
    return None


def match_tournaments(tournaments: list[dict]) -> dict[str, dict | None]:
    """{league key: tournament row or None}.

    ``None`` is a result, not a failure: whether CPBL is carried at all is one
    of the two things this run exists to settle.
    """
    found: dict[str, dict | None] = {key: None for key in LEAGUE_NEEDLES}
    for row in tournaments or []:
        haystack = (f"{row.get('tournamentName', '')} {row.get('tournamentSlug', '')} "
                    f"{row.get('categoryName', '')}").lower()
        for key, needles in LEAGUE_NEEDLES.items():
            if found[key] is None and any(n in haystack for n in needles):
                found[key] = row
    return found


def market_catalogue(markets: list[dict]) -> dict[int, dict]:
    """{marketId: {name, type, outcomes: {outcomeId: name}}}.

    The odds payload carries nothing but these ids, so this mapping is the
    whole reason a parser can be written at all.
    """
    catalogue = {}
    for market in markets or []:
        try:
            market_id = int(market.get("marketId"))
        except (TypeError, ValueError):
            continue
        outcomes = {}
        for outcome in market.get("outcomes") or []:
            try:
                outcomes[int(outcome.get("outcomeId"))] = outcome.get("outcomeName", "")
            except (TypeError, ValueError):
                continue
        catalogue[market_id] = {"name": market.get("marketName", ""),
                                "type": market.get("marketType", ""),
                                "outcomes": outcomes}
    return catalogue


def _prices(outcome: dict) -> list:
    """Every price under an outcome, across whatever `players` keys it has."""
    out = []
    for player in (outcome.get("players") or {}).values():
        if isinstance(player, dict) and player.get("price") is not None:
            out.append(player["price"])
    if outcome.get("price") is not None:
        out.append(outcome["price"])
    return out


def describe_odds(fixture: dict, catalogue: dict[int, dict],
                  bookmaker: str = BOOKMAKER) -> list[str]:
    """One fixture's board, with every id it can resolve spelled out.

    Unresolved ids are printed as ``未知`` rather than skipped — an id the
    catalogue does not explain is exactly the finding worth carrying back.
    """
    lines = [
        f"fixtureId   {fixture.get('fixtureId')}",
        f"tournament  {fixture.get('tournamentName')}",
        f"startTime   {fixture.get('startTime')}",
        f"participant1 {fixture.get('participant1Name')}   "
        f"participant2 {fixture.get('participant2Name')}",
        "  （participant1 是不是主隊，要拿同一場比賽的既有盤口對照才能確認）",
        "",
    ]
    board = (fixture.get("bookmakerOdds") or {}).get(bookmaker) or {}
    markets = board.get("markets") or {}
    if not markets:
        lines.append(f"  {bookmaker} 這場沒有任何市場")
        return lines
    for market_id, market in markets.items():
        try:
            known = catalogue.get(int(market_id))
        except (TypeError, ValueError):
            known = None
        label = (f"{known['name']} [{known['type']}]" if known
                 else "未知 market（catalogue 沒有這個 id）")
        lines.append(f"  market {market_id}: {label}")
        # Anything beyond the outcome map is where a handicap or a total's line
        # would have to live, so show the market's own keys too.
        extra = {k: v for k, v in market.items() if k != "outcomes"}
        if extra:
            lines.append(f"    market 其他欄位: {json.dumps(extra, ensure_ascii=False)[:300]}")
        for outcome_id, outcome in (market.get("outcomes") or {}).items():
            try:
                name = (known or {}).get("outcomes", {}).get(int(outcome_id))
            except (TypeError, ValueError):
                name = None
            prices = _prices(outcome)
            lines.append(f"    outcome {outcome_id}: {name or '未知 outcome'}"
                         f"  price={prices}")
            rest = {k: v for k, v in outcome.items() if k != "players"}
            if rest:
                lines.append(f"      outcome 其他欄位: "
                             f"{json.dumps(rest, ensure_ascii=False)[:300]}")
    return lines


def discover(api_key: str, league: str = "npb", *, session=None,
             out_dir: str | None = None) -> None:
    sports = _get("sports", api_key=api_key, session=session)
    sport = find_sport(sports if isinstance(sports, list) else sports.get("data", []))
    print(f"[discover] 運動項目 {len(sports)} 個；baseball -> {sport}")
    if not sport:
        print("[discover] 找不到 baseball，後面都不用看了")
        return

    sport_id = sport["sportId"]
    tours = _get("tournaments", api_key=api_key, session=session, sportId=sport_id)
    tours = tours if isinstance(tours, list) else tours.get("data", [])
    found = match_tournaments(tours)
    print(f"\n[discover] baseball 底下 {len(tours)} 個賽事：")
    for key, row in found.items():
        print(f"  {key:<5} {'✓ ' + str(row.get('tournamentName')) + ' (id=' + str(row.get('tournamentId')) + ')' if row else '✗ 沒有'}")

    target = found.get(league)
    if not target:
        print(f"\n[discover] {league.upper()} 不在裡面 —— 這個來源對它沒用")
        return

    fixtures = _get("fixtures", api_key=api_key, session=session,
                    sportId=sport_id, tournamentId=target["tournamentId"],
                    hasOdds="true", bookmakers=BOOKMAKER)
    fixtures = fixtures if isinstance(fixtures, list) else fixtures.get("data", [])
    print(f"\n[discover] {league.upper()} 有盤口的賽事 {len(fixtures)} 場")
    if not fixtures:
        print("[discover] 這個時段沒有賽事，換一天再跑")
        return

    markets = _get("markets", api_key=api_key, session=session, sportId=sport_id)
    markets = markets if isinstance(markets, list) else markets.get("data", [])
    catalogue = market_catalogue(markets)
    print(f"[discover] market catalogue {len(catalogue)} 個")

    sample = fixtures[0]
    odds = _get("odds", api_key=api_key, session=session,
                fixtureId=sample["fixtureId"], bookmakers=BOOKMAKER)
    if isinstance(odds, list):
        odds = odds[0] if odds else {}
    print("\n[discover] 一場的完整盤口：\n")
    print("\n".join(describe_odds({**sample, **odds}, catalogue)))

    history = _get("historical-odds", api_key=api_key, session=session,
                   fixtureId=sample["fixtureId"], bookmakers=BOOKMAKER)
    print("\n[discover] historical-odds 原始回應（前 1200 字）：")
    print(json.dumps(history, ensure_ascii=False)[:1200])

    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        for name, blob in (("fixture", {**sample, **odds}),
                           ("markets", markets), ("history", history)):
            path = os.path.join(out_dir, f"oddspapi_{league}_{name}.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(blob, fh, ensure_ascii=False, indent=1)
            print(f"[discover] 已存 {path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find out what OddsPapi carries, before building on it")
    parser.add_argument("command", choices=["discover"])
    parser.add_argument("--league", default="npb", choices=sorted(LEAGUE_NEEDLES))
    parser.add_argument("--out-dir", help="也把原始 JSON 存起來，方便接著寫解析器")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("ODDSPAPI_KEY")
    if not api_key:
        parser.error("ODDSPAPI_KEY is not set")
    discover(api_key, args.league, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
