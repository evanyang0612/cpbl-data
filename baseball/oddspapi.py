"""Collect Pinnacle's full board from OddsPapi — including CPBL, which nobody
else sells.

Every price this returns matched the PS3838 scraper exactly when the two were
read side by side on 2026-09-13 (27 of 32 fields identical; the five that
differed were only *which rung* each calls the main line, never a price). It
is the same book, reached a better way:

* **The whole ladder.** PS3838's compact feed hands us three rungs; this hands
  us all of them — totals 4.5 through 7.5, spreads -2.5 through +2.5. That is
  what ``baseball.asian_lines`` needs and has never had.
* **Both periods**, full game and 1st-5-innings, as ``result`` and
  ``p1+p2+p3+p4+p5``.
* **Every price change**, not a 30-minute sample, so any instant before first
  pitch can be replayed exactly (see ``replay``).
* **CPBL** (tournament 32233), which The Odds API does not carry at all.
* Free.

What it will **not** do is reach into the past. On the free tier ``/fixtures``
lists upcoming games with odds and 66 finished ones **all carrying
``hasOdds: false``**, and no paging parameter changes that — so a finished
game cannot be addressed, and ``/historical-odds`` is only useful for a
fixture still on the board. The 2026-03 → 07 hole in ``盤口`` therefore still
needs The Odds API (see ``baseball/odds_history.py``); whether a paid OddsPapi
tier opens the past up is untested.

Run daily, it captures each game's whole line path from the moment the board
opened — roughly nineteen hours for a next-day NPB game.

    ODDSPAPI_KEY=... uv run python -m baseball.oddspapi discover
    ODDSPAPI_KEY=... uv run python -m baseball.oddspapi backfill --league cpbl \\
        --start 2026-09-13 --end 2026-09-20 --dry-run
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
    # The key rides in the query string, not a header — every endpoint takes
    # `apiKey`, and sending it as `x-api-key` earns a bare 401.
    getter = session or requests
    resp = getter.get(f"{API_BASE}/{path.lstrip('/')}",
                      params={**params, "apiKey": api_key}, timeout=30)
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
        catalogue[market_id] = {
            "name": market.get("marketName", ""),
            "type": market.get("marketType", ""),
            # The line itself lives here, not on the payload: every rung of the
            # ladder is its own marketId, and `handicap` is what tells them
            # apart (totals 6.0 vs 6.5, spreads -1.5 vs +1.5).
            "handicap": market.get("handicap"),
            # "result" (full game) or "p1+p2+p3+p4+p5" (the 1st 5 innings).
            "period": market.get("period", ""),
            "outcomes": outcomes,
        }
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


# --- reading a board ------------------------------------------------------

# Written to the 盤口 sheet's ``source`` column.
SOURCE = "oddspapi"

# OddsPapi's period names, mapped to the two 盤口 already records. Everything
# else (individual innings, and so on) is skipped.
PERIOD_LABELS = {"result": "final", "p1+p2+p3+p4+p5": "half"}

# Market types worth a 盤口 row. ``teamtotals-team1`` / ``-team2`` are on the
# board too; we do not bet them and the sheet has nowhere to put them.
MONEYLINE, TOTALS, SPREADS = "moneyline", "totals", "spreads"

# Outcome names. Pinnacle numbers the sides rather than naming them, and
# participant1 is the home team — confirmed against the scraper's own row for
# 2026-09-13 Seibu vs Nippon-Ham, where home 西武 was 1.8 and outcome "1" is.
HOME_OUTCOME, AWAY_OUTCOME = "1", "2"


def _team_norm(name: str, league) -> str:
    """English club name -> whatever short name the league's sheets key on."""
    from baseball import odds_history

    return odds_history.normalize_team(name, league)


def _price_of(outcome: dict):
    for player in (outcome.get("players") or {}).values():
        if isinstance(player, dict) and player.get("price") is not None:
            return float(player["price"])
    return None


def _is_main(market: dict) -> bool:
    """Pinnacle's own main line, as opposed to an alternate.

    Every line on the ladder is its own market, and only the main one's
    ``bookmakerMarketId`` starts with ``line/`` — the rest say ``altLine/``.
    This is a different answer from the PS3838 scraper's balanced-juice
    heuristic: on 2026-09-13 Pinnacle's main total was 6.0 while the most
    balanced line was 6.5. Taking the book's own word for it is why a
    backfilled row and a scraped one can name different numbers.
    """
    return str(market.get("bookmakerMarketId", "")).startswith("line/")


def parse_fixture(fixture: dict, catalogue: dict[int, dict], league,
                  at: str | None = None, bookmaker: str = BOOKMAKER,
                  board: dict | None = None) -> list[dict]:
    """One fixture's board -> one 盤口 row per period.

    ``board`` overrides the fixture's own odds, which is how a replayed
    historical snapshot is parsed with the same code as a live one.
    """

    book = board or (fixture.get("bookmakerOdds") or {}).get(bookmaker) or {}
    markets = book.get("markets") or {}
    if not markets:
        return []

    start = _parse_iso(fixture.get("startTime"))
    taken = _parse_iso(at) or start
    home, away = fixture.get("participant1Name", ""), fixture.get("participant2Name", "")
    local_start = start.astimezone(league.tz) if start else None

    periods: dict[str, dict] = {}
    for market_id, body in markets.items():
        try:
            known = catalogue.get(int(market_id))
        except (TypeError, ValueError):
            known = None
        if not known:
            continue
        label = PERIOD_LABELS.get(known.get("period"))
        kind = known.get("type")
        if label is None or kind not in (MONEYLINE, TOTALS, SPREADS):
            continue
        prices = {}
        for outcome_id, outcome in (body.get("outcomes") or {}).items():
            try:
                name = known["outcomes"].get(int(outcome_id))
            except (TypeError, ValueError):
                name = None
            if name:
                prices[name] = _price_of(outcome)
        slot = periods.setdefault(label, {"totals": [], "spreads": [],
                                          "main_total": None, "main_spread": None,
                                          "ml": {}})
        if kind == MONEYLINE:
            slot["ml"] = {"ml_home": prices.get(HOME_OUTCOME),
                          "ml_away": prices.get(AWAY_OUTCOME),
                          "ml_draw": prices.get("X")}
        elif kind == TOTALS:
            over, under = prices.get("Over"), prices.get("Under")
            if over is None or under is None:
                continue
            row = {"line": known.get("handicap"), "over": over, "under": under}
            slot["totals"].append(row)
            if _is_main(body):
                slot["main_total"] = row
        else:
            home_price, away_price = prices.get(HOME_OUTCOME), prices.get(AWAY_OUTCOME)
            if home_price is None or away_price is None:
                continue
            row = {"hdp": known.get("handicap"), "home": home_price,
                   "away": away_price}
            slot["spreads"].append(row)
            if _is_main(body):
                slot["main_spread"] = row

    rows = []
    for label, slot in periods.items():
        total = slot["main_total"] or {}
        spread = slot["main_spread"] or {}
        rows.append({
            "source": SOURCE,
            "captured_at": (taken.astimezone(league.tz).strftime("%Y-%m-%d %H:%M:%S")
                            if taken else ""),
            # The same number the PS3838 scraper writes, so a backfilled row
            # and a scraped one join on it exactly.
            "event_id": (fixture.get("externalProviders") or {}).get("pinnacleId")
                        or fixture.get("fixtureId", ""),
            "league": fixture.get("tournamentName", ""),
            "home_team": home, "away_team": away,
            "home_norm": _team_norm(home, league),
            "away_norm": _team_norm(away, league),
            league.start_column: local_start.isoformat() if local_start else "",
            "start": local_start,
            "game_date": local_start.strftime("%Y-%m-%d") if local_start else "",
            "status": "pregame" if (start and taken and taken < start) else "live",
            "mins_to_start": (round((start - taken).total_seconds() / 60)
                              if start and taken else ""),
            "period": label,
            **slot["ml"],
            "total_line": total.get("line"), "total_over": total.get("over"),
            "total_under": total.get("under"),
            "all_totals": sorted(slot["totals"], key=lambda r: r["line"] or 0),
            "spread_hdp": spread.get("hdp"), "spread_home": spread.get("home"),
            "spread_away": spread.get("away"),
            "all_spreads": sorted(slot["spreads"], key=lambda r: r["hdp"] or 0),
        })
    return rows


def _parse_iso(value):
    import datetime as dt

    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def replay(history: dict, at: str, bookmaker: str = BOOKMAKER) -> dict:
    """Rebuild the board as it stood at ``at`` from the change series.

    ``/historical-odds`` gives every price change rather than fixed snapshots,
    so any instant can be reconstructed: for each outcome, the last change at
    or before ``at``. An outcome with no change yet simply is not there.
    """
    when = _parse_iso(at)
    book = (history.get("bookmakers") or {}).get(bookmaker) or {}
    out: dict = {"markets": {}}
    for market_id, market in (book.get("markets") or {}).items():
        outcomes = {}
        for outcome_id, outcome in (market.get("outcomes") or {}).items():
            players = {}
            for player_id, series in (outcome.get("players") or {}).items():
                if not isinstance(series, list):
                    continue
                latest = None
                for change in series:
                    stamp = _parse_iso(change.get("createdAt"))
                    if stamp and when and stamp <= when:
                        latest = change
                if latest and latest.get("active", True):
                    players[player_id] = {"price": latest.get("price")}
            if players:
                outcomes[outcome_id] = {"players": players}
        if outcomes:
            out["markets"][market_id] = {
                "bookmakerMarketId": market.get("bookmakerMarketId", ""),
                "outcomes": outcomes,
            }
    return out


# --- backfilling ----------------------------------------------------------

# OddsPapi's own league ids, resolved once by `discover` so a backfill does not
# have to walk the 87-tournament catalogue every run.
TOURNAMENTS = {"npb": 1036, "cpbl": 32233, "mlb": 109, "kbo": 2541}
SPORT_BASEBALL = 13

# Minutes before first pitch to replay the board at. The history is every
# price change, so this costs nothing extra per point — unlike The Odds API,
# where each one is another paid request.
DEFAULT_LEADS = (720, 360, 120, 30, 10)


def _rate_limited_get(path, *, api_key, session=None, pause=1.2, tries=4,
                      **params):
    """Sleep, call, and back off when told to.

    The free tier rate-limits hard and answers with a 429 carrying its own
    ``retryMs``; honouring that is the difference between a clean sweep and a
    run that loses half its fixtures to a status code.
    """
    import time

    for attempt in range(tries):
        time.sleep(pause * (attempt + 1))
        try:
            return _get(path, api_key=api_key, session=session, **params)
        except requests.HTTPError as exc:
            response = exc.response
            if response is None or response.status_code != 429:
                raise
            wait = 1.0
            try:
                wait = max(wait, float(response.json()["error"]["retryMs"]) / 1000)
            except Exception:
                pass
            time.sleep(wait)
    raise RuntimeError(f"{path}: still rate limited after {tries} attempts")


# /fixtures refuses a window wider than this — with a bare 404, not a message.
MAX_WINDOW_DAYS = 10


def fixtures_between(start: str, end: str, league_key: str, *, api_key: str,
                     session=None) -> list[dict]:
    """Every fixture with odds in the window, oldest first, de-duplicated.

    Walked in ten-day steps because that is all the endpoint accepts; ask for
    more and it 404s rather than saying so.
    """
    import datetime as dt

    first, last = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    seen, out = set(), []
    cursor = first
    while cursor <= last:
        # A window with nothing in it answers 404 rather than an empty list.
        stop = min(cursor + dt.timedelta(days=MAX_WINDOW_DAYS - 1), last)
        try:
            rows = _rate_limited_get(
                "fixtures", api_key=api_key, session=session,
                sportId=SPORT_BASEBALL, tournamentId=TOURNAMENTS[league_key],
                hasOdds="true",
                **{"from": f"{cursor.isoformat()}T00:00:00Z",
                   "to": f"{stop.isoformat()}T23:59:59Z"})
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                rows = []          # nothing scheduled in this window
            else:
                raise
        rows = rows if isinstance(rows, list) else rows.get("data", [])
        for row in rows:
            if row.get("fixtureId") not in seen:
                seen.add(row.get("fixtureId"))
                out.append(row)
        cursor = stop + dt.timedelta(days=1)
    return sorted(out, key=lambda f: f.get("startTime", ""))


def backfill(start: str, end: str, *, league, league_key: str, api_key: str,
             leads=DEFAULT_LEADS, write: bool = True, session=None) -> list[dict]:
    """Replay every fixture's history at each lead time and append the rows."""
    import datetime as dt

    from baseball.pinnacle_odds import snapshots_to_rows, write_snapshots

    catalogue = market_catalogue(
        _rate_limited_get("markets", api_key=api_key, session=session,
                          sportId=SPORT_BASEBALL))
    fixtures = fixtures_between(start, end, league_key, api_key=api_key,
                                session=session)
    # A finished game is not addressable on this tier — see the module
    # docstring — so a window in the past comes back empty rather than wrong.
    fixtures = [f for f in fixtures if f.get("hasOdds")]
    print(f"[oddspapi] {league_key.upper()} {start}~{end}: {len(fixtures)} 場，"
          f"每場 {len(leads)} 個時點")
    if not write:
        return []

    leads = tuple(sorted({int(m) for m in leads}, reverse=True))
    labels = {leads[0]: "open", leads[-1]: "close"}
    all_rows = []
    for n, fixture in enumerate(fixtures, 1):
        first_pitch = _parse_iso(fixture.get("startTime"))
        if not first_pitch:
            continue
        try:
            history = _rate_limited_get("historical-odds", api_key=api_key,
                                        session=session,
                                        fixtureId=fixture["fixtureId"],
                                        bookmakers=BOOKMAKER)
        except Exception as exc:
            print(f"[oddspapi] {fixture['fixtureId']} 取歷史失敗（{exc}）")
            continue
        got = 0
        for lead in leads:
            at = (first_pitch - dt.timedelta(minutes=lead)).strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            board = replay(history, at)
            if not board["markets"]:
                continue    # the board had not opened that far out
            rows = parse_fixture(fixture, catalogue, league, at=at, board=board)
            for row in rows:
                row["snapshot_type"] = labels.get(lead, "interim")
            all_rows.extend(rows)
            got += len(rows)
        print(f"[oddspapi] ({n}/{len(fixtures)}) {fixture.get('startTime','')[:10]} "
              f"{fixture.get('participant2Name')} @ {fixture.get('participant1Name')}"
              f" -> {got} 列")

    if all_rows:
        written = 0
        by_capture: dict = {}
        for row in all_rows:
            by_capture.setdefault(row["captured_at"], []).append(row)
        for captured_at, group in sorted(by_capture.items()):
            # Each row carries its own snapshot_type, which wins over this one.
            written += write_snapshots(
                snapshots_to_rows(group, "interim", captured_at, league), league)
        print(f"[oddspapi] 寫入 {written} 列到『盤口』")
    return all_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find out what OddsPapi carries, before building on it")
    parser.add_argument("command", choices=["discover", "backfill"])
    parser.add_argument("--league", default="npb", choices=sorted(LEAGUE_NEEDLES))
    parser.add_argument("--out-dir", help="也把原始 JSON 存起來，方便接著寫解析器")
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--leads", type=int, nargs="+", default=list(DEFAULT_LEADS),
                        metavar="MIN",
                        help="賽前幾分鐘各取一個時點（歷史是逐次變動，多取不加價）")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("ODDSPAPI_KEY")
    if not api_key:
        parser.error("ODDSPAPI_KEY is not set")
    if args.command == "discover":
        discover(api_key, args.league, out_dir=args.out_dir)
        return

    if not args.start or not args.end:
        parser.error("backfill needs --start and --end")
    from baseball.pinnacle_odds import LEAGUES

    backfill(args.start, args.end, league=LEAGUES[args.league],
             league_key=args.league, api_key=api_key,
             leads=tuple(args.leads), write=not args.dry_run)


if __name__ == "__main__":
    main()
