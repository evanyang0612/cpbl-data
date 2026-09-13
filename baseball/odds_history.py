"""Backfill the ``盤口`` sheet from The Odds API's historical Pinnacle board.

Our own PS3838 scraper started on 2026-07-18. That is 259 NPB games — enough
to see that the pipeline works, nowhere near enough to tell a 3% edge from
noise, which needs something like 4,400 settled bets. Waiting for the ledger
to fill costs three more seasons.

The Odds API keeps Pinnacle's board back to **2020-06-06** (10-minute
snapshots; 5-minute from 2022-09) and carries ``baseball_npb`` and
``baseball_mlb``. Five and a half NPB seasons is roughly 4,700 games — the
same ledger, eighteen times longer, without waiting for it.

Rows land in the existing ``盤口`` tab in the scraper's own column order, so
backfilled and live rows are one table. Two honest differences:

* The featured endpoint returns Pinnacle's **main line only**, not the whole
  ladder, so ``all_totals`` / ``all_spreads`` carry a single entry. That is
  enough for closing-line value and for modelling the main number; it is not
  enough for ``baseball.asian_lines``, which needs the full margin curve.
* ``snapshot_type`` is whatever the caller says it is. A snapshot is taken at
  a fixed instant before first pitch, and ``mins_to_start`` on every row says
  exactly how far out it was, so a mislabelled run is still readable.

The API bills 10 credits per market per region per request, so a plan is
printed and confirmed before anything is spent.

    # confirm Pinnacle really covers this league, for ~30 credits
    uv run python -m baseball.odds_history probe --league npb

    # what a range would cost, without spending anything
    uv run python -m baseball.odds_history backfill --league npb \\
        --start 2026-04-01 --end 2026-04-30 --dry-run

    uv run python -m baseball.odds_history backfill --league npb \\
        --start 2026-04-01 --end 2026-04-30 --snapshot-type close
"""

import argparse
import datetime as dt
import os
import time

import requests

from baseball.pinnacle_odds import (
    LEAGUES, NPB, LeagueSpec, snapshots_to_rows, write_snapshots,
)

API_BASE = os.getenv("ODDS_API_BASE", "https://api.the-odds-api.com/v4").rstrip("/")

# The Odds API's own league keys. It does not carry CPBL — only NPB, MLB, KBO,
# MiLB and NCAA — so a CPBL backfill has to come from somewhere else.
SPORT_KEYS = {"npb": "baseball_npb", "mlb": "baseball_mlb"}

# Pinnacle sits in the EU region. Asking for more regions multiplies the bill
# without adding the one book we are here for.
REGIONS = ("eu",)
MARKETS = ("h2h", "spreads", "totals")
BOOKMAKER = "pinnacle"
# Written to the ``盤口`` sheet's ``source`` column. It matters: this feed gives
# Pinnacle's own featured line and no ladder, while the PS3838 scraper reads the
# whole ladder and picks the most balanced line. Their moneylines agree to the
# third decimal; their totals and run lines often name a different number.
SOURCE = "the_odds_api"
CREDITS_PER_MARKET_REGION = 10

# The instants a day's board is sampled at, in league-local time. NPB starts at
# 14:00 or 18:00 JST and a card rarely strays from those, so two snapshots
# cover a full slate; MLB's card is spread across the evening and needs more.
LOCAL_START_HOURS = {"npb": ("14:00", "18:00"),
                     "mlb": ("13:00", "16:00", "19:00", "22:00")}

# The Odds API spells NPB clubs in English. Folded by distinctive substring to
# the short names every other NPB sheet keys on.
TEAM_SUBSTRING_MAP = {
    "giants": "巨人", "yomiuri": "巨人",
    "swallows": "ヤクルト", "yakult": "ヤクルト",
    "baystars": "DeNA", "dena": "DeNA",
    "dragons": "中日", "chunichi": "中日",
    "tigers": "阪神", "hanshin": "阪神",
    "carp": "広島", "hiroshima": "広島",
    "lions": "西武", "seibu": "西武",
    "fighters": "日本ハム", "nippon-ham": "日本ハム", "nippon ham": "日本ハム",
    "marines": "ロッテ", "lotte": "ロッテ",
    "buffaloes": "オリックス", "orix": "オリックス",
    "hawks": "ソフトバンク", "softbank": "ソフトバンク",
    "eagles": "楽天", "rakuten": "楽天",
}


def normalize_team(name: str | None, league: LeagueSpec = NPB) -> str:
    """English club name -> the short name the league's sheets key on.

    MLB already stores the API's own English spelling, so only NPB folds.
    """
    text = str(name or "").strip()
    if league.key != "npb":
        return text
    folded = text.lower()
    for needle, short in TEAM_SUBSTRING_MAP.items():
        if needle in folded:
            return short
    return ""


def credit_cost(*, markets=MARKETS, regions=REGIONS) -> int:
    return CREDITS_PER_MARKET_REGION * len(markets) * len(regions)


def snapshot_times(date: str, league: LeagueSpec = NPB,
                   lead_minutes: int = 10) -> list[str]:
    """The UTC instants to sample ``date``'s board at, one per start time.

    Sampling per start time rather than on a fixed grid is what keeps the bill
    down: a six-game NPB card that all starts at 18:00 costs one request, not
    six, and every row still carries its own ``mins_to_start``.
    """
    out = []
    for clock in LOCAL_START_HOURS.get(league.key, ("18:00",)):
        hour, minute = (int(part) for part in clock.split(":"))
        local = dt.datetime.fromisoformat(date).replace(
            hour=hour, minute=minute, tzinfo=league.tz)
        stamp = (local - dt.timedelta(minutes=lead_minutes)).astimezone(dt.timezone.utc)
        out.append(stamp.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return sorted(out)


def plan(dates: list[str], league: LeagueSpec = NPB, lead_minutes: int = 10,
         *, markets=MARKETS, regions=REGIONS) -> dict:
    """What a backfill will ask for, and what it will cost, before it runs."""
    times = [ts for date in dates
             for ts in snapshot_times(date, league, lead_minutes)]
    return {"dates": len(dates), "requests": len(times),
            "credits": len(times) * credit_cost(markets=markets, regions=regions),
            "times": times}


def _price(value):
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


def _market(bookmaker: dict, key: str) -> dict | None:
    for market in bookmaker.get("markets") or []:
        if market.get("key") == key:
            return market
    return None


def _h2h(bookmaker, home, away) -> dict:
    market = _market(bookmaker, "h2h") or {}
    prices = {o.get("name"): _price(o.get("price"))
              for o in market.get("outcomes") or []}
    return {"ml_home": prices.get(home), "ml_away": prices.get(away),
            "ml_draw": prices.get("Draw")}


def _spreads(bookmaker, home, away) -> dict:
    """Pinnacle's main run line, stored from the home side.

    ``spread_hdp`` negative means home is laying runs — the same convention the
    PS3838 scraper writes, so the two sources sort together.
    """
    market = _market(bookmaker, "spreads") or {}
    sides = {o.get("name"): o for o in market.get("outcomes") or []}
    home_side, away_side = sides.get(home), sides.get(away)
    if not home_side or not away_side:
        return {"spread_hdp": None, "spread_home": None, "spread_away": None,
                "all_spreads": []}
    row = {"hdp": home_side.get("point"),
           "home": _price(home_side.get("price")),
           "away": _price(away_side.get("price"))}
    if row["hdp"] is None or row["home"] is None or row["away"] is None:
        return {"spread_hdp": None, "spread_home": None, "spread_away": None,
                "all_spreads": []}
    return {"spread_hdp": row["hdp"], "spread_home": row["home"],
            "spread_away": row["away"], "all_spreads": [row]}


def _totals(bookmaker) -> dict:
    market = _market(bookmaker, "totals") or {}
    over = under = line = None
    for outcome in market.get("outcomes") or []:
        if outcome.get("name") == "Over":
            over, line = _price(outcome.get("price")), outcome.get("point")
        elif outcome.get("name") == "Under":
            under = _price(outcome.get("price"))
    if over is None or under is None or line is None:
        return {"total_line": None, "total_over": None, "total_under": None,
                "all_totals": []}
    return {"total_line": line, "total_over": over, "total_under": under,
            "all_totals": [{"line": line, "over": over, "under": under}]}


def _parse_utc(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_snapshot(raw: dict, league: LeagueSpec = NPB,
                   bookmaker_key: str = BOOKMAKER) -> list[dict]:
    """One snapshot payload -> rows in the ``盤口`` scraper's own shape.

    An event Pinnacle did not price is dropped rather than written blank: a
    blank row joins to the game and reads as "the line was missing", which is
    a different claim from "we were not looking".
    """
    taken = _parse_utc(raw.get("timestamp"))
    payload = raw.get("data")
    events = payload if isinstance(payload, list) else [payload] if payload else []

    rows = []
    for event in events:
        book = next((b for b in event.get("bookmakers") or []
                     if b.get("key") == bookmaker_key), None)
        if book is None:
            continue
        home, away = event.get("home_team", ""), event.get("away_team", "")
        start = _parse_utc(event.get("commence_time"))
        local_start = start.astimezone(league.tz) if start else None
        row = {
            "source": SOURCE,
            "captured_at": (taken.astimezone(league.tz).strftime("%Y-%m-%d %H:%M:%S")
                            if taken else ""),
            "event_id": event.get("id", ""),
            "league": event.get("sport_title", ""),
            "home_team": home,
            "away_team": away,
            "home_norm": normalize_team(home, league),
            "away_norm": normalize_team(away, league),
            league.start_column: local_start.isoformat() if local_start else "",
            "start": local_start,
            "game_date": local_start.strftime("%Y-%m-%d") if local_start else "",
            "status": "pregame" if (start and taken and taken < start) else "live",
            "mins_to_start": (round((start - taken).total_seconds() / 60)
                              if start and taken else ""),
            "period": "final",
        }
        row.update(_h2h(book, home, away))
        row.update(_spreads(book, home, away))
        row.update(_totals(book))
        if not any(row.get(k) is not None for k in
                   ("ml_home", "ml_away", "total_line", "spread_home")):
            continue
        rows.append(row)
    return rows


def fetch_snapshot(sport_key: str, when: str, *, api_key: str,
                   markets=MARKETS, regions=REGIONS,
                   session: requests.Session | None = None,
                   timeout: int = 30) -> tuple[dict, dict]:
    """(payload, credit headers) for one historical instant."""
    getter = session or requests
    resp = getter.get(
        f"{API_BASE}/historical/sports/{sport_key}/odds",
        params={"apiKey": api_key, "regions": ",".join(regions),
                "markets": ",".join(markets), "oddsFormat": "decimal",
                "dateFormat": "iso", "date": when},
        timeout=timeout,
    )
    resp.raise_for_status()
    credits = {k: resp.headers.get(k) for k in
               ("x-requests-used", "x-requests-remaining", "x-requests-last")}
    return resp.json(), credits


def daterange(start: str, end: str) -> list[str]:
    first, last = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    if last < first:
        raise ValueError(f"end {end} is before start {start}")
    return [(first + dt.timedelta(days=i)).isoformat()
            for i in range((last - first).days + 1)]


# The npb.jp box-score cache, one file per game, named YYYYMMDD-<code>.json.
# It is the cheapest possible answer to "did anybody play that day".
NPB_BOX_CACHE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".cache", "npb_box")


def game_dates(start: str, end: str, league: LeagueSpec = NPB,
               cache_dir: str | None = None) -> list[str]:
    """The days in the range that actually had games.

    A request on an off day buys an empty payload at the full 30 credits, and
    the 2020-06-06 → today range is two thirds off days: two winters, most
    Mondays, and the all-star break. The box-score cache already knows which
    days had games, so ask only for those. With no cache to consult — another
    league, or a range newer than the cache — every day is kept, which costs
    credits but never misses a slate.
    """
    days = daterange(start, end)
    cache_dir = cache_dir or (NPB_BOX_CACHE if league.key == "npb" else None)
    if not cache_dir or not os.path.isdir(cache_dir):
        return days
    played = {name[:8] for name in os.listdir(cache_dir) if name.endswith(".json")}
    if not played:
        return days
    return [d for d in days if d.replace("-", "") in played]


def backfill(start: str, end: str, *, league: LeagueSpec = NPB, api_key: str,
             snapshot_type: str = "close", lead_minutes: int = 10,
             write: bool = True, pause: float = 0.2, every_day: bool = False,
             session: requests.Session | None = None) -> list[dict]:
    """Walk the range, one request per start time per day, and append the rows."""
    dates = (daterange(start, end) if every_day
             else game_dates(start, end, league))
    budget = plan(dates, league, lead_minutes)
    skipped = len(daterange(start, end)) - len(dates)
    print(f"[history] {league.key.upper()} {start}~{end}: "
          f"{budget['dates']} 個比賽日"
          f"{f'（跳過 {skipped} 個無比賽日）' if skipped else ''} / "
          f"{budget['requests']} 次請求 / 約 {budget['credits']} credits")
    if not write:
        return []

    sport_key = SPORT_KEYS[league.key]
    session = session or requests.Session()
    all_rows, seen, credits = [], set(), {}
    for when in budget["times"]:
        try:
            raw, credits = fetch_snapshot(sport_key, when, api_key=api_key,
                                          session=session)
        except requests.RequestException as exc:
            print(f"[history] {when} 失敗（{exc}），跳過")
            continue
        rows = parse_snapshot(raw, league)
        # A slate that has not moved between two sampled instants comes back
        # twice; the ledger only wants one row per (event, snapshot).
        fresh = [r for r in rows
                 if (r["event_id"], r["captured_at"]) not in seen]
        seen.update((r["event_id"], r["captured_at"]) for r in fresh)
        all_rows.extend(fresh)
        print(f"[history] {when} -> {len(fresh)} 場"
              f"（剩 {credits.get('x-requests-remaining')} credits）")
        time.sleep(pause)

    if all_rows:
        by_capture = {}
        for row in all_rows:
            by_capture.setdefault(row["captured_at"], []).append(row)
        written = 0
        for captured_at, group in sorted(by_capture.items()):
            written += write_snapshots(
                snapshots_to_rows(group, snapshot_type, captured_at, league),
                league)
        print(f"[history] 寫入 {written} 列到『盤口』")
    return all_rows


def probe(league: LeagueSpec = NPB, *, api_key: str,
          dates: list[str] | None = None,
          session: requests.Session | None = None) -> None:
    """Cheapest possible check that Pinnacle really covers this league.

    Secondary leagues are where a provider's coverage quietly thins out, and
    the archive is the half nobody advertises — so sample one instant per
    season before paying for a plan sized to the whole range.
    """
    sport_key = SPORT_KEYS[league.key]
    session = session or requests.Session()
    dates = dates or ["2021-07-01", "2023-07-01", "2025-07-01"]
    print(f"[probe] {league.key.upper()} ({sport_key}) — "
          f"{credit_cost()} credits/次，共 {len(dates)} 次")
    for date in dates:
        when = snapshot_times(date, league)[-1]
        try:
            raw, credits = fetch_snapshot(sport_key, when, api_key=api_key,
                                          session=session)
        except requests.RequestException as exc:
            print(f"  {date}  失敗：{exc}")
            continue
        payload = raw.get("data")
        events = payload if isinstance(payload, list) else [payload] if payload else []
        rows = parse_snapshot(raw, league)
        books = sorted({b.get("key") for e in events
                        for b in (e.get("bookmakers") or [])})
        print(f"  {date}  賽事 {len(events)} 場，其中 Pinnacle 有報價 {len(rows)} 場"
              f"（剩 {credits.get('x-requests-remaining')} credits）")
        if events and not rows:
            print(f"    這個時點的書商：{books or '（無）'}")
        for row in rows[:3]:
            print(f"    {row['game_date']} {row['away_norm']} @ {row['home_norm']}"
                  f"  ML {row['ml_away']}/{row['ml_home']}"
                  f"  O/U {row['total_line']}  RL {row['spread_hdp']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill 盤口 from The Odds API's historical Pinnacle board")
    parser.add_argument("command", choices=["backfill", "probe"])
    parser.add_argument("--league", default="npb", choices=sorted(SPORT_KEYS))
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--snapshot-type", default="close",
                        choices=["open", "close", "interim"])
    parser.add_argument("--lead-minutes", type=int, default=10,
                        help="how long before first pitch to sample (default: 10)")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the request plan and its cost, spend nothing")
    parser.add_argument("--every-day", action="store_true",
                        help="do not skip days the box-score cache says had no games")
    args = parser.parse_args()

    api_key = os.getenv("ODDS_API_KEY")
    league = LEAGUES[args.league]
    if args.command == "probe":
        if not api_key:
            parser.error("ODDS_API_KEY is not set")
        probe(league, api_key=api_key)
        return

    if not args.start or not args.end:
        parser.error("backfill needs --start and --end")
    if not args.dry_run and not api_key:
        parser.error("ODDS_API_KEY is not set")
    backfill(args.start, args.end, league=league, api_key=api_key or "",
             snapshot_type=args.snapshot_type, lead_minutes=args.lead_minutes,
             write=not args.dry_run, every_day=args.every_day)


if __name__ == "__main__":
    main()
