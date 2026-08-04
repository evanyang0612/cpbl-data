"""Scrape NPB and MLB betting lines from the PS3838 public compact odds feed.

PS3838's web app is a React SPA that reads odds from a public JSON endpoint
(`/sports-service/sv/compact/events`) which needs **no login and no API
access** — unlike the official `api.ps3838.com` API, which is gated behind a
funded/API-enabled account (`NO_API_ACCESS`). We hit the same endpoint the
browser uses, with the exact query the SPA sends for baseball (`sp=3`).

Feed shape (positional/compact arrays):

    { "l": [ [sportId, sportName, [ [leagueId, leagueName, [ event, ... ]] ]] ] }

Each event:

    [ event_id, home_zh, away_zh, ?, start_ms, ..., odds, ... ]

`odds` is keyed by period ("0" = full game). Each period is
`[spreads, totals, moneyline]`:

    spreads  : [[home_hdp, away_hdp, line_str, home_price, away_price, ...], ...]
    totals   : [[points_str, points, over_price, under_price, id, ...], ...]
    moneyline: [home_price, away_price, draw_price, id, ...]

Prices are decimal odds as strings ("1.826"); "" means no price.

This module fetches, parses, and (optionally) appends snapshot rows to a
``盤口`` worksheet, one per league: NPB lines go to the main NPB spreadsheet
(``NPB_SPREADSHEET_KEY``, alongside 彙資/分析表紀錄), MLB lines to the MLB
spreadsheet (alongside 紀錄). Both so open/close lines can be compared against
the recorded games to measure edge. Pick the league with ``--league`` and
override the target sheet with ``ODDS_SPREADSHEET_KEY`` /
``MLB_ODDS_SPREADSHEET_KEY``.
"""

import argparse
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

import requests

BASE_URL = os.getenv("PS3838_WEB_BASE", "https://www.ps3838.com").rstrip("/")
EVENTS_PATH = "/sports-service/sv/compact/events"
BASEBALL_SPORT_ID = 3
NPB_LEAGUE_ID = 187703  # 日本職業棒球賽
MLB_LEAGUE_ID = 246     # MLB

JST = timezone(timedelta(hours=9))
# MLB game dates follow the ballpark's local day, so US Eastern (with DST) is
# the closest single zone for labelling snapshots; the authoritative
# ``officialDate`` still comes from the MLB schedule during enrichment.
ET = ZoneInfo("America/New_York")

# The exact query the SPA sends for baseball. `sp=3` selects baseball,
# `pimo=0,1` requests full-game (period 0) and 1st-5-innings (period 1),
# `o=1` = decimal odds, `mk=1` = full snapshot (mk=2 is a delta update).
EVENTS_PARAMS = {
    "btg": 1, "c": "", "cl": 3, "d": "", "ec": "", "ev": "", "g": "",
    "hle": "false", "ic": "false", "ice": "false", "inl": "false",
    "l": 3, "lang": "", "lg": "", "lv": 0, "me": 0, "me01": "",
    "mk": 1, "more": "false", "o": 1, "ot": 1, "pa": 0, "pimo": "0,1",
    "pn": -1, "pv": 1, "ru": "", "sp": BASEBALL_SPORT_ID, "tm": 0, "v": 0,
    "locale": "zh_TW", "withCredentials": "true",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.ps3838.com/zh-tw/sports/baseball",
}

# PS3838 (zh) team name -> the internal Japanese short name used elsewhere in
# the repo. Matched by distinctive substring so minor name variants still map.
TEAM_SUBSTRING_MAP = {
    "巨人": "巨人",          # Yomiuri Giants
    "養樂多": "ヤクルト",     # Yakult Swallows
    "燕子": "ヤクルト",
    "DeNA": "DeNA",          # Yokohama DeNA BayStars
    "海灣": "DeNA",
    "灣星": "DeNA",
    "中日": "中日",          # Chunichi Dragons
    "阪神": "阪神",          # Hanshin Tigers
    "廣島": "広島",          # Hiroshima Carp
    "西武": "西武",          # Seibu Lions
    "火腿": "日本ハム",       # Hokkaido Nippon-Ham Fighters
    "羅德": "ロッテ",         # Chiba Lotte Marines
    "歐力士": "オリックス",   # Orix Buffaloes
    "軟銀": "ソフトバンク",   # SoftBank Hawks
    "樂天": "楽天",          # Rakuten Eagles
}

# Period key ("0" = full game, "1" = 1st 5 innings). Anything not listed here
# is skipped; parse_period() warns about keys that are neither recorded nor
# knowingly ignored, so a new market shows up in the logs.
PERIOD_LABELS = {"0": "final", "1": "half"}

# Period "3" is the 1st-inning 3-way market (0.5 total, moneyline with a draw
# price). MLB events always carry it. We don't bet it, so skip it quietly.
IGNORED_PERIOD_KEYS = {"3"}

SHEET_NAME = "盤口"


def _sheet_headers(start_column: str, game_id_column: str,
                   extra_team_columns: tuple[str, ...] = ()) -> list[str]:
    """Column layout shared by every league's 盤口 sheet.

    Only the start-time column, the join-key column, and any extra team
    identifiers differ between leagues.
    """
    return [
        "captured_at",       # when this snapshot was taken (league-local time)
        "snapshot_type",     # open / close / interim
        "event_id",          # PS3838 event id
        "game_date",         # YYYY-MM-DD
        start_column,        # ISO start time (league-local)
        "league",            # league name
        "home_team",         # PS3838 zh name
        "away_team",
        "home_norm",         # name matching how the league's sheets spell it
        "away_norm",
        *extra_team_columns,
        game_id_column,      # join key into the league's record sheet
        "status",            # pregame / live
        "mins_to_start",     # minutes until first pitch (negative once live)
        "period",            # final / half
        "ml_home", "ml_away", "ml_draw",
        "total_line", "total_over", "total_under",
        "spread_hdp", "spread_home", "spread_away",
        "all_totals",        # JSON of every total line (backtest flexibility)
        "all_spreads",       # JSON of every spread line
    ]


# The NPB sheet predates the multi-league split; its column order is frozen.
SHEET_HEADERS = _sheet_headers("start_jst", "npb_game_id")


def _price(value):
    """Decimal-odds string -> float, or None when blank/zero."""
    if value in (None, "", "0", 0):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _clean(name) -> str:
    """Feed names occasionally carry stray whitespace/newlines ("響尾蛇\\n")."""
    return str(name or "").strip()


def normalize_team(zh_name: str) -> str:
    for needle, norm in TEAM_SUBSTRING_MAP.items():
        if needle in (zh_name or ""):
            return norm
    return ""


def _npb_team_fields(ev: list) -> dict:
    """NPB sheets key teams by Japanese short name, mapped from the zh feed name."""
    return {
        "home_norm": normalize_team(ev[1]),
        "away_norm": normalize_team(ev[2]),
    }


def _mlb_team_fields(ev: list) -> dict:
    """MLB events carry English names at [24]/[25] — the MLB Stats API spelling."""
    return {
        "home_norm": _clean(ev[24]) if len(ev) > 24 else "",
        "away_norm": _clean(ev[25]) if len(ev) > 25 else "",
    }


@dataclass(frozen=True)
class LeagueSpec:
    key: str                        # --league value
    league_id: int
    tz: timezone | ZoneInfo
    start_column: str               # ISO start-time column name
    game_id_column: str            # join key into the league's record sheet
    team_fields: Callable[[list], dict]
    spreadsheet_env: str            # env var overriding the target spreadsheet
    default_spreadsheet_key: Callable[[], str]
    extra_team_columns: tuple[str, ...] = ()
    # Optional post-parse pass that resolves the join key from the league's own
    # API (see enrich_mlb); receives and mutates the snapshot list.
    enrich: Callable[[list[dict]], None] | None = None
    headers: list[str] = field(default_factory=list)

    def sheet_headers(self) -> list[str]:
        return self.headers or _sheet_headers(
            self.start_column, self.game_id_column, self.extra_team_columns
        )

    def spreadsheet_key(self) -> str:
        return os.getenv(self.spreadsheet_env) or self.default_spreadsheet_key()


def _npb_spreadsheet_key() -> str:
    # Odds live in the main NPB spreadsheet (alongside 彙資 / 分析表紀錄), in a
    # dedicated 盤口 tab.
    import npb
    return npb.NPB_SPREADSHEET_KEY


def _mlb_spreadsheet_key() -> str:
    # Same spreadsheet as the MLB 紀錄 worksheet (see
    # migration/update_mlb_record.py), so odds can be joined by gamePk.
    return "11FV70TXVAxLTwYH6pLj7HwK1qq-fIa61QrePRCC8YUM"


NPB = LeagueSpec(
    key="npb",
    league_id=NPB_LEAGUE_ID,
    tz=JST,
    start_column="start_jst",
    game_id_column="npb_game_id",
    team_fields=_npb_team_fields,
    spreadsheet_env="ODDS_SPREADSHEET_KEY",
    default_spreadsheet_key=_npb_spreadsheet_key,
    headers=SHEET_HEADERS,
)

MLB = LeagueSpec(
    key="mlb",
    league_id=MLB_LEAGUE_ID,
    tz=ET,
    start_column="start_et",
    game_id_column="mlb_game_pk",
    team_fields=_mlb_team_fields,
    extra_team_columns=("home_abbr", "away_abbr"),
    spreadsheet_env="MLB_ODDS_SPREADSHEET_KEY",
    default_spreadsheet_key=_mlb_spreadsheet_key,
    enrich=lambda snapshots: enrich_mlb(snapshots),
)

LEAGUES = {spec.key: spec for spec in (NPB, MLB)}


def _build_session() -> requests.Session:
    """Session that routes through the Decodo proxy when configured.

    PS3838 geo-blocks datacenter IPs (e.g. GitHub Actions runners get 403), so
    on CI we tunnel through the same residential proxy the CPBL scraper uses.
    Locally, with no proxy set, requests go out directly.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    proxy_url = os.environ.get("DECODO_PROXY_URL")
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}
    return session


def fetch_baseball_events(*, session: requests.Session | None = None,
                          timeout: int = 30) -> dict:
    getter = session or _build_session()
    resp = getter.get(
        f"{BASE_URL}{EVENTS_PATH}", params=EVENTS_PARAMS,
        headers=HEADERS, timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def parse_moneyline(block) -> dict:
    ml = block[2] if len(block) > 2 and isinstance(block[2], list) else []
    return {
        "ml_home": _price(ml[0]) if len(ml) > 0 else None,
        "ml_away": _price(ml[1]) if len(ml) > 1 else None,
        "ml_draw": _price(ml[2]) if len(ml) > 2 else None,
    }


def _main_total(totals: list) -> dict:
    """Pick the primary total: the line with the most balanced over/under juice."""
    parsed = []
    for t in totals:
        if len(t) < 4:
            continue
        points = t[1]
        over, under = _price(t[2]), _price(t[3])
        if over is None or under is None or points in (None, "", 0):
            continue
        parsed.append({"line": points, "over": over, "under": under})
    if not parsed:
        return {"total_line": None, "total_over": None, "total_under": None,
                "all_totals": parsed}
    main = min(parsed, key=lambda x: abs(x["over"] - x["under"]))
    return {
        "total_line": main["line"], "total_over": main["over"],
        "total_under": main["under"], "all_totals": parsed,
    }


def _main_spread(spreads: list) -> dict:
    """Pick the primary run line: prefer ±1.5, else most balanced juice."""
    parsed = []
    for s in spreads:
        if len(s) < 5:
            continue
        home_hdp = s[0]
        home_price, away_price = _price(s[3]), _price(s[4])
        if home_price is None or away_price is None or s[2] in (None, ""):
            continue
        parsed.append({"hdp": home_hdp, "home": home_price, "away": away_price})
    if not parsed:
        return {"spread_hdp": None, "spread_home": None, "spread_away": None,
                "all_spreads": parsed}
    exact = [p for p in parsed if abs(p["hdp"]) == 1.5]
    pool = exact or parsed
    main = min(pool, key=lambda x: abs(x["home"] - x["away"]))
    return {
        "spread_hdp": main["hdp"], "spread_home": main["home"],
        "spread_away": main["away"], "all_spreads": parsed,
    }


def parse_period(period_key: str, block) -> dict | None:
    if not isinstance(block, list) or len(block) < 3:
        return None
    label = PERIOD_LABELS.get(period_key)
    if label is None:
        if period_key not in IGNORED_PERIOD_KEYS:
            # Unmapped period (e.g. a new in-play segment) — skip but make it
            # visible so we can decide whether to record it.
            print(f"[odds] skipping unmapped period key {period_key!r}")
        return None
    spreads = block[0] if len(block) > 0 and isinstance(block[0], list) else []
    totals = block[1] if len(block) > 1 and isinstance(block[1], list) else []
    row = {"period": label}
    row.update(parse_moneyline(block))
    row.update(_main_total(totals))
    row.update(_main_spread(spreads))
    # A period with no live prices at all (closed) is not worth a row.
    if not any(row.get(k) is not None for k in (
        "ml_home", "ml_away", "total_line", "spread_home")):
        return None
    return row


def parse_events(raw: dict, *, include_live: bool = False,
                 league: LeagueSpec = NPB,
                 all_leagues: bool = False) -> list[dict]:
    """Flatten the feed into one dict per (event, period) with parsed odds.

    The compact feed splits games into two buckets: ``"n"`` = non-live
    (pre-game) and ``"l"`` = live (走地). Pre-game lines are what we want for
    edge; live games are skipped by default.

    Only ``league``'s events are returned unless ``all_leagues`` is set (used
    by probes to see everything PS3838 currently books).
    """
    out = []
    now_ms = datetime.now(tz=timezone.utc).timestamp() * 1000
    buckets = [("n", False), ("l", True)]
    for bucket_key, bucket_live in buckets:
        for sport in raw.get(bucket_key) or []:
            if sport[0] != BASEBALL_SPORT_ID:
                continue
            for league_row in sport[2]:
                league_id, league_name = league_row[0], league_row[1]
                if not all_leagues and league_id != league.league_id:
                    continue
                for ev in league_row[2]:
                    row = _parse_one_event(
                        ev, league_id, league_name, now_ms, league,
                        bucket_live=bucket_live, include_live=include_live,
                    )
                    out.extend(row)
    return out


def _parse_one_event(ev, league_id, league_name, now_ms, league: LeagueSpec, *,
                     bucket_live: bool, include_live: bool) -> list[dict]:
    out = []
    event_id = ev[0]
    start_ms = ev[4] if len(ev) > 4 and ev[4] else None
    odds = ev[8] if len(ev) > 8 and isinstance(ev[8], dict) else {}
    # The feed bucket is authoritative: "n" = pre-game, "l" = live (走地).
    live = bucket_live
    if live and not include_live:
        return out
    start_dt = (
        datetime.fromtimestamp(start_ms / 1000, tz=league.tz) if start_ms else None
    )
    mins_to_start = round((start_ms - now_ms) / 60000) if start_ms else ""
    base = {
        "event_id": event_id,
        "league": league_name,
        "league_id": league_id,
        "home_team": _clean(ev[1]),
        "away_team": _clean(ev[2]),
        **league.team_fields(ev),
        league.start_column: start_dt.isoformat() if start_dt else "",
        "start": start_dt,
        "game_date": start_dt.strftime("%Y-%m-%d") if start_dt else "",
        "status": "live" if live else "pregame",
        "mins_to_start": mins_to_start,
    }
    for pk, block in odds.items():
        parsed = parse_period(str(pk), block)
        if parsed:
            out.append({**base, **parsed})
    return out


def enrich_mlb(snapshots: list[dict]) -> None:
    """Fill in ``mlb_game_pk``, ``officialDate`` and abbreviations in place.

    Resolved from the MLB Stats API schedule so every row joins to the ``紀錄``
    worksheet by gamePk. A game we cannot match keeps its feed-derived date and
    a blank gamePk rather than blocking the snapshot.
    """
    from baseball.mlb_games import build_index

    starts = [s["start"] for s in snapshots if s.get("start")]
    if not starts:
        return
    try:
        index = build_index(starts)
    except requests.RequestException as exc:
        print(f"[odds] MLB schedule lookup failed ({exc}); rows keep blank gamePk")
        return
    unmatched = 0
    for s in snapshots:
        game = index.find(s.get("home_norm", ""), s.get("away_norm", ""),
                          s.get("start"))
        if not game:
            unmatched += 1
            continue
        s["mlb_game_pk"] = game["game_pk"]
        s["home_abbr"] = game["home_abbr"]
        s["away_abbr"] = game["away_abbr"]
        if game["official_date"]:
            s["game_date"] = game["official_date"]
    if unmatched:
        print(f"[odds] {unmatched} snapshot(s) had no MLB schedule match")


def snapshots_to_rows(snapshots: list[dict], snapshot_type: str,
                      captured_at: str, league: LeagueSpec = NPB) -> list[list]:
    headers = league.sheet_headers()
    rows = []
    for s in snapshots:
        record = {
            "captured_at": captured_at,
            "snapshot_type": snapshot_type,
            league.game_id_column: "",
            **s,
            "all_totals": json.dumps(s.get("all_totals", []), ensure_ascii=False),
            "all_spreads": json.dumps(s.get("all_spreads", []), ensure_ascii=False),
        }
        rows.append([
            "" if record.get(h) is None else record.get(h, "")
            for h in headers
        ])
    return rows


def _open_worksheet(league: LeagueSpec):
    from baseball.sheets import GoogleSheetsClient

    headers = league.sheet_headers()
    client = GoogleSheetsClient()
    spreadsheet = client.spreadsheet(league.spreadsheet_key())
    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except Exception:  # gspread WorksheetNotFound
        ws = spreadsheet.add_worksheet(
            title=SHEET_NAME, rows=1000, cols=len(headers)
        )
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws
    values = ws.get_all_values()
    if not values:
        ws.append_row(headers, value_input_option="USER_ENTERED")
    return ws


def write_snapshots(rows: list[list], league: LeagueSpec = NPB) -> int:
    if not rows:
        return 0
    ws = _open_worksheet(league)
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


def _now_local(league: LeagueSpec) -> str:
    return datetime.now(tz=league.tz).strftime("%Y-%m-%d %H:%M:%S")


def run_once(snapshot_type: str = "interim", *, write: bool = True,
             include_live: bool = False, league: LeagueSpec = NPB) -> list[dict]:
    raw = fetch_baseball_events()
    snapshots = parse_events(raw, include_live=include_live, league=league)
    if league.enrich and snapshots:
        league.enrich(snapshots)
    captured_at = _now_local(league)
    print(f"[odds] {captured_at} {league.key.upper()}: parsed {len(snapshots)} "
          "pre-game (event, period) snapshots")
    for s in snapshots:
        print(
            f"  {s['game_date']} {s['away_team']}({s['away_norm']}) @ "
            f"{s['home_team']}({s['home_norm']}) [{s['status']}/{s['period']}] "
            f"T-{s.get('mins_to_start')}m "
            f"ML {s.get('ml_away')}/{s.get('ml_home')} "
            f"O/U {s.get('total_line')} {s.get('total_over')}/{s.get('total_under')} "
            f"RL {s.get('spread_hdp')} {s.get('spread_home')}/{s.get('spread_away')}"
        )
    if write:
        rows = snapshots_to_rows(snapshots, snapshot_type, captured_at, league)
        n = write_snapshots(rows, league)
        print(f"[odds] wrote {n} rows to '{SHEET_NAME}'")
    return snapshots


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape NPB / MLB odds from PS3838")
    parser.add_argument(
        "--league", default="npb", choices=sorted(LEAGUES),
        help="which league's lines to scrape (default: npb)",
    )
    parser.add_argument(
        "--snapshot-type", default="interim",
        choices=["open", "close", "interim"],
        help="label stored with the snapshot",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="fetch and print parsed odds without writing to the sheet",
    )
    parser.add_argument(
        "--include-live", action="store_true",
        help="also show in-play (走地) games — for inspection, not for the ledger",
    )
    args = parser.parse_args()
    run_once(args.snapshot_type, write=not args.dry_run,
             include_live=args.include_live, league=LEAGUES[args.league])


if __name__ == "__main__":
    main()
