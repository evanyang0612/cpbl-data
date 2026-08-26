"""NPB bullpen usage & fatigue.

Builds a per-appearance relief log from Yahoo Baseball box scores, tags every
appearance with the game state the pitcher inherited (inning + run differential),
and rolls that up into a rolling role assignment (勝利組 / 落後組) plus a
per-team daily fatigue snapshot.

Why the game state and not a hand-kept roster: NPB bullpen roles churn fast
(一軍登録・抹消 forces a 10-day absence), so a written-down 勝利の方程式 goes
stale within a fortnight. Classifying by the situations a pitcher is actually
trusted with re-derives the roster every day for free.

Two Yahoo pages per game:
  /stats — per-pitcher box score rows (result marker 勝/敗/Ｓ/Ｈ, IP, pitches,
           runs) plus the line score. Appearance order is table order.
  /text  — play-by-play. Its 投手交代 events, read in order, give the running
           score at the exact moment each reliever took the mound. Entry inning
           itself comes from cumulative outs off /stats, which is exact and
           needs no text at all; /text is only consulted for the score.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta

import aiohttp
from bs4 import BeautifulSoup as bs

BASE_URL = "https://baseball.yahoo.co.jp/npb/"
MAX_RETRY = 3
# Yahoo starts 500-ing the whole site — not just the endpoint being hit — after
# a sustained burst, and stays that way for a long while. A 30-day backfill is
# ~250 pages, so it is worth crawling it slowly rather than earning a block that
# outlasts the job. Cached games cost nothing, so a re-run after a block only
# fetches what is actually missing.
MAX_CONCURRENT_REQUESTS = 2
REQUEST_SPACING_SECONDS = 0.5

# Teams as Yahoo spells them, in standard NPB order (central then pacific).
NPB_TEAM_ORDER = [
    "巨人",
    "阪神",
    "DeNA",
    "広島",
    "ヤクルト",
    "中日",
    "ソフトバンク",
    "日本ハム",
    "オリックス",
    "ロッテ",
    "楽天",
    "西武",
]
YAHOO_TEAM_IDS = {
    "巨人": 1,
    "ヤクルト": 2,
    "DeNA": 3,
    "中日": 4,
    "阪神": 5,
    "広島": 6,
    "西武": 7,
    "日本ハム": 8,
    "ロッテ": 9,
    "オリックス": 11,
    "ソフトバンク": 12,
    "楽天": 376,
}
TEAM_ZH = {
    "巨人": "巨人",
    "阪神": "阪神",
    "DeNA": "橫濱",
    "広島": "廣島",
    "ヤクルト": "燕子",
    "中日": "中日",
    "ソフトバンク": "軟銀",
    "日本ハム": "火腿",
    "オリックス": "歐力士",
    "ロッテ": "羅德",
    "楽天": "樂天",
    "西武": "西武",
}

# --- Situation tags -------------------------------------------------------
SIT_START = "先發"
SIT_WIN = "勝利場面"
SIT_CLOSE = "接近"
SIT_MID = "中間"
SIT_LOSE = "落後"
SIT_MOP = "消化"

# --- Role tiers -----------------------------------------------------------
# Deliberately not "中間": that word already labels a situation (an inning and
# run differential that fits no other bucket), and the same word in the role
# column meant two different things on the same row.
ROLE_CLOSER = "終結"
ROLE_SETUP = "勝利組"
ROLE_SWING = "浮動"
ROLE_MOPUP = "落後組"
ROLE_THIN = "不足"
ROLE_STARTER = "先發"

# Tiers that pitch when the game is still in reach, and the ones that follow.
# ROLE_THIN sits in neither: too few outings to be evidence of anything, so it
# is absence of information rather than a middling role, and counting it as
# bullpen depth would overstate what a team has available.
ELITE_ROLES = (ROLE_CLOSER, ROLE_SETUP)
OTHER_ROLES = (ROLE_SWING, ROLE_MOPUP)

HIGH_LEVERAGE_SITUATIONS = {SIT_WIN, SIT_CLOSE}
LOW_LEVERAGE_SITUATIONS = {SIT_LOSE, SIT_MOP}

ROLE_WINDOW_DAYS = 14
MIN_APPEARANCES_FOR_ROLE = 3
HIGH_LEVERAGE_ROLE_RATIO = 0.60
LOW_LEVERAGE_ROLE_RATIO = 0.60
CLOSER_NINTH_RATIO = 0.40

# A reliever who has not pitched in this many days is treated as off the
# active roster (injured, 抹消, or 二軍) rather than merely rested.
ACTIVE_ROSTER_DAYS = 10

# Availability rules of thumb for "can he pitch today".
HEAVY_OUTING_PITCHES = 30
THREE_DAY_PITCH_LIMIT = 50
MAX_CONSECUTIVE_DAYS = 2


# --- Fetching -------------------------------------------------------------

_REQUEST_SEMAPHORES: "dict[asyncio.AbstractEventLoop, asyncio.Semaphore]" = {}


def _request_semaphore() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    sem = _REQUEST_SEMAPHORES.get(loop)
    if sem is None:
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        _REQUEST_SEMAPHORES[loop] = sem
    return sem


async def _fetch(session: aiohttp.ClientSession, url: str) -> str | None:
    semaphore = _request_semaphore()
    for attempt in range(MAX_RETRY + 1):
        body = None
        try:
            async with semaphore:
                async with session.get(url) as res:
                    if res.status == 200:
                        body = await res.text()
                # Space requests out while still holding the semaphore, so the
                # pacing holds however fast the server answers.
                await asyncio.sleep(REQUEST_SPACING_SECONDS)
        except Exception:
            pass
        if body is not None:
            return body
        if attempt < MAX_RETRY:
            # Yahoo answers a burst with 500s rather than 429s, and keeps doing
            # so for a while — back off geometrically instead of hammering.
            await asyncio.sleep(3 * 2**attempt)
    return None


async def fetch_finished_games_for_month(
    team_id: int, month: str, session: aiohttp.ClientSession
) -> dict[str, str]:
    """{game_id: YYYY-MM-DD} for one team's finished games in `month` (YYYY-MM).

    Read off the team calendar rather than the daily 日程 page: the daily page
    answers one date per request and starts returning 500s under a burst, while
    a month of calendar covers every team in 12 requests and carries the date in
    the cell itself, so games outside the window are dropped before we ever open
    a box score.
    """
    html = await _fetch(session, f"{BASE_URL}teams/{team_id}/schedule?month={month}")
    if not html:
        return None
    soup = bs(html, "html.parser")
    found: dict[str, str] = {}
    for cell in soup.find_all(class_="bb-calendarTable__data"):
        status = cell.find(class_="bb-calendarTable__status")
        if not status or status.get_text(strip=True) != "試合終了":
            continue
        day_el = cell.find(class_="bb-calendarTable__date")
        m = re.search(r"/npb/game/(\d+)", status.get("href", ""))
        if not (day_el and m and day_el.get_text(strip=True).isdigit()):
            continue
        found[m.group(1)] = f"{month}-{int(day_el.get_text(strip=True)):02d}"
    return found


def _schedule_cache_path(cache_dir: str) -> str:
    return os.path.join(cache_dir, "schedule.json")


def _load_schedule_cache(cache_dir: str | None) -> dict:
    """{"games": {game_id: date}, "calendars": [<team_id>:<month>, ...]}.

    The calendar list is what makes resuming correct: knowing a month has *some*
    cached games says nothing about whether all twelve teams were read, and a
    half-read month would silently drop the other teams' games from the window.
    """
    empty = {"games": {}, "calendars": []}
    if not cache_dir:
        return empty
    path = _schedule_cache_path(cache_dir)
    if not os.path.exists(path):
        return empty
    with open(path, encoding="utf-8") as fh:
        cached = json.load(fh)
    return {
        "games": cached.get("games", {}),
        "calendars": cached.get("calendars", []),
    }


def _save_schedule_cache(
    cache_dir: str | None, games: dict[str, str], calendar_key: str
) -> None:
    if not cache_dir:
        return
    os.makedirs(cache_dir, exist_ok=True)
    cached = _load_schedule_cache(cache_dir)
    cached["games"].update(games)
    if calendar_key not in cached["calendars"]:
        cached["calendars"].append(calendar_key)
    with open(_schedule_cache_path(cache_dir), "w", encoding="utf-8") as fh:
        json.dump(cached, fh)


async def fetch_finished_games_in_range(
    start: str, end: str, session: aiohttp.ClientSession, *, cache_dir: str | None = None
) -> dict[str, str]:
    """{game_id: date} for every finished game between `start` and `end`.

    Team calendars are walked one at a time and each success is merged into the
    cache immediately. Yahoo's throttle flaps — a window opens for a page or two
    and shuts again — so partial progress has to survive; a run that gets three
    teams should leave the next run only nine to fetch, not twelve.
    """
    months: list[str] = []
    cursor = datetime.strptime(start[:7] + "-01", "%Y-%m-%d")
    last = datetime.strptime(end[:7] + "-01", "%Y-%m-%d")
    while cursor <= last:
        months.append(cursor.strftime("%Y-%m"))
        cursor = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)

    cached = _load_schedule_cache(cache_dir)
    games = dict(cached["games"])
    # The month the window ends in is still gaining finished games, so its
    # calendars are always re-read however recently they were cached.
    open_month = end[:7]
    done = {key for key in cached["calendars"] if not key.endswith(f":{open_month}")}
    pending = [
        (team_id, month)
        for month in months
        for team_id in YAHOO_TEAM_IDS.values()
        if f"{team_id}:{month}" not in done
    ]

    failed = 0
    for team_id, month in pending:
        result = await fetch_finished_games_for_month(team_id, month, session)
        if result is None:
            failed += 1
            continue
        games.update(result)
        _save_schedule_cache(cache_dir, result, f"{team_id}:{month}")
    if failed:
        print(f"  [bullpen] schedule pages unavailable: {failed}/{len(pending)}")
    return {gid: date for gid, date in games.items() if start <= date <= end}


# --- Box score parsing ----------------------------------------------------

# Yahoo's pitcher table header labels → our field names. Read by label rather
# than position because Yahoo has changed the column set before (an older
# 10-cell layout had no 投球数 / ボーク).
_PITCH_HEADER_FIELDS = {
    "投球回": "ip_raw",
    "投球数": "pitches",
    "打者": "batters",
    "被安打": "hits",
    "被本塁打": "hr",
    "奪三振": "so",
    "与四球": "bb",
    "与死球": "hbp",
    "失点": "runs",
    "自責点": "er",
}


def outs_from_ip(ip_raw) -> int:
    """'5.1' → 16 outs. Yahoo writes thirds as .1/.2."""
    try:
        parts = str(ip_raw).strip().split(".")
        return int(parts[0]) * 3 + (int(parts[1]) if len(parts) > 1 else 0)
    except (ValueError, IndexError):
        return 0


def ip_display(outs: int) -> str:
    full, rem = divmod(outs, 3)
    return str(full) if rem == 0 else f"{full}.{rem}"


def _normalize_result_mark(text: str) -> str:
    """Yahoo mixes full-width (Ｓ) and half-width (H) markers."""
    return (
        text.strip()
        .replace("Ｓ", "S")
        .replace("Ｈ", "H")
        .replace("ｓ", "S")
        .replace("ｈ", "H")
    )


def _parse_pitcher_table(table) -> list[dict]:
    headers = [c.get_text(strip=True) for c in table.find_all(class_="bb-scoreTable__head")]
    pitchers: list[dict] = []
    for row in table.find_all(class_="bb-scoreTable__row"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 4:
            continue
        values = [c.get_text(strip=True) for c in cells]
        record: dict = {
            "result": _normalize_result_mark(values[0]),
            "name": re.sub(r"\s*[（(][右左][）)]\s*", "", values[1]).strip(),
            "player_id": "",
            "pitches": 0,
            "batters": 0,
            "hits": 0,
            "hr": 0,
            "so": 0,
            "bb": 0,
            "hbp": 0,
            "runs": 0,
            "er": 0,
        }
        link = cells[1].find("a", href=True)
        if link:
            m = re.search(r"/npb/player/(\d+)", link["href"])
            if m:
                record["player_id"] = m.group(1)
        for header, value in zip(headers, values):
            field = _PITCH_HEADER_FIELDS.get(header)
            if not field:
                continue
            if field == "ip_raw":
                record["outs"] = outs_from_ip(value)
            else:
                try:
                    record[field] = int(value)
                except ValueError:
                    record[field] = 0
        record.setdefault("outs", 0)
        pitchers.append(record)
    return pitchers


def parse_game_stats(stats_html: str) -> dict | None:
    """Teams, date, line score and both pitching staffs off the /stats page."""
    soup = bs(stats_html, "html.parser")

    team_els = soup.find_all(class_="bb-gameScoreTable__team")
    if len(team_els) < 2:
        return None
    away = team_els[0].get_text(strip=True)
    home = team_els[1].get_text(strip=True)
    if away not in TEAM_ZH or home not in TEAM_ZH:
        return None

    title = soup.find("title")
    m = re.search(r"(\d+年\d{1,2}月\d{1,2}日)", title.text if title else "")
    if not m:
        return None
    date_str = datetime.strptime(m.group(1), "%Y年%m月%d日").strftime("%Y-%m-%d")

    away_innings: list[int] = []
    home_innings: list[int] = []
    away_runs = home_runs = 0
    score_table = soup.find(class_="bb-gameScoreTable")
    if score_table:
        rows = score_table.find_all(class_="bb-gameScoreTable__row")
        for idx, row in enumerate(rows[:2]):
            innings = away_innings if idx == 0 else home_innings
            for cell in row.find_all(class_="bb-gameScoreTable__score"):
                raw = re.sub(r"[×Xx]+$", "", cell.get_text(strip=True))
                innings.append(int(raw) if raw.isdigit() else 0)
            total_el = row.find(class_="bb-gameScoreTable__total")
            total = int(total_el.get_text(strip=True)) if total_el and total_el.get_text(strip=True).isdigit() else 0
            if idx == 0:
                away_runs = total
            else:
                home_runs = total

    tables = soup.find_all(class_="bb-scoreTable")[:2]
    if len(tables) < 2:
        return None

    return {
        "date": date_str,
        "away": away,
        "home": home,
        "away_runs": away_runs,
        "home_runs": home_runs,
        "away_innings": away_innings,
        "home_innings": home_innings,
        # tables[0] = away pitchers, [1] = home pitchers
        "away_pitchers": _parse_pitcher_table(tables[0]),
        "home_pitchers": _parse_pitcher_table(tables[1]),
    }


# --- Play-by-play: score at each pitching change --------------------------

_SCORE_PAIR = re.compile(r"(\d+)\s*[-‐－ー]\s*(\d+)")
# Yahoo writes a between-innings swap as "投手交代: A → B" but a mid-inning one
# as "ピッチャー A に代わって B がマウンドにあがる". Both carry the same meaning
# and both must be caught, or every mid-inning entry silently goes missing.
_CHANGE_MARKERS = ("投手交代", "がマウンドにあがる")


def parse_pitching_changes(text_html: str) -> dict:
    """Score at the moment of every 投手交代, per pitching side.

    Returns {"away": [(a, b), ...], "home": [...]} in chronological order, where
    each tuple is the raw score pair as Yahoo prints it. Orientation (which slot
    is the home team) is resolved separately by `resolve_score_orientation`,
    since Yahoo prints the home team first — the opposite of the box score's
    row order — and that is worth verifying per game rather than assuming.
    """
    soup = bs(text_html, "html.parser")
    score = (0, 0)
    # Whether slot 0 went up while the away team was batting. Collected as
    # evidence; a single observation is enough to fix the orientation.
    slot0_scored_in_top = 0
    slot0_scored_in_bottom = 0
    changes: dict[str, list[tuple[int, int]]] = {"away": [], "home": []}

    for section in soup.find_all("section", class_="bb-liveText"):
        inning_el = section.find(class_="bb-liveText__inning")
        if not inning_el:
            continue
        inning_text = inning_el.get_text(strip=True)
        if "回表" in inning_text:
            batting, pitching = "away", "home"
        elif "回裏" in inning_text:
            batting, pitching = "home", "away"
        else:
            continue

        for item in section.find_all("li", class_="bb-liveText__item"):
            for summary in item.find_all(class_="bb-liveText__summary"):
                classes = summary.get("class") or []
                text = summary.get_text(" ", strip=True)

                if "bb-liveText__summary--change" in classes and any(
                    marker in text for marker in _CHANGE_MARKERS
                ):
                    # The incoming pitcher is the second player link on the line
                    # ("A → B" / "A に代わって B"); anything after belongs to a
                    # fielding change tacked onto the same summary.
                    links = summary.find_all("a", class_="bb-liveText__player")
                    player_id = ""
                    if len(links) >= 2:
                        m = re.search(r"/npb/player/(\d+)", links[1].get("href", ""))
                        if m:
                            player_id = m.group(1)
                    changes[pitching].append(
                        {
                            "score": score,
                            "player_id": player_id,
                            "inning": inning_text,
                        }
                    )

                if "bb-liveText__summary--point" in classes:
                    # The line reads "…タイムリー！ 巨 1-3 神 二三塁"; earlier
                    # digit pairs are ball-strike counts, so the score is the
                    # last pair on the line.
                    pairs = _SCORE_PAIR.findall(text)
                    if not pairs:
                        continue
                    new_score = (int(pairs[-1][0]), int(pairs[-1][1]))
                    if new_score[0] > score[0]:
                        if batting == "away":
                            slot0_scored_in_top += 1
                        else:
                            slot0_scored_in_bottom += 1
                    score = new_score

    return {
        "away": changes["away"],
        "home": changes["home"],
        "final": score,
        "slot0_top": slot0_scored_in_top,
        "slot0_bottom": slot0_scored_in_bottom,
    }


def resolve_score_orientation(changes: dict, away_runs: int, home_runs: int) -> str | None:
    """Which slot of the play-by-play score pair holds the away team.

    Decided by which slot went up while the away side was batting; falls back to
    matching the final pair against the box score. Returns "away_first",
    "home_first", or None when the game gave no usable evidence.
    """
    if changes["slot0_top"] and not changes["slot0_bottom"]:
        return "away_first"
    if changes["slot0_bottom"] and not changes["slot0_top"]:
        return "home_first"
    final = changes.get("final", (0, 0))
    if final == (away_runs, home_runs) and final != (home_runs, away_runs):
        return "away_first"
    if final == (home_runs, away_runs) and final != (away_runs, home_runs):
        return "home_first"
    return None


# --- Situation classification --------------------------------------------


def classify_situation(inning: int, diff: int) -> str:
    """Tag the game state a reliever inherited.

    `diff` is his own team's score minus the opponent's. Checked worst-first so
    the tags stay mutually exclusive: a five-run lead is mop-up no matter how
    late it is, and a three-run deficit is a losing spot even in the ninth.
    """
    if diff >= 5:
        return SIT_MOP
    if diff <= -3:
        return SIT_LOSE
    if inning >= 7 and 1 <= diff <= 3:
        return SIT_WIN
    if diff == 0 or (inning >= 6 and abs(diff) <= 2):
        return SIT_CLOSE
    return SIT_MID


# --- Appearances ----------------------------------------------------------


def build_appearances(game: dict, changes: dict | None) -> list[dict]:
    """One record per pitcher per game, in appearance order.

    Entry inning comes from cumulative outs (exact). Entry score comes from the
    play-by-play when the change events line up one-for-one with the relievers,
    otherwise from the line score at the start of the entry inning — which is
    off only for a mid-inning entry after runs had already scored that inning.
    """
    orientation = None
    if changes:
        orientation = resolve_score_orientation(
            changes, game["away_runs"], game["home_runs"]
        )

    records: list[dict] = []
    for side in ("away", "home"):
        pitchers = game[f"{side}_pitchers"]
        opponent = game["home"] if side == "away" else game["away"]
        own_innings = game[f"{side}_innings"]
        opp_innings = game["home_innings" if side == "away" else "away_innings"]

        # Entry score per reliever, indexed by his position in the box score.
        # Matched on player id where the play-by-play supplies one, so a missed
        # or spurious change event only costs that one reliever rather than
        # shifting every score after it.
        entry_scores: dict[int, tuple[int, int]] = {}
        if changes and orientation:
            raw = changes[side]
            aligned: list[tuple[int, dict]] = []
            if len(raw) == len(pitchers) - 1:
                aligned = list(enumerate(raw, start=1))
            else:
                by_id = {p["player_id"]: idx for idx, p in enumerate(pitchers) if p["player_id"]}
                aligned = [
                    (by_id[c["player_id"]], c)
                    for c in raw
                    if c.get("player_id") and c["player_id"] in by_id
                ]
            for idx, change in aligned:
                if orientation == "away_first":
                    away_score, home_score = change["score"]
                else:
                    home_score, away_score = change["score"]
                own = away_score if side == "away" else home_score
                opp = home_score if side == "away" else away_score
                entry_scores[idx] = (own, opp)

        cumulative_outs = 0
        for order, pitcher in enumerate(pitchers, start=1):
            inning = cumulative_outs // 3 + 1
            outs_in_inning = cumulative_outs % 3

            if order == 1:
                situation = SIT_START
                own_score = opp_score = 0
            else:
                if order - 1 in entry_scores:
                    own_score, opp_score = entry_scores[order - 1]
                    score_source = "text"
                else:
                    own_score = sum(own_innings[: inning - 1])
                    opp_score = sum(opp_innings[: inning - 1])
                    score_source = "linescore"
                situation = classify_situation(inning, own_score - opp_score)

            records.append(
                {
                    "date": game["date"],
                    "game_id": game.get("game_id", ""),
                    "team": game[side],
                    "opponent": opponent,
                    "home_away": "客" if side == "away" else "主",
                    "pitcher": pitcher["name"],
                    "player_id": pitcher["player_id"],
                    "order": order,
                    "entry_inning": inning,
                    "entry_outs": outs_in_inning,
                    "own_score": own_score,
                    "opp_score": opp_score,
                    "diff": own_score - opp_score,
                    "situation": situation,
                    "score_source": "" if order == 1 else score_source,
                    "outs": pitcher["outs"],
                    "ip": ip_display(pitcher["outs"]),
                    "pitches": pitcher["pitches"],
                    "batters": pitcher["batters"],
                    "hits": pitcher["hits"],
                    "hr": pitcher["hr"],
                    "bb": pitcher["bb"],
                    "hbp": pitcher["hbp"],
                    "so": pitcher["so"],
                    "runs": pitcher["runs"],
                    "er": pitcher["er"],
                    "result": pitcher["result"],
                }
            )
            cumulative_outs += pitcher["outs"]

    return records


def _cache_path(cache_dir: str, game_id: str) -> str:
    return os.path.join(cache_dir, f"{game_id}.json")


async def fetch_game_appearances(
    game_id: str, session: aiohttp.ClientSession, *, cache_dir: str | None = None
) -> list[dict]:
    """Appearances for one game, served from `cache_dir` when already scraped.

    A finished box score never changes, so caching makes a re-run of a 30-day
    backfill nearly free — which matters because Yahoo starts returning 500s
    well before a full backfill finishes.
    """
    if cache_dir:
        path = _cache_path(cache_dir, game_id)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)

    stats_html, text_html = await asyncio.gather(
        _fetch(session, f"{BASE_URL}game/{game_id}/stats"),
        _fetch(session, f"{BASE_URL}game/{game_id}/text"),
    )
    if not stats_html:
        return []
    game = parse_game_stats(stats_html)
    if not game:
        return []
    game["game_id"] = game_id
    changes = parse_pitching_changes(text_html) if text_html else None
    records = build_appearances(game, changes)
    # Only a game whose play-by-play came through is worth keeping: caching a
    # throttled fetch would freeze its inherited scores at the line-score
    # approximation forever, where a later run would have got them exactly.
    if cache_dir and records and text_html:
        os.makedirs(cache_dir, exist_ok=True)
        with open(_cache_path(cache_dir, game_id), "w", encoding="utf-8") as fh:
            json.dump(records, fh, ensure_ascii=False)
    return records


async def collect_appearances(
    days: int, *, end_date: datetime | None = None, cache_dir: str | None = None
) -> list[dict]:
    """Every pitching appearance in the `days` days ending at `end_date`."""
    end = end_date or datetime.now()
    end_key = end.strftime("%Y-%m-%d")
    start_key = (end - timedelta(days=days - 1)).strftime("%Y-%m-%d")

    timeout = aiohttp.ClientTimeout(total=60)
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        schedule = await fetch_finished_games_in_range(
            start_key, end_key, session, cache_dir=cache_dir
        )
        game_ids = sorted(schedule)
        print(
            f"  [bullpen] {len(game_ids)} finished games {start_key} → {end_key}"
        )

        results: list[dict] = []
        pending = list(game_ids)
        # A game that comes back empty is almost always Yahoo throttling us
        # rather than an unparseable box score, so sweep the stragglers again
        # after a cool-off instead of leaving holes in the log.
        for attempt in range(3):
            if not pending:
                break
            if attempt:
                print(f"  [bullpen] retry {len(pending)} games")
                await asyncio.sleep(30)
            failed: list[str] = []
            for start in range(0, len(pending), 6):
                batch = pending[start : start + 6]
                batch_results = await asyncio.gather(
                    *(
                        fetch_game_appearances(gid, session, cache_dir=cache_dir)
                        for gid in batch
                    ),
                    return_exceptions=True,
                )
                for gid, res in zip(batch, batch_results):
                    if isinstance(res, Exception) or not res:
                        failed.append(gid)
                    else:
                        results.extend(res)
                print(f"  [bullpen] {min(start + 6, len(pending))}/{len(pending)}")
                await asyncio.sleep(1)
            pending = failed
        if pending:
            print(f"  [bullpen] gave up on {len(pending)} games: {pending}")

    results = [r for r in results if start_key <= r["date"] <= end_key]
    results.sort(key=lambda r: (r["date"], r["game_id"], r["team"], r["order"]))
    return results


# --- Roles ----------------------------------------------------------------


def assign_roles(
    appearances: list[dict], as_of: str, window_days: int = ROLE_WINDOW_DAYS
) -> dict[tuple[str, str], dict]:
    """Roll each pitcher's recent situations up into a role tier.

    Keyed by (team, pitcher). A pitcher who mostly starts is a starter; the rest
    are ranked by what share of their outings came in high-leverage spots.
    """
    cutoff = (
        datetime.strptime(as_of, "%Y-%m-%d") - timedelta(days=window_days - 1)
    ).strftime("%Y-%m-%d")

    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for rec in appearances:
        if cutoff <= rec["date"] <= as_of:
            grouped[(rec["team"], rec["pitcher"])].append(rec)

    roles: dict[tuple[str, str], dict] = {}
    for key, recs in grouped.items():
        starts = [r for r in recs if r["situation"] == SIT_START]
        relief = [r for r in recs if r["situation"] != SIT_START]
        if len(starts) >= len(relief):
            roles[key] = {
                "role": ROLE_STARTER,
                "appearances": len(recs),
                "high_ratio": 0.0,
                "ninth_ratio": 0.0,
                "saves": 0,
                "holds": 0,
            }
            continue

        total = len(relief)
        high = sum(1 for r in relief if r["situation"] in HIGH_LEVERAGE_SITUATIONS)
        low = sum(1 for r in relief if r["situation"] in LOW_LEVERAGE_SITUATIONS)
        ninth = sum(1 for r in relief if r["entry_inning"] >= 9)
        saves = sum(1 for r in relief if r["result"] == "S")
        holds = sum(1 for r in relief if r["result"] == "H")

        high_ratio = high / total
        ninth_ratio = ninth / total

        if total < MIN_APPEARANCES_FOR_ROLE:
            # Too little to judge on situations alone; a save or hold in the
            # window is still direct evidence of a high-leverage assignment.
            role = ROLE_SETUP if (saves or holds) else ROLE_THIN
        elif ninth_ratio >= CLOSER_NINTH_RATIO and high_ratio >= 0.5 and saves:
            # A save is what separates a closer from a committee that happens to
            # eat ninth innings: working tied and trailing ninths produces the
            # same ninth-inning share, and labelling all of them 終結 reads as
            # three closers on one staff.
            role = ROLE_CLOSER
        elif high_ratio >= HIGH_LEVERAGE_ROLE_RATIO or saves or holds >= 2:
            role = ROLE_SETUP
        elif low / total >= LOW_LEVERAGE_ROLE_RATIO:
            role = ROLE_MOPUP
        else:
            # Enough outings to judge, but trusted with both leads and deficits.
            role = ROLE_SWING

        roles[key] = {
            "role": role,
            "appearances": total,
            "high_ratio": round(high_ratio, 3),
            "ninth_ratio": round(ninth_ratio, 3),
            "saves": saves,
            "holds": holds,
        }
    return roles


# --- Fatigue --------------------------------------------------------------


def pitcher_fatigue(recs: list[dict], as_of: str) -> dict:
    """Rest state for one pitcher heading into the day after `as_of`."""
    as_of_date = datetime.strptime(as_of, "%Y-%m-%d")
    by_date: dict[str, list[dict]] = defaultdict(list)
    for rec in recs:
        by_date[rec["date"]].append(rec)

    def day(offset: int) -> str:
        return (as_of_date - timedelta(days=offset)).strftime("%Y-%m-%d")

    # Consecutive days pitched ending on `as_of`, counting back day by day.
    streak = 0
    while by_date.get(day(streak)):
        streak += 1

    pitches_1 = sum(r["pitches"] for r in by_date.get(day(0), []))
    pitches_3 = sum(
        r["pitches"] for offset in range(3) for r in by_date.get(day(offset), [])
    )
    last_date = max(by_date) if by_date else ""
    days_since = (
        (as_of_date - datetime.strptime(last_date, "%Y-%m-%d")).days if last_date else 99
    )

    active = days_since <= ACTIVE_ROSTER_DAYS
    available = active and not (
        streak >= MAX_CONSECUTIVE_DAYS
        or pitches_1 >= HEAVY_OUTING_PITCHES
        or pitches_3 >= THREE_DAY_PITCH_LIMIT
    )
    return {
        "streak": streak,
        "pitches_1": pitches_1,
        "pitches_3": pitches_3,
        "days_since": days_since,
        "active": active,
        "available": available,
    }


def _recent_team_games(recs: list[dict], limit: int) -> list[str]:
    """The team's most recent `limit` game ids, newest first."""
    seen = {(r["date"], r["game_id"]) for r in recs}
    return [gid for _, gid in sorted(seen, reverse=True)[:limit]]


def team_fatigue_rows(
    appearances: list[dict], as_of: str, *, recent_games: int = 5
) -> list[dict]:
    """One snapshot row per team for the day after `as_of`."""
    roles = assign_roles(appearances, as_of)

    by_team: dict[str, list[dict]] = defaultdict(list)
    for rec in appearances:
        if rec["date"] <= as_of:
            by_team[rec["team"]].append(rec)

    rows: list[dict] = []
    for team in NPB_TEAM_ORDER:
        recs = by_team.get(team, [])
        by_pitcher: dict[str, list[dict]] = defaultdict(list)
        for rec in recs:
            by_pitcher[rec["pitcher"]].append(rec)

        high_leverage_names: list[str] = []
        available_names: list[str] = []
        other_names: list[str] = []
        other_available: list[str] = []
        back_to_back = 0
        heavy_yesterday = 0

        for pitcher, precs in by_pitcher.items():
            info = roles.get((team, pitcher))
            if not info or info["role"] == ROLE_STARTER:
                continue
            fatigue = pitcher_fatigue(precs, as_of)
            if not fatigue["active"]:
                continue
            if fatigue["streak"] >= 1:
                back_to_back += 1
            if fatigue["pitches_1"] >= 25:
                heavy_yesterday += 1
            if info["role"] in ELITE_ROLES:
                high_leverage_names.append(f"{pitcher}({info['role'][:1]})")
                if fatigue["available"]:
                    available_names.append(pitcher)
            elif info["role"] in OTHER_ROLES:
                other_names.append(f"{pitcher}({info['role'][:1]})")
                if fatigue["available"]:
                    other_available.append(pitcher)

        # Bullpen workload over the team's last N games.
        game_ids = set(_recent_team_games(recs, recent_games))
        window = [r for r in recs if r["game_id"] in game_ids]
        relief = [r for r in window if r["situation"] != SIT_START]
        starters = [r for r in window if r["situation"] == SIT_START]

        relief_outs = sum(r["outs"] for r in relief)
        relief_runs = sum(r["runs"] for r in relief)
        relief_er = sum(r["er"] for r in relief)
        relief_hits = sum(r["hits"] for r in relief)
        relief_walks = sum(r["bb"] for r in relief)
        starter_outs = sum(r["outs"] for r in starters)
        games = max(len(game_ids), 1)

        # Tier ERAs run over the role window, not the last five games: split by
        # tier, five games leaves each side only five to ten innings, which is
        # too little to read. Fourteen days is still small — say so on the sheet.
        tier_cutoff = (
            datetime.strptime(as_of, "%Y-%m-%d") - timedelta(days=ROLE_WINDOW_DAYS - 1)
        ).strftime("%Y-%m-%d")
        tier_window = [
            r
            for r in recs
            if tier_cutoff <= r["date"] <= as_of and r["situation"] != SIT_START
        ]
        elite_outs = elite_er = other_outs = other_er = 0
        for rec in tier_window:
            role = roles.get((team, rec["pitcher"]), {}).get("role")
            if role in ELITE_ROLES:
                elite_outs += rec["outs"]
                elite_er += rec["er"]
            elif role in OTHER_ROLES:
                other_outs += rec["outs"]
                other_er += rec["er"]

        starter_ip_per_game = starter_outs / 3 / games

        rows.append(
            {
                "team": team,
                "team_zh": TEAM_ZH[team],
                "back_to_back": back_to_back,
                "heavy_yesterday": heavy_yesterday,
                "relief_ip": round(relief_outs / 3, 1),
                "relief_ip_per_game": round(relief_outs / 3 / games, 2),
                "relief_runs": relief_runs,
                "relief_er": relief_er,
                # Reported as ERA rather than runs per inning: same information,
                # but on the scale every other pitching number is already read on.
                "relief_era": round(relief_er * 9 / (relief_outs / 3), 2)
                if relief_outs
                else 0.0,
                "relief_whip": round((relief_hits + relief_walks) / (relief_outs / 3), 2)
                if relief_outs
                else 0.0,
                "starter_ip_per_game": round(starter_ip_per_game, 2),
                # What the bullpen is on the hook for tonight if the starter
                # goes his usual length. Nine innings is the home-team floor;
                # extras and away wins move it, but as a planning number it is
                # what makes a short-starting team's bullpen ERA matter more.
                "projected_relief_ip": round(max(9 - starter_ip_per_game, 0), 2),
                "elite_total": len(high_leverage_names),
                "elite_available": len(available_names),
                "other_total": len(other_names),
                "other_available": len(other_available),
                "elite_ip": round(elite_outs / 3, 1),
                "elite_era": round(elite_er * 9 / (elite_outs / 3), 2)
                if elite_outs
                else 0.0,
                "other_ip": round(other_outs / 3, 1),
                "other_era": round(other_er * 9 / (other_outs / 3), 2)
                if other_outs
                else 0.0,
                "games": len(game_ids),
                "high_leverage_names": "、".join(high_leverage_names),
                "available_names": "、".join(available_names),
                "other_names": "、".join(other_names),
            }
        )
    return rows
