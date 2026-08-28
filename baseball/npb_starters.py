"""Look up the announced starting pitchers for a day's NPB games.

Yahoo's schedule page carries no starters itself, only links to each game, so
the names come from the game pages: a scheduled game's page compares the two
announced starters in a 予告先発 table, home first. Each name links to the
player, and the id in that link is what lets the broadcast point at their page.

Starters are announced the day before, so they are usually in place by the time
the opening line is broadcast — but not always, and a game whose starters are
still blank simply contributes nothing.
"""

import re
import time
from dataclasses import dataclass, replace

import requests

BASE_URL = "https://baseball.yahoo.co.jp/npb/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

# Yahoo spells teams out in full ("東京ヤクルトスワローズ"); the odds feed and the
# rest of the repo use the short form. Matched on the distinctive substring so
# sponsor-name changes do not break the join.
TEAM_SUBSTRINGS = {
    "ヤクルト": "ヤクルト",
    "ジャイアンツ": "巨人",
    "ロッテ": "ロッテ",
    "ソフトバンク": "ソフトバンク",
    "オリックス": "オリックス",
    "DeNA": "DeNA",
    "日本ハム": "日本ハム",
    "西武": "西武",
    "楽天": "楽天",
    "カープ": "広島",
    "中日": "中日",
    "阪神": "阪神",
}

_GAME_ID = re.compile(r"/npb/game/(\d+)/")
_WEATHER = re.compile(
    r'class="bb-gameCard__weather"\s+href="([^"]+)".*?alt="([^"]*)"', re.S)
_PLAYER_ID = re.compile(r"/npb/player/(\d+)")
# The 予告先発 comparison table, which is the only place on the page the two
# probable starters are named. Its name cell carries the bare class; the number
# and the throwing hand sit in modified ones, so matching the bare class picks
# out one link per pitcher, home first.
_PROBABLE_STARTER = re.compile(
    r'class="bb-splitsPitcherTable__data">\s*'
    r'<a href="/npb/player/(\d+)/[^"]*"[^>]*>(.*?)</a>', re.S)
_TITLE_TEAMS = re.compile(r"(\S+)vs\.(\S+?)(?:\s|-|$)")


@dataclass(frozen=True)
class Starter:
    name: str
    player_id: str

    @property
    def url(self) -> str:
        return f"{BASE_URL}player/{self.player_id}/top"


@dataclass(frozen=True)
class Weather:
    """The pinpoint forecast for a game's start time.

    Yahoo prints only an icon against the game itself; the numbers come from
    the pinpoint page it links to, which tabulates the day in three-hour steps.
    """

    condition: str
    temp_c: str | None = None
    rain_mm: str | None = None
    wind: str | None = None
    url: str = ""
    venue: str = ""

    def is_wet(self) -> bool:
        """Enough rain at first pitch to be worth a reader's attention."""
        try:
            return float(self.rain_mm) >= RAIN_FLAG_MM
        except (TypeError, ValueError):
            return False

    def summary(self) -> str:
        if is_sheltered(self.venue):
            return f"{self.temp_c}℃" if self.temp_c else ""
        parts = [self.condition]
        if self.is_wet():
            parts.insert(0, "☔")
        if self.temp_c:
            parts.append(f"{self.temp_c}℃")
        if self.rain_mm is not None:
            parts.append(f"降水 {self.rain_mm}mm")
        if self.wind:
            compass, _, speed = self.wind.partition(" ")
            arrow = wind_effect(compass, park_bearing(self.venue))
            # A compass point says nothing until it is read against the park,
            # which is exactly what the arrow has already done — so it replaces
            # the direction rather than sitting next to it. Where no bearing is
            # recorded there is no arrow, and the raw direction is all there is.
            parts.append(f"{arrow} {speed}m/s" if arrow else f"{self.wind}m/s")
        return " ".join(parts)


@dataclass(frozen=True)
class Slate:
    """What a day's game pages carry, keyed by short team name.

    Weather is a property of the game, so both of its teams map to the same
    forecast — which lets either side join to it.
    """

    starters: dict
    weather: dict


# Yahoo answers a burst of requests with 500s, and the schedule page — the one
# that lists a day's game ids — is the first to go. Losing it costs every
# pitcher name, so it is worth a few patient retries.
_RETRIES = 3
_BACKOFF_SECONDS = 2.0


def _get(url: str, *, sleep=time.sleep) -> str:
    last = None
    for attempt in range(_RETRIES):
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code < 500:
            return response.text
        last = response.status_code
        if attempt < _RETRIES - 1:
            sleep(_BACKOFF_SECONDS * (attempt + 1))
    raise requests.HTTPError(f"{url} returned {last} after {_RETRIES} attempts")


def normalize_team(full_name: str) -> str:
    for needle, short in TEAM_SUBSTRINGS.items():
        if needle in (full_name or ""):
            return short
    return ""


def _japanese_date(game_date: str) -> str:
    year, month, day = game_date.split("-")
    return f"{int(year)}年{int(month)}月{int(day)}日"


def _parse_weather(html: str) -> Weather | None:
    """The icon Yahoo prints against the game — condition only, no numbers."""
    match = _WEATHER.search(html)
    if not match or not match.group(2):
        return None
    return Weather(condition=match.group(2), url=match.group(1))


# Venues sealed off from the weather. A forecast tells the reader nothing about
# a game played in one, so it is left out entirely rather than printed and
# ignored.
ROOFED = ("ドーム", "京セラD", "エスコンF", "PayPay")

# ベルーナドーム is the awkward middle: a roof over the field but no walls under
# it. Rain never reaches the play, so the sky and the rainfall say nothing — but
# with no walls there is no cooling either, and it is notorious for August heat.
# The temperature is the one figure that still means something there.
SHELTERED = ("ベルーナ",)

# Rainfall at first pitch, in mm over the three-hour step, past which the game
# is worth flagging. Most rows read `降水 0mm`, which trains the eye to skip
# them — so the one that could stop play has to break the pattern.
RAIN_FLAG_MM = 1.0

# Clockwise from straight out to centre field, in 45-degree steps.
_ARROWS = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"]

# Japanese compass points, clockwise from north.
_COMPASS = ["北", "北北東", "北東", "東北東", "東", "東南東", "南東", "南南東",
            "南", "南南西", "南西", "西南西", "西", "西北西", "北西", "北北西"]

# Bearing in degrees from home plate towards centre field. Wind only means
# anything to a hitter relative to this: the same southerly is a tailwind in one
# park and a headwind in another.
#
# From NPB's own record-keepers' column, which tabulates every ground by the
# line "本塁から投手板を経て二塁へ向かう線" — exactly this axis:
# https://npb.jp/news/detail/20200410_01.html
#
# Only the open-air grounds are listed; a roofed game shows no forecast at all.
# Anywhere else — a 地方球場, a neutral site — has no entry, and the raw wind is
# shown with nothing claimed about its effect.
PARK_BEARINGS: dict[str, float] = {
    "甲子園": 180,        # 南
    "神宮": 22.5,         # 北北東
    "楽天モバイル": 180,   # 南 (楽天生命パーク宮城)
    "横浜": 337.5,        # 北北西
    "ZOZOマリン": 225,    # 南西
    "マツダ": 67.5,       # 東北東 — the only ground built to the current rule
    # Regional grounds the same table covers.
    "松山": 180,          # 南 (松山坊っちゃんスタジアム)
    "倉敷": 180,          # 南 (倉敷マスカットスタジアム)
    # Orix's second home, and 63 games on record — the most-used ground NPB's
    # table skips. Home plate sits at the north end and the field opens south,
    # per Evan; no published source states it either way.
    "神戸": 180,
}


def _canonical(venue: str) -> str:
    """Fold full-width letters, so one ground has one spelling to match.

    Yahoo writes the same ground both ways — 京セラＤ大阪 in 514 of the games on
    record and 京セラD大阪 in 137 — and plain substring matching lets whichever
    spelling was not anticipated straight through.
    """
    return "".join(
        chr(ord(char) - 0xFEE0) if 0xFF01 <= ord(char) <= 0xFF5E else char
        for char in (venue or "")
    )

# The venue sits at the end of the game's description line, after the date and
# the start time: "8月25日（火） <time>18:00</time> バンテリンドーム".
_DESCRIPTION = re.compile(
    r'class="bb-gameDescription__left"[^>]*>(.*?)</p>', re.S)

_ROW_KEYS = {"天気": "condition", "気温": "temp_c",
             "降水量": "rain_mm", "風向": "wind"}


def park_bearing(venue: str) -> float | None:
    """The recorded orientation for a ground, matched on its short name."""
    folded = _canonical(venue)
    for name, bearing in PARK_BEARINGS.items():
        if name in folded:
            return bearing
    return None


def bearing_of(compass_point: str) -> float | None:
    """A Japanese compass point as degrees clockwise from north."""
    try:
        return _COMPASS.index(compass_point) * 22.5
    except ValueError:
        return None


def wind_effect(compass_point: str, park_bearing: float | None) -> str:
    """Which way the wind pushes the ball, seen from behind the plate.

    Yahoo reports the direction the wind comes *from*, so it blows towards the
    opposite bearing; comparing that with the way the park faces is what turns
    a raw direction into something a reader can use. Without a bearing for the
    park nothing is claimed at all.

    The answer is an arrow, read as the ball flies with the reader standing
    behind the plate: up carries out to centre, down holds up, left sweeps
    towards left field. Eight points rather than four, because a wind at 45
    degrees is not a tailwind and should not be filed as one — at 甲子園 a
    westerly is the 浜風 crossing right to left, holding up left-handed pull
    hitters, while an easterly does the exact opposite.
    """
    if park_bearing is None:
        return ""
    origin = bearing_of(compass_point)
    if origin is None:
        return ""
    towards = (origin + 180) % 360
    # Clockwise from centre field is the right-field side, so a positive
    # offset means the ball is being pushed towards right.
    offset = (towards - park_bearing) % 360
    # Compass points and park bearings both step in 22.5 degrees, so the offset
    # does too — and half of all winds land exactly between two arrows. Those
    # go to the diagonal, which claims less than calling a wind straight in or
    # straight out when it is a step off either.
    step = offset / 45
    lower = int(step)
    if step == lower + 0.5:
        return _ARROWS[(lower if lower % 2 else lower + 1) % 8]
    return _ARROWS[int(round(step)) % 8]



def parse_venue(html: str) -> str:
    """The ballpark, which Yahoo prints at the end of the game's round line."""
    match = _DESCRIPTION.search(html)
    if not match:
        return ""
    text = re.sub(r"<[^>]+>", " ", match.group(1))
    parts = re.sub(r"\s+", " ", text).strip().split(" ")
    return parts[-1] if parts else ""


def is_roofed(venue: str) -> bool:
    # ベルーナドーム is named for a roof it only half has, so the sheltered
    # grounds are excluded here rather than left to whichever check runs first.
    if is_sheltered(venue):
        return False
    return any(marker in _canonical(venue) for marker in ROOFED)


def is_sheltered(venue: str) -> bool:
    """Roofed over the field but open at the sides — heat still gets in."""
    return any(marker in _canonical(venue) for marker in SHELTERED)


def parse_forecast(html: str, game_date: str, hour: int) -> Weather | None:
    """Read one day's three-hourly table and take the column nearest ``hour``.

    The page tabulates today and tomorrow separately, and the evening
    broadcast is about tomorrow, so the day is matched on its heading rather
    than by position. First pitch rarely lands on the grid — 14:00 sits between
    12時 and 15時 — so the nearest step is used.
    """
    from bs4 import BeautifulSoup

    year, month, day = (int(part) for part in game_date.split("-"))
    wanted = f"{month}月{day}日"
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.select("table.yjw_table2"):
        heading = table.find_previous(["h3", "h2", "p", "div"])
        if not heading or wanted not in heading.get_text(" ", strip=True):
            continue
        rows = {}
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if cells:
                rows[cells[0]] = cells[1:]
        hours = [int(h.rstrip("時")) for h in rows.get("時刻", []) if h.rstrip("時").isdigit()]
        if not hours:
            continue
        index = min(range(len(hours)), key=lambda i: abs(hours[i] - hour))
        found = {}
        for label, cells in rows.items():
            for prefix, key in _ROW_KEYS.items():
                if label.startswith(prefix) and index < len(cells):
                    found[key] = cells[index]
        if "condition" in found:
            return Weather(**found)
    return None


def _parse_teams(html: str, wanted_date: str) -> tuple[str, str] | None:
    """(home, away) short names from the page title, if it is the right day."""
    title = re.search(r"<title>(.*?)</title>", html, re.S)
    if not title or wanted_date not in title.group(1):
        return None
    teams = _TITLE_TEAMS.search(title.group(1))
    if not teams:
        return None
    return normalize_team(teams.group(1)), normalize_team(teams.group(2))


def _parse_game(html: str, wanted_date: str) -> dict[str, Starter]:
    """Pull the two announced starters out of one game page.

    The page title reads ``<date> <home>vs.<away>`` and the 予告先発 table
    follows the same order, so the first pitcher in it is the home starter.

    Read from that table rather than from the page's player links at large: a
    fixture stops being the only two-link page on the site about an hour before
    first pitch, when the lineups are published and twenty-odd batters join
    them. The closing line goes out at thirty minutes, well inside that.
    """
    title = re.search(r"<title>(.*?)</title>", html, re.S)
    if not title or wanted_date not in title.group(1):
        return {}
    teams = _TITLE_TEAMS.search(title.group(1))
    players = [(pid, re.sub(r"<[^>]+>", "", name).strip())
               for pid, name in _PROBABLE_STARTER.findall(html)]
    # Once the game begins the table goes with the fixture: the page turns into
    # a live scorecard whose links are headed by whoever is at bat, and nothing
    # in it can be read as a probable starter any more.
    if not teams or len(players) != 2 or not all(name for _, name in players):
        return {}
    sides = [normalize_team(teams.group(1)), normalize_team(teams.group(2))]
    return {team: Starter(name=name, player_id=player_id)
            for team, (player_id, name) in zip(sides, players) if team}


def _game_ids_on(schedule_html: str, wanted_date: str) -> list[str]:
    """The game ids listed under one date heading on the weekly schedule.

    The page covers a week, and fetching every game on it would be a hundred
    requests once retries are counted — enough for Yahoo to start refusing.
    Games sit between their own date heading and the next one, so the day can
    be picked out before anything is fetched.
    """
    headings = [(m.start(), m.group(0))
                for m in re.finditer(r"\d{1,2}月\d{1,2}日", schedule_html)]
    bounds = [pos for pos, _ in headings] + [len(schedule_html)]
    found: list[str] = []
    for index, (start, label) in enumerate(headings):
        if not wanted_date.endswith(label):
            continue
        for game_id in _GAME_ID.findall(schedule_html[start:bounds[index + 1]]):
            if game_id not in found:
                found.append(game_id)
    return found


def fetch_starters(game_date: str, *, fetch=_get) -> dict[str, Starter]:
    """Announced starters for ``game_date``, keyed by short team name."""
    return fetch_slate(game_date, fetch=fetch).starters



def _first_pitch_hour(html: str) -> int:
    """The hour Yahoo prints against the game, defaulting to an evening start."""
    match = re.search(r'<time>\s*(\d{1,2}):', html) or re.search(
        r'class="bb-gameCard__time">(\d{1,2}):', html)
    return int(match.group(1)) if match else 18


def fetch_slate(game_date: str, *, fetch=_get) -> Slate:
    """Starters and forecasts for ``game_date``, from the same game pages.

    Weather costs no extra requests: Yahoo prints it on the very page the
    starters come from.

    Never raises: the lines are worth broadcasting even when Yahoo is
    unreachable or the starters have not been announced yet.
    """
    try:
        # The bare schedule page lists a week of game ids and is reliable; the
        # ?date= variant answers 500. Each game page carries its own date in
        # the title, which is what the filtering below uses anyway.
        schedule = fetch(f"{BASE_URL}schedule/")
    except Exception as exc:  # network, DNS, timeout — all equally non-fatal
        print(f"[starters] schedule lookup failed ({exc}); no pitcher names")
        return Slate(starters={}, weather={})

    wanted = _japanese_date(game_date)
    starters: dict[str, Starter] = {}
    weather: dict[str, Weather] = {}
    for game_id in _game_ids_on(schedule, wanted):
        try:
            page = fetch(f"{BASE_URL}game/{game_id}/index")
        except Exception as exc:
            print(f"[starters] game {game_id} failed ({exc}); skipped")
            continue
        starters.update(_parse_game(page, wanted))
        # Weather is on the page whether or not the starters can be read from
        # it — a game already under way, or one whose starters are not yet
        # announced, still has a sky.
        sides = _parse_teams(page, wanted)
        if sides is None:
            continue
        forecast = _parse_weather(page)
        venue = parse_venue(page)
        if forecast is not None and is_roofed(venue):
            forecast = None   # nothing outside reaches the field
        if forecast is not None and forecast.url:
            # The icon on the game page gives the condition; the page it links
            # to has the temperature and rainfall for the hour of first pitch.
            try:
                detail = parse_forecast(fetch(forecast.url), game_date,
                                        _first_pitch_hour(page))
            except Exception as exc:
                print(f"[starters] forecast for {game_id} failed ({exc})")
                detail = None
            if detail is not None:
                forecast = replace(detail, url=forecast.url)
            forecast = replace(forecast, venue=venue)
        if forecast is not None:
            weather.update({team: forecast for team in sides if team})
    return Slate(starters=starters, weather=weather)
