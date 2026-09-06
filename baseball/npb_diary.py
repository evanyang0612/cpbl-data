"""Keep the 「2026・野球日記」 tab of the プロ野球データ分析 workbook current.

A straight port of the hand-maintained 「2023・野球日記」 tab (gid 633766240): one
row per calendar day of the regular season, one column per team, and each game
written once — in the **home** team's column — as ``客先発  客点-主点  主先発``.

What the 2023 sheet encodes by hand, and this reproduces:

* a neutral venue is spelled inside the score, ``石田  3  (京セラ)  6  青柳``
* a サヨナラ ends with ``。`` instead of ``-`` (the 2023 sheet also used ``*`` for
  11 of its 50 walk-offs; nothing in the data separates the two marks, so this
  writes ``。`` throughout — see SAYONARA_MARK)
* 雨天中止 puts ``<相手コード> 戦 雨 天 中 止`` in the home column, and paints both
  teams' cells blue
* a team with no game that day is purple; a day with no games at all is grey

Schedule and status come from Yahoo (the same pages ``npb.py`` already reads);
starters come from the 賽錄 tab of the NPB spreadsheet, joined on (date, home team).

Two modes:

``--full`` rebuilds the tab from scratch — it creates the sheet, paints every
row, sets the column widths and lays the season-phase banners out. It sweeps
Yahoo's schedule for all 226 days of the season, which is enough requests that
Yahoo starts refusing partway through if it is run often.

The default is incremental and is what the scheduler runs: only the rows in a
window around today are rebuilt, so Yahoo is asked for about two dozen days
instead of the whole season. Everything outside the window is already settled —
a played game's score does not change, and neither does a rainout — so leaving
those rows untouched costs nothing and keeps the run cheap enough to ride along
with every 30-minute NPB sweep.

The one thing a window cannot see is a name collision appearing outside it: the
first day a second 松本 starts a game, every earlier 松本 row should become
松本晴. The run notices and asks for a ``--full`` pass rather than leaving the
sheet quietly inconsistent.
"""

import argparse
import datetime
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup as bs
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), ".env"))

from baseball.sheets import GoogleSheetsClient  # noqa: E402

WORKBOOK_KEY = "1FTn1L5hi3TAHSf9JjoiEpcBYUlJ46cV49cxldfL3Wzc"
SHEET_TITLE = "2026・野球日記"
SOURCE_SHEET = "2023・野球日記"

SEASON_START = datetime.date(2026, 3, 27)   # 開幕 (3/20–3/22 are オープン戦)
SEASON_END = datetime.date(2026, 10, 4)     # last dated regular-season game

# How the 2023 sheet marks a walk-off: 。 for an ordinary サヨナラ, * for a
# サヨナラ本塁打 — the asterisk is the firework.
SAYONARA_MARK = "。"
# The firework gets breathing room either side, the way the 2023 sheet writes it
# (高梨     4 * 5     床田); 。 and - sit flush against the scores.
SAYONARA_HR_MARK = " * "

# The score itself is coloured: the winner red, the loser green, a tie blue, all
# bold. The separator between them rides along with the winner's run.
WIN_COLOR = "FF0000"
LOSE_COLOR = "008000"
DRAW_COLOR = "0000FF"

# Team columns are 130px. Anything wider is clipped by the neighbouring cell, so
# every game cell is fitted: first the padding is squeezed, then the pitcher names
# shrink, then the venue, and only as a last resort is a name cut short — the same
# order Evan worked in by hand on the 2023 sheet (ビーディ at 9pt, スチュワ--ト cut).
# 148px, not the bare 130px column: the 2023 sheet let a cell bleed a little into
# its neighbour (which is usually the away team's empty column), and this figure
# reproduces its padding exactly — 5 spaces for 福谷/赤星, 4 for 田中/加藤貴,
# 3 for 小笠原/ビーディ. Only genuinely oversized cells get squeezed.
# Evan's own shorthand from the 2023 sheet, where a long katakana name is clipped
# by hand and ー written as --  (スチュワ--ト, ピ--タ--ズ). An entry here wins over
# the automatic truncation below.
NAME_OVERRIDES = {
    "スチュワート・ジュニア": "スチュワ--ト",
}

FIT_TARGET_PX = 148
MIN_FONT = 7
MIN_NAME_CHARS = 4


def text_px(text, size):
    """Rendered width in pixels: full-width glyphs are 1em, ASCII half that."""
    return sum((size * 4 / 3) if ord(ch) > 0x7F else (size * 2 / 3) for ch in text)


def name_size(name):
    return 8 if len(name) >= 5 else (9 if len(name) == 4 else 10)

YAHOO_SCHEDULE = "https://baseball.yahoo.co.jp/npb/schedule/first/all?date={date}"
# NPB's own play-by-play, the only source that says how a walk-off actually ended.
# The score URLs are collected from the monthly schedule pages rather than guessed:
# the game-number suffix is not always -01, and 交流戦 lives on its own page.
NPB_BASE = "https://npb.jp"
NPB_SCHEDULE_PAGES = ([f"/games/{{year}}/schedule_{m:02d}_detail.html" for m in range(3, 11)]
                      + ["/interleague/{year}/schedule_detail.html"])
# NPB's daily 出場選手登録／登録抹消 公示. The date form builds this path in JS
# (/common/js/announcement.js), which is why no query string reaches it.
NPB_ROSTER = "https://npb.jp/announcement/roster/roster_{md}.html"
# 予告先発. One request, today only — which is all NPB announces. A game whose
# starters are out but which has not been played yet shows the two names with no
# score, the way the 2023 sheet writes it (東克樹        横川凱).
NPB_STARTERS = "https://npb.jp/announcement/starter/"

# 公示 spells teams out in full; the note spells them the way Evan's own 2023
# comments do, and keeps the two leagues in separate blocks.
OFFICIAL_SHORT = {
    "読売ジャイアンツ": "巨人", "阪神タイガース": "阪神", "横浜DeNAベイスターズ": "ＤｅＮＡ",
    "東京ヤクルトスワローズ": "ヤクルト", "中日ドラゴンズ": "中日", "広島東洋カープ": "広島",
    "埼玉西武ライオンズ": "西武", "北海道日本ハムファイターズ": "日本ハム",
    "千葉ロッテマリーンズ": "ロッテ", "オリックス・バファローズ": "オリックス",
    "福岡ソフトバンクホークス": "ソフトB", "東北楽天ゴールデンイーグルス": "楽天",
}
CENTRAL = ("巨人", "阪神", "ＤｅＮＡ", "ヤクルト", "中日", "広島")

# The note spells two clubs differently from the way the columns are keyed.
OFFICIAL_TO_TEAM = {official: {"ＤｅＮＡ": "DeNA", "ソフトB": "ソフトバンク"}.get(short, short)
                    for official, short in OFFICIAL_SHORT.items()}

NPB_OFFICIAL_CODE = {
    "巨人": "g", "阪神": "t", "DeNA": "db", "ヤクルト": "s", "中日": "d", "広島": "c",
    "西武": "l", "日本ハム": "f", "ロッテ": "m", "オリックス": "b",
    "ソフトバンク": "h", "楽天": "e",
}
UA = {"User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")}

WEEKDAY_JP = ["月", "火", "水", "木", "金", "土", "日"]

WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
PURPLE = {"red": 0.8, "green": 0.6, "blue": 1.0}        # CC99FF 沒有比賽
BLUE = {"red": 0.6, "green": 0.8, "blue": 1.0}          # 99CCFF 雨天中止
GREY = {"red": 191 / 255, "green": 191 / 255, "blue": 191 / 255}    # BFBFBF 整天沒比賽
BLACK = {"red": 0.0, "green": 0.0, "blue": 0.0}
PINK = {"red": 1.0, "green": 0.6, "blue": 0.8}          # FF99CC 交流戦の告知

# The 2023 sheet announces a stretch of the season by taking over the Monday
# off-day row just before it starts — pink, no games, the title merged across
# H:J. 交流戦 2026 runs 5/26–6/14 (npb.jp/interleague/2026/), so its Monday is
# 5/25. Dates are explicit rather than derived: NPB moves these around.
BANNERS = {
    "2026-05-25": ("2026 日 本 生 命 セ・パ 交 流 戦", PINK),
}

# …and signs it off by carrying the same pink through the days that follow the
# last scheduled 交流戦 game — 2023 tints 6/19–6/22 after its 6/18 finish. 2026
# ends 6/14, so 6/15–6/18. Only the empty cells take the tint: the 予備日 games
# played inside the window stay white like any other game.
# The All-Star break gets the same treatment in a darker purple: four tinted days
# with the two games written across the middle and オールスターゲーム labelled on
# each league's side (2023 does this over 7/18–7/21). 全セ/全パ are not among the
# 12 clubs, so these rows are composed by hand from NPB's own box scores.
DARK_PURPLE = {"red": 0.6, "green": 0.2, "blue": 0.4}   # 993366
ALLSTAR_DAYS = ("2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30")
ALLSTAR_LABEL = "オールスターゲーム"
# The 2023 sheet floats four vector star5 shapes across the band. The Sheets API
# has no drawing layer, so this is the nearest thing it can write: a big white ★
# in a cell merged down the four rows, at the same four positions.
ALLSTAR_STAR = "★"
# (start, end) column ranges, each merged down the four rows. The inner pair
# straddles two columns so the stars sit midway between the label and the score
# box instead of hugging it.
ALLSTAR_STAR_COLS = ((2, 3), (5, 7), (10, 12), (14, 15))   # C, F:G, K:L, O
ALLSTAR_GAMES = {
    # date: (away starter, away runs, venue, home runs, home starter)
    "2026-07-28": ("伊藤", "7", "東京", "5", "山野"),      # 全パ 7 - 5 全セ @東京ドーム
    "2026-07-29": ("髙橋", "8", "富山", "7", "エスピノーザ"),  # 全セ 8 - 7 全パ @富山
}

INTERLEAGUE_TAIL = frozenset(
    ("2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18")
)


def rgb(hexstr):
    return {"red": int(hexstr[0:2], 16) / 255,
            "green": int(hexstr[2:4], 16) / 255,
            "blue": int(hexstr[4:6], 16) / 255}


# (column index 0-based, Yahoo team name, code used in 中止 text, header cell)
TEAMS = [
    (2, "巨人", "G", "巨   人", "FF6600", "000000", "華康中特圓體", 16),
    (3, "中日", "D", "中   日", "0000FF", "FFFFFF", "華康新特明體", 16),
    (4, "阪神", "T", "阪     神", "FCF600", "000000", "華康儷粗圓(P)", 16),
    (5, "ヤクルト", "S", "ヤクルト", "00009A", "FFFFFF", "PMingLiu", 16),
    (6, "広島", "C", "広   島", "EA0000", "FFFFFF", "華康新特明體", 16),
    (7, "DeNA", "YB", "横 浜 DeNA", "003366", "99CCFF", "華康新特明體", 16),
    (9, "ロッテ", "M", "ロ ッ テ", "808080", "FFFFFF", "PMingLiu", 18),
    (10, "西武", "L", "西      武", "99CCFF", "17365D", "華康儷中黑(P)", 16),
    (11, "ソフトバンク", "H", "ソフトバンク", "FFCC00", "000000", "華康超黑體", 14),
    (12, "オリックス", "Bs", "オリックス", "002060", "C4BF00", "PMingLiu", 16),
    (13, "日本ハム", "F", "日 本 ハ ム", "2B67AF", "FFFFFF", "華康新特明體", 16),
    (14, "楽天", "E", "楽   天", "800000", "FFFFFF", "華康新特明體", 16),
]
COL_OF = {name: col for col, name, *_ in TEAMS}
CODE_OF = {name: code for _, name, code, *_ in TEAMS}
TEAM_NAMES = set(COL_OF)
SPACER_COL = 8   # column I, the black divider between the two leagues
LAST_COL = 15

HOME_VENUE = {
    "巨人": "東京ドーム", "中日": "バンテリンドーム", "阪神": "甲子園",
    "ヤクルト": "神宮", "広島": "マツダスタジアム", "DeNA": "横浜",
    "ロッテ": "ZOZOマリン", "西武": "ベルーナドーム", "ソフトバンク": "みずほPayPay",
    "オリックス": "京セラD大阪", "日本ハム": "エスコンF", "楽天": "楽天モバイル",
}

# How a neutral site is spelled inside the score, mirroring the 2023 sheet's
# ``(京セラ)`` / ``(東京)`` shorthand. Anything else is already short enough.
VENUE_SHORT = {
    "京セラD大阪": "京セラ", "東京ドーム": "東京", "バンテリンドーム": "ナゴヤ",
    "マツダスタジアム": "マツダ", "ZOZOマリン": "ZOZO", "ベルーナドーム": "西武",
    "みずほPayPay": "福岡", "エスコンF": "エスコン", "楽天モバイル": "宮城",
    "ハードオフ新潟": "新潟", "県営大宮": "大宮", "山形市": "山形",
    "ほっと神戸": "神戸",
}


def fetch_schedule(start, end, cache=None):
    """Yahoo's daily calendar for every date in range -> {date: [game, ...]}."""
    if cache and os.path.exists(cache):
        cached = json.load(open(cache))
        wanted = {(start + datetime.timedelta(days=i)).isoformat()
                  for i in range((end - start).days + 1)}
        if wanted <= set(cached):
            return {d: cached[d] for d in wanted}

    def text(el):
        return re.sub(r"\s+", " ", el.get_text(" ", strip=True)) if el else ""

    def one(date):
        html = ""
        for _ in range(3):
            try:
                html = requests.get(YAHOO_SCHEDULE.format(date=date),
                                    headers=UA, timeout=30).text
                break
            except requests.RequestException:
                continue
        games = []
        for item in bs(html, "html.parser").find_all(class_="bb-score__item"):
            games.append({
                "venue": text(item.find(class_="bb-score__venue")),
                "home": text(item.find(class_="bb-score__homeLogo")),
                "away": text(item.find(class_="bb-score__awayLogo")),
                "home_score": text(item.find(class_="bb-score__score--left")),
                "away_score": text(item.find(class_="bb-score__score--right")),
                "status": text(item.find(class_="bb-score__link")),
            })
        return date, games

    dates = [(start + datetime.timedelta(days=i)).isoformat()
             for i in range((end - start).days + 1)]
    out = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for date, games in pool.map(one, dates):
            out[date] = games
    if cache:
        json.dump(out, open(cache, "w"), ensure_ascii=False)
    return out


def fetch_announced_starters():
    """{(away team, home team): (away pitcher, home pitcher)} for today.

    NPB lists the home side on the left of each 予告先発 unit and the visitor on
    the right; the team is only identifiable from the crest's alt text.
    """
    try:
        response = requests.get(NPB_STARTERS, headers=UA, timeout=30)
        response.encoding = "utf-8"
    except requests.RequestException:
        return {}
    if response.status_code != 200:
        return {}

    def side(block):
        crest, name = block.find("img"), block.find("span")
        if not crest or not name:
            return None, None
        team = OFFICIAL_TO_TEAM.get(re.sub(r"\s+", "", crest.get("alt", "")))
        return team, re.sub(r"[\s\u3000]+", " ", name.get_text(strip=True))

    out = {}
    for unit in bs(response.text, "html.parser").select("div.unit"):
        home_block, away_block = unit.select_one(".team_left"), unit.select_one(".team_right")
        if not home_block or not away_block:
            continue
        home, home_pitcher = side(home_block)
        away, away_pitcher = side(away_block)
        if home and away:
            out[(away, home)] = (away_pitcher, home_pitcher)
    return out


def fetch_roster_moves(start, end, cache=None):
    """NPB's daily 公示 -> {date: {"登録": [...], "抹消": [...]}}.

    Each entry is ``(team code, position, number, name)``; the page carries both
    leagues under repeated 出場選手登録 / 出場選手登録抹消 headings.
    """
    if cache and os.path.exists(cache):
        return json.load(open(cache))

    def one(date):
        _, month, day = date.split("-")
        try:
            resp = requests.get(NPB_ROSTER.format(md=f"{month}{day}"),
                                headers=UA, timeout=30)
            resp.encoding = "utf-8"
        except requests.RequestException:
            return date, None
        if resp.status_code != 200:
            return date, None
        soup = bs(resp.text, "html.parser")
        moves = {"登録": [], "抹消": []}
        for heading in soup.find_all("h5"):
            label = re.sub(r"\s+", "", heading.get_text(" ", strip=True))
            if label not in ("出場選手登録", "出場選手登録抹消"):
                continue
            table = heading.find_next("table")
            if not table:
                continue
            key = "抹消" if "抹消" in label else "登録"
            for tr in table.find_all("tr"):
                cells = [re.sub(r"\s+", " ", c.get_text(" ", strip=True))
                         for c in tr.find_all(["th", "td"])]
                # A move row names the team; the roster tables below do not.
                if len(cells) == 4 and cells[0] in OFFICIAL_SHORT:
                    moves[key].append([OFFICIAL_SHORT[cells[0]]] + cells[1:])
        return date, moves

    dates = [(start + datetime.timedelta(days=i)).isoformat()
             for i in range((end - start).days + 1)]
    out = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        for date, moves in pool.map(one, dates):
            out[date] = moves
    if cache:
        json.dump(out, open(cache, "w"), ensure_ascii=False)
    return out


def allstar_cell(date):
    """`伊藤   7   (東京)   5   山野`, scores at 11pt in the win/lose colours."""
    away_p, away_s, venue, home_s, home_p = ALLSTAR_GAMES[date]
    text = f"{away_p}   {away_s}   ({venue})   {home_s}   {home_p}"
    away_at = len(away_p) + 3
    home_at = away_at + len(away_s) + len(f"   ({venue})   ")
    away_color = WIN_COLOR if int(away_s) > int(home_s) else LOSE_COLOR
    home_color = LOSE_COLOR if int(away_s) > int(home_s) else WIN_COLOR
    runs = [run_at(0),
            run_at(away_at, fg=away_color, bold=True, size=11),
            run_at(away_at + len(away_s), bold=True),
            run_at(home_at, fg=home_color, bold=True, size=11),
            run_at(home_at + len(home_s), bold=True)]
    return text, runs


def _move_block(entries):
    """`ヤクルト　山野太一投手` / continuation lines indented under the same team."""
    lines = []
    previous = None
    for team, position, _number, name in entries:
        who = f"{name.replace(' ', '')}{position}"
        if team == previous:
            lines.append("\u3000" * len(team) + "\u3000" + who)
        else:
            lines.append(f"{team}\u3000{who}")
            previous = team
    return lines


def roster_note(_date_label, moves):
    """The day's 公示, laid out the way the 2023 sheet's own comments are."""
    if not moves or not (moves["登録"] or moves["抹消"]):
        return ""
    blocks = []
    for league, label in (("セ", "セ･リーグ"), ("パ", "パ・リーグ")):
        def of(key):
            return [e for e in moves[key]
                    if (e[0] in CENTRAL) == (league == "セ")]
        registered, removed = of("登録"), of("抹消")
        if not registered and not removed:
            continue
        lines = [label]
        if registered:
            lines.append("【出場選手登録】")
            lines += _move_block(registered)
            if removed:
                lines.append("【同抹消】")
                lines += _move_block(removed)
        else:
            lines.append("【出場選手登録抹消】")
            lines += _move_block(removed)
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def npb_score_urls(year, cache=None, months=None):
    """{(MMDD, home code, away code): score URL} from NPB's monthly schedule pages.

    ``months`` narrows the fetch to the months actually being asked about: a
    scheduled run has at most one or two unresolved walk-offs, and pulling all
    nine pages for them costs more than the probe itself.
    """
    if cache and os.path.exists(cache):
        return {tuple(k.split("|")): v for k, v in json.load(open(cache)).items()}
    pages = NPB_SCHEDULE_PAGES
    if months:
        wanted = {f"schedule_{m:02d}_detail" for m in months}
        pages = [p for p in pages
                 if "interleague" in p or any(w in p for w in wanted)]
    urls = {}
    for path in pages:
        resp = requests.get(NPB_BASE + path.format(year=year), headers=UA, timeout=30)
        resp.encoding = "utf-8"
        if resp.status_code != 200:
            continue
        for anchor in bs(resp.text, "html.parser").find_all("a", href=True):
            found = re.search(rf"/scores/{year}/(\d{{4}})/([a-z]+)-([a-z]+)-\d+/",
                              anchor["href"])
            if found:
                urls[(found.group(1), found.group(2), found.group(3))] = \
                    NPB_BASE + anchor["href"]
    if cache:
        json.dump({"|".join(k): v for k, v in urls.items()}, open(cache, "w"))
    return urls


def mark_walkoff_home_runs(starters, schedule, cache=None, url_cache=None,
                           window=None):
    """Set ``walkoff_hr`` on every サヨナラ whose last play was a home run.

    Nothing in the box score separates a walk-off single from a walk-off homer, so
    this reads NPB's own play-by-play and looks at the game's final play.
    """
    known = json.load(open(cache)) if cache and os.path.exists(cache) else {}
    opponents = {}
    for date, day in schedule.items():
        for game in day:
            if game["home"] in TEAM_NAMES and game["away"] in TEAM_NAMES:
                opponents[(date, game["home"])] = game["away"]

    # Only games the run will actually write are worth a request; a walk-off
    # outside the window has no opponent in ``schedule`` to build a URL from.
    pending = [key for key, info in starters.items()
               if info["walkoff"] and f"{key[0]}|{key[1]}" not in known
               and key in opponents]
    if pending:
        urls = npb_score_urls(SEASON_START.year, cache=url_cache,
                              months={int(key[0].split("-")[1]) for key in pending})

        def probe(key):
            date, home = key
            away = opponents.get(key, "")
            _, month, day = date.split("-")
            url = urls.get((f"{month}{day}", NPB_OFFICIAL_CODE[home],
                            NPB_OFFICIAL_CODE.get(away, "")))
            if not url:
                return key, False
            try:
                resp = requests.get(url.rstrip("/") + "/playbyplay.html",
                                    headers=UA, timeout=30)
                resp.encoding = "utf-8"
            except requests.RequestException:
                return key, False
            if resp.status_code != 200:
                return key, False
            body = re.sub(r"\s+", " ",
                          bs(resp.text, "html.parser").get_text(" ", strip=True))
            # Everything after the last play is the site footer.
            last_play = body.split("一般社団法人日本野球機構について")[0][-45:]
            return key, ("ホームラン" in last_play or "本塁打" in last_play)

        with ThreadPoolExecutor(max_workers=4) as pool:
            for key, is_hr in pool.map(probe, pending):
                known[f"{key[0]}|{key[1]}"] = is_hr
        if cache:
            json.dump(known, open(cache, "w"), ensure_ascii=False)

    for key, info in starters.items():
        info["walkoff_hr"] = bool(known.get(f"{key[0]}|{key[1]}"))
    # Count only what this run covers; the cache carries the whole season.
    return sum(1 for key, info in starters.items()
               if info.get("walkoff_hr") and key in opponents)


def load_starters(client):
    """賽錄 -> ({(date, home team): {starters, walk-off flag}}, display-name map).

    The map is returned because 予告先発 names arrive from a different source and
    have to be spelled the same way as the ones already on the sheet.
    """
    import npb
    ws = client.open_by_key(npb.NPB_SPREADSHEET_KEY).worksheet("賽錄")
    rows = ws.get_all_values()
    idx = {name: i for i, name in enumerate(rows[0])}

    def num(value):
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    out = {}
    for row in rows[1:]:
        if not any(row) or not row[idx["日期"]].startswith("2026"):
            continue
        away = [num(row[idx[f"客{i}"]]) for i in range(1, 13)]
        home = [num(row[idx[f"主{i}"]]) for i in range(1, 13)]
        played = [i for i in range(12) if away[i] is not None or home[i] is not None]
        last = max(played) if played else 8
        away_r, home_r = num(row[idx["客總分"]]), num(row[idx["主總"]])
        # サヨナラ: the home side takes the lead in its own final at-bat.
        walkoff = (home_r is not None and away_r is not None and home_r > away_r
                   and (home[last] or 0) > 0 and home_r - away_r <= (home[last] or 0))
        key = (row[idx["日期"]], row[idx["主場隊伍"]].replace("ＤｅＮＡ", "DeNA"))
        out[key] = {"away_starter": row[idx["客場先發"]].strip(),
                    "home_starter": row[idx["主場先發"]].strip(),
                    "walkoff": walkoff}

    display = resolve_display_names({info[key] for info in out.values()
                                     for key in ("away_starter", "home_starter")
                                     if info[key]})
    extended = sum(1 for full, short in display.items() if short != surname(full))
    print(f"先發投手 {len(display)} 人，其中 {extended} 人因姓氏重複而加註名字")
    for info in out.values():
        for key in ("away_starter", "home_starter"):
            info[key] = display.get(info[key], surname(info[key]))
    return out, display


# NPB's 公示 prefixes a foreign player with an initial — Ｊ．ルケーシー — and 賽錄
# occasionally does too (E.ラミレス). The diary drops it, so the same pitcher is
# spelled one way whether his game has been played or only announced.
_INITIAL = re.compile(r"^[Ａ-ＺA-Z][．.]\s*")


def surname(name):
    """賽錄 stores full names ("村上 頌樹"); the diary shows only the family name,
    the way the 2023 sheet does. Katakana names carry no space and stay whole."""
    bare = _INITIAL.sub("", (name or "").strip())
    return bare.split(" ")[0].split("\u3000")[0].strip()


def resolve_display_names(full_names):
    """{full name: what the diary calls him}.

    The family name alone, except where the season has more than one pitcher
    with it — 髙橋 covers 光成, 遥人 and 宏斗 in 2026 — in which case as much of
    the given name is appended as it takes to tell them apart. This is Evan's own
    convention on the 2023 sheet (髙橋宏, 高橋光, 松本航, 佐々木朗).
    """
    groups = {}
    for full in full_names:
        groups.setdefault(surname(full), []).append(full)

    display = {}
    for family, members in groups.items():
        if len(members) == 1:
            display[members[0]] = family
            continue
        for extra in range(1, 6):
            attempt = {}
            for full in members:
                given = full.split(" ", 1)[1].strip() if " " in full else ""
                attempt[full] = family + given[:extra]
            if len(set(attempt.values())) == len(members):
                display.update(attempt)
                break
        else:  # identical names, nothing left to distinguish them by
            display.update({full: full.replace(" ", "") for full in members})
    return display


def _compose(away_p, home_p, away_s, home_s, mark, venue, pad, size_a, size_h, size_v):
    """Build the cell text plus its coloured runs for one candidate layout."""
    gap = " " * pad
    if venue:
        middle = f"{away_s}{gap}({venue}){gap}{home_s}"
    else:
        middle = f"{away_s}{mark}{home_s}"
    text = f"{away_p}{gap}{middle}{gap}{home_p}"

    away_at = len(away_p) + pad
    home_at = away_at + len(middle) - len(home_s)
    tail_at = away_at + len(middle)

    width = (text_px(away_p, size_a) + text_px(home_p, size_h)
             + text_px(gap * 2, 10) + text_px(away_s + home_s, 10))
    width += text_px(mark, 10) if not venue else text_px(f"{gap}({venue}){gap}", size_v)
    return text, away_at, home_at, tail_at, width


def fit_layout(away_p, home_p, away_s, home_s, mark, venue):
    """Squeeze one cell until it clears the column: padding, then font, then name.

    Every branch strictly shrinks something, so this always terminates.
    """
    pad, size_v = 5, 10
    size_a, size_h = name_size(away_p), name_size(home_p)
    while _compose(away_p, home_p, away_s, home_s, mark, venue,
                   pad, size_a, size_h, size_v)[4] > FIT_TARGET_PX:
        if pad > 1:
            pad -= 1
        elif size_a > MIN_FONT and len(away_p) > 3:
            size_a -= 1
        elif size_h > MIN_FONT and len(home_p) > 3:
            size_h -= 1
        elif venue and size_v > MIN_FONT:
            size_v -= 1
        elif len(away_p) > MIN_NAME_CHARS and len(away_p) >= len(home_p):
            away_p = away_p[:-1]
        elif len(home_p) > MIN_NAME_CHARS:
            home_p = home_p[:-1]
        else:
            break  # two short names and a venue: nothing left to give
    return away_p, home_p, pad, size_a, size_h, size_v


def _game_parts(game, starters):
    """(away surname, home surname, away score, home score, mark, venue) or None."""
    if game["status"] != "試合終了":
        return None
    info = starters.get((game["date"], game["home"]), {})
    venue, mark = None, "-"
    if game["venue"] != HOME_VENUE.get(game["home"]):
        venue = VENUE_SHORT.get(game["venue"], game["venue"])
    elif info.get("walkoff"):
        mark = SAYONARA_HR_MARK if info.get("walkoff_hr") else SAYONARA_MARK
    def who(key):
        name = info.get(key, "")
        return NAME_OVERRIDES.get(name, name)

    return (who("away_starter"), who("home_starter"),
            game["away_score"], game["home_score"], mark, venue)


def solve_name_lengths(schedule, starters):
    """One length per pitcher, season-wide.

    Fitting each cell on its own would spell the same pitcher デュプランティエ in
    one game and デュプラ in another. So take the shortest form any cell forces
    and use it everywhere; shortening one cell can push another over, hence the
    loop to a fixed point.
    """
    games = [dict(g, date=date) for date, day in schedule.items() for g in day
             if g["home"] in TEAM_NAMES and g["away"] in TEAM_NAMES]
    limits = {}
    for _ in range(8):
        changed = False
        for game in games:
            parts = _game_parts(game, starters)
            if not parts:
                continue
            away_full, home_full, away_s, home_s, mark, venue = parts
            away_p = away_full[:limits.get(away_full, len(away_full))]
            home_p = home_full[:limits.get(home_full, len(home_full))]
            fitted_a, fitted_h, *_ = fit_layout(away_p, home_p, away_s, home_s,
                                                mark, venue)
            for full, fitted in ((away_full, fitted_a), (home_full, fitted_h)):
                if len(fitted) < limits.get(full, len(full)):
                    limits[full] = len(fitted)
                    changed = True
        if not changed:
            break
    return {name: length for name, length in limits.items() if length < len(name)}


def game_text(game, starters, name_limits=None, announced=None):
    """The cell that goes in the home team's column: (text, kind, textFormatRuns)."""
    if game["status"] in ("試合中止", "ノーゲーム"):
        # Half-width gaps: the 2023 sheet's ideographic ones ran ~145px wide.
        text = " ".join(f"{CODE_OF[game['away']]} 戦 雨 天 中 止".split())
        return text, "cancelled", None
    if game["status"] != "試合終了":
        pair = (announced or {}).get((game["away"], game["home"]))
        if not pair or not all(pair):
            return CODE_OF[game["away"]], "scheduled", None
        # Starters announced but no score yet: two names, no separator, exactly
        # how the 2023 sheet writes a game it knows the pitchers for.
        limits = name_limits or {}
        away_p, home_p = (NAME_OVERRIDES.get(p, p) for p in pair)
        away_p = away_p[:limits.get(away_p, len(away_p))]
        home_p = home_p[:limits.get(home_p, len(home_p))]
        pad = 5
        while (text_px(away_p, name_size(away_p)) + text_px(home_p, name_size(home_p))
               + text_px(" " * pad, 10) > FIT_TARGET_PX and pad > 1):
            pad -= 1
        text = f"{away_p}{' ' * pad}{home_p}"
        return text, "announced", [run_at(0, size=name_size(away_p)),
                                   run_at(len(away_p) + pad, size=name_size(home_p))]

    away_p, home_p, away_s, home_s, mark, venue = _game_parts(game, starters)
    limits = name_limits or {}
    away_p = away_p[:limits.get(away_p, len(away_p))]
    home_p = home_p[:limits.get(home_p, len(home_p))]

    away_p, home_p, pad, size_a, size_h, size_v = fit_layout(
        away_p, home_p, away_s, home_s, mark, venue)

    text, away_at, home_at, tail_at, _ = _compose(
        away_p, home_p, away_s, home_s, mark, venue, pad, size_a, size_h, size_v)

    try:
        away_n, home_n = int(away_s), int(home_s)
    except ValueError:
        return text, "played", None

    if away_n == home_n:
        away_color = home_color = DRAW_COLOR
    else:
        away_color = WIN_COLOR if away_n > home_n else LOSE_COLOR
        home_color = LOSE_COLOR if away_n > home_n else WIN_COLOR

    runs = [run_at(0, size=size_a),
            run_at(away_at, fg=away_color, bold=True)]
    if venue:
        # The venue sits between the two scores and stays plain black.
        runs.append(run_at(away_at + len(away_s), size=size_v))
        runs.append(run_at(home_at, fg=home_color, bold=True))
    elif away_n == home_n:
        pass  # one blue run covers "1-1" whole
    elif away_n > home_n:
        # The winner's run swallows the separator next to its score.
        runs.append(run_at(home_at, fg=home_color, bold=True))
    else:
        # The winner's run starts at the mark itself, so a leading space stays
        # with the loser's colour — exactly how the 2023 cells are painted.
        offset = len(mark) - 1 if mark.startswith(" ") else len(mark)
        runs.append(run_at(home_at - offset, fg=home_color, bold=True))
    runs.append(run_at(tail_at, size=size_h))
    return text, "played", runs


def build_rows(schedule, starters, roster=None, name_limits=None, window=None,
               announced=None):
    """-> list of (date label, weekday, {col: (text, background, runs)}, note)."""
    rows = []
    first, last = window or (SEASON_START, SEASON_END)
    day = max(first, SEASON_START)
    stop = min(last, SEASON_END)
    while day <= stop:
        iso = day.isoformat()
        games = [dict(g, date=iso) for g in schedule.get(iso, [])
                 if g["home"] in TEAM_NAMES and g["away"] in TEAM_NAMES]
        cells = {}
        if iso in ALLSTAR_DAYS:
            for col, *_ in TEAMS:
                cells[col] = ("", DARK_PURPLE, None)
            if iso == ALLSTAR_DAYS[0]:
                for start_col, _ in ALLSTAR_STAR_COLS:
                    cells[start_col] = (ALLSTAR_STAR, DARK_PURPLE, None)
            if iso in ALLSTAR_GAMES:
                text, runs = allstar_cell(iso)
                cells[COL_OF["DeNA"]] = (text, WHITE, runs)      # column H
                cells[COL_OF["中日"]] = (ALLSTAR_LABEL, DARK_PURPLE, None)   # D
                cells[COL_OF["オリックス"]] = (ALLSTAR_LABEL, DARK_PURPLE, None)  # M
            rows.append((f"{day.month}/{day.day}", WEEKDAY_JP[day.weekday()], cells,
                         roster_note(f"{day.month}/{day.day}", (roster or {}).get(iso))))
            day += datetime.timedelta(days=1)
            continue
        banner = BANNERS.get(iso)
        if banner and not games:
            title, bg = banner
            for col, *_ in TEAMS:
                cells[col] = ("", bg, None)
            cells[TEAMS[0][0]] = (title, bg, None)   # column C, the merge anchor
        elif not games:
            blank = PINK if iso in INTERLEAGUE_TAIL else GREY
            for col, *_ in TEAMS:
                cells[col] = ("", blank, None)
        else:
            idle = PINK if iso in INTERLEAGUE_TAIL else PURPLE
            for col, *_ in TEAMS:
                cells[col] = ("", idle, None)
            for game in games:
                text, kind, runs = game_text(game, starters, name_limits, announced)
                bg = BLUE if kind == "cancelled" else WHITE
                cells[COL_OF[game["home"]]] = (text, bg, runs)
                cells[COL_OF[game["away"]]] = ("", bg, None)
        label = f"{day.month}/{day.day}"
        note = roster_note(label, (roster or {}).get(iso))
        rows.append((label, WEEKDAY_JP[day.weekday()], cells, note))
        day += datetime.timedelta(days=1)
    return rows


def solid_border(hexstr):
    """All four sides, one colour. Painting the band's own colour over the grid
    is how the dark-purple block reads as solid: the 2023 sheet uses black lines,
    invisible against 993366, where an unbordered cell shows Sheets' pale grid."""
    side = {"style": "SOLID", "color": rgb(hexstr)}
    return {edge: dict(side) for edge in ("top", "bottom", "left", "right")}


def cell(value, *, bg=None, font=None, size=10, bold=False, fg="000000",
         halign="CENTER", valign="MIDDLE", runs=None, note=None, borders=None):
    """``font=None`` leaves the sheet default in place.

    The 2023 sheet asks for PMingLiu, a Windows font Google does not carry; the
    substitute it picks renders digits so tight that a bold score collides with
    the run beside it. The game cells therefore inherit the default face.
    """
    text_format = {"fontSize": size, "bold": bold, "foregroundColor": rgb(fg)}
    if font:
        text_format["fontFamily"] = font
    fmt = {"textFormat": text_format,
           "horizontalAlignment": halign, "verticalAlignment": valign}
    if bg is not None:
        fmt["backgroundColor"] = bg
    if borders:
        fmt["borders"] = borders
    out = {"userEnteredValue": {"stringValue": value},
           "userEnteredFormat": fmt}
    if runs:
        out["textFormatRuns"] = runs
    if note:
        out["note"] = note
    return out


def run_at(start, *, fg="000000", bold=False, size=10):
    return {"startIndex": start,
            "format": {"foregroundColor": rgb(fg), "bold": bold, "fontSize": size}}


def sheet_payload(rows, with_header=True):
    """rowData for the tab; ``with_header`` off yields just the day rows."""
    header = [cell("日 程 表", bg=WHITE, font="Microsoft JhengHei", size=12,
                   bold=True, valign="BOTTOM"), cell("", bg=WHITE)]
    for col in range(2, LAST_COL):
        spec = next((t for t in TEAMS if t[0] == col), None)
        if spec is None:
            header.append(cell("", bg=BLACK))
            continue
        _, _, _, label, bg_hex, fg_hex, font, size = spec
        header.append(cell(label, bg=rgb(bg_hex), fg=fg_hex, font=font,
                           size=size, bold=True, valign="BOTTOM"))

    data = [{"values": header}] if with_header else []
    for label, weekday, cells, note in rows:
        values = [cell(label, bg=WHITE, font="Arial Narrow", size=13),
                  cell(weekday, bg=WHITE, font="Microsoft JhengHei", size=11)]
        for col in range(2, LAST_COL):
            if col == SPACER_COL:
                # The black bar between the two leagues runs the height of the
                # sheet; the day's 登録抹消 hangs off it as a note. A banner row
                # takes the bar over, exactly as the 2023 sheet does.
                dark = any(bg is DARK_PURPLE for _, bg, _ in cells.values())
                bar = (PINK if any(bg is PINK for _, bg, _ in cells.values())
                       else DARK_PURPLE if dark else BLACK)
                values.append(cell("", bg=bar, note=note,
                                   borders=solid_border("993366") if dark else None))
                continue
            text, bg, runs = cells[col]
            if text == ALLSTAR_STAR:
                values.append(cell(text, bg=bg, size=40, fg="FFFFFF",
                                   borders=solid_border("993366")))
            elif bg is DARK_PURPLE:
                # オールスターゲーム label, pinned to the middle of the sheet.
                values.append(cell(text, bg=bg, size=18 if text else 10,
                                   bold=bool(text), fg="FFFFFF",
                                   halign="RIGHT" if col < SPACER_COL else "LEFT",
                                   borders=solid_border("993366")))
            elif runs and bg is WHITE and any(r["format"].get("fontSize") == 11
                                              for r in runs):
                values.append(cell(text, bg=bg, bold=True, runs=runs, halign="CENTER",
                                   borders=solid_border("BFBFBF")))
            elif bg is PINK and text:
                # Season-phase banner: same white 18pt as the 2023 sheet's.
                values.append(cell(text, bg=bg, size=18, bold=True, fg="FFFFFF"))
            else:
                values.append(cell(text, bg=bg, bold="雨" in text, runs=runs))
        data.append({"values": values})
    return data


def season_window(back, forward):
    """The stretch of days a scheduled run rebuilds, clamped to the season."""
    today = datetime.date.today()
    return (max(SEASON_START, today - datetime.timedelta(days=back)),
            min(SEASON_END, today + datetime.timedelta(days=forward)))


def warn_on_late_collisions(name_limits, starters, window):
    """Surnames that only became ambiguous inside the window.

    Display names are resolved across the whole season, so the day a second
    松本 starts, every 松本 row already on the sheet is spelled wrong — and those
    rows sit outside the window, so this run will not rewrite them. Flag exactly
    that case: a colliding surname where one pitcher is new to the window and
    another has been starting since before it.
    """
    first_seen = {}
    for (date, _), info in starters.items():
        for key in ("away_starter", "home_starter"):
            name = info[key]
            if name:
                first_seen[name] = min(first_seen.get(name, date), date)

    families = {}
    for name in first_seen:
        families.setdefault(surname(name), []).append(name)

    start = window[0].isoformat()
    stale = []
    for family, members in families.items():
        if len(members) < 2:
            continue
        fresh = [m for m in members if first_seen[m] >= start]
        older = [m for m in members if first_seen[m] < start]
        if fresh and older:
            stale.append(f"{family}（{'、'.join(sorted(older))} 的舊列仍是舊寫法）")
    return stale


def main(dry_run=False, cache=None, hr_cache=None, url_cache=None,
         roster_cache=None, full=False, window_back=3, window_forward=21):
    client = GoogleSheetsClient().client
    window = (SEASON_START, SEASON_END) if full else season_window(window_back, window_forward)
    if not full:
        print(f"增量更新窗口 {window[0]} ~ {window[1]}"
              f"（{(window[1] - window[0]).days + 1} 天）")
    schedule = fetch_schedule(window[0], window[1], cache=cache)
    starters, display = load_starters(client)
    homers = mark_walkoff_home_runs(starters, schedule, cache=hr_cache,
                                    url_cache=url_cache, window=window)
    walkoffs = sum(1 for (date, _), info in starters.items()
                   if info["walkoff"]
                   and window[0].isoformat() <= date <= window[1].isoformat())
    print(f"サヨナラ {walkoffs} 場，其中再見全壘打 {homers} 場")
    roster = fetch_roster_moves(max(SEASON_START, window[0]),
                                min(SEASON_END, window[1], datetime.date.today()),
                                cache=roster_cache)
    name_limits = solve_name_lengths(schedule, starters)
    if name_limits:
        print("為了塞進欄寬而統一縮短的投手名: "
              + "、".join(f"{n}→{n[:k]}" for n, k in sorted(name_limits.items())))
    def spell(name):
        # The 公示 spelling is not a key in the map, so fall back to the bare
        # family name — which is what the map would have produced anyway.
        return display.get(name) or display.get(surname(name)) or surname(name)

    announced = {teams: tuple(spell(p) for p in pair)
                 for teams, pair in fetch_announced_starters().items()}
    if announced:
        print(f"予告先発 {len(announced)} 場")
    rows = build_rows(schedule, starters, roster, name_limits, window=window,
                      announced=announced)

    played = sum(1 for _, _, c, _ in rows for t, bg, _ in c.values() if bg is WHITE and t)
    cancelled = sum(1 for _, _, c, _ in rows for t, bg, _ in c.values() if bg is BLUE and t)
    blank_days = sum(1 for _, _, c, _ in rows if all(bg is GREY for _, bg, _ in c.values()))
    noted = sum(1 for _, _, _, note in rows if note)
    print(f"{len(rows)} 天：有比分/對戰的欄位 {played}、雨天中止 {cancelled}、"
          f"整天沒比賽 {blank_days} 天、有登録抹消附註 {noted} 天")

    if dry_run:
        for label, weekday, cells, _ in rows[:12]:
            line = " | ".join(
                (cells[col][0] or ("灰" if cells[col][1] is GREY else
                                   "紫" if cells[col][1] is PURPLE else "·"))[:18]
                for col, *_ in TEAMS)
            print(f"{label:>5} {weekday}  {line}")
        return

    spreadsheet = client.open_by_key(WORKBOOK_KEY)

    if not full:
        # Row 1 is the header, so the season's first day sits on row 2.
        start_row = (window[0] - SEASON_START).days + 1
        sheet_id = spreadsheet.worksheet(SHEET_TITLE).id
        spreadsheet.batch_update({"requests": [{"updateCells": {
            "rows": sheet_payload(rows, with_header=False),
            "fields": "userEnteredValue,userEnteredFormat,textFormatRuns,note",
            "start": {"sheetId": sheet_id, "rowIndex": start_row, "columnIndex": 0},
        }}]})
        print(f"已更新第 {start_row + 1}~{start_row + len(rows)} 列")
        stale = warn_on_late_collisions(name_limits, starters, window)
        if stale:
            print("注意：以下投手的顯示名是全季計算的，但有比賽落在窗口之外，"
                  f"那些列不會被改到，需要跑一次 --full：{'、'.join(stale)}")
        return

    try:
        existing = spreadsheet.worksheet(SHEET_TITLE)
        sheet_id = existing.id
        spreadsheet.batch_update({"requests": [
            {"updateCells": {"range": {"sheetId": sheet_id},
                             "fields": "userEnteredValue,userEnteredFormat,"
                                       "textFormatRuns,note"}}]})
    except Exception:
        source = spreadsheet.worksheet(SOURCE_SHEET)
        created = spreadsheet.batch_update({"requests": [{"addSheet": {"properties": {
            "title": SHEET_TITLE,
            "index": source.index + 1,
            "gridProperties": {"rowCount": len(rows) + 40, "columnCount": 26,
                               "frozenRowCount": 1, "frozenColumnCount": 2},
            "tabColor": {"red": 0.6, "green": 0.8, "blue": 1.0},
        }}}]})
        sheet_id = created["replies"][0]["addSheet"]["properties"]["sheetId"]

    requests_ = [
        {"updateCells": {"rows": sheet_payload(rows),
                         "fields": "userEnteredValue,userEnteredFormat,textFormatRuns,note",
                         "start": {"sheetId": sheet_id, "rowIndex": 0,
                                   "columnIndex": 0}}},
        {"mergeCells": {"range": {"sheetId": sheet_id, "startRowIndex": 0,
                                  "endRowIndex": 1, "startColumnIndex": 0,
                                  "endColumnIndex": 2}, "mergeType": "MERGE_ALL"}},
    ]
    for iso in BANNERS:
        offset = (datetime.date.fromisoformat(iso) - SEASON_START).days
        if 0 <= offset < len(rows):
            # C:O, not the 2023 sheet's H:J — 18pt across 10 full-width glyphs
            # needs ~396px and H:J only spans 265, which clipped the title.
            requests_.append({"mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": offset + 1,
                          "endRowIndex": offset + 2, "startColumnIndex": 2,
                          "endColumnIndex": LAST_COL}, "mergeType": "MERGE_ALL"}})
    if ALLSTAR_GAMES:
        first = min((datetime.date.fromisoformat(d) - SEASON_START).days
                    for d in ALLSTAR_GAMES) + 1
        last = max((datetime.date.fromisoformat(d) - SEASON_START).days
                   for d in ALLSTAR_GAMES) + 2
        for date in ALLSTAR_GAMES:
            offset = (datetime.date.fromisoformat(date) - SEASON_START).days + 1
            requests_.append({"mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": offset,
                          "endRowIndex": offset + 1, "startColumnIndex": 7,
                          "endColumnIndex": 10}, "mergeType": "MERGE_ALL"}})
        top = min((datetime.date.fromisoformat(d) - SEASON_START).days
                  for d in ALLSTAR_DAYS) + 1
        bottom = max((datetime.date.fromisoformat(d) - SEASON_START).days
                     for d in ALLSTAR_DAYS) + 2
        for start_col, end_col in ALLSTAR_STAR_COLS:
            requests_.append({"mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": top,
                          "endRowIndex": bottom, "startColumnIndex": start_col,
                          "endColumnIndex": end_col}, "mergeType": "MERGE_ALL"}})
        for start_col, end_col in ((3, 5), (12, 14)):   # D:E and M:N
            requests_.append({"mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": first,
                          "endRowIndex": last, "startColumnIndex": start_col,
                          "endColumnIndex": end_col}, "mergeType": "MERGE_ALL"}})
    for col, width in [(0, 33), (1, 14), (SPACER_COL, 5)]:
        requests_.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": col, "endIndex": col + 1},
            "properties": {"pixelSize": width}, "fields": "pixelSize"}})
    for start, end in [(2, SPACER_COL), (SPACER_COL + 1, LAST_COL)]:
        requests_.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": start, "endIndex": end},
            "properties": {"pixelSize": 130}, "fields": "pixelSize"}})
    requests_.append({"updateDimensionProperties": {
        "range": {"sheetId": sheet_id, "dimension": "ROWS",
                  "startIndex": 0, "endIndex": 1},
        "properties": {"pixelSize": 30}, "fields": "pixelSize"}})
    requests_.append({"updateDimensionProperties": {
        "range": {"sheetId": sheet_id, "dimension": "ROWS",
                  "startIndex": 1, "endIndex": len(rows) + 1},
        "properties": {"pixelSize": 26}, "fields": "pixelSize"}})

    spreadsheet.batch_update({"requests": requests_})
    print(f"已寫入「{SHEET_TITLE}」(gid={sheet_id})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="只印出前幾列，不寫試算表")
    parser.add_argument("--cache", help="Yahoo 賽程 JSON 快取檔")
    parser.add_argument("--hr-cache", help="再見全壘打判定結果的快取檔")
    parser.add_argument("--url-cache", help="npb.jp 比賽網址對照的快取檔")
    parser.add_argument("--roster-cache", help="登録抹消公示的快取檔")
    parser.add_argument("--full", action="store_true",
                        help="重建整張表（會掃 Yahoo 整季，只在改版面時用）")
    parser.add_argument("--window-back", type=int, default=3,
                        help="增量模式往回幾天（預設 3）")
    parser.add_argument("--window-forward", type=int, default=21,
                        help="增量模式往後幾天（預設 21）")
    args = parser.parse_args()
    main(dry_run=args.dry_run, cache=args.cache, hr_cache=args.hr_cache,
         url_cache=args.url_cache, roster_cache=args.roster_cache,
         full=args.full, window_back=args.window_back,
         window_forward=args.window_forward)
