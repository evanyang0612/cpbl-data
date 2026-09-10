"""Read a pitching line out of an npb.jp box score.

Yahoo serves only the current season — a request for any game before it comes
back empty, not 404 — so every earlier year has to come from npb.jp. Its box
score is also the richer of the two for this purpose: 賽錄 records a starter's
innings and earned runs and nothing else, while this page gives 投球数 and
四球 for every pitcher who appeared.

The page is stable back to 2016. 2015 and earlier have no schedule pages at
all, so that is where the history ends.

    https://npb.jp/scores/<year>/<mmdd>/<code>/box.html
"""

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

# The two pitching tables. "t" is the side batting in the top half — the away
# team — and "b" the home team.
AWAY_TABLE_ID = "tablefix_t_p"
HOME_TABLE_ID = "tablefix_b_p"
LINE_SCORE_ID = "tablefix_ls"

# Neither of these names a pitcher: one heads the table, the other sums it.
NON_PITCHER_NAMES = {"投手", "チーム計", ""}

_CANCELLED_MARKERS = ("試合中止", "中止")


@dataclass(frozen=True)
class PitcherLine:
    """One pitcher's line, in the order he appeared."""

    order: int
    name: str
    player_id: str | None
    result: str
    pitches: int
    batters: int
    outs: int
    hits: int
    hr: int
    bb: int
    hbp: int
    so: int
    wp: int
    balk: int
    runs: int
    er: int

    @property
    def is_starter(self) -> bool:
        return self.order == 1

    @property
    def ip(self) -> float:
        """Innings as a number that can be summed, not `.1`/`.2` notation."""
        return self.outs / 3


def is_cancelled(html: str) -> bool:
    """Whether the page says the game was called off.

    A rained-out game serves a real 200 with no pitching tables, which looks
    exactly like a throttled or truncated response. Only the games this returns
    True for are safely "no data"; anything else that fails to parse has to be
    retried rather than cached, or a refused request gets frozen into the
    record as a game nobody pitched.
    """
    return any(marker in html for marker in _CANCELLED_MARKERS)


def _cells(row):
    """The row's own cells, ignoring those of the nested innings table."""
    return row.find_all(["th", "td"], recursive=False)


def _int(cell) -> int:
    text = cell.get_text(strip=True).replace("\xa0", "")
    try:
        return int(text)
    except ValueError:
        return 0


def _outs(cell) -> int:
    """Innings pitched, as outs.

    npb.jp renders them as a nested one-row table: whole innings in the <th>,
    thirds in the <td> as ".1" or ".2". A whole-inning outing leaves the second
    cell empty rather than writing ".0".
    """
    inner = cell.find("table")
    if inner is None:
        return 0
    whole_cell, frac_cell = inner.find("th"), inner.find("td")
    whole = _int(whole_cell) if whole_cell else 0
    frac = 0
    if frac_cell:
        text = frac_cell.get_text(strip=True).replace("\xa0", "").lstrip(".")
        frac = int(text) if text.isdigit() else 0
    return whole * 3 + frac


def _parse_pitcher_table(table) -> list[PitcherLine]:
    lines: list[PitcherLine] = []
    for row in table.find_all("tr"):
        # Rows of the nested innings table are <tr>s too; only the outer
        # table's own rows carry a full pitching line.
        if row.find_parent("table") is not table:
            continue
        cells = _cells(row)
        if len(cells) < 14:
            continue
        name = cells[1].get_text(strip=True).replace("\xa0", "")
        if name in NON_PITCHER_NAMES:
            continue
        link = cells[1].find("a", href=True)
        player_id = None
        if link:
            player_id = link["href"].rsplit("/", 1)[-1].removesuffix(".html")
        lines.append(PitcherLine(
            order=len(lines) + 1,
            name=name,
            player_id=player_id,
            result=cells[0].get_text(strip=True).replace("\xa0", ""),
            pitches=_int(cells[2]),
            batters=_int(cells[3]),
            outs=_outs(cells[4]),
            hits=_int(cells[5]),
            hr=_int(cells[6]),
            bb=_int(cells[7]),
            hbp=_int(cells[8]),
            so=_int(cells[9]),
            wp=_int(cells[10]),
            balk=_int(cells[11]),
            runs=_int(cells[12]),
            er=_int(cells[13]),
        ))
    return lines


def _team_name(cell) -> str:
    """The short spelling, which is how 賽錄 and 分析表紀錄 write teams.

    The cell carries both forms, one hidden per breakpoint: `hide_sp` holds
    「中日ドラゴンズ」 and `hide_pc` holds 「中日」.
    """
    short = cell.find("span", class_="hide_pc")
    if short:
        return short.get_text(strip=True)
    return cell.get_text(strip=True)


def parse_box(html: str) -> dict | None:
    """Both sides' pitching, or None when the page carries no box score.

    None covers a called-off game and a response that arrived incomplete alike;
    `is_cancelled` is what separates them.
    """
    soup = BeautifulSoup(html, "html.parser")
    away_table = soup.find("table", id=AWAY_TABLE_ID)
    home_table = soup.find("table", id=HOME_TABLE_ID)
    # Half a page is worse than none: cached, it would read as a game only one
    # side pitched in.
    if away_table is None or home_table is None:
        return None

    away_pitchers = _parse_pitcher_table(away_table)
    home_pitchers = _parse_pitcher_table(home_table)
    if not away_pitchers or not home_pitchers:
        return None

    away = home = ""
    line_score = soup.find("table", id=LINE_SCORE_ID)
    if line_score:
        rows = line_score.find_all("tr")
        if len(rows) >= 3:
            away = _team_name(rows[1].find(["th", "td"]))
            home = _team_name(rows[2].find(["th", "td"]))

    return {
        "away": away,
        "home": home,
        "away_pitchers": away_pitchers,
        "home_pitchers": home_pitchers,
    }


# ---------------------------------------------------------------- fetching

BASE_URL = "https://npb.jp"
SCHEDULE_URL = BASE_URL + "/games/{year}/schedule_{month:02d}_detail.html"
# The season, plus the months either side that carry 交流戦 and the postseason.
SEASON_MONTHS = range(3, 12)
# 2015 and earlier have no schedule pages, so the history starts here.
EARLIEST_YEAR = 2016

_GAME_HREF = re.compile(r"/scores/(\d{4})/(\d{4})/([a-z0-9-]+)/")

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}


class Throttled(Exception):
    """A page came back neither parseable nor explicably empty.

    npb.jp answers a refused request with a 200 and a page that simply has no
    tables — indistinguishable from a rained-out game except that a rainout
    says so. Raising here rather than returning None is what stops the caller
    caching a refusal as fact.
    """


def game_code(year: str, mmdd: str, code: str) -> str:
    """A stable id for one game, usable as a cache filename."""
    return f"{year}{mmdd}-{code}"


def box_url(year: str, mmdd: str, code: str) -> str:
    return f"{BASE_URL}/scores/{year}/{mmdd}/{code}/box.html"


def parse_schedule(html: str) -> list[tuple[str, str, str]]:
    """Every game linked from one monthly schedule page, deduplicated.

    The links are read off the page rather than built from a pattern: the
    game-number suffix is not always `-01`, and 交流戦 lives on its own page.
    """
    return sorted(set(_GAME_HREF.findall(html)))


def read_box(html: str) -> dict | None:
    """Parse a fetched page, or say why it cannot be trusted.

    Returns None only for a game that was called off — the one case where
    "no box score" is the truth and worth remembering.
    """
    box = parse_box(html)
    if box is not None:
        return box
    if is_cancelled(html):
        return None
    raise Throttled("no pitching tables and no cancellation notice")
