"""Each team's pitching, split starter / bullpen and home / away.

分析表紀錄 already carries, for both sides of every game, the starting
pitcher's line and the team's whole-game line. NPB publishes no separate relief
total, so the bullpen here is the second minus the first — which keeps the
sheet reading one source rather than standing up a second scrape beside it.

Nothing in this module touches the network or Google Sheets;
``migration/add_npb_pitching_splits_sheet.py`` drives the read and the write.
"""

from collections import defaultdict

# 分析表紀錄 column positions, 0-based. Both sides carry the same two blocks —
# the starter's line, then the team's whole-game line — laid out identically.
COL_DATE = 1
COL_AWAY_TEAM = 8
COL_HOME_TEAM = 11
SIDE_COLUMNS = {
    # venue: (team, starter innings, starter ER, team innings, team ER)
    "客": (COL_AWAY_TEAM, 34, 40, 43, 51),
    "主": (COL_HOME_TEAM, 59, 65, 68, 76),
}

STARTER = "先發"
BULLPEN = "中繼"
TEAM = "總計"
SEGMENTS = (STARTER, BULLPEN, TEAM)

# The total reads 投手總計 rather than 總計投手: it is the team's pitching, not
# a third kind of pitcher beside the other two.
SECTION_TITLES = {STARTER: "【先發投手】", BULLPEN: "【中繼投手】",
                  TEAM: "【投手總計】"}

VENUES = ("主", "客")

# Interleague games count towards a team's own totals, so the league a game is
# labelled with says nothing about whose column a row belongs in. The team does.
LEAGUES = {
    "巨人": "央聯", "阪神": "央聯", "横浜": "央聯",
    "広島": "央聯", "中日": "央聯", "ヤクルト": "央聯",
    "ソフトバンク": "洋聯", "日本ハム": "洋聯", "ロッテ": "洋聯",
    "オリックス": "洋聯", "楽天": "洋聯", "西武": "洋聯",
}
LEAGUE_ORDER = ("央聯", "洋聯")

# What each row of the payload is, so the formatter can paint the tab without
# re-deriving the layout from the strings in it.
ROW_TITLE = "title"
ROW_INFO = "info"
ROW_NOTE = "note"
ROW_SECTION = "section"
ROW_LEAGUE = "league"
ROW_HEADER = "header"
ROW_DATA = "data"
ROW_BLANK = "blank"

# No 聯盟 column: each league sits under its own band, and a column repeating
# what the band above it says is the repeated header all over again.
HEADERS = ["球隊", "局數", "ERA", "名次",
           "主場局數", "主場ERA", "名次",
           "客場局數", "客場ERA", "名次", "主-客"]

# Title, source, note, then the header — written once and pinned. Three stacked
# tables each repeating the same twelve labels is three chances to misread
# which table you are looking at, and it costs a row every time.
FROZEN_ROWS = 4

# Below this a split ERA is noise rather than a reading — a bullpen that has
# thrown a handful of innings in one park says nothing about the park.
MIN_INNINGS = 20.0

BLANK = "—"


def _number(value) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def era(innings: float, earned_runs: float) -> float | None:
    """Earned runs per nine innings, or ``None`` when nothing was pitched.

    A bullpen with no innings is not a perfect bullpen, so it has no ERA at all
    rather than a zero that would rank first.
    """
    if innings <= 0:
        return None
    return earned_runs * 9.0 / innings


def game_sides(row: list) -> tuple[dict, dict] | None:
    """The two sides of one 分析表紀錄 row, or ``None`` if it is not a game."""
    sides = []
    for venue, (team_col, s_ip, s_er, t_ip, t_er) in SIDE_COLUMNS.items():
        team = str(row[team_col]).strip() if len(row) > team_col else ""
        if not team:
            return None
        starter = (_number(row[s_ip]), _number(row[s_er]))
        total = (_number(row[t_ip]), _number(row[t_er]))
        sides.append({
            "team": team,
            "venue": venue,
            STARTER: starter,
            BULLPEN: (total[0] - starter[0], total[1] - starter[1]),
            TEAM: total,
        })
    # 客 first, so a caller can read the pair the way the row is written.
    sides.sort(key=lambda side: side["venue"] == "主")
    return tuple(sides)


def accumulate(rows: list[list]) -> dict[tuple[str, str, str], tuple[float, float]]:
    """Innings and earned runs by (team, segment, venue)."""
    totals: dict[tuple[str, str, str], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for row in rows:
        sides = game_sides(row)
        if sides is None:
            continue
        for side in sides:
            for segment in SEGMENTS:
                innings, runs = side[segment]
                bucket = totals[(side["team"], segment, side["venue"])]
                bucket[0] += innings
                bucket[1] += runs
    return {key: (round(value[0], 4), value[1]) for key, value in totals.items()}


def rank_within(eras: dict[str, float | None]) -> dict[str, int]:
    """Rank by ERA, lowest first. Teams level share a rank; the next is skipped.

    A team with no ERA to rank is left out rather than ranked last: it has not
    pitched, which is not the same as having pitched badly.
    """
    ranked = sorted(((value, team) for team, value in eras.items() if value is not None))
    ranks: dict[str, int] = {}
    for position, (value, team) in enumerate(ranked, start=1):
        previous = ranked[position - 2][0] if position > 1 else None
        ranks[team] = ranks[ranked[position - 2][1]] if value == previous else position
    return ranks


def _cell(value: float | None, *, digits: int = 2) -> str | float:
    return BLANK if value is None else round(value, digits)


def _split(totals, team: str, segment: str, venue: str | None) -> tuple[float, float]:
    venues = VENUES if venue is None else (venue,)
    innings = sum(totals.get((team, segment, side), (0.0, 0.0))[0] for side in venues)
    runs = sum(totals.get((team, segment, side), (0.0, 0.0))[1] for side in venues)
    return innings, runs


def _section(totals, segment: str) -> list[list]:
    """One segment's table: both leagues, each ranked on its own.

    No header of its own, and no blank row between the leagues — the header is
    pinned above all three tables, and the leagues are told apart by colour.
    """
    rows = [[SECTION_TITLES[segment]]]
    for index, league in enumerate(LEAGUE_ORDER):
        if index:
            rows.append([])
        rows.append([league])
        teams = [team for team, lg in LEAGUES.items() if lg == league]
        overall = {team: era(*_split(totals, team, segment, None)) for team in teams}
        # Ranked on the season, and on each venue separately: a bullpen can be
        # mid-table overall and worst in the league away from home, which is
        # exactly the difference this sheet is for.
        ranks = {venue: rank_within({
            team: (era(*_split(totals, team, segment, venue))
                   if _split(totals, team, segment, venue)[0] >= MIN_INNINGS else None)
            for team in teams})
            for venue in VENUES}
        ranks[None] = rank_within(overall)
        for team in sorted(teams, key=lambda t: (ranks[None].get(t, 99), t)):
            line = [team]
            for venue in (None, "主", "客"):
                innings, runs = _split(totals, team, segment, venue)
                line += [round(innings, 1), _cell(era(innings, runs)),
                         ranks[venue].get(team, BLANK)]
            home, away = (era(*_split(totals, team, segment, side)) for side in VENUES)
            line.append(BLANK if home is None or away is None else round(home - away, 2))
            rows.append(line)
    return rows


def build_sheet(rows: list[list], *, updated_at: str, season: str = "") -> list[list]:
    """The whole tab, top to bottom, as a values payload."""
    totals = accumulate(rows)
    games = sum(1 for row in rows if game_sides(row) is not None)
    values = [
        [f"NPB {season} 投手分項 — 先發 / 中繼 / 總計，主客場與名次".strip()],
        [f"資料來源：分析表紀錄 {games} 場　　更新：{updated_at}"],
        [f"中繼 = 球隊全場 − 先發；名次為該聯盟內排序（ERA 低者為 1）；"
         f"「主-客」負值代表主場較佳；主客場未滿 {MIN_INNINGS:g} 局不列入名次"],
        HEADERS,
    ]
    for index, segment in enumerate(SEGMENTS):
        if index:
            values.append([])
        values += _section(totals, segment)
    return values


def row_roles(values: list[list]) -> list[str]:
    """What each row of ``build_sheet``'s payload is.

    The formatter needs to know which band a row belongs to, and reading that
    back off the text is the kind of guess that breaks the first time a team
    is renamed. Derived here, beside the code that lays the rows out.
    """
    roles = []
    for index, row in enumerate(values):
        first = str(row[0]) if row else ""
        if index == 0:
            roles.append(ROW_TITLE)
        elif index == 1:
            roles.append(ROW_INFO)
        elif index == 2:
            roles.append(ROW_NOTE)
        elif not first:
            roles.append(ROW_BLANK)
        elif first.startswith("【"):
            roles.append(ROW_SECTION)
        elif first in LEAGUE_ORDER:
            roles.append(ROW_LEAGUE)
        elif row == HEADERS:
            roles.append(ROW_HEADER)
        else:
            roles.append(ROW_DATA)
    return roles


def league_blocks(values: list[list]) -> list[tuple[str, int, int]]:
    """(league, first data row, last data row) for every block in the tab.

    Each league is ranked against itself, so it is also shaded and graded
    against itself; the formatter needs the blocks to do either.
    """
    blocks: list[list] = []
    for index, role in enumerate(row_roles(values)):
        if role == ROW_LEAGUE:
            blocks.append([str(values[index][0]), None, None])
        elif role == ROW_DATA and blocks:
            block = blocks[-1]
            block[1] = index if block[1] is None else block[1]
            block[2] = index
    return [(league, first, last) for league, first, last in blocks
            if first is not None]
