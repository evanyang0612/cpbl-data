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
ROW_GROUP = "group"
ROW_HEADER = "header"
ROW_DATA = "data"
ROW_BLANK = "blank"

# No 聯盟 column: each league sits under its own band, and a column repeating
# what the band above it says is the repeated header all over again.
#
# The three splits repeat the same three columns, so they are named once above
# in a band of their own rather than folded into every label. Two shallow rows
# read as three blocks; one row of 主場局數 / 主場ERA / 名次 reads as nine
# columns in a line.
SUB_LABELS = ("局數", "ERA", "名次")
COLUMN_GROUPS = (("球隊", 1), ("全場", 3), ("主場", 3), ("客場", 3), ("主-客", 1))

GROUP_ROW = [label if offset == 0 else ""
             for label, width in COLUMN_GROUPS for offset in range(width)]
HEADERS = [SUB_LABELS[offset] if width == 3 else ""
           for _, width in COLUMN_GROUPS for offset in range(width)]

# Where each group starts, within one league's half of the tab.
GROUP_STARTS = []
_cursor = 0
for _label, _width in COLUMN_GROUPS:
    GROUP_STARTS.append(_cursor)
    _cursor += _width

# The two leagues sit side by side rather than stacked: 央聯 on the left, 洋聯
# on the right, with a narrow column between them. Six teams ranked against
# each other fit on one screen that way, and the two tables can be read against
# each other instead of scrolled between.
LEAGUE_WIDTH = len(HEADERS)
LEFT_START = 0
RIGHT_START = LEAGUE_WIDTH + 1
TOTAL_WIDTH = RIGHT_START + LEAGUE_WIDTH
LEAGUE_STARTS = {"央聯": LEFT_START, "洋聯": RIGHT_START}

# Title, source, note, the league bands, then the two header rows — all
# pinned. Which league a column belongs to, which split it is, and what it
# measures never scroll away, so each table below needs only its own 【…】 band.
FROZEN_ROWS = 6

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


def _team_lines(totals, segment: str, league: str) -> list[list]:
    """One league's six rows for one segment, best team first."""
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

    lines = []
    for team in sorted(teams, key=lambda t: (ranks[None].get(t, 99), t)):
        line = [team]
        for venue in (None, "主", "客"):
            innings, runs = _split(totals, team, segment, venue)
            line += [round(innings, 1), _cell(era(innings, runs)),
                     ranks[venue].get(team, BLANK)]
        home, away = (era(*_split(totals, team, segment, side)) for side in VENUES)
        line.append(BLANK if home is None or away is None else round(home - away, 2))
        lines.append(line)
    return lines


def _section(totals, segment: str) -> list[list]:
    """One segment's table: the two leagues abreast, each ranked on its own."""
    rows = [[SECTION_TITLES[segment]]]
    left = _team_lines(totals, segment, "央聯")
    right = _team_lines(totals, segment, "洋聯")
    for central, pacific in zip(left, right):
        rows.append(central + [""] + pacific)
    return rows


def _league_band() -> list:
    """The row naming which half of the tab is which league."""
    row = [""] * TOTAL_WIDTH
    for league, start in LEAGUE_STARTS.items():
        row[start] = league
    return row


def build_sheet(rows: list[list], *, updated_at: str, season: str = "") -> list[list]:
    """The whole tab, top to bottom, as a values payload."""
    totals = accumulate(rows)
    games = sum(1 for row in rows if game_sides(row) is not None)
    values = [
        [f"NPB {season} 投手分項 — 先發 / 中繼 / 總計，主客場與名次".strip()],
        [f"資料來源：分析表紀錄 {games} 場　　更新：{updated_at}"],
        [f"中繼 = 球隊全場 − 先發；名次為該聯盟內排序（ERA 低者為 1）；"
         f"「主-客」負值代表主場較佳；主客場未滿 {MIN_INNINGS:g} 局不列入名次"],
        _league_band(),
        GROUP_ROW + [""] + GROUP_ROW,
        HEADERS + [""] + HEADERS,
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
        elif index == 3:
            roles.append(ROW_LEAGUE)
        elif index == 4:
            roles.append(ROW_GROUP)
        elif index == 5:
            roles.append(ROW_HEADER)
        elif not first:
            roles.append(ROW_BLANK)
        elif first.startswith("【"):
            roles.append(ROW_SECTION)
        else:
            roles.append(ROW_DATA)
    return roles


def league_blocks(values: list[list]) -> list[tuple[str, int, int, int]]:
    """(league, first column, first data row, last data row) for every block.

    Each league is ranked against itself, so it is also shaded and graded
    against itself; the formatter needs the blocks to do either. Both leagues
    share every data row, which is why a block is a column range as well as a
    row range.
    """
    spans: list[list] = []
    for index, role in enumerate(row_roles(values)):
        if role == ROW_SECTION:
            spans.append([None, None])
        elif role == ROW_DATA and spans:
            span = spans[-1]
            span[0] = index if span[0] is None else span[0]
            span[1] = index
    return [(league, start, first, last)
            for first, last in (span for span in spans if span[0] is not None)
            for league, start in LEAGUE_STARTS.items()]
