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
ROW_RAW = "raw"

# One split at a time. Showing 全場, 主場 and 客場 side by side put nine
# columns in a row that mostly repeated each other; the dropdown picks which
# one is on the page, and the table is sorted by that split's own ERA. There
# is no direction to choose either — the lowest ERA is the best staff.
SPLITS = (("全場", None), ("主場", "主"), ("客場", "客"))
SPLIT_LABELS = tuple(label for label, _ in SPLITS)
SORT_CELL = "B2"
DEFAULT_SPLIT = SPLIT_LABELS[0]

SUB_LABELS = ("局數", "ERA", "名次")

# 球隊 spans both header rows; the split's name sits over its three columns and
# is a formula, so it says which split is on the page as the dropdown changes.
COLUMN_GROUPS = (("球隊", 1), (f"=${SORT_CELL}", len(SUB_LABELS)))
GROUP_ROW = [label if offset == 0 else ""
             for label, width in COLUMN_GROUPS for offset in range(width)]
HEADERS = [SUB_LABELS[offset] if width == len(SUB_LABELS) else ""
           for _, width in COLUMN_GROUPS for offset in range(width)]

GROUP_STARTS = []
_cursor = 0
for _label, _width in COLUMN_GROUPS:
    GROUP_STARTS.append(_cursor)
    _cursor += _width

# The two leagues sit side by side rather than stacked: 央聯 on the left, 洋聯
# on the right, with a narrow column between them.
LEAGUE_WIDTH = len(HEADERS)
LEFT_START = 0
RIGHT_START = LEAGUE_WIDTH + 1
TOTAL_WIDTH = RIGHT_START + LEAGUE_WIDTH
LEAGUE_STARTS = {"央聯": LEFT_START, "洋聯": RIGHT_START}

# Title, source, note, the league bands, then the two header rows — all pinned.
FROZEN_ROWS = 6

# The hidden blocks carry every split; the formula above takes the three
# columns the dropdown asks for. 球隊, then 局數 / ERA / 名次 per split.
RAW_WIDTH = 1 + len(SPLITS) * len(SUB_LABELS)
RAW_LABEL = "↓ 排序用原始資料（勿刪，此區已隱藏）"

TEAMS_PER_LEAGUE = 6

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
    """One league's six rows for one segment: the team, then every split.

    Written best-first on the season, but the order on the page comes from the
    SORT() above them — this is only the order the hidden block happens to sit
    in.
    """
    teams = [team for team, lg in LEAGUES.items() if lg == league]
    # Ranked on the season, and on each venue separately: a bullpen can be
    # mid-table overall and worst in the league away from home, which is
    # exactly the difference this sheet is for.
    ranks = {}
    for _label, venue in SPLITS:
        eras = {}
        for team in teams:
            innings, runs = _split(totals, team, segment, venue)
            eras[team] = (era(innings, runs)
                          if venue is None or innings >= MIN_INNINGS else None)
        ranks[venue] = rank_within(eras)

    lines = []
    for team in sorted(teams, key=lambda t: (ranks[None].get(t, 99), t)):
        line = [team]
        for _label, venue in SPLITS:
            innings, runs = _split(totals, team, segment, venue)
            line += [round(innings, 1), _cell(era(innings, runs)),
                     ranks[venue].get(team, BLANK)]
        lines.append(line)
    return lines


def _column_letter(index: int) -> str:
    """A1 column letter for a 0-based column index."""
    letters = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _sort_formula(first_raw_row: int) -> str:
    """The team column plus whichever split the dropdown asks for, sorted.

    Built as a nested IF so the three-column window moves without the block
    being rewritten; sorted on the third column of the result, which is that
    split's ERA whichever split it is.
    """
    top, bottom = first_raw_row + 1, first_raw_row + TEAMS_PER_LEAGUE
    windows = []
    for index in range(len(SPLITS)):
        first = _column_letter(1 + index * len(SUB_LABELS))
        last = _column_letter(len(SUB_LABELS) + index * len(SUB_LABELS))
        windows.append(f"${first}${top}:${last}${bottom}")
    choice = windows[-1]
    for (label, _venue), window in reversed(list(zip(SPLITS[:-1], windows))):
        choice = f'IF(${SORT_CELL}="{label}", {window}, {choice})'
    return f"=SORT({{$A${top}:$A${bottom}, {choice}}}, 3, TRUE)"


def _league_band() -> list:
    """The row naming which half of the tab is which league."""
    row = [""] * TOTAL_WIDTH
    for league, start in LEAGUE_STARTS.items():
        row[start] = league
    return row


def build_sheet(rows: list[list], *, updated_at: str, season: str = "") -> list[list]:
    """The whole tab, top to bottom, as a values payload.

    The visible tables are one SORT() each over a block kept in the hidden rows
    at the bottom, so the reader can change which split is shown, and how the
    tables are ordered, without the sheet being rebuilt.
    """
    totals = accumulate(rows)
    games = sum(1 for row in rows if game_sides(row) is not None)
    control = [""] * TOTAL_WIDTH
    control[0], control[1] = "顯示", DEFAULT_SPLIT
    control[3] = f"分析表紀錄 {games} 場　更新：{updated_at}"
    values = [
        [f"NPB {season} 投手分項 — 先發 / 中繼 / 總計".strip()],
        control,
        [f"中繼 = 球隊全場 − 先發；名次為該聯盟內同一項目的 ERA 排序（低者為 1）；"
         f"主客場未滿 {MIN_INNINGS:g} 局不列入名次"],
        _league_band(),
        GROUP_ROW + [""] + GROUP_ROW,
        HEADERS + [""] + HEADERS,
    ]

    # The visible tables first, then the blocks they sort, so the formulas can
    # point at rows that do not exist yet.
    anchors = []
    for index, segment in enumerate(SEGMENTS):
        if index:
            values.append([])
        values.append([SECTION_TITLES[segment]])
        anchors.append(len(values))
        values += [[""] * TOTAL_WIDTH for _ in range(TEAMS_PER_LEAGUE)]

    values.append([])
    values.append([RAW_LABEL])
    # Stacked in column A rather than laid out beside each other: the visible
    # tables are only four columns wide, and a block placed to the right of one
    # would sit under the other league's table.
    for position, segment in enumerate(SEGMENTS):
        for league, start in LEAGUE_STARTS.items():
            first_raw = len(values)
            values += _team_lines(totals, segment, league)
            values[anchors[position]][start] = _sort_formula(first_raw)
    return values


def row_roles(values: list[list]) -> list[str]:
    """What each row of ``build_sheet``'s payload is.

    The formatter needs to know which band a row belongs to, and reading that
    back off the text is the kind of guess that breaks the first time a team is
    renamed — the visible table rows do not even hold a team name any more,
    only the SORT() that fills them. Derived here, beside the code that lays
    the rows out.
    """
    fixed = [ROW_TITLE, ROW_INFO, ROW_NOTE, ROW_LEAGUE, ROW_GROUP, ROW_HEADER]
    roles = list(fixed[:len(values)])
    remaining, raw = 0, False
    for index in range(len(fixed), len(values)):
        first = str(values[index][0]) if values[index] else ""
        if raw:
            roles.append(ROW_RAW)
        elif first == RAW_LABEL:
            roles.append(ROW_RAW)
            raw = True
        elif remaining:
            roles.append(ROW_DATA)
            remaining -= 1
        elif first.startswith("【"):
            roles.append(ROW_SECTION)
            remaining = TEAMS_PER_LEAGUE
        else:
            roles.append(ROW_BLANK)
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
