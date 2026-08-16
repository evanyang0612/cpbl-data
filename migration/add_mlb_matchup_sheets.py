"""One-off script: rebuild the MLB spreadsheet's 設定 + 對戰 (1)…(15) tabs as a
straight port of the NPB spreadsheet's 設定 + 対戦 (n) layout.

Source layout (NPB, spreadsheet 1XBATQ…, tabs 設定 / 対戦 (1)):

  設定    one 7-column block per matchup (B..G, I..N, …, stride 7), three row bands
          (3-7 / 10-14 / 17-21) = the three games of the series. Each band holds two
          rows of 隊伍 + 先發 (both dropdowns) and previews, per season, the starter's
          ERA versus that opponent (VS. row) and overall (合 row). Those previews divide
          hidden helper rows 39-71, which hold the earned-run and starter-innings sums.

  対戦(n) one tab per matchup: six 10-column blocks (B/K, V/AE, AP/AY) = three games ×
          two starters. Rows 4-7 are the starter's overall 客/主/合 line (場次, QS, QS%,
          ERA, IP, opponent runs through 5, opponent runs total), rows 9-12 the same
          split versus the actual opponent, then one 5-row block per team in the
          starter's own league.

Differences from the source, all deliberate:
  * 15 matchups / 15 tabs and 15 league teams per block instead of NPB's 6 and 6.
  * MLB team colours in the conditional formats.
  * The per-team VS. blocks pick the AL or NL list from the block's own team, since an
    MLB tab is AL one day and NL the next (the cells stay dropdowns for overriding).
  * COUNTIFS/SUMIFS instead of SUMPRODUCT, and 合 rows derived from the two rows above
    them. Identical numbers, far less recalculation over 紀錄's 22k rows.

Named-range mapping, NPB 賽錄 -> MLB 紀錄:
  日期→日期  客場隊伍→客隊隊伍  主場隊伍→主隊隊伍  客場先發→客隊先發  主場先發→主隊先發
  客責失→客自責  主責失→主自責  客先局→客先局  主先局→主先局  客總5→客五總  主總5→主五總
  客總分→客隊總分  主總分→主隊總分  球隊→隊名  客QS=1→客QS="QS"  主QS=1→主QS="QS"

Usage:
    uv run python migration/add_mlb_matchup_sheets.py --dry-run
    uv run python migration/add_mlb_matchup_sheets.py
"""

import argparse
import sys

sys.path.insert(0, "/Users/evansmac/cpbl")

from dotenv import load_dotenv

load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")

MLB_SPREADSHEET_KEY = "11FV70TXVAxLTwYH6pLj7HwK1qq-fIa61QrePRCC8YUM"

SETTING_TITLE = "設定"
MATCHUP_COUNT = 15
MATCHUP_TITLES = [f"對戰 ({n})" for n in range(1, MATCHUP_COUNT + 1)]

# Tabs the previous hand-built attempts left behind; all confirmed to be referenced
# only by each other, so deleting them cannot strand a formula elsewhere.
DOOMED_TITLES = [
    "設定",
    *MATCHUP_TITLES,
    "對戰1",
    "對戰1copy",
    "對戰2",
    "對戰3",
    "「設定」的副本",
    "「対戦 (1)」的副本",
    "MLB對戰設定新版",
    "MLB對戰新版",
    "ALE",
]

# 資料!C2:C16 and 資料!D2:D16, in the workbook's own order (by division).
AL_TEAMS = ["NYY", "TB", "BOS", "TOR", "BAL", "MIN", "CLE", "CWS", "KC", "DET",
            "HOU", "OAK", "TEX", "LAA", "SEA"]
NL_TEAMS = ["ATL", "WSH", "PHI", "NYM", "MIA", "CHC", "STL", "MIL", "CIN", "PIT",
            "LAD", "AZ", "SF", "SD", "COL"]
LEAGUE_SIZE = len(AL_TEAMS)

# Team codes that need a second criterion because 紀錄 holds more than one code for
# the franchise. Empty since migration/normalize_mlb_team_codes.py collapsed the
# Athletics' ATH rows onto OAK and baseball/mlb_teams.py keeps new rows canonical;
# add an entry here if MLB ever renames another club mid-history.
TEAM_ALIASES: dict[str, str] = {}

TEAM_PRIMARY_HEX = {
    "NYY": "1C2841", "BOS": "BD3039", "TOR": "134A8E", "TB": "092C5C", "BAL": "DF4601",
    "MIN": "002B5C", "CLE": "0C2340", "CWS": "27251F", "KC": "004687", "DET": "0C2340",
    "HOU": "002D62", "OAK": "003831", "TEX": "003278", "LAA": "BA0021", "SEA": "0C2C56",
    "ATL": "13274F", "WSH": "AB0003", "PHI": "E81828", "NYM": "002D72", "MIA": "00A3E0",
    "CHC": "0E3386", "STL": "C41E3A", "MIL": "12284B", "CIN": "C6011F", "PIT": "27251F",
    "LAD": "005A9C", "AZ": "A71930", "SF": "FD5A1E", "SD": "2F241D", "COL": "333366",
}

# --- named ranges on the MLB spreadsheet -------------------------------------------
DATE = "日期"
AWAY_TEAM, HOME_TEAM = "客隊隊伍", "主隊隊伍"
AWAY_SP, HOME_SP = "客隊先發", "主隊先發"
AWAY_QS, HOME_QS = "客QS", "主QS"
AWAY_ER, HOME_ER = "客自責", "主自責"
AWAY_IP, HOME_IP = "客先局", "主先局"
AWAY_R5, HOME_R5 = "客五總", "主五總"
AWAY_R, HOME_R = "客隊總分", "主隊總分"
TEAM_LIST, VENUE_LIST = "隊名", "場地"
AL_RANGE, NL_RANGE = "AL", "NL"

# Row-1 date windows are dropdowns, as on NPB, off two lists of season bounds.
SEASON_START_RANGE, SEASON_END_RANGE = "開賽年度", "閉幕年度"
SEASON_LIST_SHEET = "資料"
SEASON_START_COL, SEASON_END_COL = 6, 7  # 資料!G / 資料!H
FIRST_SEASON = 2017                      # 紀錄 starts in 2017
SEASON_LIST_ROWS = 40                    # how far the named range reaches (to 2055)

WINDOW_FROM_DEFAULT = "=DATE(YEAR(TODAY())-2,3,1)"
WINDOW_TO_DEFAULT = "=DATE(YEAR(TODAY()),12,1)"

# --- colours / fonts ---------------------------------------------------------------
GREY = "666666"
WHITE = "FFFFFF"
BLACK = "000000"
DARK_TEXT = "434343"
DATE_BLUE = "4A86E8"
GREEN = "3D8329"
RED = "DA0000"
FONT_TEAM = "Microsoft JhengHei"
FONT_DATA = "Arial, sans-serif"

# --- 設定 geometry -----------------------------------------------------------------
SETTING_BANDS = 3
SETTING_BLOCK_WIDTH = 7
SETTING_BAND_HEADER_ROWS = (3, 10, 17)
SETTING_YEAR_ROWS = (39, 40, 41)  # season label / window start / window end
# helper rows per (band, side): VS. numerator, VS. denominator, 合 numerator, 合 denominator
SETTING_HELPER_ROWS = {
    (0, 0): (43, 44, 45, 46), (0, 1): (48, 49, 50, 51),
    (1, 0): (53, 54, 55, 56), (1, 1): (58, 59, 60, 61),
    (2, 0): (63, 64, 65, 66), (2, 1): (68, 69, 70, 71),
}
SETTING_NOTE_ROWS = (23, 25)
SETTING_ROW_COUNT = 120
SETTING_HELPER_COL_START = 107  # first pitcher-dropdown helper column (0-based)

# --- 對戰 geometry -----------------------------------------------------------------
MATCHUP_BLOCK_WIDTH = 8   # 隊伍/場次, QS, QS%, ERA, IP, 5, 9+ (label column included)
MATCHUP_GAMES = 3
VS_ROW_START = 15
VS_ROW_STRIDE = 5
MATCHUP_LAST_ROW = VS_ROW_START + (LEAGUE_SIZE - 1) * VS_ROW_STRIDE + 3  # 88
MATCHUP_ROW_COUNT = MATCHUP_LAST_ROW + 4
MATCHUP_FROZEN_ROWS = 13
# The starter's name is shown broken after the first name, so every block reads the
# same two lines whatever the name's length. Row 2 keeps the raw name — that is what
# the aggregates match against; it is白 on white and 11px tall, so it never shows.
MATCHUP_NAME_FONT_SIZE = 11
MATCHUP_NAME_ROW_HEIGHT = 38
MATCHUP_KEY_ROW = 2
MATCHUP_NAME_ROW = 3

NUMERIC_RULES = (
    ("era", "<=3.5", GREEN),
    ("era", ">=4", RED),
    ("five", "<2", GREEN),
    ("five", ">=2.5", RED),
    ("nine", "<4", GREEN),
    ("nine", ">=4.5", RED),
)
# column offset within a block for each metric the numeric rules colour
METRIC_OFFSET = {"era": 4, "five": 6, "nine": 7}

# Thresholds as stated in the legend below, so what the sheet says is what it does.
QS_RATE_GREEN = ">66%"
QS_RATE_RED = "<=40%"

# Ballparks whose stored name should not follow what the API currently reports —
# MLB feeds Dodger Stadium under a sponsor's name that nobody calls it by.
HOME_PARK_OVERRIDES = {"LAD": "Dodger Stadium"}
HOME_PARK_ROWS = (2, 31)  # 資料!A2:B31 — team code beside its home park
HOME_PARK_SCAN_ROWS = 2000  # roughly the last two seasons of 紀錄
RECORD_TITLE = "紀錄"

LEGEND_ROWS = (27, 28)  # title, then the red and green boxes side by side
LEGEND_TITLE = "★ 為提高警示及便於瀏覽，對戰分頁中數字色彩的顯示條件如下"
LEGEND_RED = [
    ("先發QS率低於(含) ", None), ("40%", RED),
    ("\n對戰防禦率高於(含) ", None), ("4.00", RED),
    ("\n5局失分高於(含) ", None), ("2.50", RED),
    ("\n9局失分高於(含) ", None), ("4.50", RED),
    ("\n數字顯示為 ", None), ("紅色", RED),
]
LEGEND_GREEN = [
    ("先發QS率高於 ", None), ("66%", GREEN),
    ("\n對戰防禦率低於(包含) ", None), ("3.50", GREEN),
    ("\n5局失分低於(不含) ", None), ("2.00", GREEN),
    ("\n9局失分低於(不含) ", None), ("4.00", GREEN),
    ("\n數字顯示為 ", None), ("綠色", GREEN),
]

# Zero renders as blank: the third section of a number format is the zero case, and
# leaving it empty hides it. Without this the VS. blocks are a wall of 0 / 0.00 / 0%
# for every opponent a starter has never faced.
FORMAT_COUNT = "0;-0;;@"
FORMAT_PERCENT = "0%;-0%;;@"
FORMAT_NUMBER = "0.00;-0.00;;@"


# ===================================================================================
# geometry helpers
# ===================================================================================
def col_letter(idx0: int) -> str:
    """0-indexed column -> A1 letter(s)."""
    idx = idx0 + 1
    letters = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def matchup_title(n: int) -> str:
    return f"對戰 ({n})"


def setting_block_col(block: int) -> int:
    return 1 + block * SETTING_BLOCK_WIDTH


def setting_band_header_row(band: int) -> int:
    return SETTING_BAND_HEADER_ROWS[band]


def setting_team_cell(block: int, band: int, side: int) -> str:
    return f"{col_letter(setting_block_col(block))}{setting_band_header_row(band) + 1 + side * 2}"


def setting_pitcher_cell(block: int, band: int, side: int) -> str:
    return f"{col_letter(setting_block_col(block) + 1)}{setting_band_header_row(band) + 1 + side * 2}"


def setting_helper_rows(band: int, side: int) -> tuple[int, int, int, int]:
    return SETTING_HELPER_ROWS[(band, side)]


def setting_helper_column(block: int, band: int, side: int) -> int:
    """Column holding the INDIRECT() pitcher list backing one 先發 dropdown."""
    return SETTING_HELPER_COL_START + (band * MATCHUP_COUNT + block) * 2 + side


def matchup_block_col(game: int, side: int) -> int:
    """0-based first column of one starter's block on a 對戰 tab."""
    return 1 + game * 20 + side * 9


def matchup_window_cells(game: int) -> tuple[str, str]:
    """The two row-1 cells holding this game's date window (from, to)."""
    first = matchup_block_col(game, 0)
    second = matchup_block_col(game, 1)
    return f"${col_letter(first + 5)}$1", f"${col_letter(second)}$1"


def vs_group_row(index: int) -> int:
    """Header row of a VS. group: 0 is the actual opponent, 1..15 the league list."""
    return 9 if index == 0 else VS_ROW_START + (index - 1) * VS_ROW_STRIDE


def data_row_spans() -> list[tuple[int, int]]:
    """The 客/主/合 row triples, excluding every header and separator row."""
    spans = [(5, 7)]
    spans += [(vs_group_row(i) + 1, vs_group_row(i) + 3)
              for i in range(0, LEAGUE_SIZE + 1)]
    return spans


def contrast_text_hex(bg_hex: str) -> str:
    r, g, b = (int(bg_hex[i:i + 2], 16) for i in (0, 2, 4))
    luma = 0.299 * r + 0.587 * g + 0.114 * b
    return BLACK if luma > 150 else WHITE


# ===================================================================================
# formula builders
# ===================================================================================
def _window(window: tuple[str, str]) -> str:
    return f'{DATE},">="&{window[0]},{DATE},"<"&{window[1]}'


def _with_alias(build, cell: str | None) -> str:
    """Aggregate over a team cell, plus a second term per alternate code in play.

    COUNTIFS/SUMIFS refuse to broadcast an array criterion ({"OAK";"ATH"} silently
    counts OAK only), so a franchise recorded under two codes needs its own term. It
    sits inside an IF so Sheets only evaluates it when that team is actually selected.
    With TEAM_ALIASES empty — 紀錄 normalised — this is just the plain aggregate.
    """
    primary = build(cell)
    if cell is None or not TEAM_ALIASES:
        return primary
    quoted = lambda code: build(chr(34) + code + chr(34))  # noqa: E731
    term = primary
    for code, alias in TEAM_ALIASES.items():
        term += (f'+IF({cell}="{code}",{quoted(alias)},'
                 f'IF({cell}="{alias}",{quoted(code)},0))')
    return term


def _side_ranges(side: int) -> dict[str, str]:
    if side == 0:
        return {"sp": AWAY_SP, "qs": AWAY_QS, "er": AWAY_ER, "ip": AWAY_IP,
                "opp_team": HOME_TEAM, "opp_r5": HOME_R5, "opp_r": HOME_R}
    return {"sp": HOME_SP, "qs": HOME_QS, "er": HOME_ER, "ip": HOME_IP,
            "opp_team": AWAY_TEAM, "opp_r5": AWAY_R5, "opp_r": AWAY_R}


def _data_row_formulas(block_col: int, window: tuple[str, str], row: int, side: int,
                       opponent_cell: str | None) -> dict[str, str]:
    """One 客 or 主 row: 場次, QS, QS%, ERA, IP, opponent runs through 5, and total."""
    r = _side_ranges(side)
    pitcher = f"${col_letter(block_col + 5)}${MATCHUP_KEY_ROW}"
    base = f"{r['sp']},{pitcher},{_window(window)}"

    def aggregate(fn: str, value_range: str | None = None, extra: str = ""):
        def build(criterion):
            args = f"{value_range}," if value_range else ""
            args += base + extra
            if criterion is not None:
                args += f",{r['opp_team']},{criterion}"
            return f"{fn}({args})"
        return _with_alias(build, opponent_cell)

    count = aggregate("COUNTIFS")
    qs = aggregate("COUNTIFS", extra=f',{r["qs"]},"QS"')
    er = aggregate("SUMIFS", r["er"])
    ip = aggregate("SUMIFS", r["ip"])
    runs5 = aggregate("SUMIFS", r["opp_r5"])
    runs = aggregate("SUMIFS", r["opp_r"])

    # only an alias-widened expression is a sum, and only a sum needs bracketing
    def group(expr: str) -> str:
        return f"({expr})" if opponent_cell and TEAM_ALIASES else expr

    c = lambda off: f"{col_letter(block_col + off)}{row}"  # noqa: E731
    return {
        c(1): f"={count}",
        c(2): f"={qs}",
        c(3): f"=IFERROR({c(2)}/{c(1)},0)",
        c(4): f"=IFERROR({group(er)}*9/{group(ip)},0)",
        c(5): f"=IFERROR({group(ip)}/{c(1)},0)",
        c(6): f"=IFERROR({group(runs5)}/{c(1)},0)",
        c(7): f"=IFERROR({group(runs)}/{c(1)},0)",
    }


def _total_row_formulas(block_col: int, row: int) -> dict[str, str]:
    """合 row, derived from the 客 and 主 rows above it (exact, and much cheaper)."""
    away, home = row - 2, row - 1
    c = lambda off, r=row: f"{col_letter(block_col + off)}{r}"  # noqa: E731
    ip_away = f"{c(5, away)}*{c(1, away)}"
    ip_home = f"{c(5, home)}*{c(1, home)}"
    return {
        c(1): f"={c(1, away)}+{c(1, home)}",
        c(2): f"={c(2, away)}+{c(2, home)}",
        c(3): f"=IFERROR({c(2)}/{c(1)},0)",
        c(4): f"=IFERROR(({c(4, away)}*{ip_away}+{c(4, home)}*{ip_home})"
              f"/({ip_away}+{ip_home}),0)",
        c(5): f"=IFERROR(({ip_away}+{ip_home})/{c(1)},0)",
        c(6): f"=IFERROR(({c(6, away)}*{c(1, away)}+{c(6, home)}*{c(1, home)})/{c(1)},0)",
        c(7): f"=IFERROR(({c(7, away)}*{c(1, away)}+{c(7, home)}*{c(1, home)})/{c(1)},0)",
    }


def overall_rows(block_col: int, window: tuple[str, str]) -> dict[str, str]:
    """Rows 5-7: the starter's whole record inside the date window."""
    cells = {}
    cells.update(_data_row_formulas(block_col, window, 5, side=0, opponent_cell=None))
    cells.update(_data_row_formulas(block_col, window, 6, side=1, opponent_cell=None))
    cells.update(_total_row_formulas(block_col, 7))
    return cells


def versus_rows(block_col: int, window: tuple[str, str], header_row: int) -> dict[str, str]:
    """The three rows under a VS. header, split by the opponent named in that header."""
    opponent = f"${col_letter(block_col + 1)}${header_row}"
    cells = {}
    cells.update(_data_row_formulas(block_col, window, header_row + 1, 0, opponent))
    cells.update(_data_row_formulas(block_col, window, header_row + 2, 1, opponent))
    cells.update(_total_row_formulas(block_col, header_row + 3))
    return cells


def opponent_team_formula(block_col: int) -> str:
    """Row 9 faces the other starter in the same game."""
    game = (block_col - 1) // 20
    side = 0 if (block_col - 1) % 20 == 0 else 1
    partner = matchup_block_col(game, 1 - side)
    return f"={col_letter(partner)}3"


def vs_team_formula(block_col: int, index: int) -> str:
    """Nth team of the league this block's team plays in."""
    team_cell = f"${col_letter(block_col)}$3"
    return (f'=IFERROR(INDEX(IF(COUNTIF({AL_RANGE},{team_cell})>0,'
            f'{AL_RANGE},{NL_RANGE}),{index}),"")')


def name_display_formula(key_cell: str) -> str:
    """Break a name after the first name, so every block shows two lines.

    REGEXREPLACE leaves a one-word name alone and a blank cell blank, and only the
    first space is replaced — "Simeon Woods Richardson" becomes "Simeon" /
    "Woods Richardson" rather than three lines.
    """
    return f'=REGEXREPLACE({key_cell},"^([^ ]+) ","$1"&CHAR(10))'


def matchup_header_formulas(matchup: int) -> dict[str, str]:
    """Row 3 shows team and starter; row 2 holds the name the aggregates match on."""
    block = matchup - 1
    cells = {}
    for game in range(MATCHUP_GAMES):
        for side in range(2):
            col = matchup_block_col(game, side)
            team = setting_team_cell(block, game, side)
            pitcher = setting_pitcher_cell(block, game, side)
            key = f"{col_letter(col + 5)}{MATCHUP_KEY_ROW}"
            cells[f"{col_letter(col)}{MATCHUP_NAME_ROW}"] = f"='{SETTING_TITLE}'!{team}"
            cells[key] = f"='{SETTING_TITLE}'!{pitcher}"
            cells[f"{col_letter(col + 5)}{MATCHUP_NAME_ROW}"] = name_display_formula(key)
    return cells


def setting_year_formulas(block: int) -> dict[str, str]:
    """Rows 39-41: the three season windows every preview column is measured over."""
    cells = {}
    for offset, back in enumerate((2, 1, 0)):
        col = col_letter(setting_block_col(block) + 3 + offset)
        year = f"YEAR(TODAY())-{back}" if back else "YEAR(TODAY())"
        cells[f"{col}{SETTING_YEAR_ROWS[0]}"] = f"=YEAR(TODAY())-{2000 + back}"
        cells[f"{col}{SETTING_YEAR_ROWS[1]}"] = f"=DATE({year},3,1)"
        cells[f"{col}{SETTING_YEAR_ROWS[2]}"] = f"=DATE({year},12,31)"
    return cells


def setting_helper_formulas(block: int) -> dict[str, str]:
    """Rows 43-71: earned runs × 9 and starter innings, per season, per starter."""
    cells = {}
    base_col = setting_block_col(block)
    for band in range(SETTING_BANDS):
        for side in range(2):
            vs_num, vs_den, all_num, all_den = setting_helper_rows(band, side)
            pitcher = _absolute(setting_pitcher_cell(block, band, side))
            opponent = _absolute(setting_team_cell(block, band, 1 - side))
            for offset in range(3):
                col = col_letter(base_col + 3 + offset)
                start = f"{col}{SETTING_YEAR_ROWS[1]}"
                end = f"{col}{SETTING_YEAR_ROWS[2]}"
                window = f'{DATE},">="&{start},{DATE},"<"&{end}'

                def versus(value_range, opp_range, sp_range, _w=window):
                    def build(criterion):
                        return (f"SUMIFS({value_range},{_w},{opp_range},{criterion},"
                                f"{sp_range},{pitcher})")
                    return _with_alias(build, opponent)

                cells[f"{col}{vs_num}"] = (
                    f"=({versus(AWAY_ER, HOME_TEAM, AWAY_SP)}"
                    f"+{versus(HOME_ER, AWAY_TEAM, HOME_SP)})*9"
                )
                cells[f"{col}{vs_den}"] = (
                    f"={versus(AWAY_IP, HOME_TEAM, AWAY_SP)}"
                    f"+{versus(HOME_IP, AWAY_TEAM, HOME_SP)}"
                )
                away_all = f"{window},{AWAY_SP},{pitcher}"
                home_all = f"{window},{HOME_SP},{pitcher}"
                cells[f"{col}{all_num}"] = (
                    f"=(SUMIFS({AWAY_ER},{away_all})"
                    f"+SUMIFS({HOME_ER},{home_all}))*9"
                )
                cells[f"{col}{all_den}"] = (
                    f"=SUMIFS({AWAY_IP},{away_all})"
                    f"+SUMIFS({HOME_IP},{home_all})"
                )
    return cells


def setting_display_formulas(block: int) -> dict[str, str]:
    """The VS./合 ERA previews shown next to each starter."""
    cells = {}
    base_col = setting_block_col(block)
    for band in range(SETTING_BANDS):
        header = setting_band_header_row(band)
        for side in range(2):
            vs_num, vs_den, all_num, all_den = setting_helper_rows(band, side)
            vs_row = header + 1 + side * 2
            all_row = vs_row + 1
            for offset in range(3):
                col = col_letter(base_col + 3 + offset)
                cells[f"{col}{vs_row}"] = (
                    f'=IF(AND({col}{vs_num}=0,{col}{vs_den}=0),"",{col}{vs_num}/{col}{vs_den})'
                )
                cells[f"{col}{all_row}"] = (
                    f'=IF(AND({col}{all_num}=0,{col}{all_den}=0),"",'
                    f'{col}{all_num}/{col}{all_den})'
                )
    return cells


def setting_venue_formula(block: int) -> str:
    """Row 3's ballpark, read off the home team below it.

    The cell is a label — no aggregate reads it — so deriving it from 主隊 saves a
    manual pick per matchup. It keeps its dropdown for neutral-site series.
    """
    home = setting_team_cell(block, 0, 1)
    return f'=IFERROR(VLOOKUP({home},資料!$A$2:$B$31,2,FALSE),"")'


def current_home_parks(pairs) -> dict[str, str]:
    """Each team's home ballpark, taken as the one it hosts in most often.

    Reading it off 紀錄 rather than trusting the stored list keeps renames current
    (Miller Park → American Family Field) without letting a neutral-site game — a
    London or Mexico City series — masquerade as a home park.
    """
    tally: dict[str, dict[str, int]] = {}
    for team, park in pairs:
        team, park = str(team).strip(), str(park).strip()
        if not team or not park:
            continue
        tally.setdefault(team, {})
        tally[team][park] = tally[team].get(park, 0) + 1
    parks = {team: max(counts.items(), key=lambda kv: kv[1])[0]
             for team, counts in tally.items()}
    parks.update({team: park for team, park in HOME_PARK_OVERRIDES.items()
                  if team in parks})
    return parks


def setting_mirror_formulas(block: int) -> dict[str, str]:
    """Games 2 and 3 inherit the ballpark and both teams from game 1.

    A series is the same two clubs three nights running, so NPB only asks for them
    once: the second band mirrors the first and the third mirrors the second, leaving
    just the two 先發 cells to fill in per game. The cells stay dropdowns, so a split
    doubleheader or a rain-shifted opponent can still be overridden by hand.
    """
    base = setting_block_col(block)
    venue = f"{col_letter(base)}{setting_band_header_row(0)}"
    cells = {}
    for band in range(1, SETTING_BANDS):
        head = setting_band_header_row(band)
        cells[f"{col_letter(base)}{head}"] = f"={venue}"
        for side in range(2):
            cells[setting_team_cell(block, band, side)] = (
                f"={setting_team_cell(block, band - 1, side)}"
            )
    return cells


def setting_helper_column_formulas(block: int, band: int, side: int) -> dict[str, str]:
    """Header cell mirrors the team, the cell below spills that team's pitcher list."""
    col = col_letter(setting_helper_column(block, band, side))
    team = _absolute(setting_team_cell(block, band, side))
    return {f"{col}1": f"={team}", f"{col}2": f"=INDIRECT({col}1)"}


def qs_rate_rules(block_col: int) -> tuple[dict, dict]:
    """場次 / QS / QS% turn green above a 65% quality-start rate, red below 41%.

    Matches NPB, which colours the same three columns (C:E per block) off the QS%
    cell. The ISNUMBER guard keeps blank cells — VS. header rows, unplayed matchups —
    out of it, since a blank would otherwise read as 0%.
    """
    qs_pct = col_letter(block_col + 3)
    span = (block_col + 1, block_col + 4)  # 場次 .. QS%
    return (
        {"columns": span,
         "formula": f"=AND(ISNUMBER(${qs_pct}5), ${qs_pct}5{QS_RATE_GREEN})",
         "colour": GREEN},
        {"columns": span,
         "formula": f"=AND(ISNUMBER(${qs_pct}5), ${qs_pct}5{QS_RATE_RED})",
         "colour": RED},
    )


def season_bound_formulas(first: int = FIRST_SEASON) -> tuple[str, str]:
    """One spilling formula per list, so a new season appears on its own each January.

    SEQUENCE runs from `first` to next year, so the dropdown always offers one season
    of headroom without anyone maintaining the list.
    """
    years = f"SEQUENCE(YEAR(TODAY())-{first - 2},1,{first})"
    return (f"=ARRAYFORMULA(DATE({years},3,1))",
            f"=ARRAYFORMULA(DATE({years},12,1))")


def date_validation_requests(sheet_id: int) -> list[dict]:
    """Turn every row-1 window cell into a dropdown, the way NPB's are."""
    requests = []
    for game in range(MATCHUP_GAMES):
        from_cell, to_cell = matchup_window_cells(game)
        for a1, source in ((from_cell, SEASON_START_RANGE),
                           (to_cell, SEASON_END_RANGE)):
            col = 0
            for ch in a1.replace("$", "")[:-1]:
                col = col * 26 + (ord(ch) - 64)
            requests.append(one_of_range_validation(sheet_id, 1, col - 1, source))
    return requests


def numeric_rule_formula(metric: str, test: str) -> str:
    """One rule spans all six blocks; the relative ref shifts per range."""
    col = col_letter(matchup_block_col(0, 0) + METRIC_OFFSET[metric])
    return f"=AND(ISNUMBER({col}5), {col}5{test})"


def _absolute(a1: str) -> str:
    letters = "".join(ch for ch in a1 if ch.isalpha())
    digits = "".join(ch for ch in a1 if ch.isdigit())
    return f"${letters}${digits}"


# ===================================================================================
# Sheets request builders
# ===================================================================================
def _rgb(hex_colour: str | None) -> dict | None:
    if hex_colour is None:
        return None
    r, g, b = (int(hex_colour[i:i + 2], 16) / 255 for i in (0, 2, 4))
    return {"red": r, "green": g, "blue": b}


def cell_format(*, bg=None, fg=None, size=11, bold=True, italic=False,
                font=FONT_DATA, pattern=None, halign="CENTER", wrap=False,
                valign="MIDDLE") -> dict:
    text_format = {"fontFamily": font, "fontSize": size, "bold": bold, "italic": italic}
    if fg:
        text_format["foregroundColor"] = _rgb(fg)
    fmt = {"textFormat": text_format, "horizontalAlignment": halign,
           "verticalAlignment": valign,
           "wrapStrategy": "WRAP" if wrap else "OVERFLOW_CELL"}
    if bg:
        fmt["backgroundColor"] = _rgb(bg)
    if pattern:
        if pattern == ";;;":
            kind = "TEXT"
        elif "y" in pattern:
            kind = "DATE"
        elif "%" in pattern:
            kind = "PERCENT"
        else:
            kind = "NUMBER"
        fmt["numberFormat"] = {"type": kind, "pattern": pattern}
    return fmt


FORMAT_FIELDS = ("userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,"
                 "verticalAlignment,numberFormat,wrapStrategy)")


def repeat_cell(sheet_id: int, row0: int, row1: int, col0: int, col1: int,
                fmt: dict) -> dict:
    return {"repeatCell": {
        "range": {"sheetId": sheet_id, "startRowIndex": row0, "endRowIndex": row1,
                  "startColumnIndex": col0, "endColumnIndex": col1},
        "cell": {"userEnteredFormat": fmt},
        "fields": FORMAT_FIELDS,
    }}


def border(style="SOLID"):
    return {"style": style, "width": 1, "color": _rgb(BLACK)}


def merge_rows(sheet_id: int, row0: int, row1: int, col: int) -> dict:
    """Merge one column across two rows — how NPB carries a 隊伍 / 先發 cell."""
    return {"mergeCells": {
        "range": {"sheetId": sheet_id, "startRowIndex": row0, "endRowIndex": row1,
                  "startColumnIndex": col, "endColumnIndex": col + 1},
        "mergeType": "MERGE_ALL",
    }}


def update_borders(sheet_id: int, row0: int, row1: int, col0: int, col1: int) -> dict:
    return {"updateBorders": {
        "range": {"sheetId": sheet_id, "startRowIndex": row0, "endRowIndex": row1,
                  "startColumnIndex": col0, "endColumnIndex": col1},
        "top": border(), "bottom": border(), "left": border(), "right": border(),
        "innerHorizontal": border(), "innerVertical": border(),
    }}


def merge(sheet_id: int, row: int, col0: int, col1: int) -> dict:
    return {"mergeCells": {
        "range": {"sheetId": sheet_id, "startRowIndex": row - 1, "endRowIndex": row,
                  "startColumnIndex": col0, "endColumnIndex": col1},
        "mergeType": "MERGE_ROWS",
    }}


def dimension(sheet_id: int, kind: str, start: int, end: int, pixels: int) -> dict:
    return {"updateDimensionProperties": {
        "range": {"sheetId": sheet_id, "dimension": kind,
                  "startIndex": start, "endIndex": end},
        "properties": {"pixelSize": pixels},
        "fields": "pixelSize",
    }}


def show_row(sheet_id: int, start: int, end: int, pixels: int) -> dict:
    return {"updateDimensionProperties": {
        "range": {"sheetId": sheet_id, "dimension": "ROWS",
                  "startIndex": start, "endIndex": end},
        "properties": {"hiddenByUser": False, "pixelSize": pixels},
        "fields": "hiddenByUser,pixelSize",
    }}


def hide_columns(sheet_id: int, start: int, end: int) -> dict:
    return {"updateDimensionProperties": {
        "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                  "startIndex": start, "endIndex": end},
        "properties": {"hiddenByUser": True},
        "fields": "hiddenByUser",
    }}


def one_of_range_validation(sheet_id: int, row: int, col: int, source: str) -> dict:
    return {"setDataValidation": {
        "range": {"sheetId": sheet_id, "startRowIndex": row - 1, "endRowIndex": row,
                  "startColumnIndex": col, "endColumnIndex": col + 1},
        "rule": {"condition": {"type": "ONE_OF_RANGE",
                               "values": [{"userEnteredValue": f"={source}"}]},
                 "showCustomUi": True, "strict": False},
    }}


def team_colour_rules(sheet_id: int, ranges: list[dict], index: int = 0) -> list[dict]:
    """One TEXT_EQ rule per team, painting whatever cell holds that team's code."""
    requests = []
    for team, bg in TEAM_PRIMARY_HEX.items():
        requests.append({"addConditionalFormatRule": {"index": index, "rule": {
            "ranges": ranges,
            "booleanRule": {
                "condition": {"type": "TEXT_EQ",
                              "values": [{"userEnteredValue": team}]},
                "format": {"backgroundColor": _rgb(bg),
                           "textFormat": {"foregroundColor":
                                          _rgb(contrast_text_hex(bg)), "bold": True}},
            },
        }}})
    return requests


def grid_range(sheet_id: int, row0: int, row1: int, col0: int, col1: int) -> dict:
    return {"sheetId": sheet_id, "startRowIndex": row0, "endRowIndex": row1,
            "startColumnIndex": col0, "endColumnIndex": col1}


# ===================================================================================
# 對戰 (n) sheet
# ===================================================================================
def matchup_values(matchup: int) -> list[list]:
    """The full A1:.. grid for one 對戰 tab, as values/formulas."""
    width = matchup_block_col(2, 1) + MATCHUP_BLOCK_WIDTH
    grid = [["" for _ in range(width)] for _ in range(MATCHUP_LAST_ROW)]

    def put(a1_cells: dict[str, str]):
        for a1, value in a1_cells.items():
            letters = "".join(ch for ch in a1 if ch.isalpha())
            row = int("".join(ch for ch in a1 if ch.isdigit()))
            col = 0
            for ch in letters:
                col = col * 26 + (ord(ch) - 64)
            grid[row - 1][col - 1] = value

    put(matchup_header_formulas(matchup))
    for game in range(MATCHUP_GAMES):
        window = matchup_window_cells(game)
        # row 1 date window: last three seasons by default, then pick from the dropdown
        from_cell, to_cell = (w.replace("$", "") for w in window)
        put({from_cell: WINDOW_FROM_DEFAULT, to_cell: WINDOW_TO_DEFAULT})
        for side in range(2):
            block = matchup_block_col(game, side)
            head = {col_letter(block) + "4": "總 先 発",
                    col_letter(block + 2) + "4": "QS",
                    col_letter(block + 3) + "4": "QS%",
                    col_letter(block + 4) + "4": "ERA",
                    col_letter(block + 5) + "4": "IP",
                    col_letter(block + 6) + "4": "'5",
                    col_letter(block + 7) + "4": "9 +"}
            put(head)
            put(overall_rows(block, window))
            for row, label in ((5, "客"), (6, "主"), (7, "合")):
                put({f"{col_letter(block)}{row}": label})
            for index in range(0, LEAGUE_SIZE + 1):
                header_row = vs_group_row(index)
                team_cell = f"{col_letter(block + 1)}{header_row}"
                put({f"{col_letter(block)}{header_row}": "VS.",
                     team_cell: (opponent_team_formula(block) if index == 0
                                 else vs_team_formula(block, index)),
                     f"{col_letter(block + 5)}{header_row}": "IP",
                     f"{col_letter(block + 6)}{header_row}": "'5",
                     f"{col_letter(block + 7)}{header_row}": "9 +"})
                put(versus_rows(block, window, header_row))
                for offset, label in ((1, "客"), (2, "主"), (3, "合")):
                    put({f"{col_letter(block)}{header_row + offset}": label})
    return grid


def matchup_wrap_requests(sheet_id: int) -> list[dict]:
    """Wrap the starter-name cells in row 3 and give the row a second line.

    The cell spans three 37px columns, so anything past ~14 characters — most MLB
    names — was being clipped by the block beside it.
    """
    pitcher = cell_format(bg=WHITE, size=MATCHUP_NAME_FONT_SIZE, wrap=True)
    team = cell_format(size=12, font=FONT_TEAM)
    # ";;;" is an empty format for every value type, so the key row renders nothing
    # while still holding the name the aggregates match on. White-on-white ghosted at
    # some zoom levels, and hiding the row cost the blank gap NPB leaves there.
    hidden = cell_format(fg=WHITE, size=8, bold=False, pattern=";;;")
    requests = [dimension(sheet_id, "ROWS", MATCHUP_NAME_ROW - 1, MATCHUP_NAME_ROW,
                          MATCHUP_NAME_ROW_HEIGHT),
                show_row(sheet_id, MATCHUP_KEY_ROW - 1, MATCHUP_KEY_ROW, 11)]
    for game in range(MATCHUP_GAMES):
        for side in range(2):
            block = matchup_block_col(game, side)
            requests.append(repeat_cell(sheet_id, 2, 3, block, block + 5, team))
            requests.append(repeat_cell(sheet_id, 2, 3, block + 5, block + 8, pitcher))
            requests.append(repeat_cell(sheet_id, 1, 2, block + 5, block + 8, hidden))
    return requests


def matchup_format_requests(sheet_id: int) -> list[dict]:
    requests: list[dict] = []
    width = matchup_block_col(2, 1) + MATCHUP_BLOCK_WIDTH

    # column widths / row heights
    requests.append(dimension(sheet_id, "COLUMNS", 0, 1, 13))
    for game in range(MATCHUP_GAMES):
        for side in range(2):
            block = matchup_block_col(game, side)
            requests.append(dimension(sheet_id, "COLUMNS", block, block + 3, 28))
            requests.append(dimension(sheet_id, "COLUMNS", block + 3, block + 8, 37))
            if side == 0:
                requests.append(dimension(sheet_id, "COLUMNS", block + 8, block + 9, 10))
            elif game < MATCHUP_GAMES - 1:
                requests.append(dimension(sheet_id, "COLUMNS", block + 8, block + 9, 16))
                requests.append(dimension(sheet_id, "COLUMNS", block + 9, block + 10, 4))
                requests.append(dimension(sheet_id, "COLUMNS", block + 10, block + 11, 16))
    requests.append(dimension(sheet_id, "ROWS", 0, MATCHUP_ROW_COUNT, 21))
    requests.append(dimension(sheet_id, "ROWS", 2, 3, MATCHUP_NAME_ROW_HEIGHT))
    for row in (2, 8):
        requests.append(dimension(sheet_id, "ROWS", row - 1, row, 11))
    requests.append(dimension(sheet_id, "ROWS", 12, 14, 12))
    for index in range(1, LEAGUE_SIZE + 1):
        separator = vs_group_row(index) + 4
        requests.append(dimension(sheet_id, "ROWS", separator - 1, separator, 11))

    label = cell_format(bg=WHITE, size=11)
    label_total = cell_format(bg=GREY, fg=WHITE, size=11)
    count = cell_format(bg=WHITE, fg=DARK_TEXT, size=11, italic=True,
                        pattern=FORMAT_COUNT)
    percent = cell_format(bg=WHITE, fg=DARK_TEXT, size=11, pattern=FORMAT_PERCENT)
    number = cell_format(bg=WHITE, fg=DARK_TEXT, size=11, pattern=FORMAT_NUMBER)
    ip = cell_format(bg=GREY, fg=WHITE, size=11, pattern=FORMAT_NUMBER, italic=True)
    header = cell_format(bg=GREY, fg=WHITE, size=10)
    vs_label = cell_format(bg=GREY, fg=WHITE, size=9, italic=True)
    team = cell_format(size=12, font=FONT_TEAM)
    pitcher = cell_format(bg=WHITE, size=MATCHUP_NAME_FONT_SIZE, wrap=True)
    date = cell_format(bg=DATE_BLUE, fg=WHITE, size=11, pattern="yyyy-mm-dd")
    blank = cell_format(bg=WHITE, fg=WHITE, size=11)

    for game in range(MATCHUP_GAMES):
        for side in range(2):
            block = matchup_block_col(game, side)
            column_styles = [label, count, count, percent, number, ip, number, number]
            for offset, style in enumerate(column_styles):
                requests.append(repeat_cell(sheet_id, 4, MATCHUP_LAST_ROW,
                                            block + offset, block + offset + 1, style))
            requests.append(repeat_cell(sheet_id, 2, 3, block, block + 5, team))
            requests.append(repeat_cell(sheet_id, 2, 3, block + 5, block + 8, pitcher))
            requests.append(repeat_cell(sheet_id, 3, 4, block, block + 8, header))
            requests.append(merge(sheet_id, 3, block, block + 5))
            requests.append(merge(sheet_id, 3, block + 5, block + 8))
            requests.append(merge(sheet_id, 4, block, block + 2))
            requests.append(update_borders(sheet_id, 2, 7, block, block + 8))
            for index in range(0, LEAGUE_SIZE + 1):
                head = vs_group_row(index)
                requests.append(repeat_cell(sheet_id, head - 1, head, block, block + 1,
                                            vs_label))
                requests.append(repeat_cell(sheet_id, head - 1, head, block + 1,
                                            block + 5, team))
                requests.append(repeat_cell(sheet_id, head - 1, head, block + 5,
                                            block + 8, header))
                requests.append(merge(sheet_id, head, block + 1, block + 5))
                requests.append(repeat_cell(sheet_id, head + 2, head + 3, block,
                                            block + 1, label_total))
                requests.append(update_borders(sheet_id, head - 1, head + 3, block,
                                              block + 8))
                requests.append(one_of_range_validation(sheet_id, head, block + 1,
                                                        TEAM_LIST))
            requests.append(repeat_cell(sheet_id, 6, 7, block, block + 1, label_total))
            requests.append(one_of_range_validation(sheet_id, 3, block, TEAM_LIST))
            # blank separator rows keep the block visually split
            for row in (8, 13, 14, *[vs_group_row(i) + 4 for i in range(1, LEAGUE_SIZE + 1)]):
                requests.append(repeat_cell(sheet_id, row - 1, row, block,
                                            block + MATCHUP_BLOCK_WIDTH + 1, blank))
            if side == 0:
                requests.append(repeat_cell(sheet_id, 0, MATCHUP_LAST_ROW, block + 8,
                                            block + 9, blank))
        first, second = matchup_block_col(game, 0), matchup_block_col(game, 1)
        requests.append(repeat_cell(sheet_id, 0, 1, first + 5, first + 6, date))
        requests.append(repeat_cell(sheet_id, 0, 1, second, second + 1, date))
        requests.append(merge(sheet_id, 1, first + 5, first + 9))
        requests.append(merge(sheet_id, 1, second, second + 4))

    requests.extend(date_validation_requests(sheet_id))
    requests.extend(conditional_format_requests(sheet_id))
    _ = width
    return requests


def conditional_format_requests(sheet_id: int) -> list[dict]:
    """The whole colour layer: value thresholds, QS rate, then team colours.

    Numeric rules are added first and team colours last, because
    addConditionalFormatRule inserts at the top — so the team rules end up highest
    priority and a blank VS. header row cannot be painted by a threshold rule.
    """
    requests: list[dict] = []
    blocks = [matchup_block_col(g, s) for g in range(MATCHUP_GAMES) for s in range(2)]

    def rule(ranges, formula, colour):
        return {"addConditionalFormatRule": {"index": 0, "rule": {
            "ranges": ranges,
            "booleanRule": {
                "condition": {"type": "CUSTOM_FORMULA",
                              "values": [{"userEnteredValue": formula}]},
                "format": {"textFormat": {"foregroundColor": _rgb(colour),
                                          "bold": True}},
            },
        }}}

    for metric, test, colour in NUMERIC_RULES:
        ranges = [grid_range(sheet_id, 4, MATCHUP_LAST_ROW,
                             block + METRIC_OFFSET[metric],
                             block + METRIC_OFFSET[metric] + 1)
                  for block in blocks]
        requests.append(rule(ranges, numeric_rule_formula(metric, test), colour))

    for block in blocks:
        for spec in qs_rate_rules(block):
            first, last = spec["columns"]
            requests.append(rule(
                [grid_range(sheet_id, 4, MATCHUP_LAST_ROW, first, last)],
                spec["formula"], spec["colour"]))

    colour_ranges = []
    for block in blocks:
        colour_ranges.append(grid_range(sheet_id, 2, 3, block, block + 1))
        colour_ranges.append(grid_range(sheet_id, 8, MATCHUP_LAST_ROW,
                                        block + 1, block + 2))
    requests.extend(team_colour_rules(sheet_id, colour_ranges))
    return requests


# ===================================================================================
# 設定 sheet
# ===================================================================================
def setting_values() -> list[list]:
    width = SETTING_HELPER_COL_START + MATCHUP_COUNT * SETTING_BANDS * 2
    grid = [["" for _ in range(width)] for _ in range(SETTING_ROW_COUNT)]

    def put(cells: dict[str, str]):
        for a1, value in cells.items():
            letters = "".join(ch for ch in a1 if ch.isalpha())
            row = int("".join(ch for ch in a1 if ch.isdigit()))
            col = 0
            for ch in letters:
                col = col * 26 + (ord(ch) - 64)
            grid[row - 1][col - 1] = value

    put({"B1": "數據更新日:", "C1": f"=MAX({DATE})"})
    put({f"D{SETTING_NOTE_ROWS[0]}":
         "設定對戰資料便可更新各種數據，可以一次設定所有資料，"
         "及在此預覽先發投手對戰數據，缺點是每一次更改都要回此分頁設定。",
         f"D{SETTING_NOTE_ROWS[1]}":
         "注意 ~ 此頁面 VS.列 為各隊投手以先發身分登板時對戰敵隊之各年度ERA數據，"
         "若是空白則表示沒有先發對戰過， 合列 則為該投手於各年度先發出賽時的ERA數據總和。"})

    for block in range(MATCHUP_COUNT):
        base = setting_block_col(block)
        # NPB labels each block with its ballpark; with 15 blocks side by side the tab
        # each one feeds needs saying too, so it goes in the spare row above.
        put({f"{col_letter(base)}2": matchup_title(block + 1)})
        for band in range(SETTING_BANDS):
            head = setting_band_header_row(band)
            put({f"{col_letter(base + 1)}{head}": "先   発",
                 f"{col_letter(base + 2)}{head}": "ERA"})
            for offset, back in enumerate((2, 1, 0)):
                col = col_letter(base + 3 + offset)
                put({f"{col}{head}": f"=YEAR(TODAY())-{2000 + back}"})
            for side in range(2):
                row = head + 1 + side * 2
                put({f"{col_letter(base + 2)}{row}": "VS.",
                     f"{col_letter(base + 2)}{row + 1}": "合"})
        put({f"{col_letter(base)}{setting_band_header_row(0)}":
             setting_venue_formula(block)})
        put(setting_mirror_formulas(block))
        put(setting_display_formulas(block))
        put(setting_year_formulas(block))
        put(setting_helper_formulas(block))
        for band in range(SETTING_BANDS):
            put({f"{col_letter(base + 2)}{setting_helper_rows(band, 0)[0]}": str(band + 1)})
            for side in range(2):
                put(setting_helper_column_formulas(block, band, side))
    return grid


def rich_text_cell(segments: list[tuple[str, str | None]], *, size: int = 14) -> dict:
    """A CellData whose numbers are coloured individually, as NPB's legend is."""
    text = "".join(part for part, _ in segments)
    runs, index = [], 0
    for part, colour in segments:
        fmt = {"foregroundColor": _rgb(colour)} if colour else {"foregroundColor":
                                                                _rgb(BLACK)}
        runs.append({"startIndex": index, "format": fmt})
        index += len(part)
    return {
        "userEnteredValue": {"stringValue": text},
        "userEnteredFormat": {
            "textFormat": {"fontFamily": FONT_TEAM, "fontSize": size, "bold": True},
            "verticalAlignment": "MIDDLE",
            "backgroundColor": _rgb(WHITE),
        },
        "textFormatRuns": runs,
    }


def setting_legend_requests(sheet_id: int) -> list[dict]:
    """The colour key, ported from NPB 設定!P27:V28."""
    title_row, body_row = LEGEND_ROWS
    red_col = setting_block_col(0)        # B
    green_col = setting_block_col(1)      # I, one block over — as NPB spaces them
    end_col = setting_block_col(2)        # P, the right edge of the box

    def cell_request(row: int, col: int, cell: dict) -> dict:
        return {"updateCells": {
            "start": {"sheetId": sheet_id, "rowIndex": row - 1, "columnIndex": col},
            "rows": [{"values": [cell]}],
            "fields": "userEnteredValue,userEnteredFormat,textFormatRuns",
        }}

    title = {
        "userEnteredValue": {"stringValue": LEGEND_TITLE},
        "userEnteredFormat": {
            "textFormat": {"fontFamily": FONT_TEAM, "fontSize": 14, "bold": True},
            "horizontalAlignment": "LEFT", "verticalAlignment": "BOTTOM",
        },
    }
    return [
        dimension(sheet_id, "ROWS", title_row - 1, title_row, 30),
        dimension(sheet_id, "ROWS", body_row - 1, body_row, 126),
        cell_request(title_row, red_col, title),
        cell_request(body_row, red_col, rich_text_cell(LEGEND_RED)),
        cell_request(body_row, green_col, rich_text_cell(LEGEND_GREEN)),
        update_borders(sheet_id, title_row - 1, body_row, red_col, end_col),
    ]


def setting_wrap_requests(sheet_id: int) -> list[dict]:
    """Wrap the 先発 cells: MLB names run long ("Simeon Woods Richardson") and the
    cell is merged over two rows, so a second line costs nothing."""
    pitcher = cell_format(size=10, wrap=True)
    requests = []
    for block in range(MATCHUP_COUNT):
        col = setting_block_col(block) + 1
        for band in range(SETTING_BANDS):
            head = setting_band_header_row(band)
            for side in range(2):
                row = head + 1 + side * 2
                requests.append(repeat_cell(sheet_id, row - 1, row + 1,
                                            col, col + 1, pitcher))
    return requests


def setting_frame_requests(sheet_id: int) -> list[dict]:
    """Borders and the two-row merges that box each band in, as on NPB.

    每個 band 是 5 列：標題、VS.、合、VS.、合。隊伍與先發各佔 VS.+合 兩列（合併），
    所以那兩格底下的空白列不畫框。
    """
    requests: list[dict] = []
    for block in range(MATCHUP_COUNT):
        base = setting_block_col(block)
        for band in range(SETTING_BANDS):
            head = setting_band_header_row(band)
            # merge before bordering: a merged pair carries its bottom edge on the
            # lower cell, so drawing the box first and clearing that cell afterwards
            # takes the block's bottom line with it
            for side in range(2):
                vs_row = head + 1 + side * 2
                for col in (base, base + 1):
                    requests.append(merge_rows(sheet_id, vs_row - 1, vs_row + 1, col))
            requests.append(update_borders(sheet_id, head - 1, head + 4, base, base + 6))
    return requests


def setting_format_requests(sheet_id: int) -> list[dict]:
    requests: list[dict] = []
    requests.append(dimension(sheet_id, "COLUMNS", 0, 1, 7))
    for block in range(MATCHUP_COUNT):
        base = setting_block_col(block)
        requests.append(dimension(sheet_id, "COLUMNS", base, base + 2, 86))
        requests.append(dimension(sheet_id, "COLUMNS", base + 2, base + 3, 23))
        requests.append(dimension(sheet_id, "COLUMNS", base + 3, base + 6, 32))
        requests.append(dimension(sheet_id, "COLUMNS", base + 6, base + 7, 13))
    helper_end = SETTING_HELPER_COL_START + MATCHUP_COUNT * SETTING_BANDS * 2
    requests.append(hide_columns(sheet_id, SETTING_HELPER_COL_START, helper_end))

    requests.append(dimension(sheet_id, "ROWS", 0, 1, 28))
    requests.append(dimension(sheet_id, "ROWS", 1, 2, 27))
    for band in range(SETTING_BANDS):
        head = setting_band_header_row(band)
        requests.append(dimension(sheet_id, "ROWS", head - 1, head, 26))
        requests.append(dimension(sheet_id, "ROWS", head, head + 4, 24))

    venue = cell_format(bg=WHITE, size=9)
    sub_header = cell_format(bg=GREY, fg=WHITE, size=10)
    era_header = cell_format(bg=GREY, fg=WHITE, size=8)
    team = cell_format(size=10, font=FONT_TEAM)
    pitcher = cell_format(size=10)
    vs_label = cell_format(size=10)
    total_label = cell_format(bg=GREY, fg=WHITE, size=11)
    ratio = cell_format(size=10, pattern="0.00")
    note = cell_format(size=10, bold=False, halign="LEFT")

    tab_label = cell_format(size=11, font=FONT_TEAM, halign="LEFT")
    for block in range(MATCHUP_COUNT):
        base = setting_block_col(block)
        requests.append(repeat_cell(sheet_id, 1, 2, base, base + 3, tab_label))
        requests.append(merge(sheet_id, 2, base, base + 3))
        for band in range(SETTING_BANDS):
            head = setting_band_header_row(band)
            requests.append(repeat_cell(sheet_id, head - 1, head, base, base + 1, venue))
            requests.append(repeat_cell(sheet_id, head - 1, head, base + 1, base + 2,
                                        sub_header))
            requests.append(repeat_cell(sheet_id, head - 1, head, base + 2, base + 3,
                                        era_header))
            requests.append(repeat_cell(sheet_id, head - 1, head, base + 3, base + 6,
                                        sub_header))
            requests.append(one_of_range_validation(sheet_id, head, base, VENUE_LIST))
            for side in range(2):
                row = head + 1 + side * 2
                requests.append(repeat_cell(sheet_id, row - 1, row, base, base + 1, team))
                requests.append(repeat_cell(sheet_id, row - 1, row, base + 1, base + 2,
                                            pitcher))
                requests.append(repeat_cell(sheet_id, row - 1, row, base + 2, base + 3,
                                            vs_label))
                requests.append(repeat_cell(sheet_id, row, row + 1, base + 2, base + 3,
                                            total_label))
                requests.append(repeat_cell(sheet_id, row - 1, row + 1, base + 3,
                                            base + 6, ratio))
                requests.append(one_of_range_validation(sheet_id, row, base, TEAM_LIST))
                helper_col = setting_helper_column(block, band, side)
                source = (f"'{SETTING_TITLE}'!${col_letter(helper_col)}$2:"
                          f"${col_letter(helper_col)}$30")
                requests.append(one_of_range_validation(sheet_id, row, base + 1, source))

    requests.extend(setting_frame_requests(sheet_id))
    requests.extend(setting_wrap_requests(sheet_id))
    requests.extend(setting_legend_requests(sheet_id))

    for row in SETTING_NOTE_ROWS:
        requests.append(repeat_cell(sheet_id, row - 1, row, 3, 4, note))
    # 數據更新日 label + the =MAX(日期) it annotates
    requests.append(repeat_cell(sheet_id, 0, 1, 1, 2,
                                cell_format(size=11, halign="RIGHT")))
    requests.append(repeat_cell(sheet_id, 0, 1, 2, 3,
                                cell_format(size=11, pattern="yyyy-mm-dd")))

    colour_ranges = [grid_range(sheet_id, 2, 21, setting_block_col(b),
                                setting_block_col(b) + 1)
                     for b in range(MATCHUP_COUNT)]
    requests.extend(team_colour_rules(sheet_id, colour_ranges))
    return requests


# ===================================================================================
# main
# ===================================================================================
def _chunked(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def restyle_requests(sheet_id: int, existing_rule_count: int) -> list[dict]:
    """Replace a 對戰 tab's colour layer and number formats, leaving formulas alone.

    Used to roll a change out to the 15 live tabs without rebuilding them (which would
    take minutes and throw away anything hand-edited on the sheet).
    """
    requests = [{"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": 0}}
                for _ in range(existing_rule_count)]

    styles = {1: FORMAT_COUNT, 2: FORMAT_COUNT, 3: FORMAT_PERCENT,
              4: FORMAT_NUMBER, 5: FORMAT_NUMBER, 6: FORMAT_NUMBER, 7: FORMAT_NUMBER}
    for game in range(MATCHUP_GAMES):
        for side in range(2):
            block = matchup_block_col(game, side)
            for offset, pattern in styles.items():
                kind = "PERCENT" if "%" in pattern else "NUMBER"
                requests.append({"repeatCell": {
                    "range": grid_range(sheet_id, 4, MATCHUP_LAST_ROW,
                                        block + offset, block + offset + 1),
                    "cell": {"userEnteredFormat": {
                        "numberFormat": {"type": kind, "pattern": pattern}}},
                    "fields": "userEnteredFormat.numberFormat",
                }})
    requests.extend(matchup_wrap_requests(sheet_id))
    requests.extend(date_validation_requests(sheet_id))
    requests.extend(conditional_format_requests(sheet_id))
    return requests


def matchup_formula_ranges(matchup: int) -> list[dict]:
    """Just the aggregate cells of a 對戰 tab, as values_batch_update data.

    Deliberately excludes row 1 (date window), row 3 (team/starter) and every VS.
    header cell, so a rewrite cannot undo a window or an opponent picked by hand.
    """
    data = []
    for game in range(MATCHUP_GAMES):
        window = matchup_window_cells(game)
        for side in range(2):
            block = matchup_block_col(game, side)
            cells = dict(overall_rows(block, window))
            for index in range(0, LEAGUE_SIZE + 1):
                cells.update(versus_rows(block, window, vs_group_row(index)))
            first = col_letter(block + 1)
            last = col_letter(block + 7)
            for row in {int("".join(c for c in a1 if c.isdigit())) for a1 in cells}:
                data.append({
                    "range": f"'{matchup_title(matchup)}'!{first}{row}:{last}{row}",
                    "values": [[cells[f"{col_letter(block + off)}{row}"]
                                for off in range(1, 8)]],
                })
    return data


def setting_formula_ranges() -> list[dict]:
    """Just 設定's hidden helper rows — never the 隊伍 / 先發 the user picked."""
    data = []
    for block in range(MATCHUP_COUNT):
        cells = setting_helper_formulas(block)
        base = setting_block_col(block)
        rows = {int("".join(c for c in a1 if c.isdigit())) for a1 in cells}
        for row in sorted(rows):
            first, last = col_letter(base + 3), col_letter(base + 5)
            data.append({
                "range": f"'{SETTING_TITLE}'!{first}{row}:{last}{row}",
                "values": [[cells[f"{col_letter(base + 3 + off)}{row}"]
                            for off in range(3)]],
            })
    return data


def refresh_home_parks(ss, *, dry_run: bool = False) -> list[tuple[str, str, str]]:
    """Re-derive 資料!B (each team's home park) from what 紀錄 actually shows."""
    record = ss.worksheet(RECORD_TITLE)
    rows = record.get(f"A2:A{record.row_count}", value_render_option="FORMATTED_VALUE")
    scan_from = max(2, len(rows) - HOME_PARK_SCAN_ROWS)
    home, venue = record.batch_get(
        [f"R{scan_from}:R{len(rows) + 1}", f"AG{scan_from}:AG{len(rows) + 1}"],
        value_render_option="FORMATTED_VALUE")
    pairs = [(h[0] if h else "", v[0] if v else "") for h, v in zip(home, venue)]
    parks = current_home_parks(pairs)

    data = ss.worksheet(SEASON_LIST_SHEET)
    first, last = HOME_PARK_ROWS
    teams = data.get(f"A{first}:A{last}", value_render_option="FORMATTED_VALUE")
    stored = data.get(f"B{first}:B{last}", value_render_option="FORMATTED_VALUE")
    changes, updates = [], []
    for offset, team_row in enumerate(teams):
        team = team_row[0].strip() if team_row else ""
        was = stored[offset][0] if offset < len(stored) and stored[offset] else ""
        now = parks.get(team)
        if not team or not now or now == was:
            continue
        changes.append((team, was, now))
        updates.append({"range": f"'{SEASON_LIST_SHEET}'!B{first + offset}",
                        "values": [[now]]})
    for team, was, now in changes:
        print(f"  {team}: {was!r} -> {now!r}")
    if updates and not dry_run:
        ss.values_batch_update({"valueInputOption": "RAW", "data": updates})
    print(f"home parks: {len(changes)} renamed"
          f"{' (dry run)' if dry_run and changes else ''}")
    return changes


def ensure_season_lists(ss) -> None:
    """Put the self-extending season lists on 資料 and point the named ranges at them.

    The named ranges span more rows than the formulas currently fill; Sheets skips the
    blank tail when it builds the dropdown, so the list grows on its own each year.
    """
    start_formula, end_formula = season_bound_formulas()
    ws = ss.worksheet(SEASON_LIST_SHEET)
    start_col = col_letter(SEASON_START_COL)
    end_col = col_letter(SEASON_END_COL)
    last_row = 1 + SEASON_LIST_ROWS

    ws.batch_clear([f"{start_col}2:{end_col}{last_row}"])
    ws.update([[SEASON_START_RANGE, SEASON_END_RANGE]], f"{start_col}1",
              value_input_option="USER_ENTERED")
    ws.update([[start_formula]], f"{start_col}2", value_input_option="USER_ENTERED")
    ws.update([[end_formula]], f"{end_col}2", value_input_option="USER_ENTERED")

    existing = {nr["name"]: nr for nr in ss.list_named_ranges()}
    requests = []
    for name, col in ((SEASON_START_RANGE, SEASON_START_COL),
                      (SEASON_END_RANGE, SEASON_END_COL)):
        span = {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": last_row,
                "startColumnIndex": col, "endColumnIndex": col + 1}
        if name in existing:
            requests.append({"updateNamedRange": {
                "namedRange": {"namedRangeId": existing[name]["namedRangeId"],
                               "name": name, "range": span},
                "fields": "range",
            }})
        else:
            requests.append({"addNamedRange": {
                "namedRange": {"name": name, "range": span}}})
    ss.batch_update({"requests": requests})
    print(f"season dropdowns: {SEASON_LIST_SHEET}!{start_col}2:{end_col}{last_row}, "
          f"auto-extending from {FIRST_SEASON} to next year")


def restyle(dry_run: bool = False) -> None:
    """Push the current colour/number-format spec onto the live 對戰 tabs."""
    from baseball.sheets import GoogleSheetsClient

    client = GoogleSheetsClient()
    ss = client.spreadsheet(MLB_SPREADSHEET_KEY)
    if not dry_run:
        ensure_season_lists(ss)
        refresh_home_parks(ss)

    # 設定: frame each band and let games 2/3 follow game 1. Only the ballpark and
    # 隊伍 cells are rewritten — every 先發 already picked stays put.
    setting_ws = ss.worksheet(SETTING_TITLE)
    mirrors = [(f"{col_letter(setting_block_col(block))}"
                f"{setting_band_header_row(0)}", setting_venue_formula(block))
               for block in range(MATCHUP_COUNT)]
    mirrors += [(a1, formula) for block in range(MATCHUP_COUNT)
                for a1, formula in setting_mirror_formulas(block).items()]
    frame = (setting_frame_requests(setting_ws.id)
             + setting_wrap_requests(setting_ws.id)
             + setting_legend_requests(setting_ws.id))
    print(f"{SETTING_TITLE}: {len(mirrors)} mirrored cell(s), {len(frame)} "
          f"border/merge/legend request(s), colour legend at row {LEGEND_ROWS[0]}")
    if not dry_run:
        ss.values_batch_update({
            "valueInputOption": "USER_ENTERED",
            "data": [{"range": f"'{SETTING_TITLE}'!{a1}", "values": [[formula]]}
                     for a1, formula in mirrors],
        })
        for chunk in _chunked(frame, 150):
            ss.batch_update({"requests": chunk})
    meta = client.client.http_client.spreadsheets_get(
        MLB_SPREADSHEET_KEY,
        params={"fields": "sheets(properties(sheetId,title),conditionalFormats)"})
    for sheet in meta["sheets"]:
        title = sheet["properties"]["title"]
        if title not in MATCHUP_TITLES:
            continue
        rules = len(sheet.get("conditionalFormats", []))
        requests = restyle_requests(sheet["properties"]["sheetId"], rules)
        print(f"{title}: {rules} old rule(s) -> "
              f"{len(conditional_format_requests(0))} new, 48 number-format ranges, "
              f"{len(date_validation_requests(0))} date dropdowns")
        if not dry_run:
            for chunk in _chunked(requests, 150):
                ss.batch_update({"requests": chunk})
            # only refresh a window still holding the original default
            ws = ss.worksheet(title)
            for game in range(MATCHUP_GAMES):
                to_cell = matchup_window_cells(game)[1].replace("$", "")
                current = ws.get(to_cell, value_render_option="FORMULA")
                if current and current[0] and str(current[0][0]) == "=DATE(YEAR(TODAY())+1,1,1)":
                    ws.update([[WINDOW_TO_DEFAULT]], to_cell,
                              value_input_option="USER_ENTERED")


def reformulate(dry_run: bool = False) -> None:
    """Rewrite the aggregate formulas in place, keeping every hand-set cell."""
    from baseball.sheets import GoogleSheetsClient

    client = GoogleSheetsClient()
    ss = client.spreadsheet(MLB_SPREADSHEET_KEY)
    jobs = [(SETTING_TITLE, setting_formula_ranges())]
    jobs += [(matchup_title(n), matchup_formula_ranges(n))
             for n in range(1, MATCHUP_COUNT + 1)]
    for title, data in jobs:
        cells = sum(len(d["values"][0]) for d in data)
        print(f"{title}: {len(data)} range(s), {cells} formula cell(s)")
        if dry_run:
            continue
        for chunk in _chunked(data, 120):
            ss.values_batch_update({"valueInputOption": "USER_ENTERED", "data": chunk})


def main(dry_run: bool = False) -> None:
    from baseball.sheets import GoogleSheetsClient

    client = GoogleSheetsClient()
    ss = client.spreadsheet(MLB_SPREADSHEET_KEY)
    existing = {ws.title: ws for ws in ss.worksheets()}

    doomed = [t for t in DOOMED_TITLES if t in existing]
    print(f"delete {len(doomed)} old tab(s): {', '.join(doomed) or '(none)'}")
    print(f"create {SETTING_TITLE} + {MATCHUP_COUNT} 對戰 tabs "
          f"({MATCHUP_LAST_ROW} rows × {matchup_block_col(2, 1) + MATCHUP_BLOCK_WIDTH} cols each)")

    if dry_run:
        grid = matchup_values(1)
        print("\nsample 對戰 (1) cells:")
        for a1 in ("B3", "G3", "C5", "F5", "C7", "C9", "C16", "F16", "C15", "C85"):
            letters = "".join(ch for ch in a1 if ch.isalpha())
            row = int("".join(ch for ch in a1 if ch.isdigit()))
            col = 0
            for ch in letters:
                col = col * 26 + (ord(ch) - 64)
            print(f"  {a1}: {grid[row - 1][col - 1]}")
        setting = setting_values()
        print("\nsample 設定 cells:")
        for a1 in ("C1", "E4", "E39", "E43", "E44", "E45"):
            letters = "".join(ch for ch in a1 if ch.isalpha())
            row = int("".join(ch for ch in a1 if ch.isdigit()))
            col = 0
            for ch in letters:
                col = col * 26 + (ord(ch) - 64)
            print(f"  {a1}: {setting[row - 1][col - 1]}")
        print(f"\nformat requests: 對戰 {len(matchup_format_requests(0))}, "
              f"設定 {len(setting_format_requests(0))}")
        return

    if doomed:
        ss.batch_update({"requests": [
            {"deleteSheet": {"sheetId": existing[t].id}} for t in doomed
        ]})

    # --- 設定 -----------------------------------------------------------------------
    setting_cols = SETTING_HELPER_COL_START + MATCHUP_COUNT * SETTING_BANDS * 2
    response = ss.batch_update({"requests": [{"addSheet": {"properties": {
        "title": SETTING_TITLE,
        "index": 0,
        "gridProperties": {"rowCount": SETTING_ROW_COUNT,
                           "columnCount": setting_cols,
                           "hideGridlines": True},
    }}}]})
    setting_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
    setting_ws = ss.worksheet(SETTING_TITLE)
    setting_ws.update(setting_values(), "A1", value_input_option="USER_ENTERED")
    for chunk in _chunked(setting_format_requests(setting_id), 150):
        ss.batch_update({"requests": chunk})
    print(f"wrote {SETTING_TITLE}")

    # --- 對戰 (1) ---------------------------------------------------------------------
    matchup_cols = matchup_block_col(2, 1) + MATCHUP_BLOCK_WIDTH + 2
    response = ss.batch_update({"requests": [{"addSheet": {"properties": {
        "title": MATCHUP_TITLES[0],
        "index": 1,
        "gridProperties": {"rowCount": MATCHUP_ROW_COUNT,
                           "columnCount": matchup_cols,
                           "frozenRowCount": MATCHUP_FROZEN_ROWS,
                           "hideGridlines": True},
    }}}]})
    first_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
    first_ws = ss.worksheet(MATCHUP_TITLES[0])
    first_ws.update(matchup_values(1), "A1", value_input_option="USER_ENTERED")
    for chunk in _chunked(matchup_format_requests(first_id), 150):
        ss.batch_update({"requests": chunk})
    print(f"wrote {MATCHUP_TITLES[0]}")

    # --- 對戰 (2)…(15): duplicate, then repoint row 3 at the right 設定 block ----------
    for n in range(2, MATCHUP_COUNT + 1):
        ss.batch_update({"requests": [{"duplicateSheet": {
            "sourceSheetId": first_id,
            "insertSheetIndex": n,
            "newSheetName": MATCHUP_TITLES[n - 1],
        }}]})
        header = matchup_header_formulas(n)
        ss.values_batch_update({
            "valueInputOption": "USER_ENTERED",
            "data": [{"range": f"'{MATCHUP_TITLES[n - 1]}'!{a1}", "values": [[formula]]}
                     for a1, formula in header.items()],
        })
        print(f"wrote {MATCHUP_TITLES[n - 1]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="print the plan and a formula sample, write nothing")
    parser.add_argument("--restyle", action="store_true",
                        help="only refresh colours, number formats and date dropdowns "
                             "on the existing 對戰 tabs; leaves formulas alone")
    parser.add_argument("--reformulate", action="store_true",
                        help="only rewrite the aggregate formulas in place; leaves the "
                             "設定 picks, date windows and dropdown overrides alone")
    args = parser.parse_args()
    if args.restyle:
        restyle(dry_run=args.dry_run)
    elif args.reformulate:
        reformulate(dry_run=args.dry_run)
    else:
        main(dry_run=args.dry_run)
