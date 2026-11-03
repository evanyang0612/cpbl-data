"""Unit tests for the 2026・野球日記 tab writer (baseball/npb_diary.py)."""

import datetime

from baseball import npb_diary as nd


def _game(date, away, home, status="試合前"):
    return {"date": date, "away": away, "home": home, "status": status,
            "away_score": "", "home_score": "", "venue": ""}


# NPB plays 3連戦: the same pair, at the same ballpark, three days running.
SERIES = [_game(f"2026-09-{day}", "中日", "阪神") for day in (13, 14, 15)]


def test_announced_starters_reach_only_the_day_they_were_announced_for():
    """予告先発 is one day's news, but a 3連戦 repeats the pairing for three.

    Keyed on the pairing alone, 9/13's announcement was written onto 9/14 and
    9/15 as well — three identical rows for games whose starters nobody had
    named yet.
    """
    announced = {("2026-09-13", "中日", "阪神"): ("髙橋宏", "才木")}
    texts = [nd.game_text(g, {}, None, announced)[0] for g in SERIES]
    assert "髙橋宏" in texts[0] and "才木" in texts[0]
    # The rest of the series is not announced yet: the cell falls back to the
    # opponent's code, the way an unannounced scheduled game is written.
    assert texts[1] == nd.CODE_OF["中日"]
    assert texts[2] == nd.CODE_OF["中日"]


def test_announced_starters_are_kept_off_a_game_already_played():
    """A played game takes its starters from 賽錄, never from the announcement."""
    played = dict(SERIES[0], status="試合終了", away_score="3", home_score="1")
    announced = {("2026-09-13", "中日", "阪神"): ("髙橋宏", "才木")}
    text, kind, _ = nd.game_text(played, {}, None, announced)
    assert kind == "played"


ANNOUNCEMENT_HTML = """
<html><body>
<h4>9月13日の予告先発投手</h4>
<div class="unit">
  <div class="team_left"><img alt="阪神タイガース"><span>才木　浩人</span></div>
  <div class="team_right"><img alt="中日ドラゴンズ"><span>髙橋　宏斗</span></div>
</div>
<div class="unit"><div class="nav">no teams here</div></div>
</body></html>
"""


def test_fetch_announced_starters_reads_the_date_off_the_page(monkeypatch):
    """The page says which day it is announcing; that is the only safe key."""
    class _Resp:
        status_code = 200
        text = ANNOUNCEMENT_HTML
        encoding = "utf-8"

    monkeypatch.setattr(nd.requests, "get", lambda *a, **kw: _Resp())
    out = nd.fetch_announced_starters(year=2026)
    # Ideographic spaces inside a name are normalised to one half-width space.
    assert out == {("2026-09-13", "中日", "阪神"): ("髙橋 宏斗", "才木 浩人")}


def test_fetch_announced_starters_is_empty_when_the_page_is_down(monkeypatch):
    class _Resp:
        status_code = 503
        text = ""
        encoding = "utf-8"

    monkeypatch.setattr(nd.requests, "get", lambda *a, **kw: _Resp())
    assert nd.fetch_announced_starters(year=2026) == {}


# --- 予告先発 cell padding ------------------------------------------------

def _announced_text(away_p, home_p):
    announced = {("2026-09-13", "中日", "阪神"): (away_p, home_p)}
    text, kind, _ = nd.game_text(SERIES[0], {}, None, announced)
    assert kind == "announced"
    return text


def test_announced_names_sit_ten_spaces_apart():
    """The 2023 sheet's own gap: 野村          松葉, 伊藤          大関.

    A played cell is 投手 5格 比分 5格 投手; an announced one has no score
    between them, so the gap has to carry that width itself. Starting from the
    played cell's single-sided 5 left the two names half a score too close.
    """
    assert _announced_text("髙橋宏", "才木") == "髙橋宏" + " " * 10 + "才木"
    assert _announced_text("野村", "松葉") == "野村" + " " * 10 + "松葉"


def test_announced_padding_is_squeezed_when_the_names_are_long():
    """Exactly how the 2023 sheet writes them: 石田        メンデス at 8."""
    text = _announced_text("デュプランティエ", "ビーディ")
    gap = len(text) - len("デュプランティエ") - len("ビーディ")
    assert 1 <= gap < 10
    assert text.startswith("デュプランティエ") and text.endswith("ビーディ")


def test_announced_cell_stays_inside_the_column():
    for away_p, home_p in (("髙橋宏", "才木"), ("デュプランティエ", "ビーディ"),
                           ("エンス", "M"), ("石田", "メンデス")):
        text = _announced_text(away_p, home_p)
        gap = len(text) - len(away_p) - len(home_p)
        width = (nd.text_px(away_p, nd.name_size(away_p))
                 + nd.text_px(home_p, nd.name_size(home_p))
                 + nd.text_px(" " * gap, 10))
        assert width <= nd.FIT_TARGET_PX or gap == 1, (away_p, home_p, width)


def test_the_diary_runs_through_the_last_makeup_game():
    # Rainouts were made up 10/5–10/8, after the last date on the original
    # schedule; ヤクルト@阪神 on 10/8 closed the regular season.
    schedule = {"2026-10-08": [_game("2026-10-08", "ヤクルト", "阪神")]}
    rows = {label: cells for label, _, cells, _ in nd.build_rows(schedule, {})}
    assert rows["10/8"][nd.COL_OF["阪神"]][1] is nd.WHITE


# --- Postseason ----------------------------------------------------------
#
# Evan's own 2026 sheet (プロ野球データ分析2026.xlsx, tab 2026) carries the
# Climax Series on by hand from 10/8; these pin the program to that layout.

STANDINGS_2026 = {
    "阪神": ("セ", 1), "巨人": ("セ", 2), "DeNA": ("セ", 3),
    "広島": ("セ", 4), "ヤクルト": ("セ", 5), "中日": ("セ", 6),
    "ソフトバンク": ("パ", 1), "西武": ("パ", 2), "日本ハム": ("パ", 3),
    "オリックス": ("パ", 4), "ロッテ": ("パ", 5), "楽天": ("パ", 6),
}


def _final(date, away, home, away_score, home_score, venue=""):
    return {"date": date, "away": away, "home": home, "status": "試合終了",
            "away_score": str(away_score), "home_score": str(home_score),
            "venue": venue or nd.HOME_VENUE[home]}


def _postseason_2026():
    games = [
        _final("2026-10-08", "ヤクルト", "阪神", 4, 0),
        _final("2026-10-10", "DeNA", "巨人", 2, 1),
        _final("2026-10-10", "日本ハム", "西武", 2, 3),
        _final("2026-10-11", "DeNA", "巨人", 4, 1),
        _final("2026-10-11", "日本ハム", "西武", 1, 0),
        _final("2026-10-12", "日本ハム", "西武", 4, 1),
        _final("2026-10-14", "DeNA", "阪神", 2, 0),
        _final("2026-10-14", "日本ハム", "ソフトバンク", 4, 6),
        _final("2026-10-15", "DeNA", "阪神", 1, 0),
        _final("2026-10-15", "日本ハム", "ソフトバンク", 2, 6),
        _final("2026-10-16", "DeNA", "阪神", 4, 0),
        _final("2026-10-16", "日本ハム", "ソフトバンク", 2, 6),
        _final("2026-10-17", "DeNA", "阪神", 8, 5),
        _game("2026-10-24", "ソフトバンク", "DeNA"),
        _game("2026-10-25", "ソフトバンク", "DeNA"),
        _game("2026-10-27", "DeNA", "ソフトバンク"),
    ]
    schedule = {}
    for g in games:
        schedule.setdefault(g["date"], []).append(g)
    return schedule


def _rows_by_date(schedule=None, standings=STANDINGS_2026):
    rows = nd.build_rows(schedule or _postseason_2026(), {}, standings=standings,
                         window=(nd.SEASON_END, nd.POSTSEASON_END))
    out = {}
    for label, _, cells, _ in rows:
        month, day = label.split("/")
        out[f"2026-{int(month):02d}-{int(day):02d}"] = cells
    return out


def test_the_diary_runs_past_the_regular_season_into_the_postseason():
    rows = _rows_by_date()
    assert "2026-10-17" in rows and "2026-10-27" in rows
    assert max(rows) == nd.POSTSEASON_END.isoformat()


def test_final_standings_label_each_column_on_the_last_day_it_is_free():
    """10/8 for every club that did not play the last regular-season day; 阪神
    and ヤクルト played it, so theirs drop to 10/9 — as Evan laid it out."""
    rows = _rows_by_date()
    giants = rows["2026-10-08"][nd.COL_OF["巨人"]]
    assert giants[0] == "セ・リーグ   二  位"
    assert giants[1] == nd.rgb("FF6600")
    assert rows["2026-10-08"][nd.COL_OF["ソフトバンク"]][0] == "パ･リーグ   優  勝"
    assert rows["2026-10-08"][nd.COL_OF["楽天"]][0] == "パ･リーグ   六  位"
    # 阪神 hosted the 10/8 game, so that cell is still the game.
    assert "4" in rows["2026-10-08"][nd.COL_OF["阪神"]][0]
    assert rows["2026-10-09"][nd.COL_OF["阪神"]][0] == "セ・リーグ   優  勝"
    assert rows["2026-10-09"][nd.COL_OF["ヤクルト"]][0] == "セ・リーグ   五  位"


def test_standings_wait_for_the_regular_season_to_finish():
    schedule = _postseason_2026()
    schedule["2026-10-08"] = [_game("2026-10-08", "ヤクルト", "阪神")]
    rows = _rows_by_date(schedule)
    assert rows["2026-10-08"][nd.COL_OF["巨人"]][0] == ""


def test_postseason_days_are_white_not_purple():
    rows = _rows_by_date()
    for date in ("2026-10-10", "2026-10-13", "2026-10-20"):
        for col, *_ in nd.TEAMS:
            text, bg, _ = rows[date][col]
            if not text:
                assert bg is nd.WHITE, (date, col)


def test_climax_series_games_go_in_the_hosts_column():
    rows = _rows_by_date()
    first = rows["2026-10-10"]
    assert first[nd.COL_OF["巨人"]][0].replace(" ", "").startswith("2-1")
    assert first[nd.COL_OF["西武"]][0].replace(" ", "").startswith("2")
    assert first[nd.COL_OF["DeNA"]][0] == ""


def test_each_stage_is_titled_the_day_before_it_opens():
    rows = _rows_by_date()
    assert rows["2026-10-09"][nd.COL_OF["巨人"]][0] == "Climax Series 1st"
    assert rows["2026-10-09"][nd.COL_OF["巨人"]][1] == nd.rgb("99CC00")
    assert rows["2026-10-09"][nd.COL_OF["西武"]][0] == "Climax Series 1st"
    assert rows["2026-10-09"][nd.COL_OF["西武"]][1] == nd.rgb("3366FF")
    assert rows["2026-10-13"][nd.COL_OF["阪神"]][0] == "C S  Final  Stage"
    assert rows["2026-10-13"][nd.COL_OF["ソフトバンク"]][0] == "C S  Final  Stage"


def test_the_series_winner_is_named_the_day_after_it_clinches():
    rows = _rows_by_date()
    # DeNA swept the 1st stage 10/10–10/11; 日本ハム took it 2-1 on 10/12.
    yokohama = rows["2026-10-12"][nd.COL_OF["巨人"]]
    assert yokohama[0] == "橫  浜   F S   進 出"
    assert yokohama[1] == nd.rgb("003366")
    assert rows["2026-10-13"][nd.COL_OF["西武"]][0] == "日本ハム   F S   進 出"
    # ソフトバンク's advantage win plus 10/14–10/16 makes four.
    assert rows["2026-10-17"][nd.COL_OF["ソフトバンク"]][0].startswith("ソフトB")
    assert "日本一" in rows["2026-10-17"][nd.COL_OF["ソフトバンク"]][0]
    assert rows["2026-10-18"][nd.COL_OF["阪神"]][0].startswith("橫  浜")


def test_series_result_counts_the_hosts_advantage_and_its_ties():
    def series(*results):
        return [_final(f"2026-10-{10 + i}", "日本ハム", "西武", a, h)
                for i, (a, h) in enumerate(results)]
    # 1st stage: a tie and a host win leave the visitor unable to reach two.
    assert nd.series_result(series((1, 1), (0, 2)), "first") == ("2026-10-11", "西武")
    assert nd.series_result(series((1, 0), (2, 0)), "first") == ("2026-10-11", "日本ハム")
    assert nd.series_result(series((1, 0),), "first") is None
    # Final stage: the pennant winner starts a game up.
    assert nd.series_result(series((0, 1), (0, 1), (0, 1)), "final") == ("2026-10-12", "西武")
    assert nd.series_result(series((1, 0), (1, 0), (1, 0)), "final") is None


def test_japan_series_is_written_across_the_middle():
    rows = _rows_by_date()
    middle = nd.JAPAN_SERIES_COL
    assert rows["2026-10-23"][middle][0] == "2026  年 度    日  本  シリーズ"
    assert rows["2026-10-23"][middle][1] == nd.rgb("0000FF")
    # Not played, starters not announced: which league's park it is.
    assert rows["2026-10-24"][middle][0] == "(セ・リーグ)"
    assert rows["2026-10-27"][middle][0] == "(パ・リーグ)"
    # The travel day between the two parks is greyed out.
    assert rows["2026-10-26"][middle][1] == nd.JAPAN_SERIES_OFF
    # The home club's own column does not repeat the game.
    assert nd.japan_series_merge_dates(_postseason_2026(), STANDINGS_2026) >= {
        "2026-10-23", "2026-10-24", "2026-10-25", "2026-10-26", "2026-10-27"}


def test_japan_series_champion_is_named_the_day_after():
    schedule = _postseason_2026()
    for date in ("2026-10-24", "2026-10-25"):
        schedule[date] = [_final(date, "ソフトバンク", "DeNA", 3, 1)]
    schedule["2026-10-27"] = [_final("2026-10-27", "DeNA", "ソフトバンク", 0, 2)]
    schedule["2026-10-28"] = [_final("2026-10-28", "DeNA", "ソフトバンク", 0, 5)]
    rows = _rows_by_date(schedule)
    champion = rows["2026-10-29"][nd.JAPAN_SERIES_COL]
    assert champion[0] == "福岡ソフトバンクホークス  日本シリーズ   優  勝"
    assert champion[1] == nd.rgb("FFCC00")
    assert "2026-10-29" in nd.japan_series_merge_dates(schedule, STANDINGS_2026)


def test_postseason_play_by_play_is_numbered_by_game_of_the_series():
    """CS and 日本シリーズ pages are not on the monthly schedule; their URL
    counts games within the series (/scores/2026/1012/l-f-03/)."""
    urls = nd.postseason_score_urls(_postseason_2026(), STANDINGS_2026, 2026)
    assert urls[("2026-10-12", "西武")] == "https://npb.jp/scores/2026/1012/l-f-03/"
    assert urls[("2026-10-17", "阪神")] == "https://npb.jp/scores/2026/1017/t-db-04/"
    assert ("2026-10-08", "阪神") not in urls


STANDINGS_HTML = """
<table class="tablefix2"><tbody class="bis-table-body">
<tr class="ststats"><td>阪神タイガース</td><td>143</td></tr>
<tr class="ststats"><td>読売ジャイアンツ</td><td>143</td></tr>
<tr class="ststats"><td>横浜DeNAベイスターズ</td><td>143</td></tr>
</tbody></table>
"""


def test_parse_standings_reads_npbs_order():
    assert nd.parse_standings(STANDINGS_HTML, "セ") == {
        "阪神": ("セ", 1), "巨人": ("セ", 2), "DeNA": ("セ", 3)}


def test_parse_standings_keeps_the_first_table():
    """The page carries more than one table of the same six clubs; only the
    first is the standings, and the rest must not renumber them."""
    assert nd.parse_standings(STANDINGS_HTML * 2, "セ")["DeNA"] == ("セ", 3)


YAHOO_DAY_HTML = """
<header class="bb-head01"><h2 class="bb-head01__title">{title}</h2></header>
<li class="bb-score__item">
  <p class="bb-score__venue">横浜</p>
  <p class="bb-score__homeLogo">DeNA</p><p class="bb-score__awayLogo">ソフトバンク</p>
  <span class="bb-score__score--left"></span><span class="bb-score__score--right"></span>
  <p class="bb-score__link">試合前</p>
</li>
"""


def test_fetch_schedule_ignores_the_page_yahoo_falls_back_to(monkeypatch):
    """Past the last scheduled day Yahoo answers every date with that day's
    page — 11/5 came back as 11/1's 日本シリーズ game 7, and the diary wrote a
    game on every day to the end of the tab."""
    class _Resp:
        def __init__(self, url):
            title = "11月1日（日）"
            self.text = YAHOO_DAY_HTML.format(title=title)

    monkeypatch.setattr(nd.requests, "get", lambda url, **kw: _Resp(url))
    import datetime
    out = nd.fetch_schedule(datetime.date(2026, 11, 1), datetime.date(2026, 11, 2))
    assert len(out["2026-11-01"]) == 1
    assert out["2026-11-02"] == []


# --- オープン戦 -----------------------------------------------------------
#
# The tab now opens with the spring games, 2/21–3/22, above 開幕. 賽錄 only
# covers 公式戦, so their starters come from NPB's own オープン戦 box scores.

PRESEASON_BOX = """
<html><body>
<div id="table_linescore"><table id="tablefix_ls"><tbody>
  <tr class="top"><th>西武</th>
    <td>4</td><td>2</td><td>1</td><td>0</td><td>0</td><td>1</td><td>0</td><td>2</td><td>0</td>
    <td class="total-1">10</td><td class="total-2">12</td><td class="total-2">0</td></tr>
  <tr class="bottom"><th>ソフトバンク</th>
    <td>0</td><td>2</td><td>1</td><td>0</td><td>1</td><td>0</td><td>2</td><td>0</td><td>0</td>
    <td class="total-1">6</td><td class="total-2">11</td><td class="total-2">0</td></tr>
</tbody></table></div>
<table id="tablefix_t_p"><tbody>
  <tr><td>○</td><td class="player"><a href="/bis/players/31035150.html">佐藤爽</a></td></tr>
  <tr><td></td><td class="player"><a href="/bis/players/01005136.html">高橋礼</a></td></tr>
</tbody></table>
<table id="tablefix_b_p"><tbody>
  <tr><td>●</td><td class="player"><a href="/bis/players/01005137.html">東浜</a></td></tr>
  <tr><td></td><td class="player"><a href="/bis/players/53755138.html">スチュワート・ジュニア</a></td></tr>
</tbody></table>
</body></html>
"""


def test_preseason_box_names_the_first_pitcher_on_each_side():
    box = nd.parse_preseason_box(PRESEASON_BOX)
    assert box["away_starter"] == "31035150"
    assert box["home_starter"] == "01005137"
    assert box["walkoff"] is False


def test_preseason_box_reads_a_walkoff_off_the_linescore():
    """NPB writes the deciding frame as ``1x``; a bare ``x`` is a bottom half
    the home side never had to bat, which is no walk-off at all."""
    walkoff = PRESEASON_BOX.replace("<td>0</td>\n    <td class=\"total-1\">6",
                                    "<td>1x</td>\n    <td class=\"total-1\">6")
    assert nd.parse_preseason_box(walkoff)["walkoff"] is True
    unplayed = PRESEASON_BOX.replace("<td>0</td>\n    <td class=\"total-1\">6",
                                     "<td>x</td>\n    <td class=\"total-1\">6")
    assert nd.parse_preseason_box(unplayed)["walkoff"] is False


def test_the_tab_opens_on_the_preseason_banner():
    rows = nd.build_rows({}, {}, window=(nd.SHEET_START, nd.SEASON_START))
    label, _, cells, _ = rows[0]
    assert label == "2/20"
    assert nd.row_index(nd.SHEET_START) == 1
    assert "オ ー プ ン 戦" in cells[nd.TEAMS[0][0]][0]


def test_opening_day_is_announced_on_the_day_before():
    """2023 titles its 公式戦 on the free day before 開幕; 2026's is 3/26."""
    rows = {label: cells for label, _, cells, _ in
            nd.build_rows({}, {}, window=(nd.SHEET_START, nd.SEASON_START))}
    title, bg, _ = rows["3/26"][nd.TEAMS[0][0]]
    assert "公 式 戦" in title and bg is nd.PURPLE


def test_banner_text_is_white_18pt_whatever_its_colour():
    rows = nd.build_rows({}, {}, window=(nd.SHEET_START, nd.SHEET_START))
    banner = nd.sheet_payload(rows, with_header=False)[0]["values"][nd.TEAMS[0][0]]
    fmt = banner["userEnteredFormat"]["textFormat"]
    assert fmt["fontSize"] == 18 and fmt["bold"]
    assert fmt["foregroundColor"] == nd.rgb("FFFFFF")


def test_a_preseason_game_is_written_like_any_other():
    """アイビー is SoftBank's camp park in Miyazaki, so the venue goes in the score."""
    game = _final("2026-03-01", "西武", "ソフトバンク", 10, 6, venue="アイビー")
    starters = {("2026-03-01", "ソフトバンク"): {
        "away_starter": "佐藤爽", "home_starter": "東浜", "walkoff": False}}
    rows = {label: cells for label, _, cells, _ in
            nd.build_rows({"2026-03-01": [game]}, starters,
                          window=(nd.SHEET_START, nd.SEASON_START))}
    text, bg, _ = rows["3/1"][nd.COL_OF["ソフトバンク"]]
    assert bg is nd.WHITE
    assert text.startswith("佐藤爽") and text.endswith("東浜") and "(アイビー)" in text
    # Nobody else played that day.
    assert rows["3/1"][nd.COL_OF["巨人"]][1] is nd.PURPLE


def test_preseason_and_regular_season_starters_share_one_spelling():
    """A second 髙橋 who only started in March still makes every 髙橋 ambiguous."""
    regular = {("2026-04-01", "西武"): {"away_starter": "髙橋 光成",
                                        "home_starter": "今井 達也", "walkoff": False}}
    spring = {("2026-03-01", "中日"): {"away_starter": "東 克樹",
                                       "home_starter": "髙橋 宏斗", "walkoff": False}}
    out, _ = nd.resolve_starters({**regular, **spring})
    assert out[("2026-04-01", "西武")]["away_starter"] == "髙橋光"
    assert out[("2026-03-01", "中日")]["home_starter"] == "髙橋宏"


def test_a_preseason_walkoff_is_probed_at_its_own_score_page(monkeypatch):
    """オープン戦 games are not on the monthly 公式戦 schedule pages."""
    def no_monthly_pages(*a, **kw):
        raise AssertionError("the 公式戦 schedule pages were fetched")

    class _Resp:
        status_code = 200
        encoding = "utf-8"
        text = "<p>10回裏 正木 レフトソロホームラン（打点1）</p><p>一般社団法人日本野球機構について</p>"

    asked = []
    monkeypatch.setattr(nd, "npb_score_urls", no_monthly_pages)
    monkeypatch.setattr(nd.requests, "get",
                        lambda url, **kw: asked.append(url) or _Resp())
    key = ("2026-03-01", "ソフトバンク")
    starters = {key: {"away_starter": "佐藤爽", "home_starter": "東浜", "walkoff": True,
                      "score_url": "https://npb.jp/scores/2026/0301/h-l-01/"}}
    schedule = {"2026-03-01": [_final("2026-03-01", "西武", "ソフトバンク", 6, 7)]}
    assert nd.mark_walkoff_home_runs(starters, schedule) == 1
    assert asked == ["https://npb.jp/scores/2026/0301/h-l-01/playbyplay.html"]


def test_npb_player_names_are_spelled_the_way_saiboku_spells_them():
    """NPB's player pages write 伊藤大海's 海 as U+FA45 and keep a foreigner's
    initial (Ａ．コックス); 賽錄 has neither. Left alone, each read as a second
    pitcher of the same name, and 伊藤大 turned into 伊藤大海 all season."""
    assert nd.normalize_full_name("伊藤　大海") == "伊藤 大海"
    assert nd.normalize_full_name("Ａ．コックス") == "コックス"
    assert nd.normalize_full_name("Ｃ．スチュワート・ジュニア") == "スチュワート・ジュニア"
    assert nd.normalize_full_name("髙橋　宏斗") == "髙橋 宏斗"


def test_practice_games_are_left_off_the_preseason():
    """Yahoo's calendar carries 練習試合 alongside the オープン戦 (2/25 SOKKEN
    ロッテ@オリックス); only NPB's own オープン戦 schedule decides what is written.
    A rained-out オープン戦 is still on it, and so is every 公式戦 game."""
    practice = _final("2026-02-25", "ロッテ", "オリックス", 1, 6, venue="SOKKEN")
    rainout = dict(_game("2026-03-03", "中日", "DeNA"), status="試合中止")
    regular = _final("2026-03-27", "巨人", "阪神", 1, 3)
    schedule = {"2026-02-25": [practice], "2026-03-03": [rainout],
                "2026-03-27": [regular]}
    preseason = {("2026-03-03", "DeNA"): {}}
    kept = nd.drop_practice_games(schedule, preseason)
    assert kept == {"2026-02-25": [], "2026-03-03": [rainout], "2026-03-27": [regular]}


def test_a_crowded_spring_cell_does_not_shorten_a_name_all_season():
    """Spring games are mostly at camp parks, so the venue rides inside the score
    and the cell is tighter. Fitting the whole season at once cut ウィットリー to
    ウィット on every 公式戦 cell he ever started; only March should give."""
    spring = _final("2026-02-23", "広島", "巨人", 0, 3, venue="那覇")
    regular = _final("2026-03-31", "中日", "阪神", 5, 2)
    starters = {
        ("2026-02-23", "巨人"): {"away_starter": "ウィットリー", "home_starter": "ラグズデール",
                                 "walkoff": False},
        ("2026-03-31", "阪神"): {"away_starter": "ウィットリー", "home_starter": "金丸",
                                 "walkoff": False},
    }
    schedule = {"2026-02-23": [spring], "2026-03-31": [regular]}
    season, early = nd.solve_season_name_lengths(schedule, starters)
    assert "ウィットリー" not in season
    assert early["ウィットリー"] < len("ウィットリー")
    rows = {label: cells for label, _, cells, _ in nd.build_rows(
        schedule, starters, name_limits=season, preseason_limits=early,
        window=(nd.SHEET_START, datetime.date(2026, 3, 31)))}
    assert rows["3/31"][nd.COL_OF["阪神"]][0].startswith("ウィットリー")
    assert not rows["2/23"][nd.COL_OF["巨人"]][0].startswith("ウィットリー")


# --- Rolling over to the next season --------------------------------------
#
# Nothing about a season is typed in any more: its dates come off npb.jp, so
# the diary starts 2027's tab on its own. 2026, derived the same way, has to
# come out exactly as it was laid out by hand.

SCHEDULE_PAGE = """
<table>
<tr id="date0401" class="">
  <th rowspan="2">4/1（水）</th>
  <td><div class="team1">ヤクルト</div>
      <a href="/scores/2026/0401/s-c-02/"><div class="cancel">中止</div></a>
      <div class="team2">広島</div></td>
  <td><div class="place">神　宮</div></td>
</tr>
<tr>
  <td><div class="team1">阪神</div>
      <a href="/scores/2026/0401/t-g-02/"><div class="score1">3</div>
      <div class="state">-</div><div class="score2">1</div></a>
      <div class="team2">巨人</div></td>
  <td><div class="place">甲子園</div></td>
</tr>
<tr id="date0402" class=" last">
  <th>4/2（木）</th><td>&nbsp;</td><td>&nbsp;</td>
</tr>
<tr id="date0403" class="">
  <th>4/3（金）</th>
  <td><div class="team1">西武</div><div class="team2">楽天</div></td>
  <td><div class="place">ベルーナドーム</div></td>
</tr>
</table>
"""


def test_a_schedule_page_lists_every_game_and_marks_rainouts():
    games = nd.parse_schedule_page(SCHEDULE_PAGE, 2026)
    assert games == [
        {"date": "2026-04-01", "home": "ヤクルト", "away": "広島", "cancelled": True},
        {"date": "2026-04-01", "home": "阪神", "away": "巨人", "cancelled": False},
        # Not played yet: still on the schedule, still counts.
        {"date": "2026-04-03", "home": "西武", "away": "楽天", "cancelled": False},
    ]


def _season(days, rainout=None):
    """Six pairings a day; ``rainout`` is a date whose games were called off."""
    pairs = [("巨人", "阪神"), ("中日", "広島"), ("ヤクルト", "DeNA"),
             ("西武", "楽天"), ("ロッテ", "日本ハム"), ("ソフトバンク", "オリックス")]
    return [{"date": d, "home": h, "away": a, "cancelled": d == rainout}
            for d in days for h, a in pairs]


def test_the_season_ends_the_day_every_club_has_played_its_quota(monkeypatch):
    """The monthly pages run on into the CS and 日本シリーズ with nothing to mark
    them; the regular season is over once all twelve clubs reach 143."""
    monkeypatch.setattr(nd, "GAMES_PER_TEAM", 3)
    games = _season(["2026-03-27", "2026-03-28", "2026-03-29"])
    games.append({"date": "2026-10-10", "home": "巨人", "away": "DeNA", "cancelled": False})
    assert nd.season_bounds(games) == (datetime.date(2026, 3, 27),
                                       datetime.date(2026, 3, 29))


def test_a_rainout_does_not_count_toward_the_quota(monkeypatch):
    monkeypatch.setattr(nd, "GAMES_PER_TEAM", 3)
    games = _season(["2026-03-27", "2026-03-28", "2026-03-29", "2026-03-30"],
                    rainout="2026-03-28")
    assert nd.season_bounds(games)[1] == datetime.date(2026, 3, 30)


def test_an_unfinished_season_runs_to_its_last_scheduled_day(monkeypatch):
    """Mid-season the quota is not reached yet — makeups are still unscheduled —
    so the end is the last day on the schedule, and moves when one is added."""
    monkeypatch.setattr(nd, "GAMES_PER_TEAM", 143)
    games = _season(["2026-03-27", "2026-09-30"])
    assert nd.season_bounds(games)[1] == datetime.date(2026, 9, 30)


def test_interleague_ends_on_its_last_full_day_not_its_makeups():
    """2026: six games a day 5/26–6/14, then makeups 6/16–6/17 one or two at a
    time. The banner tail starts after 6/14, the way the hand layout had it."""
    central = ["巨人", "阪神", "中日", "広島", "ヤクルト", "DeNA"]
    pacific = ["西武", "楽天", "ロッテ", "日本ハム", "ソフトバンク", "オリックス"]
    full_day = lambda d: [{"date": d, "home": c, "away": p, "cancelled": False}
                          for c, p in zip(central, pacific)]
    games = (full_day("2026-05-26") + full_day("2026-06-14")
             + [{"date": "2026-06-16", "home": "阪神", "away": "西武", "cancelled": False}]
             + [{"date": "2026-06-20", "home": "阪神", "away": "巨人", "cancelled": False}])
    assert nd.interleague_bounds(games) == (datetime.date(2026, 5, 26),
                                            datetime.date(2026, 6, 14))


def test_the_sponsor_title_is_spaced_out_but_keeps_se_pa_whole():
    assert nd.spaced_title("日本生命セ・パ交流戦") == "日 本 生 命 セ・パ 交 流 戦"


ALLSTAR_BOX = """
<div class="game_tit">2026年7月29日（水） 富 山 【オールスターゲーム】 パシフィック・リーグ vs セントラル・リーグ 第2戦</div>
<table id="tablefix_ls"><tbody>
  <tr class="top"><th>セ</th><td>1</td><td class="total-1">8</td><td class="total-2">12</td><td class="total-2">0</td></tr>
  <tr class="bottom"><th>パ</th><td>2</td><td class="total-1">7</td><td class="total-2">9</td><td class="total-2">1</td></tr>
</tbody></table>
<table id="tablefix_t_p"><tr><td class="player"><a href="/bis/players/1.html">髙橋</a></td></tr></table>
<table id="tablefix_b_p"><tr><td class="player"><a href="/bis/players/2.html">エスピノーザ</a></td></tr></table>
"""


def test_an_allstar_game_is_read_off_its_box_score():
    """Exactly what was typed in by hand for 2026's second game."""
    assert nd.parse_allstar_box(ALLSTAR_BOX) == ("髙橋", "8", "富山", "7", "エスピノーザ")
    # The live page pads it with an ideographic space.
    assert nd.parse_allstar_box(ALLSTAR_BOX.replace("富 山", "富\u3000山"))[2] == "富山"
    tokyo = ALLSTAR_BOX.replace("富 山", "東京ドーム")
    assert nd.parse_allstar_box(tokyo)[2] == "東京"


CALENDAR_2026 = dict(
    year=2026,
    preseason_start=datetime.date(2026, 2, 21),
    season_start=datetime.date(2026, 3, 27),
    season_end=datetime.date(2026, 10, 8),
    interleague=(datetime.date(2026, 5, 26), datetime.date(2026, 6, 14)),
    interleague_title="日本生命セ・パ交流戦",
    allstar_games={
        "2026-07-28": ("伊藤", "7", "東京", "5", "山野"),
        "2026-07-29": ("髙橋", "8", "富山", "7", "エスピノーザ"),
    },
)


def test_2026_derived_from_its_dates_is_the_2026_laid_out_by_hand():
    cal = nd.Calendar(**CALENDAR_2026)
    assert cal.sheet_title == "2026・野球日記"
    assert cal.sheet_start == datetime.date(2026, 2, 20)
    assert cal.postseason_end == datetime.date(2026, 11, 8)
    assert cal.banners == {
        "2026-02-20": ("2026 年 オ ー プ ン 戦", nd.PURPLE),
        "2026-03-26": ("2026 年 シーズン 公 式 戦", nd.PURPLE),
        "2026-05-25": ("2026 日 本 生 命 セ・パ 交 流 戦", nd.PINK),
    }
    assert cal.allstar_days == ("2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30")
    assert cal.interleague_tail == frozenset(
        ("2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18"))
    assert cal.preseason_file.endswith("npb_preseason_2026.json")


def test_using_a_calendar_moves_the_whole_tab_to_that_year():
    later = dict(CALENDAR_2026, year=2027,
                 preseason_start=datetime.date(2027, 2, 20),
                 season_start=datetime.date(2027, 3, 26),
                 season_end=datetime.date(2027, 10, 3),
                 interleague=(datetime.date(2027, 5, 25), datetime.date(2027, 6, 13)),
                 allstar_games={})
    before = nd.Calendar(**CALENDAR_2026)
    try:
        nd.use_calendar(nd.Calendar(**later))
        assert nd.SHEET_TITLE == "2027・野球日記"
        assert nd.row_index(datetime.date(2027, 2, 19)) == 1
        rows = nd.build_rows({}, {}, window=(nd.SHEET_START, nd.SEASON_START))
        assert rows[0][0] == "2/19" and "2027" in rows[0][2][nd.TEAMS[0][0]][0]
    finally:
        nd.use_calendar(before)
    assert nd.SHEET_TITLE == "2026・野球日記"


def test_no_calendar_is_quiet_in_the_winter_and_an_alarm_once_spring_comes():
    """npb.jp publishes next year's schedule some time over the winter. Before
    March a missing one is normal; after that it is the 10/4 stall again."""
    assert nd.missing_calendar_is_an_error(datetime.date(2027, 1, 10)) is False
    assert nd.missing_calendar_is_an_error(datetime.date(2027, 2, 28)) is False
    assert nd.missing_calendar_is_an_error(datetime.date(2027, 3, 1)) is True


def test_the_preseason_page_dates_its_rows_only_in_the_header_cell():
    """npb.jp/preseason/ has no tr#dateMMDD — just ``<th>2/21（土）</th>``."""
    page = """<table><tbody>
      <tr><th class="saturday" rowspan="2">2/21（土）</th>
        <td><div class="team1">楽天</div><div class="team2">ロッテ</div></td></tr>
      <tr><td><div class="team1">中日</div><div class="team2">阪神</div></td></tr>
    </tbody></table>"""
    games = nd.parse_schedule_page(page, 2026)
    assert [(g["date"], g["home"]) for g in games] == [
        ("2026-02-21", "楽天"), ("2026-02-21", "中日")]


def test_a_season_not_yet_published_is_quiet_until_march():
    winter = datetime.date(2027, 1, 15)
    assert nd.run_window(winter, None, back=3, forward=21) is None
    try:
        nd.run_window(datetime.date(2027, 3, 2), None, back=3, forward=21)
    except nd.CalendarMissing:
        pass
    else:
        raise AssertionError("a missing calendar in March must fail the run")


def test_the_window_is_empty_before_the_tab_opens_and_after_it_closes():
    cal = nd.Calendar(**CALENDAR_2026)
    assert nd.run_window(datetime.date(2026, 1, 20), cal, back=3, forward=21) is None
    assert nd.run_window(datetime.date(2026, 12, 1), cal, back=3, forward=21) is None
    # Three weeks ahead reaches into the tab before it opens.
    assert nd.run_window(datetime.date(2026, 2, 5), cal, back=3, forward=21) == (
        datetime.date(2026, 2, 20), datetime.date(2026, 2, 26))
    assert nd.run_window(datetime.date(2026, 11, 7), cal, back=3, forward=21) == (
        datetime.date(2026, 11, 4), datetime.date(2026, 11, 8))


PRESEASON_SCHEDULE_PAGE = """
<a href="/scores/2026/0221/e-m-01/">x</a>
<a href="/scores/2026/0307/g-s-01/">x</a>
"""


def test_a_spring_run_reads_only_the_box_scores_in_its_window(monkeypatch, tmp_path):
    """While the オープン戦 are on there is no finished file to read, and
    re-reading every box score so far on each 30-minute run is ~200 requests."""
    asked = []

    class _Resp:
        status_code = 200
        encoding = "utf-8"

        def __init__(self, url):
            self.text = (PRESEASON_SCHEDULE_PAGE if "schedule_detail" in url
                         else PRESEASON_BOX if url.endswith("box.html")
                         else "<title>佐藤　爽（埼玉西武ライオンズ）</title>")

    monkeypatch.setattr(nd.requests, "get",
                        lambda url, **kw: asked.append(url) or _Resp(url))
    cache = tmp_path / "npb_preseason_2026.json"
    out = nd.fetch_preseason_starters(2026, cache=str(cache),
                                      dates=("2026-03-05", "2026-03-08"))
    assert list(out) == [("2026-03-07", "巨人")]
    assert not any("0221" in url for url in asked)
    # A partial sweep is never saved as if it were the finished one.
    assert not cache.exists()
