"""Unit tests for the 2026・野球日記 tab writer (baseball/npb_diary.py)."""

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
