"""Unit tests for the npb.jp box-score parser (baseball/npb_box.py).

Yahoo only serves the current season, so every year before this one has to come
from npb.jp instead. Its box score carries the one thing 賽錄 never had — the
per-pitcher 投球数 and 四球 — which is what these tests are guarding.
"""

import pytest

from baseball import npb_box


# npb.jp renders 投球回 as a nested one-row table: the whole innings in a <th>
# and the thirds in a <td> as ".1" / ".2". An integer outing leaves the <td>
# empty rather than writing ".0".
def _innings(whole, frac=""):
    return (f'<td><table class="table_inning"><tbody><tr>'
            f"<th>{whole}</th><td>{frac}</td></tr></tbody></table></td>")


def _pitcher_row(name, *, pid="1", mark="", pitches=100, batters=25,
                 whole=6, frac="", hits=5, hr=1, bb=2, hbp=0, so=7,
                 wp=0, balk=0, runs=3, er=3):
    link = f'<a href="/bis/players/{pid}.html">{name}</a>' if pid else name
    return (
        "<tr>"
        f"<td>{mark}</td>"
        f'<td class="player">{link}</td>'
        f"<td>{pitches}</td><td>{batters}</td>"
        + _innings(whole, frac)
        + f"<td>{hits}</td><td>{hr}</td><td>{bb}</td><td>{hbp}</td>"
        f"<td>{so}</td><td>{wp}</td><td>{balk}</td>"
        f"<td>{runs}</td><td>{er}</td>"
        "</tr>"
    )


# The 合計 row has the same shape as a pitcher's but names no player, so it has
# to be dropped by name rather than by position.
_TOTAL_ROW = _pitcher_row("チーム計", pid=None)

_HEADER_ROW = (
    "<tr><th></th><th>投手</th><th>投球数</th><th>打者</th><th>投球回</th>"
    "<th>安打</th><th>本塁打</th><th>四球</th><th>死球</th><th>三振</th>"
    "<th>暴投</th><th>ボーク</th><th>失点</th><th>自責点</th></tr>"
)


def _team_cell(full, short):
    return (f'<th><span class="flag"><span class="hide_sp">{full}</span>'
            f'<span class="hide_pc">{short}</span></span></th>')


def _box_html(away_rows, home_rows, *, away=("中日ドラゴンズ", "中日"),
              home=("広島東洋カープ", "広島"), body=""):
    def table(tid, rows):
        return (f'<table id="{tid}"><tbody>{_HEADER_ROW}'
                + "".join(rows) + "</tbody></table>")

    return (
        "<html><body>"
        f'<table id="tablefix_ls"><tbody>'
        f"<tr><th></th><th>1</th><th>計</th></tr>"
        f"<tr>{_team_cell(*away)}<td>1</td><td>7</td></tr>"
        f"<tr>{_team_cell(*home)}<td>0</td><td>1</td></tr>"
        "</tbody></table>"
        + table("tablefix_t_p", away_rows)
        + table("tablefix_b_p", home_rows)
        + body
        + "</body></html>"
    )


def test_parses_both_sides_with_short_team_names():
    html = _box_html([_pitcher_row("マラー", mark="○")],
                     [_pitcher_row("斉藤優", mark="●")])

    box = npb_box.parse_box(html)

    # 賽錄 spells teams the short way, so that is the form worth keeping.
    assert box["away"] == "中日"
    assert box["home"] == "広島"
    assert [p.name for p in box["away_pitchers"]] == ["マラー"]
    assert [p.name for p in box["home_pitchers"]] == ["斉藤優"]


def test_reads_the_columns_the_backfill_exists_for():
    html = _box_html(
        [_pitcher_row("大野", pitches=117, batters=29, whole=6, frac=".2",
                      hits=5, hr=0, bb=5, hbp=0, so=2, runs=2, er=2)],
        [_pitcher_row("相手")],
    )

    starter = npb_box.parse_box(html)["away_pitchers"][0]

    assert starter.pitches == 117
    assert starter.batters == 29
    assert starter.bb == 5
    assert starter.so == 2
    assert starter.hits == 5
    assert starter.er == 2


@pytest.mark.parametrize("whole,frac,outs", [
    (6, "", 18),      # a whole-inning outing leaves the thirds cell blank
    (5, ".1", 16),
    (0, ".2", 2),
    (1, ".1", 4),
])
def test_innings_are_counted_in_outs(whole, frac, outs):
    html = _box_html([_pitcher_row("投", whole=whole, frac=frac)],
                     [_pitcher_row("相手")])

    starter = npb_box.parse_box(html)["away_pitchers"][0]

    assert starter.outs == outs
    assert starter.ip == pytest.approx(outs / 3)


def test_team_total_row_is_not_a_pitcher():
    html = _box_html(
        [_pitcher_row("先発"), _pitcher_row("中継"), _TOTAL_ROW],
        [_pitcher_row("相手"), _TOTAL_ROW],
    )

    box = npb_box.parse_box(html)

    assert [p.name for p in box["away_pitchers"]] == ["先発", "中継"]
    assert [p.name for p in box["home_pitchers"]] == ["相手"]


def test_appearance_order_marks_the_starter():
    html = _box_html([_pitcher_row("先発"), _pitcher_row("二番手")],
                     [_pitcher_row("相手")])

    away = npb_box.parse_box(html)["away_pitchers"]

    assert [p.order for p in away] == [1, 2]
    assert away[0].is_starter and not away[1].is_starter


def test_player_id_comes_off_the_profile_link():
    html = _box_html([_pitcher_row("大野", pid="61765157")],
                     [_pitcher_row("相手")])

    assert npb_box.parse_box(html)["away_pitchers"][0].player_id == "61765157"


def test_win_loss_mark_is_kept():
    html = _box_html([_pitcher_row("勝", mark="○")],
                     [_pitcher_row("負", mark="●")])

    box = npb_box.parse_box(html)

    assert box["away_pitchers"][0].result == "○"
    assert box["home_pitchers"][0].result == "●"


# A rained-out game still serves a 200 with a real page — it just has no
# pitcher tables. Telling that apart from a throttled fetch is what keeps the
# backfill from caching an empty result as though it were the truth.
def test_cancelled_game_has_no_box():
    html = "<html><body><p>試合中止</p></body></html>"

    assert npb_box.parse_box(html) is None
    assert npb_box.is_cancelled(html) is True


def test_a_page_with_neither_tables_nor_a_cancellation_is_not_cancelled():
    html = "<html><body><p>Too Many Requests</p></body></html>"

    assert npb_box.parse_box(html) is None
    assert npb_box.is_cancelled(html) is False


def test_one_sided_page_is_refused_rather_than_half_parsed():
    # A truncated response that happens to contain the first table would
    # otherwise be cached as a game where only one side pitched.
    html = _box_html([_pitcher_row("先発")], []).replace(
        '<table id="tablefix_b_p">', '<table id="something_else">')

    assert npb_box.parse_box(html) is None


# --- fetching helpers ---

# The game-number suffix is not always -01 and 交流戦 sits on its own page, so
# the links are read off the schedule rather than built from a pattern.
def test_schedule_links_are_read_and_deduplicated():
    html = """
      <a href="/scores/2026/0801/c-d-15/index.html">1</a>
      <a href="/scores/2026/0801/c-d-15/box.html">same game</a>
      <a href="/scores/2026/0801/e-h-16/index.html">2</a>
      <a href="/npb/other/">not a game</a>
    """

    assert npb_box.parse_schedule(html) == [
        ("2026", "0801", "c-d-15"),
        ("2026", "0801", "e-h-16"),
    ]


def test_game_code_and_url():
    assert npb_box.game_code("2026", "0801", "c-d-15") == "20260801-c-d-15"
    assert npb_box.box_url("2026", "0801", "c-d-15") == (
        "https://npb.jp/scores/2026/0801/c-d-15/box.html")


def test_read_box_returns_none_only_for_a_cancellation():
    assert npb_box.read_box("<p>試合中止</p>") is None


# The failure this guards is the one that silently poisons a backfill: a
# refused request answered with a 200 and an empty page, cached as fact.
def test_read_box_raises_when_a_page_is_merely_empty():
    with pytest.raises(npb_box.Throttled):
        npb_box.read_box("<html><body>Too Many Requests</body></html>")


def test_read_box_returns_the_parsed_game():
    html = _box_html([_pitcher_row("先発")], [_pitcher_row("相手")])

    assert npb_box.read_box(html)["away"] == "中日"
