"""Unit tests for the probable-starter lookup (baseball/npb_starters.py)."""

from baseball import npb_starters as ns

# The weekly schedule groups games under a date heading; a game belongs to the
# heading above it.
SCHEDULE_HTML = """
<html><body>
  <h2>8月25日</h2>
  <a href="/npb/game/2021039325/index">試合</a>
  <a href="/npb/game/2021039325/index">同じ試合の別リンク</a>
  <a href="/npb/game/2021039329/index">試合</a>
  <h2>8月29日</h2>
  <a href="/npb/game/2021039400/index">来週の試合</a>
</body></html>
"""


def _game_html(date, home, away, pitchers):
    links = "".join(
        f'<a href="/npb/player/{pid}/top">{name}</a>' for pid, name in pitchers
    )
    return (f"<html><head><title>{date} {home}vs.{away} - プロ野球</title></head>"
            f"<body>{links}</body></html>")


GAMES = {
    "2021039325": _game_html("2026年8月25日", "東京ヤクルトスワローズ",
                             "読売ジャイアンツ",
                             [("2103794", "吉村 貢司郎"), ("1200117", "則本 昂大")]),
    "2021039329": _game_html("2026年8月25日", "千葉ロッテマリーンズ",
                             "福岡ソフトバンクホークス",
                             [("2107880", "吉川 悠斗"), ("1700011", "モイネロ")]),
    # A game on another date, reachable from the same weekly schedule page.
    "2021039400": _game_html("2026年8月29日", "広島東洋カープ",
                             "東京ヤクルトスワローズ", []),
}


def _fetch(url):
    if url.endswith("/schedule/"):
        return SCHEDULE_HTML
    for game_id, html in GAMES.items():
        if game_id in url:
            return html
    raise AssertionError(f"unexpected url {url}")


def test_starters_are_keyed_by_the_team_names_the_odds_use():
    """Yahoo spells teams out in full; the odds feed uses short names."""
    found = ns.fetch_starters("2026-08-25", fetch=_fetch)
    assert set(found) == {"ヤクルト", "巨人", "ロッテ", "ソフトバンク"}
    assert found["巨人"].name == "則本 昂大"
    assert found["ヤクルト"].name == "吉村 貢司郎"


def test_each_starter_carries_a_link_to_their_yahoo_page():
    found = ns.fetch_starters("2026-08-25", fetch=_fetch)
    assert found["ソフトバンク"].url == (
        "https://baseball.yahoo.co.jp/npb/player/1700011/top")


def test_the_home_starter_is_the_first_listed():
    """Yahoo titles read home-first, and so do the two player links."""
    found = ns.fetch_starters("2026-08-25", fetch=_fetch)
    assert found["ロッテ"].name == "吉川 悠斗"       # home
    assert found["ソフトバンク"].name == "モイネロ"   # away


def test_only_the_wanted_days_games_are_fetched():
    """A week of games is a hundred requests once retries count, so the day is
    picked out of the schedule before anything is fetched."""
    asked = []

    def counting(url):
        asked.append(url)
        return _fetch(url)

    found = ns.fetch_starters("2026-08-25", fetch=counting)
    assert "広島" not in found
    assert not any("2021039400" in url for url in asked)


def test_a_game_without_announced_starters_is_skipped():
    found = ns.fetch_starters("2026-08-29", fetch=_fetch)
    assert found == {}


def test_a_failed_lookup_yields_no_starters_rather_than_raising():
    """The broadcast is still worth sending without pitcher names."""
    def broken(url):
        raise OSError("network down")

    assert ns.fetch_starters("2026-08-25", fetch=broken) == {}


def test_a_server_error_is_retried_before_giving_up(monkeypatch):
    """Yahoo answers bursts with 500s, and losing the schedule page costs every
    pitcher name on the slate."""
    class _Response:
        def __init__(self, code):
            self.status_code = code
            self.text = "ok"

    codes = [500, 500, 200]
    monkeypatch.setattr(ns.requests, "get",
                        lambda url, **kw: _Response(codes.pop(0)))
    slept = []

    assert ns._get("https://example.test", sleep=slept.append) == "ok"
    assert slept == [2.0, 4.0]


def test_retries_stop_and_the_broadcast_survives(monkeypatch):
    class _Response:
        status_code = 500
        text = ""

    monkeypatch.setattr(ns.requests, "get", lambda url, **kw: _Response())
    monkeypatch.setattr(ns.time, "sleep", lambda s: None)

    assert ns.fetch_starters("2026-08-25") == {}


WEATHER_HTML = (
    '<a class="bb-gameCard__weather" '
    'href="https://weather.yahoo.co.jp/weather/jp/23/5110/23102.html">'
    '<img class="bb-gameCard__weatherImg" src="/next/pinpoint/20_night.png" '
    'alt="晴れ"></a>'
)


def test_the_forecast_comes_off_the_same_page_as_the_starters():
    """Yahoo prints the pinpoint forecast for first pitch on the game page, so
    it costs no request of its own."""
    games = dict(GAMES)
    games["2021039325"] = games["2021039325"].replace(
        "<body>", "<body>" + WEATHER_HTML)

    def fetch(url):
        if url.endswith("/schedule/"):
            return SCHEDULE_HTML
        for game_id, html in games.items():
            if game_id in url:
                return html
        raise AssertionError(url)

    slate = ns.fetch_slate("2026-08-25", fetch=fetch)
    assert slate.weather["巨人"].condition == "晴れ"
    assert slate.weather["ヤクルト"] == slate.weather["巨人"]   # one game, one sky
    assert "ロッテ" not in slate.weather                        # no markup there


FORECAST_HTML = """
<html><body>
<h3>今日の天気 - 8月25日( 火 )</h3>
<table class="yjw_table2">
 <tr><td>時刻</td><td>12時</td><td>15時</td><td>18時</td><td>21時</td></tr>
 <tr><td>天気</td><td>晴れ</td><td>晴れ</td><td>曇り</td><td>晴れ</td></tr>
 <tr><td>気温（℃）</td><td>35</td><td>36</td><td>33</td><td>30</td></tr>
 <tr><td>湿度（％）</td><td>58</td><td>52</td><td>64</td><td>73</td></tr>
 <tr><td>降水量（mm）</td><td>0.1</td><td>0.1</td><td>0</td><td>0</td></tr>
 <tr><td>風向 風速（m/s）</td><td>北東 1</td><td>南南西 2</td><td>南 4</td><td>南南東 3</td></tr>
</table>
<h3>明日の天気 - 8月26日( 水 )</h3>
<table class="yjw_table2">
 <tr><td>時刻</td><td>12時</td><td>15時</td><td>18時</td><td>21時</td></tr>
 <tr><td>天気</td><td>晴れ</td><td>曇り</td><td>雨</td><td>雨</td></tr>
 <tr><td>気温（℃）</td><td>35</td><td>37</td><td>28</td><td>27</td></tr>
 <tr><td>湿度（％）</td><td>51</td><td>46</td><td>61</td><td>74</td></tr>
 <tr><td>降水量（mm）</td><td>0.1</td><td>0.1</td><td>4.5</td><td>2.0</td></tr>
 <tr><td>風向 風速（m/s）</td><td>南南西 2</td><td>南南西 3</td><td>南 4</td><td>南東 3</td></tr>
</table>
</body></html>
"""


def test_the_forecast_is_read_for_the_hour_the_game_starts():
    got = ns.parse_forecast(FORECAST_HTML, "2026-08-25", 18)
    assert got.condition == "曇り"
    assert got.temp_c == "33"
    assert got.rain_mm == "0"
    assert got.wind == "南 4"


def test_the_right_day_is_picked_out_of_today_and_tomorrow():
    """The evening broadcast is about tomorrow, so the second table is the one
    that matters — and a wet game is exactly what a reader wants flagged."""
    got = ns.parse_forecast(FORECAST_HTML, "2026-08-26", 18)
    assert got.condition == "雨"
    assert got.rain_mm == "4.5"


def test_an_off_grid_start_takes_the_nearest_three_hour_slot():
    """The table steps in threes; a 14:00 first pitch is nearest 15時."""
    assert ns.parse_forecast(FORECAST_HTML, "2026-08-25", 14).temp_c == "36"


def test_a_day_the_page_does_not_cover_has_no_forecast():
    assert ns.parse_forecast(FORECAST_HTML, "2026-09-09", 18) is None


def test_the_summary_reads_as_one_line():
    got = ns.parse_forecast(FORECAST_HTML, "2026-08-26", 18)
    assert got.summary() == "☔ 雨 28℃ 降水 4.5mm 南 4m/s"


def _round_line(venue):
    return ('<p id="async-gameCard" class="bb-gameDescription__left">\n'
            f'  8月25日（火）\n  <time>18:00</time>\n  {venue}\n</p>')


def test_the_venue_is_read_off_the_game_page():
    assert ns.parse_venue(_round_line("バンテリンドーム")) == "バンテリンドーム"
    assert ns.parse_venue(_round_line("横浜")) == "横浜"
    assert ns.parse_venue("<p>no description line</p>") == ""


def test_roofed_venues_are_recognised():
    """A roof is what decides whether a forecast tells the reader anything."""
    assert ns.is_roofed("バンテリンドーム")
    assert ns.is_roofed("東京ドーム")
    assert ns.is_roofed("京セラD大阪")
    assert ns.is_roofed("エスコンF")      # retractable, normally closed
    assert not ns.is_roofed("横浜")
    assert not ns.is_roofed("ZOZOマリン")
    assert not ns.is_roofed("甲子園")


def test_a_roofed_game_carries_no_forecast():
    games = dict(GAMES)
    games["2021039325"] = games["2021039325"].replace(
        "<body>", "<body>" + WEATHER_HTML + _round_line("東京ドーム"))
    games["2021039329"] = games["2021039329"].replace(
        "<body>", "<body>" + WEATHER_HTML + _round_line("ZOZOマリン"))

    def fetch(url):
        if url.endswith("/schedule/"):
            return SCHEDULE_HTML
        if "weather.yahoo" in url:
            return FORECAST_HTML
        for game_id, html in games.items():
            if game_id in url:
                return html
        raise AssertionError(url)

    slate = ns.fetch_slate("2026-08-25", fetch=fetch)
    assert "巨人" not in slate.weather          # 東京ドーム
    assert slate.weather["ロッテ"].condition    # ZOZOマリン, open to the sky


def test_a_compass_point_becomes_a_bearing():
    assert ns.bearing_of("北") == 0
    assert ns.bearing_of("南") == 180
    assert ns.bearing_of("南南西") == 202.5
    assert ns.bearing_of("東北東") == 67.5
    assert ns.bearing_of("なし") is None


def test_wind_is_read_relative_to_the_way_the_park_faces():
    """Yahoo reports where the wind blows *from*; what matters to a hitter is
    whether it ends up going out to centre or back in at him."""
    # A park whose centre field lies due north of the plate. The arrow reads
    # as the ball flies: up is out towards centre, left is towards left field.
    assert ns.wind_effect("南", 0) == "↑"       # from the south, blowing out
    assert ns.wind_effect("北", 0) == "↓"       # straight in from centre
    assert ns.wind_effect("東", 0) == "←"
    assert ns.wind_effect("西", 0) == "→"
    # Eight points, so a wind between the axes is not forced onto one.
    assert ns.wind_effect("南西", 0) == "↗"
    assert ns.wind_effect("南東", 0) == "↖"
    assert ns.wind_effect("北東", 0) == "↙"
    assert ns.wind_effect("北西", 0) == "↘"


def test_a_wind_between_two_arrows_takes_the_diagonal():
    """Compass points and park bearings both step in 22.5 degrees, so half of
    all winds land exactly between two arrows. Those go to the diagonal: it
    claims less than saying the wind is straight in or straight out."""
    # Centre field due north; 南南西 blows towards 22.5, half a step off.
    assert ns.wind_effect("南南西", 0) == "↗"
    assert ns.wind_effect("南南東", 0) == "↖"
    # ZOZO faces southwest, and a 南南西 there comes in off the left-centre
    # side rather than straight from centre field.
    assert ns.wind_effect("南南西", ns.park_bearing("ZOZOマリン")) == "↘"


def test_wind_is_left_unlabelled_when_the_park_orientation_is_unknown():
    """A wrong 順風 reads as fact; no label reads as no claim."""
    assert ns.wind_effect("南", None) == ""


def test_the_arrow_replaces_the_compass_point_it_was_read_from(monkeypatch):
    """An unrecorded park keeps the raw direction, since there is no arrow to
    stand in for it; a recorded one shows the arrow instead."""
    got = ns.Weather(condition="晴れ", temp_c="28", rain_mm="0",
                     wind="南 4", venue="どこかの地方球場")
    assert got.summary() == "晴れ 28℃ 降水 0mm 南 4m/s"

    monkeypatch.setitem(ns.PARK_BEARINGS, "どこかの地方球場", 0)
    assert got.summary() == "晴れ 28℃ 降水 0mm ↑ 4m/s"


def test_park_bearings_are_matched_by_the_name_yahoo_prints():
    """Yahoo prints short venue names; the official table uses formal ones."""
    assert ns.park_bearing("甲子園") == 180
    assert ns.park_bearing("神宮") == 22.5
    assert ns.park_bearing("楽天モバイル") == 180
    assert ns.park_bearing("東京ドーム") is None       # roofed, never needed
    assert ns.park_bearing("どこかの地方球場") is None


def test_the_hamakaze_at_koshien_blows_right_to_left():
    """甲子園 faces due south, so the batter looks south and right field lies
    west. The 浜風 comes in from there and sweeps across to left — which is the
    whole reason it is talked about, and what a bare "crosswind" would lose.
    """
    koshien = ns.park_bearing("甲子園")
    assert ns.wind_effect("西", koshien) == "←"
    assert ns.wind_effect("東", koshien) == "→"   # the opposite effect
    assert ns.wind_effect("南", koshien) == "↓"
    assert ns.wind_effect("北", koshien) == "↑"


def test_the_seibu_dome_is_treated_as_sheltered_rather_than_roofed():
    """ベルーナドーム has a roof but no walls: rain never reaches the field, and
    yet it is famously an oven in August. Temperature is the part that still
    tells a reader something."""
    # The name Yahoo actually prints, which contains ドーム — the two classes
    # have to be exclusive by construction, not by the order they are asked in.
    assert ns.is_sheltered("ベルーナドーム")
    assert not ns.is_roofed("ベルーナドーム")
    assert not ns.is_sheltered("ZOZOマリン")
    assert not ns.is_sheltered("東京ドーム")


def test_a_sheltered_park_reports_only_the_temperature():
    got = ns.Weather(condition="雨", temp_c="34", rain_mm="5.0",
                     wind="南 4", venue="ベルーナドーム")
    assert got.summary() == "34℃"


def test_an_open_park_still_reports_everything():
    got = ns.Weather(condition="雨", temp_c="34", rain_mm="5.0",
                     wind="南 4", venue="ZOZOマリン")
    assert got.summary() == "☔ 雨 34℃ 降水 5.0mm ↘ 4m/s"


def test_rain_worth_worrying_about_is_flagged():
    """Six games of `降水 0mm` train the eye to skip the line, so the one that
    matters has to break the pattern rather than sit inside it."""
    wet = ns.Weather(condition="雨", temp_c="24", rain_mm="4.5",
                     wind="南 4", venue="ZOZOマリン")
    assert wet.summary().startswith("☔")

    trace = ns.Weather(condition="曇り", temp_c="28", rain_mm="0.1",
                       wind="南 4", venue="ZOZOマリン")
    assert not trace.summary().startswith("☔")   # a trace is not a washout

    dry = ns.Weather(condition="晴れ", temp_c="30", rain_mm="0",
                     wind="南 4", venue="ZOZOマリン")
    assert not dry.summary().startswith("☔")



def test_a_game_already_under_way_yields_no_starters():
    """Once a game starts the page stops being a fixture and becomes a live
    scorecard: 62 player links instead of 2, headed by whoever is batting. The
    two-link shape is what makes a page readable as a fixture at all.
    """
    live = _game_html("2026年8月25日", "中日ドラゴンズ", "阪神タイガース",
                      [("1600001", "細川 成也"), ("1600002", ""),
                       ("1600003", "神宮 僚介"), ("1600004", "福永 裕基")])
    assert ns._parse_game(live, "2026年8月25日") == {}


def test_a_blank_name_is_not_taken_as_a_starter():
    blank = _game_html("2026年8月25日", "中日ドラゴンズ", "阪神タイガース",
                       [("1600001", ""), ("1600002", "西 勇輝")])
    assert ns._parse_game(blank, "2026年8月25日") == {}


def test_the_forecast_survives_a_page_with_no_readable_starters():
    """Weather does not depend on the starters being announced, or on the game
    not having begun — it is printed on the page either way."""
    live = _game_html("2026年8月25日", "千葉ロッテマリーンズ",
                      "福岡ソフトバンクホークス",
                      [("1", "打者A"), ("2", ""), ("3", "打者B")])
    live = live.replace("<body>", "<body>" + WEATHER_HTML + _round_line("ZOZOマリン"))

    def fetch(url):
        if url.endswith("/schedule/"):
            return SCHEDULE_HTML
        if "weather.yahoo" in url:
            return FORECAST_HTML
        return live

    slate = ns.fetch_slate("2026-08-25", fetch=fetch)
    assert slate.starters == {}
    assert slate.weather["ロッテ"].condition == "曇り"


def test_venues_are_matched_through_their_full_width_spelling():
    """Yahoo writes the same ground both ways — 京セラＤ大阪 in 514 of the games
    on record and 京セラD大阪 in 137 — so plain substring matching silently
    lets one spelling through.
    """
    assert ns.is_roofed("京セラＤ大阪") and ns.is_roofed("京セラD大阪")
    assert ns.is_roofed("エスコンＦ") and ns.is_roofed("エスコンF")
    assert ns.park_bearing("ＺＯＺＯマリン") == ns.park_bearing("ZOZOマリン") == 225


def test_the_softbank_dome_is_recognised_by_the_name_yahoo_prints():
    """It appears as みずほPayPay — no ドーム, no D — in 580 recorded games."""
    assert ns.is_roofed("みずほPayPay")


def test_the_regional_grounds_npb_publishes_are_known():
    assert ns.park_bearing("松山") == 180      # 松山坊っちゃんスタジアム
    assert ns.park_bearing("倉敷") == 180      # 倉敷マスカットスタジアム
    assert ns.park_bearing("ほっと神戸") is None   # not published anywhere found
