"""Unit tests for the weather log (baseball/npb_weather.py).

Yahoo's pinpoint forecast only ever covers today — a request for yesterday
comes back empty — so a reading not recorded on the day is gone. Everything
here exists to make sure the daily sweep keeps one clean row per game.
"""

from baseball import npb_weather as nw
from baseball.npb_starters import Slate, Weather


def _slate(*games):
    """A Slate as fetch_slate builds it: both teams point at one forecast."""
    weather = {}
    for teams, w in games:
        for team in teams:
            weather[team] = w
    return Slate(starters={}, weather=weather)


def _weather(venue, **over):
    fields = dict(condition="晴", temp_c="24", rain_mm="0", wind="南 4",
                  venue=venue)
    fields.update(over)
    return Weather(**fields)


C = {name: i for i, name in enumerate(nw.COLUMNS)}


def test_both_teams_of_a_game_yield_one_row():
    slate = _slate(((["DeNA", "巨人"]), _weather("横浜")))

    rows = nw.weather_rows(slate, "2026-09-10", "2026-09-10 17:30")

    assert len(rows) == 1
    assert rows[0][C["球場"]] == "横浜"


def test_a_row_per_game_when_several_are_played():
    slate = _slate((["DeNA", "巨人"], _weather("横浜")),
                   (["阪神", "広島"], _weather("甲子園")))

    rows = nw.weather_rows(slate, "2026-09-10", "2026-09-10 17:30")

    assert {r[C["球場"]] for r in rows} == {"横浜", "甲子園"}


def test_the_reading_is_recorded_as_given():
    slate = _slate((["DeNA", "巨人"],
                    _weather("横浜", condition="雨", temp_c="20",
                             rain_mm="1", wind="北 3")))

    row = nw.weather_rows(slate, "2026-09-10", "2026-09-10 17:30")[0]

    assert row[C["日期"]] == "2026-09-10"
    assert row[C["天候"]] == "雨"
    assert row[C["氣溫"]] == 20.0
    assert row[C["降水mm"]] == 1.0
    assert row[C["風向"]] == "北"
    assert row[C["風速"]] == 3.0
    assert row[C["抓取時間"]] == "2026-09-10 17:30"


# A compass point means nothing until it is read against the park's bearing,
# which is what npb_starters.wind_effect already works out for the broadcast.
def test_wind_is_resolved_against_the_park():
    slate = _slate((["DeNA", "巨人"], _weather("横浜", wind="南 6")))

    row = nw.weather_rows(slate, "2026-09-10", "2026-09-10 17:30")[0]

    assert row[C["對球場"]] != ""


# Six of the twelve parks are covered. Their rows are still worth keeping —
# temperature is real indoors — but anything downstream has to know not to read
# rain or wind there, and the flag is cheaper than re-deriving it every time.
def test_a_covered_park_is_flagged():
    slate = _slate((["巨人", "中日"], _weather("東京ドーム")),
                   (["阪神", "広島"], _weather("甲子園")))

    rows = {r[C["球場"]]: r for r in
            nw.weather_rows(slate, "2026-09-10", "2026-09-10 17:30")}

    assert rows["東京ドーム"][C["屋頂"]] == "有"
    assert rows["甲子園"][C["屋頂"]] == ""


def test_missing_numbers_stay_blank_rather_than_becoming_zero():
    slate = _slate((["DeNA", "巨人"],
                    _weather("横浜", temp_c=None, rain_mm=None, wind=None)))

    row = nw.weather_rows(slate, "2026-09-10", "2026-09-10 17:30")[0]

    assert row[C["氣溫"]] == ""
    assert row[C["降水mm"]] == ""
    assert row[C["風向"]] == ""
    assert row[C["風速"]] == ""


def test_a_slate_with_no_weather_yields_nothing():
    assert nw.weather_rows(Slate(starters={}, weather={}),
                           "2026-09-10", "2026-09-10 17:30") == []


# The sweep runs every 30 minutes, so the same game is read a dozen times a
# day. The last reading before first pitch is the one worth keeping, and a
# dozen near-identical rows would only make the tab harder to join against.
def test_a_second_reading_replaces_the_first_for_that_game():
    existing = [
        ["2026-09-10", "横浜", "", "曇", 22.0, 0.0, "南", 4.0, "→", "17:00"],
        ["2026-09-10", "甲子園", "", "晴", 26.0, 0.0, "西", 2.0, "←", "17:00"],
    ]
    fresh = [["2026-09-10", "横浜", "", "雨", 20.0, 1.0, "北", 3.0, "↑", "18:00"]]

    merged = nw.merge_rows(existing, fresh)

    assert len(merged) == 2
    yokohama = [r for r in merged if r[C["球場"]] == "横浜"][0]
    assert yokohama[C["天候"]] == "雨"
    assert yokohama[C["抓取時間"]] == "18:00"


def test_a_different_day_at_the_same_park_is_a_new_row():
    existing = [["2026-09-10", "横浜", "", "曇", 22.0, 0.0, "南", 4.0, "→", "17:00"]]
    fresh = [["2026-09-11", "横浜", "", "晴", 25.0, 0.0, "南", 2.0, "→", "17:00"]]

    merged = nw.merge_rows(existing, fresh)

    assert len(merged) == 2


def test_merged_rows_stay_in_date_then_park_order():
    existing = [["2026-09-11", "甲子園", "", "晴", 26.0, 0.0, "西", 2.0, "←", "17:00"]]
    fresh = [["2026-09-10", "横浜", "", "雨", 20.0, 1.0, "北", 3.0, "↑", "18:00"]]

    merged = nw.merge_rows(existing, fresh)

    assert [r[C["日期"]] for r in merged] == ["2026-09-10", "2026-09-11"]
