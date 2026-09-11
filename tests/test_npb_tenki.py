"""Unit tests for the tenki.jp forecast provider (baseball/npb_tenki.py)."""

import pytest

from baseball import npb_tenki as tenki


def _row(css, label, values):
    head = f"<th>{label}</th>" if label is not None else ""
    cells = "".join(f"<td>{v}</td>" for v in values)
    return f'<tr class="{css}">{head}{cells}</tr>'


def _hours(n=24):
    return [f"{h:02d}" for h in range(1, n + 1)]


def _day_table(date_label, *, weather=None, temp=None, precip=None,
               blow=None, speed=None, humidity=None):
    def fill(values, default):
        return values if values is not None else [default] * 24

    return (
        '<table class="forecast-point-1h"><tbody>'
        + _row("head", "日付", [date_label])
        + _row("hour", None, _hours())
        + _row("weather", "天気", fill(weather, "曇り"))
        + _row("temperature", None, fill(temp, "20.0"))
        + _row("precipitation", None, fill(precip, "0"))
        + _row("humidity", "湿度 (%)", fill(humidity, "60"))
        + _row("wind-blow", "風向 風速 (m/s)", fill(blow, "北"))
        + _row("wind-speed", None, fill(speed, "3"))
        + "</tbody></table>"
    )


def _page(*tables):
    return "<html><body>" + "".join(tables) + "</body></html>"


# tenki.jp tabulates today, tomorrow and the day after in three tables of the
# same shape, so the day has to be matched on its heading rather than taken by
# position — the evening broadcast is about tomorrow.
def test_the_right_day_is_picked_out_of_three():
    html = _page(
        _day_table("今日 2026年09月10日( 木 )", weather=["晴"] * 24),
        _day_table("明日 2026年09月11日( 金 )", weather=["雨"] * 24),
        _day_table("明後日 2026年09月12日( 土 )", weather=["雪"] * 24),
    )

    assert tenki.parse_forecast(html, "2026-09-11", 18).condition == "雨"


def test_the_hour_of_first_pitch_is_read():
    weather = ["曇り"] * 24
    weather[17] = "雷雨"          # the 18:00 column
    temp = [f"{10 + h}.0" for h in range(24)]
    html = _page(_day_table("今日 2026年09月10日( 木 )",
                            weather=weather, temp=temp))

    forecast = tenki.parse_forecast(html, "2026-09-10", 18)

    assert forecast.condition == "雷雨"
    assert forecast.temp_c == "27.0"      # hour 18 is the 18th column, index 17


# Yahoo's table steps three hours at a time, so 18:00 had to be approximated
# from the 15時 or 18時 column. tenki.jp has every hour, which is the whole
# reason for the switch.
@pytest.mark.parametrize("hour,expected", [(14, "14"), (18, "18"), (1, "1")])
def test_every_hour_has_its_own_column(hour, expected):
    temp = [f"{h}" for h in range(1, 25)]
    html = _page(_day_table("今日 2026年09月10日( 木 )", temp=temp))

    assert tenki.parse_forecast(html, "2026-09-10", hour).temp_c == expected


# Weather keeps wind as one string so that summary() can split it back apart,
# the same shape the Yahoo reader produced.
def test_wind_is_direction_then_speed():
    blow = ["北"] * 24
    blow[17] = "南南西"
    speed = ["2"] * 24
    speed[17] = "7"
    html = _page(_day_table("今日 2026年09月10日( 木 )", blow=blow, speed=speed))

    assert tenki.parse_forecast(html, "2026-09-10", 18).wind == "南南西 7"


def test_rainfall_is_carried_through():
    precip = ["0"] * 24
    precip[17] = "2.5"
    html = _page(_day_table("今日 2026年09月10日( 木 )", precip=precip))

    assert tenki.parse_forecast(html, "2026-09-10", 18).rain_mm == "2.5"


def test_a_day_that_is_not_on_the_page_reads_as_nothing():
    html = _page(_day_table("今日 2026年09月10日( 木 )"))

    assert tenki.parse_forecast(html, "2026-09-20", 18) is None


def test_a_page_without_the_table_reads_as_nothing():
    assert tenki.parse_forecast("<html><body>?</body></html>",
                                "2026-09-10", 18) is None


# --- the park map ---

# Every ground NPB's twelve clubs call home, plus Orix's second at Kobe, so
# the common case never needs a fallback. Matched the way park_bearing matches,
# on the short name, since the venue string comes off Yahoo's page in more than
# one spelling.
@pytest.mark.parametrize("venue", [
    "東京ドーム", "神宮", "横浜", "バンテリンドーム", "甲子園", "マツダスタジアム",
    "ベルーナドーム", "エスコンフィールド", "ZOZOマリン", "京セラD大阪",
    "みずほPayPayドーム", "楽天モバイルパーク", "ほっと神戸",
])
def test_every_home_park_resolves_to_a_forecast_page(venue):
    url = tenki.forecast_url(venue)

    assert url and url.startswith("https://tenki.jp/forecast/")
    assert url.endswith("/1hour.html")


def test_full_width_spellings_resolve_the_same_way():
    # Yahoo writes this ground both ways; 京セラＤ大阪 appears in 514 of the
    # games on record and 京セラD大阪 in 137.
    assert tenki.forecast_url("京セラＤ大阪") == tenki.forecast_url("京セラD大阪")


# Every 地方球場 NPB has used is mapped, but the list rotates from season to
# season and a new one will always turn up. Saying so lets the caller fall back
# to Yahoo rather than silently losing the sky.
def test_a_ground_nobody_has_played_yet_has_no_page():
    assert tenki.forecast_url("まだ無い球場") is None
    assert tenki.forecast_url("") is None


# Every ground the sheet actually holds, including the spellings it holds them
# in: an ideographic space between the characters, a ハードオフ sponsor prefix,
# a 市 suffix on some rows and not others.
@pytest.mark.parametrize("venue", [
    "那覇", "県営大宮", "北九州", "松山", "静岡", "前橋", "岐阜", "鹿児島",
    "秋田", "旭川", "熊本", "富山", "弘前", "倉敷", "豊橋", "新潟", "山形",
    "帯広", "釧路", "山形市", "長崎", "盛岡", "福島", "舊盛岡", "郡山", "三次",
    "金沢", "長野", "福井", "宮崎", "浜松", "宇都宮", "ハードオフ新潟", "呉",
    "函館", "松本", "ひたちなか", "尾道", "京都", "佐賀", "岐\u3000阜",
    "郡\u3000山", "いわき",
])
def test_every_regional_ground_on_record_resolves(venue):
    assert tenki.forecast_url(venue), venue


# The variants have to land on the same city as the plain spelling, not merely
# resolve to something.
@pytest.mark.parametrize("variant,plain", [
    ("山形市", "山形"), ("舊盛岡", "盛岡"), ("ハードオフ新潟", "新潟"),
    ("岐\u3000阜", "岐阜"), ("郡\u3000山", "郡山"),
])
def test_spelling_variants_land_on_the_same_city(variant, plain):
    assert tenki.forecast_url(variant) == tenki.forecast_url(plain)


# The ward matters, not the city: Yokohama is 437km² and its ground sits on the
# harbour, so a city-wide reading would average the sea breeze away.
def test_grounds_are_keyed_on_their_own_ward():
    assert "14104" in tenki.forecast_url("横浜")        # 横浜市中区
    assert "12106" in tenki.forecast_url("ZOZOマリン")   # 千葉市美浜区
