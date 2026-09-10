"""Read a game's forecast from tenki.jp instead of Yahoo.

Yahoo's pinpoint page steps three hours at a time, so an 18:00 first pitch has
to be approximated from the 15時 or 18時 column. tenki.jp (日本気象協会) gives
every hour, and covers the day after tomorrow as well, which Yahoo does not.

The cost is that Yahoo's game page links to the right forecast by itself while
tenki.jp does not, so the grounds have to be mapped by hand — tenki.jp knows
municipalities, not ballparks. The twelve home parks are here, plus Orix's
second ground at Kobe; a 地方球場 rotates from season to season and will always be
missing some, which is why `forecast_url` returns None rather than guessing —
the caller falls back to Yahoo rather than losing the sky.

Every row of the hourly table carries a semantic class (`hour`, `weather`,
`temperature`, `precipitation`, `wind-blow`, `wind-speed`), so nothing here
depends on counting rows or columns.
"""

import re

from bs4 import BeautifulSoup

BASE_URL = "https://tenki.jp"

# Short ground name -> the city tenki.jp forecasts it under. Matched by
# substring against the folded venue string, the way park_bearing is, because
# the venue comes off Yahoo's page in more than one spelling.
PARK_PATHS: dict[str, str] = {
    # Keyed on the ward the ground actually stands in, not the city. Yokohama
    # is 437km² and its ground is on the harbour; Chiba's is on the bay. A
    # city-wide reading would average the coast away, and the wind off the
    # water is exactly what the arrow is for.
    "エスコン": "/forecast/1/2/1400/1234/",        # 北海道北広島市
    "楽天モバイル": "/forecast/2/7/3410/4102/",     # 仙台市宮城野区
    "ベルーナ": "/forecast/3/14/4310/11208/",      # 埼玉県所沢市
    "ZOZOマリン": "/forecast/3/15/4510/12106/",    # 千葉市美浜区
    "東京ドーム": "/forecast/3/16/4410/13105/",     # 東京都文京区
    "神宮": "/forecast/3/16/4410/13104/",          # 東京都新宿区
    "横浜": "/forecast/3/17/4610/14104/",          # 横浜市中区
    "バンテリン": "/forecast/5/26/5110/23102/",     # 名古屋市東区
    "京セラD": "/forecast/6/30/6200/27106/",       # 大阪市西区
    "甲子園": "/forecast/6/31/6310/28204/",        # 兵庫県西宮市
    "神戸": "/forecast/6/31/6310/28107/",          # 神戸市須磨区 (ほっともっと)
    "マツダ": "/forecast/7/37/6710/34103/",        # 広島市南区
    "PayPay": "/forecast/9/43/8210/40133/",       # 福岡市中央区
}

# The 地方球場 NPB has actually used. Each is keyed on the city it stands in,
# not the ward: these grounds see single figures of games a decade, city-level
# precision is ample for them, and ward names collide nationwide — 中央区 alone
# would resolve to Tokyo's. Substring matching covers the variants the sheet
# holds: ハードオフ新潟 finds 新潟, 山形市 finds 山形, 舊盛岡 finds 盛岡.
REGIONAL_PATHS: dict[str, str] = {
    "旭川": "/forecast/1/1/1200/1204/",           # 北海道旭川市
    "帯広": "/forecast/1/3/2000/1207/",           # 北海道帯広市
    "釧路": "/forecast/1/3/1900/1206/",           # 北海道釧路市
    "函館": "/forecast/1/4/2300/1202/",           # 北海道函館市
    "弘前": "/forecast/2/5/3110/2202/",           # 青森県弘前市
    "盛岡": "/forecast/2/6/3310/3201/",           # 岩手県盛岡市
    "秋田": "/forecast/2/8/3210/5201/",           # 秋田県秋田市
    "山形": "/forecast/2/9/3510/6201/",           # 山形県山形市
    "福島": "/forecast/2/10/3610/7201/",          # 福島県福島市
    "郡山": "/forecast/2/10/3610/7203/",          # 福島県郡山市
    "いわき": "/forecast/2/10/3620/7204/",         # 福島県いわき市
    "ひたちなか": "/forecast/3/11/4010/8221/",      # 茨城県ひたちなか市
    "宇都宮": "/forecast/3/12/4110/9201/",         # 栃木県宇都宮市
    "前橋": "/forecast/3/13/4210/10201/",         # 群馬県前橋市
    "大宮": "/forecast/3/14/4310/11100/",         # 埼玉県さいたま市
    "長野": "/forecast/3/23/4810/20201/",         # 長野県長野市
    "松本": "/forecast/3/23/4820/20202/",         # 長野県松本市
    "新潟": "/forecast/4/18/5410/15100/",         # 新潟県新潟市
    "富山": "/forecast/4/19/5510/16201/",         # 富山県富山市
    "金沢": "/forecast/4/20/5610/17201/",         # 石川県金沢市
    "福井": "/forecast/4/21/5710/18201/",         # 福井県福井市
    "岐阜": "/forecast/5/24/5210/21201/",         # 岐阜県岐阜市
    "静岡": "/forecast/5/25/5010/22100/",         # 静岡県静岡市
    "浜松": "/forecast/5/25/5040/22130/",         # 静岡県浜松市
    "豊橋": "/forecast/5/26/5120/23201/",         # 愛知県豊橋市
    "京都": "/forecast/6/29/6110/26100/",         # 京都府京都市
    "倉敷": "/forecast/7/36/6610/33202/",         # 岡山県倉敷市
    "呉": "/forecast/7/37/6710/34202/",           # 広島県呉市
    "尾道": "/forecast/7/37/6710/34205/",         # 広島県尾道市
    "三次": "/forecast/7/37/6720/34209/",         # 広島県三次市
    "松山": "/forecast/8/41/7310/38201/",         # 愛媛県松山市
    "北九州": "/forecast/9/43/8220/40100/",        # 福岡県北九州市
    "佐賀": "/forecast/9/44/8510/41201/",         # 佐賀県佐賀市
    "長崎": "/forecast/9/45/8410/42201/",         # 長崎県長崎市
    "熊本": "/forecast/9/46/8610/43100/",         # 熊本県熊本市
    "宮崎": "/forecast/9/48/8710/45201/",         # 宮崎県宮崎市
    "鹿児島": "/forecast/9/49/8810/46201/",        # 鹿児島県鹿児島市
    "那覇": "/forecast/10/50/9110/47201/",        # 沖縄県那覇市
}


# Row classes in the hourly table, and the Weather field each one fills.
_WEATHER_ROW = "weather"
_TEMP_ROW = "temperature"
_PRECIP_ROW = "precipitation"
_BLOW_ROW = "wind-blow"
_SPEED_ROW = "wind-speed"
_HOUR_ROW = "hour"

_DATE = re.compile(r"(\d{1,2})月(\d{1,2})日")


def _fold(venue: str) -> str:
    """One ground, one spelling: full-width letters folded, padding dropped.

    賽錄 holds 岐　阜 and 郡　山 with an ideographic space between the
    characters, which no amount of substring matching would otherwise reach.
    """
    return "".join(
        chr(ord(char) - 0xFEE0) if 0xFF01 <= ord(char) <= 0xFF5E else char
        for char in (venue or "")
        if char not in ("\u3000", " ")
    )


def forecast_url(venue: str) -> str | None:
    """The hourly forecast page for a ground, or None if it is not mapped."""
    folded = _fold(venue)
    if not folded:
        return None
    # Home parks first: a 地方球場 key is short enough to sit inside another
    # ground's name, and the park a club plays 70 times a year should win.
    for table in (PARK_PATHS, REGIONAL_PATHS):
        for name, path in table.items():
            if _fold(name) in folded:
                return f"{BASE_URL}{path}1hour.html"
    return None


def _cells(row) -> list[str]:
    return [cell.get_text(" ", strip=True).replace("\xa0", " ")
            for cell in row.find_all("td")]


def _row_of(table, css_class):
    return table.find("tr", class_=css_class)


def _column_for(table, hour: int) -> int | None:
    """Which column of the table holds `hour`, by reading the hour row."""
    row = _row_of(table, _HOUR_ROW)
    if row is None:
        return None
    hours = []
    for index, text in enumerate(_cells(row)):
        digits = text.strip()
        if digits.isdigit():
            hours.append((index, int(digits)))
    if not hours:
        return None
    return min(hours, key=lambda pair: abs(pair[1] - hour))[0]


def _value(table, css_class, column: int) -> str | None:
    row = _row_of(table, css_class)
    if row is None:
        return None
    cells = _cells(row)
    if column >= len(cells):
        return None
    value = cells[column].strip()
    return value or None


def _table_for(soup, game_date: str):
    """The day's table, matched on the date its heading names."""
    _, month, day = (int(part) for part in game_date.split("-"))
    for table in soup.select("table.forecast-point-1h"):
        head = _row_of(table, "head")
        if head is None:
            continue
        found = _DATE.search(head.get_text(" ", strip=True))
        if found and (int(found.group(1)), int(found.group(2))) == (month, day):
            return table
    return None


def parse_forecast(html: str, game_date: str, hour: int):
    """The forecast for one game, or None when the day is not on the page.

    Returns a `npb_starters.Weather`, so every reader downstream — the
    broadcast's summary line, the wind arrow, the 天氣 log — is unchanged.
    """
    from baseball.npb_starters import Weather

    soup = BeautifulSoup(html, "html.parser")
    table = _table_for(soup, game_date)
    if table is None:
        return None
    column = _column_for(table, hour)
    if column is None:
        return None

    condition = _value(table, _WEATHER_ROW, column)
    if not condition:
        return None

    blow = _value(table, _BLOW_ROW, column)
    speed = _value(table, _SPEED_ROW, column)
    # Weather keeps the wind as one string so summary() can split it back into
    # a compass point and a speed, which is the shape the Yahoo reader gave.
    wind = f"{blow} {speed}" if blow and speed else (blow or None)

    return Weather(
        condition=condition,
        temp_c=_value(table, _TEMP_ROW, column),
        rain_mm=_value(table, _PRECIP_ROW, column),
        wind=wind,
    )
