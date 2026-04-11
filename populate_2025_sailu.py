"""
One-off script: scrape all finished 2025 NPB games from NPB.jp and write them to
the analysis spreadsheet.

- Regular-season / postseason games are written into the 賽錄 sheet's placeholder
  rows (columns B–AY only; AZ onwards remain formula-driven).
- Open-sen / exhibition games are appended to 熱身賽紀錄.

This script does not depend on Yahoo's 2025 schedule pages. It discovers game
pages via official NPB.jp daily index pages and parses the official boxscore
pages directly.
"""
from __future__ import annotations

import asyncio
import re
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup as bs

from npb import (
    EXHIBITION_SHEET_NAME,
    MAX_CONCURRENT,
    NPB_TEAMS,
    SAILU_SHEET_NAME,
    SAILU_SPREADSHEET_KEY,
    _exhibition_row,
    _sailu_row,
    display_team_name,
    get_worksheet,
)

BASE_URL = "https://npb.jp"
YEAR = 2025

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

TEAM_NAME_MAP = {
    "読売": "巨人",
    "巨人": "巨人",
    "読 売": "巨人",
    "東京ヤクルト": "ヤクルト",
    "ヤクルト": "ヤクルト",
    "横浜DeNA": "DeNA",
    "DeNA": "DeNA",
    "中日": "中日",
    "阪神": "阪神",
    "広島東洋": "広島",
    "広島": "広島",
    "埼玉西武": "西武",
    "西武": "西武",
    "北海道日本ハム": "日本ハム",
    "日本ハム": "日本ハム",
    "千葉ロッテ": "ロッテ",
    "ロッテ": "ロッテ",
    "オリックス": "オリックス",
    "福岡ソフトバンク": "ソフトバンク",
    "ソフトバンク": "ソフトバンク",
    "東北楽天": "楽天",
    "楽天": "楽天",
}

SCHEDULE_DETAIL_PATHS = [
    "/preseason/2025/schedule_detail.html",
    "/games/2025/schedule_03_detail.html",
    "/games/2025/schedule_04_detail.html",
    "/games/2025/schedule_05_detail.html",
    "/games/2025/schedule_06_detail.html",
    "/games/2025/schedule_07_detail.html",
    "/games/2025/schedule_08_detail.html",
    "/games/2025/schedule_09_detail.html",
    "/games/2025/schedule_10_detail.html",
    "/interleague/2025/schedule_detail.html",
    "/climax/2025/schedule_detail.html",
    "/nippons/2025/schedule_detail.html",
]


async def _fetch(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url) as res:
            if res.status != 200:
                return None
            return await res.text(encoding="utf-8", errors="ignore")
    except Exception:
        return None


def _official_to_raw_team(name: str) -> str:
    norm = re.sub(r"\s+", "", name)
    for official, raw in TEAM_NAME_MAP.items():
        if official in norm:
            return raw
    raise ValueError(f"Unknown official team name: {name}")


def _player_page_to_name_and_hand(html: str) -> tuple[str, str]:
    soup = bs(html, "html.parser")
    title = soup.title.text.strip() if soup.title else ""
    full_name = title.split("（", 1)[0].strip() if "（" in title else title
    hand = ""
    for tr in soup.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if len(cells) >= 2 and cells[0] == "投打":
            hand = cells[1][:1]
            break
    return full_name, hand


def _parse_ip(whole: str, frac: str) -> str:
    whole = whole.strip()
    frac = frac.strip()
    if frac in ("", "+"):
        return whole or "0"
    return f"{whole}{frac}"


def _ip_to_outs(ip: str) -> int:
    try:
        parts = str(ip).split(".")
        return int(parts[0]) * 3 + (int(parts[1]) if len(parts) > 1 else 0)
    except Exception:
        return 0


def _parse_innings(score_row) -> tuple[list[str], int, int, int]:
    vals = [td.get_text(strip=True) for td in score_row.find_all("td", class_="gmscore")]
    if len(vals) < 4:
        raise ValueError("Unexpected scoreboard row format")
    total_r, total_h, total_e = map(int, vals[-3:])
    innings_raw = vals[:-3]
    while innings_raw and innings_raw[-1] == "-":
        innings_raw.pop()

    innings = []
    for v in innings_raw[:12]:
        cleaned = re.sub(r"[Xx]+$", "", v)
        innings.append(cleaned if cleaned != "-" else "")
    innings.extend([""] * (12 - len(innings)))
    return innings[:12], total_r, total_h, total_e


def _extract_pitchers(stats_table) -> tuple[list[list[str]], list[list[str]]]:
    rows = stats_table.find_all("tr")
    pitcher_header = None
    for idx, tr in enumerate(rows):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if "投 回" in "".join(cells) and "打 者" in "".join(cells):
            pitcher_header = idx
            break

    if pitcher_header is None:
        raise ValueError("Could not locate pitcher tables")

    parsed_rows = [[td.get_text(" ", strip=True) for td in tr.find_all("td")] for tr in rows[pitcher_header + 1 :]]
    separators = [idx for idx, cells in enumerate(parsed_rows) if not cells]
    if len(separators) < 2:
        raise ValueError("Pitcher separators missing")

    away_rows = [cells[:10] for cells in parsed_rows[separators[0] + 1 : separators[1]] if len(cells) >= 10]
    home_rows = [cells[:10] for cells in parsed_rows[separators[1] + 1 :] if len(cells) >= 10]
    if not away_rows or not home_rows:
        raise ValueError("Pitcher blocks missing")
    return away_rows, home_rows


def _starter_info(pitch_rows: list[list[str]]) -> tuple[str, str, int, int]:
    if not pitch_rows:
        return "", "", 0, 0
    row = pitch_rows[0]
    name = row[1].strip()
    ip = _parse_ip(row[2], row[3])
    try:
        er = int(row[9])
    except ValueError:
        er = 0
    outs = _ip_to_outs(ip)
    qs = 1 if (
        (outs >= 21 and er <= 3)
        or (outs >= 18 and er <= 2)
        or (outs >= 15 and er <= 1)
    ) else 0
    return name, ip, er, qs


def parse_official_game(game_id: str, html: str) -> dict:
    soup = bs(html, "html.parser")
    title = soup.title.text.strip()
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", title)
    if not m:
        raise ValueError(f"Could not parse date: {game_id}")
    game_date = f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    score_tables = [
        table for table in soup.find_all("table")
        if len(table.find_all("td", class_="gmscoreteam")) >= 2
    ]
    if not score_tables:
        raise ValueError(f"Score table not found: {game_id}")
    score_table = score_tables[-1]

    score_rows = score_table.find_all("tr")
    away_row = score_rows[1]
    home_row = score_rows[2]
    away_team = _official_to_raw_team(away_row.find("td", class_="gmscoreteam").get_text(" ", strip=True))
    home_team = _official_to_raw_team(home_row.find("td", class_="gmscoreteam").get_text(" ", strip=True))

    away_innings, away_r, away_h, away_e = _parse_innings(away_row)
    home_innings, home_r, home_h, home_e = _parse_innings(home_row)

    meta_table = score_table.find_previous("table")
    meta_text = meta_table.get_text(" ", strip=True) if meta_table else ""
    venue = meta_table.find("td").get_text(" ", strip=True) if meta_table else ""
    start_time = ""
    tm = re.search(r"開始\s*(\d{1,2}:\d{2})", meta_text)
    if tm:
        start_time = tm.group(1)

    stats_table = None
    for table in soup.find_all("table"):
        if "打 数" in table.get_text(" ", strip=True) and "投 回" in table.get_text(" ", strip=True):
            stats_table = table
            break
    if stats_table is None:
        raise ValueError(f"Stats table not found: {game_id}")

    away_pitch_rows, home_pitch_rows = _extract_pitchers(stats_table)
    away_starter, away_ip, away_er, away_qs = _starter_info(away_pitch_rows)
    home_starter, home_ip, home_er, home_qs = _starter_info(home_pitch_rows)

    return {
        "賽事編號": game_id,
        "客場隊伍": away_team,
        "客場先發": away_starter,
        "主場隊伍": home_team,
        "主場先發": home_starter,
        "時間": start_time,
        "球場": venue,
        "主審": "",
        "away_innings": away_innings,
        "home_innings": home_innings,
        "客總分": away_r,
        "客安打": away_h,
        "客失誤": away_e,
        "主總": home_r,
        "主安打": home_h,
        "主失誤": home_e,
        "賽事狀態": "正常",
        "日期": game_date,
        "客隊代號": NPB_TEAMS[away_team]["id"],
        "主隊代號": NPB_TEAMS[home_team]["id"],
        "客投別": "",
        "主投別": "",
        "客投局": away_ip,
        "主投局": home_ip,
        "客責失": away_er,
        "客QS": away_qs,
        "主責失": home_er,
        "主QS": home_qs,
    }


async def get_daily_game_ids(day: date, session: aiohttp.ClientSession, prefix: str) -> set[str]:
    page_name = "opgm" if prefix == "ops" else "gm"
    url = f"{BASE_URL}/bis/{YEAR}/games/{page_name}{day.strftime('%Y%m%d')}.html"
    html = await _fetch(session, url)
    if not html:
        return set()
    matches = re.findall(rf"{prefix}{YEAR}\d+\.html", html)
    return {m[:-5] for m in matches}


async def build_score_url_map(session: aiohttp.ClientSession) -> dict[tuple[str, frozenset[str]], str]:
    mapping: dict[tuple[str, frozenset[str]], str] = {}
    for path in SCHEDULE_DETAIL_PATHS:
        html = await _fetch(session, urljoin(BASE_URL, path))
        if not html:
            continue
        soup = bs(html, "html.parser")
        current_date = ""
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            if th and re.search(r"\d+/\d+（", th.get_text(" ", strip=True)):
                current_date = th.get_text(" ", strip=True)
            score_link = None
            for a in tr.find_all("a", href=True):
                if "/scores/2025/" in a["href"]:
                    score_link = a
                    break
            team1 = tr.find("div", class_="team1")
            team2 = tr.find("div", class_="team2")
            if not current_date or not score_link or not team1 or not team2:
                continue
            href = score_link["href"]
            m = re.match(r"(\d{1,2})/(\d{1,2})", current_date)
            if not m:
                continue
            game_date = f"{YEAR}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
            try:
                team_a = _official_to_raw_team(team1.get_text(" ", strip=True))
                team_b = _official_to_raw_team(team2.get_text(" ", strip=True))
            except ValueError:
                continue
            key = (game_date, frozenset([team_a, team_b]))
            mapping[key] = urljoin(BASE_URL, href)
    return mapping


async def enrich_with_score_page(
    data: dict,
    session: aiohttp.ClientSession,
    score_url_map: dict[tuple[str, frozenset[str]], str],
    player_cache: dict[str, tuple[str, str]],
):
    key = (data["日期"], frozenset([data["客場隊伍"], data["主場隊伍"]]))
    score_url = score_url_map.get(key)
    if not score_url:
        return

    html = await _fetch(session, score_url)
    if not html:
        return
    soup = bs(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"球審：\s*([^\s、]+)", text)
    if m:
        data["主審"] = m.group(1)
    away_raw = data["客場隊伍"]
    home_raw = data["主場隊伍"]

    pitch_rows = {}
    for tr in soup.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = th.get_text(" ", strip=True)
        if not label.startswith("【") or "】" not in label:
            continue
        if "‐" not in td.get_text(" ", strip=True) and "-" not in td.get_text(" ", strip=True):
            continue
        team_label = label[1:].split("】", 1)[0]
        links = td.find_all("a", href=True)
        player_links = [a for a in links if "/bis/players/" in a["href"]]
        if player_links:
            try:
                pitch_rows[_official_to_raw_team(team_label)] = player_links
            except ValueError:
                continue

    for side, raw_team in (("客", away_raw), ("主", home_raw)):
        links = pitch_rows.get(raw_team)
        if not links:
            continue
        starter = links[0]
        player_url = urljoin(BASE_URL, starter["href"])
        if player_url not in player_cache:
            player_html = await _fetch(session, player_url)
            if not player_html:
                continue
            player_cache[player_url] = _player_page_to_name_and_hand(player_html)
        full_name, hand = player_cache[player_url]
        data[f"{side}場先發"] = full_name
        data[f"{side}投別"] = hand


def _existing_exhibition_identities(sheet) -> set[tuple[str, str, str]]:
    rows = sheet.get_all_values()[1:]
    identities = set()
    for row in rows:
        if len(row) < 6 or not row[0]:
            continue
        try:
            dt = datetime.strptime(row[0], "%Y/%m/%d").strftime("%Y-%m-%d")
        except ValueError:
            continue
        identities.add((dt, row[2], row[5]))
    return identities


def _exhibition_identity(data: dict) -> tuple[str, str, str]:
    return (
        data["日期"],
        display_team_name(data["客場隊伍"]),
        display_team_name(data["主場隊伍"]),
    )


async def main():
    sailu_sheet = get_worksheet(SAILU_SHEET_NAME, SAILU_SPREADSHEET_KEY)
    exhibition_sheet = get_worksheet(EXHIBITION_SHEET_NAME, SAILU_SPREADSHEET_KEY)

    sailu_col_a = sailu_sheet.col_values(1)[1:]
    sailu_col_b = sailu_sheet.col_values(2)[1:]
    gid_to_row = {gid: idx + 2 for idx, gid in enumerate(sailu_col_b) if gid}
    existing_regular_ids = {v for v in sailu_col_b if v}
    existing_exhibition = _existing_exhibition_identities(exhibition_sheet)

    placeholder_rows = [
        i + 2
        for i, a in enumerate(sailu_col_a)
        if a and not (sailu_col_b[i] if i < len(sailu_col_b) else "")
    ]
    print(f"[2025] sailu placeholder row(s): {len(placeholder_rows)}")

    all_regular_ids: set[str] = set()
    all_exhibition_ids: set[str] = set()

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        score_url_map = await build_score_url_map(session)
        player_cache: dict[str, tuple[str, str]] = {}
        days = []
        current = date(YEAR, 1, 1)
        end = date(YEAR, 12, 31)
        while current <= end:
            days.append(current)
            current += timedelta(days=1)

        for i in range(0, len(days), MAX_CONCURRENT):
            batch = days[i : i + MAX_CONCURRENT]
            regular_results = await asyncio.gather(
                *[get_daily_game_ids(day, session, "s") for day in batch],
                return_exceptions=True,
            )
            exhibition_results = await asyncio.gather(
                *[get_daily_game_ids(day, session, "ops") for day in batch],
                return_exceptions=True,
            )
            for result in regular_results:
                if isinstance(result, set):
                    all_regular_ids.update(result)
            for result in exhibition_results:
                if isinstance(result, set):
                    all_exhibition_ids.update(result)

        print(f"[2025] discovered {len(all_regular_ids)} regular/postseason game(s)")
        print(f"[2025] discovered {len(all_exhibition_ids)} exhibition game(s)")

        target_regular_ids = sorted(gid for gid in all_regular_ids if gid not in existing_regular_ids)
        target_exhibition_ids = sorted(all_exhibition_ids)
        print(f"[2025] new regular game(s): {len(target_regular_ids)}")
        print(f"[2025] exhibition game(s) to inspect: {len(target_exhibition_ids)}")

        scraped_regular: list[tuple[str, dict]] = []
        scraped_exhibition: list[tuple[str, dict]] = []
        all_target_ids = [(gid, "regular") for gid in target_regular_ids] + [
            (gid, "exhibition") for gid in target_exhibition_ids
        ]

        for i in range(0, len(all_target_ids), MAX_CONCURRENT):
            batch = all_target_ids[i : i + MAX_CONCURRENT]
            html_results = await asyncio.gather(
                *[_fetch(session, f"{BASE_URL}/bis/{YEAR}/games/{gid}.html") for gid, _ in batch],
                return_exceptions=True,
            )
            for (gid, kind), html in zip(batch, html_results):
                if isinstance(html, Exception) or not html:
                    print(f"  [skip] {gid} — could not fetch")
                    continue
                try:
                    data = parse_official_game(gid, html)
                    await enrich_with_score_page(data, session, score_url_map, player_cache)
                except Exception as e:
                    print(f"  [error] {gid}: {e}")
                    continue
                if kind == "regular":
                    scraped_regular.append((gid, data))
                else:
                    ident = _exhibition_identity(data)
                    if ident not in existing_exhibition:
                        scraped_exhibition.append((gid, data))
                        existing_exhibition.add(ident)

        scraped_regular.sort(key=lambda x: (x[1]["日期"], x[0]))
        scraped_exhibition.sort(key=lambda x: (x[1]["日期"], x[0]))

        # Overwrite existing 2025 regular rows to backfill fields like umpire,
        # full starter names, and pitcher handedness.
        overwrite_regular_ids = sorted(gid for gid in existing_regular_ids if gid.startswith("s2025"))
        overwrite_updates = []
        for i in range(0, len(overwrite_regular_ids), MAX_CONCURRENT):
            batch = overwrite_regular_ids[i : i + MAX_CONCURRENT]
            html_results = await asyncio.gather(
                *[_fetch(session, f"{BASE_URL}/bis/{YEAR}/games/{gid}.html") for gid in batch],
                return_exceptions=True,
            )
            for gid, html in zip(batch, html_results):
                if isinstance(html, Exception) or not html:
                    continue
                try:
                    data = parse_official_game(gid, html)
                    await enrich_with_score_page(data, session, score_url_map, player_cache)
                except Exception:
                    continue
                row_num = gid_to_row.get(gid)
                if not row_num:
                    continue
                overwrite_updates.append(
                    {
                        "range": f"B{row_num}:AY{row_num}",
                        "values": [_sailu_row(0, data)[1:]],
                    }
                )

        if scraped_regular:
            if len(scraped_regular) > len(placeholder_rows):
                raise RuntimeError(
                    f"Not enough placeholder rows in 賽錄: need {len(scraped_regular)}, have {len(placeholder_rows)}"
                )
            updates = []
            for (gid, data), row_num in zip(scraped_regular, placeholder_rows):
                updates.append(
                    {
                        "range": f"B{row_num}:AY{row_num}",
                        "values": [_sailu_row(0, data)[1:]],
                    }
                )
                print(f"  [regular] row {row_num} ← {gid}")

            for i in range(0, len(updates), 200):
                sailu_sheet.batch_update(updates[i : i + 200], value_input_option="USER_ENTERED")
            print(f"[2025] wrote {len(scraped_regular)} regular/postseason row(s) into 賽錄")
        else:
            print("[2025] no new regular/postseason rows to write")

        if overwrite_updates:
            for i in range(0, len(overwrite_updates), 200):
                sailu_sheet.batch_update(
                    overwrite_updates[i : i + 200], value_input_option="USER_ENTERED"
                )
            print(f"[2025] refreshed {len(overwrite_updates)} existing regular-season row(s)")

        if scraped_exhibition:
            rows = [_exhibition_row(data) for _, data in scraped_exhibition]
            exhibition_sheet.append_rows(rows, value_input_option="USER_ENTERED", table_range="A:AB")
            print(f"[2025] appended {len(rows)} exhibition row(s) into 熱身賽紀錄")
        else:
            print("[2025] no new exhibition rows to write")


if __name__ == "__main__":
    asyncio.run(main())
