"""
One-off script: scrape all finished 2026 NPB games and append to 賽錄副本.

Note: Yahoo Baseball game IDs no longer encode date (e.g. "2021038658" = 2026-04-03).
We discover game IDs by scanning the March and April 2026 schedule pages directly.
"""
import asyncio
import re
import aiohttp
from bs4 import BeautifulSoup as bs
from npb import (
    NPB_TEAMS, MAX_CONCURRENT, BASE_URL,
    SAILU_SPREADSHEET_KEY,
    _fetch, get_sailu_game_data, _sailu_row, get_worksheet,
)

SHEET_NAME = "賽錄副本"
MONTHS_2026 = ["2026-03", "2026-04"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


async def get_2026_game_ids(team_id: int, session: aiohttp.ClientSession) -> set[str]:
    """Scan March and April 2026 schedule pages and return all finished game IDs."""
    ids: set[str] = set()
    for month in MONTHS_2026:
        html = await _fetch(session, f"{BASE_URL}teams/{team_id}/schedule?month={month}")
        if not html:
            continue
        soup = bs(html, "html.parser")
        for entry in soup.find_all(class_="bb-calendarTable__data"):
            status = entry.find(class_="bb-calendarTable__status")
            if not status:
                continue
            if status.text.strip() != "試合終了":
                continue
            href = status.get("href", "")
            m = re.search(r"npb/game/([^/]+)", href)
            if m:
                ids.add(m.group(1))
    return ids


async def main(overwrite: bool = False):
    """
    overwrite=True  → re-scrape all 2026 game IDs already in the sheet and
                       update their rows in-place (fixes bad data from a prior run).
    overwrite=False → normal mode: only append games not yet in the sheet.
    """
    sheet  = get_worksheet(SHEET_NAME, SAILU_SPREADSHEET_KEY)
    col_a  = sheet.col_values(1)[1:]   # 編號
    col_b  = sheet.col_values(2)[1:]   # 賽事編號

    # Build a map of game_id → sheet row number (1-based, data from row 2)
    gid_to_row: dict[str, int] = {
        v: i + 2 for i, v in enumerate(col_b) if v
    }
    existing = set(gid_to_row.keys())

    # Last 編號 already in the sheet
    last_seq = 0
    for v in reversed(col_a):
        try:
            last_seq = int(v); break
        except (ValueError, TypeError):
            pass
    print(f"Last 編號 in sheet: {last_seq}  |  overwrite={overwrite}")

    async with aiohttp.ClientSession(headers=HEADERS) as session:

        if overwrite:
            # In overwrite mode use IDs already in the sheet — skip schedule discovery
            target_ids = sorted(existing)
            print(f"{len(target_ids)} game(s) to overwrite (from sheet)\n")
        else:
            # ── Step 1: collect all 2026 finished game IDs ────────────────────
            all_ids: set[str] = set()
            for key, info in NPB_TEAMS.items():
                ids = await get_2026_game_ids(info["id"], session)
                all_ids.update(ids)
                print(f"  {key}: {len(ids)} finished game(s)")
                await asyncio.sleep(0.5)
            target_ids = sorted(gid for gid in all_ids if gid not in existing)

        if not overwrite:
            print(f"\n{len(target_ids)} new game(s) to add\n")
        if not target_ids:
            print("Nothing to do.")
            return

        # ── Step 2: scrape full box scores ────────────────────────────────
        scraped_games: list[tuple[str, dict]] = []
        for i in range(0, len(target_ids), MAX_CONCURRENT):
            batch = target_ids[i: i + MAX_CONCURRENT]
            results = await asyncio.gather(
                *[get_sailu_game_data(gid, session) for gid in batch],
                return_exceptions=True,
            )
            for gid, data in zip(batch, results):
                if isinstance(data, Exception):
                    print(f"  [error] {gid}: {data}")
                elif data:
                    scraped_games.append((gid, data))
                    score = f"{data['客總分']}–{data['主總']}"
                    print(f"  OK  {gid}  {data['日期']}  {data['客場隊伍']} {score} {data['主場隊伍']}"
                          f"  [{data['客投別'] or '?'}|{data['主投別'] or '?'}]"
                          f"  ump:{data['主審'] or '-'}"
                          f"  time:{data['時間'] or '-'}")
                else:
                    print(f"  [skip] {gid} — no data returned")
            if i + MAX_CONCURRENT < len(target_ids):
                await asyncio.sleep(3)

        if not scraped_games:
            print("No data scraped.")
            return

        scraped_games.sort(key=lambda x: x[1]["日期"] + x[0])

        if overwrite:
            # ── Step 3a: update existing rows in-place ────────────────────
            for i, (gid, data) in enumerate(scraped_games):
                row_num = gid_to_row[gid]
                seq = col_a[row_num - 2]  # keep original 編號
                try:
                    seq = int(seq)
                except (ValueError, TypeError):
                    seq = 0
                row_values = _sailu_row(seq, data)[1:]  # drop col A (編號 unchanged)
                sheet.update(
                    values=[row_values],
                    range_name=f"B{row_num}:AY{row_num}",
                    value_input_option="USER_ENTERED",
                )
                print(f"  [{i+1}/{len(scraped_games)}] wrote row {row_num}  {gid}")
                await asyncio.sleep(1.5)  # stay under Sheets write quota
            print(f"\nDone — overwrote {len(scraped_games)} row(s) in '{SHEET_NAME}'.")
        else:
            # ── Step 3b: append new rows ──────────────────────────────────
            rows = [
                _sailu_row(last_seq + 1 + i, data)
                for i, (_, data) in enumerate(scraped_games)
            ]
            sheet.append_rows(rows, value_input_option="USER_ENTERED")
            print(f"\nDone — appended {len(rows)} row(s) to '{SHEET_NAME}'.")


asyncio.run(main(overwrite=True))
