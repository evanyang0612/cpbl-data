"""
One-off script: scrape all finished 2026 NPB games and write to 賽錄副本 sheet.

Scans each team's monthly schedule pages to discover finished game IDs, scrapes
full box scores (starting pitcher stats), and appends new rows to the 賽錄副本
sheet.  Already-present game IDs (column B) are skipped.

Columns A–AY are written; column AZ onwards are formula-driven and left untouched.
QS formula: ≥7IP & ≤3ER, or ≥6IP & ≤2ER, or ≥5IP & ≤1ER (handled in get_sailu_game_data).
IP format: x.1 / x.2 as read directly from Yahoo Baseball (not decimal).
"""

import asyncio
import re
import aiohttp
from bs4 import BeautifulSoup as bs
from npb import (
    NPB_TEAMS,
    MAX_CONCURRENT,
    BASE_URL,
    _fetch,
    get_sailu_game_data,
    _sailu_row,
    get_worksheet,
)

TARGET_SPREADSHEET_KEY = "1X2oaXk6DJLkx1MPVjc0lgLNtqa88X5qdNdKuKyikrbg"
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
    """Scan MONTHS_2026 schedule pages and return all finished game IDs."""
    ids: set[str] = set()
    for month in MONTHS_2026:
        html = await _fetch(
            session, f"{BASE_URL}teams/{team_id}/schedule?month={month}"
        )
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


async def main():
    sheet = get_worksheet(SHEET_NAME, TARGET_SPREADSHEET_KEY)

    # Column B = 賽事編號 (game ID); column A = 賽事場次 / placeholder seq
    col_b = sheet.col_values(2)[1:]  # skip header
    existing = {v for v in col_b if v}

    col_a = sheet.col_values(1)[1:]
    placeholder_rows: list[int] = [
        i + 2
        for i, a in enumerate(col_a)
        if a and not (col_b[i] if i < len(col_b) else "")
    ]
    print(
        f"Existing game IDs: {len(existing)}  |  placeholder row(s): {len(placeholder_rows)}"
    )

    async with aiohttp.ClientSession(headers=HEADERS) as session:

        # ── Step 1: collect finished game IDs for all teams ───────────────
        all_ids: set[str] = set()
        for key, info in NPB_TEAMS.items():
            ids = await get_2026_game_ids(info["id"], session)
            all_ids.update(ids)
            print(f"  {key}: {len(ids)} finished game(s)")
            await asyncio.sleep(0.5)

        new_ids = sorted(gid for gid in all_ids if gid not in existing)
        print(f"\n{len(new_ids)} new game(s) to add\n")

        if not new_ids:
            print("Nothing to do.")
            return

        # ── Step 2: scrape box scores ─────────────────────────────────────
        scraped: list[tuple[str, dict]] = []
        for i in range(0, len(new_ids), MAX_CONCURRENT):
            batch = new_ids[i : i + MAX_CONCURRENT]
            results = await asyncio.gather(
                *[get_sailu_game_data(gid, session) for gid in batch],
                return_exceptions=True,
            )
            for gid, data in zip(batch, results):
                if isinstance(data, Exception):
                    print(f"  [error] {gid}: {data}")
                elif data:
                    scraped.append((gid, data))
                    print(
                        f"  OK  {gid}  {data['日期']}  "
                        f"{data['客場隊伍']} {data['客總分']}–{data['主總']} {data['主場隊伍']}"
                    )
                else:
                    print(f"  [skip] {gid} — no data returned")
            if i + MAX_CONCURRENT < len(new_ids):
                await asyncio.sleep(3)

        if not scraped:
            print("No data scraped.")
            return

        # Sort by date then game ID for stable ordering
        scraped.sort(key=lambda x: (x[1]["日期"], x[0]))

        if not placeholder_rows:
            print("No placeholder rows available.")
            return

        # ── Step 3: fill B–AY into existing placeholder rows ─────────────
        filled = 0
        for (gid, data), row_num in zip(scraped, placeholder_rows):
            row_values = _sailu_row(0, data)[1:]  # keep column A as-is in the sheet
            sheet.update(
                range_name=f"B{row_num}:AY{row_num}",
                values=[row_values],
                value_input_option="USER_ENTERED",
            )
            filled += 1
            print(f"  wrote row {row_num} ← {gid}")
            await asyncio.sleep(1.5)

        overflow = scraped[len(placeholder_rows) :]
        if overflow:
            print(
                f"\nWARNING: skipped {len(overflow)} game(s) due to missing placeholder rows: "
                + str([gid for gid, _ in overflow])
            )

        print(f"\nDone — filled {filled} row(s) in '{SHEET_NAME}'.")


asyncio.run(main())
