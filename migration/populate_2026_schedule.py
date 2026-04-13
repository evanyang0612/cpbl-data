"""
One-off script: scrape all finished 2026 NPB games and write to 賽程 sheet.

Scans each team's monthly schedule pages to discover finished game IDs, scrapes
full box scores (pitching + batting stats), and appends new rows to the 賽程 sheet
in the NPB spreadsheet.  Already-present game IDs (column A) are skipped.
"""
import asyncio
import re
import aiohttp
from bs4 import BeautifulSoup as bs
from npb import (
    NPB_TEAMS, MAX_CONCURRENT, BASE_URL, NPB_SPREADSHEET_KEY,
    _fetch, get_schedule_game_data, _schedule_row, get_worksheet,
)

SHEET_NAME = "賽程"
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


async def main():
    sheet   = get_worksheet(SHEET_NAME, NPB_SPREADSHEET_KEY)
    col_a   = sheet.col_values(1)[1:]  # 賽事編號, skip header

    # Treat values that look like real game IDs (not "1"–"9" placeholders)
    existing = {v for v in col_a if v and not v.strip().isdigit()}

    # Last 場次 already in the sheet (from column B)
    col_b = sheet.col_values(2)[1:]
    last_seq = 0
    for v in reversed(col_b):
        try:
            last_seq = int(v); break
        except (ValueError, TypeError):
            pass
    print(f"Existing game IDs: {len(existing)}  |  last 場次: {last_seq}")

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

        # ── Step 2: scrape full box scores ────────────────────────────────
        scraped: list[tuple[str, dict]] = []
        for i in range(0, len(new_ids), MAX_CONCURRENT):
            batch = new_ids[i: i + MAX_CONCURRENT]
            results = await asyncio.gather(
                *[get_schedule_game_data(gid, session) for gid in batch],
                return_exceptions=True,
            )
            for gid, data in zip(batch, results):
                if isinstance(data, Exception):
                    print(f"  [error] {gid}: {data}")
                elif data:
                    scraped.append((gid, data))
                    print(f"  OK  {gid}  {data['日期']}  "
                          f"{data['客隊']} {data['客總分']}–{data['主總分']} {data['主隊']}")
                else:
                    print(f"  [skip] {gid} — no data returned")
            if i + MAX_CONCURRENT < len(new_ids):
                await asyncio.sleep(3)

        if not scraped:
            print("No data scraped.")
            return

        # Sort by date then game ID for stable ordering
        scraped.sort(key=lambda x: (x[1]["日期"], x[0]))

        # ── Step 3: append rows ───────────────────────────────────────────
        rows = [
            _schedule_row(last_seq + 1 + i, data)
            for i, (_, data) in enumerate(scraped)
        ]
        sheet.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"\nDone — appended {len(rows)} row(s) to '{SHEET_NAME}'.")


asyncio.run(main())
