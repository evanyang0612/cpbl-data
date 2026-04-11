"""
Targeted repair script for 2025 賽錄 enrichment fields.

This avoids the heavier full-year importer and only refreshes the known 2025
regular-season row block already written into 賽錄, backfilling:
- full starter names
- 主審
- 客投別 / 主投別

The row block is intentionally narrow to reduce Google Sheets read pressure.
"""
from __future__ import annotations

import asyncio
import time

import aiohttp
import gspread

from npb import get_worksheet
from populate_2025_sailu import (
    BASE_URL,
    YEAR,
    _fetch,
    _sailu_row,
    build_score_url_map,
    enrich_with_score_page,
    parse_official_game,
)

KEY = "1qPdgcy_4s4Dj2xKo0QJawxPRaB6u9sGM3D4avkAjJUw"
SHEET_NAME = "賽錄"
ROW_START = 10480
ROW_END = 11355
UPDATE_CHUNK = 100


def with_retries(fn, *args, **kwargs):
    last_err = None
    for attempt in range(5):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            last_err = e
            if attempt == 4:
                raise
            time.sleep(2 * (attempt + 1))
    raise last_err


async def main():
    ws = get_worksheet(SHEET_NAME, KEY)
    gid_rows = with_retries(ws.get, f"B{ROW_START}:B{ROW_END}")
    gid_to_row = {
        row[0]: ROW_START + idx
        for idx, row in enumerate(gid_rows)
        if row and row[0].startswith("s2025")
    }
    print(f"target rows: {len(gid_to_row)}")

    updates = []
    async with aiohttp.ClientSession() as session:
        score_map = await build_score_url_map(session)
        player_cache = {}

        gids = sorted(gid_to_row.keys())
        for i in range(0, len(gids), 5):
            batch = gids[i : i + 5]
            html_results = await asyncio.gather(
                *[_fetch(session, f"{BASE_URL}/bis/{YEAR}/games/{gid}.html") for gid in batch]
            )
            for gid, html in zip(batch, html_results):
                if not html:
                    print(f"[skip] {gid} fetch failed")
                    continue
                try:
                    data = parse_official_game(gid, html)
                    await enrich_with_score_page(data, session, score_map, player_cache)
                except Exception as e:
                    print(f"[skip] {gid} {e}")
                    continue
                row_num = gid_to_row[gid]
                updates.append(
                    {
                        "range": f"B{row_num}:AY{row_num}",
                        "values": [_sailu_row(0, data)[1:]],
                    }
                )
            if i + 5 < len(gids):
                await asyncio.sleep(0.5)

    print(f"prepared updates: {len(updates)}")
    for i in range(0, len(updates), UPDATE_CHUNK):
        chunk = updates[i : i + UPDATE_CHUNK]
        with_retries(ws.batch_update, chunk, value_input_option="USER_ENTERED")
        print(f"wrote chunk {i + 1}-{i + len(chunk)}")
        time.sleep(1.0)


if __name__ == "__main__":
    asyncio.run(main())
