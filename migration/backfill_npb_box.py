"""Backfill every NPB box score npb.jp still serves, into a local cache.

賽錄 records a starter's innings and earned runs and nothing else, so the
question of whether a starter's recent walks and pitch count predict his next
outing has never had more than one season of data behind it. This fetches the
rest: npb.jp keeps per-pitcher 投球数 and 四球 back to 2016.

A finished box score never changes, so every game is written to
`.cache/npb_box/` once and re-runs are free. That is also what makes a
throttled run recoverable — it stops, and the next run picks up where it left
off instead of starting over.

    python migration/backfill_npb_box.py --years 2026 --months 8   # a trial
    python migration/backfill_npb_box.py                          # 2016 onward
    python migration/backfill_npb_box.py --concurrency 8
"""

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict
from datetime import date

import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from baseball import npb_box  # noqa: E402

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), ".cache", "npb_box")

# npb.jp answers a refused request with a 200 and an empty page. One of those
# is a hiccup; a run of them means the site has stopped serving us, and
# carrying on would only deepen the hole.
CONSECUTIVE_REFUSALS_BEFORE_STOP = 12
RETRIES = 3


class Refused(Exception):
    """The site stopped serving this run."""


def cache_path(code: str) -> str:
    return os.path.join(CACHE_DIR, f"{code}.json")


async def _get(session, url, *, retries=RETRIES):
    """Fetch one page, retrying transient failures with a widening wait."""
    wait = 2
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 200:
                    return await r.text()
                if r.status == 404:
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        if attempt < retries - 1:
            await asyncio.sleep(wait)
            wait *= 2
    return None


async def fetch_game(session, sem, year, mmdd, code, counters):
    """Cache one game. Returns True when it was fetched rather than skipped."""
    gid = npb_box.game_code(year, mmdd, code)
    path = cache_path(gid)
    if os.path.exists(path):
        counters["cached"] += 1
        return False
    # A monthly schedule page runs past its own month, so it lists games that
    # have not been played. Their box pages are served — empty — and would
    # otherwise read exactly like a refusal and trip the stop.
    game_day = f"{year}-{mmdd[:2]}-{mmdd[2:]}"
    if game_day >= date.today().isoformat():
        counters["future"] += 1
        return False

    async with sem:
        html = await _get(session, npb_box.box_url(year, mmdd, code))
    if html is None:
        counters["refused"] += 1
        counters["consecutive"] += 1
        return True

    try:
        box = npb_box.read_box(html)
    except npb_box.Throttled:
        counters["refused"] += 1
        counters["consecutive"] += 1
        return True

    counters["consecutive"] = 0
    if box is None:
        record = {"game_code": gid, "date": game_day, "cancelled": True}
        counters["cancelled"] += 1
    else:
        record = {
            "game_code": gid,
            "date": game_day,
            "away": box["away"],
            "home": box["home"],
            "away_pitchers": [asdict(p) for p in box["away_pitchers"]],
            "home_pitchers": [asdict(p) for p in box["home_pitchers"]],
        }
        counters["fetched"] += 1

    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(record, fh, ensure_ascii=False)
    return True


async def games_for_year(session, sem, year, months):
    """Every game linked from that year's monthly schedule pages."""
    async def one(month):
        async with sem:
            html = await _get(session, npb_box.SCHEDULE_URL.format(
                year=year, month=month))
        return npb_box.parse_schedule(html) if html else []

    found = await asyncio.gather(*(one(m) for m in months))
    return sorted({g for month in found for g in month})


async def run(years, months, concurrency, limit):
    sem = asyncio.Semaphore(concurrency)
    counters = {"fetched": 0, "cached": 0, "cancelled": 0,
                "future": 0, "refused": 0, "consecutive": 0}
    async with aiohttp.ClientSession(headers=npb_box.BROWSER_HEADERS) as session:
        for year in years:
            games = await games_for_year(session, sem, str(year), months)
            if limit:
                games = games[:limit]
            print(f"{year}: {len(games)} 場", flush=True)
            for i in range(0, len(games), concurrency):
                batch = games[i:i + concurrency]
                await asyncio.gather(*(
                    fetch_game(session, sem, y, d, c, counters)
                    for y, d, c in batch))
                if counters["consecutive"] >= CONSECUTIVE_REFUSALS_BEFORE_STOP:
                    raise Refused(
                        f"{counters['consecutive']} 連續無回應,停在 {year} 第 "
                        f"{i + len(batch)} 場。已抓到的都在快取裡,重跑會接續。")
                if (i // concurrency) % 20 == 0 and i:
                    print(f"   … {i}/{len(games)}  {counters}", flush=True)
            print(f"  {year} 完成: {counters}", flush=True)
    return counters


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=int, nargs="*")
    p.add_argument("--months", type=int, nargs="*")
    p.add_argument("--concurrency", type=int, default=5)
    p.add_argument("--limit", type=int, help="每年最多幾場,試跑用")
    args = p.parse_args()

    years = args.years or list(range(npb_box.EARLIEST_YEAR, 2027))
    months = args.months or list(npb_box.SEASON_MONTHS)
    try:
        counters = asyncio.run(run(years, months, args.concurrency, args.limit))
    except Refused as exc:
        print(f"\n停止: {exc}", flush=True)
        raise SystemExit(1)
    print(f"\n完成: {counters}")
    print(f"快取: {CACHE_DIR}")


if __name__ == "__main__":
    main()
