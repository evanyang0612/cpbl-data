"""Key 紀錄's starters on a player id instead of a name.

MLB publishes two active pitchers as exactly "Luis Ortiz", ids 682847 and
656814, so a name is provably not a unique key for this sheet. The same
pitcher already sits in 紀錄 under two spellings -- 20 starts as
"Luis L. Ortiz" and 16 as "Luis Ortiz", every one of them id 682847 -- and
設定, which matches a pitcher's games by name, counts him as one or the other
and never as 36. Following MLB's respellings closes most of that, but not the
part where the name itself cannot tell two people apart.

This adds the columns the fix needs and fills them from each game's own feed,
which names the starter by id and cannot be argued with. It writes only the
two new columns: nothing that exists is touched, and no formula changes.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cpbl import _sheets_client
from migration.update_mlb_record import (
    MLB_API,
    MAX_CONCURRENT,
    SPREADSHEET_KEY,
    WORKSHEET_NAME,
    _get_json,
    _starter,
    _with_retries,
)

CACHE_PATH = Path(tempfile.gettempdir()) / "mlb_starter_ids.json"
AWAY_ID_COLUMN = "BE"
HOME_ID_COLUMN = "BF"
AWAY_ID_HEADER = "客先發ID"
HOME_ID_HEADER = "主先發ID"


def starter_ids_from_feed(feed: dict[str, Any]) -> tuple[int | None, int | None]:
    """The away and home starting pitchers' ids, straight from the boxscore."""
    teams = feed["liveData"]["boxscore"]["teams"]
    ids: list[int | None] = []
    for side in ("away", "home"):
        pitcher_id, _ = _starter(teams.get(side, {}))
        ids.append(pitcher_id)
    return ids[0], ids[1]


def _runs(rows: list[int]) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    for row in rows:
        if spans and row == spans[-1][1] + 1:
            spans[-1] = (spans[-1][0], row)
        else:
            spans.append((row, row))
    return spans


def plan_id_writes(
    ids_by_row: dict[int, tuple[int | None, int | None]]
) -> list[dict[str, Any]]:
    """One value range per contiguous run, per column."""
    writes: list[dict[str, Any]] = []
    for index, column in enumerate((AWAY_ID_COLUMN, HOME_ID_COLUMN)):
        rows = sorted(ids_by_row)
        for start, end in _runs(rows):
            writes.append(
                {
                    "range": f"'{WORKSHEET_NAME}'!{column}{start}:{column}{end}",
                    "values": [
                        [ids_by_row[row][index] if ids_by_row[row][index] else ""]
                        for row in range(start, end + 1)
                    ],
                }
            )
    return writes


def gather_starter_ids(
    session: requests.Session,
    game_pks: list[str],
    cached: dict[str, list],
    cache_path: Path,
    *, every: int = 250,
) -> dict[str, list]:
    """Fetch each feed once, saving as it goes.

    An hour of feeds is worth not losing. Writing the cache only at the end
    survives an exception but not a kill, and the first attempt at this was
    stopped at 3,500 games with nothing on disk to show for it.
    """
    started = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {
            pool.submit(
                _get_json, session, f"{MLB_API}/v1.1/game/{game_pk}/feed/live"
            ): game_pk
            for game_pk in game_pks
        }
        for future in as_completed(futures):
            game_pk = futures[future]
            try:
                cached[game_pk] = list(starter_ids_from_feed(future.result()))
            except Exception as error:  # a game may have no usable live feed
                print(f"  no feed for {game_pk}: {error}", flush=True)
                cached[game_pk] = [None, None]
            done += 1
            if done % every == 0 or done == len(game_pks):
                cache_path.write_text(json.dumps(cached))
                print(
                    f"Fetched {done}/{len(game_pks)} feed(s), "
                    f"{len(cached)} cached ({time.time() - started:.0f}s)",
                    flush=True,
                )
    return cached


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-year",
        type=int,
        default=2022,
        help="earliest season to fill; the aggregates reach back to 2022",
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    spreadsheet = _sheets_client.spreadsheet(SPREADSHEET_KEY)
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    keys = _with_retries("read A:B", lambda: worksheet.get("A:B"))

    wanted: dict[str, int] = {}
    for index, row in enumerate(keys[1:], start=2):
        date_text = str(row[0]).strip() if row else ""
        game_pk = str(row[1]).strip() if len(row) > 1 else ""
        year = date_text.split("/")[0]
        if game_pk and year.isdigit() and int(year) >= args.from_year:
            wanted[game_pk] = index
    print(f"{len(wanted)} game(s) dated {args.from_year} or later", flush=True)
    if not wanted:
        return

    # An hour of feeds is worth caching: the fetch is the whole cost here, and
    # a run that is interrupted should not start over.
    cached: dict[str, list] = {}
    if CACHE_PATH.exists():
        cached = json.loads(CACHE_PATH.read_text())
        print(f"{len(cached)} id(s) already cached in {CACHE_PATH}", flush=True)

    missing = sorted((pk for pk in wanted if pk not in cached), key=int)
    if missing:
        print(f"Fetching {len(missing)} feed(s) into {CACHE_PATH}.", flush=True)
        gather_starter_ids(requests.Session(), missing, cached, CACHE_PATH)

    ids_by_row = {
        wanted[game_pk]: tuple(pair)
        for game_pk, pair in cached.items()
        if game_pk in wanted
    }
    filled = sum(
        1 for pair in ids_by_row.values() for value in pair if value is not None
    )
    print(f"{filled} starter id(s) resolved across {len(ids_by_row)} game(s)")

    writes = plan_id_writes(ids_by_row)
    header = {
        "range": f"'{WORKSHEET_NAME}'!{AWAY_ID_COLUMN}1:{HOME_ID_COLUMN}1",
        "values": [[AWAY_ID_HEADER, HOME_ID_HEADER]],
    }
    print(f"{len(writes) + 1} range(s) to write; e.g. {writes[0]['range']}")

    if not args.apply:
        print("Dry run; pass --apply to write.", flush=True)
        return

    needed_columns = 58  # through BF
    if worksheet.col_count < needed_columns:
        _with_retries(
            "add_cols",
            lambda: worksheet.add_cols(needed_columns - worksheet.col_count),
        )
        print(f"Widened {WORKSHEET_NAME} to {needed_columns} columns.", flush=True)

    _with_retries(
        "write starter ids",
        lambda: spreadsheet.values_batch_update(
            {"valueInputOption": "RAW", "data": [header] + writes}
        ),
    )
    print(f"Wrote ids for {len(ids_by_row)} game(s).", flush=True)


if __name__ == "__main__":
    main()
