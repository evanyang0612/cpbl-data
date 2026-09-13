"""One-off: put the handicap's sign back on 盤口 rows written before PR #79.

PS3838 serves a spread as a bare positional array, and the two halves of it run
in opposite orders::

    [away_hdp, home_hdp, line, home_price, away_price]
       s[0]       s[1]           s[3]         s[4]

The scraper read ``home_hdp`` from ``s[0]`` — the *away* team's handicap —
while taking the price from ``s[3]``, the home team's. Since the two handicaps
are always mirrors, every affected row recorded the right price against the
wrong side of the line: a home team laying 1.5 was written as receiving it.
``0faface`` (2026-08-25 20:26 JST) changed ``s[0]`` to ``s[1]`` and everything
written since is sound.

Prices were never wrong, and ``s[0] == -s[1]`` exactly, so negating the stored
handicap restores the row with nothing lost. The main line does not move
either: it is chosen by ``abs(hdp) == 1.5`` and the most balanced juice, and
neither survives a sign change.

**Rows are found by reading the ladder, never by date.** A ladder that gets
longer as the team receives more runs is impossible — receiving runs can only
shorten a price — so a rising ladder is broken and a falling one is not. That
distinction matters: on 2026-08-25 the MLB board produced both, and 692 rows
from that morning are sound despite predating the fix. A date cutoff would
have corrupted every one of them.

Verified against an independent record of the same board: bettingiscool's
capture of 2026-08-22 オリックス @ ソフトバンク reads -2.0 2.19, -1.5 1.925,
+1.5 1.263 where our repaired row reads 2.18 / 1.917 / 1.259 — the two agree
rung for rung, five seconds of drift apart.

A single-rung ladder has no direction to read and is left untouched.

    uv run python migration/repair_odds_spread_sign.py --dry-run
    uv run python migration/repair_odds_spread_sign.py
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/Users/evansmac/cpbl")

from dotenv import load_dotenv

load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")

BACKUP_DIR = Path("/Users/evansmac/cpbl/.cache")
SHEET_NAME = "盤口"


def _rungs(all_spreads: str) -> list[dict]:
    try:
        ladder = json.loads(all_spreads or "[]")
    except (TypeError, ValueError):
        return []
    return [r for r in ladder
            if isinstance(r, dict) and r.get("hdp") is not None and r.get("home")]


def ladder_direction(rungs: list[dict]) -> int:
    """+1 if the home price rises with the handicap, -1 if it falls, 0 if neither.

    Only the sign matters, and only a rise is diagnostic.
    """
    points = sorted((float(r["hdp"]), float(r["home"])) for r in rungs)
    if len(points) < 2:
        return 0
    up = sum(1 for a, b in zip(points, points[1:]) if b[1] > a[1])
    down = sum(1 for a, b in zip(points, points[1:]) if b[1] < a[1])
    return 1 if up > down else (-1 if down > up else 0)


def needs_flip(all_spreads: str) -> bool:
    return ladder_direction(_rungs(all_spreads)) > 0


def _neg(value):
    """-0.0 is a real float and reads as a typo in a sheet; keep it at 0."""
    flipped = -float(value)
    return 0.0 if flipped == 0 else flipped


def flip_ladder(all_spreads: str) -> str:
    rungs = _rungs(all_spreads)
    return json.dumps([{**r, "hdp": _neg(r["hdp"])} for r in rungs],
                      ensure_ascii=False)


def plan_row(all_spreads: str, spread_hdp: str, *, row_number: int) -> dict | None:
    """What to rewrite in one row, or None when the row is already sound."""
    if not needs_flip(all_spreads):
        return None
    plan = {"row": row_number, "all_spreads": flip_ladder(all_spreads),
            "old_all_spreads": all_spreads}
    plan["old_spread_hdp"] = str(spread_hdp)
    try:
        # Sheets normalises 4.0 to "4"; %g writes it back the same way.
        plan["spread_hdp"] = f"{_neg(spread_hdp):g}"
    except (TypeError, ValueError):
        plan["spread_hdp"] = None      # blank main line; the ladder still gets fixed
    return plan


def col_letter(idx0: int) -> str:
    idx, letters = idx0 + 1, ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def plan_sheet(values: list[list]) -> tuple[list[dict], dict]:
    """(rewrites, tally) for one 盤口 worksheet's raw values."""
    header, rows = values[0], values[1:]
    try:
        i_lad = header.index("all_spreads")
        i_hdp = header.index("spread_hdp")
    except ValueError as exc:
        raise KeyError("盤口 is missing all_spreads / spread_hdp") from exc

    plans, tally = [], {"scanned": 0, "sound": 0, "broken": 0, "unjudgeable": 0}
    for offset, row in enumerate(rows):
        tally["scanned"] += 1
        ladder = row[i_lad] if i_lad < len(row) else ""
        hdp = row[i_hdp] if i_hdp < len(row) else ""
        rungs = _rungs(ladder)
        if len(rungs) < 2:
            tally["unjudgeable"] += 1
            continue
        plan = plan_row(ladder, hdp, row_number=offset + 2)
        if plan is None:
            tally["sound"] += 1
        else:
            tally["broken"] += 1
            plans.append({**plan, "_cols": (col_letter(i_hdp), col_letter(i_lad))})
    return plans, tally


def main(dry_run: bool = False) -> None:
    from baseball import pinnacle_odds as po
    from baseball.sheets import GoogleSheetsClient

    client = GoogleSheetsClient()
    everything = []
    for spec in (po.NPB, po.MLB, po.CPBL):
        spreadsheet = client.spreadsheet(spec.spreadsheet_key())
        ws = spreadsheet.worksheet(SHEET_NAME)
        values = ws.get_all_values()
        if len(values) < 2:
            print(f"{spec.key.upper():<5} 盤口 is empty")
            continue
        plans, tally = plan_sheet(values)
        print(f"{spec.key.upper():<5} {tally['scanned']:>6} 列  "
              f"正常 {tally['sound']:>6}  要修 {tally['broken']:>5}  "
              f"無法判定 {tally['unjudgeable']:>4}")
        if plans:
            first, last = plans[0], plans[-1]
            print(f"      第 {first['row']}~{last['row']} 列")
            print(f"      例：hdp {first['old_spread_hdp']} -> {first['spread_hdp']}")
            print(f"          {first['old_all_spreads'][:96]}")
            print(f"       -> {first['all_spreads'][:96]}")
        everything.extend({**p, "sheet": spec.key,
                           "key": spec.spreadsheet_key()} for p in plans)

    total = len(everything)
    print(f"\n合計要修 {total} 列")
    if not total or dry_run:
        if dry_run:
            print("（--dry-run，什麼都沒寫）")
        return

    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = BACKUP_DIR / f"odds_spread_sign_repair_{stamp}.json"
    backup.write_text(json.dumps(everything, ensure_ascii=False, indent=1))
    print(f"備份已寫入 {backup}")

    by_sheet = {}
    for plan in everything:
        by_sheet.setdefault(plan["key"], []).append(plan)
    for key, plans in by_sheet.items():
        data = []
        for p in plans:
            hdp_col, lad_col = p["_cols"]
            if p["spread_hdp"] is not None:
                data.append({"range": f"'{SHEET_NAME}'!{hdp_col}{p['row']}",
                             "values": [[p["spread_hdp"]]]})
            data.append({"range": f"'{SHEET_NAME}'!{lad_col}{p['row']}",
                         "values": [[p["all_spreads"]]]})
        client.spreadsheet(key).values_batch_update(
            {"valueInputOption": "RAW", "data": data})
        print(f"  {key[:12]}… 寫入 {len(data)} 格")
    print(f"修好 {total} 列")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="印出要改什麼，不寫入")
    main(dry_run=parser.parse_args().dry_run)
