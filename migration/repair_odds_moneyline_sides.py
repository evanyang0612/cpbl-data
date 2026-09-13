"""One-off: put the moneyline back on the right side for pre-#79 盤口 rows.

``0faface`` fixed two readings of PS3838's positional arrays at once, and only
one of them has been backfilled. ``migration/repair_odds_spread_sign.py`` put
the handicap's sign back; this puts the moneyline's sides back::

    -        "ml_home": _price(ml[0]),
    -        "ml_away": _price(ml[1]),
    +        "ml_away": _price(ml[0]),
    +        "ml_home": _price(ml[1]),

Both were the same misreading — index 0 taken for the home team when it names
the away one — so the same rows are affected, and repairing only the handicap
left the two markets contradicting each other inside one row.

A row is judged by the bracket ``baseball.odds_audit`` checks::

    home price receiving runs  <  home moneyline  <  home price laying runs

and is only rewritten when swapping the two prices demonstrably moves the
moneyline inside it. A row that neither orientation satisfies is reported and
left exactly as it is: something other than this bug is going on there, and
overwriting it would bury the evidence.

Run the handicap repair first — the bracket is read off the ladder, so a
ladder still carrying the old sign would judge the moneyline against a
mirror of itself.

    uv run python migration/repair_odds_moneyline_sides.py --dry-run
    uv run python migration/repair_odds_moneyline_sides.py
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/Users/evansmac/cpbl")

from dotenv import load_dotenv

load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")

from baseball import odds_audit  # noqa: E402

BACKUP_DIR = Path("/Users/evansmac/cpbl/.cache")
SHEET_NAME = "盤口"


def _both_prices(ml_home, ml_away):
    home, away = odds_audit._price(ml_home), odds_audit._price(ml_away)
    return (home, away) if home and away else (None, None)


def plan_row(all_spreads, ml_home, ml_away, *, row_number: int) -> dict | None:
    """The swap this row needs, or None when it needs none or cannot be judged."""
    home, away = _both_prices(ml_home, ml_away)
    if home is None or not odds_audit._rungs(all_spreads):
        return None
    if odds_audit.check_row(all_spreads, home) is None:
        return None                       # already inside the bracket
    if odds_audit.check_row(all_spreads, away) is not None:
        return None                       # the swap would not help either
    return {"row": row_number,
            "ml_home": str(ml_away), "ml_away": str(ml_home),
            "old_ml_home": str(ml_home), "old_ml_away": str(ml_away)}


def classify(rows) -> tuple[list[dict], dict]:
    """(swaps, tally) over (all_spreads, ml_home, ml_away) triples."""
    plans = []
    tally = {"scanned": 0, "sound": 0, "swapped": 0, "unresolved": 0, "skipped": 0}
    for offset, (ladder, ml_home, ml_away) in enumerate(rows):
        tally["scanned"] += 1
        home, away = _both_prices(ml_home, ml_away)
        if home is None or not odds_audit._rungs(ladder):
            tally["skipped"] += 1
            continue
        if odds_audit.check_row(ladder, home) is None:
            tally["sound"] += 1
            continue
        plan = plan_row(ladder, ml_home, ml_away, row_number=offset + 2)
        if plan is None:
            tally["unresolved"] += 1
        else:
            tally["swapped"] += 1
            plans.append(plan)
    return plans, tally


def col_letter(idx0: int) -> str:
    idx, letters = idx0 + 1, ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def main(dry_run: bool = False) -> None:
    from baseball import pinnacle_odds as po
    from baseball.sheets import GoogleSheetsClient

    client = GoogleSheetsClient()
    everything = []
    for spec in (po.NPB, po.MLB, po.CPBL):
        ws = client.spreadsheet(spec.spreadsheet_key()).worksheet(SHEET_NAME)
        values = ws.get_all_values()
        if len(values) < 2:
            print(f"{spec.key.upper():<5} 盤口 是空的")
            continue
        header = values[0]
        i_lad, i_h, i_a = (header.index("all_spreads"), header.index("ml_home"),
                           header.index("ml_away"))
        triples = [(r[i_lad] if i_lad < len(r) else "",
                    r[i_h] if i_h < len(r) else "",
                    r[i_a] if i_a < len(r) else "") for r in values[1:]]
        plans, tally = classify(triples)
        print(f"{spec.key.upper():<5} {tally['scanned']:>6} 列  "
              f"正常 {tally['sound']:>6}  要對調 {tally['swapped']:>5}  "
              f"對調也修不好 {tally['unresolved']:>4}  無法判定 {tally['skipped']:>5}")
        if plans:
            p = plans[0]
            print(f"      第 {plans[0]['row']}~{plans[-1]['row']} 列；例："
                  f"主{p['old_ml_home']}/客{p['old_ml_away']}"
                  f" -> 主{p['ml_home']}/客{p['ml_away']}")
        everything.extend({**p, "sheet": spec.key, "key": spec.spreadsheet_key(),
                           "_cols": (col_letter(i_h), col_letter(i_a))}
                          for p in plans)

    total = len(everything)
    print(f"\n合計要對調 {total} 列")
    if not total or dry_run:
        if dry_run:
            print("（--dry-run，什麼都沒寫）")
        return

    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = BACKUP_DIR / f"odds_moneyline_sides_repair_{stamp}.json"
    backup.write_text(json.dumps(everything, ensure_ascii=False, indent=1))
    print(f"備份已寫入 {backup}")

    by_sheet = {}
    for plan in everything:
        by_sheet.setdefault(plan["key"], []).append(plan)
    for key, plans in by_sheet.items():
        data = []
        for p in plans:
            h_col, a_col = p["_cols"]
            data.append({"range": f"'{SHEET_NAME}'!{h_col}{p['row']}",
                         "values": [[p["ml_home"]]]})
            data.append({"range": f"'{SHEET_NAME}'!{a_col}{p['row']}",
                         "values": [[p["ml_away"]]]})
        client.spreadsheet(key).values_batch_update(
            {"valueInputOption": "RAW", "data": data})
        print(f"  {key[:12]}… 寫入 {len(data)} 格")
    print(f"修好 {total} 列")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="印出要改什麼，不寫入")
    main(dry_run=parser.parse_args().dry_run)
