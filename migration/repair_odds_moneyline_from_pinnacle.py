"""One-off: un-swap NPB 盤口 moneylines that the bracket repair could not see.

``repair_odds_moneyline_sides.py`` judged each row by the ladder bracket

    home price receiving runs  <  home moneyline  <  home price laying runs

and only swapped a row the bracket rejected. That misses a swapped row whose
underdog price happens to sit just inside the bracket, which is common: the
dog's moneyline and the favourite's -1.5 are often a few cents apart. In
August 2026 those rows were still there, as exact mirror images.

Pinnacle's own tick history (BettingIsCool, ``.cache/pinnacle_history``) gives
a reference that does not depend on the ladder: the board at the very second
the row was captured. A row is swapped only when the sheet's prices are that
board's prices with the sides exchanged, and clearly not the unswapped ones.
Anything else is left as it is and counted.

Only NPB: the reference covers NPB and CPBL, and CPBL has not been pulled yet.

    uv run python migration/repair_odds_moneyline_from_pinnacle.py --dry-run
    uv run python migration/repair_odds_moneyline_from_pinnacle.py
"""

import argparse
import datetime as dt
import glob
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, "/Users/evansmac/cpbl")

from dotenv import load_dotenv  # noqa: E402

load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")

ROOT = Path("/Users/evansmac/cpbl")
EVENTS = ROOT / ".cache" / "pinnacle_history" / "npb" / "events"
SHEET_NAME = "盤口"
# Per side. The scraper and the archive read the same board, so a match is
# usually exact; this only absorbs a tick landing between the two clocks.
MATCH_TOL = 0.03
PERIODS = {"final": 0, "half": 1}
# 0faface (#79) fixed the away-first reading. Rows captured after it were read
# correctly, so a mirror after this instant is something else and is not ours
# to swap.
FIX_MERGED_JST = "2026-08-25 20:26:37"


def before_fix(captured_at_jst: str) -> bool:
    return captured_at_jst < FIX_MERGED_JST


def _f(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


def verdict(sheet_home, sheet_away, ref_home, ref_away) -> str:
    """ok / swap / ambiguous / unmatched / skip for one row against Pinnacle."""
    h, a = _f(sheet_home), _f(sheet_away)
    if None in (h, a, ref_home, ref_away):
        return "skip"
    # A near pick'em mirrors itself: within the tolerance either side fits.
    if abs(ref_home - ref_away) <= 2 * MATCH_TOL:
        return "ambiguous"
    direct = max(abs(h - ref_home), abs(a - ref_away))
    mirror = max(abs(h - ref_away), abs(a - ref_home))
    if direct <= MATCH_TOL:
        return "ok"
    if mirror <= MATCH_TOL:
        return "swap"
    return "unmatched"


def _reference_index() -> dict:
    """(game_date, home_norm, away_norm) -> raw record, from the local archive."""
    from baseball.pinnacle_snapshots import _norm
    from baseball.pinnacle_odds import JST
    from baseball.pinnacle_history import _parse_ts

    index = {}
    for path in glob.glob(str(EVENTS / "*.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            record = json.load(fh)
        fx = record["fixture"]
        day = _parse_ts(fx["starts"]).astimezone(JST).strftime("%Y-%m-%d")
        index[(day, _norm(fx["runner_home"], "npb"),
               _norm(fx["runner_away"], "npb"))] = record
    return index


def reference_moneyline(record: dict, period: str, captured_at_jst: str):
    from baseball.pinnacle_odds import JST
    from baseball.pinnacle_snapshots import board_at, parse_board

    when = dt.datetime.strptime(captured_at_jst, "%Y-%m-%d %H:%M:%S").replace(tzinfo=JST)
    parsed = parse_board(PERIODS[period], board_at(record["odds"], PERIODS[period], when))
    if not parsed:
        return None, None
    return parsed["ml_home"], parsed["ml_away"]


def col_letter(idx0: int) -> str:
    idx, letters = idx0 + 1, ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def main(dry_run: bool) -> None:
    from baseball import pinnacle_odds as po
    from baseball.sheets import GoogleSheetsClient

    index = _reference_index()
    first_day = min(k[0] for k in index)
    print(f"Pinnacle 參考資料：{len(index)} 場，最早 {first_day}")

    client = GoogleSheetsClient()
    key = po.NPB.spreadsheet_key()
    values = client.spreadsheet(key).worksheet(SHEET_NAME).get_all_values()
    header, rows = values[0], values[1:]
    ix = {h: i for i, h in enumerate(header)}

    def cell(row, name):
        i = ix[name]
        return row[i] if i < len(row) else ""

    tally, plans, examples = {}, [], {}
    for offset, row in enumerate(rows):
        if cell(row, "source") not in ("", "ps3838") or cell(row, "status") != "pregame":
            continue
        period = cell(row, "period")
        record = index.get((cell(row, "game_date"), cell(row, "home_norm"),
                            cell(row, "away_norm")))
        if period not in PERIODS or not before_fix(cell(row, "captured_at")):
            continue
        if record is None:
            outcome = "no_reference"
        else:
            ref_home, ref_away = reference_moneyline(record, period, cell(row, "captured_at"))
            outcome = verdict(cell(row, "ml_home"), cell(row, "ml_away"), ref_home, ref_away)
        tally[outcome] = tally.get(outcome, 0) + 1
        examples.setdefault(outcome, (offset + 2, row))
        if outcome == "swap":
            plans.append({"row": offset + 2, "captured_at": cell(row, "captured_at"),
                          "game": f"{cell(row, 'away_norm')}@{cell(row, 'home_norm')}",
                          "old_ml_home": cell(row, "ml_home"),
                          "old_ml_away": cell(row, "ml_away")})

    print("結果：", tally)
    if "no_reference" in tally:
        print(f"  no_reference 包含 {first_day} 以前的列（Pinnacle 資料還沒抓到）")
    days = sorted({p["captured_at"][:10] for p in plans})
    print(f"要對調 {len(plans)} 列，日期：{days[0] if days else '-'} ~ {days[-1] if days else '-'}")
    for p in plans[:5]:
        print(f"  第 {p['row']} 列 {p['captured_at']} {p['game']} "
              f"主{p['old_ml_home']}/客{p['old_ml_away']} -> "
              f"主{p['old_ml_away']}/客{p['old_ml_home']}")
    if not plans or dry_run:
        if dry_run:
            print("（--dry-run，什麼都沒寫）")
        return

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = ROOT / ".cache" / f"odds_moneyline_pinnacle_repair_{stamp}.json"
    backup.write_text(json.dumps(plans, ensure_ascii=False, indent=1))
    print(f"備份已寫入 {backup}")
    h_col, a_col = col_letter(ix["ml_home"]), col_letter(ix["ml_away"])
    data = []
    for p in plans:
        data.append({"range": f"'{SHEET_NAME}'!{h_col}{p['row']}",
                     "values": [[p["old_ml_away"]]]})
        data.append({"range": f"'{SHEET_NAME}'!{a_col}{p['row']}",
                     "values": [[p["old_ml_home"]]]})
    client.spreadsheet(key).values_batch_update({"valueInputOption": "RAW", "data": data})
    print(f"修好 {len(plans)} 列（{len(data)} 格）")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="印出要改什麼，不寫入")
    main(dry_run=parser.parse_args().dry_run)
