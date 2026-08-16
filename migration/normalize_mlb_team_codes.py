"""One-off: rewrite legacy team codes onto the canonical one.

MLB Stats API began abbreviating the Athletics as ATH in 2025; two sheets ended up
holding both codes for the one franchise:

* `紀錄` — OAK for 2017-2024, ATH from 2025, which silently splits every aggregation
  keyed on a team label (MLB勝敗表 shows 0 wins for OAK the moment its window reaches
  2025).
* `盤口` — collected from 2026-08 onwards, so every Athletics row says ATH while 紀錄
  says OAK. The odds rows join to 紀錄 on `mlb_game_pk`, so nothing breaks, but
  filtering either sheet by team code would miss half the franchise's games.

`baseball/mlb_teams.py` now maps the API's code through `canonical_team_code()` on the
way in, so this script only has to fix rows written before that. It touches nothing but
the team-code columns, dumps every change to a JSON backup first, and can be re-run
safely — a second pass finds nothing to do.

    uv run python migration/normalize_mlb_team_codes.py --dry-run
    uv run python migration/normalize_mlb_team_codes.py
"""

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/Users/evansmac/cpbl")

from dotenv import load_dotenv

load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")

from baseball.mlb_teams import TEAM_CODE_ALIASES, canonical_team_code  # noqa: E402
from migration.update_mlb_record import SPREADSHEET_KEY  # noqa: E402

BACKUP_DIR = Path("/Users/evansmac/cpbl/.cache")


@dataclass(frozen=True)
class Target:
    """A sheet holding team codes, and where in it those codes live."""

    title: str
    first_row: int
    columns: list[int] | None = None       # fixed positions, 0-indexed
    header_names: list[str] | None = None  # or resolved from the header row


TARGETS = (
    # 紀錄!C 客隊隊伍 and 紀錄!R 主隊隊伍 have been in place for a decade
    Target(title="紀錄", first_row=2, columns=[2, 17]),
    # 盤口 gained columns as the odds scraper grew, so look its up by name
    Target(title="盤口", first_row=2, header_names=["home_abbr", "away_abbr"]),
)


def col_letter(idx0: int) -> str:
    idx = idx0 + 1
    letters = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def resolve_columns(header: list[str], names: list[str]) -> list[int]:
    """Positions of the named header cells, complaining if one has gone missing."""
    positions = []
    for name in names:
        try:
            positions.append(header.index(name))
        except ValueError as exc:
            raise KeyError(f"column {name!r} not found in header") from exc
    return positions


def plan_row_updates(rows: list[list], *, first_row: int,
                     columns: list[int]) -> list[dict]:
    """Every cell needing a rewrite, as {row, column, old, new}."""
    updates = []
    for offset, row in enumerate(rows):
        for col_idx in columns:
            if col_idx >= len(row):
                continue
            value = str(row[col_idx])
            canonical = canonical_team_code(value)
            if canonical != value:
                updates.append({
                    "row": first_row + offset,
                    "column": col_letter(col_idx),
                    "old": value,
                    "new": canonical,
                })
    return updates


def plan_for_target(ws, target: Target) -> tuple[list[dict], int]:
    """Read one sheet and work out what to rewrite in it."""
    values = ws.get_all_values()
    if not values:
        return [], 0
    header, rows = values[0], values[target.first_row - 1:]
    columns = (target.columns if target.columns is not None
               else resolve_columns(header, target.header_names))
    return plan_row_updates(rows, first_row=target.first_row, columns=columns), len(rows)


def main(dry_run: bool = False) -> None:
    from baseball.sheets import GoogleSheetsClient

    client = GoogleSheetsClient()
    spreadsheet = client.spreadsheet(SPREADSHEET_KEY)

    everything: list[dict] = []
    for target in TARGETS:
        ws = spreadsheet.worksheet(target.title)
        updates, scanned = plan_for_target(ws, target)
        counts: dict[str, int] = {}
        for update in updates:
            label = f'{update["old"]} -> {update["new"]}'
            counts[label] = counts.get(label, 0) + 1
        print(f"{target.title}: {scanned} data rows scanned, "
              f"{len(updates)} cell(s) to rewrite")
        for label, n in sorted(counts.items()):
            print(f"  {label}: {n}")
        if updates:
            print(f"  rows {updates[0]['row']}..{updates[-1]['row']}")
        everything.extend({**u, "sheet": target.title} for u in updates)

    if not everything or dry_run:
        return

    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = BACKUP_DIR / f"mlb_team_code_backfill_{stamp}.json"
    backup.write_text(json.dumps({
        "spreadsheet": SPREADSHEET_KEY,
        "aliases": TEAM_CODE_ALIASES,
        "updates": everything,
    }, ensure_ascii=False, indent=2))
    print(f"backup written: {backup}")

    spreadsheet.values_batch_update({
        "valueInputOption": "RAW",
        "data": [{"range": f"'{u['sheet']}'!{u['column']}{u['row']}",
                  "values": [[u["new"]]]} for u in everything],
    })
    print(f"rewrote {len(everything)} cell(s)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would change, write nothing")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
