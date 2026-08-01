"""
Repair the 賽錄 starter columns AT:AY. Two independent fixes:

qs — recompute 客QS / 主QS under the tiered 優質先發 rule
    Rows written before this fix used get_schedule_game_data's old
    single-threshold rule (≥6 IP & ER ≤3). The agreed rule is the tiered one
    already used by 分析表 / the CPBL importer:
        ≥7 IP & ER ≤3, or ≥6 IP & ER ≤2, or ≥5 IP & ER ≤1

ip — restore 客投局 / 主投局 to baseball notation ("5.1" = 5⅓ IP)
    On 2026-06-05 the 賽錄 scraper was switched to get_schedule_game_data,
    which emits decimal-thirds ("5.3333") because that is what 分析表 needs.
    AT/AU inherited that format, but the sheet's 客先局/主先局 formulas (BK/BL)
    parse .1/.2 notation, so "5.3333" was converted to 5.799966667 instead of
    5.333333 — a +0.47 skew that also lets 4⅔ IP clear a ">=5" threshold.
    Only non-integer values from 2026-06-05 onward are affected.

Only the six columns AT:AY are read and written; scraped game data is never
touched, and both fixes are idempotent, so this can be re-run safely.

    AT 客投局   AU 主投局   AV 客責失   AW 客QS   AX 主責失   AY 主QS

賽錄 lives in two spreadsheets (source + target); both are processed.

Usage:
    python -m migration.backfill_sailu_qs                    # dry run, both fixes
    python -m migration.backfill_sailu_qs --fix ip           # dry run, IP only
    python -m migration.backfill_sailu_qs --fix ip --apply   # write
"""

from __future__ import annotations

import argparse
import time

import gspread

from npb import (
    SAILU_SHEET_NAME,
    SAILU_SPREADSHEET_KEY,
    SAILU_TARGET_SPREADSHEET_KEY,
    get_worksheet,
)
from baseball.npb_services import NpbRowsService

FIRST_DATA_ROW = 2
UPDATE_CHUNK = 200
SAMPLE_LIMIT = 20


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


def _cell(row: list[str], idx: int) -> str:
    return (row[idx] if idx < len(row) else "").strip()


def _expected_flag(ip_raw: str, er_raw: str) -> int | None:
    """Recomputed QS flag, or None when the row lacks the inputs to decide."""
    if ip_raw == "" or er_raw == "":
        return None
    return NpbRowsService.qs_flag(ip_raw, er_raw)


def plan_sheet(ws, fixes: set[str]) -> tuple[list[dict], list[str]]:
    """Return (batch_update requests, human-readable diff lines)."""
    game_ids = with_retries(ws.get, f"B{FIRST_DATA_ROW}:B{ws.row_count}")
    stats = with_retries(ws.get, f"AT{FIRST_DATA_ROW}:AY{ws.row_count}")

    updates: list[dict] = []
    diffs: list[str] = []
    for offset, row in enumerate(stats):
        row_num = FIRST_DATA_ROW + offset
        gid_row = game_ids[offset] if offset < len(game_ids) else []
        gid = gid_row[0].strip() if gid_row else ""
        tag = gid or "(no id)"

        # AT 客投局, AU 主投局, AV 客責失, AW 客QS, AX 主責失, AY 主QS
        for label, ip_idx, ip_col, er_idx, qs_idx, qs_col in (
            ("客", 0, "AT", 2, 3, "AW"),
            ("主", 1, "AU", 4, 5, "AY"),
        ):
            ip_raw = _cell(row, ip_idx)
            er_raw = _cell(row, er_idx)

            if "ip" in fixes and ip_raw:
                wanted = NpbRowsService.sailu_ip_str(ip_raw)
                if wanted != ip_raw:
                    updates.append(
                        {"range": f"{ip_col}{row_num}", "values": [[wanted]]}
                    )
                    diffs.append(
                        f"  row {row_num} {tag} {label}投局: "
                        f"{ip_raw} -> {wanted}"
                    )

            if "qs" in fixes:
                expected = _expected_flag(ip_raw, er_raw)
                current = _cell(row, qs_idx)
                if expected is not None and current != str(expected):
                    updates.append(
                        {"range": f"{qs_col}{row_num}", "values": [[expected]]}
                    )
                    diffs.append(
                        f"  row {row_num} {tag} {label}先發 "
                        f"{ip_raw}局/{er_raw}自責: "
                        f"{current or '(blank)'} -> {expected}"
                    )

    return updates, diffs


def process(key: str, label: str, fixes: set[str], apply: bool) -> int:
    print(f"\n=== {label} ({key}) ===")
    ws = get_worksheet(SAILU_SHEET_NAME, key)
    updates, diffs = plan_sheet(ws, fixes)

    print(f"rows needing correction: {len(updates)}")
    for line in diffs[:SAMPLE_LIMIT]:
        print(line)
    if len(diffs) > SAMPLE_LIMIT:
        print(f"  ... and {len(diffs) - SAMPLE_LIMIT} more")

    if not updates:
        return 0
    if not apply:
        print("dry run — pass --apply to write")
        return len(updates)

    for i in range(0, len(updates), UPDATE_CHUNK):
        chunk = updates[i : i + UPDATE_CHUNK]
        with_retries(ws.batch_update, chunk, value_input_option="USER_ENTERED")
        print(f"wrote {i + 1}-{i + len(chunk)}")
        time.sleep(1.0)
    return len(updates)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="write changes (default: dry run)"
    )
    parser.add_argument(
        "--sheet",
        choices=("source", "target", "both"),
        default="both",
        help="which 賽錄 spreadsheet to process",
    )
    parser.add_argument(
        "--fix",
        choices=("qs", "ip", "both"),
        default="both",
        help="qs = 客QS/主QS flags, ip = 客投局/主投局 notation",
    )
    args = parser.parse_args()

    fixes = {"qs", "ip"} if args.fix == "both" else {args.fix}

    targets = []
    if args.sheet in ("source", "both"):
        targets.append((SAILU_SPREADSHEET_KEY, "source 賽錄"))
    if args.sheet in ("target", "both"):
        targets.append((SAILU_TARGET_SPREADSHEET_KEY, "target 賽錄"))

    total = sum(process(key, label, fixes, args.apply) for key, label in targets)
    verb = "corrected" if args.apply else "would correct"
    print(f"\n{verb} {total} cell(s)")


if __name__ == "__main__":
    main()
