"""Re-scrape a window of finished NPB games and diff them against the sheets.

The daily run is append-only, so a game recorded on the night it was played is
never revisited. NPB publishes 公式記録の訂正 days later, a scrape can fail
half-way, and a parser fix only helps games scraped after it shipped. This
walks back over the last N days, rebuilds each game's rows from a fresh scrape,
and reports every cell that disagrees with what the sheets hold.

Report-only by default. `--write-sheet` pastes the games that need changing
into the 資料更新 tab (same layout as 彙資, data from B3) so they can be eyeballed
before anything is overwritten in place.

    uv run python migration/audit_npb_history.py --days 30
    uv run python migration/audit_npb_history.py --days 30 --write-sheet
    uv run python migration/audit_npb_history.py --game-ids 2021039221
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta

import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import npb  # noqa: E402
import utils  # noqa: E402
from baseball.npb_audit import (  # noqa: E402
    ANALYSIS_WIDTH,
    SAILU_GAME_ID_INDEX,
    SAILU_LAST_RAW_INDEX,
    coverage_shortfall,
    diff_row,
    has_score_diff,
    is_blanked_scrape,
    sailu_game_ids_in_window,
    telegram_incomplete,
    telegram_summary,
    update_sheet_values,
)
from baseball.npb_services import NpbRowsService  # noqa: E402

UPDATE_SHEET_NAME = "資料更新"
UPDATE_FIRST_ROW = 3
CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".cache"
)


def _window(args) -> tuple[str, str]:
    end = (
        datetime.strptime(args.end, "%Y-%m-%d")
        if args.end
        else datetime.now() - timedelta(days=1)
    )
    start = (
        datetime.strptime(args.start, "%Y-%m-%d")
        if args.start
        else end - timedelta(days=args.days - 1)
    )
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


async def _scrape(game_ids: list[str]) -> list[tuple[str, dict]]:
    scraped: list[tuple[str, dict]] = []
    async with aiohttp.ClientSession(headers=npb.BROWSER_HEADERS) as session:
        for i in range(0, len(game_ids), npb.MAX_CONCURRENT):
            batch = game_ids[i : i + npb.MAX_CONCURRENT]
            results = await asyncio.gather(
                *[npb.get_schedule_game_data(gid, session, retry=True) for gid in batch],
                return_exceptions=True,
            )
            for gid, data in zip(batch, results):
                if isinstance(data, Exception):
                    print(f"  [audit] {gid}: scrape failed: {data}")
                elif data:
                    scraped.append((gid, data))
                else:
                    print(f"  [audit] {gid}: no data")
            done = min(i + npb.MAX_CONCURRENT, len(game_ids))
            print(f"  [audit] scraped {done}/{len(game_ids)}", flush=True)
            if done < len(game_ids):
                await asyncio.sleep(1)
    return scraped


def _sailu_rows_by_game_id(rows: list[list[str]]) -> dict[str, tuple[int, list[str]]]:
    indexed: dict[str, tuple[int, list[str]]] = {}
    for row_num, row in enumerate(rows[1:], start=2):
        if len(row) <= SAILU_GAME_ID_INDEX:
            continue
        game_id = str(row[SAILU_GAME_ID_INDEX]).strip()
        if game_id and game_id not in indexed:
            indexed[game_id] = (row_num, row)
    return indexed


def _analysis_rows_by_identity(rows: list[list[str]], rows_service):
    indexed: dict[tuple[str, str, str], tuple[int, list[str]]] = {}
    duplicates: set[tuple[str, str, str]] = set()
    for row_num, row in enumerate(rows[2:], start=3):
        identity = rows_service.analysis_identity_from_row(row)
        if not identity:
            continue
        if identity in indexed:
            duplicates.add(identity)
            continue
        indexed[identity] = (row_num, row)
    return indexed, duplicates


def audit(game_ids: list[str], scraped: list[tuple[str, dict]]) -> list[dict]:
    rows_service = NpbRowsService(module=npb)

    analysis_sheet = npb.get_worksheet(npb.ANALYSIS_SHEET_NAME, npb.NPB_SPREADSHEET_KEY)
    analysis_rows = analysis_sheet.get_all_values()
    analysis_index, duplicates = _analysis_rows_by_identity(analysis_rows, rows_service)

    sailu_sheets = {
        "source": npb.get_worksheet(npb.SAILU_SHEET_NAME, npb.SAILU_SPREADSHEET_KEY),
        "target": npb.get_worksheet(
            npb.SAILU_SHEET_NAME, npb.SAILU_TARGET_SPREADSHEET_KEY
        ),
    }
    sailu_index = {
        label: _sailu_rows_by_game_id(sheet.get_all_values())
        for label, sheet in sailu_sheets.items()
    }

    findings: list[dict] = []
    for game_id, data in scraped:
        identity = rows_service.analysis_identity(data)
        fresh_analysis = rows_service.analysis_row(0, data)
        fresh_sailu = rows_service.sailu_row(0, data)

        finding = {
            "game_id": game_id,
            "date": data["日期"],
            "identity": list(identity),
            "analysis": None,
            "sailu": {},
            "fresh_analysis_row": fresh_analysis,
            "notes": [],
        }

        if identity in duplicates:
            finding["notes"].append(
                "分析表紀錄 has more than one row for this date/matchup; skipped."
            )
        elif identity not in analysis_index:
            finding["notes"].append("分析表紀錄 has no row for this game.")
        else:
            row_num, sheet_row = analysis_index[identity]
            diffs = diff_row(sheet_row, fresh_analysis, last_index=ANALYSIS_WIDTH - 1)
            if is_blanked_scrape(diffs):
                # Every cell the sheet holds and this row does not: the page
                # was refused, not corrected. Reported as unread rather than
                # as a difference, so it can never be pasted over the record.
                finding["notes"].append(
                    f"分析表紀錄: the re-scrape came back blank in {len(diffs)} "
                    "cell(s) the sheet has values for; not compared."
                )
            elif diffs:
                finding["analysis"] = {"row": row_num, "diffs": diffs}

        for label, index in sailu_index.items():
            if game_id not in index:
                finding["notes"].append(f"賽錄 ({label}) has no row for this game.")
                continue
            row_num, sheet_row = index[game_id]
            diffs = diff_row(
                sheet_row, fresh_sailu, last_index=SAILU_LAST_RAW_INDEX
            )
            if is_blanked_scrape(diffs):
                finding["notes"].append(
                    f"賽錄 ({label}): the re-scrape came back blank in "
                    f"{len(diffs)} cell(s) the sheet has values for; not compared."
                )
            elif diffs:
                finding["sailu"][label] = {"row": row_num, "diffs": diffs}

        if finding["analysis"] or finding["sailu"] or finding["notes"]:
            findings.append(finding)

    missing = set(game_ids) - {gid for gid, _ in scraped}
    for game_id in sorted(missing):
        findings.append(
            {
                "game_id": game_id,
                "date": "",
                "identity": [],
                "analysis": None,
                "sailu": {},
                "fresh_analysis_row": [],
                "notes": ["Re-scrape returned nothing; could not be compared."],
            }
        )
    return findings


def _print_report(findings: list[dict], start: str, end: str, scanned: int) -> None:
    changed = [f for f in findings if f["analysis"] or f["sailu"]]
    print(f"\n=== NPB audit {start} → {end} ===")
    print(f"[audit] {scanned} game(s) re-scraped, {len(changed)} with differences.")

    for finding in findings:
        header = f"  {finding['date']} {'/'.join(finding['identity'])} ({finding['game_id']})"
        if not (finding["analysis"] or finding["sailu"] or finding["notes"]):
            continue
        print(header)
        for note in finding["notes"]:
            print(f"    ! {note}")
        if finding["analysis"]:
            block = finding["analysis"]
            flag = " [SCORE — do not auto-apply]" if has_score_diff(block["diffs"]) else ""
            print(f"    分析表紀錄 row {block['row']}{flag}")
            for diff in block["diffs"]:
                print(f"      {diff['column']}: {diff['sheet']!r} → {diff['fresh']!r}")
        for label, block in finding["sailu"].items():
            print(f"    賽錄 ({label}) row {block['row']}")
            for diff in block["diffs"]:
                print(f"      {diff['column']}: {diff['sheet']!r} → {diff['fresh']!r}")


def _save_report(findings: list[dict], start: str, end: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(CACHE_DIR, f"npb_audit_{stamp}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(
            {"window": {"start": start, "end": end}, "findings": findings},
            handle,
            ensure_ascii=False,
            indent=2,
        )
    return path


def write_update_sheet(findings: list[dict], *, dry_run: bool = False) -> int:
    """Paste the games that need changing into 資料更新, starting at B3."""
    changed = [f for f in findings if (f["analysis"] or f["sailu"]) and f["fresh_analysis_row"]]
    changed.sort(key=lambda f: (f["date"], f["game_id"]))

    sheet = npb.get_worksheet(UPDATE_SHEET_NAME, npb.NPB_SPREADSHEET_KEY)
    capacity = sheet.row_count - UPDATE_FIRST_ROW + 1
    if len(changed) > capacity:
        print(
            f"[audit] {len(changed)} game(s) need changing but {UPDATE_SHEET_NAME} "
            f"holds {capacity}; writing the {capacity} oldest."
        )
        changed = changed[:capacity]

    values = update_sheet_values([f["fresh_analysis_row"] for f in changed])
    last_row = sheet.row_count
    end_col = npb.col_to_letter(ANALYSIS_WIDTH)

    if dry_run:
        print(
            f"[audit] dry run: would clear B{UPDATE_FIRST_ROW}:{end_col}{last_row} "
            f"and write {len(values)} row(s)."
        )
        for row in values[:5]:
            print(f"    {row[:13]}")
        return len(values)

    sheet.batch_clear([f"B{UPDATE_FIRST_ROW}:{end_col}{last_row}"])
    if values:
        end_row = UPDATE_FIRST_ROW + len(values) - 1
        sheet.update(
            range_name=f"B{UPDATE_FIRST_ROW}:{end_col}{end_row}",
            values=values,
            value_input_option="USER_ENTERED",
        )
    print(f"[audit] Wrote {len(values)} row(s) to {UPDATE_SHEET_NAME}.")
    return len(values)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Re-scrape recent NPB games and diff them against the sheets."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=10,
        help="Window size in days. Defaults to 10 — a week plus slack, which "
             "is what a weekly sweep needs, and few enough games that Yahoo "
             "serves the whole window.",
    )
    parser.add_argument("--start", help="Window start, YYYY-MM-DD.")
    parser.add_argument("--end", help="Window end, YYYY-MM-DD. Defaults to yesterday.")
    parser.add_argument(
        "--game-ids", nargs="+", help="Audit these game IDs instead of a date window."
    )
    parser.add_argument(
        "--write-sheet",
        action="store_true",
        help=f"Paste the games needing changes into {UPDATE_SHEET_NAME} from B3.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --write-sheet, print the payload instead of writing it.",
    )
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Telegram the games that disagree with the sheets. A clean window "
             "sends nothing.",
    )
    args = parser.parse_args()

    if args.game_ids:
        game_ids = list(dict.fromkeys(args.game_ids))
        start = end = ""
        print(f"[audit] Auditing {len(game_ids)} game ID(s).")
    else:
        start, end = _window(args)
        sailu = npb.get_worksheet(npb.SAILU_SHEET_NAME, npb.SAILU_TARGET_SPREADSHEET_KEY)
        game_ids = sailu_game_ids_in_window(sailu.get_all_values(), start, end)
        print(f"[audit] {start} → {end}: {len(game_ids)} recorded game(s).")

    if not game_ids:
        print("[audit] Nothing to audit.")
        return 0

    scraped = asyncio.run(_scrape(game_ids))
    findings = audit(game_ids, scraped)
    _print_report(findings, start, end, len(scraped))
    print(f"[audit] Report saved to {_save_report(findings, start, end)}")

    shortfall = coverage_shortfall(len(scraped), len(game_ids))
    if shortfall:
        # Yahoo refused the rest of the window. The report is still on disk,
        # but nothing is written and nothing is announced as a finding: a
        # verdict drawn from the part that was read arrives wearing the same
        # green tick as a real one.
        print(f"[audit] {shortfall}; not writing {UPDATE_SHEET_NAME}.")
        if args.notify:
            utils.send_telegram(
                telegram_incomplete(
                    scanned=len(scraped),
                    requested=len(game_ids),
                    start=start,
                    end=end,
                )
            )
        return 1

    pasted = None
    if args.write_sheet:
        written = write_update_sheet(findings, dry_run=args.dry_run)
        if not args.dry_run:
            pasted = written

    if args.notify:
        # The audit corrects nothing on its own, so the report is only worth
        # producing if someone hears about it — but a note on a clean window
        # would train the reader to skip the one that matters.
        message = telegram_summary(
            findings, start=start, end=end, scanned=len(scraped), pasted=pasted
        )
        if message is None:
            print("[audit] Nothing disagrees with the sheets; no notification sent.")
        else:
            utils.send_telegram(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
