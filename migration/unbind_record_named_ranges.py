"""Point 紀錄's per-row formulas at their own row instead of a whole column.

Columns AT:BD hold per-row verdicts — quality start, division matchup, win or
loss — written as `IF(AND(客先局>=5,客自責<=2),...)`. Those names are named
ranges with no row bounds, so each one covers all 22k rows of its column.
Sheets resolves them by implicit intersection and computes the right answer,
but the dependency it records is the whole column: every one of those cells
declares that it depends on 22k others. Writing a single game therefore
dirties the lot, and the recalculation that follows makes reads of the same
workbook time out at three minutes and answer 503 — 4s of reading turned into
483s of waiting in a measured before-and-after.

Replacing each name with the same-row A1 reference cannot change a single
computed value, because implicit intersection already resolved to that cell.
It only tells Sheets the truth about what the formula reads.

Rows whose AT:BD are static values are left static; only cells that currently
hold a formula are rewritten.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from gspread.utils import rowcol_to_a1

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cpbl import _sheets_client


SPREADSHEET_KEY = "11FV70TXVAxLTwYH6pLj7HwK1qq-fIa61QrePRCC8YUM"
WORKSHEET_NAME = "紀錄"
FORMULA_START_COL_0IDX = 41
FORMULA_END_COL_0IDX = 56

# A name is only a name when the whole token matches: 五總分 contains 五總 and
# 客五總 ends with it, so a substring replace would corrupt both.
_TOKEN = re.compile(r"[0-9A-Za-z_一-鿿]+")
_STRING = re.compile(r'"[^"]*"')


def rewrite_formula(formula: str, row: int, columns: dict[str, str]) -> str:
    """Rewrite whole-column names in `formula` as references to `row`."""
    if not formula.startswith("="):
        return formula
    parts: list[str] = []
    position = 0
    for literal in _STRING.finditer(formula):
        parts.append(_substitute(formula[position : literal.start()], row, columns))
        parts.append(literal.group())
        position = literal.end()
    parts.append(_substitute(formula[position:], row, columns))
    return "".join(parts)


def _substitute(text: str, row: int, columns: dict[str, str]) -> str:
    return _TOKEN.sub(
        lambda match: (
            f"{columns[match.group()]}{row}"
            if match.group() in columns
            else match.group()
        ),
        text,
    )


def whole_column_names(metadata: dict[str, Any], sheet_id: int) -> dict[str, str]:
    """Map each unbounded single-column named range on `sheet_id` to its column."""
    columns: dict[str, str] = {}
    for named_range in metadata.get("namedRanges", []):
        target = named_range["range"]
        if target.get("sheetId") != sheet_id or "startRowIndex" in target:
            continue
        start = target.get("startColumnIndex")
        end = target.get("endColumnIndex")
        if start is None or end is None or end - start != 1:
            continue
        columns[named_range["name"]] = rowcol_to_a1(1, start + 1).rstrip("1")
    return columns


def _runs(rows: list[int]) -> list[tuple[int, int]]:
    """Collapse sorted row numbers into contiguous (first, last) spans."""
    spans: list[tuple[int, int]] = []
    for row in rows:
        if spans and row == spans[-1][1] + 1:
            spans[-1] = (spans[-1][0], row)
        else:
            spans.append((row, row))
    return spans


def plan_updates(
    formulas: list[list[Any]], columns: dict[str, str], first_row: int = 2
) -> list[dict[str, Any]]:
    """Build the value ranges that rewrite every formula cell that changes."""
    rewritten: dict[str, dict[int, str]] = {}
    for offset, row_values in enumerate(formulas):
        row = first_row + offset
        for index in range(FORMULA_END_COL_0IDX - FORMULA_START_COL_0IDX):
            cell = str(row_values[index]) if index < len(row_values) else ""
            if not cell.startswith("="):
                continue
            new = rewrite_formula(cell, row, columns)
            if new == cell:
                continue
            column = rowcol_to_a1(1, FORMULA_START_COL_0IDX + index + 1).rstrip("1")
            rewritten.setdefault(column, {})[row] = new

    updates: list[dict[str, Any]] = []
    for column, by_row in sorted(rewritten.items()):
        for start, end in _runs(sorted(by_row)):
            updates.append(
                {
                    "range": f"'{WORKSHEET_NAME}'!{column}{start}:{column}{end}",
                    "values": [[by_row[row]] for row in range(start, end + 1)],
                }
            )
    return updates


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the rewritten formulas; without it, only report the plan",
    )
    parser.add_argument(
        "--plan-out", help="write the planned value ranges to this JSON file"
    )
    args = parser.parse_args()

    spreadsheet = _sheets_client.spreadsheet(SPREADSHEET_KEY)
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    metadata = spreadsheet.fetch_sheet_metadata()
    columns = whole_column_names(metadata, worksheet.id)
    print(f"{len(columns)} whole-column named range(s) on {WORKSHEET_NAME}", flush=True)

    last_column = rowcol_to_a1(1, FORMULA_END_COL_0IDX).rstrip("1")
    first_column = rowcol_to_a1(1, FORMULA_START_COL_0IDX + 1).rstrip("1")
    range_name = f"'{WORKSHEET_NAME}'!{first_column}2:{last_column}{worksheet.row_count}"
    formulas = spreadsheet.values_batch_get(
        [range_name], params={"valueRenderOption": "FORMULA"}
    )["valueRanges"][0].get("values", [])
    print(f"Read {len(formulas)} row(s) of formulas", flush=True)

    updates = plan_updates(formulas, columns)
    cells = sum(len(update["values"]) for update in updates)
    print(f"{cells} cell(s) to rewrite across {len(updates)} contiguous range(s)")
    for update in updates[:5]:
        print(f"  {update['range']}  e.g. {update['values'][0][0][:110]}")

    if args.plan_out:
        Path(args.plan_out).write_text(json.dumps(updates, ensure_ascii=False))
        print(f"Plan written to {args.plan_out}", flush=True)

    if not args.apply:
        print("Dry run; pass --apply to write.", flush=True)
        return

    spreadsheet.values_batch_update(
        {"valueInputOption": "USER_ENTERED", "data": updates}
    )
    print(f"Rewrote {cells} cell(s).", flush=True)


if __name__ == "__main__":
    main()
