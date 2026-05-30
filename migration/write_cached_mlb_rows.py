"""Resume writing cached MLB rows to the 紀錄 worksheet."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import requests
from gspread.exceptions import APIError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cpbl import _sheets_client


SPREADSHEET_KEY = "11FV70TXVAxLTwYH6pLj7HwK1qq-fIa61QrePRCC8YUM"
WORKSHEET_NAME = "紀錄"
RAW_END_COLUMN = "AO"


def _with_retries(label: str, fn: Any) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            return fn()
        except (APIError, requests.RequestException) as exc:
            last_error = exc
            if attempt == 5:
                break
            sleep_for = min(2**attempt, 20)
            print(f"{label} failed ({attempt}/5), retrying in {sleep_for}s", flush=True)
            time.sleep(sleep_for)
    raise RuntimeError(f"{label} failed after retries") from last_error


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cache_path")
    parser.add_argument("--start-row", type=int, required=True)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--chunk-size", type=int, default=500)
    args = parser.parse_args()

    rows = json.loads(Path(args.cache_path).read_text())
    remaining = rows[args.offset :]
    worksheet = _sheets_client.worksheet(SPREADSHEET_KEY, WORKSHEET_NAME)
    print(
        f"Writing {len(remaining)} cached row(s) from row {args.start_row}",
        flush=True,
    )

    for offset in range(0, len(remaining), args.chunk_size):
        chunk = remaining[offset : offset + args.chunk_size]
        chunk_start = args.start_row + offset
        chunk_end = chunk_start + len(chunk) - 1
        _with_retries(
            f"write rows {chunk_start}:{chunk_end}",
            lambda chunk_start=chunk_start, chunk_end=chunk_end, chunk=chunk: worksheet.update(
                range_name=f"A{chunk_start}:{RAW_END_COLUMN}{chunk_end}",
                values=chunk,
                value_input_option="USER_ENTERED",
            ),
        )
        print(f"Wrote rows {chunk_start}:{chunk_end}", flush=True)


if __name__ == "__main__":
    main()
