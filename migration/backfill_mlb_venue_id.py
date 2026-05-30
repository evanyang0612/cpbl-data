"""Add and backfill venue_id in the MLB 紀錄 worksheet."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from gspread.exceptions import APIError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cpbl import _sheets_client


SPREADSHEET_KEY = "11FV70TXVAxLTwYH6pLj7HwK1qq-fIa61QrePRCC8YUM"
WORKSHEET_NAME = "紀錄"
MLB_API = "https://statsapi.mlb.com/api"
REQUEST_TIMEOUT = (10, 45)

GAME_PK_COL = 2
DATE_COL = 1
VENUE_NAME_COL = 33
VENUE_ID_COL = 34
VENUE_ID_COL_LETTER = "AH"
VENUE_ID_HEADER = "venue_id"


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


def _get_json(session: requests.Session, url: str, **params: Any) -> dict[str, Any]:
    return _with_retries(
        f"fetch {url}",
        lambda: _raise_for_json(
            session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        ),
    )


def _raise_for_json(response: requests.Response) -> dict[str, Any]:
    response.raise_for_status()
    return response.json()


def _ensure_venue_id_column(worksheet: Any, dry_run: bool) -> bool:
    headers = _with_retries("read header row", lambda: worksheet.row_values(1))
    current_header = (
        headers[VENUE_ID_COL - 1] if len(headers) >= VENUE_ID_COL else ""
    ).strip()
    if current_header == VENUE_ID_HEADER:
        return False
    if VENUE_ID_HEADER in headers:
        raise RuntimeError(
            f"{VENUE_ID_HEADER} already exists outside column {VENUE_ID_COL}."
        )
    venue_name_header = (
        headers[VENUE_NAME_COL - 1] if len(headers) >= VENUE_NAME_COL else ""
    ).strip()
    if venue_name_header != "球場名稱":
        raise RuntimeError(
            f"Expected 球場名稱 in column {VENUE_NAME_COL}, got {venue_name_header!r}."
        )
    if dry_run:
        print(f"Would insert {VENUE_ID_HEADER} at column {VENUE_ID_COL}.")
        return True

    spreadsheet = worksheet.spreadsheet
    _with_retries(
        "insert venue_id column",
        lambda: spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "insertDimension": {
                            "range": {
                                "sheetId": worksheet.id,
                                "dimension": "COLUMNS",
                                "startIndex": VENUE_ID_COL - 1,
                                "endIndex": VENUE_ID_COL,
                            },
                            "inheritFromBefore": True,
                        }
                    }
                ]
            }
        ),
    )
    _with_retries(
        "write venue_id header",
        lambda: worksheet.update(
            range_name=f"{VENUE_ID_COL_LETTER}1",
            values=[[VENUE_ID_HEADER]],
            value_input_option="USER_ENTERED",
        ),
    )
    return True


def _venue_id_for_game(session: requests.Session, game_pk: str) -> str:
    feed = _get_json(session, f"{MLB_API}/v1.1/game/{game_pk}/feed/live")
    venue_id = feed.get("gameData", {}).get("venue", {}).get("id", "")
    return str(venue_id) if venue_id not in ("", None) else ""


def _parse_sheet_date(value: str) -> date | None:
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def _venue_ids_from_schedule(
    session: requests.Session, start_date: date, end_date: date
) -> dict[str, str]:
    venue_ids: dict[str, str] = {}
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(cursor + timedelta(days=30), end_date)
        data = _get_json(
            session,
            f"{MLB_API}/v1/schedule",
            sportId=1,
            startDate=cursor.isoformat(),
            endDate=chunk_end.isoformat(),
            hydrate="venue",
        )
        for day in data.get("dates", []):
            for game in day.get("games", []):
                venue_id = game.get("venue", {}).get("id")
                if venue_id not in ("", None):
                    venue_ids[str(game.get("gamePk"))] = str(venue_id)
        print(
            f"Fetched schedule venue ids through {chunk_end.isoformat()}",
            flush=True,
        )
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.03)
    return venue_ids


def _last_dated_row(date_values: list[str]) -> int:
    for index in range(len(date_values), 0, -1):
        if str(date_values[index - 1]).strip():
            return index
    return 1


def backfill_venue_ids(dry_run: bool = False) -> int:
    worksheet = _sheets_client.worksheet(SPREADSHEET_KEY, WORKSHEET_NAME)
    inserted_or_would_insert = _ensure_venue_id_column(worksheet, dry_run)

    date_values = _with_retries(
        "read date column", lambda: worksheet.col_values(DATE_COL)
    )
    last_row = _last_dated_row(date_values)
    game_pks = [
        row[0] if row else ""
        for row in _with_retries(
            "read game ids", lambda: worksheet.get(f"B1:B{last_row}")
        )
    ]
    existing_venue_ids = (
        []
        if inserted_or_would_insert
        else [
            row[0] if row else ""
            for row in _with_retries(
                "read venue ids",
                lambda: worksheet.get(
                    f"{VENUE_ID_COL_LETTER}1:{VENUE_ID_COL_LETTER}{last_row}"
                ),
            )
        ]
    )
    values: list[list[str]] = []
    session = requests.Session()
    updated = 0
    missing_game_pks: set[str] = set()

    for row_number in range(2, last_row + 1):
        game_pk = (
            game_pks[row_number - 1].strip()
            if row_number <= len(game_pks)
            else ""
        )
        existing = (
            existing_venue_ids[row_number - 1].strip()
            if row_number <= len(existing_venue_ids)
            else ""
        )
        if not game_pk or existing:
            values.append([existing])
            continue
        if dry_run:
            values.append([""])
            updated += 1
            continue
        missing_game_pks.add(game_pk)
        values.append([""])
        updated += 1

    venue_id_by_game_pk: dict[str, str] = {}
    if missing_game_pks and not dry_run:
        sheet_dates = [
            parsed
            for parsed in (_parse_sheet_date(value) for value in date_values[1:last_row])
            if parsed
        ]
        if not sheet_dates:
            raise RuntimeError("No valid dates found for schedule venue_id backfill.")
        venue_id_by_game_pk = _venue_ids_from_schedule(
            session, min(sheet_dates), max(sheet_dates)
        )

    fallback_fetches = 0
    for index, row_number in enumerate(range(2, last_row + 1)):
        if values[index][0]:
            continue
        game_pk = (
            game_pks[row_number - 1].strip()
            if row_number <= len(game_pks)
            else ""
        )
        if not game_pk or dry_run:
            continue
        venue_id = venue_id_by_game_pk.get(game_pk)
        if not venue_id:
            venue_id = _venue_id_for_game(session, game_pk)
            fallback_fetches += 1
            time.sleep(0.03)
        values[index][0] = venue_id
        if (index + 1) % 500 == 0:
            print(f"Resolved venue_id through row {row_number}", flush=True)

    if fallback_fetches:
        print(f"Fetched {fallback_fetches} venue_id value(s) from feed fallback.")

    if dry_run:
        print(f"Would backfill {updated} missing venue_id value(s).")
        return updated

    if values:
        _with_retries(
            "write venue ids",
            lambda: worksheet.update(
                range_name=f"{VENUE_ID_COL_LETTER}2:{VENUE_ID_COL_LETTER}{last_row}",
                values=values,
                value_input_option="USER_ENTERED",
            ),
        )
    return updated


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    count = backfill_venue_ids(dry_run=args.dry_run)
    print(f"Done. {'Would backfill' if args.dry_run else 'Backfilled'} {count} row(s).")


if __name__ == "__main__":
    main()
