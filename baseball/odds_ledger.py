"""The 盤口 ledger, kept in BigQuery (``pinnacle_history.odds_snapshots``).

Until the 2026-27 off-season every league's odds snapshots were appended to a
``盤口`` worksheet. The MLB one alone grew 1.2M cells in four months, and the
ledger now also holds the Pinnacle history backfill, so it lives in BigQuery
and the worksheets are frozen as they were on 2026-11-23.

The columns are still the worksheet's (``LeagueSpec.sheet_headers()``) plus
``league_key``, so everything that thinks in sheet rows keeps working:

* the scraper builds rows with ``snapshots_to_rows`` and hands them to
  ``append``;
* readers that took a worksheet's ``get_all_values()`` call ``read_values``,
  which returns the same header-plus-strings shape;
* the models' ``--odds`` CSV comes from ``export``.

    uv run python -m baseball.odds_ledger export --league npb > 盤口.csv
"""

import argparse
import csv
import datetime as dt
import re
import sys

from baseball import pinnacle_bigquery as pbq
from baseball.pinnacle_odds import LEAGUES, LeagueSpec

TABLE = "odds_snapshots"
# Rows written before the worksheets had a ``source`` column all came from the scraper.
DEFAULT_SOURCE = "ps3838"
# The Pinnacle-history backfill. Derived, not captured: readers that audit the
# scraper leave it out.
DERIVED_SOURCE = "bettingiscool"


def _pad_time(value: str) -> str:
    """Sheets re-renders "09:31:00" as "9:31:00"; DATETIME wants two digits."""
    return re.sub(r" (\d):", r" 0\1:", str(value).strip())


def to_records(header: list[str], rows: list[list], league_key: str) -> list[dict]:
    """Sheet-shaped rows -> typed BigQuery records. Rows with no capture time are dropped."""
    out = []
    for row in rows:
        values = {h: (row[i] if i < len(row) else "") for i, h in enumerate(header)}
        if not values.get("captured_at"):
            continue
        values["captured_at"] = _pad_time(values["captured_at"])
        values["source"] = values.get("source") or DEFAULT_SOURCE
        out.append(pbq._row(values, TABLE, {"league_key": league_key}))
    return out


def _store():
    return pbq.BigQueryStore(ensure=False)


def append(rows: list[list], league: LeagueSpec, store=None) -> int:
    """Append scraper rows (``snapshots_to_rows`` output) to the ledger."""
    if not rows:
        return 0
    records = to_records(league.sheet_headers(), rows, league.key)
    (store or _store()).load(TABLE, records)
    return len(records)


def _text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value)


def read_values(league: LeagueSpec, store=None, *,
                include_derived: bool = False) -> list[list[str]]:
    """The ledger as a worksheet's ``get_all_values()`` would have returned it."""
    header = league.sheet_headers()
    rows = (store or _store()).ledger_rows(
        league.key, exclude_source=None if include_derived else DERIVED_SOURCE)
    return [header] + [[_text(r.get(h)) for h in header] for r in rows]


def main() -> None:
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="The 盤口 ledger in BigQuery")
    parser.add_argument("command", choices=["export"])
    parser.add_argument("--league", default="npb", choices=sorted(LEAGUES))
    parser.add_argument("--include-derived", action="store_true",
                        help="also export the Pinnacle-history (bettingiscool) rows")
    args = parser.parse_args()
    values = read_values(LEAGUES[args.league], include_derived=args.include_derived)
    csv.writer(sys.stdout).writerows(values)
    print(f"[odds_ledger] {args.league.upper()} {len(values) - 1} 列", file=sys.stderr)


if __name__ == "__main__":
    main()
