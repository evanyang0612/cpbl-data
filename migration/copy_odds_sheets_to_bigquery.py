"""Copy every league's 盤口 tab into BigQuery ``pinnacle_history.odds_snapshots``.

Same columns as the sheets (the table was laid out from them), plus
``league_key``. The sheets are only read, never changed.

Re-runnable: each league's sheet-sourced rows (anything not
``source = bettingiscool``) are deleted and loaded again, so a second run
replaces rather than duplicates. Run it while nothing is writing to the
sheets — in the off-season, or with the scraper paused.

    uv run python migration/copy_odds_sheets_to_bigquery.py --dry-run
    uv run python migration/copy_odds_sheets_to_bigquery.py
"""

import argparse
import sys

sys.path.insert(0, "/Users/evansmac/cpbl")

from dotenv import load_dotenv  # noqa: E402

load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")

from baseball import pinnacle_bigquery as pbq  # noqa: E402
from baseball import pinnacle_odds as po  # noqa: E402
from baseball.odds_ledger import DERIVED_SOURCE, to_records  # noqa: E402,F401

SHEET_NAME = "盤口"


def safe_to_replace(sheet_latest: str, ledger_latest: str | None) -> bool:
    """False once the ledger holds a capture newer than the frozen sheet: those
    rows were written straight to BigQuery and a reload would delete them."""
    return ledger_latest is None or ledger_latest <= sheet_latest


def main(dry_run: bool) -> None:
    from baseball.sheets import GoogleSheetsClient

    client = GoogleSheetsClient()
    store = None if dry_run else pbq.BigQueryStore()
    for spec in (po.NPB, po.CPBL, po.MLB):
        values = client.spreadsheet(spec.spreadsheet_key()).worksheet(SHEET_NAME).get_all_values()
        records = to_records(values[0], values[1:], spec.key)
        sources = {}
        for r in records:
            sources[r["source"]] = sources.get(r["source"], 0) + 1
        print(f"{spec.key.upper():<5} 試算表 {len(values) - 1} 列 -> {len(records)} 筆 {sources}")
        if dry_run:
            continue
        table = store._table("odds_snapshots")
        params = store.bq.QueryJobConfig(query_parameters=[
            store.bq.ScalarQueryParameter("league", "STRING", spec.key),
            store.bq.ScalarQueryParameter("derived", "STRING", DERIVED_SOURCE)])
        [latest] = store.client.query(
            f"SELECT FORMAT_DATETIME('%F %T', MAX(captured_at)) t FROM `{table}` "
            "WHERE league_key = @league AND source != @derived", job_config=params).result()
        sheet_latest = max(r["captured_at"] for r in records)
        if not safe_to_replace(sheet_latest, latest.t):
            print(f"      ✗ BigQuery 有 {latest.t} 的資料，比試算表最後一筆 {sheet_latest} 新："
                  "爬蟲已經直接寫入 BigQuery，不能再從試算表重載，跳過")
            continue
        store.client.query(
            f"DELETE FROM `{table}` WHERE league_key = @league AND source != @derived",
            job_config=store.bq.QueryJobConfig(query_parameters=[
                store.bq.ScalarQueryParameter("league", "STRING", spec.key),
                store.bq.ScalarQueryParameter("derived", "STRING", DERIVED_SOURCE)]),
        ).result()
        store.load("odds_snapshots", records)
        [count] = store.client.query(
            f"SELECT COUNT(*) n FROM `{table}` WHERE league_key = @league AND source != @derived",
            job_config=store.bq.QueryJobConfig(query_parameters=[
                store.bq.ScalarQueryParameter("league", "STRING", spec.key),
                store.bq.ScalarQueryParameter("derived", "STRING", DERIVED_SOURCE)]),
        ).result()
        status = "✓" if count.n == len(records) else "✗ 數量不符"
        print(f"      BigQuery 現有 {count.n} 筆 {status}")
    if dry_run:
        print("（--dry-run，什麼都沒寫）")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    main(dry_run=parser.parse_args().dry_run)
