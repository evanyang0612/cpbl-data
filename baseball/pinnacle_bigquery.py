"""Load the pulled Pinnacle history (``.cache/pinnacle_history``) into BigQuery.

The local gzip files are the staging area; BigQuery is where the data lives,
so a lost laptop does not mean buying the archive again. Three tables in
``<project>.pinnacle_history`` (asia-east1), all partitioned by month of
``game_date`` (the league's local date) and clustered on league / event:

* ``fixtures`` — one row per event. It is loaded **last**, so it doubles as
  the commit marker: an event is "in BigQuery" once its fixture row is.
* ``odds``     — every price change of every line, both periods.
* ``results``  — per-period scores and settlement status.

A run killed between the odds load and the fixture load leaves odds rows with
no fixture; the next sync deletes those orphans and loads the event again, so
re-running is always safe. Load jobs are free; only the orphan check is a
(tiny) query.

    uv run python -m baseball.pinnacle_bigquery sync --league npb cpbl
"""

import argparse
import datetime as dt
import gzip
import json
import os
import pathlib

from google.oauth2 import service_account

from baseball.pinnacle_history import CACHE_ROOT, LEAGUE_IDS, _parse_ts, done_ids
from baseball.pinnacle_odds import ET, JST, TPE

PROJECT = os.getenv("PINNACLE_BQ_PROJECT", "project-e0a5748a-0bec-4063-a99")
DATASET = os.getenv("PINNACLE_BQ_DATASET", "pinnacle_history")
LOCATION = "asia-east1"
LEAGUE_TZ = {"npb": JST, "cpbl": TPE, "mlb": ET}
# ~1,800 rows an event, so 200 events is a ~360k-row load: big enough to keep
# the job count low, small enough that a failure costs little to redo.
BATCH_EVENTS = 200

SCHEMAS = {
    "fixtures": [
        ("league", "STRING"), ("event_id", "INT64"), ("game_date", "DATE"),
        ("starts", "TIMESTAMP"), ("runner_home", "STRING"), ("runner_away", "STRING"),
        ("event_status", "STRING"), ("version", "INT64"), ("odds_rows", "INT64"),
        ("fetched_at", "TIMESTAMP"),
    ],
    "odds": [
        ("league", "STRING"), ("event_id", "INT64"), ("game_date", "DATE"),
        ("period", "INT64"), ("market", "STRING"), ("line", "FLOAT64"),
        ("line_id", "INT64"), ("alt_line_id", "INT64"),
        ("odds1", "FLOAT64"), ("odds0", "FLOAT64"), ("odds2", "FLOAT64"),
        ("todds1", "FLOAT64"), ("todds0", "FLOAT64"), ("todds2", "FLOAT64"),
        ("timestamp", "TIMESTAMP"), ("cutoff", "TIMESTAMP"), ("max_win", "FLOAT64"),
        ("status", "INT64"), ("is_open", "INT64"), ("bettable", "INT64"),
        # tick = a row of the full history; opening / closing = a line's
        # first / last price from those endpoints (the default pull since
        # 2026-11-23). Added after the table, so it is last.
        ("kind", "STRING"),
    ],
    "results": [
        ("league", "STRING"), ("event_id", "INT64"), ("game_date", "DATE"),
        ("period", "INT64"), ("status", "INT64"), ("score_home", "INT64"),
        ("score_away", "INT64"), ("settlement_id", "INT64"),
        ("cancellation_reason", "STRING"), ("timestamp", "TIMESTAMP"),
    ],
    # The 盤口 ledger's own columns (every league's union, so the live Sheets
    # tabs can be copied in later unchanged) plus ``league_key``. Built from the
    # raw history by baseball.pinnacle_snapshots; see that module for the rules.
    "odds_snapshots": [
        ("league_key", "STRING"), ("captured_at", "DATETIME"),
        ("snapshot_type", "STRING"), ("event_id", "INT64"), ("game_date", "DATE"),
        ("start_jst", "STRING"), ("start_tpe", "STRING"), ("start_et", "STRING"),
        ("league", "STRING"), ("home_team", "STRING"), ("away_team", "STRING"),
        ("home_norm", "STRING"), ("away_norm", "STRING"),
        ("home_abbr", "STRING"), ("away_abbr", "STRING"), ("kind_code", "STRING"),
        ("npb_game_id", "STRING"), ("cpbl_game_sno", "STRING"),
        ("mlb_game_pk", "STRING"), ("status", "STRING"), ("mins_to_start", "INT64"),
        ("period", "STRING"), ("ml_home", "FLOAT64"), ("ml_away", "FLOAT64"),
        ("ml_draw", "FLOAT64"), ("total_line", "FLOAT64"), ("total_over", "FLOAT64"),
        ("total_under", "FLOAT64"), ("spread_hdp", "FLOAT64"),
        ("spread_home", "FLOAT64"), ("spread_away", "FLOAT64"),
        ("all_totals", "STRING"), ("all_spreads", "STRING"), ("source", "STRING"),
    ],
}
CLUSTERING = {"odds_snapshots": ["league_key", "event_id"]}


# --- flattening -------------------------------------------------------------

def _ts(value):
    if value in (None, ""):
        return None
    return _parse_ts(value).astimezone(dt.timezone.utc).isoformat(sep=" ")


def _cast(value, kind):
    if value in (None, ""):
        return None
    if kind == "FLOAT64":
        return float(value)
    if kind == "INT64":
        return int(float(value))
    if kind == "TIMESTAMP":
        return _ts(value)
    if kind in ("DATE", "DATETIME"):
        return str(value)
    return str(value)


def _row(source: dict, table: str, base: dict) -> dict:
    """Pick the table's columns from an API row, typed; unknown keys are dropped."""
    row = dict(base)
    for name, kind in SCHEMAS[table]:
        if name not in row:
            row[name] = _cast(source.get(name), kind)
    return row


def to_rows(record: dict, league: str) -> tuple[dict, list[dict], list[dict]]:
    fixture = record["fixture"]
    starts = _parse_ts(fixture["starts"])
    base = {"league": league, "event_id": int(fixture["event_id"]),
            "game_date": starts.astimezone(LEAGUE_TZ[league]).date().isoformat()}
    if "opening" in record:
        odds = ([_row(r, "odds", {**base, "kind": "opening"}) for r in record["opening"]]
                + [_row(r, "odds", {**base, "kind": "closing"}) for r in record["closing"]])
    else:
        odds = [_row(r, "odds", {**base, "kind": "tick"}) for r in record["odds"]]
    fixture_row = _row({**fixture, "odds_rows": len(odds),
                        "fetched_at": record.get("fetched_at")}, "fixtures", base)
    results = [_row(r, "results", base) for r in record["results"]]
    return fixture_row, odds, results


def snapshot_rows(record: dict, league: str) -> list[dict]:
    from baseball.pinnacle_snapshots import event_snapshots
    return [_row(s, "odds_snapshots", {"league_key": league})
            for s in event_snapshots(record, league)]


# --- sync -------------------------------------------------------------------

def _read(events_dir, event_id) -> dict:
    with gzip.open(pathlib.Path(events_dir) / f"{event_id}.json.gz", "rt",
                   encoding="utf-8") as fh:
        return json.load(fh)


def sync(events_dir, league: str, store, *, batch_events: int = BATCH_EVENTS) -> int:
    """Load every cached event BigQuery does not have yet. Returns how many."""
    store.delete_orphans()
    todo = sorted(done_ids(events_dir) - store.loaded_ids(league))
    for i in range(0, len(todo), batch_events):
        fixtures, odds, results = [], [], []
        for event_id in todo[i:i + batch_events]:
            f, o, r = to_rows(_read(events_dir, event_id), league)
            fixtures.append(f)
            odds.extend(o)
            results.extend(r)
        store.load("odds", odds)
        store.load("results", results)
        store.load("fixtures", fixtures)   # last: marks the batch as loaded
        print(f"[pinnacle_bigquery] {league.upper()} +{len(fixtures)} 場 / "
              f"{len(odds)} 列盤口（{min(i + batch_events, len(todo))}/{len(todo)}）")
    return len(todo)


def sync_snapshots(events_dir, league: str, store, *,
                   batch_events: int = BATCH_EVENTS) -> int:
    """Build 盤口-format snapshots for events already in BigQuery.

    Derived data, so it is rebuilt from the local files rather than carried in
    the raw sync: change the rules, drop the table, run this again. One load
    job per batch is atomic, so a crash leaves no half-built event.
    """
    from baseball.pinnacle_snapshots import SOURCE
    # Only rows this builder made: the scraper's rows copied from the sheets
    # carry the same Pinnacle event ids and say nothing about ours.
    todo = sorted((done_ids(events_dir) & store.loaded_ids(league))
                  - store.snapshot_ids(league, SOURCE))
    for i in range(0, len(todo), batch_events):
        rows = [row for event_id in todo[i:i + batch_events]
                for row in snapshot_rows(_read(events_dir, event_id), league)]
        store.load("odds_snapshots", rows)
        print(f"[pinnacle_bigquery] {league.upper()} 盤口快照 +{len(rows)} 列"
              f"（{min(i + batch_events, len(todo))}/{len(todo)} 場）")
    return len(todo)


SCOPES = ["https://www.googleapis.com/auth/bigquery"]


def _credentials(credentials_file: str | None = None):
    """The same service account the Sheets code uses: GitHub Actions passes its
    JSON in ``GOOGLE_CREDENTIALS``, a laptop points ``GOOGLE_CREDENTIALS_FILE``
    at the key file."""
    raw = os.environ.get("GOOGLE_CREDENTIALS")
    if raw and not credentials_file:
        return service_account.Credentials.from_service_account_info(
            json.loads(raw), scopes=SCOPES)
    return service_account.Credentials.from_service_account_file(
        credentials_file or os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES)


class BigQueryStore:
    def __init__(self, project: str = PROJECT, dataset: str = DATASET,
                 credentials_file: str | None = None, *, ensure: bool = True):
        from google.cloud import bigquery

        self.bq = bigquery
        self.client = bigquery.Client(project=project,
                                      credentials=_credentials(credentials_file),
                                      location=LOCATION)
        self.dataset = f"{project}.{dataset}"
        if ensure:
            self._ensure()

    def _table(self, name: str) -> str:
        return f"{self.dataset}.{name}"

    def _schema(self, name: str):
        return [self.bq.SchemaField(col, kind) for col, kind in SCHEMAS[name]]

    def _ensure(self) -> None:
        ds = self.bq.Dataset(self.dataset)
        ds.location = LOCATION
        self.client.create_dataset(ds, exists_ok=True)
        for name in SCHEMAS:
            table = self.bq.Table(self._table(name), schema=self._schema(name))
            table.time_partitioning = self.bq.TimePartitioning(
                type_=self.bq.TimePartitioningType.MONTH, field="game_date")
            table.clustering_fields = CLUSTERING.get(name, ["league", "event_id"])
            table = self.client.create_table(table, exists_ok=True)
            # A column added to SCHEMAS after the table was created
            have = {f.name for f in table.schema}
            missing = [f for f in self._schema(name) if f.name not in have]
            if missing:
                table.schema = list(table.schema) + missing
                self.client.update_table(table, ["schema"])

    def loaded_ids(self, league: str) -> set[int]:
        job = self.client.query(
            f"SELECT event_id FROM `{self._table('fixtures')}` WHERE league = @league",
            job_config=self.bq.QueryJobConfig(query_parameters=[
                self.bq.ScalarQueryParameter("league", "STRING", league)]))
        return {row.event_id for row in job.result()}

    def snapshot_ids(self, league: str, source: str) -> set[int]:
        job = self.client.query(
            f"SELECT DISTINCT event_id FROM `{self._table('odds_snapshots')}` "
            "WHERE league_key = @league AND source = @source",
            job_config=self.bq.QueryJobConfig(query_parameters=[
                self.bq.ScalarQueryParameter("league", "STRING", league),
                self.bq.ScalarQueryParameter("source", "STRING", source)]))
        return {row.event_id for row in job.result()}

    def ledger_rows(self, league_key: str, exclude_source: str | None = None) -> list[dict]:
        """Every 盤口 row of a league, oldest capture first."""
        sql = (f"SELECT * FROM `{self._table('odds_snapshots')}` "
               "WHERE league_key = @league AND source != IFNULL(@exclude, '')"
               " ORDER BY captured_at")
        job = self.client.query(sql, job_config=self.bq.QueryJobConfig(query_parameters=[
            self.bq.ScalarQueryParameter("league", "STRING", league_key),
            self.bq.ScalarQueryParameter("exclude", "STRING", exclude_source)]))
        return [dict(row.items()) for row in job.result()]

    def delete_orphans(self) -> None:
        fixtures = self._table("fixtures")
        for name in ("odds", "results"):
            self.client.query(
                f"DELETE FROM `{self._table(name)}` WHERE event_id NOT IN "
                f"(SELECT event_id FROM `{fixtures}`)").result()

    def load(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        config = self.bq.LoadJobConfig(
            schema=self._schema(table),
            write_disposition=self.bq.WriteDisposition.WRITE_APPEND)
        self.client.load_table_from_json(rows, self._table(table),
                                         job_config=config).result()


def sync_leagues(leagues, store=None) -> None:
    store = store or BigQueryStore()
    for league in leagues:
        sync(CACHE_ROOT / league / "events", league, store)
        sync_snapshots(CACHE_ROOT / league / "events", league, store)


def main() -> None:
    from dotenv import load_dotenv
    load_dotenv(CACHE_ROOT.parent.parent / ".env")

    parser = argparse.ArgumentParser(description="Load the Pinnacle history into BigQuery")
    parser.add_argument("command", choices=["sync"])
    parser.add_argument("--league", nargs="+", default=["npb", "cpbl", "mlb"],
                        choices=sorted(LEAGUE_IDS))
    args = parser.parse_args()
    sync_leagues(args.league)


if __name__ == "__main__":
    main()
