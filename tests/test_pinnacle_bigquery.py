"""Unit tests for loading the Pinnacle history into BigQuery (baseball/pinnacle_bigquery.py)."""

from baseball import pinnacle_bigquery as pbq
from baseball import pinnacle_history as ph


def _record(event_id=1634054636, starts="2026-08-22T09:00:00Z"):
    return {
        "fixture": {"event_id": event_id, "starts": starts,
                    "runner_home": "Fukuoka SoftBank Hawks",
                    "runner_away": "Orix Buffaloes", "event_status": "settled",
                    "resulting_unit": None, "superseded_by": None, "version": 3},
        "odds": [
            {"event_id": event_id, "period": 1, "market": "totals", "line": 4.5,
             "line_id": 3012345678, "alt_line_id": 991, "odds1": "1.95",
             "odds2": 1.9, "odds0": None, "todds1": 1.99, "todds2": 2.01,
             "todds0": None, "timestamp": "2026-08-22T08:50:00Z",
             "cutoff": "2026-08-22T09:00:00Z", "max_win": 1000, "status": 1,
             "is_open": 1, "bettable": 1, "score_home": None, "score_away": None,
             "red_cards_home": None, "something_new": "x"},
        ],
        "results": [{"period": 1, "status": 1, "score_home": 3, "score_away": 1,
                     "sets_home": None, "sets_away": None, "settlement_id": 77,
                     "cancellation_reason": None,
                     "timestamp": "2026-08-22T11:00:00Z"}],
        "fetched_at": "2026-11-23T00:00:00Z",
    }


class FakeStore:
    def __init__(self, loaded=()):
        self.loaded = set(loaded)
        self.loads = []
        self.orphans_cleared = 0

    def loaded_ids(self, league):
        return set(self.loaded)

    def delete_orphans(self):
        self.orphans_cleared += 1

    def load(self, table, rows):
        self.loads.append((table, rows))
        if table == "fixtures":
            self.loaded.update(r["event_id"] for r in rows)


def test_to_rows_flattens_one_event_into_three_tables():
    fixture, odds, results = pbq.to_rows(_record(), "npb")
    assert fixture["league"] == "npb"
    assert fixture["game_date"] == "2026-08-22"
    assert fixture["starts"] == "2026-08-22 09:00:00+00:00"
    assert odds == [{
        "league": "npb", "event_id": 1634054636, "game_date": "2026-08-22",
        "period": 1, "market": "totals", "line": 4.5, "line_id": 3012345678,
        "alt_line_id": 991, "odds1": 1.95, "odds0": None, "odds2": 1.9,
        "todds1": 1.99, "todds0": None, "todds2": 2.01,
        "timestamp": "2026-08-22 08:50:00+00:00",
        "cutoff": "2026-08-22 09:00:00+00:00", "max_win": 1000.0, "status": 1,
        "is_open": 1, "bettable": 1, "kind": "tick",
    }]
    assert results[0]["score_home"] == 3 and results[0]["settlement_id"] == 77
    assert results[0]["game_date"] == "2026-08-22"


def test_openclose_record_loads_both_ends_labelled():
    rec = _record()
    row = rec.pop("odds")[0]
    rec.update({"opening": [row], "closing": [dict(row, odds1=1.80)], "mode": "openclose"})
    _, odds, _ = pbq.to_rows(rec, "npb")
    assert [(r["kind"], r["odds1"]) for r in odds] == [("opening", 1.95), ("closing", 1.80)]


def test_game_date_follows_the_league_clock_not_utc():
    # a 01:00 JST start is still the previous day in UTC
    fixture, _, _ = pbq.to_rows(_record(starts="2026-08-21T16:00:00Z"), "npb")
    assert fixture["game_date"] == "2026-08-22"


def test_mlb_game_date_is_the_eastern_date():
    fixture, _, _ = pbq.to_rows(_record(starts="2026-08-24T01:10:00Z"), "mlb")
    assert fixture["game_date"] == "2026-08-23"               # 21:10 ET


def test_sync_loads_only_new_events_and_fixtures_last(tmp_path):
    ph.save_event(tmp_path, _record(1))
    ph.save_event(tmp_path, _record(2))
    store = FakeStore(loaded={1})
    assert pbq.sync(tmp_path, "npb", store) == 1
    assert store.orphans_cleared == 1
    assert [t for t, _ in store.loads] == ["odds", "results", "fixtures"]
    assert {r["event_id"] for _, rows in store.loads for r in rows} == {2}


def test_sync_batches_large_backlogs(tmp_path):
    for i in range(5):
        ph.save_event(tmp_path, _record(i))
    store = FakeStore()
    assert pbq.sync(tmp_path, "npb", store, batch_events=2) == 5
    assert [t for t, _ in store.loads].count("fixtures") == 3


def test_sync_with_nothing_new_loads_nothing(tmp_path):
    ph.save_event(tmp_path, _record(1))
    store = FakeStore(loaded={1})
    assert pbq.sync(tmp_path, "npb", store) == 0
    assert store.loads == []


# --- 盤口-format snapshots --------------------------------------------------

def _board_record(event_id):
    rec = _record(event_id)
    rec["fixture"]["runner_home"] = "Hanshin Tigers"
    rec["fixture"]["runner_away"] = "Yomiuri Giants"
    rec["odds"] = [
        {"period": 0, "market": "moneyline", "line": None, "odds1": 1.8, "odds2": 2.1,
         "odds0": None, "line_id": 1, "timestamp": "2026-08-22T07:00:00",
         "status": 1, "is_open": 1},
        {"period": 0, "market": "totals", "line": 7.5, "odds1": 1.9, "odds2": 1.95,
         "odds0": None, "line_id": 1, "timestamp": "2026-08-22T07:00:00",
         "status": 1, "is_open": 1},
    ]
    return rec


class SnapshotStore(FakeStore):
    """``snapshotted`` maps source -> event ids that already have rows."""

    def __init__(self, loaded=(), snapshotted=None):
        super().__init__(loaded)
        self.snapshotted = snapshotted or {}

    def snapshot_ids(self, league, source):
        return set(self.snapshotted.get(source, ()))


def test_snapshot_rows_are_typed_for_the_ledger_table():
    row = pbq.snapshot_rows(_board_record(5), "npb")[0]
    assert set(row) == {name for name, _ in pbq.SCHEMAS["odds_snapshots"]}
    assert row["league_key"] == "npb"
    assert row["captured_at"] == "2026-08-22 16:30:00"   # open + 30 min
    assert row["start_jst"] == "2026-08-22T18:00:00+09:00"
    assert row["start_tpe"] is None
    assert row["total_line"] == 7.5 and row["mins_to_start"] == 90


def test_scraper_rows_for_the_same_game_do_not_count_as_snapshotted(tmp_path):
    # PS3838 rows copied from the sheets carry the same Pinnacle event ids
    ph.save_event(tmp_path, _board_record(1))
    store = SnapshotStore(loaded={1}, snapshotted={"ps3838": {1}})
    assert pbq.sync_snapshots(tmp_path, "npb", store) == 1


def test_sync_snapshots_builds_only_loaded_events_not_yet_snapshotted(tmp_path):
    for i in (1, 2, 3):
        ph.save_event(tmp_path, _board_record(i))
    store = SnapshotStore(loaded={1, 2}, snapshotted={"bettingiscool": {1}})
    assert pbq.sync_snapshots(tmp_path, "npb", store) == 1
    [(table, rows)] = store.loads
    assert table == "odds_snapshots"
    assert {r["event_id"] for r in rows} == {2}
