"""Tests for the BigQuery-backed 盤口 ledger (baseball/odds_ledger.py)."""

import json

from baseball import odds_ledger as ledger
from baseball import pinnacle_bigquery as pbq
from baseball import pinnacle_odds as po


class FakeStore:
    def __init__(self, rows=()):
        self.loads = []
        self.rows = list(rows)
        self.asked = []

    def load(self, table, rows):
        self.loads.append((table, rows))

    def ledger_rows(self, league_key, exclude_source=None):
        self.asked.append((league_key, exclude_source))
        return [r for r in self.rows if r["league_key"] == league_key
                and r["source"] != exclude_source]


def _snapshot(**over):
    snap = {"event_id": 1634054636, "league": "日本職業棒球賽", "home_team": "軟銀鷹",
            "away_team": "橫濱DeNA海灣之星", "home_norm": "ソフトバンク",
            "away_norm": "DeNA", "start_jst": "2026-10-28T18:00:00+09:00",
            "game_date": "2026-10-28", "status": "pregame", "mins_to_start": 30,
            "period": "final", "ml_home": 1.537, "ml_away": 2.62, "ml_draw": None,
            "total_line": 5.5, "total_over": 1.862, "total_under": 2.0,
            "spread_hdp": -1.5, "spread_home": 2.09, "spread_away": 1.8,
            "all_totals": [{"line": 5.5, "over": 1.862, "under": 2.0}],
            "all_spreads": [{"hdp": -1.5, "home": 2.09, "away": 1.8}]}
    snap.update(over)
    return snap


def test_append_loads_scraper_rows_into_the_ledger_table():
    rows = po.snapshots_to_rows([_snapshot()], "interim", "2026-10-28 17:30:00", po.NPB)
    store = FakeStore()
    assert ledger.append(rows, po.NPB, store=store) == 1
    [(table, [record])] = store.loads
    assert table == "odds_snapshots"
    assert record["league_key"] == "npb"
    assert record["source"] == "ps3838"
    assert record["captured_at"] == "2026-10-28 17:30:00"
    assert record["ml_home"] == 1.537 and record["mins_to_start"] == 30
    assert json.loads(record["all_spreads"]) == [{"hdp": -1.5, "home": 2.09, "away": 1.8}]


def test_append_nothing_touches_nothing():
    store = FakeStore()
    assert ledger.append([], po.NPB, store=store) == 0
    assert store.loads == []


def test_write_snapshots_now_goes_to_the_ledger(monkeypatch):
    sent = []
    monkeypatch.setattr(ledger, "append",
                        lambda rows, league, store=None: sent.append((rows, league)) or len(rows))
    rows = po.snapshots_to_rows([_snapshot()], "interim", "2026-10-28 17:30:00", po.NPB)
    assert po.write_snapshots(rows, po.NPB) == 1
    assert sent == [(rows, po.NPB)]


def test_read_values_rebuilds_the_sheet_shape_for_old_readers():
    record = pbq._row({**_snapshot(), "captured_at": "2026-10-28 17:30:00",
                       "snapshot_type": "interim", "source": "ps3838",
                       "all_totals": "[]", "all_spreads": "[]"},
                      "odds_snapshots", {"league_key": "npb"})
    store = FakeStore([record, {**record, "source": "bettingiscool"}])
    values = ledger.read_values(po.NPB, store=store)
    assert values[0] == po.NPB.sheet_headers()
    assert len(values) == 2                       # header + the scraper row only
    row = dict(zip(values[0], values[1]))
    assert row["ml_home"] == "1.537"
    assert row["npb_game_id"] == ""
    assert row["captured_at"] == "2026-10-28 17:30:00"
    assert store.asked == [("npb", "bettingiscool")]


def test_credentials_prefer_the_ci_json_env(monkeypatch, tmp_path):
    info = {"type": "service_account", "project_id": "p", "client_email": "x@p.iam",
            "private_key": "k", "token_uri": "https://oauth2.googleapis.com/token"}
    monkeypatch.setenv("GOOGLE_CREDENTIALS", json.dumps(info))
    seen = {}
    monkeypatch.setattr(pbq.service_account.Credentials, "from_service_account_info",
                        classmethod(lambda cls, i, scopes: seen.setdefault("info", i)))
    pbq._credentials()
    assert seen["info"] == info
