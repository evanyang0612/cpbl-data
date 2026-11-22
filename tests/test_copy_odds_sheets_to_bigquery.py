"""Tests for migration/copy_odds_sheets_to_bigquery.py."""

import importlib.util
import pathlib

from baseball import pinnacle_bigquery as pbq
from baseball import pinnacle_odds as po

_PATH = pathlib.Path(__file__).resolve().parent.parent / "migration" / \
    "copy_odds_sheets_to_bigquery.py"
_spec = importlib.util.spec_from_file_location("copy_odds", _PATH)
copy_odds = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(copy_odds)


def _sheet_row(spec, **values):
    header = spec.sheet_headers()
    return header, [values.get(h, "") for h in header]


def test_a_sheet_row_becomes_a_typed_ledger_record():
    header, row = _sheet_row(
        po.NPB, captured_at="2026-10-28 9:31:00", snapshot_type="interim",
        event_id="1634054636", game_date="2026-10-28",
        start_jst="2026-10-28T18:00:00+09:00", home_norm="ソフトバンク",
        away_norm="DeNA", status="pregame", mins_to_start="509", period="final",
        ml_home="1.537", ml_away="2.62", total_line="5.5", spread_hdp="-1.5",
        all_totals='[{"line": 5.5, "over": 1.86, "under": 2.0}]', source="ps3838")
    [record] = copy_odds.to_records(header, [row], "npb")
    assert set(record) == {name for name, _ in pbq.SCHEMAS["odds_snapshots"]}
    assert record["league_key"] == "npb"
    assert record["captured_at"] == "2026-10-28 09:31:00"   # zero-padded for DATETIME
    assert record["event_id"] == 1634054636
    assert record["mins_to_start"] == 509
    assert record["ml_home"] == 1.537 and record["spread_hdp"] == -1.5
    assert record["start_tpe"] is None and record["npb_game_id"] is None
    assert record["all_totals"].startswith('[{"line": 5.5')


def test_rows_from_before_the_source_column_are_the_scraper():
    header, row = _sheet_row(po.MLB, captured_at="2026-08-04 11:24:09",
                             game_date="2026-08-03", period="half", source="")
    [record] = copy_odds.to_records(header, [row], "mlb")
    assert record["source"] == "ps3838"


def test_league_specific_columns_are_kept():
    header, row = _sheet_row(po.CPBL, captured_at="2026-09-11 10:10:02",
                             game_date="2026-09-11", start_tpe="2026-09-11T18:35:00+08:00",
                             kind_code="A", cpbl_game_sno="251", period="final")
    [record] = copy_odds.to_records(header, [row], "cpbl")
    assert (record["kind_code"], record["cpbl_game_sno"]) == ("A", "251")
    assert record["start_tpe"] == "2026-09-11T18:35:00+08:00"


def test_blank_rows_are_dropped():
    header, row = _sheet_row(po.NPB)
    assert copy_odds.to_records(header, [row, []], "npb") == []


def test_refuses_to_replace_rows_the_scraper_wrote_after_the_freeze():
    # once the scraper writes to BigQuery, reloading the frozen sheet would delete them
    assert copy_odds.safe_to_replace("2026-10-28 09:31:00", None)
    assert copy_odds.safe_to_replace("2026-10-28 09:31:00", "2026-10-28 09:31:00")
    assert not copy_odds.safe_to_replace("2026-10-28 09:31:00", "2027-02-20 12:00:00")
