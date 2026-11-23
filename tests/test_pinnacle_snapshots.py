"""Unit tests for turning Pinnacle tick history into 盤口-format snapshots
(baseball/pinnacle_snapshots.py)."""

import json

from baseball import pinnacle_snapshots as ps

START = "2026-08-22T09:00:00"          # 18:00 JST


def _row(ts, line_id, market, line=None, o1=None, o2=None, *, period=0, o0=None,
         status=1, is_open=1):
    return {"period": period, "market": market, "line": line, "odds1": o1,
            "odds0": o0, "odds2": o2, "line_id": line_id, "timestamp": ts,
            "status": status, "is_open": is_open}


def _board(ts, line_id, *, ml=(1.80, 2.10), total=8.5, period=0):
    """One full update of a period: moneyline, a 3-line total ladder, run lines."""
    rows = [
        _row(ts, line_id, "totals", total - 1, 1.60, 2.40, period=period),
        _row(ts, line_id, "totals", total, 1.93, 1.93, period=period),
        _row(ts, line_id, "totals", total + 1, 2.30, 1.65, period=period),
        _row(ts, line_id, "spread", -1.5, 2.40, 1.60, period=period),
        _row(ts, line_id, "spread", -0.5, 1.80, 2.05, period=period),
        _row(ts, line_id, "home_totals", 4.5, 1.9, 1.9, period=period),
    ]
    if ml:
        rows.append(_row(ts, line_id, "moneyline", None, ml[0], ml[1], period=period))
    return rows


def _record(odds, *, home="Hanshin Tigers", away="Yokohama Bay Stars"):
    return {"fixture": {"event_id": 42, "starts": START, "runner_home": home,
                        "runner_away": away,
                        "league_name": "Nippon Professional Baseball"},
            "odds": odds, "results": []}


# --- the board at an instant -------------------------------------------------

def test_board_at_takes_the_latest_update_at_or_before_the_instant():
    odds = _board("2026-08-21T23:00:00", 1, total=8.5) + \
        _board("2026-08-22T08:00:00", 2, total=7.5) + \
        _board("2026-08-22T08:58:00", 3, total=6.5)
    board = ps.board_at(odds, 0, ps._parse_ts("2026-08-22T08:55:00"))
    assert {r["line"] for r in board["totals"]} == {6.5, 7.5, 8.5}
    assert board["moneyline"][0]["line_id"] == 2


def test_board_at_keeps_a_market_the_last_partial_update_left_out():
    odds = _board("2026-08-22T07:00:00", 1, ml=(1.80, 2.10)) + \
        [_row("2026-08-22T08:00:00", 2, "moneyline", None, 1.70, 2.25)]
    board = ps.board_at(odds, 0, ps._parse_ts("2026-08-22T08:55:00"))
    assert board["moneyline"][0]["odds1"] == 1.70
    assert len(board["totals"]) == 3          # still the 07:00 ladder


def test_board_at_drops_pulled_markets_and_suspended_periods():
    odds = _board("2026-08-22T07:00:00", 1) + [
        _row("2026-08-22T08:00:00", 2, "spread", -1.5, 2.4, 1.6, is_open=0),
        _row("2026-08-22T08:00:00", 2, "spread", -0.5, 1.8, 2.05, is_open=0),
    ]
    board = ps.board_at(odds, 0, ps._parse_ts("2026-08-22T08:55:00"))
    assert "spread" not in board
    suspended = odds + [_row("2026-08-22T08:30:00", 3, None, status=2)]
    assert ps.board_at(suspended, 0, ps._parse_ts("2026-08-22T08:55:00")) == {}


def test_board_at_before_the_period_opens_is_empty():
    odds = _board("2026-08-22T07:00:00", 1)
    assert ps.board_at(odds, 0, ps._parse_ts("2026-08-22T06:00:00")) == {}


# --- converting a board with the scraper's own rules -------------------------

def test_parse_board_uses_the_scraper_conventions():
    board = ps.board_at(_board("2026-08-22T07:00:00", 1), 0,
                        ps._parse_ts("2026-08-22T08:55:00"))
    parsed = ps.parse_board(0, board)
    assert parsed["period"] == "final"
    assert (parsed["ml_home"], parsed["ml_away"]) == (1.80, 2.10)
    # most balanced total
    assert (parsed["total_line"], parsed["total_over"], parsed["total_under"]) == \
        (8.5, 1.93, 1.93)
    # run line prefers ±1.5, stored from the home side
    assert (parsed["spread_hdp"], parsed["spread_home"], parsed["spread_away"]) == \
        (-1.5, 2.40, 1.60)
    assert {"hdp": -0.5, "home": 1.80, "away": 2.05} in parsed["all_spreads"]
    assert {"line": 9.5, "over": 2.30, "under": 1.65} in parsed["all_totals"]


def test_first_five_period_is_labelled_half():
    board = ps.board_at(_board("2026-08-22T07:00:00", 1, period=1, ml=None), 1,
                        ps._parse_ts("2026-08-22T08:55:00"))
    parsed = ps.parse_board(1, board)
    assert parsed["period"] == "half"
    assert parsed["ml_home"] is None


# --- one event -> snapshot rows ----------------------------------------------

def test_full_history_samples_open_plus_30_every_hour_and_the_close():
    odds = _board("2026-08-21T20:00:00", 1, total=9.5) + \
        _board("2026-08-22T06:30:00", 2, total=8.5) + \
        _board("2026-08-22T08:57:00", 3, total=7.5) + \
        _board("2026-08-22T09:10:00", 4, total=6.5)   # in play: not pregame
    snaps = [s for s in ps.event_snapshots(_record(odds), "npb") if s["period"] == "final"]
    got = [(s["snapshot_type"], s["captured_at"], s["mins_to_start"], s["total_line"])
           for s in snaps]
    hourly = [("interim", f"2026-08-22 {h:02d}:00:00", (18 - h) * 60,
               9.5 if h <= 15 else 8.5) for h in range(6, 18)]
    assert got == [("open", "2026-08-22 05:30:00", 750, 9.5)] + hourly + \
        [("close", "2026-08-22 17:57:00", 3, 7.5)]


def test_a_board_opened_less_than_30_minutes_out_has_no_open_row():
    odds = _board("2026-08-22T08:45:00", 1)
    types = [s["snapshot_type"] for s in ps.event_snapshots(_record(odds), "npb")
             if s["period"] == "final"]
    assert types == ["close"]


def _oc_record(opening, closing):
    rec = _record([])
    del rec["odds"]
    rec.update({"opening": opening, "closing": closing, "mode": "openclose"})
    return rec


def test_openclose_record_builds_open_from_the_first_board():
    first = _board("2026-08-21T20:00:00", 1, total=8.5)
    # an alternate total Pinnacle only added at noon has a later first price
    late_line = [_row("2026-08-22T03:00:00", 9, "totals", 6.5, 1.3, 3.4)]
    snaps = ps.event_snapshots(_oc_record(first + late_line, []), "npb")
    [snap] = snaps
    assert (snap["snapshot_type"], snap["captured_at"]) == ("open", "2026-08-22 05:00:00")
    assert 6.5 not in [t["line"] for t in json.loads(snap["all_totals"])]


def test_openclose_record_builds_close_from_the_lines_still_up_at_the_end():
    board = _board("2026-08-22T08:59:30", 7, total=7.5)
    # 9.5 was pulled at 10:00 JST; /api/closing still returns its last price
    stale = [_row("2026-08-22T01:00:00", 2, "totals", 9.5, 2.5, 1.5)]
    snaps = ps.event_snapshots(_oc_record([], board + stale), "npb")
    [snap] = snaps
    assert (snap["snapshot_type"], snap["captured_at"], snap["mins_to_start"]) == \
        ("close", "2026-08-22 17:59:30", 0)
    assert 9.5 not in [t["line"] for t in json.loads(snap["all_totals"])]
    assert snap["total_line"] == 7.5


def test_event_snapshots_fill_the_ledger_identity_columns():
    snap = ps.event_snapshots(_record(_board("2026-08-22T07:00:00", 1)), "npb")[0]
    assert snap["event_id"] == 42
    assert snap["game_date"] == "2026-08-22"
    assert snap["start_jst"] == "2026-08-22T18:00:00+09:00"
    assert (snap["home_norm"], snap["away_norm"]) == ("阪神", "DeNA")
    assert snap["status"] == "pregame"
    assert snap["source"] == "bettingiscool"
    assert json.loads(snap["all_totals"])[0] == {"line": 7.5, "over": 1.6, "under": 2.4}


def test_event_snapshots_open_each_period_on_its_own_first_update():
    odds = _board("2026-08-21T20:00:00", 1) + \
        _board("2026-08-22T07:00:00", 5, period=1, ml=None)
    opens = [(s["period"], s["captured_at"]) for s in ps.event_snapshots(
        _record(odds), "npb") if s["snapshot_type"] == "open"]
    assert opens == [("final", "2026-08-22 05:30:00"), ("half", "2026-08-22 16:30:00")]


def test_open_is_the_first_bettable_update_not_a_board_posted_closed():
    closed = [dict(r, status=2, is_open=0) for r in _board("2026-08-21T20:00:00", 1)]
    odds = closed + _board("2026-08-21T20:00:05", 2)
    opens = [s for s in ps.event_snapshots(_record(odds), "npb")
             if s["snapshot_type"] == "open"]
    assert [(s["captured_at"], s["total_line"]) for s in opens] == \
        [("2026-08-22 05:30:05", 8.5)]


def test_event_snapshots_drop_incoherent_boards():
    # home laying 1.5 runs cannot be priced shorter than home simply winning
    odds = _board("2026-08-22T07:00:00", 1, ml=(2.60, 1.50))
    odds = [dict(r, odds1=1.30, odds2=3.50) if r["market"] == "spread" and r["line"] == -1.5
            else r for r in odds]
    assert ps.event_snapshots(_record(odds), "npb") == []


def test_mlb_keeps_the_feed_spelling_and_eastern_dates():
    rec = _record(_board("2026-08-22T23:00:00", 1),
                  home="New York Yankees", away="Boston Red Sox")
    rec["fixture"]["starts"] = "2026-08-23T23:05:00"        # 19:05 ET
    snap = ps.event_snapshots(rec, "mlb")[0]
    assert snap["start_et"] == "2026-08-23T19:05:00-04:00"
    assert snap["game_date"] == "2026-08-23"
    assert (snap["home_norm"], snap["away_norm"]) == ("New York Yankees", "Boston Red Sox")


def test_cpbl_uses_its_own_clock_and_names():
    rec = _record(_board("2026-08-22T07:00:00", 1),
                  home="CTBC Brothers", away="Rakuten Monkeys")
    snap = ps.event_snapshots(rec, "cpbl")[0]
    assert snap["start_tpe"] == "2026-08-22T17:00:00+08:00"
    assert (snap["home_norm"], snap["away_norm"]) == ("中信兄弟", "樂天")
