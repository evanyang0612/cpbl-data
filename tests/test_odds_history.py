"""Unit tests for the The-Odds-API 盤口 backfill (baseball/odds_history.py)."""


from baseball import odds_history as oh
from baseball import pinnacle_odds as po

SNAPSHOT_TS = "2026-07-01T08:50:00Z"          # 17:50 JST
COMMENCE = "2026-07-01T09:00:00Z"             # 18:00 JST first pitch


def _bookmaker(key, *, ml=("2.100", "1.800"), hdp=-1.5,
               spread=("2.580", "1.505"), total=7.5,
               ou=("1.925", "1.892")):
    """One bookmaker's board: h2h / spreads / totals, home-team first."""
    return {
        "key": key,
        "title": key.title(),
        "markets": [
            {"key": "h2h", "outcomes": [
                {"name": "Hanshin Tigers", "price": float(ml[0])},
                {"name": "Chunichi Dragons", "price": float(ml[1])},
            ]},
            {"key": "spreads", "outcomes": [
                {"name": "Hanshin Tigers", "price": float(spread[0]), "point": hdp},
                {"name": "Chunichi Dragons", "price": float(spread[1]), "point": -hdp},
            ]},
            {"key": "totals", "outcomes": [
                {"name": "Over", "price": float(ou[0]), "point": total},
                {"name": "Under", "price": float(ou[1]), "point": total},
            ]},
        ],
    }


def _event(bookmakers):
    return {
        "id": "abc123",
        "sport_key": "baseball_npb",
        "sport_title": "NPB",
        "commence_time": COMMENCE,
        "home_team": "Hanshin Tigers",
        "away_team": "Chunichi Dragons",
        "bookmakers": bookmakers,
    }


def _raw(bookmakers=None, *, as_list=True):
    event = _event(bookmakers if bookmakers is not None else [_bookmaker("pinnacle")])
    return {"timestamp": SNAPSHOT_TS,
            "previous_timestamp": "2026-07-01T08:45:00Z",
            "next_timestamp": "2026-07-01T08:55:00Z",
            "data": [event] if as_list else event}


def test_parses_pinnacle_into_the_existing_盤口_shape():
    rows = oh.parse_snapshot(_raw(), league=po.NPB)
    assert len(rows) == 1
    r = rows[0]
    assert r["period"] == "final"
    assert (r["ml_home"], r["ml_away"]) == (2.1, 1.8)
    assert (r["total_line"], r["total_over"], r["total_under"]) == (7.5, 1.925, 1.892)
    # spread_hdp is stored from the home side: negative means home lays runs.
    assert r["spread_hdp"] == -1.5
    assert (r["spread_home"], r["spread_away"]) == (2.58, 1.505)


def test_team_names_are_folded_to_the_japanese_short_names():
    r = oh.parse_snapshot(_raw(), league=po.NPB)[0]
    assert (r["home_norm"], r["away_norm"]) == ("阪神", "中日")
    assert r["home_team"] == "Hanshin Tigers"


def test_only_pinnacle_is_read():
    """The point of the backfill is Pinnacle's number, not a consensus."""
    rows = oh.parse_snapshot(
        _raw([_bookmaker("betfair_ex_eu", ml=("9.9", "9.9")),
              _bookmaker("pinnacle")]), league=po.NPB)
    assert len(rows) == 1
    assert rows[0]["ml_home"] == 2.1


def test_an_event_pinnacle_did_not_price_is_skipped():
    """A blank row would join to the game and say the line was missing."""
    assert oh.parse_snapshot(_raw([_bookmaker("betfair_ex_eu")]), league=po.NPB) == []


def test_single_event_payload_is_accepted_too():
    """The per-event endpoint returns `data` as an object, not a list."""
    assert len(oh.parse_snapshot(_raw(as_list=False), league=po.NPB)) == 1


def test_snapshot_timing_is_recorded_against_first_pitch():
    r = oh.parse_snapshot(_raw(), league=po.NPB)[0]
    assert r["mins_to_start"] == 10
    assert r["game_date"] == "2026-07-01"
    assert r["captured_at"] == "2026-07-01 17:50:00"
    assert r["status"] == "pregame"


def test_rows_match_the_scraper_s_own_headers():
    """Backfilled rows land in the same 盤口 tab, so the layout has to agree."""
    rows = oh.parse_snapshot(_raw(), league=po.NPB)
    values = po.snapshots_to_rows(rows, "close", rows[0]["captured_at"], po.NPB)
    headers = po.NPB.sheet_headers()
    assert len(values[0]) == len(headers)
    row = dict(zip(headers, values[0]))
    assert row["ml_home"] == 2.1
    assert row["snapshot_type"] == "close"
    assert row["home_norm"] == "阪神"


def test_close_snapshot_times_bracket_the_npb_start_times():
    """NPB starts at 14:00 or 18:00 JST; one snapshot each, just before."""
    times = oh.snapshot_times("2026-07-01", po.NPB, leads=(10,))
    assert [ts for ts, _ in times] == ["2026-07-01T04:50:00Z",
                                       "2026-07-01T08:50:00Z"]


def test_several_leads_sample_the_path_and_label_themselves():
    """The archive is a 5-10 minute series, not one opening number, so the
    caller says how much of the path to buy. The furthest sample out is the
    open, the nearest is the close, and anything between is interim — the row
    should not have to be told which run it came from."""
    times = oh.snapshot_times("2026-07-01", po.NPB, leads=(10, 240, 720))
    evening = [(ts, kind) for ts, kind in times if ts.startswith("2026-07-01T0")
               and kind]
    assert ("2026-07-01T08:50:00Z", "close") in evening      # 10m out
    assert ("2026-07-01T05:00:00Z", "interim") in evening    # 4h out
    assert ("2026-06-30T21:00:00Z", "open") in [(t, k) for t, k in times]


def test_snapshot_times_are_chronological():
    times = oh.snapshot_times("2026-07-01", po.NPB, leads=(10, 720))
    assert [ts for ts, _ in times] == sorted(ts for ts, _ in times)


def test_credit_cost_is_ten_per_market_per_region():
    assert oh.credit_cost(markets=("h2h", "spreads", "totals"), regions=("eu",)) == 30
    assert oh.credit_cost(markets=("h2h",), regions=("eu",)) == 10


def test_backfill_plan_reports_what_it_will_cost_before_spending():
    plan = oh.plan(["2026-07-01", "2026-07-02"], po.NPB, leads=(10,))
    assert plan["requests"] == 4          # two start times a day
    assert plan["credits"] == 4 * 30


def test_plan_scales_with_how_much_of_the_path_is_bought():
    """Each extra sample point is another full-price request; the plan says so
    before a single credit is spent."""
    one = oh.plan(["2026-07-01"], po.NPB, leads=(10,))
    three = oh.plan(["2026-07-01"], po.NPB, leads=(10, 240, 720))
    assert three["requests"] == one["requests"] * 3
    assert three["credits"] == one["credits"] * 3


def test_off_days_are_dropped_from_the_plan(tmp_path):
    """A request on a day with no games buys an empty payload at full price.

    NPB's 2,291 days from 2020-06-06 include two winters and most Mondays.
    The box-score cache already knows which days had games, so the plan asks
    only for those.
    """
    for name in ("20260401-t-c-01.json", "20260403-g-s-01.json"):
        (tmp_path / name).write_text("{}")
    dates = oh.game_dates("2026-04-01", "2026-04-05", po.NPB, cache_dir=str(tmp_path))
    assert dates == ["2026-04-01", "2026-04-03"]


def test_game_dates_falls_back_to_every_day_without_a_cache(tmp_path):
    dates = oh.game_dates("2026-04-01", "2026-04-03", po.NPB,
                          cache_dir=str(tmp_path / "missing"))
    assert dates == ["2026-04-01", "2026-04-02", "2026-04-03"]


def test_backfilled_rows_are_labelled_with_their_own_source():
    """The two feeds pick a different main line — see SOURCE in pinnacle_odds —
    so a backfilled row must never read as one the scraper wrote."""
    rows = oh.parse_snapshot(_raw(), league=po.NPB)
    assert rows[0]["source"] == oh.SOURCE != po.SOURCE
    values = po.snapshots_to_rows(rows, "close", rows[0]["captured_at"], po.NPB)
    assert dict(zip(po.NPB.sheet_headers(), values[0]))["source"] == "the_odds_api"
