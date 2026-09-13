"""Unit tests for the OddsPapi discovery pass (baseball/oddspapi.py)."""

from baseball import oddspapi as op

SPORTS = [{"sportId": 1, "sportName": "Soccer", "sportSlug": "soccer"},
          {"sportId": 13, "sportName": "Baseball", "sportSlug": "baseball"}]

TOURNAMENTS = [
    {"tournamentId": 39, "tournamentName": "MLB", "categoryName": "USA"},
    {"tournamentId": 77, "tournamentName": "NPB", "categoryName": "Japan"},
    {"tournamentId": 78, "tournamentName": "CPBL", "categoryName": "Taiwan"},
    {"tournamentId": 79, "tournamentName": "KBO League", "categoryName": "South Korea"},
]

MARKETS = [
    {"marketId": 101, "marketName": "Full Time Result", "marketType": "1x2",
     "outcomes": [{"outcomeId": 102, "outcomeName": "1"},
                  {"outcomeId": 103, "outcomeName": "2"}]},
    {"marketId": 106, "marketName": "Over Under Full Time", "marketType": "totals",
     "outcomes": [{"outcomeId": 107, "outcomeName": "Over"},
                  {"outcomeId": 108, "outcomeName": "Under"}]},
]


def test_finds_the_baseball_sport_id():
    assert op.find_sport(SPORTS, "baseball")["sportId"] == 13


def test_finds_sport_by_name_when_the_slug_differs():
    assert op.find_sport([{"sportId": 13, "sportName": "Baseball"}],
                         "baseball")["sportId"] == 13


def test_returns_none_for_a_sport_that_is_not_carried():
    assert op.find_sport(SPORTS, "kabaddi") is None


def test_picks_out_the_leagues_we_care_about():
    """The question discovery exists to answer: are NPB and CPBL in there."""
    found = op.match_tournaments(TOURNAMENTS)
    assert found["npb"]["tournamentId"] == 77
    assert found["cpbl"]["tournamentId"] == 78
    assert found["mlb"]["tournamentId"] == 39


def test_a_missing_league_is_reported_as_missing_not_guessed():
    found = op.match_tournaments([t for t in TOURNAMENTS
                                  if t["tournamentName"] != "CPBL"])
    assert found["npb"] is not None
    assert found["cpbl"] is None


def test_market_catalogue_is_keyed_by_id_with_outcome_names_resolved():
    """The odds payload is all numeric ids; this is the only thing that says
    what 106 or 107 mean, so the whole parser depends on it."""
    cat = op.market_catalogue(MARKETS)
    assert cat[106]["name"] == "Over Under Full Time"
    assert cat[106]["type"] == "totals"
    assert cat[106]["outcomes"][107] == "Over"
    # The line and the period live on the catalogue entry, not on the payload.
    assert "handicap" in cat[106] and "period" in cat[106]


def test_describe_odds_names_every_id_it_can_and_flags_the_rest():
    """Discovery has to be readable by a human deciding whether to build on
    it — an unmapped id is the finding, not an error."""
    fixture = {
        "fixtureId": "id123", "startTime": "2026-04-13T09:00:00.000Z",
        "participant1Name": "Hanshin Tigers", "participant2Name": "Chunichi Dragons",
        "tournamentName": "NPB",
        "bookmakerOdds": {"pinnacle": {"markets": {
            "106": {"outcomes": {"107": {"players": {"0": {"price": 1.95}}},
                                 "108": {"players": {"0": {"price": 1.87}}}}},
            "9999": {"outcomes": {"1": {"players": {"0": {"price": 2.0}}}}},
        }}},
    }
    lines = op.describe_odds(fixture, op.market_catalogue(MARKETS))
    text = "\n".join(lines)
    assert "Over Under Full Time" in text and "Over" in text and "1.95" in text
    assert "9999" in text and "未知" in text


# --- parsing a real board -------------------------------------------------

from baseball import pinnacle_odds as po  # noqa: E402

# Shapes taken from the live NPB board on 2026-09-13 (Seibu vs Nippon-Ham),
# whose every number matched the PS3838 scraper's own row for the same game.
BB_CATALOGUE = [
    {"marketId": 131, "marketName": "Winner (incl. extra innings)", "sportId": 13,
     "handicap": 0.0, "period": "result", "marketType": "moneyline",
     "outcomes": [{"outcomeId": 131, "outcomeName": "1"},
                  {"outcomeId": 132, "outcomeName": "2"}]},
    {"marketId": 1316, "marketName": "Over Under", "sportId": 13, "handicap": 6.0,
     "period": "result", "marketType": "totals",
     "outcomes": [{"outcomeId": 1316, "outcomeName": "Over"},
                  {"outcomeId": 1317, "outcomeName": "Under"}]},
    {"marketId": 1318, "marketName": "Over Under", "sportId": 13, "handicap": 6.5,
     "period": "result", "marketType": "totals",
     "outcomes": [{"outcomeId": 1318, "outcomeName": "Over"},
                  {"outcomeId": 1319, "outcomeName": "Under"}]},
    {"marketId": 1368, "marketName": "Handicap", "sportId": 13, "handicap": -1.5,
     "period": "result", "marketType": "spreads",
     "outcomes": [{"outcomeId": 1368, "outcomeName": "1"},
                  {"outcomeId": 1369, "outcomeName": "2"}]},
    {"marketId": 131273, "marketName": "Over Under", "sportId": 13, "handicap": 3.5,
     "period": "p1+p2+p3+p4+p5", "marketType": "totals",
     "outcomes": [{"outcomeId": 131273, "outcomeName": "Over"},
                  {"outcomeId": 131274, "outcomeName": "Under"}]},
    {"marketId": 13658, "marketName": "Team 1 Over Under", "sportId": 13,
     "handicap": 3.5, "period": "result", "marketType": "teamtotals-team1",
     "outcomes": [{"outcomeId": 13658, "outcomeName": "Over"}]},
]


def _px(price):
    return {"players": {"0": {"price": price}}}


NPB_FIXTURE = {
    "fixtureId": "id130010", "startTime": "2026-09-13T08:00:00.000Z",
    "participant1Name": "Saitama Seibu Lions",
    "participant2Name": "Hokkaido Nippon-Ham Fighters",
    "tournamentName": "NPB", "externalProviders": {"pinnacleId": 1635957904},
    "bookmakerOdds": {"pinnacle": {"markets": {
        "131":    {"bookmakerMarketId": "line/3/187703/1/0/moneyline",
                   "outcomes": {"131": _px(1.8), "132": _px(2.11)}},
        "1316":   {"bookmakerMarketId": "line/3/187703/1/0/totals",
                   "outcomes": {"1316": _px(1.74), "1317": _px(2.15)}},
        "1318":   {"bookmakerMarketId": "altLine/3/187703/1/0/totals",
                   "outcomes": {"1318": _px(1.892), "1319": _px(1.943)}},
        "1368":   {"bookmakerMarketId": "line/3/187703/1/0/spreads",
                   "outcomes": {"1368": _px(2.9), "1369": _px(1.442)}},
        "131273": {"bookmakerMarketId": "line/3/187703/1/1/totals",
                   "outcomes": {"131273": _px(1.934), "131274": _px(1.884)}},
        "13658":  {"bookmakerMarketId": "line/3/187703/1/0/teamtotals",
                   "outcomes": {"13658": _px(2.22)}},
    }}},
}


def _rows(at="2026-09-13T05:50:00Z"):
    return op.parse_fixture(NPB_FIXTURE, op.market_catalogue(BB_CATALOGUE),
                            league=po.NPB, at=at)


def test_participant1_is_the_home_side():
    """Checked against the scraper's own row for this game: home 西武 was 1.8
    and away 日本ハム 2.11, which is exactly what outcomes 1 and 2 hold."""
    row = next(r for r in _rows() if r["period"] == "final")
    assert (row["home_norm"], row["away_norm"]) == ("西武", "日本ハム")
    assert (row["ml_home"], row["ml_away"]) == (1.8, 2.11)


def test_both_periods_come_back():
    """OddsPapi spells the 1st-5 period p1+p2+p3+p4+p5; 盤口 calls it half."""
    assert {r["period"] for r in _rows()} == {"final", "half"}


def test_the_main_line_is_the_one_pinnacle_marks_line_not_the_balanced_one():
    """Every line is its own market, and only the main one's
    bookmakerMarketId starts with `line/` — which is how The Odds API picks
    6.0 where this repo's balanced-juice heuristic picks 6.5."""
    row = next(r for r in _rows() if r["period"] == "final")
    assert row["total_line"] == 6.0
    assert (row["total_over"], row["total_under"]) == (1.74, 2.15)


def test_the_whole_ladder_is_kept():
    """Unlike The Odds API, every alternate line is here — which is what
    baseball.asian_lines needs and what the main-line-only feed cannot give."""
    row = next(r for r in _rows() if r["period"] == "final")
    assert sorted(t["line"] for t in row["all_totals"]) == [6.0, 6.5]


def test_handicap_is_read_from_the_home_side():
    row = next(r for r in _rows() if r["period"] == "final")
    assert row["spread_hdp"] == -1.5
    assert (row["spread_home"], row["spread_away"]) == (2.9, 1.442)


def test_team_totals_are_not_recorded():
    """We do not bet them, and 盤口 has nowhere to put them."""
    row = next(r for r in _rows() if r["period"] == "final")
    assert all(abs(t["line"]) != 3.5 for t in row["all_totals"])


def test_rows_carry_the_pinnacle_event_id_so_they_join_to_our_own():
    """externalProviders.pinnacleId is the same number the PS3838 scraper
    writes as event_id, so backfilled and scraped rows join exactly."""
    assert all(r["event_id"] == 1635957904 for r in _rows())


def test_rows_are_labelled_and_fit_the_盤口_layout():
    rows = _rows()
    assert all(r["source"] == op.SOURCE for r in rows)
    values = po.snapshots_to_rows(rows, "close", rows[0]["captured_at"], po.NPB)
    assert len(values[0]) == len(po.NPB.sheet_headers())


def test_historical_series_is_replayed_as_of_a_moment():
    """/historical-odds gives every price change; a snapshot is the last
    change at or before the instant asked for."""
    history = {"bookmakers": {"pinnacle": {"markets": {"131": {"outcomes": {
        "131": {"players": {"0": [
            {"createdAt": "2026-09-12T13:10:22Z", "price": 1.5, "active": True},
            {"createdAt": "2026-09-13T05:00:00Z", "price": 1.8, "active": True},
            {"createdAt": "2026-09-13T07:00:00Z", "price": 1.9, "active": True},
        ]}},
        "132": {"players": {"0": [
            {"createdAt": "2026-09-12T13:10:22Z", "price": 2.5, "active": True},
        ]}},
    }}}}}}
    board = op.replay(history, at="2026-09-13T06:00:00Z")
    prices = board["markets"]["131"]["outcomes"]
    assert prices["131"]["players"]["0"]["price"] == 1.8   # not 1.5, not 1.9
    assert prices["132"]["players"]["0"]["price"] == 2.5


def test_replay_before_the_first_change_has_nothing_to_show():
    history = {"bookmakers": {"pinnacle": {"markets": {"131": {"outcomes": {
        "131": {"players": {"0": [
            {"createdAt": "2026-09-13T05:00:00Z", "price": 1.8, "active": True}]}},
    }}}}}}
    assert op.replay(history, at="2026-09-13T04:00:00Z")["markets"] == {}
