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
