"""Unit tests for the PS3838 NPB/MLB odds parser (baseball/pinnacle_odds.py)."""

from datetime import datetime, timedelta, timezone

from baseball import pinnacle_odds as po
from baseball.mlb_games import MlbGameIndex

NPB = po.NPB_LEAGUE_ID
MLB = po.MLB_LEAGUE_ID
START_MS = 1784350800000  # fixed timestamp; bucket (not time) decides live/pregame


def _event(event_id, home_zh, away_zh, odds, english=None):
    """Build a minimal compact event array (parser reads [0,1,2,4,8,24,25])."""
    ev = [event_id, home_zh, away_zh, 0, START_MS, 0, 0, 0, odds]
    if english:
        ev.extend([0] * (24 - len(ev)))
        ev.extend(english)  # [24] = home English name, [25] = away
    return ev


# A full-game period: [spreads, totals, moneyline].
FULL_PERIOD = [
    [  # spreads: a lopsided +1.5 line and the balanced -1.5 run line
        [1.5, -1.5, "1.5", "4.580", "1.201", 0, 1, 1, 0, 250.0, 1],
        [-1.5, 1.5, "1.5", "1.925", "1.943", 0, 1, 2, 0, 250.0, 1],
        [-2.0, 2.0, "2.0", "1.724", "2.180", 0, 1, 3, 0, 250.0, 1],
    ],
    [  # totals: 7.5 / 7.0 / 6.5 — 7.0 is the most balanced
        ["7.5", 7.5, "2.040", "1.806", 4, 1, 250.0, 1],
        ["7.0", 7.0, "1.847", "2.010", 5, 1, 250.0, 1],
        ["6.5", 6.5, "1.680", "2.230", 6, 1, 250.0, 1],
    ],
    ["1.529", "2.650", None, 7, 0, 250.0, 1],  # moneyline: home / away
]

# 1st-5-innings period: no moneyline (None), like real NPB half markets.
HALF_PERIOD = [
    [[-0.5, 0.5, "0.5", "2.040", "1.781", 0, 1, 8, 0, 250.0, 1]],
    [["4.0", 4.0, "1.970", "1.847", 9, 1, 250.0, 1]],
    None,
]


def _raw(pregame=True, live=False):
    raw = {"n": None, "l": None}
    game = _event(100, "千葉羅德海洋", "福岡軟銀鷹",
                  {"0": FULL_PERIOD, "1": HALF_PERIOD})
    baseball = [po.BASEBALL_SPORT_ID, "Baseball", [[NPB, "日本職業棒球賽", [game]]]]
    if pregame:
        raw["n"] = [baseball]
    if live:
        raw["l"] = [baseball]
    return raw


def test_normalize_team_maps_known_names():
    assert po.normalize_team("讀賣巨人") == "巨人"
    assert po.normalize_team("橫濱海灣之星") == "DeNA"
    assert po.normalize_team("東京益力多燕子") == "ヤクルト"
    assert po.normalize_team("福岡軟銀鷹") == "ソフトバンク"
    assert po.normalize_team("不明球隊") == ""


def test_parse_events_returns_one_row_per_period():
    rows = po.parse_events(_raw())
    assert len(rows) == 2
    periods = {r["period"] for r in rows}
    assert periods == {"final", "half"}
    assert all(r["status"] == "pregame" for r in rows)
    assert all(r["home_norm"] == "ロッテ" for r in rows)
    assert all(r["away_norm"] == "ソフトバンク" for r in rows)


def test_final_period_odds_parsed():
    final = next(r for r in po.parse_events(_raw()) if r["period"] == "final")
    assert final["ml_home"] == 1.529
    assert final["ml_away"] == 2.65
    # main total is the most balanced line (7.0), not 7.5 / 6.5
    assert final["total_line"] == 7.0
    assert final["total_over"] == 1.847
    # main run line prefers ±1.5 and the balanced side
    assert final["spread_hdp"] == -1.5
    assert final["spread_home"] == 1.925


def test_half_period_has_no_moneyline_but_keeps_totals():
    half = next(r for r in po.parse_events(_raw()) if r["period"] == "half")
    assert half["ml_home"] is None
    assert half["ml_away"] is None
    assert half["total_line"] == 4.0
    assert half["spread_hdp"] == -0.5


def test_live_games_skipped_by_default():
    raw = _raw(pregame=False, live=True)
    assert po.parse_events(raw) == []
    included = po.parse_events(raw, include_live=True)
    assert len(included) == 2
    assert all(r["status"] == "live" for r in included)


def test_non_npb_leagues_filtered_out():
    raw = _raw()
    mlb_game = _event(200, "道奇", "教士", {"0": FULL_PERIOD})
    raw["n"][0][2].append([MLB, "MLB", [mlb_game]])
    rows = po.parse_events(raw)
    assert {r["league_id"] for r in rows} == {NPB}


# --- MLB ------------------------------------------------------------------

# The 1st-inning 3-way market MLB events always carry. Never recorded.
INNING1_PERIOD = [
    [[0.0, 0.0, "0.0", "1.628", "2.320", 0, 0, 1, 0, 250.0, 1]],
    [["0.5", 0.5, "2.000", "1.840", 1, 0, 250.0, 1]],
    ["4.630", "3.390", "1.746", 1, 0, 250.0, 1],
]


def _mlb_raw():
    game = _event(
        300, "紐約洋基", "聖路易紅雀\n",
        {"0": FULL_PERIOD, "1": HALF_PERIOD, "3": INNING1_PERIOD},
        english=["New York Yankees", "St. Louis Cardinals"],
    )
    return {"n": [[po.BASEBALL_SPORT_ID, "Baseball", [[MLB, "MLB", [game]]]]],
            "l": None}


def test_mlb_uses_english_names_and_own_columns():
    rows = po.parse_events(_mlb_raw(), league=po.MLB)
    assert len(rows) == 2  # final + half; the 1st-inning period is dropped
    final = next(r for r in rows if r["period"] == "final")
    assert final["home_norm"] == "New York Yankees"
    assert final["away_norm"] == "St. Louis Cardinals"
    assert final["away_team"] == "聖路易紅雀"  # feed newline stripped
    assert "start_et" in final and "start_jst" not in final


def test_mlb_first_inning_period_ignored_without_warning(capsys):
    po.parse_events(_mlb_raw(), league=po.MLB)
    assert "unmapped period" not in capsys.readouterr().out


def test_mlb_rows_follow_mlb_headers():
    rows = po.parse_events(_mlb_raw(), league=po.MLB)
    rows[0]["mlb_game_pk"] = 823520
    rows[0]["home_abbr"], rows[0]["away_abbr"] = "NYY", "STL"
    values = po.snapshots_to_rows(rows, "close", "2026-08-03 19:00:00", po.MLB)
    headers = po.MLB.sheet_headers()
    row = dict(zip(headers, values[0]))
    assert row["mlb_game_pk"] == 823520
    assert (row["home_abbr"], row["away_abbr"]) == ("NYY", "STL")
    assert row["snapshot_type"] == "close"
    assert len(values[0]) == len(headers)


def test_all_leagues_keeps_everything():
    raw = _raw()
    raw["n"][0][2].append([MLB, "MLB", [_event(400, "道奇", "教士",
                                               {"0": FULL_PERIOD})]])
    rows = po.parse_events(raw, all_leagues=True)
    assert {r["league_id"] for r in rows} == {NPB, MLB}


# --- MLB schedule join ----------------------------------------------------

def _sched_game(game_pk, official_date, home, away, start_utc):
    def team(name, club, nick, abbr):
        return {"name": name, "clubName": club, "teamName": nick,
                "abbreviation": abbr}

    return {
        "gamePk": game_pk,
        "officialDate": official_date,
        "gameDate": start_utc,
        "teams": {"home": {"team": team(*home)}, "away": {"team": team(*away)}},
    }


DBACKS = ("Arizona Diamondbacks", "Diamondbacks", "D-backs", "AZ")
PADRES = ("San Diego Padres", "Padres", "Padres", "SD")
YANKEES = ("New York Yankees", "Yankees", "Yankees", "NYY")
CARDS = ("St. Louis Cardinals", "Cardinals", "Cardinals", "STL")


def _index():
    return MlbGameIndex([
        _sched_game(825095, "2026-08-03", DBACKS, PADRES, "2026-08-04T01:40:00Z"),
        _sched_game(823520, "2026-08-03", YANKEES, CARDS, "2026-08-03T23:05:00Z"),
        # doubleheader: same pairing, two start times
        _sched_game(823521, "2026-08-05", YANKEES, CARDS, "2026-08-05T17:05:00Z"),
        _sched_game(823522, "2026-08-05", YANKEES, CARDS, "2026-08-05T21:05:00Z"),
    ])


def _utc(iso):
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def test_index_matches_despite_nickname_mismatch():
    # PS3838 says "Arizona Diamondbacks"; the API's teamName is "D-backs".
    game = _index().find("Arizona Diamondbacks", "San Diego Padres",
                         _utc("2026-08-04T01:40:00"))
    assert game["game_pk"] == 825095
    # officialDate is the ballpark's day, not the UTC day
    assert game["official_date"] == "2026-08-03"


def test_index_picks_nearest_start_for_doubleheader():
    game = _index().find("New York Yankees", "St. Louis Cardinals",
                         _utc("2026-08-05T21:10:00"))
    assert game["game_pk"] == 823522


def test_index_rejects_start_beyond_drift_window():
    far = _utc("2026-08-03T23:05:00") + timedelta(hours=9)
    assert _index().find("New York Yankees", "St. Louis Cardinals", far) is None


def test_index_returns_none_for_unknown_teams():
    assert _index().find("Yokohama DeNA BayStars", "Hanshin Tigers",
                         _utc("2026-08-03T23:05:00")) is None


def test_enrich_mlb_fills_join_columns(monkeypatch):
    monkeypatch.setattr("baseball.mlb_games.build_index", lambda starts, **kw: _index())
    snapshots = po.parse_events(_mlb_raw(), league=po.MLB)
    for s in snapshots:
        s["start"] = _utc("2026-08-03T23:05:00")
    po.enrich_mlb(snapshots)
    assert all(s["mlb_game_pk"] == 823520 for s in snapshots)
    assert all(s["game_date"] == "2026-08-03" for s in snapshots)
    assert all(s["home_abbr"] == "NYY" for s in snapshots)
