"""Unit tests for the PS3838 NPB odds parser (baseball/pinnacle_odds.py)."""

from baseball import pinnacle_odds as po

NPB = po.NPB_LEAGUE_ID
START_MS = 1784350800000  # fixed timestamp; bucket (not time) decides live/pregame


def _event(event_id, home_zh, away_zh, odds):
    """Build a minimal compact event array (parser reads [0,1,2,4,8])."""
    return [event_id, home_zh, away_zh, 0, START_MS, 0, 0, 0, odds]


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
    raw["n"][0][2].append([246, "美國職業棒球大聯盟", [mlb_game]])
    rows = po.parse_events(raw)
    assert {r["league_id"] for r in rows} == {NPB}
