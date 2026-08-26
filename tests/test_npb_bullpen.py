"""
Unit tests for NPB bullpen usage & fatigue logic.
Covers: outs/IP conversion, box-score pitcher parsing, play-by-play pitching
        changes and score orientation, situation classification, appearance
        assembly, role assignment and per-team fatigue snapshots.
"""

from baseball.npb_bullpen import (
    ROLE_CLOSER,
    ROLE_MOPUP,
    ROLE_SETUP,
    ROLE_STARTER,
    ROLE_SWING,
    ROLE_THIN,
    SIT_CLOSE,
    SIT_LOSE,
    SIT_MID,
    SIT_MOP,
    SIT_START,
    SIT_WIN,
    assign_roles,
    build_appearances,
    classify_situation,
    ip_display,
    outs_from_ip,
    parse_game_stats,
    parse_pitching_changes,
    pitcher_fatigue,
    resolve_score_orientation,
    team_fatigue_rows,
)

# --- Fixtures -------------------------------------------------------------


def _pitcher_row(result, name, player_id, ip, pitches, runs=0, hits=0, walks=0):
    return f"""
    <tr class="bb-scoreTable__row">
      <td>{result}</td>
      <td class="bb-scoreTable__data--player">
        <a href="/npb/player/{player_id}/top">{name}</a>
      </td>
      <td>2.50</td><td>{ip}</td><td>{pitches}</td><td>10</td>
      <td>{hits}</td><td>0</td><td>3</td><td>{walks}</td><td>0</td><td>0</td>
      <td>{runs}</td><td>{runs}</td>
    </tr>
    """


PITCH_HEADER = """
  <tr>
    <th class="bb-scoreTable__head"></th>
    <th class="bb-scoreTable__head">選手名</th>
    <th class="bb-scoreTable__head">防御率</th>
    <th class="bb-scoreTable__head">投球回</th>
    <th class="bb-scoreTable__head">投球数</th>
    <th class="bb-scoreTable__head">打者</th>
    <th class="bb-scoreTable__head">被安打</th>
    <th class="bb-scoreTable__head">被本塁打</th>
    <th class="bb-scoreTable__head">奪三振</th>
    <th class="bb-scoreTable__head">与四球</th>
    <th class="bb-scoreTable__head">与死球</th>
    <th class="bb-scoreTable__head">ボーク</th>
    <th class="bb-scoreTable__head">失点</th>
    <th class="bb-scoreTable__head">自責点</th>
  </tr>
"""


def _stats_html():
    """A game where 阪神 (away) beat 巨人 (home) 5-3."""
    away_rows = _pitcher_row("勝", "村上", "1", "6", 95, runs=3) + _pitcher_row(
        "", "岩崎", "2", "2", 25, runs=0
    ) + _pitcher_row("Ｓ", "岩貞", "3", "1", 12, runs=0)
    home_rows = _pitcher_row("敗", "山﨑", "4", "5.1", 90, runs=4) + _pitcher_row(
        "", "田和", "5", "2.2", 30, runs=1
    ) + _pitcher_row("", "代木", "6", "1", 15, runs=0)
    return f"""
    <html><head><title>2026年8月11日 読売ジャイアンツvs.阪神タイガース 試合出場成績</title></head>
    <body>
      <table class="bb-gameScoreTable">
        <tr class="bb-gameScoreTable__row">
          <th class="bb-gameScoreTable__team">阪神</th>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">2</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">1</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">2</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__total">5</td>
        </tr>
        <tr class="bb-gameScoreTable__row">
          <th class="bb-gameScoreTable__team">巨人</th>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">1</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">2</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__score">0</td>
          <td class="bb-gameScoreTable__total">3</td>
        </tr>
      </table>
      <table class="bb-scoreTable">{PITCH_HEADER}{away_rows}</table>
      <table class="bb-scoreTable">{PITCH_HEADER}{home_rows}</table>
    </body></html>
    """


def _live_section(inning, items):
    body = "".join(items)
    return f"""
    <section class="bb-liveText">
      <div class="bb-liveText__inning">{inning}</div>
      {body}
    </section>
    """


def _change_item(from_name, from_id, to_name, to_id, *, mid_inning=False):
    text = (
        f"ピッチャー {from_name} に代わって {to_name} がマウンドにあがる"
        if mid_inning
        else "投手交代:"
    )
    return f"""
    <li class="bb-liveText__item">
      <p class="bb-liveText__summary bb-liveText__summary--change">
        <span class="bb-liveText__state">{text}</span>
        <a class="bb-liveText__player" href="/npb/player/{from_id}/top">{from_name}</a>
        <span class="bb-liveText__state">→</span>
        <a class="bb-liveText__player" href="/npb/player/{to_id}/top">{to_name}</a>
      </p>
    </li>
    """


def _point_item(text):
    return f"""
    <li class="bb-liveText__item">
      <p class="bb-liveText__summary bb-liveText__summary--point">
        <span class="bb-liveText__state">{text}</span>
      </p>
    </li>
    """


def _text_html():
    """Play-by-play for the same game. Yahoo prints the home team first."""
    return f"""
    <html><body>
      {_live_section("2回表", [_point_item("2ランホームラン！ 巨 0-2 神")])}
      {_live_section("3回裏", [_point_item("タイムリー 巨 1-2 神")])}
      {_live_section("5回表", [_point_item("犠牲フライ 巨 1-3 神")])}
      {_live_section("6回表", [_change_item("山﨑", "4", "田和", "5", mid_inning=True)])}
      {_live_section("6回裏", [_point_item("2点タイムリー 巨 3-3 神")])}
      {_live_section("7回表", [_point_item("2点タイムリー 巨 3-5 神")])}
      {_live_section("8回表", [_change_item("田和", "5", "代木", "6")])}
      {_live_section("8回裏", [_change_item("村上", "1", "岩崎", "2")])}
      {_live_section("9回裏", [_change_item("岩崎", "2", "岩貞", "3")])}
    </body></html>
    """


# --- Innings pitched ------------------------------------------------------


def test_outs_from_ip_handles_thirds():
    assert outs_from_ip("5") == 15
    assert outs_from_ip("5.1") == 16
    assert outs_from_ip("5.2") == 17
    assert outs_from_ip("") == 0
    assert outs_from_ip("-") == 0


def test_ip_display_round_trips():
    for raw in ("0", "1", "5.1", "5.2", "9"):
        assert ip_display(outs_from_ip(raw)) == raw


# --- Box score ------------------------------------------------------------


def test_parse_game_stats_reads_teams_date_and_staffs():
    game = parse_game_stats(_stats_html())
    assert game["date"] == "2026-08-11"
    # Row order on the stats page is away first, regardless of the title.
    assert game["away"] == "阪神"
    assert game["home"] == "巨人"
    assert game["away_runs"] == 5
    assert game["home_runs"] == 3
    assert [p["name"] for p in game["away_pitchers"]] == ["村上", "岩崎", "岩貞"]
    assert [p["outs"] for p in game["home_pitchers"]] == [16, 8, 3]


def test_parse_game_stats_normalizes_fullwidth_save_marker():
    game = parse_game_stats(_stats_html())
    assert game["away_pitchers"][2]["result"] == "S"


def test_parse_game_stats_rejects_non_npb_matchups():
    html = _stats_html().replace("阪神", "全パ")
    assert parse_game_stats(html) is None


# --- Play-by-play ---------------------------------------------------------


def test_parse_pitching_changes_catches_both_wordings():
    changes = parse_pitching_changes(_text_html())
    # The home staff's 6th-inning swap uses the "に代わって" wording; missing it
    # would drop every mid-inning entry.
    assert len(changes["home"]) == 2
    assert len(changes["away"]) == 2
    assert changes["final"] == (3, 5)


def test_score_orientation_detected_from_batting_half():
    changes = parse_pitching_changes(_text_html())
    assert resolve_score_orientation(changes, away_runs=5, home_runs=3) == "home_first"


def test_score_orientation_falls_back_to_final_score():
    changes = {"slot0_top": 0, "slot0_bottom": 0, "final": (5, 3)}
    assert resolve_score_orientation(changes, away_runs=5, home_runs=3) == "away_first"


def test_score_orientation_gives_up_on_a_tie_with_no_evidence():
    changes = {"slot0_top": 0, "slot0_bottom": 0, "final": (3, 3)}
    assert resolve_score_orientation(changes, away_runs=3, home_runs=3) is None


# --- Situations -----------------------------------------------------------


def test_classify_situation_save_spot_is_a_winning_spot():
    assert classify_situation(8, 1) == SIT_WIN
    assert classify_situation(9, 3) == SIT_WIN


def test_classify_situation_big_lead_is_mop_up_however_late():
    assert classify_situation(9, 5) == SIT_MOP
    assert classify_situation(6, 7) == SIT_MOP


def test_classify_situation_three_run_deficit_is_losing_however_late():
    assert classify_situation(9, -3) == SIT_LOSE
    assert classify_situation(7, -6) == SIT_LOSE


def test_classify_situation_tie_is_close_at_any_point():
    assert classify_situation(5, 0) == SIT_CLOSE
    assert classify_situation(9, 0) == SIT_CLOSE


def test_classify_situation_early_or_four_run_lead_is_middle():
    assert classify_situation(4, 2) == SIT_MID
    assert classify_situation(8, 4) == SIT_MID


# --- Appearances ----------------------------------------------------------


def test_build_appearances_derives_entry_inning_from_cumulative_outs():
    game = parse_game_stats(_stats_html())
    game["game_id"] = "g1"
    records = build_appearances(game, parse_pitching_changes(_text_html()))

    home = [r for r in records if r["team"] == "巨人"]
    assert [(r["entry_inning"], r["entry_outs"]) for r in home] == [
        (1, 0),
        (6, 1),
        (9, 0),
    ]


def test_build_appearances_takes_entry_score_from_play_by_play():
    game = parse_game_stats(_stats_html())
    game["game_id"] = "g1"
    records = build_appearances(game, parse_pitching_changes(_text_html()))

    by_name = {r["pitcher"]: r for r in records}
    # 田和 came on down 1-3 in the 6th; the line score alone would have said 1-3
    # too, but 代木 entered at 3-5 after the 7th, which only the text knows.
    assert (by_name["田和"]["own_score"], by_name["田和"]["opp_score"]) == (1, 3)
    assert (by_name["代木"]["own_score"], by_name["代木"]["opp_score"]) == (3, 5)
    # Both 阪神 relievers inherited a two-run lead late — save territory.
    assert by_name["岩崎"]["situation"] == SIT_WIN
    assert by_name["岩貞"]["situation"] == SIT_WIN
    assert all(r["score_source"] == "text" for r in records if r["order"] > 1)


def test_build_appearances_falls_back_to_line_score_without_text():
    game = parse_game_stats(_stats_html())
    game["game_id"] = "g1"
    records = build_appearances(game, None)

    relievers = [r for r in records if r["order"] > 1]
    assert all(r["score_source"] == "linescore" for r in relievers)
    by_name = {r["pitcher"]: r for r in records}
    # Entering in the 9th, the line score through 8 innings is exact.
    assert (by_name["岩貞"]["own_score"], by_name["岩貞"]["opp_score"]) == (5, 3)


def test_build_appearances_marks_starters():
    game = parse_game_stats(_stats_html())
    game["game_id"] = "g1"
    records = build_appearances(game, parse_pitching_changes(_text_html()))
    starters = [r for r in records if r["order"] == 1]
    assert len(starters) == 2
    assert all(r["situation"] == SIT_START for r in starters)


# --- Roles ----------------------------------------------------------------


def _appearance(date, team, pitcher, situation, *, inning=8, pitches=15, result=""):
    return {
        "date": date,
        "game_id": f"{date}-{team}",
        "team": team,
        "opponent": "巨人",
        "home_away": "主",
        "pitcher": pitcher,
        "player_id": pitcher,
        "order": 1 if situation == SIT_START else 2,
        "entry_inning": inning,
        "entry_outs": 0,
        "own_score": 0,
        "opp_score": 0,
        "diff": 0,
        "situation": situation,
        "score_source": "text",
        "outs": 3,
        "ip": "1",
        "pitches": pitches,
        "batters": 4,
        "hits": 1,
        "hr": 0,
        "bb": 0,
        "hbp": 0,
        "so": 1,
        "runs": 0,
        "er": 0,
        "result": result,
    }


def test_assign_roles_separates_high_and_low_leverage_arms():
    appearances = []
    for day in range(1, 6):
        date = f"2026-08-{day:02d}"
        appearances.append(_appearance(date, "阪神", "岩崎", SIT_WIN))
        appearances.append(_appearance(date, "阪神", "桐敷", SIT_LOSE, inning=6))

    roles = assign_roles(appearances, "2026-08-05")
    assert roles[("阪神", "岩崎")]["role"] == ROLE_SETUP
    assert roles[("阪神", "桐敷")]["role"] == ROLE_MOPUP


def test_assign_roles_identifies_the_closer_by_ninth_inning_share():
    appearances = [
        _appearance(f"2026-08-{d:02d}", "阪神", "岩貞", SIT_WIN, inning=9, result="S")
        for d in range(1, 5)
    ]
    roles = assign_roles(appearances, "2026-08-04")
    assert roles[("阪神", "岩貞")]["role"] == ROLE_CLOSER
    assert roles[("阪神", "岩貞")]["saves"] == 4


def test_assign_roles_needs_a_save_to_call_someone_the_closer():
    # Eating the ninth is not the same as closing: a committee that works tied
    # and trailing ninths racks up the same ninth-inning share as a real closer,
    # so a save in the window is what separates them.
    appearances = [
        _appearance(f"2026-08-{d:02d}", "巨人", "赤星", SIT_CLOSE, inning=9)
        for d in range(1, 5)
    ]
    roles = assign_roles(appearances, "2026-08-04")
    assert roles[("巨人", "赤星")]["role"] == ROLE_SETUP
    assert roles[("巨人", "赤星")]["ninth_ratio"] == 1.0


def test_assign_roles_treats_mostly_starting_pitchers_as_starters():
    appearances = [
        _appearance(f"2026-08-{d:02d}", "阪神", "村上", SIT_START) for d in (1, 6, 12)
    ]
    roles = assign_roles(appearances, "2026-08-12")
    assert roles[("阪神", "村上")]["role"] == ROLE_STARTER


def test_assign_roles_uses_saves_and_holds_on_a_thin_sample():
    # Two outings is below the situation-based threshold, but a hold is direct
    # evidence the manager trusts him with a lead.
    appearances = [
        _appearance("2026-08-01", "阪神", "石井", SIT_MID, result="H"),
        _appearance("2026-08-03", "阪神", "石井", SIT_MID),
    ]
    roles = assign_roles(appearances, "2026-08-03")
    assert roles[("阪神", "石井")]["role"] == ROLE_SETUP


def test_assign_roles_separates_too_thin_to_judge_from_genuinely_mixed():
    # Under three outings with no save or hold is not a middling role, it is an
    # absence of evidence — a just-promoted arm looks identical to a swing man
    # until he has pitched enough, and counting him as bullpen depth overstates
    # what the team actually has.
    thin = [
        _appearance("2026-08-01", "阪神", "富田", SIT_MID),
        _appearance("2026-08-03", "阪神", "富田", SIT_MID),
    ]
    assert assign_roles(thin, "2026-08-03")[("阪神", "富田")]["role"] == ROLE_THIN

    # Four outings split evenly between high and low leverage clears neither
    # threshold: a real swing man.
    mixed = [
        _appearance("2026-08-01", "阪神", "及川", SIT_WIN),
        _appearance("2026-08-02", "阪神", "及川", SIT_CLOSE),
        _appearance("2026-08-03", "阪神", "及川", SIT_LOSE),
        _appearance("2026-08-04", "阪神", "及川", SIT_MID),
    ]
    assert assign_roles(mixed, "2026-08-04")[("阪神", "及川")]["role"] == ROLE_SWING


def test_assign_roles_ignores_outings_outside_the_window():
    old = [
        _appearance("2026-07-01", "阪神", "岩崎", SIT_WIN),
        _appearance("2026-07-02", "阪神", "岩崎", SIT_WIN),
    ]
    assert assign_roles(old, "2026-08-05") == {}


# --- Fatigue --------------------------------------------------------------


def test_pitcher_fatigue_counts_consecutive_days():
    recs = [
        _appearance("2026-08-03", "阪神", "岩崎", SIT_WIN),
        _appearance("2026-08-04", "阪神", "岩崎", SIT_WIN),
        _appearance("2026-08-05", "阪神", "岩崎", SIT_WIN),
    ]
    fatigue = pitcher_fatigue(recs, "2026-08-05")
    assert fatigue["streak"] == 3
    assert fatigue["available"] is False


def test_pitcher_fatigue_flags_a_heavy_single_outing():
    recs = [_appearance("2026-08-05", "阪神", "岩崎", SIT_WIN, pitches=35)]
    fatigue = pitcher_fatigue(recs, "2026-08-05")
    assert fatigue["streak"] == 1
    assert fatigue["available"] is False


def test_pitcher_fatigue_treats_a_rested_arm_as_available():
    recs = [_appearance("2026-08-03", "阪神", "岩崎", SIT_WIN, pitches=12)]
    fatigue = pitcher_fatigue(recs, "2026-08-05")
    assert fatigue["streak"] == 0
    assert fatigue["available"] is True


def test_pitcher_fatigue_marks_a_long_absence_as_off_the_roster():
    # NPB demotion costs at least ten days, so a gap that long is a roster move
    # rather than rest — and an inactive arm is not an available one.
    recs = [_appearance("2026-07-20", "阪神", "岩崎", SIT_WIN)]
    fatigue = pitcher_fatigue(recs, "2026-08-05")
    assert fatigue["active"] is False
    assert fatigue["available"] is False


def test_team_fatigue_rows_cover_every_team_and_count_available_arms():
    appearances = []
    for day in range(1, 6):
        date = f"2026-08-{day:02d}"
        appearances.append(_appearance(date, "阪神", "岩崎", SIT_WIN))
    # Rested setup man: eligible and available.
    appearances.append(_appearance("2026-08-01", "阪神", "岩貞", SIT_WIN, result="S"))
    appearances.append(_appearance("2026-08-02", "阪神", "岩貞", SIT_WIN, result="S"))
    appearances.append(_appearance("2026-08-03", "阪神", "岩貞", SIT_WIN, result="S"))

    rows = {row["team"]: row for row in team_fatigue_rows(appearances, "2026-08-05")}
    assert len(rows) == 12
    hanshin = rows["阪神"]
    assert hanshin["elite_total"] == 2
    # 岩崎 is on a five-day run, 岩貞 has had two days off.
    assert hanshin["elite_available"] == 1
    assert "岩貞" in hanshin["available_names"]


def test_team_fatigue_rows_report_bullpen_workload_over_recent_games():
    appearances = []
    for day in range(1, 6):
        date = f"2026-08-{day:02d}"
        appearances.append(_appearance(date, "阪神", "村上", SIT_START))
        appearances.append(_appearance(date, "阪神", "岩崎", SIT_WIN))

    row = {r["team"]: r for r in team_fatigue_rows(appearances, "2026-08-05")}["阪神"]
    assert row["games"] == 5
    assert row["relief_ip"] == 5.0
    assert row["relief_ip_per_game"] == 1.0
    assert row["starter_ip_per_game"] == 1.0


def test_team_fatigue_rows_report_bullpen_era_from_earned_runs():
    # Five relief innings, four earned of five total runs → 4 * 9 / 5 = 7.20.
    appearances = []
    for day in range(1, 6):
        date = f"2026-08-{day:02d}"
        rec = _appearance(date, "阪神", "岩崎", SIT_WIN)
        rec["runs"] = 1
        rec["er"] = 1 if day > 1 else 0
        appearances.append(rec)

    row = {r["team"]: r for r in team_fatigue_rows(appearances, "2026-08-05")}["阪神"]
    assert row["relief_ip"] == 5.0
    assert row["relief_runs"] == 5
    assert row["relief_er"] == 4
    assert row["relief_era"] == 7.20


def test_team_fatigue_rows_report_zero_era_for_an_unused_bullpen():
    appearances = [_appearance("2026-08-01", "阪神", "村上", SIT_START)]
    row = {r["team"]: r for r in team_fatigue_rows(appearances, "2026-08-05")}["阪神"]
    assert row["relief_ip"] == 0.0
    assert row["relief_era"] == 0.0


def test_team_fatigue_rows_split_era_by_tier_over_the_role_window():
    # A staff can be excellent at the top and awful underneath; blending them
    # into one bullpen ERA hides exactly the half that decides a total.
    appearances = []
    for day in range(1, 6):
        date = f"2026-08-{day:02d}"
        good = _appearance(date, "ロッテ", "中森", SIT_WIN)
        good["er"] = 0
        appearances.append(good)
        bad = _appearance(date, "ロッテ", "坂本", SIT_LOSE, inning=6)
        bad["er"] = 2
        appearances.append(bad)

    row = {r["team"]: r for r in team_fatigue_rows(appearances, "2026-08-05")}["ロッテ"]
    assert row["elite_era"] == 0.0
    assert row["other_era"] == 18.0
    assert row["elite_ip"] == 5.0
    assert row["other_ip"] == 5.0


def test_team_fatigue_rows_count_other_tier_depth_excluding_thin_samples():
    appearances = []
    for day in range(1, 6):
        date = f"2026-08-{day:02d}"
        appearances.append(_appearance(date, "阪神", "桐敷", SIT_LOSE, inning=6))
    # One outing, no save or hold — unknown, not depth.
    appearances.append(_appearance("2026-08-05", "阪神", "新人", SIT_MID))

    row = {r["team"]: r for r in team_fatigue_rows(appearances, "2026-08-05")}["阪神"]
    assert row["other_total"] == 1
    assert "桐敷" in row["other_names"]
    assert "新人" not in row["other_names"]


def test_team_fatigue_rows_project_bullpen_innings_from_starter_length():
    appearances = []
    for day in range(1, 6):
        date = f"2026-08-{day:02d}"
        starter = _appearance(date, "阪神", "村上", SIT_START)
        starter["outs"] = 15  # five innings
        appearances.append(starter)

    row = {r["team"]: r for r in team_fatigue_rows(appearances, "2026-08-05")}["阪神"]
    assert row["starter_ip_per_game"] == 5.0
    assert row["projected_relief_ip"] == 4.0
