import pathlib

import migration.update_mlb_last10 as m
from migration.update_mlb_last10 import (
    _aggregate_batting_average,
    _appearance_reset_request,
    _aggregate_on_base_percentage,
    _batting_average,
    _direction_from_description,
    _game_font_color_requests,
    _home_run_events_by_team,
    _base_font_request,
    _hide_gridlines_request,
    _header_format_request,
    _hide_top_rows_request,
    HR_BATTER_SPAN,
    HR_BAT_SIDE_COL,
    HR_DASH_SPAN,
    HR_DIRECTION_SPAN,
    HR_OPPONENT_COL,
    HR_PITCHER_SPAN,
    _hr_bat_side_font_requests,
    _hr_direction_font_requests,
    _hr_merge_requests,
    _hr_opponent_font_requests,
    _is_opposite_field_home_run,
    _extra_base_hits,
    _display_venue_name,
    _home_run_event_row_count,
    _home_run_layout,
    _on_base_percentage,
    _rate_text,
    _score_bracket_requests,
    _recent_home_runs,
)


def test_display_venue_name_shortens_oriole_park() -> None:
    assert _display_venue_name("Oriole Park at Camden Yards") == "Orioles"


def test_display_venue_name_prefers_venue_id() -> None:
    assert _display_venue_name("Any Sponsor Name", 2) == "Orioles"


def test_display_venue_name_uses_id_for_sponsored_venue_name_changes() -> None:
    assert _display_venue_name("UNIQLO Field at Dodger Stadium", 22) == "Dodgers"


def test_display_venue_name_uses_stable_team_name_for_astros_venue() -> None:
    assert _display_venue_name("Daikin Park", 2392) == "Astros"


def test_display_venue_name_uses_stable_team_name_for_giants_venue() -> None:
    assert _display_venue_name("Oracle Park", 2395) == "Giants"


def test_display_venue_name_keeps_unknown_venue() -> None:
    assert _display_venue_name("Fenway Park") == "Fenway Park"


def test_rate_text_strips_leading_zero() -> None:
    assert _rate_text(0.212) == ".212"


def test_rate_text_none_is_blank() -> None:
    assert _rate_text(None) == ""


def test_batting_average_divides_hits_by_at_bats() -> None:
    game = {"打數": 33, "安打": 7}
    assert _batting_average(game) == 7 / 33


def test_batting_average_none_without_at_bats() -> None:
    assert _batting_average({"安打": 7}) is None


def test_on_base_percentage_matches_mlb_feed_example() -> None:
    game = {"安打": 7, "四死": 10, "打數": 33, "犧飛": 2}
    assert round(_on_base_percentage(game), 3) == round(17 / 45, 3)


def test_on_base_percentage_none_when_denominator_zero() -> None:
    assert _on_base_percentage({"安打": 0, "四死": 0, "打數": 0, "犧飛": 0}) is None


def test_aggregate_batting_average_sums_across_games() -> None:
    games = [{"打數": 30, "安打": 6}, {"打數": 33, "安打": 7}]
    assert _aggregate_batting_average(games) == 13 / 63


def test_aggregate_on_base_percentage_sums_across_games() -> None:
    games = [
        {"安打": 6, "四死": 5, "打數": 30, "犧飛": 1},
        {"安打": 7, "四死": 10, "打數": 33, "犧飛": 2},
    ]
    assert round(_aggregate_on_base_percentage(games), 3) == round(28 / 81, 3)


def test_direction_from_description_left_center() -> None:
    description = "Elly De La Cruz homers (19) on a fly ball to left center field."
    assert _direction_from_description(description) == "左中本"


def test_direction_from_description_right_field() -> None:
    assert _direction_from_description("homers on a line drive to right field.") == "右本"


def test_direction_from_description_center_field() -> None:
    assert _direction_from_description("homers on a fly ball to center field.") == "中本"


def test_direction_from_description_unrecognized_is_blank() -> None:
    assert _direction_from_description("groundout to shortstop") == ""


def _game(date: str, hr_details: list | None = None) -> dict:
    return {
        "日期": date,
        "球場": "Yankee Stadium",
        "對戰球隊": "BOS",
        "對戰先發": "Some Pitcher",
        "全壘打明細": hr_details or [],
    }


def test_recent_home_runs_placeholder_row_for_no_hr_game() -> None:
    events = _recent_home_runs([_game("2026/8/8")])
    assert len(events) == 1
    assert events[0]["_no_hr"] is True
    assert events[0]["打者"] == "─" * 12
    assert events[0]["投手"] == "Some Pitcher"


def test_recent_home_runs_expands_multiple_events_same_game() -> None:
    details = [
        {"打者": "Player A", "左右打": "L", "方向": "左本", "投手": "Pitcher X"},
        {"打者": "Player B", "左右打": "R", "方向": "中本", "投手": "Pitcher X"},
    ]
    events = _recent_home_runs([_game("2026/8/8", details)])
    assert len(events) == 2
    assert events[0]["_日期"] == "2026/8/8"
    assert events[1]["_日期"] == ""
    assert events[0]["打者"] == "Player A"
    assert events[1]["打者"] == "Player B"


def test_recent_home_runs_only_looks_back_six_games() -> None:
    games = [_game(f"2026/8/{day}") for day in range(1, 8)]
    events = _recent_home_runs(games)
    assert len(events) == 6


def test_home_run_event_row_count_minimum_one() -> None:
    assert _home_run_event_row_count([]) == 1


def test_home_run_layout_uses_max_across_matchups() -> None:
    many_hr_details = [
        {"打者": f"Player {i}", "左右打": "L", "方向": "中本", "投手": "P"}
        for i in range(3)
    ]
    games_by_team = {
        "AWAY1": [_game("2026/8/8")],
        "HOME1": [_game("2026/8/8", many_hr_details)],
    }
    layout = _home_run_layout([("AWAY1", "HOME1")], games_by_team)
    assert layout["top_event_rows"] == 1
    assert layout["bottom_event_rows"] == 3
    assert layout["top_end"] == layout["top_header"] + 1
    assert layout["bottom_header"] == layout["top_end"] + 2
    assert layout["bottom_end"] == layout["bottom_header"] + 3


class TestAppearanceReset:
    def test_reset_covers_the_whole_block_area_including_the_gaps(self):
        request = _appearance_reset_request(7, 3, 52, 2, 45)["repeatCell"]
        assert request["range"] == {
            "sheetId": 7, "startRowIndex": 2, "endRowIndex": 52,
            "startColumnIndex": 1, "endColumnIndex": 45,
        }
        # blank cell + these fields means "back to default", fill and font colour both
        assert request["cell"] == {}
        assert request["fields"] == (
            "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
        )

    def test_reset_runs_before_anything_is_painted(self):
        # a reset issued after the header fill would wipe it
        source = pathlib.Path("migration/update_mlb_last10.py").read_text()
        body = source[source.index("def _update_sheet"):]
        assert body.index("_appearance_reset_request") < body.index(
            "_header_format_request"
        )

    def test_the_pitcher_column_is_never_given_a_colour(self):
        games = [{"得分": 5, "失分": 1, "安打": 12, "打率": ".300", "上率": ".380",
                  "對戰先發": "Walker Buehler"}] * 10
        coloured = {
            r["repeatCell"]["range"]["startColumnIndex"]
            for r in _game_font_color_requests(7, games, 4, 2)
        }
        pitcher_col = 2 + 1
        assert pitcher_col not in coloured


def test_gridlines_are_hidden_like_npb():
    request = _hide_gridlines_request(7)["updateSheetProperties"]
    assert request["properties"] == {
        "sheetId": 7, "gridProperties": {"hideGridlines": True},
    }
    assert request["fields"] == "gridProperties.hideGridlines"


class TestExtraBaseHits:
    def test_counts_doubles_triples_and_home_runs(self):
        # NPB's 近十場 shows 長打, so MLB's M column matches rather than showing
        # home runs alone — the home-run detail lives in its own block below
        batting = {"doubles": 3, "triples": 1, "homeRuns": 2}
        assert _extra_base_hits(batting) == 6

    def test_missing_fields_count_as_zero(self):
        assert _extra_base_hits({"homeRuns": 1}) == 1
        assert _extra_base_hits({}) == 0

    def test_header_reads_長打(self):
        source = pathlib.Path("migration/update_mlb_last10.py").read_text()
        assert '"長 打"' in source
        assert '"本 打"' not in source


class TestHomeRunMerges:
    def _games(self, with_hr: bool):
        game = {"日期": "2026-08-14", "球場": "Rogers Centre", "對戰球隊": "TOR",
                "對戰先發": "Kevin Gausman", "全壘打明細": (
                    [{"打者": "Aaron Judge", "左右打": "R", "方向": "左本",
                      "投手": "Kevin Gausman"}] if with_hr else [])}
        return [game]

    def test_wide_fields_get_the_columns_they_need(self):
        requests = _hr_merge_requests(7, self._games(True), 30, 2, 1)
        merges = [r["mergeCells"]["range"] for r in requests if "mergeCells" in r]
        spans = {(m["startColumnIndex"], m["endColumnIndex"]) for m in merges}
        base = 2 - 1
        # 打者 takes the wide columns, 方向 two, 投手 five — nothing is clipped
        assert (base + HR_BATTER_SPAN[0], base + HR_BATTER_SPAN[1]) in spans
        assert (base + HR_DIRECTION_SPAN[0], base + HR_DIRECTION_SPAN[1]) in spans
        assert (base + HR_PITCHER_SPAN[0], base + HR_PITCHER_SPAN[1]) in spans
        # 球場 is gone: one home park per club, so 球隊 already says where

    def test_header_row_is_merged_too(self):
        requests = _hr_merge_requests(7, self._games(True), 30, 2, 1)
        rows = {r["mergeCells"]["range"]["startRowIndex"] for r in requests
                if "mergeCells" in r}
        assert 29 in rows  # the 日期/打者/投手 header itself
        assert 30 in rows  # and the event row under it

    def test_a_game_without_a_home_run_runs_its_dash_across_to_方向(self):
        with_hr = _hr_merge_requests(7, self._games(True), 30, 2, 1)
        without = _hr_merge_requests(7, self._games(False), 30, 2, 1)
        batter_span = (1 + HR_DASH_SPAN[0], 1 + HR_DASH_SPAN[1])

        def spans(reqs):
            return {(r["mergeCells"]["range"]["startColumnIndex"],
                     r["mergeCells"]["range"]["endColumnIndex"])
                    for r in reqs if "mergeCells" in r
                    and r["mergeCells"]["range"]["startRowIndex"] == 30}

        assert batter_span in spans(without)
        assert batter_span not in spans(with_hr)

    def test_existing_merges_are_dropped_first_so_a_re_run_is_safe(self):
        requests = _hr_merge_requests(7, self._games(True), 30, 2, 2)
        assert "unmergeCells" in requests[0]
        area = requests[0]["unmergeCells"]["range"]
        assert area["startRowIndex"] == 29 and area["endRowIndex"] == 32
        assert area["startColumnIndex"] == 1 and area["endColumnIndex"] == 15


class TestOppositeField:
    def test_a_lefty_pulling_to_right_is_not_flagged(self):
        assert _is_opposite_field_home_run("L", "右本") is False

    def test_a_lefty_going_the_other_way_is_flagged(self):
        assert _is_opposite_field_home_run("L", "左本") is True
        assert _is_opposite_field_home_run("L", "左中本") is True

    def test_a_righty_going_the_other_way_is_flagged(self):
        assert _is_opposite_field_home_run("R", "右本") is True
        assert _is_opposite_field_home_run("R", "右中本") is True
        assert _is_opposite_field_home_run("R", "左本") is False

    def test_dead_centre_is_always_flagged(self):
        assert _is_opposite_field_home_run("L", "中本") is True
        assert _is_opposite_field_home_run("R", "中本") is True

    def test_unknown_side_or_direction_is_not_flagged(self):
        assert _is_opposite_field_home_run("", "左本") is False
        assert _is_opposite_field_home_run("R", "") is False


class TestHomeRunFontColours:
    def _games(self, side, direction, opponent="TOR"):
        return [{"日期": "2026-08-14", "球場": "Rogers Centre", "對戰球隊": opponent,
                 "對戰先發": "Kevin Gausman",
                 "全壘打明細": [{"打者": "Aaron Judge", "左右打": side,
                                 "方向": direction, "投手": "Kevin Gausman"}]}]

    def _colour(self, request):
        fmt = request["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"]
        rgb = fmt["foregroundColor"]
        return "%02x%02x%02x" % tuple(round(rgb.get(k, 0) * 255)
                                      for k in ("red", "green", "blue"))

    def test_bat_side_is_blue_for_right_red_for_left(self):
        right = _hr_bat_side_font_requests(7, self._games("R", "左本"), 30, 2, 1)
        left = _hr_bat_side_font_requests(7, self._games("L", "右本"), 30, 2, 1)
        assert self._colour(right[0]) == "1155cc"
        assert self._colour(left[0]) == "cc0000"
        # the 打位 column, not its neighbours
        assert right[0]["repeatCell"]["range"]["startColumnIndex"] == 1 + HR_BAT_SIDE_COL

    def test_direction_turns_red_only_for_opposite_field_and_centre(self):
        opposite = _hr_direction_font_requests(7, self._games("R", "右本"), 30, 2, 1)
        pulled = _hr_direction_font_requests(7, self._games("R", "左本"), 30, 2, 1)
        centre = _hr_direction_font_requests(7, self._games("L", "中本"), 30, 2, 1)
        assert self._colour(opposite[0]) == "ff0000"
        assert self._colour(centre[0]) == "ff0000"
        assert self._colour(pulled[0]) != "ff0000"
        assert opposite[0]["repeatCell"]["range"]["startColumnIndex"] == 1 + HR_DIRECTION_SPAN[0]

    def test_opponent_cell_takes_that_team_colour(self):
        requests = _hr_opponent_font_requests(7, self._games("R", "左本", "TOR"),
                                              30, 2, 1)
        assert self._colour(requests[0]) == "134a8e"  # Blue Jays
        assert requests[0]["repeatCell"]["range"]["startColumnIndex"] == 1 + HR_OPPONENT_COL

    def test_empty_rows_are_reset_rather_than_left_coloured(self):
        # two slots, one event: the second row must still be emitted, in default ink
        requests = _hr_bat_side_font_requests(7, self._games("L", "左本"), 30, 2, 2)
        assert len(requests) == 2
        assert self._colour(requests[1]) == "202124"


def test_unmerge_is_issued_before_values_are_written():
    """A value written onto a cell an old merge swallowed is dropped silently."""
    source = pathlib.Path("migration/update_mlb_last10.py").read_text()
    body = source[source.index("clear_col_l = "):]
    assert body.index('f"unmerge ') < body.index('f"write ')
    assert body.index('f"write ') < body.index('f"format ')


class TestSheetChrome:
    def test_the_two_empty_top_rows_are_hidden(self):
        request = _hide_top_rows_request(7)["updateDimensionProperties"]
        assert request["range"] == {"sheetId": 7, "dimension": "ROWS",
                                    "startIndex": 0, "endIndex": 2}
        assert request["properties"] == {"hiddenByUser": True}

    def test_the_seam_between_blocks_is_hairline(self):
        from migration.update_mlb_last10 import GAP_COLUMN_WIDTH
        assert GAP_COLUMN_WIDTH == 2  # as NPB's P / AE are

    def test_one_typeface_across_the_area(self):
        request = _base_font_request(7, 3, 52, 2, 45)["repeatCell"]
        text = request["cell"]["userEnteredFormat"]["textFormat"]
        assert text == {"fontFamily": "Arial Black", "fontSize": 10, "bold": True}
        # colour is left alone here — it is set per cell further down
        assert "foregroundColor" not in request["fields"]

    def test_the_header_keeps_the_typeface_it_overwrites(self):
        # this request replaces the whole textFormat, so it has to restate the font
        text = _header_format_request(7, "NYY", 3, 2)["repeatCell"]["cell"][
            "userEnteredFormat"]["textFormat"]
        assert text["fontFamily"] == "Arial Black"
        assert text["fontSize"] == 10
        assert text["bold"] is True

    def test_base_font_is_applied_before_the_smaller_overrides(self):
        source = pathlib.Path("migration/update_mlb_last10.py").read_text()
        body = source[source.index("def _update_sheet"):]
        assert body.index("_base_font_request") < body.index("_hr_pitcher_font_requests")


class TestScheduleTeamCodes:
    """近十場 reads the API directly, so it needs the same code mapping 紀錄 uses.

    The schedule still calls the Athletics ATH; 紀錄 stores OAK. Left unmapped, the
    block header said ATH while its ten game rows — looked up by 紀錄's code — came
    back empty.
    """

    def test_matchups_from_the_schedule_are_canonical(self):
        payload = {"dates": [{"games": [{
            "gameType": "R", "gameDate": "2026-08-15T20:10:00Z",
            "teams": {"away": {"team": {"abbreviation": "TEX"}},
                      "home": {"team": {"abbreviation": "ATH"}}},
        }]}]}
        assert m._matchups_from_schedule(payload) == [("TEX", "OAK")]

    def test_home_run_events_are_keyed_by_the_canonical_code(self):
        feed = {"liveData": {"plays": {"allPlays": [{
            "result": {"eventType": "home_run", "description": "homers to left field"},
            "about": {"isTopInning": False},
            "matchup": {"batter": {"fullName": "Brent Rooker"},
                        "batSide": {"code": "R"},
                        "pitcher": {"fullName": "Jacob deGrom"}},
        }]}}}
        game_data = {"teams": {"away": {"abbreviation": "TEX"},
                               "home": {"abbreviation": "ATH"}}}
        events = _home_run_events_by_team(feed, game_data)
        assert set(events) == {"TEX", "OAK"}
        assert events["OAK"][0]["打者"] == "Brent Rooker"


class TestScoreBracket:
    def test_dashed_rules_hug_得点_and_失点(self):
        clear, bracket = _score_bracket_requests(7, 4, 2)
        area = bracket["updateBorders"]["range"]
        assert (area["startColumnIndex"], area["endColumnIndex"]) == (6, 8)  # G:H
        assert bracket["updateBorders"]["left"]["style"] == "DASHED"
        assert bracket["updateBorders"]["right"]["style"] == "DASHED"
        assert "innerVertical" not in bracket["updateBorders"]
        assert clear["updateBorders"]["left"] == {"style": "NONE"}

    def test_only_the_ten_game_rows_are_bracketed(self):
        _, bracket = _score_bracket_requests(7, 4, 2)
        area = bracket["updateBorders"]["range"]
        # rows 4-13; the 近十場 / 近五場 averages at 14-15 stay open
        assert area["startRowIndex"] == 3
        assert area["endRowIndex"] == 13

    def test_the_clear_covers_the_whole_block_not_just_the_pair(self):
        clear, _ = _score_bracket_requests(7, 17, 17)
        area = clear["updateBorders"]["range"]
        assert (area["startColumnIndex"], area["endColumnIndex"]) == (16, 30)


def _record_row(date_text, game_id, away, home):
    """Minimal 紀錄 raw row: only the columns _record_to_team_games reads."""
    row = [""] * 41
    row[0], row[1], row[2], row[17] = date_text, game_id, away, home
    return row


class TestRecordTailRead:
    """紀錄 is read from the bottom, not in full.

    Reading A2:AO over all 22k rows made the Sheets backend time out and answer
    503 after ~185s a time; the retry loop turned one read into 605s in CI while
    the same call took 4s locally. Only the last ten games per team are ever
    used, so the read is windowed to the end of the sheet.
    """

    class _Worksheet:
        def __init__(self, row_count, rows_by_range=None):
            self.row_count = row_count
            self.ranges = []
            self._rows_by_range = rows_by_range or {}

        def get(self, range_name, **_):
            self.ranges.append(range_name)
            return self._rows_by_range.get(range_name, [])

    def _patch(self, monkeypatch, worksheet):
        monkeypatch.setattr(
            m._sheets_client, "worksheet", lambda *a, **k: worksheet, raising=False
        )

    def test_reads_only_the_tail_of_the_sheet(self, monkeypatch):
        worksheet = self._Worksheet(22322)
        self._patch(monkeypatch, worksheet)

        m._read_team_games()

        assert worksheet.ranges == [f"A{22322 - m.RECORD_TAIL_ROWS + 1}:AO22322"]

    def test_short_sheets_still_start_below_the_header(self, monkeypatch):
        worksheet = self._Worksheet(50)
        self._patch(monkeypatch, worksheet)

        m._read_team_games()

        assert worksheet.ranges == ["A2:AO50"]

    def test_widens_the_window_when_a_team_is_short_of_ten_games(self, monkeypatch):
        tail = f"A{22322 - m.RECORD_TAIL_ROWS + 1}:AO22322"
        wider = f"A{22322 - m.RECORD_TAIL_ROWS * 4 + 1}:AO22322"
        worksheet = self._Worksheet(
            22322,
            {
                tail: [_record_row("2026/08/17", "1", "NYY", "BOS")],
                wider: [
                    _record_row(f"2026/08/{day:02d}", str(day), "NYY", "BOS")
                    for day in range(1, 12)
                ],
            },
        )
        self._patch(monkeypatch, worksheet)

        games = m._read_team_games()

        assert worksheet.ranges == [tail, wider]
        assert len(games["NYY"]) == m.GAMES_COUNT
