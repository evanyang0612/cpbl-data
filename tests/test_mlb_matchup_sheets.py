"""Tests for the MLB 設定 / 對戰 (n) sheet builder.

The layout is a straight port of the NPB spreadsheet's 設定 + 対戦 (n) tabs, so the
assertions below pin the geometry and the formula text against the originals (with
NPB's named ranges swapped for MLB's and SUMPRODUCT swapped for COUNTIFS/SUMIFS).
"""

import migration.add_mlb_matchup_sheets as m


class TestGeometry:
    def test_col_letter(self):
        assert m.col_letter(0) == "A"
        assert m.col_letter(1) == "B"
        assert m.col_letter(25) == "Z"
        assert m.col_letter(26) == "AA"
        assert m.col_letter(57) == "BF"

    def test_matchup_titles(self):
        assert m.matchup_title(1) == "對戰 (1)"
        assert m.matchup_title(15) == "對戰 (15)"
        assert len(m.MATCHUP_TITLES) == 15

    def test_setting_blocks_stride_seven_columns_from_b(self):
        assert m.setting_block_col(0) == 1  # B
        assert m.setting_block_col(1) == 8  # I
        assert m.setting_block_col(14) == 99
        assert m.SETTING_BANDS == 3

    def test_setting_band_rows_match_npb(self):
        assert [m.setting_band_header_row(b) for b in range(3)] == [3, 10, 17]
        assert m.setting_team_cell(0, 0, 0) == "B4"
        assert m.setting_pitcher_cell(0, 0, 0) == "C4"
        assert m.setting_team_cell(0, 0, 1) == "B6"
        assert m.setting_pitcher_cell(0, 0, 1) == "C6"
        assert m.setting_team_cell(0, 1, 0) == "B11"
        assert m.setting_pitcher_cell(0, 2, 1) == "C20"
        # second matchup block sits at column I / J
        assert m.setting_team_cell(1, 0, 0) == "I4"
        assert m.setting_pitcher_cell(1, 0, 0) == "J4"

    def test_setting_helper_rows_match_npb(self):
        assert m.setting_helper_rows(0, 0) == (43, 44, 45, 46)
        assert m.setting_helper_rows(0, 1) == (48, 49, 50, 51)
        assert m.setting_helper_rows(1, 0) == (53, 54, 55, 56)
        assert m.setting_helper_rows(1, 1) == (58, 59, 60, 61)
        assert m.setting_helper_rows(2, 0) == (63, 64, 65, 66)
        assert m.setting_helper_rows(2, 1) == (68, 69, 70, 71)
        assert m.SETTING_YEAR_ROWS == (39, 40, 41)

    def test_matchup_blocks_match_npb_columns(self):
        # three games across, two starters each: B/K, V/AE, AP/AY
        assert [m.matchup_block_col(g, s) for g in range(3) for s in range(2)] == [
            1, 10, 21, 30, 41, 50
        ]

    def test_matchup_window_cells_match_npb(self):
        assert m.matchup_window_cells(0) == ("$G$1", "$K$1")
        assert m.matchup_window_cells(1) == ("$AA$1", "$AE$1")
        assert m.matchup_window_cells(2) == ("$AU$1", "$AY$1")

    def test_vs_group_rows(self):
        assert m.vs_group_row(0) == 9  # the actual opponent block
        assert m.vs_group_row(1) == 15
        assert m.vs_group_row(15) == 85
        assert m.MATCHUP_LAST_ROW == 88
        assert m.LEAGUE_SIZE == 15


class TestTeams:
    def test_league_lists_match_the_workbook(self):
        assert m.AL_TEAMS[:5] == ["NYY", "TB", "BOS", "TOR", "BAL"]
        assert m.NL_TEAMS[:5] == ["ATL", "WSH", "PHI", "NYM", "MIA"]
        assert len(m.AL_TEAMS) == len(m.NL_TEAMS) == 15

    def test_every_team_has_a_colour(self):
        assert set(m.TEAM_PRIMARY_HEX) == set(m.AL_TEAMS + m.NL_TEAMS)

    def test_text_colour_contrasts_with_background(self):
        assert m.contrast_text_hex("FFFFFF") == "000000"
        assert m.contrast_text_hex("0C2340") == "FFFFFF"


class TestOverallFormulas:
    def setup_method(self):
        self.rows = m.overall_rows(block_col=1, window=("$G$1", "$K$1"))

    def test_away_row_counts_and_rates(self):
        assert self.rows["C5"] == (
            '=COUNTIFS(客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1)'
        )
        assert self.rows["D5"] == (
            '=COUNTIFS(客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1,客QS,"QS")'
        )
        assert self.rows["E5"] == "=IFERROR(D5/C5,0)"
        assert self.rows["F5"] == (
            '=IFERROR(SUMIFS(客自責,客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1)*9'
            '/SUMIFS(客先局,客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1),0)'
        )
        assert self.rows["G5"] == (
            '=IFERROR(SUMIFS(客先局,客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1)/C5,0)'
        )
        # 5 / 9+ are what the opposing line-up scored, so they flip sides
        assert self.rows["H5"] == (
            '=IFERROR(SUMIFS(主五總,客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1)/C5,0)'
        )
        assert self.rows["I5"] == (
            '=IFERROR(SUMIFS(主隊總分,客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1)/C5,0)'
        )

    def test_home_row_uses_home_named_ranges(self):
        assert self.rows["C6"] == (
            '=COUNTIFS(主隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1)'
        )
        assert self.rows["D6"].endswith('主QS,"QS")')
        assert "客五總" in self.rows["H6"]
        assert "客隊總分" in self.rows["I6"]

    def test_total_row_is_derived_from_the_two_rows_above(self):
        assert self.rows["C7"] == "=C5+C6"
        assert self.rows["D7"] == "=D5+D6"
        assert self.rows["E7"] == "=IFERROR(D7/C7,0)"
        assert self.rows["F7"] == "=IFERROR((F5*G5*C5+F6*G6*C6)/(G5*C5+G6*C6),0)"
        assert self.rows["G7"] == "=IFERROR((G5*C5+G6*C6)/C7,0)"
        assert self.rows["H7"] == "=IFERROR((H5*C5+H6*C6)/C7,0)"
        assert self.rows["I7"] == "=IFERROR((I5*C5+I6*C6)/C7,0)"

    def test_second_block_shifts_columns_and_window(self):
        rows = m.overall_rows(block_col=10, window=("$G$1", "$K$1"))
        assert rows["L5"].startswith('=COUNTIFS(客隊先發,$P$2,')
        assert rows["N5"] == "=IFERROR(M5/L5,0)"


class TestVersusFormulas:
    def setup_method(self):
        self.rows = m.versus_rows(block_col=1, window=("$G$1", "$K$1"), header_row=15)

    def test_opponent_criterion_is_a_single_term(self):
        # 紀錄 is normalised (baseball/mlb_teams.py), so no alias term is emitted
        assert self.rows["C16"] == (
            '=COUNTIFS(客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1,主隊隊伍,$C$15)'
        )
        assert self.rows["C17"] == (
            '=COUNTIFS(主隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1,客隊隊伍,$C$15)'
        )
        assert "IF($C$15=" not in self.rows["C16"]

    def test_era_divides_earned_runs_by_starter_innings(self):
        assert self.rows["F16"] == (
            '=IFERROR(SUMIFS(客自責,客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1,'
            '主隊隊伍,$C$15)*9'
            '/SUMIFS(客先局,客隊先發,$G$2,日期,">="&$G$1,日期,"<"&$K$1,主隊隊伍,$C$15),0)'
        )

    def test_total_row_is_derived(self):
        assert self.rows["C18"] == "=C16+C17"
        assert self.rows["F18"] == "=IFERROR((F16*G16*C16+F17*G17*C17)/(G16*C16+G17*C17),0)"

    def test_opponent_block_reads_the_partner_block_team(self):
        # 對戰 row 9 always faces the other starter's team
        assert m.opponent_team_formula(block_col=1) == "=K3"
        assert m.opponent_team_formula(block_col=10) == "=B3"
        assert m.opponent_team_formula(block_col=21) == "=AE3"

    def test_league_team_cells_follow_the_blocks_own_league(self):
        assert m.vs_team_formula(block_col=1, index=1) == (
            '=IFERROR(INDEX(IF(COUNTIF(AL,$B$3)>0,AL,NL),1),"")'
        )
        assert m.vs_team_formula(block_col=1, index=15).endswith(",15),\"\")")


class TestSettingFormulas:
    def test_year_helper_rows(self):
        cells = m.setting_year_formulas(block=0)
        assert cells["E39"] == "=YEAR(TODAY())-2002"
        assert cells["G39"] == "=YEAR(TODAY())-2000"
        assert cells["E40"] == "=DATE(YEAR(TODAY())-2,3,1)"
        assert cells["G41"] == "=DATE(YEAR(TODAY()),12,31)"
        # second block keeps its own copy in its own columns
        assert "L39" in m.setting_year_formulas(block=1)

    def test_versus_helper_pairs_pitcher_against_the_other_team(self):
        cells = m.setting_helper_formulas(block=0)
        assert cells["E43"] == (
            '=(SUMIFS(客自責,日期,">="&E40,日期,"<"&E41,主隊隊伍,$B$6,客隊先發,$C$4)'
            '+SUMIFS(主自責,日期,">="&E40,日期,"<"&E41,客隊隊伍,$B$6,主隊先發,$C$4))*9'
        )
        assert cells["E44"] == (
            '=SUMIFS(客先局,日期,">="&E40,日期,"<"&E41,主隊隊伍,$B$6,客隊先發,$C$4)'
            '+SUMIFS(主先局,日期,">="&E40,日期,"<"&E41,客隊隊伍,$B$6,主隊先發,$C$4)'
        )

    def test_overall_helper_rows_drop_the_opponent_criterion(self):
        cells = m.setting_helper_formulas(block=0)
        assert cells["E45"] == (
            '=(SUMIFS(客自責,日期,">="&E40,日期,"<"&E41,客隊先發,$C$4)'
            '+SUMIFS(主自責,日期,">="&E40,日期,"<"&E41,主隊先發,$C$4))*9'
        )
        assert "主隊隊伍" not in cells["E46"]

    def test_second_pitcher_helper_faces_the_first_teams_side(self):
        cells = m.setting_helper_formulas(block=0)
        assert "$B$4" in cells["E48"] and "$C$6" in cells["E48"]

    def test_display_cells_reference_their_helper_rows(self):
        cells = m.setting_display_formulas(block=0)
        assert cells["E4"] == '=IF(AND(E43=0,E44=0),"",E43/E44)'
        assert cells["E5"] == '=IF(AND(E45=0,E46=0),"",E45/E46)'
        assert cells["G7"] == '=IF(AND(G50=0,G51=0),"",G50/G51)'
        assert cells["E11"] == '=IF(AND(E53=0,E54=0),"",E53/E54)'
        assert cells["G21"] == '=IF(AND(G70=0,G71=0),"",G70/G71)'

    def test_later_games_inherit_the_ballpark_and_both_teams(self):
        cells = m.setting_mirror_formulas(block=0)
        assert cells == {
            "B10": "=B3", "B11": "=B4", "B13": "=B6",      # game 2 follows game 1
            "B17": "=B3", "B18": "=B11", "B20": "=B13",    # game 3 follows game 2
        }
        # nothing in the 先發 column is written over
        assert not any(a1.startswith("C") for a1 in cells)
        # second block mirrors within its own columns
        assert m.setting_mirror_formulas(block=1)["I11"] == "=I4"

    def test_each_band_is_boxed_with_two_row_team_cells(self):
        requests = m.setting_frame_requests(sheet_id=0)
        kinds = [next(iter(r)) for r in requests]
        blocks, bands = m.MATCHUP_COUNT, m.SETTING_BANDS
        assert kinds.count("updateBorders") == blocks * bands       # one box per band
        assert kinds.count("mergeCells") == blocks * bands * 4     # 隊伍 + 先發, ×2 sides
        # merges must be requested before the box, or the box's bottom edge is lost
        assert kinds.index("mergeCells") < kinds.index("updateBorders")
        first_merge = next(r for r in requests if "mergeCells" in r)["mergeCells"]
        assert first_merge["range"]["startRowIndex"] == 3   # B4:B5
        assert first_merge["range"]["endRowIndex"] == 5
        assert first_merge["mergeType"] == "MERGE_ALL"

    def test_pitcher_dropdown_helper_columns_are_unique_and_indirect(self):
        cols = [
            m.setting_helper_column(block, band, side)
            for band in range(3)
            for block in range(15)
            for side in range(2)
        ]
        assert len(cols) == len(set(cols)) == 90
        assert min(cols) > m.setting_block_col(14) + 6  # clear of the display area
        cells = m.setting_helper_column_formulas(block=0, band=0, side=0)
        first = m.col_letter(m.setting_helper_column(0, 0, 0))
        assert cells[f"{first}1"] == "=$B$4"
        assert cells[f"{first}2"] == f"=INDIRECT({first}1)"


class TestMatchupSheetReferences:
    def test_row_three_pulls_team_and_pitcher_from_its_setting_block(self):
        cells = m.matchup_header_formulas(matchup=1)
        assert cells["B3"] == "='設定'!B4"
        assert cells["K3"] == "='設定'!B6"
        assert cells["V3"] == "='設定'!B11"
        assert cells["AY3"] == "='設定'!B20"
        # the name itself lands in the hidden key row; row 3 only displays it
        assert cells["G2"] == "='設定'!C4"
        assert cells["P2"] == "='設定'!C6"
        assert cells["BD2"] == "='設定'!C20"
        assert cells["G3"] == m.name_display_formula("G2")

    def test_the_displayed_name_always_breaks_after_the_first_name(self):
        # one formula, so every block reads as two lines regardless of name length
        assert m.name_display_formula("G2") == (
            '=REGEXREPLACE(G2,"^([^ ]+) ","$1"&CHAR(10))'
        )

    def test_second_matchup_reads_the_second_setting_block(self):
        cells = m.matchup_header_formulas(matchup=2)
        assert cells["B3"] == "='設定'!I4"
        assert cells["G2"] == "='設定'!J4"


class TestConditionalFormats:
    def test_numeric_thresholds_match_npb(self):
        assert m.NUMERIC_RULES == (
            ("era", "<=3.5", m.GREEN),
            ("era", ">=4", m.RED),
            ("five", "<2", m.GREEN),
            ("five", ">=2.5", m.RED),
            ("nine", "<4", m.GREEN),
            ("nine", ">=4.5", m.RED),
        )

    def test_numeric_rule_guards_against_blank_cells(self):
        # one rule per threshold covers all six blocks; the relative ref shifts
        assert m.numeric_rule_formula("era", "<=3.5") == "=AND(ISNUMBER(F5), F5<=3.5)"
        assert m.numeric_rule_formula("five", ">=2.5") == "=AND(ISNUMBER(H5), H5>=2.5)"
        assert m.numeric_rule_formula("nine", "<4") == "=AND(ISNUMBER(I5), I5<4)"

    def test_quality_start_rate_colours_starts_qs_and_rate_like_npb(self):
        green, red = m.qs_rate_rules(block_col=1)
        # thresholds are the ones the on-sheet legend states
        assert green["formula"] == "=AND(ISNUMBER($E5), $E5>66%)"
        assert red["formula"] == "=AND(ISNUMBER($E5), $E5<=40%)"
        assert green["columns"] == (2, 5)  # 場次 / QS / QS%, i.e. C:E
        # the fourth block anchors on its own QS% column
        assert m.qs_rate_rules(block_col=30)[0]["formula"] == (
            "=AND(ISNUMBER($AH5), $AH5>66%)"
        )
        assert m.qs_rate_rules(block_col=30)[0]["columns"] == (31, 34)  # AF:AH

    def test_season_lists_extend_themselves_each_year(self):
        starts, ends = m.season_bound_formulas(2017)
        # SEQUENCE length YEAR(TODAY())-2015 spans 2017 .. next year: in 2026 that is
        # 2026-2015 = 11 rows, i.e. 2017..2027
        assert starts == "=ARRAYFORMULA(DATE(SEQUENCE(YEAR(TODAY())-2015,1,2017),3,1))"
        assert ends == "=ARRAYFORMULA(DATE(SEQUENCE(YEAR(TODAY())-2015,1,2017),12,1))"
        assert m.SEASON_LIST_ROWS > 2100 - m.FIRST_SEASON - 2000  # room to spill into
        assert m.WINDOW_TO_DEFAULT == "=DATE(YEAR(TODAY()),12,1)"  # a value on the list

    def test_every_window_cell_gets_a_dropdown(self):
        requests = m.date_validation_requests(sheet_id=0)
        assert len(requests) == 6  # three games × from/to
        cells = [(r["setDataValidation"]["range"]["startColumnIndex"],
                  r["setDataValidation"]["rule"]["condition"]["values"][0]["userEnteredValue"])
                 for r in requests]
        assert cells == [(6, "=開賽年度"), (10, "=閉幕年度"),    # G1 / K1
                         (26, "=開賽年度"), (30, "=閉幕年度"),   # AA1 / AE1
                         (46, "=開賽年度"), (50, "=閉幕年度")]   # AU1 / AY1
        assert all(r["setDataValidation"]["range"]["startRowIndex"] == 0
                   for r in requests)

    def test_zero_is_hidden_by_the_number_format(self):
        # third section of a number format is the zero case; empty means blank
        assert m.FORMAT_COUNT == "0;-0;;@"
        assert m.FORMAT_PERCENT == "0%;-0%;;@"
        assert m.FORMAT_NUMBER == "0.00;-0.00;;@"

    def test_rule_layer_is_one_per_threshold_plus_team_colours(self):
        requests = m.conditional_format_requests(sheet_id=0)
        assert len(requests) == 6 + 12 + 30
        # team colours are added last so they sit on top
        last = requests[-1]["addConditionalFormatRule"]["rule"]["booleanRule"]
        assert last["condition"]["type"] == "TEXT_EQ"

    def test_restyle_leaves_formulas_alone(self):
        requests = m.restyle_requests(sheet_id=0, existing_rule_count=96)
        kinds = [next(iter(r)) for r in requests]
        assert kinds.count("deleteConditionalFormatRule") == 96
        assert kinds.count("addConditionalFormatRule") == 48
        assert kinds.count("setDataValidation") == 6  # the date dropdowns
        # number formats per column, plus the six wrapped starter-name cells
        assert kinds.count("repeatCell") == 6 * 7 + 18
        assert kinds.count("updateDimensionProperties") == 2  # name + key rows
        assert not any(k in ("updateCells", "mergeCells") for k in kinds)
        # every repeatCell is presentation only — none of them writes a value
        assert all("userEnteredValue" not in r["repeatCell"]["cell"]
                   for r in requests if "repeatCell" in r)
        assert sum(r["repeatCell"]["fields"] == "userEnteredFormat.numberFormat"
                   for r in requests if "repeatCell" in r) == 6 * 7


class TestColourLegend:
    def test_legend_states_the_thresholds_the_rules_use(self):
        red = "".join(part for part, _ in m.LEGEND_RED)
        green = "".join(part for part, _ in m.LEGEND_GREEN)
        # the legend is the spec: every number in it must appear in a rule
        assert "40%" in red and m.QS_RATE_RED == "<=40%"
        assert "66%" in green and m.QS_RATE_GREEN == ">66%"
        for metric, test, _ in m.NUMERIC_RULES:
            threshold = test.lstrip("<=>")
            assert threshold in (red if ">" in test else green), (metric, test)

    def test_numbers_are_coloured_run_by_run(self):
        cell = m.rich_text_cell(m.LEGEND_RED)
        text = cell["userEnteredValue"]["stringValue"]
        assert text.startswith("先發QS率低於(含) 40%")
        assert text.endswith("數字顯示為 紅色")
        runs = cell["textFormatRuns"]
        assert [r["startIndex"] for r in runs] == sorted(r["startIndex"] for r in runs)
        coloured = [text[r["startIndex"]:] for r in runs
                    if r["format"]["foregroundColor"] == m._rgb(m.RED)]
        assert coloured[0].startswith("40%")

    def test_legend_sits_below_the_blocks_and_is_boxed(self):
        requests = m.setting_legend_requests(sheet_id=0)
        kinds = [next(iter(r)) for r in requests]
        assert kinds.count("updateCells") == 3      # title + red + green
        assert kinds.count("updateBorders") == 1
        assert kinds.count("updateDimensionProperties") == 2  # the two row heights
        rows = {r["updateCells"]["start"]["rowIndex"] + 1
                for r in requests if "updateCells" in r}
        assert rows == set(m.LEGEND_ROWS)
        assert max(rows) < m.SETTING_YEAR_ROWS[0]   # clear of the helper rows


class TestVenueAndDisplay:
    def test_ballpark_follows_the_home_team(self):
        # 設定 row 3 is a label only, so it can safely be derived from the 主隊 cell
        assert m.setting_venue_formula(block=0) == (
            '=IFERROR(VLOOKUP(B6,資料!$A$2:$B$31,2,FALSE),"")'
        )
        assert m.setting_venue_formula(block=1) == (
            '=IFERROR(VLOOKUP(I6,資料!$A$2:$B$31,2,FALSE),"")'
        )

    def test_current_home_park_is_the_one_a_team_uses_most(self):
        pairs = [("ATL", "SunTrust Park"), ("ATL", "Truist Park"),
                 ("ATL", "Truist Park"), ("NYY", "Yankee Stadium"),
                 ("NYY", "London Stadium")]
        parks = m.current_home_parks(pairs)
        assert parks["ATL"] == "Truist Park"      # the rename wins on volume
        assert parks["NYY"] == "Yankee Stadium"   # a neutral-site one-off does not

    def test_sponsor_renames_can_be_pinned(self):
        pairs = [("LAD", "UNIQLO Field at Dodger Stadium")] * 5
        assert m.current_home_parks(pairs)["LAD"] == "Dodger Stadium"
        assert m.HOME_PARK_OVERRIDES["LAD"] == "Dodger Stadium"

    def test_matchup_starter_names_wrap_too(self):
        requests = m.matchup_wrap_requests(sheet_id=0)
        rows = [r["updateDimensionProperties"] for r in requests
                if "updateDimensionProperties" in r]
        assert len(rows) == 2  # the name row's height, and hiding the key row
        assert rows[0]["properties"]["pixelSize"] == 38
        # the key row keeps NPB's blank gap: visible and 11px, but its cells render
        # nothing because of the ";;;" format
        assert rows[1]["properties"] == {"hiddenByUser": False, "pixelSize": 11}
        assert rows[1]["range"]["startIndex"] == m.MATCHUP_KEY_ROW - 1
        cells = [r for r in requests if "repeatCell" in r]
        key = cells[2]["repeatCell"]["cell"]["userEnteredFormat"]
        assert key["numberFormat"] == {"type": "TEXT", "pattern": ";;;"}
        assert m.MATCHUP_NAME_FONT_SIZE == 11  # back to a readable size
        assert cells[1]["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"][
            "fontSize"] == 11
        assert len(cells) == 18  # per block: team, name, hidden key
        # the key row is white on white, so the raw name never shows twice
        key_range = cells[2]["repeatCell"]["range"]
        assert key_range["startRowIndex"] == 1 and key_range["endRowIndex"] == 2
        # everything on the row centres vertically, so one- and two-line names line up
        assert all(c["repeatCell"]["cell"]["userEnteredFormat"]["verticalAlignment"]
                   == "MIDDLE" for c in cells)
        name_cell = cells[1]["repeatCell"]   # team cell first, then the name
        assert name_cell["range"]["startRowIndex"] == 2
        assert name_cell["range"]["endRowIndex"] == 3
        assert name_cell["range"]["startColumnIndex"] == m.matchup_block_col(0, 0) + 5
        assert name_cell["cell"]["userEnteredFormat"]["wrapStrategy"] == "WRAP"

    def test_starter_names_wrap_instead_of_being_clipped(self):
        # "Simeon Woods Richardson" cannot fit an 86px cell on one line, and the cell
        # is merged over two 24px rows, so wrapping is free
        fmt = m.cell_format(size=10, wrap=True)
        assert fmt["wrapStrategy"] == "WRAP"
        assert fmt["verticalAlignment"] == "MIDDLE"
        assert "verticalAlignment" in m.FORMAT_FIELDS
        assert "wrapStrategy" in m.FORMAT_FIELDS
        requests = m.setting_wrap_requests(sheet_id=0)
        assert len(requests) == m.MATCHUP_COUNT * m.SETTING_BANDS * 2
        first = requests[0]["repeatCell"]
        assert first["range"]["startColumnIndex"] == m.setting_block_col(0) + 1
        assert first["cell"]["userEnteredFormat"]["wrapStrategy"] == "WRAP"
