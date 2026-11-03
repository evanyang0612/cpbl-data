from baseball.npb_record_sync import (
    layout_actions,
    plan_rows,
    row_year,
    tab_year,
    view_formula,
    year_tab,
)


def _row(date, away="阪神", home="巨人", runs=1):
    row = [""] * 83
    row[1], row[8], row[9], row[11] = date, away, runs, home
    return row


class TestYearTabs:
    def test_names_round_trip(self):
        assert year_tab(2027) == "2027・紀錄總表"
        assert tab_year("2027・紀錄總表") == 2027

    def test_other_tabs_are_not_year_tabs(self):
        assert tab_year("紀錄總表") is None
        assert tab_year("2026・野球日記") is None

    def test_row_year_reads_the_date_column(self):
        assert row_year(_row("2027/3/26")) == 2027
        assert row_year(_row("")) is None
        assert row_year(["", "日  期"]) is None


class TestViewFormula:
    def test_picks_the_year_in_b1_out_of_every_year_tab(self):
        assert view_formula([2027, 2026]) == (
            "=ARRAYFORMULA(CHOOSE(MATCH(VALUE($B$1),{2026,2027},0),"
            "'2026・紀錄總表'!A4:CE993,'2027・紀錄總表'!A4:CE993))"
        )


class TestLayoutActions:
    def test_first_run_turns_the_current_sheet_into_its_years_tab(self):
        # 紀錄總表 holds 2026 and nothing has a year tab yet: it is copied as it
        # stands, so any hand edit in it survives.
        assert layout_actions(view_years={2026}, tab_years=set(),
                              source_years={2026}) == [("adopt", 2026, None)]

    def test_a_new_season_starts_from_last_seasons_tab_emptied(self):
        assert layout_actions(view_years=set(), tab_years={2026},
                              source_years={2026, 2027}) == [("new", 2027, 2026)]

    def test_both_at_once(self):
        assert layout_actions(view_years={2026}, tab_years=set(),
                              source_years={2026, 2027}) == [
            ("adopt", 2026, None), ("new", 2027, 2026)]

    def test_nothing_to_do_once_every_year_has_a_tab(self):
        assert layout_actions(view_years=set(), tab_years={2026, 2027},
                              source_years={2026, 2027}) == []

    def test_an_empty_sheet_is_adopted_as_the_earliest_source_year(self):
        assert layout_actions(view_years=set(), tab_years=set(),
                              source_years={2027}) == [("adopt", 2027, None)]

    def test_a_sheet_mixing_seasons_is_refused(self):
        try:
            layout_actions(view_years={2025, 2026}, tab_years=set(), source_years={2026})
        except RuntimeError as e:
            assert "2025" in str(e)
        else:
            raise AssertionError("expected a RuntimeError")


class TestPlanRows:
    def test_appends_after_the_last_filled_row_and_corrects_changed_ones(self):
        target = [_row("2026/3/27"), _row("2026/3/28", runs=5), [3]]
        source = [_row("2026/3/27"), _row("2026/3/28", runs=6),
                  _row("2026/3/29")]

        appends, corrections = plan_rows(source, target)

        assert corrections == [(5, source[1])]
        assert appends == [(6, source[2])]

    def test_an_empty_tab_fills_from_row_four(self):
        appends, corrections = plan_rows([_row("2027/3/26")], [[1], [2]])

        assert appends == [(4, _row("2027/3/26"))]
        assert corrections == []

    def test_a_season_that_outgrows_the_tab_is_refused(self):
        target = [_row(f"2026/4/{d}") for d in range(1, 31)] * 33  # 990 rows
        try:
            plan_rows([_row("2026/10/30")], target)
        except RuntimeError as e:
            assert "993" in str(e)
        else:
            raise AssertionError("expected a RuntimeError")
