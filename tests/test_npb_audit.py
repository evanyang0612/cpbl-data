"""
Unit tests for the NPB historical audit comparator.
Covers: cell normalization, row diffing against sheet values, game-id window
        selection from 賽錄, and the 資料更新 paste payload.
"""

from baseball.npb_audit import (
    SCORE_SENSITIVE_ANALYSIS_COLUMNS,
    cells_equal,
    coverage_shortfall,
    is_blanked_scrape,
    telegram_incomplete,
    telegram_summary,
    diff_row,
    has_score_diff,
    sailu_game_ids_in_window,
    update_sheet_values,
)

# --- Cell normalization ---------------------------------------------------


def test_numeric_cells_compare_by_value_not_text():
    assert cells_equal(0, "0")
    assert cells_equal(3.0, "3")
    assert cells_equal("4.50", 4.5)
    assert not cells_equal(1, "2")


def test_innings_pitched_notation_compares_as_text_not_float():
    # 3.1 IP is 3⅓ innings, not 3.1. Comparing them as floats is fine for
    # equality, but "3.10" must not read as equal to "3.1" in a way that
    # hides a real change from 3.1 to 3.2.
    assert cells_equal("3.1", "3.1")
    assert not cells_equal("3.1", "3.2")


def test_blank_and_zero_are_different():
    # 分析表紀錄 uses "" for an inning that was never batted (×) and 0 for a
    # scoreless inning. Treating them as equal would hide a real correction.
    assert not cells_equal("", 0)
    assert not cells_equal(0, "")
    assert cells_equal("", "")
    assert cells_equal(None, "")


def test_surrounding_whitespace_is_ignored():
    assert cells_equal(" 巨人 ", "巨人")


# --- Row diffing ----------------------------------------------------------


def test_diff_row_reports_column_letter_and_both_values():
    sheet_row = ["1", "2026/8/4", "夜", "央盟"]
    fresh_row = ["1", "2026/8/4", "日", "央盟"]

    diffs = diff_row(sheet_row, fresh_row)

    assert diffs == [{"index": 2, "column": "C", "sheet": "夜", "fresh": "日"}]


def test_diff_row_skips_the_leading_sequence_column():
    sheet_row = ["11", "2026/8/4"]
    fresh_row = ["57", "2026/8/4"]

    assert diff_row(sheet_row, fresh_row) == []


def test_diff_row_skips_columns_outside_the_compared_range():
    sheet_row = ["1", "a", "b"]
    fresh_row = ["1", "a", "ZZ"]

    assert diff_row(sheet_row, fresh_row, last_index=1) == []


def test_diff_row_pads_a_short_sheet_row():
    sheet_row = ["1", "2026/8/4"]
    fresh_row = ["1", "2026/8/4", "夜"]

    diffs = diff_row(sheet_row, fresh_row)

    assert [d["column"] for d in diffs] == ["C"]
    assert diffs[0]["sheet"] == ""


def test_score_sensitive_diff_is_flagged():
    # Columns J/K hold the final score; the prediction ledger settles off them.
    score_index = min(SCORE_SENSITIVE_ANALYSIS_COLUMNS)
    diffs = [{"index": score_index, "column": "J", "sheet": "3", "fresh": "4"}]

    assert has_score_diff(diffs)
    assert not has_score_diff([{"index": 4, "column": "E", "sheet": "", "fresh": ""}])


# --- Window selection -----------------------------------------------------


def _sailu_sheet_row(game_id: str, date_str: str) -> list[str]:
    row = [""] * 51
    row[1] = game_id
    row[40] = date_str
    return row


def test_sailu_game_ids_in_window_is_inclusive_on_both_ends():
    rows = [
        ["header"],
        _sailu_sheet_row("1", "2026-07-16"),
        _sailu_sheet_row("2", "2026-07-17"),
        _sailu_sheet_row("3", "2026-08-15"),
        _sailu_sheet_row("4", "2026-08-16"),
    ]

    ids = sailu_game_ids_in_window(rows, "2026-07-17", "2026-08-15")

    assert ids == ["2", "3"]


def test_sailu_game_ids_in_window_dedupes_and_ignores_blank_rows():
    rows = [
        ["header"],
        _sailu_sheet_row("7", "2026-08-01"),
        _sailu_sheet_row("7", "2026-08-01"),
        _sailu_sheet_row("", "2026-08-01"),
        ["short", "row"],
    ]

    assert sailu_game_ids_in_window(rows, "2026-08-01", "2026-08-01") == ["7"]


# --- 資料更新 paste payload ------------------------------------------------


def test_update_sheet_values_drops_the_sequence_column_and_pads_to_83():
    fresh_row = [99, "2026/8/4", "夜"]

    values = update_sheet_values([fresh_row])

    assert len(values) == 1
    assert len(values[0]) == 82
    assert values[0][:2] == ["2026/8/4", "夜"]
    assert values[0][2:] == [""] * 80


def test_update_sheet_values_truncates_an_overlong_row():
    fresh_row = list(range(90))

    values = update_sheet_values([fresh_row])

    assert len(values[0]) == 82


# --- Telegram summary ------------------------------------------------------


def _finding(date, away, home, *, analysis_diffs=(), sailu_diffs=(), notes=()):
    return {
        "game_id": "2021039221",
        "date": date,
        "identity": [date.replace("-", "/"), away, home],
        "analysis": {"row": 12, "diffs": list(analysis_diffs)} if analysis_diffs else None,
        "sailu": {"target": {"row": 40, "diffs": list(sailu_diffs)}} if sailu_diffs else {},
        "fresh_analysis_row": [],
        "notes": list(notes),
    }


def _diff(index, column="C"):
    return {"index": index, "column": column, "sheet": "夜", "fresh": "日"}


def test_a_clean_window_sends_nothing():
    """A quiet week has to stay quiet, or the alert stops being read."""
    assert telegram_summary([], start="2026-08-01", end="2026-08-28", scanned=130) is None


def test_the_summary_names_every_game_that_disagrees():
    findings = [
        _finding("2026-08-12", "ヤクルト", "巨人", analysis_diffs=[_diff(2)]),
        _finding("2026-08-15", "中日", "阪神", sailu_diffs=[_diff(46, "AU"), _diff(47, "AV")]),
    ]

    message = telegram_summary(findings, start="2026-08-01", end="2026-08-28", scanned=130)

    assert "2026-08-01 → 2026-08-28" in message
    assert "130" in message and "2" in message
    assert "ヤクルト @ 巨人" in message
    assert "中日 @ 阪神" in message


def test_a_score_difference_is_flagged_as_not_auto_applicable():
    """預測紀錄 settles off those columns and its balance is cumulative, so the
    reader has to see which games must not be corrected in bulk."""
    score = next(iter(SCORE_SENSITIVE_ANALYSIS_COLUMNS))
    findings = [_finding("2026-08-12", "ヤクルト", "巨人", analysis_diffs=[_diff(score, "J")])]

    message = telegram_summary(findings, start="2026-08-01", end="2026-08-28", scanned=130)

    assert "比分" in message


def test_a_game_that_could_not_be_compared_is_still_reported():
    """A game the re-scrape never returned is not a clean game."""
    findings = [_finding("2026-08-12", "ヤクルト", "巨人", notes=["Re-scrape returned nothing."])]

    message = telegram_summary(findings, start="2026-08-01", end="2026-08-28", scanned=129)

    assert message is not None
    assert "無法比對" in message


def test_a_long_list_is_truncated_rather_than_split_by_telegram():
    findings = [_finding(f"2026-08-{day:02d}", "中日", "阪神", analysis_diffs=[_diff(2)])
                for day in range(1, 26)]

    message = telegram_summary(findings, start="2026-08-01", end="2026-08-28", scanned=130)

    assert len(message.splitlines()) < 25
    assert "還有" in message


# --- A sweep Yahoo cut short ----------------------------------------------


def test_a_fully_read_window_has_no_shortfall():
    assert coverage_shortfall(142, 142) is None
    assert coverage_shortfall(0, 0) is None


def test_a_window_read_only_in_part_is_not_a_clean_audit():
    """The 2026-08-29 sweep read 57 of 142 games — Yahoo started refusing the
    game endpoints around the 51st — and still reported "3 games disagree"."""
    shortfall = coverage_shortfall(57, 142)

    assert shortfall is not None
    assert "57" in shortfall and "142" in shortfall


def test_a_window_a_few_games_short_still_counts_as_read():
    assert coverage_shortfall(138, 142) is None


def test_cells_that_only_went_blank_are_a_throttled_scrape_not_a_correction():
    """Every diff in that run pointed the same way — the sheet holds 18:00, the
    re-scrape holds nothing. A correction changes a value; a blocked request
    loses all of them at once."""
    blanked = [
        {"index": 6, "column": "G", "sheet": "18:00", "fresh": ""},
        {"index": 8, "column": "I", "sheet": "名幸", "fresh": ""},
        {"index": 43, "column": "AR", "sheet": "右", "fresh": ""},
    ]

    assert is_blanked_scrape(blanked)


def test_a_real_correction_among_the_blanks_is_still_a_correction():
    mixed = [
        {"index": 6, "column": "G", "sheet": "18:00", "fresh": ""},
        {"index": 8, "column": "I", "sheet": "名幸", "fresh": ""},
        {"index": 21, "column": "V", "sheet": "3", "fresh": "4"},
    ]

    assert not is_blanked_scrape(mixed)


def test_one_cell_going_blank_is_not_enough_to_dismiss():
    """NPB does void a recorded value now and then; that is the audit's job."""
    assert not is_blanked_scrape([{"index": 6, "column": "G", "sheet": "18:00", "fresh": ""}])


def test_the_incomplete_message_says_what_was_missed_and_that_nothing_was_written():
    message = telegram_incomplete(scanned=57, requested=142,
                                  start="2026-07-30", end="2026-08-28")

    assert "57" in message and "142" in message
    assert "未完成" in message
