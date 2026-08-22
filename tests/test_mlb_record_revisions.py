import unittest

from migration.update_mlb_record import RAW_COLUMN_COUNT, revised_cells

# A:AO as _row_from_game builds it. Only the shape matters here.
HITS_AWAY_0IDX = 15
ERRORS_AWAY_0IDX = 16
EARNED_HOME_0IDX = 40


def _row(overrides=None):
    row = ["2026/6/23", 822718, "NYY", "Max Fried", "L"] + [0] * 9 + [4, 8, 1]
    row += ["BOS"] + [0] * 9 + [3, 7, 0, "Sonny Gray", "R", "Fenway Park", 3]
    row += [6.0, 5.1, "Umpire", "ALE", "ALE", 1, 3]
    assert len(row) == RAW_COLUMN_COUNT, len(row)
    for index, value in (overrides or {}).items():
        row[index] = value
    return row


class RevisedCellsTest(unittest.TestCase):
    """紀錄 keeps what was true on the night; the API carries the correction.

    Games already written are never revisited, so an official scorer's later
    ruling on a hit, an error or an earned run never reaches the sheet. Over
    sixty days that left 94 stale values across 756 games -- all of them in
    exactly those fields, and none anywhere else.
    """

    def test_an_unchanged_game_needs_no_write(self):
        self.assertEqual(revised_cells(_row(), _row()), {})

    def test_a_revised_hit_count_is_reported(self):
        # 822718: 紀錄 kept 9, the official scorer settled on 8.
        sheet = _row({HITS_AWAY_0IDX: 9})
        self.assertEqual(revised_cells(_row(), sheet), {HITS_AWAY_0IDX: 8})

    def test_several_revisions_in_one_game(self):
        sheet = _row({HITS_AWAY_0IDX: 9, ERRORS_AWAY_0IDX: 0, EARNED_HOME_0IDX: 5})
        self.assertEqual(
            revised_cells(_row(), sheet),
            {HITS_AWAY_0IDX: 8, ERRORS_AWAY_0IDX: 1, EARNED_HOME_0IDX: 3},
        )

    def test_a_number_read_back_as_text_is_the_same_number(self):
        sheet = _row({HITS_AWAY_0IDX: "8"})
        api = _row({HITS_AWAY_0IDX: 8})
        self.assertEqual(revised_cells(api, sheet), {})

    def test_a_date_read_back_as_a_serial_is_the_same_date(self):
        # Sheets hands back 2026/6/23 as 46196 under UNFORMATTED_VALUE.
        self.assertEqual(revised_cells(_row(), _row({0: 46196})), {})

    def test_a_genuinely_different_date_is_reported(self):
        self.assertEqual(revised_cells(_row(), _row({0: 46195}))[0], "2026/6/23")

    def test_a_row_the_api_trimmed_is_padded_before_comparing(self):
        sheet = _row()
        sheet[EARNED_HOME_0IDX] = ""
        api = _row({EARNED_HOME_0IDX: 3})
        self.assertEqual(revised_cells(api, sheet), {EARNED_HOME_0IDX: 3})

    def test_a_short_sheet_row_is_padded(self):
        self.assertEqual(revised_cells(_row(), _row()[:20])[EARNED_HOME_0IDX], 3)

    def test_a_blank_from_the_api_never_clobbers_a_filled_cell(self):
        api = _row({EARNED_HOME_0IDX: ""})
        self.assertEqual(revised_cells(api, _row()), {})

    def test_a_renamed_pitcher_is_reported(self):
        api = _row({3: "Zac Thornton"})
        self.assertEqual(revised_cells(api, _row({3: "Zach Thornton"})), {3: "Zac Thornton"})


if __name__ == "__main__":
    unittest.main()
