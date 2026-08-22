import unittest

from migration.update_mlb_record import placeholder_formulas_ready


class PlaceholderFormulasReadyTest(unittest.TestCase):
    """紀錄 keeps pre-built rows whose AP:BD formulas are already in place.

    Laying formulas over rows that already carry them costs a whole extra
    recalculation of the workbook -- measured at roughly eight minutes -- so
    the daily run has to be able to tell that it has nothing to lay.
    """

    def test_a_formula_on_every_target_row_is_ready(self):
        cells = [["=SUM(F10:J10)"], ["=SUM(F11:J11)"], ["=SUM(F12:J12)"]]
        self.assertTrue(placeholder_formulas_ready(cells, 10, 12))

    def test_a_gap_in_the_middle_is_not_ready(self):
        cells = [["=SUM(F10:J10)"], [""], ["=SUM(F12:J12)"]]
        self.assertFalse(placeholder_formulas_ready(cells, 10, 12))

    def test_trailing_rows_the_api_trimmed_are_not_ready(self):
        # values_batch_get drops trailing empty rows, so a short reply means
        # the last placeholders are missing.
        cells = [["=SUM(F10:J10)"]]
        self.assertFalse(placeholder_formulas_ready(cells, 10, 12))

    def test_a_static_value_is_not_a_placeholder(self):
        cells = [["=SUM(F10:J10)"], [7], ["=SUM(F12:J12)"]]
        self.assertFalse(placeholder_formulas_ready(cells, 10, 12))

    def test_an_empty_reply_is_not_ready(self):
        self.assertFalse(placeholder_formulas_ready([], 10, 12))

    def test_extra_rows_beyond_the_target_do_not_matter(self):
        cells = [["=a"], ["=b"], ["=c"], ["=d"]]
        self.assertTrue(placeholder_formulas_ready(cells, 10, 12))

    def test_a_single_row_target(self):
        self.assertTrue(placeholder_formulas_ready([["=a"]], 5, 5))
        self.assertFalse(placeholder_formulas_ready([[""]], 5, 5))


if __name__ == "__main__":
    unittest.main()
