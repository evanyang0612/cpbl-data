import unittest

from migration.unbind_record_named_ranges import rewrite_formula

COLUMNS = {
    "客先局": "AY",
    "客自責": "AN",
    "客聯區": "AL",
    "主聯區": "AM",
    "客隊總分": "O",
    "主隊總分": "AB",
    "客五總": "AP",
    "主五總": "AQ",
    "五總分": "AR",
}


class RewriteFormulaTest(unittest.TestCase):
    def test_whole_column_name_becomes_a_reference_to_the_same_row(self):
        self.assertEqual(
            rewrite_formula("=IF(客先局>=5,1,0)", 900, COLUMNS),
            "=IF(AY900>=5,1,0)",
        )

    def test_a_name_inside_a_string_literal_is_left_alone(self):
        self.assertEqual(
            rewrite_formula('=IF(客先局="客先局","客五總",客自責)', 7, COLUMNS),
            '=IF(AY7="客先局","客五總",AN7)',
        )

    def test_only_whole_tokens_match_so_shorter_names_do_not_eat_longer_ones(self):
        # 五總分 contains 五總; 客五總 ends with it. Neither may be partially hit.
        self.assertEqual(
            rewrite_formula("=客五總+主五總+五總分", 12, COLUMNS),
            "=AP12+AQ12+AR12",
        )

    def test_formulas_without_named_ranges_are_untouched(self):
        formula = '=IF(MOD(AI22372,1)=0,AI22372,IF(RIGHT(AI22372,1)="1",0,1))'
        self.assertEqual(rewrite_formula(formula, 22372, COLUMNS), formula)

    def test_literal_cells_pass_straight_through(self):
        self.assertEqual(rewrite_formula("QS", 5, COLUMNS), "QS")
        self.assertEqual(rewrite_formula("", 5, COLUMNS), "")

    def test_function_names_and_a1_references_are_not_mistaken_for_names(self):
        self.assertEqual(
            rewrite_formula('=IF(AL500="","",LEFTB(客聯區,2))', 500, COLUMNS),
            '=IF(AL500="","",LEFTB(AL500,2))',
        )

    def test_the_real_qs_formula(self):
        self.assertEqual(
            rewrite_formula(
                '=IF(AI22372="","",IF(AND(客先局>=5,客自責<=2),"QS",'
                'IF(AND(客先局>=6,客自責<=3),"QS","x")))',
                22372,
                COLUMNS,
            ),
            '=IF(AI22372="","",IF(AND(AY22372>=5,AN22372<=2),"QS",'
            'IF(AND(AY22372>=6,AN22372<=3),"QS","x")))',
        )

    def test_the_real_win_loss_formula(self):
        self.assertEqual(
            rewrite_formula(
                '=IF(客隊總分="","",IF(客五總=主五總,"平",IF(客五總>主五總,"勝","敗")))',
                800,
                COLUMNS,
            ),
            '=IF(O800="","",IF(AP800=AQ800,"平",IF(AP800>AQ800,"勝","敗")))',
        )


if __name__ == "__main__":
    unittest.main()
