import unittest

from baseball.mlb_names import normalise, rename_map


class NormaliseTest(unittest.TestCase):
    def test_accents_are_ignored(self):
        self.assertEqual(normalise("Jesús Luzardo"), normalise("Jesus Luzardo"))
        self.assertEqual(normalise("José Berríos"), normalise("Jose Berrios"))

    def test_case_is_ignored(self):
        self.assertEqual(normalise("Pablo López"), normalise("pablo lopez"))

    def test_different_people_stay_different(self):
        self.assertNotEqual(normalise("Luis L. Ortiz"), normalise("Luis Ortiz"))
        self.assertNotEqual(normalise("Zac Thornton"), normalise("Zach Thornton"))


class RenameMapTest(unittest.TestCase):
    """MLB respells its own players; 紀錄 keeps what it published on the night.

    設定 matches a pitcher's games by name, so 'Jesus Luzardo' in 紀錄 and
    'Jesús Luzardo' in 設定 silently drop those starts from his totals. The
    dropdown behind 設定 is built from 紀錄, so the two only agree while 紀錄
    carries the spelling MLB publishes today.
    """

    def test_a_respelt_name_maps_to_the_current_spelling(self):
        renames, ambiguous = rename_map(
            seen=["Jesus Luzardo"], current=["Jesús Luzardo", "Chris Sale"]
        )
        self.assertEqual(renames, {"Jesus Luzardo": "Jesús Luzardo"})
        self.assertEqual(ambiguous, [])

    def test_an_accent_removed_by_mlb_is_followed_too(self):
        renames, _ = rename_map(seen=["Ranger Suárez"], current=["Ranger Suarez"])
        self.assertEqual(renames, {"Ranger Suárez": "Ranger Suarez"})

    def test_a_name_already_current_is_not_rewritten(self):
        renames, _ = rename_map(seen=["Chris Sale"], current=["Chris Sale"])
        self.assertEqual(renames, {})

    def test_a_name_no_current_player_resembles_is_left_alone(self):
        # Retired: nothing to rename it to, and 設定 will never ask for him.
        renames, _ = rename_map(seen=["Adam Wainwright"], current=["Chris Sale"])
        self.assertEqual(renames, {})

    def test_two_current_players_sharing_a_spelling_are_never_guessed(self):
        renames, ambiguous = rename_map(
            seen=["Jose Ramirez"], current=["José Ramírez", "Jose Ramirez "]
        )
        self.assertEqual(renames, {})
        self.assertEqual(ambiguous, ["Jose Ramirez"])

    def test_a_shared_spelling_is_not_ambiguous_when_it_is_already_exact(self):
        # Two current pitchers normalise to "luis garcia", but 紀錄 holds the
        # spelling one of them uses today, so there is nothing to decide.
        renames, ambiguous = rename_map(
            seen=["Luis Garcia"], current=["Luis García", "Luis Garcia"]
        )
        self.assertEqual(renames, {})
        self.assertEqual(ambiguous, [])

    def test_blank_and_missing_names_are_skipped(self):
        renames, ambiguous = rename_map(seen=["", "   "], current=["Chris Sale"])
        self.assertEqual(renames, {})
        self.assertEqual(ambiguous, [])

    def test_every_stale_spelling_of_one_player_maps_to_the_same_name(self):
        renames, _ = rename_map(
            seen=["Jesus Luzardo", "JESUS LUZARDO"], current=["Jesús Luzardo"]
        )
        self.assertEqual(
            renames,
            {"Jesus Luzardo": "Jesús Luzardo", "JESUS LUZARDO": "Jesús Luzardo"},
        )


if __name__ == "__main__":
    unittest.main()
