import unittest

from baseball.mlb_names import normalise, rename_map


def _player(full, first=None, last=None, use=None):
    first = first or full.split()[0]
    last = last or full.split()[-1]
    return {
        "fullName": full,
        "firstName": first,
        "lastName": last,
        "useName": use or first,
    }


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
            seen=["Jesus Luzardo"], current=[_player("Jesús Luzardo"), _player("Chris Sale")]
        )
        self.assertEqual(renames, {"Jesus Luzardo": "Jesús Luzardo"})
        self.assertEqual(ambiguous, [])

    def test_an_accent_removed_by_mlb_is_followed_too(self):
        renames, _ = rename_map(seen=["Ranger Suárez"], current=[_player("Ranger Suarez")])
        self.assertEqual(renames, {"Ranger Suárez": "Ranger Suarez"})

    def test_a_name_already_current_is_not_rewritten(self):
        renames, _ = rename_map(seen=["Chris Sale"], current=[_player("Chris Sale")])
        self.assertEqual(renames, {})

    def test_a_name_no_current_player_resembles_is_left_alone(self):
        # Retired: nothing to rename it to, and 設定 will never ask for him.
        renames, _ = rename_map(seen=["Adam Wainwright"], current=[_player("Chris Sale")])
        self.assertEqual(renames, {})

    def test_two_current_players_sharing_a_spelling_are_never_guessed(self):
        renames, ambiguous = rename_map(
            seen=["Jose Ramirez"], current=[_player("José Ramírez"), _player("Jose Ramirez ", first="Jose", last="Ramirez")]
        )
        self.assertEqual(renames, {})
        self.assertEqual(ambiguous, ["Jose Ramirez"])

    def test_a_shared_spelling_is_not_ambiguous_when_it_is_already_exact(self):
        # Two current pitchers normalise to "luis garcia", but 紀錄 holds the
        # spelling one of them uses today, so there is nothing to decide.
        renames, ambiguous = rename_map(
            seen=["Luis Garcia"], current=[_player("Luis García"), _player("Luis Garcia")]
        )
        self.assertEqual(renames, {})
        self.assertEqual(ambiguous, [])

    def test_blank_and_missing_names_are_skipped(self):
        renames, ambiguous = rename_map(seen=["", "   "], current=[_player("Chris Sale")])
        self.assertEqual(renames, {})
        self.assertEqual(ambiguous, [])

    def test_every_stale_spelling_of_one_player_maps_to_the_same_name(self):
        renames, _ = rename_map(
            seen=["Jesus Luzardo", "JESUS LUZARDO"], current=[_player("Jesús Luzardo")]
        )
        self.assertEqual(
            renames,
            {"Jesus Luzardo": "Jesús Luzardo", "JESUS LUZARDO": "Jesús Luzardo"},
        )


if __name__ == "__main__":
    unittest.main()


class NicknameChangeTest(unittest.TestCase):
    """MLB also changes what a player is *called*, not just how it is accented.

    Thornton went from 'Zach' to 'Zac' mid-season while his legal first name
    stayed 'Zachary'. Accent folding cannot see that, and comparing against the
    game feed only reaches games inside the revision window -- his two starts
    were 41 and 57 days old.
    """

    THORNTONS = [
        _player("Zac Thornton", first="Zachary", use="Zac"),
        _player("Trent Thornton", first="Trent", use="Trent"),
    ]

    def test_a_shortened_first_name_follows_the_legal_one(self):
        renames, ambiguous = rename_map(seen=["Zach Thornton"], current=self.THORNTONS)
        self.assertEqual(renames, {"Zach Thornton": "Zac Thornton"})
        self.assertEqual(ambiguous, [])

    def test_the_other_thornton_is_not_swept_in(self):
        renames, _ = rename_map(seen=["Trent Thornton"], current=self.THORNTONS)
        self.assertEqual(renames, {})

    def test_an_exact_name_beats_a_longer_first_name_on_the_roster(self):
        # 'chris' prefixes 'christian', but Chris Sale is himself.
        current = [_player("Chris Sale"), _player("Christian Sale")]
        renames, ambiguous = rename_map(seen=["Chris Sale"], current=current)
        self.assertEqual(renames, {})
        self.assertEqual(ambiguous, [])

    def test_two_plausible_players_are_reported_not_guessed(self):
        current = [
            _player("Zac Thornton", first="Zachary", use="Zac"),
            _player("Zachary Thornton", first="Zachary", use="Zachary"),
        ]
        renames, ambiguous = rename_map(seen=["Zach Thornton"], current=current)
        self.assertEqual(renames, {})
        self.assertEqual(ambiguous, ["Zach Thornton"])

    def test_a_middle_initial_is_never_prefix_matched_away(self):
        # MLB uses the initial to tell two Luis Ortizes apart.
        current = [_player("Luis Ortiz")]
        renames, _ = rename_map(seen=["Luis L. Ortiz"], current=current)
        self.assertEqual(renames, {})

    def test_a_two_letter_first_name_is_too_short_to_match_on(self):
        current = [_player("James Smith")]
        renames, _ = rename_map(seen=["Ja Smith"], current=current)
        self.assertEqual(renames, {})

    def test_a_different_surname_never_matches(self):
        current = [_player("Zac Thornton", first="Zachary", use="Zac")]
        renames, _ = rename_map(seen=["Zach Thorton"], current=current)
        self.assertEqual(renames, {})
