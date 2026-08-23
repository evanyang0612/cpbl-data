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


class NameCorrectionsTest(unittest.TestCase):
    """With an id in hand, the name is not a guess any more.

    rename_map has to reason from spelling, so it cannot carry 'Louie Varland'
    to 'Louis Varland' or 'Luis L. Ortiz' to 'Luis Ortiz' -- 43 starts split
    across two pitchers. The id says who pitched, and MLB says what it calls
    him today; nothing has to be inferred.
    """

    PUBLISHED = {682847: "Luis Ortiz", 686973: "Louis Varland"}

    def test_a_name_matching_the_id_is_left_alone(self):
        from baseball.mlb_names import name_corrections

        entries = [(100, 3, "Luis Ortiz", 682847)]
        self.assertEqual(name_corrections(entries, self.PUBLISHED), {})

    def test_a_name_the_id_disagrees_with_is_corrected(self):
        from baseball.mlb_names import name_corrections

        entries = [(100, 3, "Luis L. Ortiz", 682847)]
        self.assertEqual(name_corrections(entries, self.PUBLISHED), {100: {3: "Luis Ortiz"}})

    def test_both_columns_of_one_row_are_kept_together(self):
        from baseball.mlb_names import name_corrections

        entries = [(100, 3, "Luis L. Ortiz", 682847), (100, 30, "Louie Varland", 686973)]
        self.assertEqual(
            name_corrections(entries, self.PUBLISHED),
            {100: {3: "Luis Ortiz", 30: "Louis Varland"}},
        )

    def test_an_id_mlb_no_longer_lists_is_left_alone(self):
        from baseball.mlb_names import name_corrections

        entries = [(100, 3, "Adam Wainwright", 425794)]
        self.assertEqual(name_corrections(entries, self.PUBLISHED), {})

    def test_a_row_without_an_id_is_left_to_rename_map(self):
        from baseball.mlb_names import name_corrections

        entries = [(100, 3, "Jesus Luzardo", None)]
        self.assertEqual(name_corrections(entries, self.PUBLISHED), {})

    def test_a_blank_name_beside_a_known_id_is_filled_in(self):
        from baseball.mlb_names import name_corrections

        entries = [(100, 3, "", 682847)]
        self.assertEqual(name_corrections(entries, self.PUBLISHED), {100: {3: "Luis Ortiz"}})


class SharedNamesTest(unittest.TestCase):
    """Two pitchers published under one name is the failure a name cannot survive.

    MLB lists ids 682847 and 656814 both as 'Luis Ortiz', and 671106 shares
    'Logan Allen' with someone else. Today only one of each pair ever starts,
    so nothing is actually miscounted -- but it is the one thing no spelling
    rule can ever fix, so it is worth being told about rather than discovering.
    """

    def test_two_ids_under_one_name_are_reported(self):
        from baseball.mlb_names import shared_names

        published = {682847: "Luis Ortiz", 656814: "Luis Ortiz", 621244: "José Berríos"}
        self.assertEqual(
            shared_names([682847, 656814, 621244], published),
            {"Luis Ortiz": [656814, 682847]},
        )

    def test_distinct_names_report_nothing(self):
        from baseball.mlb_names import shared_names

        published = {1: "A Pitcher", 2: "B Pitcher"}
        self.assertEqual(shared_names([1, 2], published), {})

    def test_the_same_id_twice_is_not_a_clash(self):
        from baseball.mlb_names import shared_names

        self.assertEqual(shared_names([1, 1, 1], {1: "A Pitcher"}), {})

    def test_ids_mlb_no_longer_lists_are_ignored(self):
        from baseball.mlb_names import shared_names

        self.assertEqual(shared_names([1, 99], {1: "A Pitcher"}), {})
