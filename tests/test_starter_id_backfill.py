import unittest

from migration.backfill_mlb_starter_ids import (
    AWAY_ID_COLUMN,
    HOME_ID_COLUMN,
    starter_ids_from_feed,
    plan_id_writes,
)


def _feed(away_id, home_id, away_name="A Pitcher", home_name="H Pitcher"):
    def side(pid, name):
        return {
            "pitchers": [pid],
            "players": {
                f"ID{pid}": {
                    "person": {"id": pid, "fullName": name},
                    "stats": {"pitching": {"gamesStarted": 1}},
                }
            },
        }

    return {"liveData": {"boxscore": {"teams": {
        "away": side(away_id, away_name), "home": side(home_id, home_name)}}}}


class StarterIdsFromFeedTest(unittest.TestCase):
    def test_both_starters_are_read(self):
        self.assertEqual(
            starter_ids_from_feed(_feed(111, 222)),
            (111, 222),
        )

    def test_a_side_with_no_pitchers_yields_none(self):
        feed = _feed(111, 222)
        feed["liveData"]["boxscore"]["teams"]["home"] = {"pitchers": [], "players": {}}
        self.assertEqual(starter_ids_from_feed(feed), (111, None))


class PlanIdWritesTest(unittest.TestCase):
    """紀錄 has to end up keyed on something MLB cannot respell.

    Two active pitchers are both published as 'Luis Ortiz', and one of them
    holds 36 starts split across 'Luis Ortiz' and 'Luis L. Ortiz' in 紀錄, so
    設定 counts him twice over and never whole. A name cannot tell them apart;
    an id always can.
    """

    def test_contiguous_rows_become_one_range(self):
        writes = plan_id_writes({10: (1, 2), 11: (3, 4), 12: (5, 6)})
        self.assertEqual(len(writes), 2)
        away = next(w for w in writes if w["range"].startswith(f"'紀錄'!{AWAY_ID_COLUMN}"))
        self.assertEqual(away["range"], f"'紀錄'!{AWAY_ID_COLUMN}10:{AWAY_ID_COLUMN}12")
        self.assertEqual(away["values"], [[1], [3], [5]])

    def test_a_gap_starts_a_new_range(self):
        writes = plan_id_writes({10: (1, 2), 12: (5, 6)})
        away = [w for w in writes if w["range"].startswith(f"'紀錄'!{AWAY_ID_COLUMN}")]
        self.assertEqual(
            [w["range"] for w in away],
            [f"'紀錄'!{AWAY_ID_COLUMN}10:{AWAY_ID_COLUMN}10",
             f"'紀錄'!{AWAY_ID_COLUMN}12:{AWAY_ID_COLUMN}12"],
        )

    def test_a_missing_id_is_written_blank_rather_than_skipped(self):
        writes = plan_id_writes({10: (1, None)})
        home = next(w for w in writes if w["range"].startswith(f"'紀錄'!{HOME_ID_COLUMN}"))
        self.assertEqual(home["values"], [[""]])

    def test_nothing_to_write(self):
        self.assertEqual(plan_id_writes({}), [])


if __name__ == "__main__":
    unittest.main()
