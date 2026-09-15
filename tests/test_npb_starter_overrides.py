"""Unit tests for 先發指定 (baseball/npb_starter_overrides.py)."""

import pytest

from baseball import npb_starter_overrides as so


ROWS = [
    ["日期", "球隊", "真正的先發", "備註"],
    ["2026-09-15", "西武", "平良 海馬", "森脇開局"],
    ["2026/9/22", "日本ハム", "伊藤 大海", ""],
    ["", "", "", ""],
]


class TestReadingTheTab:
    def test_a_row_names_the_real_starter_of_one_game(self):
        assert so.parse(ROWS)[("2026-09-15", "西武")] == "平良 海馬"

    def test_the_date_may_be_written_either_way(self):
        """People fill this in by hand, and Sheets renders a date how it likes."""
        assert so.parse(ROWS)[("2026-09-22", "日本ハム")] == "伊藤 大海"

    def test_blank_rows_are_not_designations(self):
        assert len(so.parse(ROWS)) == 2

    def test_a_row_missing_the_pitcher_is_ignored(self):
        """Half a designation says a game is special without saying what to do."""
        assert so.parse([ROWS[0], ["2026-09-15", "西武", "", "?"]]) == {}

    def test_a_team_written_under_another_name_still_joins(self):
        rows = [ROWS[0], ["2026-09-15", "横浜", "東 克樹", ""]]
        assert so.parse(rows)[("2026-09-15", "DeNA")] == "東 克樹"


class TestWhereTheStartersLineEnds:
    """先發指定 says who started; the span is how many pitchers that covers."""

    NAMES = ["森脇 亮介", "平良 海馬", "隅田 知一郎"]

    def test_without_a_designation_the_first_pitcher_is_the_starter(self):
        assert so.starter_span(self.NAMES, None) == 1

    def test_a_designation_folds_everyone_before_him_into_his_line(self):
        assert so.starter_span(self.NAMES, "平良 海馬") == 2

    def test_two_openers_are_both_folded_in(self):
        """Nothing has to say how many innings to merge — the name is enough."""
        assert so.starter_span(self.NAMES, "隅田 知一郎") == 3

    def test_designating_the_first_pitcher_changes_nothing(self):
        assert so.starter_span(self.NAMES, "森脇 亮介") == 1

    def test_a_box_score_holding_only_surnames_still_matches(self):
        """npb.jp writes 平良 where Yahoo writes 平良 海馬."""
        assert so.starter_span(["森脇", "平良", "隅田"], "平良 海馬") == 2

    def test_spacing_does_not_have_to_match(self):
        assert so.starter_span(self.NAMES, "平良海馬") == 2

    def test_an_exact_name_wins_over_a_shared_surname(self):
        names = ["田中 将大", "田中 太郎", "山本 由伸"]
        assert so.starter_span(names, "田中 太郎") == 2

    def test_a_pitcher_who_never_appeared_leaves_the_box_score_alone(self, capsys):
        """A typo must not silently credit the opener's line to the opener.

        Falling back to the first pitcher is what the record already says, so a
        misspelt designation costs nothing but the correction it meant to make —
        and says so, loudly enough to be fixed.
        """
        assert so.starter_span(self.NAMES, "平艮 海馬") == 1
        assert "平艮 海馬" in capsys.readouterr().out


class TestSpottingAnOpener:
    """The detector only ever asks a question; a person answers it."""

    def test_one_inning_then_a_long_outing_is_worth_asking_about(self):
        assert so.looks_like_an_opener([3, 21, 3]) is True

    def test_a_starter_chased_early_is_not_an_opener(self):
        """Identical on the first pitcher, and the difference is the second:
        a bullpen game shares the rest of the innings out, an opener hands them
        to one man. Neither shape is acted on — this only picks what to ask."""
        assert so.looks_like_an_opener([3, 6, 6, 6, 3]) is False

    def test_a_normal_start_is_not_asked_about(self):
        assert so.looks_like_an_opener([18, 3, 3, 3]) is False

    def test_a_team_that_used_one_pitcher_is_not_asked_about(self):
        assert so.looks_like_an_opener([27]) is False


class TestTheQueueOfQuestions:
    def setup_method(self):
        so.take_candidates()

    def test_a_candidate_is_kept_until_it_is_taken(self):
        so.note_candidate("2026-09-15", "西武", ["森脇 亮介", "平良 海馬"], [3, 21])
        assert so.take_candidates() == [
            {"date": "2026-09-15", "team": "西武",
             "opener": "森脇 亮介", "opener_outs": 3,
             "starter": "平良 海馬", "starter_outs": 21}
        ]

    def test_taking_them_empties_the_queue(self):
        so.note_candidate("2026-09-15", "西武", ["森脇 亮介", "平良 海馬"], [3, 21])
        so.take_candidates()
        assert so.take_candidates() == []

    def test_the_same_game_is_only_asked_about_once(self):
        """Every sweep re-reads the day; the question should not repeat."""
        for _ in range(3):
            so.note_candidate("2026-09-15", "西武", ["森脇 亮介", "平良 海馬"], [3, 21])
        assert len(so.take_candidates()) == 1

    def test_the_message_names_both_pitchers_and_what_to_do(self):
        so.note_candidate("2026-09-15", "西武", ["森脇 亮介", "平良 海馬"], [3, 21])
        message = so.candidate_message(so.take_candidates())
        assert "西武" in message and "森脇 亮介" in message and "平良 海馬" in message
        assert so.SHEET_NAME in message

    def test_no_candidates_is_no_message(self):
        assert so.candidate_message([]) is None


class TestTheLookup:
    def test_a_designation_is_found_by_date_and_team(self, monkeypatch):
        monkeypatch.setattr(so, "_designations", lambda: so.parse(ROWS))
        assert so.designated("2026-09-15", "西武") == "平良 海馬"

    def test_a_game_with_no_row_has_no_designation(self, monkeypatch):
        monkeypatch.setattr(so, "_designations", lambda: so.parse(ROWS))
        assert so.designated("2026-09-16", "西武") is None

    def test_an_unreachable_tab_costs_the_corrections_not_the_run(self, monkeypatch):
        """Without the tab every box score reads its first pitcher as the
        starter, which is the record as it stands — not a broken sweep."""
        def _boom():
            raise RuntimeError("no such worksheet")

        monkeypatch.setattr(so, "_read_sheet", _boom)
        so.reset()
        assert so.designated("2026-09-15", "西武") is None


@pytest.mark.parametrize("raw, expected", [
    ("2026-09-15", "2026-09-15"),
    ("2026/9/15", "2026-09-15"),
    ("2026/09/05", "2026-09-05"),
    (" 2026-09-15 ", "2026-09-15"),
    ("", ""),
    ("nonsense", "nonsense"),
])
def test_dates_are_read_however_they_were_written(raw, expected):
    assert so.normalise_date(raw) == expected


# The 2026-09-15 西武 @ 楽天 pitching table, cell for cell: 森脇 亮介 opened for
# an inning and 平良 海馬 threw the other seven, taking the loss 1-0.
SEIBU_PITCHING = """
<table class="bb-scoreTable">
  <tr class="bb-scoreTable__row">
    <td class="bb-scoreTable__data--player">森脇 亮介</td>
    <td class="bb-scoreTable__data--score">1.89</td>
    <td class="bb-scoreTable__data--score">1</td>
    <td class="bb-scoreTable__data--score">13</td>
    <td class="bb-scoreTable__data--score">3</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">1</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">0</td>
  </tr>
  <tr class="bb-scoreTable__row">
    <td class="bb-scoreTable__data--player">平良 海馬</td>
    <td class="bb-scoreTable__data--score">1.36</td>
    <td class="bb-scoreTable__data--score">7</td>
    <td class="bb-scoreTable__data--score">93</td>
    <td class="bb-scoreTable__data--score">23</td>
    <td class="bb-scoreTable__data--score">3</td>
    <td class="bb-scoreTable__data--score">1</td>
    <td class="bb-scoreTable__data--score">10</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">0</td>
    <td class="bb-scoreTable__data--score">1</td>
    <td class="bb-scoreTable__data--score">1</td>
  </tr>
</table>
"""


class TestReadingAnOpenersBoxScore:
    """What the tab is for, end to end, on the game that prompted it."""

    def _table(self):
        from bs4 import BeautifulSoup

        return BeautifulSoup(SEIBU_PITCHING, "html.parser").find(
            class_="bb-scoreTable")

    def _parse(self, monkeypatch, designations):
        import npb

        monkeypatch.setattr(so, "_designations", lambda: designations)
        so.take_candidates()
        return npb._parse_team_pitching(
            self._table(), game_date="2026-09-15", team="西武")

    def test_untouched_the_opener_is_the_starter(self, monkeypatch):
        starter, _, name = self._parse(monkeypatch, {})
        assert name == "森脇 亮介"
        assert (starter[0], starter[12]) == ("1", 0)   # 1 inning, no earned runs

    def test_designated_the_first_inning_lands_on_the_real_starter(self, monkeypatch):
        starter, _, name = self._parse(
            monkeypatch, {("2026-09-15", "西武"): "平良 海馬"})
        assert name == "平良 海馬"
        # [IP, BF, PC, Str, H, HR, BB, HBP, SO, WP, BK, R, ER]
        assert starter == ["8", 26, 106, 0, 3, 1, 0, 0, 11, 0, 0, 1, 1]

    def test_it_becomes_a_quality_start(self, monkeypatch):
        from baseball.npb_services import NpbRowsService

        starter, _, _ = self._parse(
            monkeypatch, {("2026-09-15", "西武"): "平良 海馬"})
        assert NpbRowsService.qs_flag(starter[0], starter[12]) == 1

    def test_the_teams_own_line_is_the_same_either_way(self, monkeypatch):
        """Only who the innings are filed under moves; the game does not."""
        _, designated, _ = self._parse(
            monkeypatch, {("2026-09-15", "西武"): "平良 海馬"})
        _, untouched, _ = self._parse(monkeypatch, {})
        assert designated == untouched

    def test_an_undesignated_opener_is_queued_as_a_question(self, monkeypatch):
        self._parse(monkeypatch, {})
        assert [c["starter"] for c in so.take_candidates()] == ["平良 海馬"]

    def test_a_designated_game_is_not_asked_about_again(self, monkeypatch):
        self._parse(monkeypatch, {("2026-09-15", "西武"): "平良 海馬"})
        assert so.take_candidates() == []
