"""Unit tests for the 先發明細 row builder (migration/write_npb_starter_log.py)."""

import importlib.util
import os

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "write_npb_starter_log",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "migration", "write_npb_starter_log.py"),
)
wsl = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(wsl)


def _pitcher(name, *, order=1, pid="1", result="", pitches=100, batters=25,
             outs=18, hits=5, hr=1, bb=2, hbp=0, so=7, wp=0, balk=0,
             runs=3, er=3):
    return dict(order=order, name=name, player_id=pid, result=result,
                pitches=pitches, batters=batters, outs=outs, hits=hits, hr=hr,
                bb=bb, hbp=hbp, so=so, wp=wp, balk=balk, runs=runs, er=er)


def _record(**over):
    record = {
        "game_code": "20260801-c-d-15",
        "date": "2026-08-01",
        "away": "中日",
        "home": "広島",
        "away_pitchers": [_pitcher("マラー", result="○"), _pitcher("藤嶋", order=2)],
        "home_pitchers": [_pitcher("斉藤優", result="●"), _pitcher("菊地", order=2)],
    }
    record.update(over)
    return record


def test_one_row_per_side_and_only_the_starter():
    rows = wsl.starter_rows(_record())

    assert len(rows) == 2
    names = [r[wsl.COLUMNS.index("投手")] for r in rows]
    assert names == ["マラー", "斉藤優"]


def test_each_row_knows_its_side_and_opponent():
    away, home = wsl.starter_rows(_record())
    team = wsl.COLUMNS.index("球隊")
    opp = wsl.COLUMNS.index("對手")
    side = wsl.COLUMNS.index("主客")

    assert (away[team], away[opp], away[side]) == ("中日", "広島", "客")
    assert (home[team], home[opp], home[side]) == ("広島", "中日", "主")


# 賽錄 stores innings in `.1`/`.2` notation, which cannot be summed. Outs are
# the only form a rolling window can add up, so the sheet gets the decimal.
def test_innings_are_written_as_a_decimal():
    rows = wsl.starter_rows(_record(
        away_pitchers=[_pitcher("大野", outs=20)],
        home_pitchers=[_pitcher("相手", outs=18)]))

    ip = wsl.COLUMNS.index("投球回")
    assert rows[0][ip] == pytest.approx(20 / 3)
    assert rows[1][ip] == 6.0


# 分析表紀錄's 「四球」 column actually holds walks plus hit batsmen. Keeping
# them apart here means either quantity can be rebuilt; merging them could not
# be undone.
def test_walks_and_hit_batsmen_are_separate_columns():
    rows = wsl.starter_rows(_record(
        away_pitchers=[_pitcher("大野", bb=5, hbp=2)],
        home_pitchers=[_pitcher("相手")]))

    assert rows[0][wsl.COLUMNS.index("四球")] == 5
    assert rows[0][wsl.COLUMNS.index("死球")] == 2


def test_a_cancelled_game_contributes_no_rows():
    assert wsl.starter_rows(
        {"game_code": "x", "date": "2026-08-01", "cancelled": True}) == []


def test_a_game_missing_one_side_contributes_no_rows():
    assert wsl.starter_rows(_record(home_pitchers=[])) == []


def test_rows_carry_the_game_code_so_they_can_be_traced_back():
    rows = wsl.starter_rows(_record())

    assert rows[0][wsl.COLUMNS.index("GameId")] == "20260801-c-d-15"


def test_rows_are_ordered_by_date_then_game():
    records = [
        _record(game_code="b", date="2026-08-02"),
        _record(game_code="a", date="2026-08-01"),
    ]

    rows = wsl.build_rows(records)

    dates = [r[wsl.COLUMNS.index("日期")] for r in rows]
    assert dates == ["2026-08-01", "2026-08-01", "2026-08-02", "2026-08-02"]


class TestAnOpenersStart:
    """先發指定 has to reach the backfill too, or it would undo the sweep."""

    def _record(self):
        return _record(
            date="2026-09-15",
            away="西武",
            home="楽天",
            away_pitchers=[_pitcher("森脇", outs=3, pitches=13, batters=3, hits=0,
                                    hr=0, bb=0, hbp=0, so=1, runs=0, er=0),
                           _pitcher("平良", order=2, result="●", outs=21,
                                    pitches=93, batters=23, hits=3, hr=1, bb=0,
                                    hbp=0, so=10, runs=1, er=1)],
        )

    def _row(self, monkeypatch, designations):
        from baseball import npb_starter_overrides as so

        monkeypatch.setattr(so, "_designations", lambda: designations)
        return wsl.starter_rows(self._record())[0]

    def test_untouched_the_opener_is_written(self, monkeypatch):
        row = self._row(monkeypatch, {})
        assert row[wsl.COLUMNS.index("投手")] == "森脇"
        assert row[wsl.COLUMNS.index("投球回")] == 1

    def test_designated_the_line_belongs_to_the_real_starter(self, monkeypatch):
        """The surname is all npb.jp writes, and it still finds 平良 海馬."""
        row = self._row(monkeypatch, {("2026-09-15", "西武"): "平良 海馬"})
        assert row[wsl.COLUMNS.index("投手")] == "平良"
        assert row[wsl.COLUMNS.index("投球回")] == 8
        assert row[wsl.COLUMNS.index("投球數")] == 106
        assert row[wsl.COLUMNS.index("三振")] == 11
        assert row[wsl.COLUMNS.index("自責")] == 1
        assert row[wsl.COLUMNS.index("勝敗")] == "●"
