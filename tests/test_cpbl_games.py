"""Unit tests for the CPBL schedule join used by the 盤口 scraper."""

from datetime import datetime, timedelta, timezone

from baseball import cpbl_games as cg

TW = timezone(timedelta(hours=8))


def _tw(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=TW)


def _sched_game(game_sno, game_date, home, away, start, kind_code="A"):
    """A schedule entry shaped like CPBL's getgamedatas payload."""
    return {
        "GameSno": game_sno,
        "KindCode": kind_code,
        "Year": game_date[:4],
        "GameDate": f"{game_date}T00:00:00",
        "GameDateTimeS": f"{game_date}T{start}",
        "PreExeDate": f"{game_date}T{start}",
        "HomeTeamName": home,
        "VisitingTeamName": away,
    }


def _index():
    return cg.CpblGameIndex([
        _sched_game(323, "2026-09-11", "樂天桃猿", "中信兄弟", "18:35:00"),
        _sched_game(324, "2026-09-11", "台鋼雄鷹", "富邦悍將", "18:35:00"),
    ])


def test_normalize_team_folds_both_sources_to_the_賽程_short_name():
    """賽程 columns D/F hold cpbl.TEAM_MAP's short names; the join needs those."""
    assert cg.normalize_team("樂天桃猿") == "樂天"
    assert cg.normalize_team("統一7-ELEVEn獅") == "統一7-ELEVEn"
    assert cg.normalize_team("中信兄弟") == "中信兄弟"
    assert cg.normalize_team("味全龍") == "味全"
    assert cg.normalize_team("富邦悍將") == "富邦"
    assert cg.normalize_team("台鋼雄鷹") == "台鋼"


def test_normalize_team_reads_the_english_name_too():
    """A locale flip on the feed would otherwise blank every name at once."""
    assert cg.normalize_team("Rakuten Monkeys") == "樂天"
    assert cg.normalize_team("Uni-President 7-Eleven Lions") == "統一7-ELEVEn"
    assert cg.normalize_team("Wei Chuan Dragons") == "味全"


def test_normalize_team_is_blank_for_an_unknown_club():
    assert cg.normalize_team("阪神") == ""
    assert cg.normalize_team("") == ""


def test_index_resolves_the_game_sno_and_its_date():
    game = _index().find("樂天", "中信兄弟", _tw("2026-09-11T18:35:00"))
    assert game["game_sno"] == "323"
    assert game["game_date"] == "2026-09-11"
    assert game["kind_code"] == "A"


def test_index_picks_the_nearest_start_for_a_doubleheader():
    index = cg.CpblGameIndex([
        _sched_game(101, "2026-05-04", "味全龍", "富邦悍將", "12:35:00"),
        _sched_game(102, "2026-05-04", "味全龍", "富邦悍將", "17:05:00"),
    ])
    assert index.find("味全", "富邦", _tw("2026-05-04T17:05:00"))["game_sno"] == "102"
    assert index.find("味全", "富邦", _tw("2026-05-04T12:35:00"))["game_sno"] == "101"


def test_index_rejects_a_start_beyond_the_drift_window():
    assert _index().find("樂天", "中信兄弟", _tw("2026-09-12T18:35:00")) is None


def test_index_returns_none_for_teams_it_does_not_carry():
    assert _index().find("味全", "統一7-ELEVEn", _tw("2026-09-11T18:35:00")) is None


def test_index_ignores_home_away_reversals():
    """中信兄弟 host 樂天 on other nights; the odds row must not match those."""
    assert _index().find("中信兄弟", "樂天", _tw("2026-09-11T18:35:00")) is None


def test_build_index_covers_every_month_the_slate_touches(monkeypatch):
    asked = []

    def fake_fetch(year, month, kind_code, session):
        asked.append((year, int(month), kind_code))
        return []

    monkeypatch.setattr(cg, "_fetch_schedule", fake_fetch)
    cg.build_index([_tw("2026-03-31T18:35:00"), _tw("2026-04-01T17:05:00")],
                   session=object())
    assert asked == [(2026, 3, "A"), (2026, 4, "A")]
