"""Unit tests for the Telegram odds broadcast (baseball/odds_notify.py)."""

from datetime import datetime

from baseball import odds_notify as on
from baseball.pinnacle_odds import JST

# G(2) = 0.40, G(1) = 0.55, G(-1) = 0.70 -> home lays a run at +33.
SPREADS = [
    {"hdp": -1.5, "home": 1 / 0.40, "away": 1 / 0.60},
    {"hdp": 1.5, "home": 1 / 0.70, "away": 1 / 0.30},
]
# H(8) = 0.45, H(7) = 0.57 -> the over pays 17 on a total of 7.
TOTALS = [
    {"line": 7.5, "over": 1 / 0.45, "under": 1 / 0.55},
    {"line": 6.5, "over": 1 / 0.57, "under": 1 / 0.43},
]


def _snapshot(event_id="1", period="final", home="巨人", away="広島",
              start="2026-08-24T18:00:00+09:00", **overrides):
    snap = {
        "event_id": event_id,
        "period": period,
        "home_norm": home,
        "away_norm": away,
        "home_team": home,
        "away_team": away,
        "start_jst": start,
        "game_date": "2026-08-24",
        "ml_home": 1 / 0.55,
        "ml_away": 1 / 0.45,
        "all_spreads": SPREADS,
        "all_totals": TOTALS,
    }
    snap.update(overrides)
    return snap


NOW = datetime(2026, 8, 24, 11, 30, tzinfo=JST)


def _rows(text, fixture="広島 @ 巨人"):
    """The rows under one game's fixture line, which carry no labels."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if fixture in line)
    out = []
    for line in lines[start + 1:]:
        if not line.strip():
            break
        out.append(line.strip())
    return out


def test_message_names_the_team_laying_the_handicap():
    """The sheet writes the line bare — ``1+35``, not ``-1 +35``.

    Which side gives the runs is carried by whose line the number sits on, so a
    minus in front of the line would only read as a second water level.
    """
    text = on.build_message([_snapshot()], now=NOW)
    assert "広島 @ 巨人" in text
    away_side, home_side = _rows(text)
    assert home_side == "巨人　１＋３５"   # G(1) = 0.55, home is the favourite
    assert away_side.startswith("広島")   # which side lays is said by position
    assert "１＋３５" not in away_side


def test_total_is_quoted_from_the_over_side_alone():
    """The sheet quotes 大小 from 大, and the 小 side is just its mirror."""
    text = on.build_message([_snapshot()], now=NOW)
    assert _rows(text)[0].endswith("７－１５")   # the away line carries it
    assert "小" not in text


def test_pick_em_is_written_against_the_home_team():
    """PK has no side laying runs, so the home team carries the line.

    G(1) = 0.49 makes the away team the nominal favourite, but it is too
    slight to lay a run — the water would be .87 — so the game is a pick'em
    and the home team is the one named.
    """
    even = _snapshot(
        all_spreads=[{"hdp": 1.5, "home": 1 / 0.64, "away": 1 / 0.36}],
        ml_home=1 / 0.49, ml_away=1 / 0.51,
    )
    text = on.build_message([even], now=NOW)
    _away, home_side = _rows(text)
    assert home_side == "巨人　ＰＫ"   # 巨人 is home, 広島 the favourite


def test_half_period_is_ignored_when_a_full_game_line_exists():
    snaps = [_snapshot(period="half"), _snapshot(period="final")]
    text = on.build_message(snaps, now=NOW)
    assert text.count("広島 @ 巨人") == 1


def test_games_are_listed_in_start_order():
    late = _snapshot(event_id="2", home="DeNA", away="阪神",
                     start="2026-08-24T18:00:00+09:00")
    early = _snapshot(event_id="3", home="楽天", away="西武",
                      start="2026-08-24T14:00:00+09:00")
    text = on.build_message([late, early], now=NOW)
    assert text.index("西武 @ 楽天") < text.index("阪神 @ DeNA")


def test_header_carries_the_slate_date_and_not_the_hour_it_was_built():
    """Readers act on the line, not on when the job happened to run: a clock
    in the header only invites the post to be read as a live price."""
    text = on.build_message([_snapshot()], now=NOW)
    assert "8/24" in text
    assert "11:30" not in text.splitlines()[0]


def test_header_is_set_off_from_the_slate_by_a_blank_line():
    """The games already breathe between each other; the header should too.

    Without it the title sits flush against the first fixture and the post
    reads as one crowded block.
    """
    text = on.build_message([_snapshot()], now=NOW)
    header, blank, first = text.splitlines()[:3]
    assert header.startswith("\u26be")
    assert blank == ""
    assert "広島 @ 巨人" in first


def test_header_dates_the_games_not_the_broadcast():
    """The board for tomorrow opens once tonight's games end, so the evening
    broadcast is about a slate that has not started yet."""
    tomorrow = _snapshot(start="2026-08-26T18:00:00+09:00",
                         game_date="2026-08-26")
    text = on.build_message([tomorrow], now=datetime(2026, 8, 25, 23, 0, tzinfo=JST))
    assert "8/26" in text
    assert "8/25" not in text


def test_only_the_target_date_is_broadcast():
    """Tonight's board still carries today's games; the post is about tomorrow."""
    today = _snapshot(event_id="9", start="2026-08-24T18:00:00+09:00",
                      game_date="2026-08-24", home="楽天", away="西武")
    tomorrow = _snapshot(event_id="8", start="2026-08-25T18:00:00+09:00",
                         game_date="2026-08-25")
    text = on.build_message([today, tomorrow], now=NOW, game_date="2026-08-25")
    assert "広島 @ 巨人" in text
    assert "西武 @ 楽天" not in text


class _Ledger:
    def __init__(self, already=()):
        self.already = set(already)
        self.recorded = []

    def sent(self, game_date, phase):
        return (game_date, phase) in self.already

    def record(self, game_date, phase, games):
        self.recorded.append((game_date, phase, games))
        self.already.add((game_date, phase))


def _stub_feed(monkeypatch, snapshots):
    sent = []
    monkeypatch.setattr(on, "fetch_baseball_events", lambda: {})
    monkeypatch.setattr(on, "parse_events", lambda raw, league=None: snapshots)
    monkeypatch.setattr(on, "_send", lambda msg: sent.append(msg) or True)
    # Starters come from Yahoo; these tests are about the broadcast, not it.
    from baseball.npb_starters import Slate
    monkeypatch.setattr(on, "_starters_for",
                        lambda league, game_date: Slate(starters={}, weather={}))
    return sent


def test_broadcast_uses_its_own_bot_not_the_alerting_one(monkeypatch):
    """Subscribers must not receive scraper failure alerts, so the broadcast
    has its own bot rather than sharing the one that reports breakages."""
    calls = {}
    monkeypatch.setenv("TELEGRAM_BROADCAST_BOT_TOKEN", "broadcast-token")
    monkeypatch.setenv("TELEGRAM_BROADCAST_CHAT_ID", "-1001111111111")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "alerting-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "972158775")
    monkeypatch.setattr("utils.send_telegram",
                        lambda msg, **kw: calls.update(kw) or True)

    assert on._send("hello") is True
    assert calls["bot_token"] == "broadcast-token"
    assert calls["chat_id"] == "-1001111111111"


def test_broadcast_refuses_to_fall_back_to_the_alerting_chat(monkeypatch):
    """Falling back would post the odds into the ops chat and leave the channel
    silent — worse than not sending at all."""
    monkeypatch.delenv("TELEGRAM_BROADCAST_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_BROADCAST_CHAT_ID", raising=False)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "alerting-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "972158775")
    called = []
    monkeypatch.setattr("utils.send_telegram",
                        lambda msg, **kw: called.append(kw) or True)

    assert on._send("hello") is False
    assert called == []


def test_open_and_close_are_tracked_as_separate_broadcasts(monkeypatch):
    """The same slate goes out twice — once when it opens, once before first
    pitch — so the ledger has to key on which one, not just the date."""
    slate = [_snapshot(game_date="2026-08-25",
                       start="2026-08-25T18:00:00+09:00", mins_to_start=20)]
    sent = _stub_feed(monkeypatch, slate)
    ledger = _Ledger()

    on.run_once(game_date="2026-08-25", phase="open", ledger=ledger)
    on.run_once(game_date="2026-08-25", phase="close", ledger=ledger)
    on.run_once(game_date="2026-08-25", phase="open", ledger=ledger)

    assert len(sent) == 2
    assert "開盤" in sent[0] and "18:00 尾盤" in sent[1]
    assert ledger.recorded == [("2026-08-25", "open", 1),
                               ("2026-08-25", "close 18:00", 1)]


def test_closing_broadcast_waits_until_first_pitch_is_near(monkeypatch):
    """A closing line posted five hours early is just the opening line again."""
    early = [_snapshot(game_date="2026-08-25",
                       start="2026-08-25T18:00:00+09:00", mins_to_start=300)]
    sent = _stub_feed(monkeypatch, early)
    ledger = _Ledger()

    assert on.run_once(game_date="2026-08-25", phase="close", ledger=ledger) == []
    assert sent == []
    assert ledger.recorded == []   # a later run, nearer the game, still posts


def test_opening_broadcast_does_not_wait_for_first_pitch(monkeypatch):
    early = [_snapshot(game_date="2026-08-25",
                       start="2026-08-25T18:00:00+09:00", mins_to_start=300)]
    sent = _stub_feed(monkeypatch, early)

    on.run_once(game_date="2026-08-25", phase="open", ledger=_Ledger())

    assert len(sent) == 1


def _staggered(monkeypatch, early_lead, late_lead):
    slate = [
        _snapshot(event_id="early", home="楽天", away="西武",
                  game_date="2026-08-25", start="2026-08-25T14:00:00+09:00",
                  mins_to_start=early_lead),
        _snapshot(event_id="late", game_date="2026-08-25",
                  start="2026-08-25T18:00:00+09:00", mins_to_start=late_lead),
    ]
    return _stub_feed(monkeypatch, slate)


def test_a_staggered_card_closes_one_start_time_at_a_time(monkeypatch):
    """The 14:00 games have long closed while the 18:00 games are still moving,
    so each first pitch gets its own closing post."""
    sent = _staggered(monkeypatch, early_lead=20, late_lead=260)
    ledger = _Ledger()

    on.run_once(game_date="2026-08-25", phase="close", ledger=ledger)

    assert len(sent) == 1
    assert "14:00 尾盤" in sent[0]
    assert "西武 @ 楽天" in sent[0] and "広島 @ 巨人" not in sent[0]
    assert ledger.recorded == [("2026-08-25", "close 14:00", 1)]


def test_the_later_start_still_closes_on_a_later_run(monkeypatch):
    sent = _staggered(monkeypatch, early_lead=20, late_lead=260)
    ledger = _Ledger()
    on.run_once(game_date="2026-08-25", phase="close", ledger=ledger)

    sent = _staggered(monkeypatch, early_lead=-220, late_lead=20)
    on.run_once(game_date="2026-08-25", phase="close", ledger=ledger)

    assert len(sent) == 1
    assert "18:00 尾盤" in sent[0]
    assert [r[1] for r in ledger.recorded] == ["close 14:00", "close 18:00"]


def test_games_sharing_a_first_pitch_close_in_one_post(monkeypatch):
    slate = [
        _snapshot(event_id="a", game_date="2026-08-25",
                  start="2026-08-25T18:00:00+09:00", mins_to_start=20),
        _snapshot(event_id="b", home="楽天", away="西武",
                  game_date="2026-08-25", start="2026-08-25T18:00:00+09:00",
                  mins_to_start=20),
    ]
    sent = _stub_feed(monkeypatch, slate)
    ledger = _Ledger()

    on.run_once(game_date="2026-08-25", phase="close", ledger=ledger)

    assert len(sent) == 1
    assert "広島 @ 巨人" in sent[0] and "西武 @ 楽天" in sent[0]
    assert ledger.recorded == [("2026-08-25", "close 18:00", 2)]


def test_a_slate_is_broadcast_once_however_often_it_is_triggered(monkeypatch):
    """cron-job.org fires every few minutes because the board opens at no fixed
    hour, so every run after the first finds the same games."""
    slate = [_snapshot(game_date="2026-08-25",
                       start="2026-08-25T18:00:00+09:00")]
    sent = _stub_feed(monkeypatch, slate)
    ledger = _Ledger()

    for _ in range(5):
        on.run_once(game_date="2026-08-25", ledger=ledger)

    assert len(sent) == 1
    assert ledger.recorded == [("2026-08-25", "open", 1)]


def test_a_quiet_run_before_the_board_opens_records_nothing(monkeypatch):
    sent = _stub_feed(monkeypatch, [_snapshot(game_date="2026-08-24")])
    ledger = _Ledger()

    assert on.run_once(game_date="2026-08-25", ledger=ledger) == []
    assert sent == []
    assert ledger.recorded == []   # so a later run still posts it


def test_no_games_produces_no_message():
    assert on.build_message([], now=NOW) is None


def test_game_without_a_derivable_ladder_still_appears():
    """A thin ladder must not silently drop the game from the broadcast."""
    thin = _snapshot(all_spreads=[], all_totals=[], ml_home=None, ml_away=None)
    text = on.build_message([thin], now=NOW)
    assert "広島 @ 巨人" in text
    assert "—" in text


def test_numbers_are_written_full_width_like_the_sheet():
    """Half-width digits sit badly next to CJK text; the sheet uses full width."""
    assert on.fullwidth("2-75") == "２－７５"
    assert on.fullwidth("8+75") == "８＋７５"
    assert on.fullwidth("PK") == "ＰＫ"
    assert on.fullwidth("6.5") == "６．５"
    assert on.fullwidth("平") == "平"


def test_starters_are_named_and_linked_to_their_yahoo_page():
    from baseball.npb_starters import Slate, Starter

    starters = {"巨人": Starter("則本 昂大", "1200117"),
                "ヤクルト": Starter("吉村 貢司郎", "2103794")}
    snap = _snapshot(home="ヤクルト", away="巨人")
    text = on.build_message([snap], now=NOW, context=Slate(starters=starters, weather={}))
    assert '<a href="https://baseball.yahoo.co.jp/npb/player/1200117/top">則本　昂大</a>' in text
    assert "吉村　貢司郎" in text


def test_the_starters_are_listed_away_first():
    """The pitcher row reads in the same order as the fixture line above it."""
    from baseball.npb_starters import Slate, Starter

    starters = {"巨人": Starter("則本 昂大", "1200117"),
                "ヤクルト": Starter("吉村 貢司郎", "2103794")}
    text = on.build_message([_snapshot(home="ヤクルト", away="巨人")],
                            now=NOW, context=Slate(starters=starters, weather={}))
    away_side, home_side = _rows(text, "巨人 @ ヤクルト")
    assert "則本　昂大" in away_side
    assert "吉村　貢司郎" in home_side


def test_an_unannounced_starter_falls_back_to_the_team():
    """The two sides always get a line, since the handicap rides on one."""
    text = on.build_message([_snapshot()], now=NOW)
    away_side, home_side = _rows(text)
    assert away_side.startswith("広島")
    assert home_side == "巨人　１＋３５"


def test_a_game_without_a_known_starter_still_shows_the_team():
    text = on.build_message([_snapshot()], now=NOW)
    assert "広島" in text and "巨人" in text


def test_the_closing_post_keeps_the_names_but_drops_the_links():
    """The close is a short "where it landed" note; the links belong to the
    opening post, which is the one people read ahead of time."""
    from baseball.npb_starters import Slate, Starter

    starters = {"巨人": Starter("則本 昂大", "1200117"),
                "ヤクルト": Starter("吉村 貢司郎", "2103794")}
    snap = _snapshot(home="ヤクルト", away="巨人")
    text = on.build_message([snap], now=NOW, context=Slate(starters=starters, weather={}), link=False)
    assert "則本　昂大" in text
    assert "<a href" not in text



def test_the_total_keeps_its_column_whether_or_not_a_handicap_precedes_it():
    """With nothing in front of it the total slides left into the handicap's
    place and reads like one, so the handicap keeps a slot even when empty."""
    # Home lays, so the away line carries the total alone.
    text = on.build_message([_snapshot()], now=NOW)
    away_side, home_side = _rows(text)
    assert home_side == "巨人　１＋３５"
    gap = away_side[len("広島"):away_side.index("７－１５")]
    assert len(gap) == len("　１＋３５　")   # the same width the handicap took


def test_a_short_handicap_is_padded_to_the_same_slot():
    """ＰＫ is two characters where １＋３５ is four; without padding the total
    would sit a column to the left on pick'em games."""
    even = _snapshot(
        all_spreads=[{"hdp": 1.5, "home": 1 / 0.64, "away": 1 / 0.36}],
        ml_home=1 / 0.49, ml_away=1 / 0.51,
    )
    text = on.build_message([even], now=NOW)
    away_side, _home = _rows(text)
    assert away_side.index("７－１５") == len("広島") + len("　１＋３５　")


def test_pitcher_names_share_a_column_across_the_whole_slate():
    """モイネロ is four characters and 吉村 貢司郎 is six; without padding the
    numbers beside them start at different places down the message."""
    from baseball.npb_starters import Slate, Starter

    starters = {"巨人": Starter("則本 昂大", "1"), "ヤクルト": Starter("モイネロ", "2"),
                "広島": Starter("吉村 貢司郎", "3"), "DeNA": Starter("西 勇輝", "4")}
    text = on.build_message(
        [_snapshot(event_id="1", home="ヤクルト", away="巨人"),
         _snapshot(event_id="2", home="DeNA", away="広島")],
        now=NOW, context=Slate(starters=starters, weather={}))
    columns = {line.index("１＋３５") for line in text.splitlines()
               if "１＋３５" in line}
    assert len(columns) == 1   # every handicap starts in the same column


def test_a_half_width_space_in_a_name_is_widened_to_match():
    """Mixing half- and full-width spaces makes the padding arithmetic lie."""
    assert on._display_name("吉川 悠斗") == "吉川　悠斗"


def test_tomorrow_is_the_next_slate_not_the_next_calendar_day(monkeypatch):
    """The board can open after midnight, and the polling window runs into the
    small hours. Counting from the calendar date would make a run at 01:00 aim
    two slates ahead, at a board that does not exist yet — and the slate that
    had just opened would never go out.
    """
    class _Clock:
        def __init__(self, when):
            self.when = when

        def now(self, tz=None):
            return self.when.astimezone(tz) if tz else self.when

    evening = datetime(2026, 8, 26, 22, 0, tzinfo=JST)
    monkeypatch.setattr(on, "datetime", _Clock(evening))
    assert on._next_day(on.NPB) == "2026-08-27"

    after_midnight = datetime(2026, 8, 27, 1, 0, tzinfo=JST)
    monkeypatch.setattr(on, "datetime", _Clock(after_midnight))
    assert on._next_day(on.NPB) == "2026-08-27"   # still the same slate

    next_evening = datetime(2026, 8, 27, 21, 0, tzinfo=JST)
    monkeypatch.setattr(on, "datetime", _Clock(next_evening))
    assert on._next_day(on.NPB) == "2026-08-28"


def test_the_opening_post_carries_the_forecast():
    from baseball.npb_starters import Slate, Weather

    sky = Slate(starters={}, weather={"巨人": Weather("曇り", temp_c="28")})
    text = on.build_message([_snapshot()], now=NOW, context=sky)
    assert "28℃" in text


def test_the_closing_post_drops_the_forecast():
    """The forecast is what a reader plans around hours ahead. By first pitch
    the sky is out of the window, and repeating it only pushes the line — the
    one thing that has actually moved — further down the message."""
    from baseball.npb_starters import Slate, Weather

    sky = Slate(starters={}, weather={"巨人": Weather("曇り", temp_c="28")})
    text = on.build_message([_snapshot()], now=NOW, context=sky, weather=False)
    assert "28℃" not in text
    assert "曇り" not in text


# The 2026-08-27 楽天 @ オリックス open: PS3838's window sat above the game,
# so no whole number in it could be posted and 7 had to be estimated.
UNSETTLED_TOTALS = [
    {"line": 8.5, "over": 2.42, "under": 1.581},
    {"line": 8.0, "over": 2.25, "under": 1.68},
    {"line": 7.5, "over": 2.04, "under": 1.806},
]


def test_a_slate_the_board_has_not_settled_waits_for_the_next_run(monkeypatch):
    """An opening board is often still filling in, and the job is triggered
    every few minutes — so a game the ladder cannot price yet is worth waiting
    for rather than announcing as an estimate."""
    slate = [_snapshot(game_date="2026-08-25", all_totals=UNSETTLED_TOTALS,
                       start="2026-08-25T18:00:00+09:00", mins_to_start=300)]
    sent = _stub_feed(monkeypatch, slate)
    ledger = _Ledger()

    assert on.run_once(game_date="2026-08-25", phase="open", ledger=ledger) == []
    assert sent == []
    assert ledger.recorded == []   # nothing sent, so a later run still may


def test_the_settled_slate_goes_out_on_the_run_that_finds_it(monkeypatch):
    slate = [_snapshot(game_date="2026-08-25", all_totals=UNSETTLED_TOTALS,
                       start="2026-08-25T18:00:00+09:00", mins_to_start=300)]
    sent = _stub_feed(monkeypatch, slate)
    ledger = _Ledger()

    on.run_once(game_date="2026-08-25", phase="open", ledger=ledger)
    slate[0]["all_totals"] = TOTALS
    on.run_once(game_date="2026-08-25", phase="open", ledger=ledger)

    assert len(sent) == 1
    assert ledger.recorded == [("2026-08-25", "open", 1)]


def test_waiting_stops_once_first_pitch_is_close(monkeypatch):
    """A gap in the post beats no post at all: past this point the board is
    not going to settle in time, and the slate goes out as it stands."""
    slate = [_snapshot(game_date="2026-08-25", all_totals=UNSETTLED_TOTALS,
                       start="2026-08-25T18:00:00+09:00", mins_to_start=5)]
    sent = _stub_feed(monkeypatch, slate)

    on.run_once(game_date="2026-08-25", phase="open", ledger=_Ledger())

    assert len(sent) == 1


def test_a_game_with_no_line_at_all_holds_the_slate(monkeypatch):
    """One lone half line prices nothing — the same unsettled board, further
    from ready, and it must not go out as a row of dashes either."""
    thin = [_snapshot(game_date="2026-08-25", all_spreads=[], all_totals=[],
                      ml_home=None, ml_away=None,
                      start="2026-08-25T18:00:00+09:00", mins_to_start=300)]
    sent = _stub_feed(monkeypatch, thin)

    assert on.run_once(game_date="2026-08-25", phase="open", ledger=_Ledger()) == []
    assert sent == []


def test_a_closing_line_waits_for_the_board_within_its_own_window(monkeypatch):
    """The close already holds until first pitch is near; an unsettled board
    holds it further, but only down to the same floor the open stops at."""
    slate = [_snapshot(game_date="2026-08-25", all_totals=UNSETTLED_TOTALS,
                       start="2026-08-25T18:00:00+09:00", mins_to_start=20)]
    sent = _stub_feed(monkeypatch, slate)

    assert on.run_once(game_date="2026-08-25", phase="close", ledger=_Ledger()) == []
    assert sent == []


def test_a_held_run_does_not_scrape_yahoo_for_the_starters(monkeypatch):
    """Yahoo answers a burst of requests with 500s, and a board that has not
    settled is re-polled every few minutes — so the starters are fetched only
    once there is a post to build them into."""
    from baseball.npb_starters import Slate

    slate = [_snapshot(game_date="2026-08-25", all_totals=UNSETTLED_TOTALS,
                       start="2026-08-25T18:00:00+09:00", mins_to_start=300)]
    _stub_feed(monkeypatch, slate)
    scrapes = []
    monkeypatch.setattr(on, "_starters_for",
                        lambda league, game_date: scrapes.append(game_date)
                        or Slate(starters={}, weather={}))

    on.run_once(game_date="2026-08-25", phase="open", ledger=_Ledger())
    assert scrapes == []

    slate[0]["all_totals"] = TOTALS
    on.run_once(game_date="2026-08-25", phase="open", ledger=_Ledger())
    assert scrapes == ["2026-08-25"]
