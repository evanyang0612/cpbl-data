"""Broadcast a slate to Telegram in the local Asian-line format.

Reads the same PS3838 feed the ``盤口`` scraper does, converts each game with
``baseball.asian_lines``, and posts one message per slate. Subscribers are
whoever has joined the channel the bot posts to, so there is no subscriber
list to keep.

A slate is announced twice, and the two are not the same job:

``--phase open`` posts the opening line the evening before. A day's slate only
reaches PS3838's 早盤 board once the previous evening's games have finished,
and not at a fixed hour, so it posts on whichever run first sees the games —
one message for the whole day.

``--phase close`` posts the closing line 30 minutes before first pitch, and is
therefore per start time rather than per day: on a staggered card the 13:00
games have long closed while the 18:00 games are still moving, so each first
pitch gets its own post. Games sharing a start time go out together.

Neither knows when it will be due, so both are built to be triggered
repeatedly — cron-job.org fires them every few minutes and the broadcast ledger
keeps every run after the first quiet, so subscribers get each post once. Being
re-triggered is also what lets a run decline to post: a board that has not
settled every game on the slate is held for the next run rather than announced
with a number the ladder could not carry.

    uv run python -m baseball.odds_notify --league npb --tomorrow
    uv run python -m baseball.odds_notify --league npb --phase close
    uv run python -m baseball.odds_notify --league npb --dry-run
"""

import argparse
from datetime import datetime, timedelta
from functools import cache

from baseball import asian_lines as al
from baseball.pinnacle_odds import LEAGUES, NPB, LeagueSpec, fetch_baseball_events, parse_events

LEAGUE_TITLES = {"npb": "日職", "mlb": "美職"}

# Shown wherever the ladder was too thin to price a line.
MISSING = "—"

# A slate is worth announcing twice: when it opens, and again just before it
# starts. They are the same message built from different snapshots of the same
# board, so everything below is keyed by phase rather than assuming the open.
PHASE_TITLES = {"open": "開盤", "close": "尾盤"}

# Handicaps run from two characters (ＰＫ) to four (１＋８５), and the total
# follows them on the away line. Padding every handicap — including an absent
# one — to the same width keeps the total in a column of its own, instead of
# sliding left into the handicap's place where it reads like a second one.
HANDICAP_SLOT = 4

# The closing line is only the closing line near first pitch. Posting one five
# hours early just repeats the open, so a close waits for the slate to come
# within this many minutes of starting.
CLOSE_WITHIN_MINUTES = 30

# A board that has just opened is often still filling in, and a game it cannot
# price yet is worth waiting for: the job is re-triggered every few minutes and
# the ladder usually settles within the hour. Waiting stops this close to first
# pitch, where a gap in the post beats no post at all.
SETTLE_WITHIN_MINUTES = 10


def _quotes(snapshot: dict) -> tuple[al.Quote | None, al.Quote | None]:
    """Price one game's handicap and total from its full odds ladder."""
    margin = al.margin_curve(
        snapshot.get("all_spreads") or [],
        snapshot.get("ml_home"),
        snapshot.get("ml_away"),
    )
    total = al.total_curve(snapshot.get("all_totals") or [])
    return al.handicap_quote(margin), al.total_quote(total)


def _unsettled(snapshots: list[dict]) -> list[str]:
    """Games the board has not settled a postable line for yet.

    Two things count as unsettled, and neither is a line worth announcing: a
    ladder too thin to price at all, and one whose window sits off the game so
    that the number can only be estimated from outside it. Both mean the board
    is still moving — the 2026-08-27 楽天 @ オリックス open was quoted
    8.5 / 8.0 / 7.5 at 22:00 and had dropped to 7.5 / 7.0 / 6.5 by 01:00, which
    is when its real number appeared.
    """
    unsettled = []
    for snap in _pick_full_game(snapshots).values():
        if any(quote is None or quote.estimated for quote in _quotes(snap)):
            away = snap.get("away_norm") or snap.get("away_team") or ""
            home = snap.get("home_norm") or snap.get("home_team") or ""
            unsettled.append(f"{away} @ {home}")
    return unsettled


def _total_text(quote: al.Quote | None) -> str:
    """Quote 大小 from 大, the way the sheet does; 小 is only its mirror."""
    if quote is None:
        return MISSING
    return fullwidth(f"{quote.line:g}{al.format_water(quote.water)}")


# Half-width digits sit badly beside CJK text, so the sheet writes them full
# width. Only the glyphs that appear in a line are mapped.
_FULLWIDTH = str.maketrans({
    **{str(d): chr(ord("０") + d) for d in range(10)},
    "+": "＋", "-": "－", ".": "．", "P": "Ｐ", "K": "Ｋ",
})


def fullwidth(text: str) -> str:
    return text.translate(_FULLWIDTH)


def _escape(text: str) -> str:
    """Telegram parses the message as HTML, so names have to be safe in it."""
    return (str(text).replace("&", "&amp;")
            .replace("<", "&lt;").replace(">", "&gt;"))


def _display_name(text: str) -> str:
    """Half-width spaces inside a name break the column arithmetic."""
    return text.replace(" ", "　")


def _width(text: str) -> int:
    """Display width in half-widths, so CJK counts double."""
    return sum(2 if ord(char) > 0x2E7F else 1 for char in text)


def _pitcher_name(team: str, starters: dict) -> str:
    starter = (starters or {}).get(team)
    return _display_name(starter.name if starter else team)


def _pitcher_label(team: str, starters: dict, *, link: bool = True) -> str:
    """The starter, linked to their Yahoo page, or the team when unannounced.

    Only the opening post links: it is the one read ahead of time, where
    following a pitcher through is worth a tap. The close is a short note on
    where the line landed.
    """
    starter = (starters or {}).get(team)
    name = _pitcher_name(team, starters)
    if starter is None or not link:
        return _escape(name)
    return f'<a href="{starter.url}">{_escape(name)}</a>'


def _start_time(snapshot: dict, league: LeagueSpec) -> tuple[str, str]:
    """(sort key, display time) for a snapshot's first pitch."""
    raw = snapshot.get(league.start_column) or ""
    try:
        return raw, datetime.fromisoformat(raw).strftime("%H:%M")
    except ValueError:
        return raw, ""


def _pick_full_game(snapshots: list[dict]) -> dict[str, dict]:
    """One row per event, preferring the full-game period over the 1st-5."""
    games: dict[str, dict] = {}
    for snap in snapshots:
        key = str(snap.get("event_id"))
        current = games.get(key)
        if current is None or (current.get("period") != "final"
                               and snap.get("period") == "final"):
            games[key] = snap
    return games


def _slate_label(rows: list[dict], now: datetime) -> str:
    """Date the games, not the broadcast.

    Tomorrow's board only opens once tonight's games have finished, so the
    evening post is about a slate that belongs to the following day.
    """
    dates = sorted({s.get("game_date") for s in rows if s.get("game_date")})
    if not dates:
        return f"{now.month}/{now.day}"
    first = datetime.fromisoformat(dates[0])
    label = f"{first.month}/{first.day}"
    if len(dates) > 1:
        last = datetime.fromisoformat(dates[-1])
        label += f"–{last.month}/{last.day}"
    return label


def build_message(snapshots: list[dict], *, now: datetime,
                  league: LeagueSpec = NPB,
                  game_date: str | None = None,
                  phase_label: str = "開盤",
                  context=None,
                  link: bool = True,
                  weather: bool = True) -> str | None:
    """Render the slate, or ``None`` when there is nothing worth sending.

    ``game_date`` narrows the board to one day: the evening broadcast goes out
    while the board still carries games that have already been played.
    """
    from baseball.npb_starters import Slate

    context = context if context is not None else Slate(starters={}, weather={})
    if game_date:
        snapshots = [s for s in snapshots if s.get("game_date") == game_date]
    games = _pick_full_game(snapshots)
    if not games:
        return None
    rows = sorted(games.values(), key=lambda s: _start_time(s, league)[0])

    # Every pitcher name is padded to the widest on the slate, so the numbers
    # beside them line up down the message. The width has to be measured over
    # the whole slate rather than per game, and on the visible name rather than
    # the markup wrapping it.
    name_slot = max(
        (_width(_pitcher_name(snap.get(f"{side}_norm")
                              or snap.get(f"{side}_team") or "",
                              context.starters))
         for snap in rows for side in ("home", "away")),
        default=0)

    # The header dates the slate and names the phase, and stops there. A clock
    # on it says when the job ran rather than anything about the games, and
    # invites the post to be read as a price still moving.
    title = LEAGUE_TITLES.get(league.key, league.key.upper())
    # Games are already separated by a blank line, so the header takes one too;
    # flush against the first fixture it reads as part of the slate rather than
    # as its title.
    lines = [f"⚾ {title} {_slate_label(rows, now)} {phase_label}", ""]
    for snap in rows:
        handicap, total = _quotes(snap)
        _, clock = _start_time(snap, league)
        away = snap.get("away_norm") or snap.get("away_team") or ""
        home = snap.get("home_norm") or snap.get("home_team") or ""
        lines.append(f"{clock}　{away} @ {home}".strip())
        # The handicap rides on its pitcher's line. In NPB the starter is the
        # single biggest input to the line, so the pitcher is what the number
        # is really about — and putting it there says which side lays by
        # position, instead of repeating a team name the fixture line above
        # already gave. The total and the sky belong to the game, not to a
        # side, so they keep rows of their own.
        marks = {"away": "", "home": ""}
        if handicap is None:
            # Say so rather than leaving a bare line that reads as "no line".
            marks["home"] = MISSING
        else:
            side = "home" if handicap.line == 0 else handicap.side
            marks[side] = fullwidth(handicap.text())
        # The total belongs to the game rather than a side, and the sheet has
        # always carried it on the away line.
        carries_total = total is not None
        for side, team in (("away", away), ("home", home)):
            pad = "　" * ((name_slot - _width(_pitcher_name(team, context.starters))) // 2)
            row = [f"　　{_pitcher_label(team, context.starters, link=link)}{pad}"]
            if marks[side] or (side == "away" and carries_total):
                row.append(marks[side].ljust(HANDICAP_SLOT, "　"))
            if side == "away" and carries_total:
                row.append(fullwidth(_total_text(total)))
            lines.append("　".join(row).rstrip("　"))
        sky = context.weather.get(home) or context.weather.get(away) if weather else None
        if sky is not None:
            lines.append(f"　　{_escape(sky.summary())}")
        lines.append("")
    return "\n".join(lines).rstrip()


# A baseball day is treated as rolling over in the early morning, not at
# midnight. The opening broadcast is polled from the evening into the small
# hours because the board can open late, and counting from the calendar date
# would make a 01:00 run aim a slate too far — at a board not yet open, while
# the one that had just appeared went unsent. Nothing starts near this hour, so
# it can sit anywhere in the small-hours gap.
DAY_ROLLS_AT = 6


def _next_day(league: LeagueSpec) -> str:
    """The slate the evening broadcast is about."""
    now = datetime.now(tz=league.tz) - timedelta(hours=DAY_ROLLS_AT)
    return (now + timedelta(days=1)).strftime("%Y-%m-%d")


class NullLedger:
    """Used for dry runs, where nothing is posted and nothing is remembered."""

    def sent(self, game_date: str, phase: str) -> bool:
        return False

    def record(self, game_date: str, phase: str, games: int) -> None:
        pass


class SheetLedger:
    """Remembers which slates have gone out, so a re-trigger stays quiet.

    A day's board opens at no fixed hour, so the broadcast is triggered
    repeatedly through the evening and posts on whichever run first finds the
    games. Every later run that evening finds the same games — without a record
    of what has already been sent, subscribers get the slate several times.
    """

    SHEET = "推播紀錄"
    HEADERS = ["game_date", "league", "phase", "sent_at", "games"]

    def __init__(self, league: LeagueSpec):
        self.league = league
        self._ws = None

    def worksheet(self):
        if self._ws is None:
            from baseball.sheets import GoogleSheetsClient

            spreadsheet = GoogleSheetsClient().spreadsheet(self.league.spreadsheet_key())
            try:
                self._ws = spreadsheet.worksheet(self.SHEET)
            except Exception:  # gspread WorksheetNotFound
                self._ws = spreadsheet.add_worksheet(
                    title=self.SHEET, rows=500, cols=len(self.HEADERS))
                self._ws.append_row(self.HEADERS, value_input_option="USER_ENTERED")
        return self._ws

    def sent(self, game_date: str, phase: str) -> bool:
        # Keyed on the game date rather than the broadcast date: what must not
        # happen twice is a slate being announced twice, and a board that opens
        # after midnight rolls the broadcast date without rolling the slate.
        return any(row[:3] == [game_date, self.league.key, phase]
                   for row in self.worksheet().get_all_values()[1:])

    def record(self, game_date: str, phase: str, games: int) -> None:
        self.worksheet().append_row(
            [game_date, self.league.key, phase,
             datetime.now(tz=self.league.tz).strftime("%Y-%m-%d %H:%M:%S"), games],
            value_input_option="USER_ENTERED")


def _send(message: str) -> bool:
    """Post to the broadcast channel, using the broadcast bot's own credentials.

    The alerting bot reports scraper failures to a private chat; subscribers
    should never see those, so the two are separate bots. There is deliberately
    no fallback: with the broadcast credentials missing, falling back would post
    the slate into the ops chat and leave the channel silent.
    """
    import os

    import utils

    token = os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_BROADCAST_CHAT_ID")
    if not token or not chat_id:
        print("[notify] TELEGRAM_BROADCAST_BOT_TOKEN / "
              "TELEGRAM_BROADCAST_CHAT_ID are not set; nothing sent")
        return False
    return utils.send_telegram(message, bot_token=token, chat_id=chat_id,
                               parse_mode="HTML")


def _starters_for(league: LeagueSpec, game_date: str | None):
    """Starters and forecasts for the slate, where the league has a source.

    Never fatal: an unreachable Yahoo, or starters not yet announced, costs the
    pitcher names and the sky, not the broadcast.
    """
    from baseball.npb_starters import Slate, fetch_slate

    if league.key != "npb" or not game_date:
        return Slate(starters={}, weather={})
    return fetch_slate(game_date)


def _minutes_to_first_pitch(games: list[dict]) -> int | None:
    """How long until the earliest game in the group starts."""
    leads = []
    for snap in games:
        try:
            leads.append(int(snap["mins_to_start"]))
        except (KeyError, TypeError, ValueError):
            continue
    return min(leads) if leads else None


def _by_first_pitch(slate: list[dict], league: LeagueSpec) -> dict[str, list[dict]]:
    """Split a slate into the groups a closing line is actually about.

    An opening line covers the whole day at once, but a closing line belongs to
    one first pitch: on a staggered card the 13:00 games have closed long
    before the 18:00 games stop moving. Games that start together are still
    announced together, so an all-18:00 card is one post, not six.
    """
    groups: dict[str, list[dict]] = {}
    for snap in slate:
        groups.setdefault(_start_time(snap, league)[1], []).append(snap)
    return groups


def _broadcast(games: list[dict], *, league: LeagueSpec, game_date: str | None,
               phase: str, slot: str, label: str, send: bool, ledger,
               context=None, link: bool = True, weather: bool = True,
               settle_within: int = SETTLE_WITHIN_MINUTES) -> str | None:
    """Post one message and remember it, unless that slot already went out.

    A slate the board has not settled is held rather than sent: nothing is
    posted and nothing is recorded, so the next trigger tries the same slot
    again against a board that has had a few more minutes to fill in.

    ``context`` is a callable rather than a slate so that nothing is scraped
    for a post that is not going out.
    """
    if game_date and ledger.sent(game_date, slot):
        print(f"[notify] {game_date} {slot} already broadcast; nothing to do")
        return None
    unsettled = _unsettled(games)
    lead = _minutes_to_first_pitch(games)
    if unsettled and lead is not None and lead > settle_within:
        print(f"[notify] the board has not settled {', '.join(unsettled)}; "
              f"holding {slot} for a later run")
        return None
    message = build_message(games, now=datetime.now(tz=league.tz),
                            league=league, phase_label=label, context=context(),
                            link=link, weather=weather)
    if message is None:
        return None
    print(message)
    if send:
        if not _send(message):
            # Nothing reached the channel, so nothing is recorded — a later
            # trigger must still be able to deliver this slate.
            return None
        if game_date:
            ledger.record(game_date, slot, len(_pick_full_game(games)))
    return message


def run_once(league: LeagueSpec = NPB, *, send: bool = True,
             game_date: str | None = None, phase: str = "open",
             ledger=None, close_within: int = CLOSE_WITHIN_MINUTES,
             settle_within: int = SETTLE_WITHIN_MINUTES) -> list[str]:
    """Broadcast whatever is due, and return the messages actually sent."""
    # Load .env before anything reads credentials. On CI the environment is
    # already populated from the workflow, but locally the ledger opens Sheets
    # before the sender would otherwise have loaded it.
    from dotenv import load_dotenv

    load_dotenv(override=False)
    ledger = ledger if ledger is not None else (
        SheetLedger(league) if send and game_date else NullLedger())
    title = PHASE_TITLES.get(phase, phase)

    if phase != "close" and game_date and ledger.sent(game_date, phase):
        print(f"[notify] {game_date} {phase} already broadcast; nothing to do")
        return []

    snapshots = parse_events(fetch_baseball_events(), league=league)
    if league.enrich and snapshots:
        league.enrich(snapshots)
    slate = ([s for s in snapshots if s.get("game_date") == game_date]
             if game_date else snapshots)
    if not slate:
        print(f"[notify] {game_date or 'any date'} is not on the board yet")
        return []

    # Asked for only once a post is actually going out, and only once per run:
    # Yahoo answers a burst with 500s, and a held slate is re-polled every few
    # minutes for a message that is not being sent.
    context = cache(lambda: _starters_for(league, game_date))

    if phase != "close":
        sent = _broadcast(slate, league=league, game_date=game_date, phase=phase,
                          slot=phase, label=title, send=send, ledger=ledger,
                          context=context, settle_within=settle_within)
        return [sent] if sent else []

    messages = []
    for clock, games in sorted(_by_first_pitch(slate, league).items()):
        lead = _minutes_to_first_pitch(games)
        if lead is None or lead > close_within:
            print(f"[notify] {clock} is still {lead}m away; holding its close")
            continue
        # The close drops both the links and the forecast: it is a short note
        # on where the line landed, read minutes before first pitch, not the
        # post anyone plans a day around.
        sent = _broadcast(games, league=league, game_date=game_date, phase=phase,
                          slot=f"{phase} {clock}", label=f"{clock} {title}",
                          send=send, ledger=ledger, context=context, link=False,
                          weather=False, settle_within=settle_within)
        if sent:
            messages.append(sent)
    return messages


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Post today's PS3838 lines to Telegram as Asian lines")
    parser.add_argument("--league", default="npb", choices=sorted(LEAGUES),
                        help="which league to broadcast (default: npb)")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the message without sending it")
    parser.add_argument("--date", default=None, metavar="YYYY-MM-DD",
                        help="slate to broadcast (default: today, or the next "
                             "day with --tomorrow)")
    parser.add_argument("--tomorrow", action="store_true",
                        help="broadcast the next day's slate, the evening before")
    parser.add_argument("--phase", default="open", choices=sorted(PHASE_TITLES),
                        help="open = post as soon as the board opens; "
                             "close = hold until first pitch is near")
    parser.add_argument("--close-within", type=int, default=CLOSE_WITHIN_MINUTES,
                        metavar="MINUTES",
                        help="how near first pitch a closing line may post")
    args = parser.parse_args()
    league = LEAGUES[args.league]
    # Always resolve to a concrete date: the ledger is keyed by it, so leaving
    # it open would disable the guard and let a re-trigger post twice.
    game_date = args.date or (_next_day(league) if args.tomorrow
                              else datetime.now(tz=league.tz).strftime("%Y-%m-%d"))
    run_once(league, send=not args.dry_run, game_date=game_date,
             phase=args.phase, close_within=args.close_within)


if __name__ == "__main__":
    main()
