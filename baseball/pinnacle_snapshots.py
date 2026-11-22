"""Turn Pinnacle's tick history into rows shaped like the ``盤口`` ledger.

After the BettingIsCool month the PS3838 scraper is the only feed, so the
model has to be trained on what that scraper sees. This replays each event's
history as if the scraper had been polling: at a handful of instants it takes
the board as it stood, then runs it through the scraper's **own** parsing —
``parse_period`` picks the main total / run line, ``incoherence`` refuses
impossible boards — so a backfilled row and a live row are made by one rule.

Instants per (event, period), matching what was agreed for the backtest:

* ``open``    — the period's first update;
* ``interim`` — 12 h, 3 h and 1 h before first pitch (skipped if not yet open);
* ``close``   — 5 minutes before first pitch, the scraper's last poll, not
  the last tick Pinnacle took.

Rows carry ``source = "bettingiscool"``. Team totals have no 盤口 column and
are left in the raw ``odds`` table.
"""

import datetime as dt
import json

from baseball import odds_history
from baseball.cpbl_games import normalize_team as cpbl_team
from baseball.odds_audit import incoherence
from baseball.pinnacle_history import _parse_ts
from baseball.pinnacle_odds import CPBL, NPB, parse_period

SOURCE = "bettingiscool"
LEADS_MIN = (720, 180, 60)
CLOSE_MIN = 5
LEAGUE_SPECS = {"npb": NPB, "cpbl": CPBL}
LEDGER_MARKETS = ("moneyline", "spread", "totals")


def _norm(name: str, league: str) -> str:
    return cpbl_team(name) if league == "cpbl" else odds_history.normalize_team(name)


# --- the board at an instant -------------------------------------------------

def board_at(odds: list[dict], period: int, when: dt.datetime) -> dict:
    """{market: [rows]} for ``period`` as it stood at ``when``.

    An update carries one ``line_id`` for every row it touches; most updates
    re-send the whole period, some only a market or two. So each market is
    read from the latest update that mentions it. A later suspension marker
    (status 2, no market) takes the whole period off, and a market Pinnacle
    pulled (``is_open = 0``) or suspended is not on the board.
    """
    latest: dict[str, tuple] = {}
    suspended_at = None
    for row in odds:
        if row.get("period") != period:
            continue
        stamp = _parse_ts(row["timestamp"])
        if stamp > when:
            continue
        market = row.get("market")
        if market is None:
            if row.get("status") == 2 and (suspended_at is None or stamp > suspended_at):
                suspended_at = stamp
            continue
        key = (stamp, row.get("line_id") or 0)
        if market not in latest or key > latest[market][0]:
            latest[market] = (key, [row])
        elif key == latest[market][0]:
            latest[market][1].append(row)
    board = {}
    for market, ((stamp, _), rows) in latest.items():
        if suspended_at is not None and suspended_at >= stamp:
            continue
        live = [r for r in rows if r.get("status") != 2 and r.get("is_open") != 0]
        if live:
            board[market] = live
    return board


# --- one board -> the scraper's parse ----------------------------------------

def _num(value):
    return None if value in (None, "") else float(value)


def parse_board(period: int, board: dict) -> dict | None:
    """Rebuild PS3838's compact ``[spreads, totals, moneyline]`` block and let
    the scraper parse it, so main-line choice and sign conventions are its own.

    Pinnacle's rows are home-first (``odds1`` = home / over, spread ``line``
    is the home handicap); the compact block is away-first in places, which is
    exactly what ``parse_period`` undoes.
    """
    spreads = [[-_num(r["line"]), _num(r["line"]), str(r["line"]),
                r.get("odds1"), r.get("odds2")]
               for r in board.get("spread", []) if r.get("line") is not None]
    totals = [[str(r["line"]), _num(r["line"]), r.get("odds1"), r.get("odds2")]
              for r in board.get("totals", []) if r.get("line") is not None]
    ml_rows = board.get("moneyline") or []
    ml = ([ml_rows[0].get("odds2"), ml_rows[0].get("odds1"), ml_rows[0].get("odds0") or ""]
          if ml_rows else [])
    return parse_period(str(period), [spreads, totals, ml])


# --- one event -> snapshots --------------------------------------------------

def _instants(first: dt.datetime, start: dt.datetime) -> list[tuple[str, dt.datetime]]:
    out = [("open", first)]
    for lead in LEADS_MIN:
        at = start - dt.timedelta(minutes=lead)
        if at > first:
            out.append(("interim", at))
    close = start - dt.timedelta(minutes=CLOSE_MIN)
    if close >= first:
        out.append(("close", close))
    return out


def event_snapshots(record: dict, league: str) -> list[dict]:
    spec = LEAGUE_SPECS[league]
    fixture, odds = record["fixture"], record["odds"]
    start = _parse_ts(fixture["starts"])
    start_local = start.astimezone(spec.tz)
    base = {
        "event_id": fixture["event_id"],
        "league": fixture.get("league_name", ""),
        "home_team": fixture.get("runner_home", ""),
        "away_team": fixture.get("runner_away", ""),
        "home_norm": _norm(fixture.get("runner_home"), league),
        "away_norm": _norm(fixture.get("runner_away"), league),
        spec.start_column: start_local.isoformat(),
        "game_date": start_local.strftime("%Y-%m-%d"),
        "status": "pregame",
        "source": SOURCE,
    }
    out = []
    for period in sorted({r.get("period") for r in odds if r.get("market") in LEDGER_MARKETS}):
        # Pinnacle sometimes posts a board closed (status 2) seconds before it
        # takes bets; the open is the first price anyone could have bet.
        stamps = [_parse_ts(r["timestamp"]) for r in odds
                  if r.get("period") == period and r.get("market") in LEDGER_MARKETS
                  and r.get("status") != 2 and r.get("is_open") != 0]
        if not stamps:
            continue
        first = min(stamps)
        if first >= start:
            continue
        for kind, at in _instants(first, start):
            parsed = parse_board(period, board_at(odds, period, at))
            if not parsed or incoherence(parsed):
                continue
            out.append({
                **base, **parsed,
                "captured_at": at.astimezone(spec.tz).strftime("%Y-%m-%d %H:%M:%S"),
                "snapshot_type": kind,
                "mins_to_start": round((start - at).total_seconds() / 60),
                "all_totals": json.dumps(parsed["all_totals"], ensure_ascii=False),
                "all_spreads": json.dumps(parsed["all_spreads"], ensure_ascii=False),
            })
    return out
