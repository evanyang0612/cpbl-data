"""Turn Pinnacle's tick history into rows shaped like the ``盤口`` ledger.

After the BettingIsCool month the PS3838 scraper is the only feed, so the
model has to be trained on what that scraper sees. This replays each event's
history as if the scraper had been polling: at a handful of instants it takes
the board as it stood, then runs it through the scraper's **own** parsing —
``parse_period`` picks the main total / run line, ``incoherence`` refuses
impossible boards — so a backfilled row and a live row are made by one rule.

Per (event, period), from a full tick history (NPB, CPBL):

* ``open``    — the board 30 minutes after the first bettable price. Pinnacle
  opens on low limits and the first price is bought: in 2026 the main total
  had moved within 30 minutes in 46% of games, and our scraper's first
  capture came a median 36 minutes after the open.
* ``interim`` — every whole hour after that until first pitch. Bets go in
  anywhere from the night before to an hour out (the pick ledger), so the
  backtest picks the hour it wants.
* ``close``   — Pinnacle's last pregame board, timestamped when it was set.

From an opening/closing record (MLB, too large for full history) only
``open`` (the raw first price, with the bias above) and ``close`` exist,
each the earliest / latest update of each market, which drops alternate
lines Pinnacle only added later and lines it pulled before the end.

Rows carry ``source = "bettingiscool"``. Team totals have no 盤口 column and
are left in the raw ``odds`` table.
"""

import datetime as dt
import json

from baseball import odds_history
from baseball.cpbl_games import normalize_team as cpbl_team
from baseball.odds_audit import incoherence
from baseball.pinnacle_history import _parse_ts
from baseball.pinnacle_odds import CPBL, MLB, NPB, parse_period

SOURCE = "bettingiscool"
OPEN_SETTLE = dt.timedelta(minutes=30)
LEAGUE_SPECS = {"npb": NPB, "cpbl": CPBL, "mlb": MLB}
LEDGER_MARKETS = ("moneyline", "spread", "totals")


def _norm(name: str, league: str) -> str:
    if league == "cpbl":
        return cpbl_team(name)
    return odds_history.normalize_team(name, LEAGUE_SPECS[league])


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

def _bettable(row: dict) -> bool:
    return (row.get("market") in LEDGER_MARKETS and row.get("status") != 2
            and row.get("is_open") != 0)


def _edge_board(rows: list[dict], period: int, *, latest: bool) -> dict:
    """{market: rows} at each market's first (or last) bettable update."""
    by_market: dict[str, list] = {}
    for row in rows:
        if row.get("period") == period and _bettable(row):
            by_market.setdefault(row["market"], []).append(row)
    board = {}
    for market, mrows in by_market.items():
        pick = (max if latest else min)(_parse_ts(r["timestamp"]) for r in mrows)
        board[market] = [r for r in mrows if _parse_ts(r["timestamp"]) == pick]
    return board


def _boards(record: dict, period: int, start: dt.datetime):
    """[(snapshot_type, board, captured_at or None)]; None = the board's own time."""
    if "opening" in record:
        return [("open", _edge_board(record["opening"], period, latest=False), None),
                ("close", _edge_board(record["closing"], period, latest=True), None)]
    odds = record["odds"]
    stamps = [_parse_ts(r["timestamp"]) for r in odds
              if r.get("period") == period and _bettable(r)]
    if not stamps or min(stamps) >= start:
        return []
    out = []
    at = min(stamps) + OPEN_SETTLE
    if at < start:
        # board_at re-reads every market's latest update, so a partial update
        # (moneyline only) still shows the ladder that stood with it.
        out.append(("open", board_at(odds, period, at), at))
        hour = at.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
        while hour < start:
            out.append(("interim", board_at(odds, period, hour), hour))
            hour += dt.timedelta(hours=1)
    out.append(("close", board_at(odds, period, start - dt.timedelta(seconds=1)), None))
    return out


def _board_time(board: dict) -> dt.datetime | None:
    stamps = [_parse_ts(r["timestamp"]) for rows in board.values() for r in rows]
    return max(stamps) if stamps else None


def event_snapshots(record: dict, league: str) -> list[dict]:
    spec = LEAGUE_SPECS[league]
    fixture = record["fixture"]
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
    rows = record.get("odds") or (record.get("opening", []) + record.get("closing", []))
    out = []
    for period in sorted({r.get("period") for r in rows if r.get("market") in LEDGER_MARKETS}):
        for kind, board, sampled in _boards(record, period, start):
            at = sampled or _board_time(board)
            if at is None or at >= start or not board:
                continue
            parsed = parse_board(period, board)
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
