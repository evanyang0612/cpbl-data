"""Pull Pinnacle's full NPB / CPBL odds history from BettingIsCool's Pinnacle Data API.

The Odds API keeps only Pinnacle's main line and has no CPBL. This archive has
every line Pinnacle posted — the whole spread and total ladder, for the full
game (period 0) and the first five innings (period 1) — with every price
change, from March 2021. That ladder is what Taiwan-line settlement and the
first-five model need; rebuilding it from a main line cannot see the starters.

The catch is the quota: Starter is 500,000 tokens a **day** (1 token per
request + 1 per row), and NPB alone is ~10M rows. So this is built to be
started once and left running:

* every finished event is one gzip file under ``.cache/pinnacle_history/``,
  so a stop at any point loses nothing and a restart skips what is done;
* events go newest first, so if the month runs out it is 2021 that is missing;
* with ``--wait``, hitting the cap sleeps until the UTC-midnight reset
  (08:00 Taiwan time) and carries on.

Nothing is written to Google Sheets: ten million rows do not fit there.
Every 200 saved events are loaded into BigQuery (``baseball.pinnacle_bigquery``),
which is the durable copy; the local files are the staging area.

    uv run python -m baseball.pinnacle_history leagues          # confirm the ids
    uv run python -m baseball.pinnacle_history status           # quota + progress
    uv run python -m baseball.pinnacle_history pull --league npb cpbl --wait
"""

import argparse
import datetime as dt
import gzip
import json
import os
import pathlib
import re
import time

import requests

from baseball.pinnacle_odds import BASEBALL_SPORT_ID, CPBL_LEAGUE_ID, NPB_LEAGUE_ID

API_BASE = os.getenv("PINNACLE_DATA_API_BASE", "https://api.bettingiscool.com").rstrip("/")
API_KEY_ENV = "PINNACLE_DATA_API_KEY"

# The archive's prematch history starts in March 2021 for both leagues.
HISTORY_START = "2021-03-01"
LEAGUE_IDS = {"npb": NPB_LEAGUE_ID, "cpbl": CPBL_LEAGUE_ID}

CACHE_ROOT = pathlib.Path(__file__).resolve().parent.parent / ".cache" / "pinnacle_history"

# An event's full history runs to a few thousand rows and its cost is unknown
# until it comes back, so stop for the day while this much is still left
# rather than let one event straddle the cap.
DEFAULT_RESERVE = 6000
# Sleep a little past the reset so the first call of the day is not refused.
RESET_SLACK_SECONDS = 90
# Network errors are retried; an event that fails this often is left for the next run.
MAX_ATTEMPTS = 3
# Events Pinnacle is still taking bets on, or never played, have nothing to pull.
SKIP_STATUSES = {"scheduled", "cancelled", "deleted"}
# Corners / Bookings style side events; baseball's main fixture has none.
MAIN_UNITS = {None, "", "Regular"}
# The league id also books things that are not a game between two clubs:
# multi-game run specials ("Home Runs (5 Games)"), Pinnacle's test events
# ("Orix Buffaloes TestTeam", "Test 1", "DO NOT BET") and the all-star game
# ("Central League" v "Pacific League"). ~330 of NPB's 5,928 fixtures, each a
# full-price pull. Doubleheaders ("G1 Hanshin Tigers") are real and kept.
NOT_A_GAME = re.compile(
    r"test|do not bet|\bruns\b|\bgames?\)|^\s*(central|pacific)( league)?\s*$|^\s*\d+\s*$",
    re.IGNORECASE)


def is_game(fixture: dict) -> bool:
    return not any(NOT_A_GAME.search(str(fixture.get(side) or ""))
                   for side in ("runner_home", "runner_away"))


class QuotaExhausted(Exception):
    def __init__(self, retry_after: int | None):
        super().__init__(f"daily token cap reached (retry after {retry_after}s)")
        self.retry_after = retry_after


class Client:
    """Thin wrapper that keeps the quota headers of the last response."""

    def __init__(self, api_key: str, *, session=None, base: str = API_BASE,
                 timeout: float = 60):
        self.api_key = api_key
        self.session = session or requests.Session()
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.remaining: int | None = None
        self.last_headers: dict = {}

    def get(self, path: str, **params):
        resp = self.session.get(f"{self.base}{path}", params=params,
                                headers={"X-API-Key": self.api_key},
                                timeout=self.timeout)
        headers = resp.headers or {}
        self.last_headers = headers
        if resp.status_code == 429:
            retry = headers.get("Retry-After")
            raise QuotaExhausted(int(retry) if retry else None)
        resp.raise_for_status()
        remaining = headers.get("X-Quota-Remaining")
        if remaining is not None:
            self.remaining = int(remaining)
        return resp.json()

    @property
    def truncated(self) -> bool:
        return str(self.last_headers.get("X-Truncated", "")).lower() == "true"


# --- fixtures ---------------------------------------------------------------

def _day(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def month_windows(start: str, end: str) -> list[tuple[str, str]]:
    """Calendar-month slices of [start, end], both ends inclusive."""
    first, last = _day(start), _day(end)
    out = []
    cur = first
    while cur <= last:
        next_month = (cur.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
        stop = min(next_month - dt.timedelta(days=1), last)
        out.append((cur.isoformat(), stop.isoformat()))
        cur = stop + dt.timedelta(days=1)
    return out


def _fixtures_window(client: Client, league_id: int, start: str, end: str) -> list[dict]:
    rows = client.get("/api/fixtures", league_id=league_id, starts_from=start,
                      starts_to=end, limit=1000, include_superseded=0)
    first, last = _day(start), _day(end)
    if not client.truncated or first == last:
        if client.truncated:
            print(f"[pinnacle_history] {start} 單日超過 1000 場，可能有遺漏")
        return rows
    mid = first + (last - first) // 2
    return (_fixtures_window(client, league_id, start, mid.isoformat())
            + _fixtures_window(client, league_id,
                               (mid + dt.timedelta(days=1)).isoformat(), end))


def list_fixtures(client: Client, league_id: int, start: str, end: str) -> list[dict]:
    """Every prematch fixture of the league in the range, de-duplicated."""
    seen: dict = {}
    for lo, hi in month_windows(start, end):
        for row in _fixtures_window(client, league_id, lo, hi):
            seen[row["event_id"]] = row
    return sorted(seen.values(), key=lambda r: r["starts"])


# --- the event cache --------------------------------------------------------

def _event_path(events_dir, event_id) -> pathlib.Path:
    return pathlib.Path(events_dir) / f"{event_id}.json.gz"


def save_event(events_dir, record: dict) -> pathlib.Path:
    """Write atomically, so a killed run never leaves a half file that looks done."""
    events_dir = pathlib.Path(events_dir)
    events_dir.mkdir(parents=True, exist_ok=True)
    path = _event_path(events_dir, record["fixture"]["event_id"])
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(record, fh, ensure_ascii=False)
    tmp.replace(path)
    return path


def done_ids(events_dir) -> set[int]:
    events_dir = pathlib.Path(events_dir)
    if not events_dir.is_dir():
        return set()
    return {int(p.name.split(".", 1)[0]) for p in events_dir.glob("*.json.gz")}


def pending(fixtures: list[dict], events_dir, now: dt.datetime | None = None) -> list[dict]:
    """Fixtures still to pull: finished, main-market, not cached — newest first."""
    now = now or dt.datetime.now(dt.timezone.utc)
    done = done_ids(events_dir)
    todo = [f for f in fixtures
            if f["event_id"] not in done
            and f.get("event_status") not in SKIP_STATUSES
            and f.get("resulting_unit") in MAIN_UNITS
            and is_game(f)
            and _parse_ts(f["starts"]) < now]
    return sorted(todo, key=lambda f: f["starts"], reverse=True)


# --- one event --------------------------------------------------------------

def _parse_ts(value: str) -> dt.datetime:
    stamp = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=dt.timezone.utc)


def _row_key(row: dict) -> str:
    return json.dumps(row, sort_keys=True)


def fetch_event(client: Client, fixture: dict) -> dict:
    """Full odds history (every period, market and alternate line) plus results.

    If a history comes back truncated, page on with ``since`` set one second
    before the last timestamp: ``since`` is exclusive, and every market of a
    period shares a timestamp, so starting exactly at the last one would drop
    whatever fell past the cut at that instant. The overlap is de-duplicated.
    """
    event_id = fixture["event_id"]
    rows = client.get("/api/odds", event_id=event_id, full_history=1)
    seen = {_row_key(r) for r in rows}
    while client.truncated and rows:
        last = max(_parse_ts(r["timestamp"]) for r in rows)
        since = (last - dt.timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        page = client.get("/api/odds", event_id=event_id, full_history=1, since=since)
        fresh = [r for r in page if _row_key(r) not in seen]
        if not fresh:
            break
        seen.update(_row_key(r) for r in fresh)
        rows.extend(fresh)
    results = client.get("/api/results", event_id=event_id)
    return {"fixture": fixture, "odds": rows, "results": results,
            "fetched_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


# --- the run loop -----------------------------------------------------------

def seconds_to_reset(now: dt.datetime | None = None) -> int:
    now = now or dt.datetime.now(dt.timezone.utc)
    midnight = (now + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0,
                                                    microsecond=0)
    return int((midnight - now).total_seconds())


def pull(client: Client, todo: list[dict], events_dir, *, wait: bool = False,
         reserve: int = DEFAULT_RESERVE, pause: float = 0.0, sleep=time.sleep) -> dict:
    """Fetch and save each fixture in order until done, or until the day's cap.

    With ``wait`` the loop sleeps through the reset instead of returning, so a
    single command runs the whole backfill over as many days as it takes.
    """
    saved, i, attempts, failed = 0, 0, 0, []
    while i < len(todo):
        if client.remaining is not None and client.remaining < reserve:
            if not wait:
                break
            delay = seconds_to_reset() + RESET_SLACK_SECONDS
            print(f"[pinnacle_history] 剩 {client.remaining} tokens，"
                  f"睡 {delay // 60} 分鐘等 UTC 午夜重置")
            sleep(delay)
            client.remaining = None
            continue
        fixture = todo[i]
        try:
            record = fetch_event(client, fixture)
        except QuotaExhausted as exc:
            if not wait:
                break
            delay = (exc.retry_after or seconds_to_reset()) + RESET_SLACK_SECONDS
            print(f"[pinnacle_history] 今日額度用完，睡 {delay // 60} 分鐘")
            sleep(delay)
            client.remaining = None
            continue
        except requests.RequestException as exc:
            attempts += 1
            if attempts >= MAX_ATTEMPTS:
                # Not cached, so the next run tries it again.
                print(f"[pinnacle_history] {fixture['event_id']} 連續失敗 "
                      f"{attempts} 次（{exc}），先跳過")
                failed.append(fixture["event_id"])
                attempts, i = 0, i + 1
                continue
            print(f"[pinnacle_history] {fixture['event_id']} 失敗（{exc}），30 秒後重試")
            sleep(30)
            continue
        save_event(events_dir, record)
        saved += 1
        attempts, i = 0, i + 1
        print(f"[pinnacle_history] {fixture['starts'][:10]} "
              f"{fixture.get('runner_away')} @ {fixture.get('runner_home')} "
              f"-> {len(record['odds'])} 列（剩 {client.remaining} tokens，"
              f"還有 {len(todo) - i} 場）")
        if pause:
            sleep(pause)
    return {"saved": saved, "left": len(todo) - i, "failed": failed}


# --- CLI --------------------------------------------------------------------

def _league_dir(league: str) -> pathlib.Path:
    return CACHE_ROOT / league


def load_fixtures(client: Client, league: str, start: str, end: str,
                  refresh: bool = False) -> list[dict]:
    """The league's fixture list, cached so a restart does not pay for it again."""
    path = _league_dir(league) / "fixtures.json"
    # `--end` defaults to today, so matching on it would re-buy the list every
    # day of the backfill; the start is what decides whether it is the same pull.
    if path.exists() and not refresh:
        cached = json.loads(path.read_text())
        if cached.get("start") == start:
            return cached["fixtures"]
    fixtures = list_fixtures(client, LEAGUE_IDS[league], start, end)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"start": start, "end": end, "fixtures": fixtures},
                               ensure_ascii=False))
    print(f"[pinnacle_history] {league.upper()} 共 {len(fixtures)} 場 fixture")
    return fixtures


def main() -> None:
    from dotenv import load_dotenv
    load_dotenv(CACHE_ROOT.parent.parent / ".env")

    parser = argparse.ArgumentParser(
        description="Pull Pinnacle's NPB/CPBL odds history from BettingIsCool")
    parser.add_argument("command", choices=["leagues", "status", "pull"])
    parser.add_argument("--league", nargs="+", default=["npb", "cpbl"],
                        choices=sorted(LEAGUE_IDS),
                        help="pulled in this order (default: npb cpbl)")
    parser.add_argument("--start", default=HISTORY_START)
    parser.add_argument("--end", default=dt.date.today().isoformat())
    parser.add_argument("--wait", action="store_true",
                        help="sleep through the daily reset instead of stopping")
    parser.add_argument("--refresh-fixtures", action="store_true")
    parser.add_argument("--reserve", type=int, default=DEFAULT_RESERVE)
    parser.add_argument("--no-bigquery", action="store_true",
                        help="keep the pull local; sync later with pinnacle_bigquery")
    args = parser.parse_args()

    api_key = os.getenv(API_KEY_ENV)
    if not api_key:
        parser.error(f"{API_KEY_ENV} is not set")
    client = Client(api_key)

    if args.command == "leagues":
        for row in client.get("/api/leagues", sport_id=BASEBALL_SPORT_ID):
            mark = " <-" if row.get("league_id") in LEAGUE_IDS.values() else ""
            print(f"{row.get('league_id'):>8}  {row.get('league_name')}{mark}")
        return

    if args.command == "status":
        print(json.dumps(client.get("/api/usage"), ensure_ascii=False, indent=2))
        for league in args.league:
            events = _league_dir(league) / "events"
            fixtures_path = _league_dir(league) / "fixtures.json"
            total = (len(json.loads(fixtures_path.read_text())["fixtures"])
                     if fixtures_path.exists() else "?")
            print(f"{league.upper()}: 已抓 {len(done_ids(events))} / fixture {total}")
        return

    store = None if args.no_bigquery else _bigquery_store()
    for league in args.league:
        fixtures = load_fixtures(client, league, args.start, args.end,
                                 refresh=args.refresh_fixtures)
        events = _league_dir(league) / "events"
        todo = pending(fixtures, events)
        print(f"[pinnacle_history] {league.upper()} 待抓 {len(todo)} 場")
        _sync(store, events, league)        # whatever an earlier run left unsynced
        for i in range(0, max(len(todo), 1), SYNC_EVERY):
            chunk = todo[i:i + SYNC_EVERY]
            stats = pull(client, chunk, events, wait=args.wait, reserve=args.reserve)
            _sync(store, events, league)
            if stats["failed"]:
                print(f"[pinnacle_history] {league.upper()} 失敗跳過：{stats['failed']}"
                      "（下次執行會再試）")
            if stats["left"]:
                print(f"[pinnacle_history] 今日額度到此，{league.upper()} 還剩 "
                      f"{len(todo) - i - len(chunk) + stats['left']} 場；"
                      "明天再跑同一個指令會接著抓")
                return
    print("[pinnacle_history] 全部抓完")


# Push to BigQuery every this many saved events, so at most this much sits
# only on the laptop.
SYNC_EVERY = 200


def _bigquery_store():
    """BigQuery is the durable copy, but a broken connection must not stop a pull
    that is spending a paid daily quota: warn, keep the local files, sync later."""
    from baseball.pinnacle_bigquery import BigQueryStore
    try:
        return BigQueryStore()
    except Exception as exc:  # noqa: BLE001 — any failure here is just "no BigQuery"
        print(f"[pinnacle_history] ⚠️ 連不上 BigQuery（{exc}）；先只存本機，"
              "之後用 `python -m baseball.pinnacle_bigquery sync` 補傳")
        return None


def _sync(store, events, league) -> None:
    if store is None:
        return
    from baseball.pinnacle_bigquery import sync, sync_snapshots
    try:
        sync(events, league, store)
        sync_snapshots(events, league, store)
    except Exception as exc:  # noqa: BLE001
        print(f"[pinnacle_history] ⚠️ BigQuery 上傳失敗（{exc}）；本機檔案都在，下次會重傳")


if __name__ == "__main__":
    main()
