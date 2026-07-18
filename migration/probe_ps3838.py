"""One-off probe for the PS3838 / Pinnacle betting API.

Verifies credentials and dumps the exact shapes we need before building the
real odds pipeline:

  1. Baseball sportId (resolved by name, not hard-coded)
  2. NPB leagueId(s)
  3. A sample of live/upcoming NPB fixtures
  4. The odds payload for those fixtures (periods 0 = full game, 1 = 1st 5 innings)

Usage:
    # add PS3838_USERNAME / PS3838_PASSWORD to .env first
    uv run python migration/probe_ps3838.py

Nothing is written anywhere; it only prints. Safe to run repeatedly.
"""

import base64
import json
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = os.getenv("PS3838_API_BASE", "https://api.ps3838.com").rstrip("/")
USERNAME = os.getenv("PS3838_USERNAME")
PASSWORD = os.getenv("PS3838_PASSWORD")


def _auth_header() -> dict:
    if not USERNAME or not PASSWORD:
        sys.exit("Missing PS3838_USERNAME / PS3838_PASSWORD in environment (.env)")
    token = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
    }


def _get(path: str, params: dict | None = None) -> dict:
    url = f"{API_BASE}{path}"
    resp = requests.get(url, headers=_auth_header(), params=params, timeout=30)
    print(f"GET {resp.url} -> {resp.status_code}")
    resp.raise_for_status()
    return resp.json()


def _find_baseball_sport(sports: dict) -> dict | None:
    for sport in sports.get("sports", []):
        if "baseball" in str(sport.get("name", "")).lower():
            return sport
    return None


def _find_npb_leagues(leagues: dict) -> list[dict]:
    hits = []
    for league in leagues.get("leagues", []):
        name = str(league.get("name", "")).lower()
        if "npb" in name or "japan" in name or "japanese" in name or "nippon" in name:
            hits.append(league)
    return hits


def main() -> None:
    # 1. balance sanity-check (confirms API access is actually enabled)
    try:
        balance = _get("/v1/client/balance")
        print("client balance:", json.dumps(balance, ensure_ascii=False))
    except Exception as exc:  # noqa: BLE001
        print(f"(balance check failed, continuing) {exc}")

    # 2. resolve baseball sportId
    sports = _get("/v1/sports")
    baseball = _find_baseball_sport(sports)
    if not baseball:
        print("Could not find Baseball in /v1/sports. Full list:")
        print(json.dumps(sports, ensure_ascii=False, indent=2))
        return
    sport_id = baseball["id"]
    print(f"\nBaseball sportId = {sport_id} (hasOfferings={baseball.get('hasOfferings')})")

    # 3. resolve NPB leagues
    leagues = _get("/v1/leagues", {"sportId": sport_id})
    npb = _find_npb_leagues(leagues)
    print("\nCandidate NPB leagues:")
    for lg in npb:
        print(f"  id={lg.get('id')}  name={lg.get('name')}")
    if not npb:
        print("  (none matched by name; dumping all baseball leagues for inspection)")
        for lg in leagues.get("leagues", []):
            print(f"  id={lg.get('id')}  name={lg.get('name')}")
        return

    league_ids = ",".join(str(lg["id"]) for lg in npb)

    # 4. fixtures for those leagues
    fixtures = _get("/v3/fixtures", {"sportId": sport_id, "leagueIds": league_ids})
    events = []
    for lg in fixtures.get("league", []):
        events.extend(lg.get("events", []))
    print(f"\nfixtures 'last' cursor = {fixtures.get('last')}; events = {len(events)}")
    for ev in events[:8]:
        print(
            f"  event {ev.get('id')}  {ev.get('starts')}  "
            f"{ev.get('away')} @ {ev.get('home')}  status={ev.get('liveStatus')}"
        )

    # 5. odds payload (decimal) — the shape we'll persist
    odds = _get(
        "/v3/odds",
        {"sportId": sport_id, "leagueIds": league_ids, "oddsFormat": "Decimal"},
    )
    print(f"\nodds 'last' cursor = {odds.get('last')}")
    sample = None
    for lg in odds.get("leagues", []):
        for ev in lg.get("events", []):
            sample = ev
            break
        if sample:
            break
    if sample:
        print("\nSample event odds (periods -> number 0=game, 1=1st 5 innings):")
        print(json.dumps(sample, ensure_ascii=False, indent=2))
    else:
        print("\nNo events with odds right now (off-hours / no upcoming NPB games).")


if __name__ == "__main__":
    main()
