"""Resolve MLB Stats API ``gamePk`` for a PS3838 odds event.

The odds sheet is only useful if a snapshot row can be joined to the ``紀錄``
worksheet, which keys games by ``gamePk`` (column B) and dates them by
``officialDate``. PS3838 gives us English team names and a scheduled start
timestamp, so we pull the MLB schedule for the same window and match on
(home nickname, away nickname, closest start time).

Matching is by team *alias* rather than exact name, because the two sources
spell clubs differently: PS3838 says "Arizona Diamondbacks" where the API's
``teamName`` is "D-backs", and PS3838 keeps city prefixes the API may drop
("Oakland Athletics" vs "Athletics"). Aliases are the API's ``name`` /
``clubName`` / ``teamName``, all of which are unique across the 30 clubs; city
names are deliberately excluded since New York and Los Angeles host two clubs
each.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

MLB_API = os.getenv("MLB_STATS_API", "https://statsapi.mlb.com/api").rstrip("/")
REQUEST_TIMEOUT = (10, 60)

# A PS3838 start time can drift from the scheduled first pitch (rain delay,
# time change). Beyond this gap we treat it as a different game.
MAX_START_DRIFT = timedelta(hours=6)


def _schedule(start_date: str, end_date: str,
              *, session: requests.Session | None = None) -> list[dict[str, Any]]:
    getter = session or requests
    resp = getter.get(
        f"{MLB_API}/v1/schedule",
        params={
            "sportId": 1,
            "startDate": start_date,
            "endDate": end_date,
            "hydrate": "team",
        },
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    games: list[dict[str, Any]] = []
    for day in resp.json().get("dates", []):
        games.extend(day.get("games", []))
    return games


def _aliases(team: dict[str, Any]) -> tuple[str, ...]:
    seen = []
    for key in ("name", "clubName", "teamName"):
        value = str(team.get(key) or "").strip().lower()
        if value and value not in seen:
            seen.append(value)
    return tuple(seen)


def _matches(ps_name: str, aliases: tuple[str, ...]) -> bool:
    haystack = (ps_name or "").strip().lower()
    return bool(haystack) and any(alias in haystack for alias in aliases)


class MlbGameIndex:
    """Lookup from (PS3838 English names, start time) -> gamePk / officialDate."""

    def __init__(self, games: list[dict[str, Any]]):
        self.games = []
        for game in games:
            teams = game.get("teams", {})
            home = teams.get("home", {}).get("team", {})
            away = teams.get("away", {}).get("team", {})
            start = _parse_utc(game.get("gameDate"))
            if not start:
                continue
            self.games.append({
                "game_pk": game.get("gamePk"),
                "official_date": game.get("officialDate", ""),
                "home_aliases": _aliases(home),
                "away_aliases": _aliases(away),
                "home_abbr": home.get("abbreviation", ""),
                "away_abbr": away.get("abbreviation", ""),
                "start": start,
            })

    def find(self, home_name: str, away_name: str,
             start: datetime | None) -> dict[str, Any] | None:
        candidates = [
            g for g in self.games
            if _matches(home_name, g["home_aliases"])
            and _matches(away_name, g["away_aliases"])
        ]
        if not candidates:
            return None
        if start is None:
            # Without a start time we can only trust an unambiguous pairing.
            return candidates[0] if len(candidates) == 1 else None
        best = min(candidates, key=lambda g: abs(g["start"] - start))
        if abs(best["start"] - start) > MAX_START_DRIFT:
            return None
        return best


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def build_index(starts: list[datetime], *,
                session: requests.Session | None = None) -> MlbGameIndex:
    """Index the MLB schedule covering ``starts`` (±1 day for date-line slop)."""
    utc_dates = [s.astimezone(timezone.utc).date() for s in starts if s]
    if not utc_dates:
        return MlbGameIndex([])
    start_date = (min(utc_dates) - timedelta(days=1)).isoformat()
    end_date = (max(utc_dates) + timedelta(days=1)).isoformat()
    return MlbGameIndex(_schedule(start_date, end_date, session=session))
