"""Resolve the CPBL ``GameSno`` for a PS3838 odds event.

A 盤口 row only earns its keep once it joins to the game it priced, and the
CPBL sheets key a game by ``GameSno`` (``賽程`` column B) dated in column C.
PS3838 gives us Chinese team names and a scheduled first pitch, so we pull
CPBL's own schedule for the months the slate touches and match on
(home, away, nearest start).

Both sources spell the clubs the same way today — PS3838 says "樂天桃猿" and
so does CPBL — but ``賽程`` stores the short names ``cpbl.TEAM_MAP`` produces
("樂天"), so every name is folded to those before matching. Folding by
distinctive substring rather than by exact string is also what keeps a
sponsor rename from silently blanking a season of join keys.

The schedule lives behind the same anti-bot front door as the box scores, so
this reuses ``cpbl.fetch_schedule`` — and with it the Decodo proxy the CPBL
scraper already tunnels through on CI.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

TW = timezone(timedelta(hours=8))

# A PS3838 start can drift from the scheduled first pitch (a rain delay, a
# time change). Beyond this gap we treat it as a different game.
MAX_START_DRIFT = timedelta(hours=6)

# Distinctive substring -> the short name 賽程 columns D/F hold. The English
# needles are a safety net: PS3838 serves the board under a locale, and a flip
# to English would otherwise blank every name at once.
TEAM_SUBSTRING_MAP = {
    "樂天": "樂天",              # 樂天桃猿
    "桃猿": "樂天",
    "monkeys": "樂天",
    "統一": "統一7-ELEVEn",      # 統一7-ELEVEn獅
    "7-eleven": "統一7-ELEVEn",
    "7-ELEVEn": "統一7-ELEVEn",
    "lions": "統一7-ELEVEn",
    "中信": "中信兄弟",           # 中信兄弟
    "兄弟": "中信兄弟",
    "brothers": "中信兄弟",
    "味全": "味全",              # 味全龍
    "wei chuan": "味全",
    "dragons": "味全",
    "富邦": "富邦",              # 富邦悍將
    "悍將": "富邦",
    "guardians": "富邦",
    "台鋼": "台鋼",              # 台鋼雄鷹
    "雄鷹": "台鋼",
    "hawks": "台鋼",
}


def normalize_team(name: str | None) -> str:
    """Any CPBL / PS3838 spelling -> the short name the sheets use, or ""."""
    haystack = str(name or "").strip()
    if not haystack:
        return ""
    folded = haystack.lower()
    for needle, short in TEAM_SUBSTRING_MAP.items():
        if needle in haystack or needle.lower() in folded:
            return short
    return ""


def _parse_tw(value: str | None) -> datetime | None:
    """CPBL timestamps are naive local time ("2026-09-11T18:35:00")."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).split(".")[0])
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=TW)


def _date_str(value: str | None) -> str:
    return str(value or "").split("T")[0].replace("/", "-")


class CpblGameIndex:
    """Lookup from (short names, start time) -> GameSno / date / kind code."""

    def __init__(self, games: list[dict[str, Any]]):
        self.games = []
        for game in games:
            start = (_parse_tw(game.get("GameDateTimeS"))
                     or _parse_tw(game.get("PreExeDate")))
            if not start:
                continue
            self.games.append({
                "game_sno": str(game.get("GameSno") or ""),
                "kind_code": str(game.get("KindCode") or ""),
                "game_date": _date_str(game.get("GameDate")),
                "home_norm": normalize_team(game.get("HomeTeamName")),
                "away_norm": normalize_team(game.get("VisitingTeamName")),
                "start": start,
            })

    def find(self, home_norm: str, away_norm: str,
             start: datetime | None) -> dict[str, Any] | None:
        if not home_norm or not away_norm:
            return None
        candidates = [g for g in self.games
                      if g["home_norm"] == home_norm
                      and g["away_norm"] == away_norm]
        if not candidates:
            return None
        if start is None:
            # Without a start time we can only trust an unambiguous pairing.
            return candidates[0] if len(candidates) == 1 else None
        best = min(candidates, key=lambda g: abs(g["start"] - start))
        if abs(best["start"] - start) > MAX_START_DRIFT:
            return None
        return best


def _fetch_schedule(year: int, month: int, kind_code: str, session) -> list[dict]:
    import cpbl

    return cpbl.fetch_schedule(year, month, kind_code, session)


def _session():
    import cpbl

    return cpbl.get_session()


def build_index(starts: list[datetime], *, session=None,
                kind_codes: tuple[str, ...] = ("A",)) -> CpblGameIndex:
    """Index the CPBL schedule for every month ``starts`` falls in.

    CPBL serves a whole month per request, so the slate's own months are all
    we ask for — a snapshot is taken hours before first pitch, never days.
    """
    months = sorted({(s.astimezone(TW).year, s.astimezone(TW).month)
                     for s in starts if s})
    if not months:
        return CpblGameIndex([])
    session = session or _session()
    games: list[dict] = []
    for year, month in months:
        for kind_code in kind_codes:
            games.extend(_fetch_schedule(year, month, kind_code, session) or [])
    return CpblGameIndex(games)
