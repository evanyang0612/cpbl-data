"""Show which baseball leagues PS3838 is booking right now.

Written to answer whether PS3838 books CPBL at all. It does — as
**台北 - 職業聯賽** (league id 208753), a name with nothing in it to search
for, which is why the question went unanswered for so long. CPBL odds are now
scraped by ``baseball.pinnacle_odds --league cpbl``; this stays as the probe
for the next league, and for checking what the board is carrying.

The compact feed and the leagues endpoint only list leagues with *currently
open* markets, and baseball lines appear just a few hours before first pitch,
so a league's absence proves nothing on an off day. Run it on a game day,
roughly 2-4 hours before the 18:35 (TW) first pitch:

    uv run python migration/probe_cpbl_odds.py

It prints every baseball league PS3838 is offering right now, flags the ones
that look Taiwanese, and dumps their parsed snapshots. Writes nothing — safe
to run repeatedly.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from baseball.pinnacle_odds import (  # noqa: E402
    BASEBALL_SPORT_ID,
    BASE_URL,
    _build_session,
    fetch_baseball_events,
    parse_events,
)

TW = timezone(timedelta(hours=8))

# Any of these in a league name means we've found it. PS3838 books CPBL as
# "台北 - 職業聯賽", so the league's own name is not among them.
CPBL_HINTS = ("中華職", "中華職業棒球", "CPBL", "Taiwan", "台灣", "臺灣", "台北",
              "臺北")

LEAGUES_PATH = "/sports-service/sv/odds/leagues"


def _looks_like_cpbl(name: str) -> bool:
    return any(hint.lower() in (name or "").lower() for hint in CPBL_HINTS)


def probe_leagues() -> list[tuple[int, str]]:
    """Leagues with open markets, straight from the leagues endpoint."""
    session = _build_session()
    resp = session.get(
        f"{BASE_URL}{LEAGUES_PATH}",
        params={"sp": BASEBALL_SPORT_ID, "lang": "zh_TW", "locale": "zh_TW"},
        timeout=30,
    )
    resp.raise_for_status()
    found = []
    for sport in resp.json() or []:
        if sport[0] != BASEBALL_SPORT_ID:
            continue
        for league in sport[2]:
            found.append((league[0], league[2]))
    return found


def probe_events() -> tuple[dict[tuple[int, str], int], dict]:
    """Leagues present in the events feed with event counts, plus the raw feed."""
    raw = fetch_baseball_events()
    counts: dict[tuple[int, str], int] = {}
    for bucket in ("n", "l"):
        for sport in raw.get(bucket) or []:
            if sport[0] != BASEBALL_SPORT_ID:
                continue
            for league in sport[2]:
                key = (league[0], league[1])
                counts[key] = counts.get(key, 0) + len(league[2])
    return counts, raw


def main() -> None:
    now = datetime.now(tz=TW).strftime("%Y-%m-%d %H:%M:%S")
    proxy = "via proxy" if os.environ.get("DECODO_PROXY_URL") else "direct"
    print(f"[probe] {now} TW ({proxy})")

    print("\n== /odds/leagues (leagues with open markets) ==")
    for league_id, name in probe_leagues():
        mark = "  <-- CPBL?" if _looks_like_cpbl(name) else ""
        print(f"  {league_id:>8}  {name}{mark}")

    print("\n== events feed ==")
    counts, raw = probe_events()
    cpbl_ids = []
    for (league_id, name), count in sorted(counts.items()):
        mark = ""
        if _looks_like_cpbl(name):
            mark = "  <-- CPBL?"
            cpbl_ids.append(league_id)
        print(f"  {league_id:>8}  {name}  events={count}{mark}")

    if not cpbl_ids:
        print("\n[probe] No CPBL league in the feed right now — the board is "
              "closed, or PS3838 renamed it again.")
        return

    print(f"\n[probe] CPBL candidate league id(s): {cpbl_ids}")
    print("[probe] Sample parsed snapshots:")
    for snap in parse_events(raw, include_live=True, all_leagues=True):
        if snap["league_id"] not in cpbl_ids:
            continue
        print("  " + json.dumps(
            {k: v for k, v in snap.items() if k != "start"},
            ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
