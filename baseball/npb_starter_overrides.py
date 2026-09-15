"""Who a club actually started, when the box score's first pitcher was not him.

Every reader of a box score here has assumed the same thing: the first pitcher
in a team's table is the starter. That is true of almost every game and wrong
about an opener — 2026-09-15 西武 sent 森脇 亮介 out for one inning and handed
the next seven to 平良 海馬, so the record read "西武's starter went 1 inning,
0 ER" for a game 平良 threw eight innings of one-run ball in.

Nothing in the data distinguishes the two cases. An opener leaving after one
inning and a starter chased out of one look identical in the box score, and
Yahoo registers the opener as 先発 either way — 予告先発 names him too, because
that is who the club announced. The difference is the club's intent, which only
a person knows, so a person says it: one row in the **先發指定** tab naming the
real starter of one game.

    日期         球隊      真正的先發   備註
    2026-09-15   西武      平良 海馬    森脇開局 1 局

The rule that row triggers is the whole mechanism: **everyone ahead of the named
pitcher is folded into his line.** One opener or two, an inning or three — the
name is enough, and nothing has to say how many innings to merge. A game with no
row behaves exactly as it does today, so the tab only ever holds exceptions.

Which games are worth a row is the one thing this can help with: a first pitcher
gone inside an inning with a long outing behind him is the shape of an opener,
and `looks_like_an_opener` collects those so the sweep can ask. It only ever
asks. Acting on the guess is what would quietly delete a real start.
"""

import re

SHEET_NAME = "先發指定"
HEADERS = ["日期", "球隊", "真正的先發", "備註"]

# Clubs the sheets and the box scores spell differently from each other. Only
# the collisions worth having; anything else is filed under what was typed.
TEAM_ALIASES = {"横浜": "DeNA", "橫濱": "DeNA", "ベイスターズ": "DeNA"}

# An opener's shape: gone inside an inning, with someone behind him who went
# long. Three outs is the opener's whole job, and three innings is more than a
# club asks of the first arm out of a bullpen game — between them they separate
# "the plan was one inning" from "the start fell apart". Both are only ever a
# question; see the module docstring.
OPENER_OUTS = 3
RELIEVED_BY_OUTS = 9

_DATE = re.compile(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$")

_cache = None
# Games the sweep noticed and nobody has designated. Kept until they are taken,
# so the question goes out once per run rather than once per box score parsed.
_candidates: dict[tuple[str, str], dict] = {}


def normalise_date(raw) -> str:
    """A hand-typed date as YYYY-MM-DD, or unchanged if it is not one."""
    text = str(raw if raw is not None else "").strip()
    found = _DATE.match(text)
    if not found:
        return text
    year, month, day = (int(part) for part in found.groups())
    return f"{year:04d}-{month:02d}-{day:02d}"


def _fold(name) -> str:
    """One pitcher, one spelling: spaces of either width dropped."""
    return re.sub(r"[\s　]+", "", str(name if name is not None else ""))


def normalise_team(raw) -> str:
    team = _fold(raw)
    return TEAM_ALIASES.get(team, team)


def parse(values: list[list]) -> dict[tuple[str, str], str]:
    """The tab's rows as {(game date, team): the real starter}.

    Anything without all three of a date, a team and a pitcher is skipped: half
    a designation says a game is unusual without saying what to do about it, and
    guessing from half of one is exactly what this exists to avoid.
    """
    found: dict[tuple[str, str], str] = {}
    for row in values:
        cells = [str(cell).strip() for cell in (list(row) + ["", "", ""])[:3]]
        date, team, pitcher = cells
        if date == HEADERS[0] or not (date and team and pitcher):
            continue
        found[(normalise_date(date), normalise_team(team))] = pitcher
    return found


def _read_sheet() -> list[list]:
    """The tab's values, read through npb.py's own Sheets client.

    Imported here rather than at module scope: npb imports this module, and the
    sweep is the only caller that has a spreadsheet to read.
    """
    import npb

    return npb.get_worksheet(SHEET_NAME, npb.SAILU_SPREADSHEET_KEY).get_all_values()


def _designations() -> dict[tuple[str, str], str]:
    """Every designation, read once per run.

    A missing or unreachable tab is not fatal and not worth retrying per game:
    without it every box score reads its first pitcher as the starter, which is
    the record as it already stands.
    """
    global _cache
    if _cache is None:
        try:
            _cache = parse(_read_sheet())
        except Exception as exc:  # WorksheetNotFound, network, credentials
            print(f"[先發指定] unavailable ({exc}); "
                  "every box score reads its first pitcher as the starter")
            _cache = {}
        else:
            print(f"[先發指定] {len(_cache)} designation(s) loaded")
    return _cache


def reset() -> None:
    """Forget the cached read, so the next lookup goes back to the sheet."""
    global _cache
    _cache = None


def designated(game_date: str, team: str) -> str | None:
    """The pitcher a person says actually started, or None for a normal game."""
    return _designations().get((normalise_date(game_date), normalise_team(team)))


def starter_span(names: list[str], designation: str | None) -> int:
    """How many of a team's pitchers the starter's line covers.

    One without a designation, which is the box score read as it always was.
    With one, everyone up to and including the named pitcher — so an opener's
    inning lands on the pitcher the club actually started the game for.

    A name that never took the mound falls back to the first pitcher and says
    so: a typo should cost the correction it meant to make, not the game.
    """
    if not designation:
        return 1
    wanted = _fold(designation)
    folded = [_fold(name) for name in names]
    if wanted in folded:
        return folded.index(wanted) + 1
    # npb.jp writes 平良 where Yahoo writes 平良 海馬, so a bare surname counts —
    # but only after every full name has been tried, or a game with two 田中 in
    # it would answer with whichever pitched first.
    for index, name in enumerate(folded):
        if name and (wanted.startswith(name) or name.startswith(wanted)):
            return index + 1
    print(f"[先發指定] {designation} did not pitch in this game; "
          "the first pitcher is being read as the starter")
    return 1


def looks_like_an_opener(outs: list[int]) -> bool:
    """Whether this team's pitching has the shape worth asking a person about.

    Never a reason to change anything on its own — see the module docstring.
    """
    if len(outs) < 2:
        return False
    return outs[0] <= OPENER_OUTS and outs[1] >= RELIEVED_BY_OUTS


def note_candidate(game_date: str, team: str, names: list[str],
                   outs: list[int]) -> None:
    """Remember a game to ask about, once, however often it is parsed."""
    key = (normalise_date(game_date), normalise_team(team))
    _candidates.setdefault(key, {
        "date": key[0],
        "team": team,
        "opener": names[0] if names else "",
        "opener_outs": outs[0] if outs else 0,
        "starter": names[1] if len(names) > 1 else "",
        "starter_outs": outs[1] if len(outs) > 1 else 0,
    })


def take_candidates() -> list[dict]:
    """Everything noticed since the last time this was asked, and clear it."""
    taken = list(_candidates.values())
    _candidates.clear()
    return taken


def _innings(outs: int) -> str:
    full, rem = divmod(int(outs), 3)
    return str(full) if rem == 0 else f"{full}.{rem}"


def candidate_message(candidates: list[dict]) -> str | None:
    """One note asking about every game noticed this run, or None for a quiet one."""
    if not candidates:
        return None
    lines = ["⚾ 疑似 opener，要指定先發嗎？", ""]
    for game in candidates:
        lines.append(f"{game['date']} {game['team']}")
        lines.append(f"　　{game['opener']} {_innings(game['opener_outs'])} 局"
                     f" → {game['starter']} {_innings(game['starter_outs'])} 局")
    lines.append("")
    lines.append(f"是 opener 就在「{SHEET_NAME}」填一列（日期／球隊／真正的先發），"
                 "先發被打爆就不用理它。")
    return "\n".join(lines)
