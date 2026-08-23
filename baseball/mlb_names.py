"""Follow MLB when it renames one of its own players.

紀錄 stores a starter's name as MLB published it on the night, and 設定 matches
a pitcher's games against that column, so the two only agree while 紀錄 carries
the spelling MLB publishes now. Two things break that:

Accents. MLB has spent years adding them to its own players -- and in one case
taking one away -- so 'Jesus Luzardo' sits in 紀錄 while 設定 is filled with
'Jesús Luzardo' from today's schedule. Across the two seasons the formulas look
back over, 36 spellings were adrift, the worst covering 27 games.

Shortened first names. Thornton went from 'Zach' to 'Zac' mid-season while his
legal first name stayed 'Zachary'. Folding accents cannot see that, and
comparing a row against its game feed only reaches games inside the revision
window -- his two starts were 41 and 57 days old when they were found.

The dropdown behind 設定's pitcher cell is itself built from 紀錄, so keeping
this column current keeps both sides of the match honest at once.
"""

from __future__ import annotations

import unicodedata
from collections import defaultdict
from typing import Any, Iterable

# Below this, a first name is an initial rather than a name, and prefix
# matching would happily fold 'Ja Smith' into any James or Jason on the roster.
MIN_PREFIX_LENGTH = 3


def normalise(name: str) -> str:
    """A spelling-insensitive key: accents and case dropped, nothing else.

    Punctuation and word breaks are kept, so 'Luis L. Ortiz' stays distinct
    from 'Luis Ortiz' -- those are two people, not two spellings.
    """
    decomposed = unicodedata.normalize("NFD", name.strip())
    without_accents = "".join(
        char for char in decomposed if unicodedata.category(char) != "Mn"
    )
    return " ".join(without_accents.lower().split())


def _prefix_compatible(one: str, other: str) -> bool:
    if len(one) < MIN_PREFIX_LENGTH or len(other) < MIN_PREFIX_LENGTH:
        return False
    return one.startswith(other) or other.startswith(one)


def rename_map(
    seen: Iterable[str], current: Iterable[dict[str, Any]]
) -> tuple[dict[str, str], list[str]]:
    """Map each stale spelling in `seen` to the name MLB publishes now.

    A name that is already one a current player uses is left alone -- there is
    nothing to decide, even when someone else shares the spelling.

    Otherwise the same spelling, accents folded, identifies the player. Failing
    that, a two-word name whose surname matches and whose first name is a
    prefix of (or prefixed by) the player's legal or preferred first name does:
    that is what carries 'Zach Thornton' to 'Zac Thornton' by way of 'Zachary'.
    A middle initial takes the name out of prefix matching entirely, because
    MLB uses one to tell two Luis Ortizes apart.

    Either way the answer has to be a single player. Two candidates are
    reported rather than guessed at, and none at all means he has retired --
    設定 will never ask for him, so his rows stay as they are.
    """
    exact: set[str] = set()
    by_spelling: dict[str, set[str]] = defaultdict(set)
    by_surname: dict[str, list[tuple[str, tuple[str, str]]]] = defaultdict(list)
    for player in current:
        full = player.get("fullName") or ""
        if not full.strip():
            continue
        exact.add(full)
        by_spelling[normalise(full)].add(full)
        surname = normalise(player.get("lastName") or full.split()[-1])
        first = normalise(player.get("firstName") or full.split()[0])
        used = normalise(player.get("useName") or first)
        by_surname[surname].append((full, (first, used)))

    renames: dict[str, str] = {}
    ambiguous: list[str] = []
    for name in seen:
        if not name or not name.strip() or name in exact:
            continue

        matches = by_spelling.get(normalise(name), set())
        if not matches:
            matches = _prefix_matches(name, by_surname)
        if len(matches) > 1:
            if name not in ambiguous:
                ambiguous.append(name)
            continue
        if len(matches) == 1:
            current_name = next(iter(matches))
            if current_name != name:
                renames[name] = current_name
    return renames, sorted(ambiguous)


def _prefix_matches(
    name: str, by_surname: dict[str, list[tuple[str, tuple[str, str]]]]
) -> set[str]:
    parts = normalise(name).split()
    if len(parts) != 2:
        return set()
    first, surname = parts
    return {
        full
        for full, forms in by_surname.get(surname, [])
        if any(_prefix_compatible(first, form) for form in forms)
    }


def name_corrections(
    entries: Iterable[tuple[int, int, str, int | None]],
    published: dict[int, str],
) -> dict[int, dict[int, str]]:
    """Cells whose stored name is not what MLB publishes for that player id.

    `entries` are (row, column, stored name, player id). With an id in hand
    nothing has to be inferred from spelling, which is what lets this reach
    the cases rename_map cannot: 'Louie Varland' is not a prefix of 'Louis',
    and 'Luis L. Ortiz' carries a middle initial that has to be respected
    because MLB uses one to tell two players apart elsewhere.

    An id MLB no longer lists belongs to someone who has retired, and a row
    with no id at all is left to rename_map.
    """
    corrections: dict[int, dict[int, str]] = {}
    for row, column, stored, player_id in entries:
        if player_id is None:
            continue
        current = published.get(player_id)
        if current is None or current == str(stored).strip():
            continue
        corrections.setdefault(row, {})[column] = current
    return corrections


def shared_names(
    player_ids: Iterable[int], published: dict[int, str]
) -> dict[str, list[int]]:
    """Names that more than one of these players is published under.

    No spelling rule can survive this: the two are not two spellings of one
    pitcher but two pitchers MLB calls the same thing. Worth being told about
    the day a second one of a pair starts a game, rather than discovering it
    in a total that is quietly wrong.
    """
    ids_by_name: dict[str, set[int]] = defaultdict(set)
    for player_id in player_ids:
        name = published.get(player_id)
        if name:
            ids_by_name[name].add(player_id)
    return {
        name: sorted(ids) for name, ids in ids_by_name.items() if len(ids) > 1
    }
