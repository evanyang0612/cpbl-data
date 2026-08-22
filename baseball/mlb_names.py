"""Follow MLB when it respells one of its own players.

紀錄 stores a starter's name as MLB published it on the night, and 設定 matches
a pitcher's games against that column. MLB has been adding accents to names for
years -- and in at least one case taking one away -- so 'Jesus Luzardo' sits in
紀錄 while 設定 is filled with 'Jesús Luzardo' from today's schedule, and those
starts drop out of his totals without any sign that they have. Measured across
the two seasons the formulas look back over: 36 spellings adrift, the worst of
them covering 27 games.

The dropdown behind 設定's pitcher cell is itself built from 紀錄, so the two
sides agree only while 紀錄 carries the spelling MLB publishes now.
"""

from __future__ import annotations

import unicodedata
from collections import defaultdict
from typing import Iterable


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


def rename_map(
    seen: Iterable[str], current: Iterable[str]
) -> tuple[dict[str, str], list[str]]:
    """Map each stale spelling in `seen` to the name MLB publishes now.

    A name only maps when exactly one current player normalises to it. Two
    players who share a spelling are reported rather than guessed at, and a
    name no current player resembles is left alone -- he has retired, and
    設定 will never ask for him again.
    """
    by_key: dict[str, set[str]] = defaultdict(set)
    for name in current:
        if name and name.strip():
            by_key[normalise(name)].add(name)

    renames: dict[str, str] = {}
    ambiguous: list[str] = []
    for name in seen:
        if not name or not name.strip():
            continue
        matches = by_key.get(normalise(name), set())
        if len(matches) > 1:
            if name not in ambiguous:
                ambiguous.append(name)
            continue
        if len(matches) == 1:
            current_name = next(iter(matches))
            if current_name != name:
                renames[name] = current_name
    return renames, sorted(ambiguous)
