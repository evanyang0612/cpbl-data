"""The team code 紀錄 stores for each MLB franchise.

MLB Stats API changed the Athletics' abbreviation from OAK to ATH for the 2025
season, when the club dropped "Oakland". 紀錄 holds a decade of rows keyed on OAK, and
every sheet that aggregates by team label (MLB勝敗表, 對戰 (n), 五局(左右投)) matches on
that label, so a mid-history code change silently splits a franchise in two. One code
per franchise is kept on the way in instead.
"""

TEAM_CODE_ALIASES = {"ATH": "OAK"}


def canonical_team_code(code: str) -> str:
    """The single code 紀錄 stores for a franchise, whatever the API calls it today."""
    return TEAM_CODE_ALIASES.get(code, code)
