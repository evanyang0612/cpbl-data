"""Comparator for the weekly NPB historical audit.

The daily scrapers are append-only: once a game lands in 賽錄 / 分析表紀錄 it is
never looked at again. NPB does publish 公式記録の訂正 after the fact, and a
scrape can also have failed or parsed wrong on the night. This module holds the
pure logic for re-deriving a game's rows and comparing them against what the
sheets already hold; `migration/audit_npb_history.py` drives the scraping and
the writes.

Nothing here touches the network or Google Sheets, so the comparison rules stay
testable on their own.
"""

# 分析表紀錄 / 彙資 / 資料更新 are all 83 columns wide, column A being a
# sequence number that carries no data.
ANALYSIS_WIDTH = 83
FIRST_DATA_INDEX = 1

# 賽錄 raw columns are B:AY; AZ onward is `sailu_formula_row` and must never be
# compared or overwritten.
SAILU_LAST_RAW_INDEX = 50
SAILU_GAME_ID_INDEX = 1
SAILU_DATE_INDEX = 40

# Final score (J/K) plus each side's first five innings (O:S / Y:AC). A change
# here can flip an already-settled row in 預測紀錄 and every running balance
# after it, so these are reported but never auto-applied.
SCORE_SENSITIVE_ANALYSIS_COLUMNS = (
    {9, 10} | set(range(14, 19)) | set(range(24, 29))
)


def column_letter(index: int) -> str:
    """A1 column letter for a 0-based column index."""
    letters = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _normalize(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def cells_equal(sheet_value, fresh_value) -> bool:
    """Compare one cell the way the sheet would read it.

    Numbers are compared by value so `0` and `"0"` agree, but a blank never
    equals a zero: 分析表紀錄 writes "" for an inning that was never batted and
    0 for a scoreless one.
    """
    left = _normalize(sheet_value)
    right = _normalize(fresh_value)
    if left == right:
        return True
    if not left or not right:
        return False
    try:
        return float(left) == float(right)
    except ValueError:
        return False


def diff_row(
    sheet_row: list,
    fresh_row: list,
    *,
    first_index: int = FIRST_DATA_INDEX,
    last_index: int | None = None,
) -> list[dict]:
    """Cells where the freshly scraped row disagrees with the sheet."""
    if last_index is None:
        last_index = len(fresh_row) - 1
    diffs = []
    for index in range(first_index, min(last_index, len(fresh_row) - 1) + 1):
        sheet_value = sheet_row[index] if index < len(sheet_row) else ""
        fresh_value = fresh_row[index]
        if cells_equal(sheet_value, fresh_value):
            continue
        diffs.append(
            {
                "index": index,
                "column": column_letter(index),
                "sheet": _normalize(sheet_value),
                "fresh": _normalize(fresh_value),
            }
        )
    return diffs


def has_score_diff(diffs: list[dict]) -> bool:
    """True when a diff lands on a column the prediction ledger settles off."""
    return any(d["index"] in SCORE_SENSITIVE_ANALYSIS_COLUMNS for d in diffs)


def sailu_game_ids_in_window(
    sailu_rows: list[list[str]], start_date: str, end_date: str
) -> list[str]:
    """Game IDs recorded in 賽錄 between two `YYYY-MM-DD` dates, inclusive.

    Reading the window off our own sheet avoids re-discovering the schedule
    from Yahoo, and it means the audit only ever looks at games we actually
    recorded.
    """
    ids: list[str] = []
    seen: set[str] = set()
    for row in sailu_rows[1:]:
        if len(row) <= SAILU_DATE_INDEX:
            continue
        date_str = _normalize(row[SAILU_DATE_INDEX])
        if not date_str or not start_date <= date_str <= end_date:
            continue
        game_id = _normalize(row[SAILU_GAME_ID_INDEX])
        if not game_id or game_id in seen:
            continue
        seen.add(game_id)
        ids.append(game_id)
    return ids


def update_sheet_values(fresh_rows: list[list]) -> list[list]:
    """Rows for 資料更新 `B3:CE`, matching how 彙資 is written."""
    values = []
    for row in fresh_rows:
        padded = list(row) + [""] * (ANALYSIS_WIDTH - len(row))
        values.append(padded[FIRST_DATA_INDEX:ANALYSIS_WIDTH])
    return values


# A window this far from fully read is not a clean audit, it is a failed one.
# Yahoo starts refusing the game endpoints partway through a long sweep — the
# 2026-08-29 run read 51 games at three seconds a batch, then returned nothing
# for the remaining 91 — and a verdict drawn from the part that was read is
# worse than no verdict, because it arrives wearing the same green tick.
MIN_COVERAGE = 0.9

# A re-scrape that lost this many cells at once, every one of them a value the
# sheet holds and the fresh row does not, was blocked rather than corrected.
# A correction changes a value; a refused request loses all of them together.
BLANKED_CELL_THRESHOLD = 3

# Telegram delivers one message; a longer list of games is truncated rather
# than split, since a wall of fixtures is not read anyway and the count below
# still says how many there were.
SUMMARY_GAME_LIMIT = 15


def coverage_shortfall(
    scanned: int, requested: int, *, minimum: float = MIN_COVERAGE
) -> str | None:
    """How far short of the window the sweep fell, or ``None`` when it did not.

    Reported rather than raised: the caller still wants the report on disk, it
    just must not draw conclusions from it.
    """
    if requested <= 0 or scanned >= requested * minimum:
        return None
    return (
        f"只讀到 {scanned}/{requested} 場（{scanned / requested * 100:.0f}%），"
        f"低於 {minimum * 100:.0f}%，這次的比對結果不可信"
    )


def is_blanked_scrape(
    diffs: list[dict], *, minimum: int = BLANKED_CELL_THRESHOLD
) -> bool:
    """True when a diff list has the shape of a scrape that came back empty."""
    if len(diffs) < minimum:
        return False
    return all(not diff["fresh"] and diff["sheet"] for diff in diffs)


def telegram_incomplete(*, scanned: int, requested: int, start: str, end: str) -> str:
    """Told to the same chat as a finding, because silence would read as clean."""
    window = f"{start} → {end}" if start and end else "指定場次"
    return "\n".join(
        [
            f"⚠️ NPB 資料稽核 {window} 未完成",
            f"{requested} 場只讀到 {scanned} 場，Yahoo 中途開始拒絕請求。",
            "",
            "這次不出結論、不寫試算表。請縮小窗口後重跑。",
        ]
    )


def telegram_summary(
    findings: list[dict],
    *,
    start: str,
    end: str,
    scanned: int,
    pasted: int | None = None,
) -> str | None:
    """A short note on what the audit found, or ``None`` when it found nothing.

    A weekly alert that also fires on a clean week stops being read, so a
    window with nothing to report sends nothing at all.
    """
    changed = [f for f in findings if f["analysis"] or f["sailu"]]
    unreadable = [
        f for f in findings if not (f["analysis"] or f["sailu"]) and f["notes"]
    ]
    if not changed and not unreadable:
        return None

    window = f"{start} → {end}" if start and end else "指定場次"
    lines = [
        f"⚾ NPB 資料稽核 {window}",
        f"重掃 {scanned} 場，{len(changed)} 場與試算表不符",
        "",
    ]
    for finding in changed[:SUMMARY_GAME_LIMIT]:
        analysis = len(finding["analysis"]["diffs"]) if finding["analysis"] else 0
        # 賽錄 is compared in two spreadsheets that hold the same row, so the
        # wider of the two is the number of cells that actually differ.
        sailu = max(
            (len(block["diffs"]) for block in finding["sailu"].values()), default=0
        )
        counts = []
        if analysis:
            counts.append(f"分析表 {analysis} 格")
        if sailu:
            counts.append(f"賽錄 {sailu} 格")
        identity = finding["identity"]
        matchup = f"{identity[1]} @ {identity[2]}" if len(identity) >= 3 else finding["game_id"]
        flag = ""
        if finding["analysis"] and has_score_diff(finding["analysis"]["diffs"]):
            flag = "　⚠️ 比分，勿整批套用"
        lines.append(f"{finding['date']} {matchup} — {'、'.join(counts)}{flag}")
    if len(changed) > SUMMARY_GAME_LIMIT:
        lines.append(f"…還有 {len(changed) - SUMMARY_GAME_LIMIT} 場")
    if unreadable:
        lines.append(f"另有 {len(unreadable)} 場無法比對")

    lines.append("")
    if pasted is None:
        lines.append("只出報告，未寫入試算表")
    else:
        lines.append(f"已貼 {pasted} 場到「資料更新」分頁，請人工核對後再改賽錄")
    return "\n".join(lines)
