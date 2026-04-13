"""
Sync 2025 and 2026 賽錄 rows from the source sheet into the target sheet.

Writes only through AY from source data, while preserving/rebuilding formula
columns AZ:BT in the target sheet.

Normalization applied to pitcher names:
- convert full-width Latin/dot/space variants to the target sheet style
- convert ideographic spaces to half-width spaces
- collapse H.メヒア to メヒア to match the target sheet's historical format
"""
from __future__ import annotations

import re
import unicodedata

from npb import get_worksheet

SOURCE_KEY = "1qPdgcy_4s4Dj2xKo0QJawxPRaB6u9sGM3D4avkAjJUw"
TARGET_KEY = "1bDBg86YndwzE4e5r9rkj9KIudnJOgKI4IM1nfJoSl-o"
SHEET_NAME = "賽錄"
WRITE_COLS_END = "BT"
FORMULA_COL_START = 52  # AZ
UPDATE_CHUNK = 200


def wanted_game_id(game_id: str) -> bool:
    return game_id.startswith("s2025") or game_id.startswith("202103")


def normalize_pitcher_name(name: str) -> str:
    if not name:
        return name

    normalized = unicodedata.normalize("NFKC", name).replace("\u3000", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if re.fullmatch(r"[A-Z]\.メヒア", normalized):
        return "メヒア"
    return normalized


def normalize_row_a_to_ay(row: list[str]) -> list[str]:
    cleaned = row[:51]
    while len(cleaned) < 51:
        cleaned.append("")
    cleaned[3] = normalize_pitcher_name(cleaned[3])  # D 客場先發
    cleaned[5] = normalize_pitcher_name(cleaned[5])  # F 主場先發
    return cleaned


def formula_row(row_num: int) -> list[str]:
    return [
        f"=SUM(J{row_num}:L{row_num})",
        f"=SUM(Y{row_num}:AA{row_num})",
        f"=SUM(J{row_num}:N{row_num})",
        f"=SUM(Y{row_num}:AC{row_num})",
        f"=SUM(J{row_num}:O{row_num})",
        f"=SUM(Y{row_num}:AD{row_num})",
        f"=SUM(J{row_num}:P{row_num})",
        f"=SUM(Y{row_num}:AE{row_num})",
        '=IF(客總分="","",IF(客總分=主總分,"平",IF(客總分>主總分,"勝","敗")))',
        '=IF(BH{0}="","",IF(BH{0}="平","平",IF(BH{0}="勝","敗","勝")))'.format(row_num),
        '=IF(BH{0}="勝",客總分-主總分,IF(BH{0}="敗",主總分-客總分,0))'.format(row_num),
        '=IF(MOD(AT{0},1)=0,AT{0},IF(RIGHT(AT{0},1)="1",(AT{0}-0.1)+1/3,(AT{0}-0.2)+2/3))'.format(row_num),
        '=IF(MOD(AU{0},1)=0,AU{0},IF(RIGHT(AU{0},1)="1",(AU{0}-0.1)+1/3,(AU{0}-0.2)+2/3))'.format(row_num),
        '=IF(客總分="","",客總5+主總5)',
        '=IF(客總分="","",客總分+主總分)',
        f'=IF(J{row_num}="","",SUM(J{row_num}:R{row_num}))',
        f'=IF(J{row_num}="","",SUM(Y{row_num}:AG{row_num}))',
        f'=IF(S{row_num}="","",SUM(S{row_num}:U{row_num}))',
        f'=IF(AH{row_num}="","",SUM(AH{row_num}:AJ{row_num}))',
        '=IF(AO{0}="","",IF(AND(客先局>=5,主總7<=3,主總6<=2,主總5<=1),1,IF(AND(客先局>=5,主總6<=2,主總5<=1),1,IF(AND(客先局>=5,主總5<=1),1,""))))'.format(row_num),
        '=IF(AO{0}="","",IF(AND(主先局>=5,客總7<=3,客總6<=2,客總5<=1),1,IF(AND(主先局>=5,客總6<=2,客總5<=1),1,IF(AND(主先局>=5,客總5<=1),1,""))))'.format(row_num),
    ]


def chunked(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def main():
    source = get_worksheet(SHEET_NAME, SOURCE_KEY)
    target = get_worksheet(SHEET_NAME, TARGET_KEY)

    source_rows = source.get_all_values()[1:]
    target_rows = target.get_all_values()[1:]

    source_map = {
        row[1]: normalize_row_a_to_ay(row)
        for row in source_rows
        if len(row) > 1 and wanted_game_id(row[1])
    }
    target_ids = {row[1] for row in target_rows if len(row) > 1 and row[1]}

    missing_rows = [
        source_map[gid]
        for gid in sorted(
            (gid for gid in source_map if gid not in target_ids),
            key=lambda gid: (source_map[gid][40], gid),
        )
    ]
    if not missing_rows:
        print("No missing 2025/2026 rows.")
        return

    col_a = target.col_values(1)[1:]
    col_b = target.col_values(2)[1:]
    placeholder_rows = [
        i + 2
        for i, a in enumerate(col_a)
        if a and not (col_b[i] if i < len(col_b) else "")
    ]
    if not placeholder_rows:
        raise RuntimeError("No placeholder rows found in target sheet.")

    start_row = placeholder_rows[0]
    end_row = start_row + len(missing_rows) - 1
    extra_rows_needed = max(0, end_row - target.row_count)
    if extra_rows_needed:
        target.add_rows(extra_rows_needed)
        print(f"Added {extra_rows_needed} row(s) to target sheet.")

    start_seq = int(target.acell(f"A{start_row}").value)
    seq_values = [[start_seq + offset] for offset in range(len(missing_rows))]
    formula_values = [formula_row(row_num) for row_num in range(start_row, end_row + 1)]
    write_values = [row[1:51] for row in missing_rows]  # B:AY

    print(f"Writing {len(missing_rows)} rows to {start_row}:{end_row}")

    for offset, chunk in enumerate(chunked(seq_values, UPDATE_CHUNK)):
        chunk_start = start_row + offset * UPDATE_CHUNK
        chunk_end = chunk_start + len(chunk) - 1
        target.update(
            range_name=f"A{chunk_start}:A{chunk_end}",
            values=chunk,
            value_input_option="USER_ENTERED",
        )

    for offset, chunk in enumerate(chunked(formula_values, UPDATE_CHUNK)):
        chunk_start = start_row + offset * UPDATE_CHUNK
        chunk_end = chunk_start + len(chunk) - 1
        target.update(
            range_name=f"AZ{chunk_start}:{WRITE_COLS_END}{chunk_end}",
            values=chunk,
            value_input_option="USER_ENTERED",
        )

    for offset, chunk in enumerate(chunked(write_values, UPDATE_CHUNK)):
        chunk_start = start_row + offset * UPDATE_CHUNK
        chunk_end = chunk_start + len(chunk) - 1
        target.update(
            range_name=f"B{chunk_start}:AY{chunk_end}",
            values=chunk,
            value_input_option="USER_ENTERED",
        )

    print("Sync complete.")


if __name__ == "__main__":
    main()
