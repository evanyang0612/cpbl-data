"""Rebuild the 投手主客 tab by hand.

The daily NPB run refreshes it after 分析表紀錄 is written; this is the same
call, for a one-off rebuild or to see the table without writing it.

    uv run python migration/add_npb_pitching_splits_sheet.py --dry-run
    uv run python migration/add_npb_pitching_splits_sheet.py
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from baseball.npb_pitching_splits_sheet import refresh  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarise each team's pitching by segment and venue.")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the table instead of writing it")
    args = parser.parse_args()
    refresh(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
