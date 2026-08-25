## Why

Betting lines are recorded to the `盤口` sheet but never reach anyone. The people
who use them read a hand-written sheet in the Taiwanese Asian-line format
(`1+70`, `2-75`, `PK`), which is **not** a different spelling of decimal odds:
the handicap runs at even money and the quoted number is the fraction of stake
settled on the one outcome that lands exactly on the line. Backing `1+70` wins
in full by two runs or more, collects 70% on exactly one, and loses in full
otherwise. Converting therefore needs the whole margin distribution, not a win
probability, so it cannot be done by any odds-format conversion.

Two defects blocked this and were found while building it:

1. **The scraper read the wrong board.** The `mk` parameter selects PS3838's
   LIVE / HIGHLIGHT / EARLY / TODAY containers, and we sent `mk=1` (TODAY). A
   game only entered the feed once its own day began, so the ledger never held
   an opening price and any game starting before the first successful poll was
   missed outright — 8 of 20 days recorded fewer than the full six NPB games.
   `mk=3` returns every board at once, and reveals that PS3838 also books CPBL
   (208753) and KBO (6227), contradicting an earlier "no CPBL" conclusion that
   had only probed TODAY.

2. **Every odds block is away-first while `ev[1]` is the home team.** PS3838 is
   an Asian skin (names read 主隊在前) over a US-convention feed. Read in event
   order, the home slot was the favourite in only 57 of 162 NPB games at a mean
   de-vigged .476 — home field runs the other way — and it disagreed with the
   hand-written sheet on 4 of 4 games. Handicaps and prices are also mirrored
   against each other within a spread row.

## What Changes

- Read the whole board (`mk=3`) instead of TODAY only, so the first capture of
  a day is the real opening line.
- Correct the away-first orientation of the moneyline and spread blocks.
- Convert de-vigged ladders into the local whole-number line: rebuild the run
  margin and total distributions, then solve for the water that makes each
  candidate line fair, posting whichever needs least compensation.
- Broadcast to a Telegram channel in two phases. The **open** is one post for
  the whole slate, sent the evening before as soon as the 早盤 board carries it.
  The **close** is per first pitch — a staggered card closes at different times
  — sent 30 minutes before each start, with games sharing a start time grouped.
- Trigger both from cron-job.org on a tight interval rather than GitHub's
  scheduler, which dropped 11 of 20 scheduled runs on 2026-08-23. Because
  neither phase knows when it will be due, each run decides for itself and a
  broadcast ledger guarantees subscribers get each post exactly once.

## Capabilities

### New Capabilities
- `asian-line-conversion`: turning a de-vigged odds ladder into the local
  whole-number handicap and total with their water levels.
- `odds-broadcast`: delivering a slate to Telegram subscribers once per phase,
  under repeated triggering.

### Modified Capabilities
<!-- No existing OpenSpec specs; the scraper corrections are captured under
     asian-line-conversion's input requirements. -->

## Impact

- `baseball/pinnacle_odds.py` — board selector, moneyline and spread orientation.
- `baseball/asian_lines.py`, `baseball/odds_notify.py` — new.
- `.github/workflows/npb_odds_broadcast.yml`,
  `.github/workflows/npb_odds_closing_broadcast.yml` — new, dispatch-only.
- New `推播紀錄` worksheet alongside `盤口`; new secret
  `TELEGRAM_BROADCAST_CHAT_ID`.
- **`盤口` rows written before 2026-08-25 have `ml_home`/`ml_away` swapped,
  `spread_hdp` sign-flipped, and the two spread prices swapped.** Nothing reads
  those columns yet, but any backfill must correct them first.
