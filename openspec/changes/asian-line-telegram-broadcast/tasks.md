## 1. Correct the feed reading

- [x] 1.1 Identify the board selector by reading the SPA bundle, and switch
      `EVENTS_PARAMS` to the combined board (`mk=3`)
- [x] 1.2 Record the CPBL and KBO league ids the combined board exposes
- [x] 1.3 Establish odds-block orientation statistically (mean de-vigged
      probability of the home slot across recorded NPB games)
- [x] 1.4 Read the moneyline away-first, and take the home handicap and home
      price from the mirrored positions in a spread row
- [x] 1.5 Cover both with regression tests that fail on either half of the fix

## 2. Convert to the local line

- [x] 2.1 De-vig a two-way market
- [x] 2.2 Build the run-margin distribution from the spread ladder and
      moneyline, solving whole-number lines against known neighbours
- [x] 2.3 Build the total-runs distribution from the over/under ladder
- [x] 2.4 Solve the fair water for a side laying a given number of runs, for
      both home and away
- [x] 2.5 Pick the posted line by smallest water, falling back to `PK`
- [x] 2.6 Render water as `+70` / `-30` / `PK`, rounded to the nearest five

## 3. Broadcast

- [x] 3.1 Render a slate: laying team plus bare line, total from the over,
      pick'em against the home team
- [x] 3.2 Date the message by the games rather than by the broadcast
- [x] 3.3 Narrow a broadcast to one target slate
- [x] 3.4 Add the opening phase: whole slate, sent as soon as it appears
- [x] 3.5 Add the closing phase: grouped by first pitch, held until 30 minutes
      before each start
- [x] 3.6 Add the Sheets-backed ledger, keyed by game date, league and phase
- [x] 3.7 Always resolve a concrete target date, so the ledger guard cannot be
      bypassed by omitting one

## 4. Scheduling

- [x] 4.1 Add the dispatch-only opening workflow with a concurrency group
- [x] 4.2 Add the dispatch-only closing workflow with its own concurrency group
- [x] 4.3 Send through the broadcast bot's own credentials, with no fallback
      to the alerting bot, and add a helper for finding the channel's chat id
- [x] 4.4 Create the broadcast bot with @BotFather, create the channel, make
      the bot an administrator, and set `TELEGRAM_BROADCAST_BOT_TOKEN` and
      `TELEGRAM_BROADCAST_CHAT_ID` (@npb_odds_bot, 每日盤口, verified end to
      end on 2026-08-25)
- [ ] 4.5 Point cron-job.org at both workflow dispatch endpoints and choose the
      intervals
- [ ] 4.6 Watch one full day end to end and compare both posts against the
      hand-written sheet

## 5. Presentation

- [x] 5.1 Look up announced starters and their Yahoo player ids for a slate
- [x] 5.2 Lay the message out the way the sheet does: handicap against the
      pitcher giving the runs, total against the away side
- [x] 5.3 Write the numbers full width, and zero water as `平` rather than `PK`
- [x] 5.4 Link each starter to their Yahoo page on the opening post, and leave
      the closing post as plain names
- [x] 5.5 Add the forecast for first pitch — condition, temperature, rainfall
      and wind — read from the pinpoint page the game page links to
- [x] 5.6 Keep one labelled row per fact, so a row that is not an odds line
      has somewhere to go
- [x] 5.7 Leave the forecast off games played under a roof
- [x] 5.8 Read the wind as 順風 / 逆風 / 側風 relative to the way the park faces
- [x] 5.9 Fill `PARK_BEARINGS` from NPB's own table of ground orientations;
      grounds not on it show the raw wind with no 順風 / 逆風 claim

## 6. Follow-ups
- [ ] 6.1 Extend the broadcast to CPBL, now that PS3838 is known to book it
- [ ] 6.2 Decide whether to backfill the pre-fix `盤口` rows
- [x] 6.3 Quote half lines (`6.5`, `0.5`) where no whole number can carry the
      game — they have no push, so they carry no water and are written bare
