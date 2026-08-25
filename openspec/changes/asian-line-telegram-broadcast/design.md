## Context

`baseball/pinnacle_odds.py` already scrapes PS3838's public compact feed into a
`盤口` worksheet every 30 minutes. Nothing consumes it, and the audience reads
lines in the Taiwanese Asian-line format instead of decimal odds.

That format settles differently rather than merely printing differently. The
handicap runs at even money; the quoted number is what the exact-margin outcome
pays. `1+70` collects the full stake on a two-run win, 70% on a one-run win, and
loses in full otherwise. Recovering it needs `P(margin == L)` and the tails
around it, so a single price cannot produce it — only the ladder can.

Two facts about the feed emerged while building this, both of which had been
silently wrong:

- `mk` selects the SPA's LIVE / HIGHLIGHT / EARLY / TODAY containers. We sent
  TODAY, which is why no opening line was ever recorded and early games were
  missed entirely.
- The odds arrays are away-first even though `ev[1]` names the home team, and
  within a spread row the handicaps and the prices are mirrored against each
  other.

## Goals / Non-Goals

**Goals:**

- Record and broadcast the genuine opening line, and the closing line.
- Derive the local line from first principles, so it holds for any ladder shape
  rather than being fitted to a rule of thumb.
- Survive being triggered at any moment and any frequency: the schedule is not
  knowable, so correctness must not depend on it.

**Non-Goals:**

- Backfilling the `盤口` rows written before the orientation fix. They are
  recoverable — the corruption is a deterministic swap — but nothing reads
  those columns yet.
- Modelling the bookmaker's own margin. We publish the fair line; a local book
  charges commission separately.
- Probable starters in the message, and the CPBL and KBO leagues the corrected
  board now exposes. Both are natural follow-ups.

## Decisions

**Solve for the water rather than approximating it.** For a side laying `L`,
fairness gives `X = [P(margin < L) - P(margin > L)] / P(margin == L)`. The
existing rule of thumb divides the two prices' difference by a constant, which
amounts to assuming a fixed `P(margin == 1)`; measured per game it ranges from
about 10% to 18%, so the constant is the approximation's whole error. Totals
reuse the identical form with the over in place of the favourite.

**Choose the line by smallest water.** A heavy favourite laying one run would
be quoted at water beyond -2, which no book posts; it moves to two runs
instead. Applying "post the line needing least compensation" to both handicaps
and totals means one rule covers both, and it reproduces what books actually do.

**De-vig multiplicatively.** Adequate for the near-even two-way markets here.
Shin's method would matter for longshot-heavy books; Pinnacle is not one.

**Report gaps instead of extrapolating.** A ladder can leave a needed point
unpinned — most often on a genuine pick'em, where PS3838 publishes no line for
a favourite to lay. Guessing would silently fabricate a line; the game is shown
with the market blank, except that a level moneyline is itself sufficient
evidence to quote `PK`.

**Prove orientation statistically, not by inspection.** Internal consistency
between the moneyline and the ladder is symmetric — flipping both preserves it —
so it cannot settle which physical team the odds belong to. Home-field advantage
does: across 162 NPB games the slot read as home was favourite in 57 and averaged
.476 de-vigged, which is decisive, and agrees with the hand-written sheet on all
four games where the two could be compared.

**Trigger externally, decide internally.** GitHub's scheduler dropped 11 of 20
runs on 2026-08-23, and neither phase has a knowable due time anyway. Both
workflows are dispatch-only and fired frequently from cron-job.org; each run
decides for itself whether anything is due. Frequency then becomes a tuning
knob outside the repo rather than a correctness concern.

**Keep the sent-record in Sheets.** Runners are stateless, so the record must
outlive the run. Sheets is already authenticated in every workflow, is
auditable, and is trivially resettable — deleting a row forces a re-post.
Telegram's own history cannot serve, as a bot cannot reliably read back its own
channel posts. Actions cache is semantically a cache and may be evicted, which
disqualifies it from a correctness guarantee.

**Key the record by game date, not send date.** What must not happen twice is a
slate being announced twice. A board opening after midnight rolls the send date
without rolling the slate, which would let the guard misfire.

**Scope the closing record by start time.** The close is per first pitch, so a
staggered card needs one record per start time; a card sharing a start time
produces one post and one record.

## Risks / Trade-offs

- **Two runs could overlap** and both find the record absent. A workflow
  concurrency group serialises them; runs are short and idempotent, so queueing
  costs nothing.
- **The `PK` threshold and the 30-minute closing window are judgement calls.**
  Both are named constants and overridable from the CLI, so they can be tuned
  against real sheets without touching the derivation.
- **We publish a fair line, a local book publishes a shaded one.** Expect small
  systematic differences; the comparison against the sheet of 2026-08-23 showed
  gaps of 0–30 points, also attributable to comparing an open against a close.
- **Historical `盤口` rows stay wrong** until someone backfills them. Recorded
  in the proposal's Impact so a later reader is not misled by them.
