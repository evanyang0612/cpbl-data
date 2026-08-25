## ADDED Requirements

### Requirement: Read every board PS3838 publishes

The scraper SHALL request the combined board (`mk=3`) so that a slate is
visible from the moment it is priced, rather than only once its own day has
begun. The first capture of a day is then the opening line.

#### Scenario: A slate is priced the evening before

- **WHEN** PS3838 has placed the next day's games on the 早盤 board
- **THEN** those games are returned with their full spread, total and moneyline
  ladders, dated by their own first pitch rather than by the day of capture

#### Scenario: A card starts before the first poll of the day

- **WHEN** a game's first pitch precedes the first capture on its own date
- **THEN** it has already been captured pre-game from the early board, instead
  of being skipped as live

### Requirement: Odds blocks are read away-first

The parser SHALL take the moneyline as `[away, home, draw]` and SHALL read a
spread row as `[away_hdp, home_hdp, line, home_price, away_price]`. Event names
read home-first (`ev[1]` is the home team) but every odds block follows the US
convention and reads away-first, and within a spread row the handicaps and the
prices are mirrored against each other — so the home handicap at index 1 pairs
with the home price at index 3.

#### Scenario: The favourite is identified correctly

- **WHEN** a full season of NPB moneylines is de-vigged
- **THEN** the home team is the favourite in appreciably more than half of
  games, consistent with home-field advantage

#### Scenario: A ladder never contradicts itself

- **WHEN** a team's prices are read across successive handicaps
- **THEN** giving away more runs only ever lengthens that team's price, and no
  team is priced shorter after conceding runs than on the moneyline

### Requirement: Rebuild the outcome distribution from the ladder

The converter SHALL de-vig each line in the ladder and assemble a cumulative
distribution over run margins and over total runs. Half-run lines fix a point
outright; whole-run lines push on the exact number and therefore fix only a
conditional ratio, which SHALL be solved against a neighbouring known point.
Points that no line pins down SHALL be reported as unavailable rather than
extrapolated.

#### Scenario: A whole-number line completes a gap

- **WHEN** the ladder offers a whole handicap and one adjacent point is known
- **THEN** the remaining point is recovered from the line's no-push price

#### Scenario: A ladder is too thin to price

- **WHEN** no line fixes a point the quote depends on
- **THEN** no line is quoted for that market, and the game is still shown with
  the missing market marked

### Requirement: Quote the whole-number line as a settlement fraction

For a side laying `L` runs, the water SHALL be the fraction of stake settled on
a margin of exactly `L` that makes the bet fair:
`X = [P(margin < L) - P(margin > L)] / P(margin == L)`. Totals SHALL use the
same form with the over in place of the favourite, and SHALL be quoted from the
over's side. Water SHALL be rendered to the nearest five points as `+70`,
`-30`, or `PK`.

#### Scenario: A line is quoted

- **WHEN** a team laying one run wins by two or more
- **THEN** the bet collects in full; on exactly one run it settles at the quoted
  water; on anything less it loses in full

### Requirement: Post the line that needs least compensation

The quoted handicap SHALL be whichever whole number carries water closest to
even, so a heavy favourite moves up the ladder instead of being quoted at a
water no book would post. A favourite too slight to carry a whole run — needing
more than a full unit of water — SHALL be quoted `PK`, as SHALL a game whose
moneyline is level and for which no favourite ladder is published.

#### Scenario: A heavy favourite

- **WHEN** laying one run would require paying multiples of the stake back on
  the exact-margin case
- **THEN** the two-run line is quoted instead

#### Scenario: A pick'em

- **WHEN** the de-vigged moneyline is level, so PS3838 publishes no line for a
  favourite to lay
- **THEN** the game is quoted `PK` against the home team

### Requirement: Fall back to a half line when no whole number fits

The quoted line SHALL be a whole number carrying water wherever one sits near
enough the middle to be postable. Where none does — where every whole number
would need close to a full unit of stake settled on the push — the line SHALL
instead be the half number nearest even, written bare: a half line cannot be
landed on, so it has no push to settle and no water to quote.

#### Scenario: Every whole number is badly placed

- **WHEN** the totals either side of the middle each need close to a full unit
  of water, while the half line between them prices even
- **THEN** the half line is posted bare, with no water

#### Scenario: A whole number still fits

- **WHEN** a whole number needs only moderate water
- **THEN** it is posted with that water, and the half line is not reached for
