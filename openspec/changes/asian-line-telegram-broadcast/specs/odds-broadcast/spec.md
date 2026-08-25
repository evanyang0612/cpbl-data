## ADDED Requirements

### Requirement: Broadcast a slate in the hand-written format

The broadcast SHALL render each game with the team laying the runs followed by
the bare line, and the total quoted from the over. The line SHALL carry no
leading sign of its own — `軟銀 2-75`, not `軟銀 -2 -75` — because naming the
laying team already says which side gives runs, and a second minus reads as a
second water level. A pick'em SHALL be written against the home team, since no
side is laying.

#### Scenario: A handicap is rendered

- **WHEN** a team lays two runs at a water of -75
- **THEN** the line reads `<team> 2-75`

#### Scenario: A total is rendered

- **WHEN** a total of 8 carries +70 from the over's side
- **THEN** the line reads `8　+70`, without the mirrored under

### Requirement: Announce the opening line the evening before

The opening broadcast SHALL cover a whole slate in one message and SHALL be
sent as soon as that slate reaches the board. A day's games appear once the
previous evening's games have finished, at no fixed hour, so the broadcast
SHALL NOT depend on being triggered at a particular time.

#### Scenario: The board has not opened

- **WHEN** the broadcast runs before the target slate is published
- **THEN** nothing is sent and nothing is recorded, so a later run still posts

#### Scenario: The board carries games already played

- **WHEN** the board holds both the current day's games and the target slate
- **THEN** only the target slate is broadcast, and the message is dated by the
  games rather than by the day it was sent

### Requirement: Announce the closing line per first pitch

The closing broadcast SHALL be sent 30 minutes before first pitch and SHALL be
scoped to one start time, because a staggered card closes at different moments.
Games sharing a start time SHALL be announced together in one message.

#### Scenario: A staggered card

- **WHEN** the earliest games are within 30 minutes of starting while later
  games are hours away
- **THEN** only the earliest start time is broadcast, and the later ones are
  broadcast by subsequent runs as each becomes due

#### Scenario: A card with a single start time

- **WHEN** every game on the slate starts at the same time
- **THEN** they are announced together in a single message

### Requirement: Deliver each broadcast exactly once

The system SHALL keep a durable record of what has been sent, outside the run,
keyed by the slate's game date, the league, and the phase — the closing phase
further keyed by start time. A broadcast whose record exists SHALL NOT be sent
again, and concurrent runs SHALL be prevented from both finding the record
absent. This is load-bearing because neither phase knows when it will be due:
both are triggered repeatedly, so every run after the first finds the same
games.

#### Scenario: Repeated triggers

- **WHEN** the broadcast is triggered many times after a slate has gone out
- **THEN** subscribers receive it once, and later runs exit quietly

#### Scenario: The same slate in both phases

- **WHEN** a slate has already been broadcast at open
- **THEN** its closing broadcast is still sent when due, being a separate record

#### Scenario: A board that opens after midnight

- **WHEN** the broadcast date differs from the date of the games
- **THEN** the record still identifies the slate, because it is keyed by the
  game date rather than the date of sending

### Requirement: Broadcast through its own bot

The broadcast SHALL post using credentials dedicated to it, and SHALL NOT fall
back to the credentials used for operational alerting when its own are absent.
The alerting bot reports scraper failures into a private chat; a subscriber to
the odds channel must never receive one, and a misconfigured broadcast must not
deliver a slate into the operators' chat instead.

#### Scenario: Broadcast credentials are missing

- **WHEN** the broadcast's own bot token or chat id is not configured
- **THEN** nothing is sent anywhere, and nothing is recorded as sent, so a
  later run can still deliver the slate once configured

### Requirement: Name the starting pitchers

The opening broadcast SHALL identify each game by its announced starting
pitchers, and SHALL link each to their player page. The closing broadcast SHALL
name them without links. A game whose starters are not yet announced, or whose
lookup fails, SHALL still be broadcast, identified by team.

#### Scenario: Starters are announced

- **WHEN** the opening post is built for a slate whose starters are known
- **THEN** each pitcher is named on their own line and linked to their page,
  with the handicap written against whichever of the two gives the runs

#### Scenario: The starter lookup fails

- **WHEN** the pitcher source is unreachable or the starters are unannounced
- **THEN** the lines are still broadcast, with teams standing in for pitchers

### Requirement: Write numbers the way the sheet does

Lines SHALL be rendered with full-width digits and signs, and a line needing no
compensation SHALL be written `平` rather than `PK` — a pick'em has no handicap
at all, whereas `8平` is a real line whose push happens to settle even.

#### Scenario: A line settles even on the push

- **WHEN** a total of 8 carries no water
- **THEN** it is written `８平`, and a game with no handicap is written `ＰＫ`

### Requirement: Report the forecast at first pitch

The opening broadcast SHALL carry the forecast for the hour each game starts —
condition, temperature, rainfall and wind — taken for that day rather than for
the day the post is written. A game whose forecast cannot be read SHALL still
be broadcast without it.

#### Scenario: The evening post is about tomorrow

- **WHEN** the forecast page tabulates today and tomorrow separately
- **THEN** the row read is the one for the day the games are played

#### Scenario: First pitch falls between forecast steps

- **WHEN** the table steps in three-hour intervals and a game starts at 14:00
- **THEN** the nearest step is used

#### Scenario: The forecast is unavailable

- **WHEN** the forecast lookup fails
- **THEN** the lines are broadcast without a weather row

### Requirement: Only forecast what the weather can reach

A forecast SHALL be reported to the extent the ground is exposed to it. A game
in a sealed park SHALL carry no weather row at all, rather than one the reader
has to know to disregard. A park roofed over the field but open at its sides
SHALL report the temperature alone: rain never reaches the play, but neither
does any cooling.

#### Scenario: A game under a sealed roof

- **WHEN** the venue is a domed park
- **THEN** the game is broadcast with its lines and no weather row

#### Scenario: A game under a roof with open sides

- **WHEN** the venue is roofed but unwalled
- **THEN** only the temperature is reported

#### Scenario: A game at a ground with no recorded orientation

- **WHEN** the venue is an open park the broadcast has no bearing for, such as
  a neutral site
- **THEN** the forecast for that ground is still reported, since it is looked
  up per game rather than per home team, and only the wind arrow is withheld

### Requirement: Report the wind as the hitter meets it

Wind SHALL be reported relative to the way the park faces, since a compass
direction alone says nothing without knowing where centre field lies. It SHALL
be shown as an arrow read as the ball flies — from behind the plate, up carries
out to centre — to one of eight points, so a wind between the axes is not filed
as though it were along one. Where a park's orientation is not recorded, the
reported direction SHALL stand alone with nothing claimed about its effect.

#### Scenario: The wind blows out to centre

- **WHEN** the wind comes from behind the plate
- **THEN** the arrow points up

#### Scenario: A crosswind keeps its side

- **WHEN** the wind crosses from right field towards left
- **THEN** the arrow points left, distinct from the reverse crossing, which
  pushes the ball the opposite way

#### Scenario: The park's orientation is not known

- **WHEN** no bearing is recorded for the park
- **THEN** the wind is shown as reported, with no arrow
