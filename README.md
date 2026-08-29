# Baseball Stats Scrapers

Automated scrapers that pull game results from CPBL, NPB, and MLB, then write stats to Google Sheets. They run on GitHub Actions cron schedules.

---

## Repository Structure

```
.
├── cpbl.py                          # CPBL scraper
├── npb.py                           # NPB scraper
├── requirements.txt
├── lastTenGames.gs                  # Google Apps Script for CPBL 近十場 sheet
├── lastTenGamesPreseason.gs         # Google Apps Script for CPBL 熱身賽 近十場 sheet
├── baseball/
│   ├── pinnacle_odds.py             # PS3838 odds scraper (NPB + MLB) -> 盤口 sheets
│   ├── npb_audit.py                 # Comparator for the weekly NPB history audit
│   └── mlb_games.py                 # Resolves MLB gamePk for an odds event
├── migration/
│   └── audit_npb_history.py         # Re-scrapes recent NPB games and diffs them
└── .github/workflows/
    ├── cpbl_scheduler.yml           # Cron: every 30 min, 07:00–16:00 UTC (via Japan VPN)
    ├── mlb_record_scheduler.yml     # cron-job.org: daily, 21:00 JST
    ├── npb_scheduler.yml            # Cron: every 30 min, 08:00–14:00 UTC
    ├── npb_audit_scheduler.yml      # cron-job.org: weekly, Monday 14:00 JST
    ├── npb_odds_scheduler.yml       # Cron: every 30 min, 01:00–10:30 UTC
    └── mlb_odds_scheduler.yml       # Cron: hourly 13:00–16:00, then every 30 min 17:00–03:30 UTC
```

---

## CPBL (`cpbl.py`)

Scrapes [cpbl.com.tw](https://www.cpbl.com.tw) for regular season (`A`) and preseason (`G`) game results and writes box score data to Google Sheets.

### Workflow

1. Fetches the monthly schedule via `POST /schedule/getgamedatas` (requires a CSRF token extracted from the schedule page)
2. For each game on or before today that hasn't been recorded yet, fetches the box score via `POST /box/getlive`
3. Parses pitching and batting stats, writes a 125-column row to the target worksheet
4. After all games are processed, refreshes the **彙資** sheet with today's games (up to 3)

### Worksheets

| Kind Code | Worksheet  | Description     |
| --------- | ---------- | --------------- |
| `A`       | 賽程       | Regular season  |
| `G`       | 熱身賽賽程 | Preseason       |
| —         | 彙資       | Today's summary |

### Scheduler

Runs every 30 minutes between **07:00–16:00 UTC** (15:00–00:00 Taiwan time) via a NordVPN WireGuard tunnel (required to access cpbl.com.tw from GitHub Actions).

The workflow defaults to NordVPN `country_id=108` for recommendations, but it can prefer a known-good server IP or hostname first. If CPBL allows a specific Nord `station` IP such as `94.156.205.102`, set `NORDVPN_STATION_ALLOWLIST=94.156.205.102`. If the acceptable servers all share a prefix, such as `94.156.205.*`, set `NORDVPN_STATION_PREFIX_ALLOWLIST=94.156.205.` and the workflow will pick the lowest-load matching server when it is available.

### Manual run (single game)

```python
# In cpbl.py __main__ block, uncomment:
main(game_sno="239", year="2025", kind_code="A")
```

---

## NPB (`npb.py`)

Scrapes [baseball.yahoo.co.jp](https://baseball.yahoo.co.jp/npb/) for the last 10 finished games of each NPB team and writes per-team stat blocks to Google Sheets.

### Workflow

1. For each league (央盟 / 洋盟), determines the next game day's matchups to set the column order
2. Fetches the last 10 finished game IDs for each team from their schedule pages
3. Fetches box scores concurrently (up to 5 at a time) and deduplicates across teams
4. Builds 13-row × 12-col blocks (header + 10 games + 近十場/近五場 averages) per team
5. Writes all blocks and applies team colour formatting in a single batch update

### Worksheets

| League | Worksheet |
| ------ | --------- |
| 央盟   | 近十場a   |
| 洋盟   | 近十場b   |

### Scheduler

Runs every 30 minutes between **08:00–14:00 UTC** (17:00–23:00 JST), covering NPB evening games. No VPN required.

### History audit (`migration/audit_npb_history.py`)

The daily run is append-only: once a game lands in 賽錄 / 分析表紀錄 it is never
looked at again. NPB publishes [公式記録の訂正](https://npb.jp/news/) days after
the fact, a scrape can fail half-way, and a parser fix only helps games scraped
after it shipped. This walks back over a window of days, re-scrapes every game
we recorded, rebuilds its rows, and reports every cell that disagrees with the
sheets.

```bash
uv run python migration/audit_npb_history.py --days 10
uv run python migration/audit_npb_history.py --days 10 --write-sheet --notify
uv run python migration/audit_npb_history.py --game-ids 2021039221
```

- Game IDs come from 賽錄 column B, filtered on the date in column AO, so the
  audit only ever revisits games we actually recorded — no re-discovering the
  schedule from Yahoo.
- 分析表紀錄 is matched on (日期, 客隊, 主隊); a duplicate matchup on one date is
  skipped with a note rather than guessed at. 賽錄 is matched on the game ID and
  compared in **both** spreadsheets, since `NpbSailuService` writes both.
- Only raw columns are compared. 賽錄 stops at `AY`; `AZ` onward is
  `sailu_formula_row` and is never read or written.
- A blank never equals a zero: 分析表紀錄 writes `""` for an inning that was never
  batted and `0` for a scoreless one, so treating them as equal would hide a
  real correction.
- Diffs on the final score or either side's first five innings are tagged
  `[SCORE — do not auto-apply]`. Those columns settle 預測紀錄, where
  `balance_after` is cumulative, so one changed score invalidates every running
  balance below it.
- Reports are written to `.cache/npb_audit_<ts>.json` and uploaded as a workflow
  artifact.
- The window defaults to **10 days** — a week plus slack, which is what a weekly
  sweep needs. Yahoo refuses the game endpoints partway through a longer sweep:
  a 30-day run (142 games) was served up to the 51st game and returned nothing
  for the remaining 91, from a GitHub runner.
- A sweep that reads less than 90% of its window writes nothing, announces
  nothing as a finding, and exits non-zero. A verdict drawn from the part that
  was read would arrive wearing the same green tick as a real one.
- Diffs where every cell the sheet holds came back blank are filed as unread
  rather than as differences. A correction changes a value; a refused request
  loses all of them at once.
- `--notify` sends the same summary to the alerting bot's Telegram chat — the
  games that disagree, how many cells each, and which of them touch the score.
  A window where everything matches sends nothing at all: a weekly note that
  also fires on a clean week is one nobody reads by the time it matters.

`--write-sheet` pastes the games needing changes into the **資料更新** tab from
`B3`, using the same 83-column layout as 彙資, so they can be eyeballed before
anything is overwritten in place. The tab holds 143 games; a longer list is
truncated to the oldest 143 with a warning.

Writing corrections back in place (`--apply`) is deliberately not implemented
yet — the first few weeks are report-only, to shake out format noise before
anything overwrites recorded history.

Note: Yahoo rate-limits `/npb/game/<id>/*` once a run gets long, and a GitHub
runner is no exception — the 2026-08-29 30-day sweep was cut off at the 51st of
142 games and never recovered. Keep the window short. Every session that
scrapes Yahoo must send `npb.BROWSER_HEADERS`.

Fired from cron-job.org every Monday at **14:00 JST**, by which point the
weekend's games are settled and the prior week's 訂正 have had time to land.
GitHub's own scheduler is not used: on a public repository it disables a
scheduled workflow after 60 days with no repository activity, and it does so
silently — a poor property for the job whose purpose is noticing silent drift.
The `days` input sets the window; the dispatch body carries it.

---

## MLB (`migration/update_mlb_record.py`)

Scrapes MLB Stats API finalized regular-season games and appends missing rows to the `紀錄` worksheet in the MLB spreadsheet.

### Workflow

1. Fetches recent regular-season games from MLB Stats API
2. Uses `gamePk` in column B to skip games already recorded
3. Fetches per-game feed data for starters, pitcher hand, line score, venue, venue ID, umpire, league/division, starter innings, and earned runs
4. Writes raw columns `A:AO` and copies formula columns `AP:BD` from the prior row
5. Refreshes `MLB近十場1` through `MLB近十場5`, with three matchup blocks per sheet

### Scheduler

Fired from cron-job.org once daily at **21:00 JST**. GitHub's own scheduler ran this 40–70 minutes late most days and twice nearly ten hours late, which is past the point where it is still writing the day it was aimed at. The record command checks the last 3 calendar dates so delayed finalization and timezone edge cases are picked up without duplicating rows:

```bash
uv run python migration/update_mlb_record.py --recent-days 3
uv run python migration/update_mlb_last10.py
```

### 設定 / 對戰 (n) matchup sheets (`migration/add_mlb_matchup_sheets.py`)

A one-off build of the MLB spreadsheet's starter-matchup tabs, ported from the NPB
spreadsheet's `設定` + `対戦 (n)` layout. Nothing scrapes them — they are formulas over
`紀錄`, so they refresh themselves whenever the daily record run appends rows.

- `設定` — one 7-column block per matchup (`對戰 1`…`對戰 15`, stride 7 from column B),
  three row bands per block for the three games of a series. Pick 隊伍 and 先發 from the
  dropdowns; the 先發 list is filtered to that team via a hidden `=INDIRECT(<team>)`
  helper column. The 24/25/26 columns preview the starter's ERA against that opponent
  (`VS.`) and overall (`合`), dividing the helper rows 39-71.
- `對戰 (1)`…`(15)` — one tab per matchup: six blocks (three games × two starters), each
  showing the starter's 客/主/合 line (場次, QS, QS%, ERA, IP, opponent runs through 5,
  opponent runs total) overall, versus the actual opponent, and versus every team in
  that starter's own league. Row 1 holds the date window per game, as two dropdowns off
  the `開賽年度` / `閉幕年度` named ranges (`資料!G2:H41`). Those two lists are single
  spilling `ARRAYFORMULA(DATE(SEQUENCE(...)))` cells running from 2017 to next year, so
  a new season shows up on its own every January — nothing to maintain by hand.

Colours match NPB: ERA `≤3.5` green / `≥4` red, opponent runs through 5 `<2` / `≥2.5`,
opponent runs total `<4` / `≥4.5`, and 場次 + QS + QS% (columns C:E per block) keyed off
the quality-start rate, green above 65% and red below 41%. Zeros are hidden through the
number format (`0.00;-0.00;;@`) rather than a conditional-format rule.

`設定` fills itself daily (`migration/update_mlb_probables.py`, run from the MLB record
workflow at 21:00 JST / 08:00 ET):

- Game 1 of every block gets both teams and both announced starters from MLB Stats API
  (`schedule?hydrate=probablePitcher,team`); game 2 gets starters only, and only where
  the pairing repeats, so the `=B4` / `=B6` mirrors survive. Game 3 is usually blank —
  probables two days out are rarely announced.
- The `AL-P` / `NL-P` lists behind the 先發 dropdowns are rebuilt from `紀錄`'s own
  starter columns, most recent start first. Only pitchers who actually started for that
  club appear, so no relievers get in. They had been hand-kept and still held 2019
  rosters.
- The ballpark cell is `=VLOOKUP(<home team>,資料!$A$2:$B$31,2,FALSE)`;
  `refresh_home_parks()` in the sheet builder re-derives that lookup table from `紀錄`
  (modal home venue per club), with `HOME_PARK_OVERRIDES` pinning names MLB feeds under
  a sponsor.

Re-run it only to rebuild from scratch — it deletes the tabs it owns and recreates them.
`--restyle` refreshes just the colour layer, number formats and date dropdowns on the
live tabs, leaving formulas and `設定` untouched:

```bash
uv run python migration/add_mlb_matchup_sheets.py --dry-run   # plan + formula sample
uv run python migration/add_mlb_matchup_sheets.py
uv run python migration/add_mlb_matchup_sheets.py --restyle
```

Note: ERA divides `客自責`/`主自責` by the decimal starter innings `客先局`/`主先局` (not
`客局數`, which is in `.1`/`.2` notation and cannot be summed).

### Team codes (`baseball/mlb_teams.py`)

MLB Stats API changed the Athletics' abbreviation from `OAK` to `ATH` for 2025, when
the club dropped "Oakland". `紀錄` holds a decade keyed on `OAK`, and every sheet that
aggregates by team label matches on it — `MLB勝敗表` returned 0 wins for the franchise
as soon as its window reached 2025. So one code per franchise is kept:

- `canonical_team_code()` maps the API's code on the way in, for both `紀錄`
  (`update_mlb_record.py`) and the odds join (`baseball/mlb_games.py`).
- `migration/normalize_mlb_team_codes.py` fixed the 282 rows written before that
  (2025/3/27–2026/8/12). It backs every change up to `.cache/` first, only touches
  `客隊隊伍` / `主隊隊伍`, and is a no-op on a second run.

If MLB renames another club mid-history, add it to `TEAM_CODE_ALIASES` and re-run that
script — nothing else needs to know.

---

## Odds / 盤口 (`baseball/pinnacle_odds.py`)

Snapshots pre-game betting lines from the PS3838 public compact feed
(`/sports-service/sv/compact/events`, no login and no API access needed) and
appends them to a `盤口` worksheet, so opening and closing lines can be compared
against recorded results to measure edge.

One scraper serves both leagues; pick one with `--league`:

| League | `--league` | PS3838 league id | Target spreadsheet | Join key    |
| ------ | ---------- | ---------------- | ------------------ | ----------- |
| NPB    | `npb`      | 187703           | NPB (with 彙資)     | —           |
| MLB    | `mlb`      | 246              | MLB (with 紀錄)     | `mlb_game_pk` |

```bash
uv run python -m baseball.pinnacle_odds --league mlb --dry-run   # print, write nothing
uv run python -m baseball.pinnacle_odds --league mlb             # append snapshot rows
```

### Notes

- One row per (event, period): `final` (full game) and `half` (first 5 innings),
  each with moneyline, the main total, the main run line, and JSON of **every**
  total/spread line for backtesting. MLB half markets have no moneyline.
- Period `3` is the 1st-inning 3-way market. Deliberately not recorded.
- Only pre-game lines are kept. Once a game starts it moves to the feed's live
  (走地) bucket and is skipped, so `--include-live` is for inspection only.
- Baseball lines only appear a few hours before first pitch, not days ahead.
- MLB rows carry `mlb_game_pk`, `home_abbr`/`away_abbr`, and MLB's own
  `officialDate` as `game_date`, all resolved from the MLB Stats API schedule so
  every row joins to `紀錄`. Teams are matched by alias (PS3838 says "Arizona
  Diamondbacks" where the API's `teamName` is "D-backs") plus nearest start
  time, which also disambiguates doubleheaders.
- PS3838 geo-blocks datacenter IPs, so CI tunnels through the Decodo residential
  proxy (`DECODO_PROXY_URL`). Locally, with no proxy set, requests go direct.
- Override the target sheet with `ODDS_SPREADSHEET_KEY` (NPB) or
  `MLB_ODDS_SPREADSHEET_KEY` (MLB).

### CPBL

PS3838 does not appear to book CPBL — only MLB, NPB and KBO have shown up in the
feed. Since the feed lists only leagues with currently open markets, confirm on a
CPBL game day, 2–4 hours before first pitch:

```bash
uv run python migration/probe_cpbl_odds.py
```

---

## GitHub Secrets

| Secret               | Used by        | Description                             |
| -------------------- | -------------- | --------------------------------------- |
| `GOOGLE_CREDENTIALS` | CPBL, NPB, MLB | Google service account JSON (full body) |
| `SPREADSHEET_KEY`    | CPBL           | Google Sheets spreadsheet ID for CPBL   |
| `NORDVPN_TOKEN`      | CPBL           | NordVPN token for WireGuard tunnel      |
| `DECODO_PROXY_URL`   | Odds           | Decodo residential proxy for PS3838     |
| `TELEGRAM_BOT_TOKEN` | CPBL, NPB, MLB | Telegram bot token for failure alerts   |
| `TELEGRAM_CHAT_ID`   | CPBL, NPB, MLB | Telegram chat ID for failure alerts     |

## GitHub Variables

Optional repository variables used by `.github/workflows/cpbl_scheduler.yml`:

| Variable                           | Default | Description                                                                      |
| ---------------------------------- | ------- | -------------------------------------------------------------------------------- |
| `NORDVPN_COUNTRY_ID`               | `108`   | Country filter for the fallback Nord recommendation query                        |
| `NORDVPN_STATION_ALLOWLIST`        | —       | Comma-separated Nord `station` IPs to prefer before fallback                     |
| `NORDVPN_STATION_PREFIX_ALLOWLIST` | —       | Comma-separated IP prefixes to prefer before fallback, for example `94.156.205.` |
| `NORDVPN_HOSTNAME_ALLOWLIST`       | —       | Comma-separated Nord hostnames to prefer before fallback                         |

## Local Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Create a .env file
echo "GOOGLE_CREDENTIALS_FILE=path/to/credentials.json" >> .env
echo "SPREADSHEET_KEY=your_spreadsheet_id" >> .env

python cpbl.py   # runs run_once() for current year
python npb.py    # runs run_once() for all NPB teams
```

For an NPB manual backfill after today's games have already finished, keep the
近十場 matchup order anchored on today with:

```bash
python npb.py --matchup-date today
```

You can also set `NPB_MATCHUP_DATE=today` or pass a date such as
`--matchup-date 2026-05-10`.

### Prediction Ledger

NPB predictions are recorded in the separate prediction spreadsheet. A pre-game
command resolves the game ID by home team and writes a pending row:

```bash
python npb.py --create-prediction 巨人 --market final_winner --pick 巨人 --rate 0.92 --stake 10
python npb.py --create-prediction 巨人 --market half_winner --pick 巨人 --rate 0.92 --stake 10
python npb.py --create-prediction 巨人 --market half_total --pick over --line 4.5 --rate 0.92 --stake 10
python npb.py --create-prediction 巨人 --market final_total --pick under --line 8.5 --rate 0.92 --stake 10
```

Short flags are also supported:

```bash
python npb.py --predict 巨人 -p 巨人 -r 0.92
python npb.py --predict 巨人 -m half_total -p over -l 4.5 -r 0.92
```

The positional value is the home team name, not a game ID. Valid home teams are:
`巨人`, `ヤクルト`, `DeNA`, `中日`, `阪神`, `広島`, `西武`, `日本ハム`,
`ロッテ`, `オリックス`, `ソフトバンク`, `楽天`. The command only looks at
today's and tomorrow's unstarted games, resolves the game ID, then prints the
date, matchup, and starters for validation before recording.

For prompted input, omit the game ID:

```bash
python npb.py --predict
```

It will list valid home team options, then ask for `Home team`, `Market`,
`Pick`, `Line` when needed, `Rate`, and `Stake`. Press Enter to accept defaults
such as `final_winner` for market and `10.0` for stake.

For the shortest daily command, add an alias:

```bash
alias npbp='cd /Users/evansmac/cpbl && uv run python npb.py --predict'
npbp
```

Use `--dry-run` to print the prediction text without writing anything.
After the game is scraped as finished, the NPB run resolves pending predictions
for that game, then updates the result and balance in the prediction sheet.
An empty prediction sheet starts at `0`; existing sheets continue from the last
non-empty `balance_after`. A 10-unit win at rate `0.92` from zero becomes `9.2`,
while a loss subtracts the stake.

The four supported markets are `half_winner` (winner through 5 innings),
`final_winner`, `half_total` (combined runs through 5 innings), and
`final_total`. Total markets require `--line`; equality with the line is a push.
