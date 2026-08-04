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
│   └── mlb_games.py                 # Resolves MLB gamePk for an odds event
└── .github/workflows/
    ├── cpbl_scheduler.yml           # Cron: every 30 min, 07:00–16:00 UTC (via Japan VPN)
    ├── mlb_record_scheduler.yml     # Cron: daily, 12:00 UTC
    ├── npb_scheduler.yml            # Cron: every 30 min, 08:00–14:00 UTC
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

Runs once daily at **12:00 UTC** (21:00 JST). The record command checks the last 3 calendar dates so delayed finalization and timezone edge cases are picked up without duplicating rows:

```bash
uv run python migration/update_mlb_record.py --recent-days 3
uv run python migration/update_mlb_last10.py
```

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
