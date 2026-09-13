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
│   ├── pinnacle_odds.py             # PS3838 odds scraper (NPB + MLB + CPBL) -> 盤口 sheets
│   ├── npb_audit.py                 # Comparator for the weekly NPB history audit
│   ├── npb_pitching_splits.py       # Starter / bullpen / total ERA by venue
│   ├── npb_pitching_splits_sheet.py # Writes and paints the 投手主客 tab
│   ├── npb_record_sync.py           # Mirrors 分析表紀錄 -> 紀錄總表 (other workbook)
│   ├── npb_diary.py                 # Keeps the 2026・野球日記 tab current
│   ├── npb_box.py                   # Reads a pitching line out of an npb.jp box score
│   ├── npb_tenki.py                 # tenki.jp hourly forecast, by ballpark
│   ├── npb_weather.py               # Records each game's weather, which cannot be backfilled
│   ├── mlb_games.py                 # Resolves MLB gamePk for an odds event
│   ├── cpbl_games.py                # Resolves CPBL GameSno for an odds event
│   └── odds_history.py              # Backfills 盤口 from The Odds API's archive
├── migration/
│   ├── audit_npb_history.py         # Re-scrapes recent NPB games and diffs them
│   ├── backfill_npb_box.py          # Caches every npb.jp box score from 2016 on
│   ├── write_npb_starter_log.py     # Lays the cached box scores out as 先発明細
│   └── add_npb_pitching_splits_sheet.py  # Rebuilds 投手主客 by hand
└── .github/workflows/
    ├── cpbl_scheduler.yml           # Cron: every 30 min, 07:00–16:00 UTC (via Japan VPN)
    ├── mlb_record_scheduler.yml     # cron-job.org: daily, 21:00 JST
    ├── npb_scheduler.yml            # Cron: every 30 min, 08:00–14:00 UTC
    ├── npb_audit_scheduler.yml      # cron-job.org: weekly, Monday 14:00 JST
    ├── npb_odds_scheduler.yml       # Cron: every 30 min, 01:00–10:30 UTC
    ├── mlb_odds_scheduler.yml       # Cron: hourly 13:00–16:00, then every 30 min 17:00–03:30 UTC
    └── cpbl_odds_scheduler.yml      # cron-job.org: every 30 min, around the clock
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

### 紀錄總表 sync (`baseball/npb_record_sync.py`)

Mirrors **分析表紀錄** into the 紀錄總表 tab of the separate
「プロ野球データ分析」 workbook. The two are the same 83-column game log — the
headers were compared cell by cell and differ only in column A, which carries a
pre-filled 編號 on the target and is blank on the source — so this is a
sheet-to-sheet copy, not a second scrape. That matters for 被壘打 (total bases
off the starter), which 分析表紀錄 has and no Yahoo box score exposes.

```bash
uv run python baseball/npb_record_sync.py --dry-run
uv run python baseball/npb_record_sync.py
```

- Games are matched on **(日期, 客場球隊, 主場球隊)**, not on row position, so a
  game NPB publishes late — or one the weekly audit corrects — updates its own
  row instead of shifting everything below it.
- Only **B–CE** is written. Column A's 編號 and the per-row formulas in CF–CK and
  CZ/DA belong to 紀錄總表 and are already filled down past the end of the season.
- The header rows are compared before anything is written; if they have drifted
  apart the run aborts rather than writing into the wrong columns.
- A run with nothing new writes nothing and says so.

Runs as the last step of **npb_scheduler.yml**, straight after the sweep that
writes 分析表紀錄 — no schedule of its own, the way `mlb_record_scheduler.yml`
chains its three scripts. Every sweep therefore leaves 紀錄總表 at most 30
minutes behind, and it costs a handful of API calls and no Yahoo traffic.

The step is `continue-on-error`: the target workbook belongs to someone else,
and a permission change there must not turn the NPB updater red. It carries its
own Telegram alert so a failure there is not reported as a failed sweep.
### 野球日記 (`baseball/npb_diary.py`)

Keeps the **2026・野球日記** tab of the analysis workbook current — a calendar of
the season, one row per day and one column per team, ported from the
hand-maintained 2023 tab. Each game is written once, in the **home** team's
column, as `客先発  客点-主点  主先発`.

What the layout encodes, all of it reproduced from the 2023 sheet:

| | |
| --- | --- |
| neutral venue | spelled inside the score: `石田  3  (京セラ)  6  青柳` |
| サヨナラ | `。` instead of `-`; `*` when it was a walk-off **home run** |
| score colours | winner red, loser green, a draw blue, all bold; the separator rides with the winner |
| 雨天中止 | `<相手コード> 戦 雨 天 中 止` in the home column, both teams' cells blue |
| no game for that team | purple; a day with no games at all is grey |
| 予告先発 | a game not yet played shows its two announced starters and no score |
| 登録抹消 | the day's 公示, as a note on the black divider between the leagues |
| season phases | 交流戦 pink, オールスター dark purple, on the Monday row before each starts |

```bash
uv run python baseball/npb_diary.py                # incremental, what the scheduler runs
uv run python baseball/npb_diary.py --dry-run
uv run python baseball/npb_diary.py --full         # rebuild the whole tab
```

- The default run rebuilds only a **window** around today (3 days back, 21
  forward). Everything outside it is settled — a score does not change, and
  neither does a rainout — so Yahoo is asked for about two dozen schedule pages
  instead of the season's 226. That is what makes it cheap enough to run on
  every sweep; a full sweep of Yahoo's schedule starts drawing 500s partway
  through.
- Scores and starters come from 賽錄, which the step above has just written.
  Yahoo is needed only for two things 賽錄 cannot know: which games were
  **中止**, and who is scheduled to play on days not yet reached.
- 予告先発 comes from npb.jp in a single request, and only ever covers today —
  that is all NPB announces. The names are put through the same season-wide
  display map as the rest of the sheet, so a pitcher is spelled the same way
  whether his game has been played or not. A game announced but then called off
  still shows 中止: the status wins.
- A walk-off home run is not visible in a box score — a walk-off single and a
  walk-off homer look identical — so those are read off NPB's own play-by-play,
  checking the game's last play. The score URLs come from npb.jp's monthly
  schedule pages rather than being guessed: the game-number suffix is not always
  `-01`, and 交流戦 lives on its own page.
- Pitchers are shown by family name, extended just far enough to be unambiguous
  across the whole season (髙橋光 / 髙橋遥 / 髙橋宏), with hand-written
  shorthands in `NAME_OVERRIDES` winning over the automatic form. A surname that
  becomes ambiguous *inside* the window would leave older rows spelled the old
  way; the run detects that and asks for a `--full` pass.
- Cells are fitted to the 130px column by squeezing the padding, then the font,
  then the venue, and only then clipping a name — the order the 2023 sheet was
  worked by hand.

`--full` also creates the tab, sets the column widths, paints the season-phase
banners and lays out the オールスター block, none of which an incremental run
touches.

### 投手主客 (`baseball/npb_pitching_splits_sheet.py`)

Each team's ERA as starters, as a bullpen and as a whole staff, at home and on
the road, ranked inside its own league — written to the **投手主客** tab of the
analysis workbook.

NPB publishes no relief total. 分析表紀錄 carries, for both sides of every game,
the starting pitcher's line **and** the team's whole-game line, so the bullpen
is the second minus the first — one source, no second scrape.

- Rebuilt whole at the end of every daily run, once 分析表紀錄 has been written.
  `migration/add_npb_pitching_splits_sheet.py` is the same call by hand, with
  `--dry-run` to print the table instead.
- 央聯 on the left, 洋聯 on the right. One split on the page at a time — 全場 /
  主場 / 客場, chosen from the dropdown in `B2` — and each table is sorted by
  that split's own ERA, ascending.
- The tables are `SORT()` over blocks kept in hidden rows at the bottom, so
  changing the dropdown re-keys them without the sheet being rebuilt. **Those
  rows must not be deleted**; the tab empties if they are.
- A venue split under 20 innings is shown but not ranked, and a segment with no
  innings has no ERA at all rather than a `0.00` that would rank first.
- Interleague games count towards a team's own totals, so rows are filed by team
  rather than by the 聯盟 label on the game.

### 先発明細 / box-score backfill (`baseball/npb_box.py`)

`賽錄` records a starter's innings and earned runs and nothing else, so any
question about *how* he got there — his walks, his pitch count — had only the
current season behind it. npb.jp still serves per-pitcher box scores back to
2016, and this reads them.

```bash
python migration/backfill_npb_box.py --years 2026 --months 8   # a trial
python migration/backfill_npb_box.py                           # 2016 onward
python migration/write_npb_starter_log.py --dry-run
python migration/write_npb_starter_log.py
```

- Yahoo serves only the current season — every earlier game comes back empty
  rather than 404 — which is why this uses npb.jp. 2015 and earlier have no
  schedule pages at all, so that is where the history ends.
- Two page layouts. Since the redesign the pitching tables carry ids
  (`tablefix_t_p` / `tablefix_b_p`); up to 2020 they sit in a `table_pitcher`
  wrapper instead and the line score names teams only in full.
- **A refused request is answered with a 200 and a page with no tables**,
  which is indistinguishable from a rainout except that a rainout says so.
  `read_box()` raises rather than returning None for the ambiguous case, so a
  throttled fetch can never be cached as a game nobody pitched. A run of them
  stops the backfill; every game already fetched is cached, so a re-run
  resumes.
- A monthly schedule page lists games past its own month, so games dated today
  or later are skipped rather than fetched — their box pages are served empty
  and would otherwise trip the same stop.
- Verified against 分析表紀錄 over 84 starter-starts: 局數, 打者, 安打, HR and
  責失 match exactly, and so does 四球 **once read as 四死**. That column holds
  walks plus hit batsmen despite its name, as does 打数, which is really
  batters faced. `先発明細` keeps 四球 and 死球 in separate columns; merging
  them could not be undone.
- Innings are written as a decimal. `.1`/`.2` notation cannot be summed, and
  every rolling window this table feeds is a sum.
- Relief appearances are parsed and cached but not written to the sheet —
  bullpen fatigue is a different question with a different shape.

### 天氣 (`baseball/npb_weather.py`)

One row per game per day in the **天氣** tab: condition, temperature, rain,
and the wind direction resolved against the park's bearing.

```bash
python -m baseball.npb_weather --dry-run
python -m baseball.npb_weather
```

- `npb_starters` already reads this forecast for the Telegram broadcast and
  then discards it. **The reading cannot be recovered later**: the forecast
  covers today and nothing else, and npb.jp's box scores carry no weather at
  all. A day not recorded on the day is gone, which is why this rides along on
  the half-hourly sweep.
- The forecast comes from **tenki.jp** (`baseball/npb_tenki.py`), with Yahoo as
  the fallback. tenki.jp steps an hour at a time where Yahoo steps three, so an
  18:00 first pitch is read rather than approximated, and it keeps answering
  after first pitch where Yahoo's game-card forecast disappears.
- tenki.jp forecasts municipalities, not ballparks, so the grounds are mapped by
  hand. The thirteen home grounds are keyed on their **ward** — Yokohama is
  437km² and its ground is on the harbour, so a city-wide reading would average
  the sea breeze away. The 地方球場 are keyed on the city: they see single
  figures of games a decade, and ward names collide nationwide (中央区 alone
  resolves to Tokyo's). Every open-air ground used since 2016 is covered.
- `RAIN_FLAG_MM` changed with the source. Yahoo reported a three-hour total,
  where 1.0 was the mark; tenki.jp reports the rate in the hour of first pitch,
  a third of the number for the same weather. 0.5 is a judgement, not a
  calibration — this log is what will eventually settle it, against the 中止
  games already marked in the box-score cache.
- Keyed on (date, park) and overwritten by each sweep, so what survives is the
  last reading before first pitch rather than a dozen near-identical rows.
- A missing number stays blank rather than becoming zero — 0mm of rain is a dry
  evening, a blank is a forecast never seen.
- Covered parks are flagged; six of the twelve are roofed, and rain and wind
  mean nothing there.

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

One scraper serves all three leagues; pick one with `--league`:

| League | `--league` | PS3838 league id | PS3838 league name | Target spreadsheet | Join key        |
| ------ | ---------- | ---------------- | ------------------ | ------------------ | --------------- |
| NPB    | `npb`      | 187703           | 日本職業棒球賽       | NPB (with 彙資)     | —               |
| MLB    | `mlb`      | 246              | MLB                | MLB (with 紀錄)     | `mlb_game_pk`   |
| CPBL   | `cpbl`     | 208753           | 台北 - 職業聯賽      | CPBL (with 賽程)    | `cpbl_game_sno` |

```bash
uv run python -m baseball.pinnacle_odds --league mlb --dry-run   # print, write nothing
uv run python -m baseball.pinnacle_odds --league mlb             # append snapshot rows
uv run python -m baseball.pinnacle_odds --league cpbl            # CPBL, into its own 盤口 tab
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
- Override the target sheet with `ODDS_SPREADSHEET_KEY` (NPB),
  `MLB_ODDS_SPREADSHEET_KEY` (MLB) or `CPBL_ODDS_SPREADSHEET_KEY` (CPBL).

### CPBL

PS3838 does book CPBL, as **台北 - 職業聯賽** (league id 208753) — a name with
neither "CPBL" nor "中華職棒" in it, which is why it went unnoticed until the
scraper started reading the whole board (`mk=3`) instead of only the TODAY one.

- The board carries the full game only. CPBL rows are all `period = final`;
  there is no 1st-5-innings market to record.
- `cpbl_game_sno` and `kind_code` are resolved by `baseball/cpbl_games.py` from
  CPBL's own schedule, so a row joins to `賽程` (column B) the way MLB rows join
  to `紀錄` by gamePk. Teams are matched by the short names `賽程` stores
  (`樂天`, `統一7-ELEVEn`, …) plus nearest start time, which also separates a
  doubleheader.
- `kind_code` tells `正式賽` (`A`) from `熱身賽` (`G`); the two number their
  games separately, so a `GameSno` alone does not identify a game. The preseason
  schedule is only fetched when a game finds no regular-season match.
- A game that cannot be matched — or a run where cpbl.com.tw is unreachable —
  still gets its snapshot, with a blank `cpbl_game_sno`; date and both team
  names are on the row either way.
- The job reads cpbl.com.tw as well as PS3838, and both refuse datacenter IPs,
  so CI needs `DECODO_PROXY_URL` for the same reason the CPBL scraper does.

To see what PS3838 is booking right now (on a game day, 2–4 hours before first
pitch):

```bash
uv run python migration/probe_cpbl_odds.py
```

### Backfilling the ledger (`baseball/odds_history.py`)

The scraper above started on 2026-07-18 — 259 NPB games. Telling a 3% edge
from noise needs something like 4,400 settled bets, so the ledger is three
seasons short and waiting is the only way it fills.

[The Odds API](https://the-odds-api.com) keeps Pinnacle's board back to
**2020-06-06** (10-minute snapshots; 5-minute from 2022-09) and carries
`baseball_npb` and `baseball_mlb`. Rows land in the same `盤口` tab, in the
scraper's own column order, so backfilled and live rows are one table.

```bash
export ODDS_API_KEY=...

# does Pinnacle really cover this league, in the archive? ~90 credits
uv run python -m baseball.odds_history probe --league npb

# what a range costs, spending nothing
uv run python -m baseball.odds_history backfill --league npb \
    --start 2026-03-27 --end 2026-07-17 --dry-run

uv run python -m baseball.odds_history backfill --league npb \
    --start 2026-03-27 --end 2026-07-17 --snapshot-type close
```

The archive is a **snapshot series, not a single opening number** — every 10
minutes from 2020-06-06, every 5 minutes from 2022-09 — so `--leads` is a
choice about how much of the line's path to buy. Each lead time is another
full-price request, and each sample labels itself: the furthest out is the
`open`, the nearest the `close`, anything between `interim`.

Over everything the archive holds (1,207 NPB game days, two start times each):

| `--leads` | Points | Requests | Credits | Plan |
| --------- | -----: | -------: | ------: | ---- |
| `10` (close only) | 1 | 2,414 | ~72,400 | $59 / 100K |
| `720 10` (open + close, **default**) | 2 | 4,828 | ~144,800 | $119 / 5M |
| `720 240 10` | 3 | 7,242 | ~217,300 | $119 / 5M |
| every 2h for 12h | 7 | 16,898 | ~506,900 | $119 / 5M |
| every 30m for 6h | 13 | 31,382 | ~941,500 | $119 / 5M |

Filling just 2026 up to the day the scraper started (3/27–7/17, 98 game days)
is 196 requests / ~5,900 credits at the default.

The API bills `10 × markets × regions` per request, so every run prints its
plan first and `--dry-run` spends nothing.

Two things keep the bill down, and one thing to know before trusting the data:

- **One request per start time, not per game.** An NPB card that all starts at
  18:00 is a single snapshot; every row still carries its own `mins_to_start`.
- **Off days are skipped.** The `.cache/npb_box` filenames already say which
  days had games — two winters, most Mondays and the all-star break come out,
  which is what takes the full backfill from ~137,000 credits to ~72,000. Pass
  `--every-day` to disable.
- **The featured endpoint returns Pinnacle's main line only**, not the whole
  ladder, so backfilled `all_totals` / `all_spreads` hold a single entry. Good
  enough for closing-line value and for modelling the main number; *not* enough
  for `baseball/asian_lines.py`, which needs the full margin curve.
- **The two feeds do not name the same main line.** Read side by side on
  2026-09-13, all four moneylines matched to the third decimal — but three of
  four totals did not, because this scraper picks the most balanced line off
  PS3838's whole ladder while The Odds API returns whatever Pinnacle flags as
  featured. Every row therefore carries a `source` column (`ps3838` /
  `the_odds_api`); compare moneylines freely, and condition on `source` before
  comparing a total or a run line.

| 2026-09-13 | The Odds API | PS3838 (this scraper) |
| --- | --- | --- |
| 日本ハム @ 西武 | ML 2.11/1.8 · O/U **6.0** | ML 2.11/1.8 · O/U **6.5** |
| 中日 @ 阪神 | ML 2.55/1.56 · O/U 5.5 | ML 2.55/1.564 · O/U 5.5 |
| 広島 @ ヤクルト | ML 2.02/1.87 · RL **−1.5** | ML 2.02/1.869 · RL **+1.5** |
| 巨人 @ DeNA | ML 2.06/1.83 · O/U **8.0** | ML 2.06/1.833 · O/U **7.5** |

The Odds API does not carry CPBL — only NPB, MLB, KBO, MiLB and NCAA.

---

## GitHub Secrets

| Secret               | Used by        | Description                             |
| -------------------- | -------------- | --------------------------------------- |
| `GOOGLE_CREDENTIALS` | CPBL, NPB, MLB | Google service account JSON (full body) |
| `SPREADSHEET_KEY`    | CPBL, Odds     | Google Sheets spreadsheet ID for CPBL   |
| `NORDVPN_TOKEN`      | CPBL           | NordVPN token for WireGuard tunnel      |
| `DECODO_PROXY_URL`   | CPBL, Odds     | Decodo residential proxy for PS3838 and cpbl.com.tw |
| `ODDS_API_KEY`       | Odds backfill  | The Odds API key (manual runs only)     |
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
