# Baseball Stats Scrapers

Automated scrapers that pull game results from CPBL and NPB, then write stats to Google Sheets. Both run on GitHub Actions cron schedules.

---

## Repository Structure

```
.
├── cpbl.py                          # CPBL scraper
├── npb.py                           # NPB scraper
├── requirements.txt
├── lastTenGames.gs                  # Google Apps Script for CPBL 近十場 sheet
├── lastTenGamesPreseason.gs         # Google Apps Script for CPBL 熱身賽 近十場 sheet
└── .github/workflows/
    ├── cpbl_scheduler.yml           # Cron: every 30 min, 07:00–16:00 UTC (via Japan VPN)
    └── npb_scheduler.yml            # Cron: every 30 min, 08:00–14:00 UTC
```

---

## CPBL (`cpbl.py`)

Scrapes [stats.cpbl.com.tw](https://stats.cpbl.com.tw) by default for regular season (`A`) game results and writes box score data to Google Sheets. The older [cpbl.com.tw](https://www.cpbl.com.tw) JSON endpoints are still available by setting `CPBL_DATA_SOURCE=legacy`.

### Workflow

1. Reads existing game numbers from the target worksheet
2. In `stats` mode, checks a small game-number window around the latest recorded game via `GET /schedule/{year}-{kindCode}-{gameSno}`
3. Extracts the embedded Next.js game JSON, adapts it to the existing box score format, and writes a 125-column row to the target worksheet
4. After all games are processed, refreshes the **彙資** sheet with today's games (up to 3)

### Worksheets

| Kind Code | Worksheet  | Description     |
|-----------|------------|-----------------|
| `A`       | 賽程        | Regular season  |
| `G`       | 熱身賽賽程   | Preseason       |
| —         | 彙資        | Today's summary |

### Scheduler

Runs every 30 minutes between **07:00–16:00 UTC** (15:00–00:00 Taiwan time). `stats.cpbl.com.tw` usually does not require the old CSRF/token POST flow, so this path should be less sensitive to GitHub Actions IP blocking.

The workflow defaults to NordVPN `country_id=108` for recommendations, but it can prefer a known-good server IP or hostname first. If CPBL allows a specific Nord `station` IP such as `94.156.205.102`, set `NORDVPN_STATION_ALLOWLIST=94.156.205.102`. If the acceptable servers all share a prefix, such as `94.156.205.*`, set `NORDVPN_STATION_PREFIX_ALLOWLIST=94.156.205.` and the workflow will pick the lowest-load matching server when it is available.

Optional CPBL source variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CPBL_DATA_SOURCE` | `stats` | Use `stats` for `stats.cpbl.com.tw`; use `legacy` for the old `www.cpbl.com.tw` schedule/box endpoints. |
| `CPBL_STATS_LOOKBACK` | `6` | Re-check this many game numbers before the latest recorded game. |
| `CPBL_STATS_LOOKAHEAD` | `12` | Check this many game numbers after the latest recorded game. |
| `CPBL_STATS_SCAN_START` | `1` | First game number to scan when the worksheet has no existing games. |
| `CPBL_STATS_SCAN_END` | `CPBL_STATS_SCAN_START + CPBL_STATS_LOOKAHEAD` | Last game number to scan when the worksheet has no existing games. |

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

| League | Worksheet  |
|--------|------------|
| 央盟    | 近十場a    |
| 洋盟    | 近十場b    |

### Scheduler

Runs every 30 minutes between **08:00–14:00 UTC** (17:00–23:00 JST), covering NPB evening games. No VPN required.

---

## GitHub Secrets

| Secret                  | Used by        | Description                              |
|-------------------------|----------------|------------------------------------------|
| `GOOGLE_CREDENTIALS`    | CPBL, NPB      | Google service account JSON (full body)  |
| `SPREADSHEET_KEY`       | CPBL           | Google Sheets spreadsheet ID for CPBL    |
| `NORDVPN_TOKEN`         | CPBL           | NordVPN token for WireGuard tunnel       |
| `TELEGRAM_BOT_TOKEN`    | CPBL, NPB      | Telegram bot token for failure alerts    |
| `TELEGRAM_CHAT_ID`      | CPBL, NPB      | Telegram chat ID for failure alerts      |

## GitHub Variables

Optional repository variables used by `.github/workflows/cpbl_scheduler.yml`:

| Variable                    | Default | Description |
|----------------------------|---------|-------------|
| `NORDVPN_COUNTRY_ID`       | `108`   | Country filter for the fallback Nord recommendation query |
| `NORDVPN_STATION_ALLOWLIST`| —       | Comma-separated Nord `station` IPs to prefer before fallback |
| `NORDVPN_STATION_PREFIX_ALLOWLIST` | — | Comma-separated IP prefixes to prefer before fallback, for example `94.156.205.` |
| `NORDVPN_HOSTNAME_ALLOWLIST` | —     | Comma-separated Nord hostnames to prefer before fallback |

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
