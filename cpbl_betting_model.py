import argparse
import csv
import json
import math
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import mean

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


SPREADSHEET_KEY_ENV = "SPREADSHEET_KEY"
DEFAULT_OUTPUT = "data/cpbl_training_dataset.csv"

BETTING_YEAR_SEGMENTS = [
    (3, 229, 2020),
    (230, 525, 2021),
    (526, 795, 2022),
    (796, 1095, 2023),
    (1096, 1450, 2024),
    (1451, 1808, 2025),
    (1809, 1863, 2026),
]

MARKETS = {
    "three_spread": {
        "upper_col": 7,
        "line_col": 8,
        "away_result_col": 34,
        "home_result_col": 35,
    },
    "half_spread": {
        "upper_col": 10,
        "line_col": 11,
        "away_result_col": 36,
        "home_result_col": 37,
    },
    "seven_spread": {
        "upper_col": 13,
        "line_col": 14,
        "away_result_col": 38,
        "home_result_col": 39,
    },
    "full_spread": {
        "upper_col": 4,
        "line_col": 5,
        "away_result_col": 40,
        "home_result_col": 41,
    },
    "three_total": {
        "line_col": 9,
        "result_col": 42,
    },
    "half_total": {
        "line_col": 12,
        "result_col": 43,
    },
    "seven_total": {
        "line_col": 15,
        "result_col": 44,
    },
    "full_total": {
        "line_col": 6,
        "result_col": 45,
    },
}


def cell(row, idx, default=""):
    if idx >= len(row):
        return default
    value = row[idx]
    return str(value).strip() if value is not None else default


def parse_float(value, default=0.0):
    text = str(value).strip()
    if not text or text.upper() == "X":
        return default
    try:
        return float(text)
    except ValueError:
        return default


def parse_date(value):
    text = str(value).strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def month_day(date_value):
    return f"{date_value.month}/{date_value.day}"


def parse_market_line(raw):
    """
    Parse sheet line formats:
    - 1+25  -> line=1.0, water=25
    - 0-50  -> line=0.0, water=-50
    - 7-75  -> line=7.0, water=-75
    - 4.5   -> line=4.5, water=0
    """
    text = str(raw).strip()
    if not text:
        return None, None
    match = re.fullmatch(r"([+-]?\d+(?:\.\d+)?)([+-]\d+)?", text)
    if not match:
        return None, None
    line = float(match.group(1))
    water = int(match.group(2) or 0)
    return line, water


def year_for_betting_row(row_number):
    for start, end, year in BETTING_YEAR_SEGMENTS:
        if start <= row_number <= end:
            return year
    return None


def get_readonly_spreadsheet():
    load_dotenv(dotenv_path="/Users/evansmac/cpbl/.env")
    scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    else:
        creds_file = os.environ.get("GOOGLE_CREDENTIALS_FILE")
        creds = Credentials.from_service_account_file(creds_file, scopes=scope)
    return gspread.authorize(creds).open_by_key(os.environ[SPREADSHEET_KEY_ENV])


def read_sheet_rows():
    spreadsheet = get_readonly_spreadsheet()
    schedule_rows = spreadsheet.worksheet("賽程").get("A1:EN3200")
    betting_rows = spreadsheet.worksheet("過盤紀錄").get("A1:AU2301")
    return schedule_rows, betting_rows


def build_betting_index(betting_rows):
    indexed = {}
    skipped = []
    for row_number, row in enumerate(betting_rows[2:], start=3):
        year = year_for_betting_row(row_number)
        md = cell(row, 0)
        away = cell(row, 1)
        home = cell(row, 2)
        if not year or not re.fullmatch(r"\d{1,2}/\d{1,2}", md) or not away or not home:
            continue
        key = (year, md, away, home)
        if key in indexed:
            skipped.append((row_number, "duplicate betting key", key))
            continue
        indexed[key] = {"row_number": row_number, "row": row}
    return indexed, skipped


def parse_schedule_rows(schedule_rows):
    parsed = []
    for row_number, row in enumerate(schedule_rows[1:], start=2):
        date_value = parse_date(cell(row, 2))
        away = cell(row, 3)
        home = cell(row, 5)
        if not date_value or not away or not home:
            continue
        parsed.append(
            {
                "row_number": row_number,
                "game_no": cell(row, 1),
                "date": date_value,
                "year": date_value.year,
                "away": away,
                "away_starter": cell(row, 4),
                "home": home,
                "home_starter": cell(row, 6),
                "park": cell(row, 7),
                "umpire": cell(row, 8),
                "away_score": parse_float(cell(row, 21)),
                "away_hits": parse_float(cell(row, 22)),
                "home_score": parse_float(cell(row, 36)),
                "home_hits": parse_float(cell(row, 37)),
                "away_starter_ip": parse_float(cell(row, 39)),
                "away_starter_er": parse_float(cell(row, 51)),
                "home_starter_ip": parse_float(cell(row, 65)),
                "home_starter_er": parse_float(cell(row, 77)),
                "away_hand": cell(row, 91),
                "home_hand": cell(row, 92),
            }
        )
    parsed.sort(key=lambda item: (item["date"], int(item["row_number"])))
    return parsed


@dataclass
class RollingHistory:
    team_games: dict = field(default_factory=lambda: defaultdict(list))
    starter_games: dict = field(default_factory=lambda: defaultdict(list))
    park_totals: dict = field(default_factory=lambda: defaultdict(list))
    umpire_totals: dict = field(default_factory=lambda: defaultdict(list))
    h2h_games: dict = field(default_factory=lambda: defaultdict(list))

    def _avg(self, values, default=0.0):
        return mean(values) if values else default

    def _team_features(self, team, prefix):
        games = self.team_games[team]
        features = {}
        for n in (5, 10):
            sample = games[-n:]
            features[f"{prefix}_last{n}_runs_for"] = self._avg([g["runs_for"] for g in sample])
            features[f"{prefix}_last{n}_runs_against"] = self._avg(
                [g["runs_against"] for g in sample]
            )
            features[f"{prefix}_last{n}_hits_for"] = self._avg([g["hits_for"] for g in sample])
            features[f"{prefix}_last{n}_hits_against"] = self._avg(
                [g["hits_against"] for g in sample]
            )
            features[f"{prefix}_last{n}_win_rate"] = self._avg(
                [1.0 if g["runs_for"] > g["runs_against"] else 0.0 for g in sample]
            )
        features[f"{prefix}_games_seen"] = len(games)
        return features

    def _starter_features(self, starter, prefix):
        games = self.starter_games[starter] if starter else []
        sample = games[-5:]
        ip = [g["ip"] for g in sample]
        er = [g["er"] for g in sample]
        total_ip = sum(ip)
        features = {
            f"{prefix}_starter_seen": len(games),
            f"{prefix}_starter_last5_ip": self._avg(ip),
            f"{prefix}_starter_last5_era": (sum(er) * 9 / total_ip) if total_ip else 0.0,
            f"{prefix}_starter_last5_qs_rate": self._avg(
                [1.0 if g["ip"] >= 6 and g["er"] <= 3 else 0.0 for g in sample]
            ),
        }
        return features

    def features_before(self, game):
        away = game["away"]
        home = game["home"]
        h2h_key = tuple(sorted([away, home]))
        h2h_sample = self.h2h_games[h2h_key][-10:]
        park_sample = self.park_totals[game["park"]][-20:]
        umpire_sample = self.umpire_totals[game["umpire"]][-20:]
        features = {
            "away_team": away,
            "home_team": home,
            "park": game["park"],
            "umpire": game["umpire"],
            "away_starter": game["away_starter"],
            "home_starter": game["home_starter"],
            "away_hand": game["away_hand"],
            "home_hand": game["home_hand"],
            "away_starter_left": 1.0 if game["away_hand"] == "左" else 0.0,
            "home_starter_left": 1.0 if game["home_hand"] == "左" else 0.0,
            "both_starters_left": 1.0
            if game["away_hand"] == "左" and game["home_hand"] == "左"
            else 0.0,
            "park_last20_total": self._avg(park_sample),
            "umpire_last20_total": self._avg(umpire_sample),
            "h2h_last10_total": self._avg([g["total"] for g in h2h_sample]),
        }
        features.update(self._team_features(away, "away"))
        features.update(self._team_features(home, "home"))
        features.update(self._starter_features(game["away_starter"], "away"))
        features.update(self._starter_features(game["home_starter"], "home"))
        return features

    def update(self, game):
        away = game["away"]
        home = game["home"]
        away_score = game["away_score"]
        home_score = game["home_score"]
        away_hits = game["away_hits"]
        home_hits = game["home_hits"]
        total = away_score + home_score

        self.team_games[away].append(
            {
                "runs_for": away_score,
                "runs_against": home_score,
                "hits_for": away_hits,
                "hits_against": home_hits,
            }
        )
        self.team_games[home].append(
            {
                "runs_for": home_score,
                "runs_against": away_score,
                "hits_for": home_hits,
                "hits_against": away_hits,
            }
        )
        if game["away_starter"]:
            self.starter_games[game["away_starter"]].append(
                {"ip": game["away_starter_ip"], "er": game["away_starter_er"]}
            )
        if game["home_starter"]:
            self.starter_games[game["home_starter"]].append(
                {"ip": game["home_starter_ip"], "er": game["home_starter_er"]}
            )
        self.park_totals[game["park"]].append(total)
        self.umpire_totals[game["umpire"]].append(total)
        self.h2h_games[tuple(sorted([away, home]))].append({"total": total})


def betting_targets(row, game):
    values = {}
    away = game["away"]
    home = game["home"]
    for market, spec in MARKETS.items():
        line, water = parse_market_line(cell(row, spec["line_col"]))
        values[f"{market}_line"] = "" if line is None else line
        values[f"{market}_water"] = "" if water is None else water
        if market.endswith("_spread"):
            upper_team = cell(row, spec["upper_col"])
            if upper_team == away:
                result = cell(row, spec["away_result_col"])
                upper_is_home = 0
            elif upper_team == home:
                result = cell(row, spec["home_result_col"])
                upper_is_home = 1
            else:
                result = ""
                upper_is_home = ""
            values[f"{market}_upper_team"] = upper_team
            values[f"{market}_upper_is_home"] = upper_is_home
            values[f"{market}_target"] = 1 if result == "勝" else 0 if result == "敗" else ""
        else:
            result = cell(row, spec["result_col"])
            values[f"{market}_target"] = 1 if result == "大" else 0 if result == "小" else ""
    return values


def build_dataset(schedule_rows, betting_rows):
    betting_index, skipped = build_betting_index(betting_rows)
    schedule = parse_schedule_rows(schedule_rows)
    history = RollingHistory()
    dataset = []
    matched_keys = set()

    for game in schedule:
        key = (game["year"], month_day(game["date"]), game["away"], game["home"])
        features = history.features_before(game)
        if key in betting_index:
            betting = betting_index[key]
            row = {
                "date": game["date"].isoformat(),
                "year": game["year"],
                "schedule_row": game["row_number"],
                "betting_row": betting["row_number"],
                "game_no": game["game_no"],
            }
            row.update(features)
            row.update(betting_targets(betting["row"], game))
            dataset.append(row)
            matched_keys.add(key)
        history.update(game)

    missing = [
        (data["row_number"], key)
        for key, data in betting_index.items()
        if key not in matched_keys
    ]
    return dataset, skipped, missing


def write_dataset(rows, output_path):
    if not rows:
        raise RuntimeError("No training rows were built.")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0].keys())
    for row in rows[1:]:
        for key in row:
            if key not in columns:
                columns.append(key)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    return output, columns


def sigmoid(value):
    if value < -35:
        return 0.0
    if value > 35:
        return 1.0
    return 1.0 / (1.0 + math.exp(-value))


class SparseLogisticRegression:
    def __init__(self, lr=0.05, epochs=300, l2=0.001):
        self.lr = lr
        self.epochs = epochs
        self.l2 = l2
        self.weights = defaultdict(float)
        self.numeric_stats = {}
        self.numeric_columns = []
        self.categorical_columns = []

    def fit(self, rows, target_col, numeric_columns, categorical_columns):
        self.numeric_columns = numeric_columns
        self.categorical_columns = categorical_columns
        self.numeric_stats = {}
        for col in numeric_columns:
            values = [float(row[col]) for row in rows if row.get(col) not in ("", None)]
            mu = mean(values) if values else 0.0
            var = mean([(v - mu) ** 2 for v in values]) if values else 0.0
            self.numeric_stats[col] = (mu, math.sqrt(var) or 1.0)

        training = [(self._features(row), int(row[target_col])) for row in rows]
        for _ in range(self.epochs):
            for features, target in training:
                pred = self._predict_features(features)
                err = pred - target
                for key, value in features.items():
                    self.weights[key] -= self.lr * (err * value + self.l2 * self.weights[key])

    def _features(self, row):
        features = {"bias": 1.0}
        for col in self.numeric_columns:
            raw = row.get(col, "")
            value = float(raw) if raw not in ("", None) else 0.0
            mu, sigma = self.numeric_stats.get(col, (0.0, 1.0))
            features[f"num:{col}"] = (value - mu) / sigma
        for col in self.categorical_columns:
            value = row.get(col, "")
            if value:
                features[f"cat:{col}={value}"] = 1.0
        return features

    def _predict_features(self, features):
        score = sum(self.weights[key] * value for key, value in features.items())
        return sigmoid(score)

    def predict_proba(self, row):
        return self._predict_features(self._features(row))


def load_dataset(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def market_columns(market):
    spec = MARKETS[market]
    columns = {
        "target": f"{market}_target",
        "line": f"{market}_line",
        "water": f"{market}_water",
    }
    if market.endswith("_spread"):
        columns["upper_team"] = f"{market}_upper_team"
        columns["upper_is_home"] = f"{market}_upper_is_home"
    return columns


def water_profit_ratio(water):
    if water in ("", None):
        return 1.0
    value = abs(float(water))
    return value / 100 if value else 1.0


def selected_profit_ratio(row, cols, selected_target, opposite_water_mode):
    known_profit = water_profit_ratio(row.get(cols["water"]))
    if selected_target == 1:
        return known_profit
    if opposite_water_mode == "mirror":
        return known_profit
    if opposite_water_mode == "even":
        return 1.0
    raise ValueError(f"Unsupported opposite water mode: {opposite_water_mode}")


def lower_side(row, cols):
    upper_team = row.get(cols["upper_team"], "")
    if not upper_team:
        return ""
    return row["home_team"] if upper_team == row["away_team"] else row["away_team"]


def lower_is_home(row, cols):
    return row.get(cols["upper_is_home"]) == "0"


def lower_is_lefty(row, cols):
    lower = lower_side(row, cols)
    if lower == row["away_team"]:
        return row.get("away_starter_left") == "1.0"
    if lower == row["home_team"]:
        return row.get("home_starter_left") == "1.0"
    return False


def lower_pitcher_score(row):
    if row.get("away_team") == row.get("home_team"):
        return 0.0
    away_score = (
        parse_float(row.get("away_starter_last5_qs_rate")) * 2.0
        + parse_float(row.get("away_starter_last5_ip")) * 0.25
        - parse_float(row.get("away_starter_last5_era")) * 0.25
    )
    home_score = (
        parse_float(row.get("home_starter_last5_qs_rate")) * 2.0
        + parse_float(row.get("home_starter_last5_ip")) * 0.25
        - parse_float(row.get("home_starter_last5_era")) * 0.25
    )
    return away_score - home_score


def lower_pitcher_edge(row, cols):
    lower = lower_side(row, cols)
    score_diff = lower_pitcher_score(row)
    if lower == row["away_team"]:
        return score_diff
    if lower == row["home_team"]:
        return -score_diff
    return 0.0


def spread_strategy_allows(row, cols, strategy):
    if strategy == "model":
        return True
    if strategy == "lower_only":
        return True
    if strategy == "home_lower":
        return lower_is_home(row, cols)
    if strategy == "lefty_lower":
        return lower_is_lefty(row, cols)
    if strategy == "home_or_lefty_lower":
        return lower_is_home(row, cols) or lower_is_lefty(row, cols)
    if strategy == "home_lefty_lower":
        return lower_is_home(row, cols) and lower_is_lefty(row, cols)
    raise ValueError(f"Unsupported strategy: {strategy}")


def select_bet(row, cols, p, threshold, market, strategy):
    if not market.endswith("_spread") or strategy == "model":
        if p >= threshold:
            return 1, "BET_TARGET"
        if p <= 1 - threshold:
            return 0, "BET_OPPOSITE"
        return None, "PASS"

    if p <= 1 - threshold and spread_strategy_allows(row, cols, strategy):
        return 0, "BET_LOWER"
    return None, "PASS"


def market_feature_columns(train_rows, market, cols, feature_set):
    if feature_set == "lean":
        numeric_columns = [
            "away_starter_left",
            "home_starter_left",
            "both_starters_left",
            "away_starter_seen",
            "away_starter_last5_ip",
            "away_starter_last5_era",
            "away_starter_last5_qs_rate",
            "home_starter_seen",
            "home_starter_last5_ip",
            "home_starter_last5_era",
            "home_starter_last5_qs_rate",
            cols["line"],
            cols["water"],
        ]
        categorical_columns = []
    else:
        base_numeric = [
            key
            for key in train_rows[0]
            if key.endswith(("_runs_for", "_runs_against", "_hits_for", "_hits_against"))
            or key.endswith(("_win_rate", "_games_seen", "_seen", "_ip", "_era", "_qs_rate"))
            or key in ("away_starter_left", "home_starter_left", "both_starters_left")
            or key in ("park_last20_total", "umpire_last20_total", "h2h_last10_total")
        ]
        numeric_columns = base_numeric + [cols["line"], cols["water"]]
        categorical_columns = [
            "away_team",
            "home_team",
            "park",
            "umpire",
            "away_starter",
            "home_starter",
            "away_hand",
            "home_hand",
        ]
    if market.endswith("_spread"):
        numeric_columns.append(cols["upper_is_home"])
        if feature_set != "lean":
            categorical_columns.append(cols["upper_team"])
    return numeric_columns, categorical_columns


def train_market_model(rows, market, max_train_year, epochs, feature_set):
    cols = market_columns(market)
    target_col = cols["target"]
    train_rows = [
        row
        for row in rows
        if int(row["year"]) <= max_train_year
        and row.get(target_col) in ("0", "1")
        and row.get(cols["line"]) not in ("", None)
    ]
    if not train_rows:
        return None, [], [], cols

    numeric_columns, categorical_columns = market_feature_columns(
        train_rows, market, cols, feature_set
    )
    model = SparseLogisticRegression(epochs=epochs)
    model.fit(train_rows, target_col, numeric_columns, categorical_columns)
    return model, numeric_columns, categorical_columns, cols


def backtest(
    dataset_path,
    market,
    threshold,
    min_train_year,
    epochs,
    opposite_water_mode,
    feature_set,
    strategy,
):
    rows = load_dataset(dataset_path)
    cols = market_columns(market)
    target_col = cols["target"]
    market_rows = [
        row
        for row in rows
        if row.get(target_col) in ("0", "1") and row.get(cols["line"]) not in ("", None)
    ]
    years = sorted({int(row["year"]) for row in market_rows})
    test_years = [year for year in years if year >= min_train_year + 1]

    print(f"market={market} rows={len(market_rows)} years={years}")
    print(f"feature_set={feature_set}")
    print(f"strategy={strategy}")
    print(f"bet threshold: p >= {threshold:.2f} or p <= {1 - threshold:.2f}")
    print(f"water mode: known side uses abs(water)/100, opposite side={opposite_water_mode}")
    print()

    all_bets = []
    all_profit = []
    for test_year in test_years:
        train_rows = [row for row in market_rows if min_train_year <= int(row["year"]) < test_year]
        test_rows = [row for row in market_rows if int(row["year"]) == test_year]
        if not train_rows or not test_rows:
            continue
        model = SparseLogisticRegression(epochs=epochs)
        numeric_columns, categorical_columns = market_feature_columns(
            train_rows, market, cols, feature_set
        )
        model.fit(train_rows, target_col, numeric_columns, categorical_columns)

        correct = 0
        brier = 0.0
        bets = []
        profit = []
        for row in test_rows:
            p = model.predict_proba(row)
            target = int(row[target_col])
            pred = 1 if p >= 0.5 else 0
            correct += 1 if pred == target else 0
            brier += (p - target) ** 2
            bet_side, _ = select_bet(row, cols, p, threshold, market, strategy)
            if bet_side is not None:
                won = 1 if bet_side == target else 0
                profit_ratio = selected_profit_ratio(row, cols, bet_side, opposite_water_mode)
                pnl = profit_ratio if won else -1.0
                bets.append(won)
                profit.append(pnl)
                all_bets.append(won)
                all_profit.append(pnl)

        accuracy = correct / len(test_rows)
        brier /= len(test_rows)
        bet_hit = sum(bets) / len(bets) if bets else 0.0
        roi_even = (sum(bets) - (len(bets) - sum(bets))) / len(bets) if bets else 0.0
        roi_water = sum(profit) / len(profit) if profit else 0.0
        print(
            f"{test_year}: games={len(test_rows)} accuracy={accuracy:.3f} "
            f"brier={brier:.3f} bets={len(bets)} bet_hit={bet_hit:.3f} "
            f"even_roi={roi_even:.3f} water_roi={roi_water:.3f}"
        )

    if all_bets:
        total_hit = sum(all_bets) / len(all_bets)
        even_roi = (sum(all_bets) - (len(all_bets) - sum(all_bets))) / len(all_bets)
        water_roi = sum(all_profit) / len(all_profit)
        print()
        print(
            f"all bets: count={len(all_bets)} hit={total_hit:.3f} "
            f"even_roi={even_roi:.3f} water_roi={water_roi:.3f}"
        )
    else:
        print()
        print("No bets were selected at this threshold.")


def predict(
    dataset_path,
    date,
    markets,
    threshold,
    epochs,
    opposite_water_mode,
    feature_set,
    strategy,
):
    rows = load_dataset(dataset_path)
    target_date = datetime.strptime(date, "%Y-%m-%d").date()
    candidate_rows = [row for row in rows if row["date"] == target_date.isoformat()]
    if not candidate_rows:
        print(f"No rows found for {target_date.isoformat()} in {dataset_path}.")
        return

    train_rows = [row for row in rows if row["date"] < target_date.isoformat()]
    if not train_rows:
        print(f"No historical rows available before {target_date.isoformat()}.")
        return

    print(
        f"date={target_date.isoformat()} games={len(candidate_rows)} "
        f"threshold={threshold:.2f} feature_set={feature_set} strategy={strategy}"
    )
    print(f"water mode: known side uses abs(water)/100, opposite side={opposite_water_mode}")
    print()

    for market in markets:
        model, _, _, cols = train_market_model(
            train_rows,
            market,
            max(int(row["year"]) for row in train_rows),
            epochs,
            feature_set,
        )
        if not model:
            print(f"[{market}] no model")
            continue
        print(f"[{market}]")
        for row in candidate_rows:
            if row.get(cols["target"]) not in ("0", "1") or row.get(cols["line"]) in ("", None):
                continue
            p = model.predict_proba(row)
            selected_target, action = select_bet(row, cols, p, threshold, market, strategy)
            price = (
                selected_profit_ratio(row, cols, selected_target, opposite_water_mode)
                if selected_target is not None
                else 0.0
            )
            if market.endswith("_spread"):
                target_label = row.get(cols["upper_team"], "")
                opposite_label = (
                    row["home_team"] if target_label == row["away_team"] else row["away_team"]
                )
            else:
                target_label = "大"
                opposite_label = "小"
            side = (
                target_label
                if selected_target == 1
                else opposite_label
                if selected_target == 0
                else "-"
            )
            print(
                f"  {row['away_team']} @ {row['home_team']} "
                f"line={row.get(cols['line'])} water={row.get(cols['water'])} "
                f"p_target={p:.3f} action={action} side={side} "
                f"lower_home={lower_is_home(row, cols) if market.endswith('_spread') else '-'} "
                f"lower_lefty={lower_is_lefty(row, cols) if market.endswith('_spread') else '-'} "
                f"lower_pitcher_edge={lower_pitcher_edge(row, cols):.2f} "
                f"profit_ratio={price:.2f}"
            )
        print()


def cmd_export(args):
    schedule_rows, betting_rows = read_sheet_rows()
    rows, skipped, missing = build_dataset(schedule_rows, betting_rows)
    output, columns = write_dataset(rows, args.output)
    print(f"wrote {len(rows)} rows to {output}")
    print(f"columns={len(columns)}")
    if skipped:
        print(f"skipped betting rows={len(skipped)}")
    if missing:
        print(f"unmatched betting rows={len(missing)}")
        for row_number, key in missing[:10]:
            print(f"  row {row_number}: {key}")


def cmd_backtest(args):
    backtest(
        args.input,
        args.market,
        args.threshold,
        args.min_train_year,
        args.epochs,
        args.opposite_water_mode,
        args.feature_set,
        args.strategy,
    )


def cmd_predict(args):
    predict(
        args.input,
        args.date,
        args.market,
        args.threshold,
        args.epochs,
        args.opposite_water_mode,
        args.feature_set,
        args.strategy,
    )


def main():
    parser = argparse.ArgumentParser(description="Build and backtest CPBL betting datasets.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export Google Sheet rows to CSV.")
    export_parser.add_argument("--output", default=DEFAULT_OUTPUT)
    export_parser.set_defaults(func=cmd_export)

    backtest_parser = subparsers.add_parser("backtest", help="Run walk-forward backtest.")
    backtest_parser.add_argument("--input", default=DEFAULT_OUTPUT)
    backtest_parser.add_argument("--market", choices=sorted(MARKETS), default="full_spread")
    backtest_parser.add_argument("--threshold", type=float, default=0.56)
    backtest_parser.add_argument("--min-train-year", type=int, default=2020)
    backtest_parser.add_argument("--epochs", type=int, default=300)
    backtest_parser.add_argument(
        "--feature-set",
        choices=["lean", "full"],
        default="lean",
        help="lean keeps pitcher ability, handedness, home/away, line, and water only.",
    )
    backtest_parser.add_argument(
        "--strategy",
        choices=[
            "model",
            "lower_only",
            "home_lower",
            "lefty_lower",
            "home_or_lefty_lower",
            "home_lefty_lower",
        ],
        default="model",
        help="For spread markets, optionally restrict bets to lower-side theses.",
    )
    backtest_parser.add_argument(
        "--opposite-water-mode",
        choices=["mirror", "even"],
        default="mirror",
        help="How to price the side opposite the sheet's listed water.",
    )
    backtest_parser.set_defaults(func=cmd_backtest)

    predict_parser = subparsers.add_parser("predict", help="Score games already in the CSV.")
    predict_parser.add_argument("--input", default=DEFAULT_OUTPUT)
    predict_parser.add_argument("--date", required=True, help="Game date, e.g. 2026-04-25.")
    predict_parser.add_argument(
        "--market",
        choices=sorted(MARKETS),
        nargs="+",
        default=["full_spread", "full_total", "half_spread", "half_total"],
    )
    predict_parser.add_argument("--threshold", type=float, default=0.56)
    predict_parser.add_argument("--epochs", type=int, default=300)
    predict_parser.add_argument(
        "--feature-set",
        choices=["lean", "full"],
        default="lean",
        help="lean keeps pitcher ability, handedness, home/away, line, and water only.",
    )
    predict_parser.add_argument(
        "--strategy",
        choices=[
            "model",
            "lower_only",
            "home_lower",
            "lefty_lower",
            "home_or_lefty_lower",
            "home_lefty_lower",
        ],
        default="model",
        help="For spread markets, optionally restrict bets to lower-side theses.",
    )
    predict_parser.add_argument(
        "--opposite-water-mode",
        choices=["mirror", "even"],
        default="mirror",
        help="How to price the side opposite the sheet's listed water.",
    )
    predict_parser.set_defaults(func=cmd_predict)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
