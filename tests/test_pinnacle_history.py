"""Unit tests for the BettingIsCool Pinnacle history pull (baseball/pinnacle_history.py)."""

import gzip
import json

import pytest

from baseball import pinnacle_history as ph


class FakeResponse:
    def __init__(self, payload, *, status=200, headers=None):
        self._payload = payload
        self.status_code = status
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Answers each GET from a handler keyed on the path; records every call."""

    def __init__(self, handlers):
        self.handlers = handlers
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        path = url.split("bettingiscool.com", 1)[-1]
        self.calls.append((path, dict(params or {})))
        return self.handlers[path](dict(params or {}))


def _client(handlers):
    session = FakeSession(handlers)
    return ph.Client("pk_test", session=session, base="https://api.bettingiscool.com"), session


def _fixture(event_id, starts, *, status="settled", unit=None):
    return {"event_id": event_id, "starts": starts, "runner_home": "Hanshin Tigers",
            "runner_away": "Chunichi Dragons", "event_status": status,
            "resulting_unit": unit, "superseded_by": None}


# --- quota handling ---------------------------------------------------------

def test_request_tracks_remaining_quota_from_headers():
    client, _ = _client({"/api/results": lambda p: FakeResponse(
        [{"period": 0}], headers={"X-Quota-Remaining": "4321"})})
    client.get("/api/results", event_id=1)
    assert client.remaining == 4321


def test_429_raises_quota_exhausted_with_retry_after():
    client, _ = _client({"/api/results": lambda p: FakeResponse(
        {"detail": "over cap"}, status=429, headers={"Retry-After": "3600"})})
    with pytest.raises(ph.QuotaExhausted) as exc:
        client.get("/api/results", event_id=1)
    assert exc.value.retry_after == 3600


# --- fixtures ---------------------------------------------------------------

def test_month_windows_cover_the_range_without_gaps():
    assert ph.month_windows("2021-03-15", "2021-05-02") == [
        ("2021-03-15", "2021-03-31"),
        ("2021-04-01", "2021-04-30"),
        ("2021-05-01", "2021-05-02"),
    ]


def test_list_fixtures_asks_per_month_for_the_league_and_hides_superseded():
    client, session = _client({"/api/fixtures": lambda p: FakeResponse(
        [_fixture(int(p["starts_from"][5:7]), p["starts_from"] + "T09:00:00Z")])})
    fixtures = ph.list_fixtures(client, 187703, "2021-03-01", "2021-04-30")
    assert [f["event_id"] for f in fixtures] == [3, 4]
    params = session.calls[0][1]
    assert params["league_id"] == 187703
    assert params["include_superseded"] == 0
    assert params["limit"] == 1000


def test_list_fixtures_splits_a_truncated_window():
    def handler(p):
        if p["starts_from"] == "2021-04-01" and p["starts_to"] == "2021-04-30":
            return FakeResponse([_fixture(0, "2021-04-01T09:00:00Z")],
                                headers={"X-Truncated": "true"})
        return FakeResponse([_fixture(hash(p["starts_from"]) % 1000,
                                      p["starts_from"] + "T09:00:00Z")])
    client, session = _client({"/api/fixtures": handler})
    fixtures = ph.list_fixtures(client, 187703, "2021-04-01", "2021-04-30")
    windows = [(c[1]["starts_from"], c[1]["starts_to"]) for c in session.calls]
    assert windows == [("2021-04-01", "2021-04-30"),
                       ("2021-04-01", "2021-04-15"), ("2021-04-16", "2021-04-30")]
    assert len(fixtures) == 2


def test_pending_skips_done_unfinished_cancelled_and_side_markets(tmp_path):
    fixtures = [
        _fixture(1, "2021-04-01T09:00:00Z"),
        _fixture(2, "2025-04-01T09:00:00Z"),
        _fixture(3, "2023-04-01T09:00:00Z"),
        _fixture(4, "2024-04-01T09:00:00Z", status="cancelled"),
        _fixture(5, "2026-12-01T09:00:00Z", status="scheduled"),
        _fixture(6, "2024-05-01T09:00:00Z", unit="Corners"),
    ]
    ph.save_event(tmp_path, {"fixture": fixtures[2], "odds": [], "results": []})
    # newest first, so a short month still leaves the most useful seasons
    assert [f["event_id"] for f in ph.pending(fixtures, tmp_path)] == [2, 1]


def test_pending_skips_specials_tests_and_all_star_games(tmp_path):
    def named(event_id, home, away):
        return {**_fixture(event_id, "2025-04-01T09:00:00Z"),
                "runner_home": home, "runner_away": away}
    fixtures = [
        named(1, "Home Runs (5 Games)", "Away Runs (5 games)"),
        named(2, "Away Runs ( 6 games)", "Home (6 games)"),
        named(3, "Orix Buffaloes TestTeam", "Hanshin Tigers TestTeam"),
        named(4, "Test 1", "Test 2"),
        named(5, "DO NOT BET", "123"),
        named(6, "Central League", "Pacific League"),
        named(7, "G1 Hanshin Tigers", "G1 Chunichi Dragons"),   # doubleheader: real
        named(8, "Yokohama Bay Stars", "Yomiuri Giants"),
    ]
    assert [f["event_id"] for f in ph.pending(fixtures, tmp_path)] == [7, 8]


# --- one event --------------------------------------------------------------

def test_fetch_event_pulls_full_history_and_results():
    odds = [{"period": 0, "market": "moneyline", "timestamp": "2026-08-22T00:00:00Z"}]
    client, session = _client({
        "/api/odds": lambda p: FakeResponse(odds),
        "/api/results": lambda p: FakeResponse([{"period": 1, "score_home": 2}]),
    })
    record = ph.fetch_event(client, _fixture(1634054636, "2026-08-22T09:00:00Z"))
    assert record["odds"] == odds
    assert record["results"] == [{"period": 1, "score_home": 2}]
    odds_params = dict(session.calls)["/api/odds"]
    assert odds_params == {"event_id": 1634054636, "full_history": 1}


def test_fetch_event_pages_a_truncated_history_without_losing_tied_rows():
    first = [{"line_id": 1, "market": "moneyline", "timestamp": "2026-08-21T01:00:00Z"},
             {"line_id": 2, "market": "spread", "timestamp": "2026-08-21T02:00:00Z"}]
    second = [{"line_id": 2, "market": "spread", "timestamp": "2026-08-21T02:00:00Z"},
              {"line_id": 2, "market": "totals", "timestamp": "2026-08-21T02:00:00Z"}]

    def odds(p):
        if "since" not in p:
            return FakeResponse(first, headers={"X-Truncated": "true"})
        assert p["since"] == "2026-08-21T01:59:59Z"   # one second back: keeps ties
        return FakeResponse(second)
    client, _ = _client({"/api/odds": odds,
                         "/api/results": lambda p: FakeResponse([])})
    record = ph.fetch_event(client, _fixture(9, "2026-08-22T09:00:00Z"))
    assert [r["market"] for r in record["odds"]] == ["moneyline", "spread", "totals"]


def test_save_event_round_trips_through_gzip(tmp_path):
    record = {"fixture": _fixture(7, "2024-04-01T09:00:00Z"), "odds": [{"a": 1}],
              "results": []}
    path = ph.save_event(tmp_path, record)
    assert path.name == "7.json.gz"
    with gzip.open(path, "rt") as fh:
        assert json.load(fh) == record
    assert ph.done_ids(tmp_path) == {7}


# --- the run loop -----------------------------------------------------------

def test_pull_stops_cleanly_when_quota_runs_out(tmp_path):
    calls = {"n": 0}

    def odds(p):
        calls["n"] += 1
        if calls["n"] > 1:
            return FakeResponse({}, status=429, headers={"Retry-After": "60"})
        return FakeResponse([], headers={"X-Quota-Remaining": "100000"})
    client, _ = _client({"/api/odds": odds,
                         "/api/results": lambda p: FakeResponse([])})
    todo = [_fixture(2, "2025-04-01T09:00:00Z"), _fixture(1, "2024-04-01T09:00:00Z")]
    stats = ph.pull(client, todo, tmp_path, wait=False)
    assert stats == {"saved": 1, "left": 1, "failed": []}
    assert ph.done_ids(tmp_path) == {2}


def test_pull_stops_before_the_cap_when_remaining_is_below_the_reserve(tmp_path):
    client, session = _client({
        "/api/odds": lambda p: FakeResponse([], headers={"X-Quota-Remaining": "10"}),
        "/api/results": lambda p: FakeResponse([], headers={"X-Quota-Remaining": "9"}),
    })
    todo = [_fixture(2, "2025-04-01T09:00:00Z"), _fixture(1, "2024-04-01T09:00:00Z")]
    stats = ph.pull(client, todo, tmp_path, wait=False, reserve=1000)
    assert stats == {"saved": 1, "left": 1, "failed": []}
    assert len(session.calls) == 2


def test_pull_skips_an_event_that_keeps_failing(tmp_path):
    import requests

    def odds(p):
        if p["event_id"] == 2:
            raise requests.ConnectionError("boom")
        return FakeResponse([])
    client, _ = _client({"/api/odds": odds,
                         "/api/results": lambda p: FakeResponse([])})
    todo = [_fixture(2, "2025-04-01T09:00:00Z"), _fixture(1, "2024-04-01T09:00:00Z")]
    stats = ph.pull(client, todo, tmp_path, sleep=lambda s: None)
    assert stats == {"saved": 1, "left": 0, "failed": [2]}
    assert ph.done_ids(tmp_path) == {1}


class WallClock:
    """time.time stand-in: each nap advances it, and so can a laptop lid."""

    def __init__(self, start=1_000_000.0):
        self.now = start
        self.naps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.naps.append(seconds)
        self.now += seconds


def test_wait_until_naps_in_short_steps_up_to_the_deadline():
    clock = WallClock()
    ph.wait_until(clock.now + 150, sleep=clock.sleep, clock=clock, step=60)
    assert clock.naps == [60, 60, 30]


def test_wait_until_wakes_on_wall_time_after_the_machine_slept():
    # time.sleep does not count system sleep on macOS: a 19-hour nap taken
    # before the lid closed for 5 hours would end 5 hours late. Checking the
    # wall clock between short naps ends it on time.
    clock = WallClock()
    deadline = clock.now + 19 * 3600

    def nap(seconds):
        clock.naps.append(seconds)
        clock.now += seconds + (5 * 3600 if len(clock.naps) == 1 else 0)
    ph.wait_until(deadline, sleep=nap, clock=clock, step=60)
    assert clock.now >= deadline
    assert clock.now - deadline < 60
    assert len(clock.naps) < 19 * 60 - 5 * 60 + 2


def test_pull_with_wait_sleeps_to_the_reset_and_carries_on(tmp_path):
    state = {"blocked": True}

    def odds(p):
        if state["blocked"]:
            return FakeResponse({}, status=429, headers={"Retry-After": "120"})
        return FakeResponse([])
    clock = WallClock()

    def fake_sleep(seconds):
        clock.sleep(seconds)
        state["blocked"] = False
    client, _ = _client({"/api/odds": odds,
                         "/api/results": lambda p: FakeResponse([])})
    stats = ph.pull(client, [_fixture(1, "2024-04-01T09:00:00Z")], tmp_path,
                    wait=True, sleep=fake_sleep, clock=clock)
    assert stats == {"saved": 1, "left": 0, "failed": []}
    assert sum(clock.naps) == 120 + ph.RESET_SLACK_SECONDS
