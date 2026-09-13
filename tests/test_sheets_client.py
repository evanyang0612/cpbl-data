import json
import unittest
from unittest.mock import patch

from gspread.exceptions import APIError
from requests import Response

from baseball.sheets import LoggingBackOffHTTPClient


def _response(code: int) -> Response:
    response = Response()
    response.status_code = code
    response._content = json.dumps(
        {"error": {"code": code, "message": f"boom {code}", "status": "ERROR"}}
    ).encode()
    return response


class _StubClient(LoggingBackOffHTTPClient):
    """Drives the backoff loop without touching auth or the network."""

    def __init__(self, codes):
        self._codes = list(codes)
        self.calls = 0

    def _send(self, *args, **kwargs):
        self.calls += 1
        if self._codes:
            raise APIError(_response(self._codes.pop(0)))
        return "ok"


class LoggingBackOffHTTPClientTest(unittest.TestCase):
    def test_retries_rate_limited_request_and_reports_each_wait(self):
        client = _StubClient([429, 429])
        with patch("baseball.sheets.time.sleep") as sleep, patch(
            "builtins.print"
        ) as printed:
            self.assertEqual(client.request("GET", "url"), "ok")

        self.assertEqual(client.calls, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 2])
        messages = " ".join(str(call.args[0]) for call in printed.call_args_list)
        self.assertIn("429", messages)
        self.assertIn("retrying in 1s", messages)
        self.assertIn("retrying in 2s", messages)

    def test_server_errors_are_retried_too(self):
        client = _StubClient([500])
        with patch("baseball.sheets.time.sleep"), patch("builtins.print"):
            self.assertEqual(client.request("GET", "url"), "ok")
        self.assertEqual(client.calls, 2)

    def test_client_errors_raise_immediately(self):
        client = _StubClient([400])
        with patch("baseball.sheets.time.sleep") as sleep, patch("builtins.print"):
            with self.assertRaises(APIError):
                client.request("GET", "url")
        self.assertEqual(client.calls, 1)
        sleep.assert_not_called()

    def setUp(self):
        LoggingBackOffHTTPClient.reset_retry_budget()

    def test_backoff_is_capped_so_a_stall_cannot_outlive_the_job(self):
        client = _StubClient([429] * 20)
        with patch("baseball.sheets.time.sleep") as sleep, patch("builtins.print"):
            with self.assertRaises(APIError):
                client.request("GET", "url")

        waits = [call.args[0] for call in sleep.call_args_list]
        self.assertEqual(waits, [1, 2, 4, 8, 16, 32])
        self.assertLessEqual(sum(waits), LoggingBackOffHTTPClient.MAX_TOTAL_WAIT)


class RetryBudgetTest(unittest.TestCase):
    """The per-request cap bounds one stall; nothing bounded a run of them.

    On 2026-09-13 npb_scheduler was cancelled at its ten-minute timeout inside
    a Sheets 503 retry, mid-way through rewriting 投手主客 — which left the tab
    empty. Each request was politely waiting under its own 63-second cap; it
    was their sum that ran the job out of time.
    """

    def setUp(self):
        LoggingBackOffHTTPClient.reset_retry_budget()

    def tearDown(self):
        LoggingBackOffHTTPClient.reset_retry_budget()

    def test_waiting_accumulates_across_requests(self):
        with patch("baseball.sheets.time.sleep"), patch("builtins.print"):
            for _ in range(2):
                _StubClient([429]).request("GET", "url")
        self.assertEqual(LoggingBackOffHTTPClient.retry_budget_spent(), 2)

    def test_a_run_of_stalls_gives_up_rather_than_outliving_the_job(self):
        """Failing at the budget is loud — the workflow alerts. Being
        cancelled at the timeout is silent, and leaves a half-written sheet."""
        spent = 0
        with patch("baseball.sheets.time.sleep"), patch("builtins.print"):
            with self.assertRaises(APIError):
                for _ in range(200):
                    _StubClient([503] * 20).request("GET", "url")
                    spent = LoggingBackOffHTTPClient.retry_budget_spent()
        self.assertLessEqual(spent, LoggingBackOffHTTPClient.MAX_RUN_WAIT)

    def test_the_budget_leaves_room_for_several_ordinary_stalls(self):
        """One slow call must not exhaust it, or a single 503 fails the run."""
        self.assertGreaterEqual(
            LoggingBackOffHTTPClient.MAX_RUN_WAIT,
            LoggingBackOffHTTPClient.MAX_TOTAL_WAIT * 2)


if __name__ == "__main__":
    unittest.main()
