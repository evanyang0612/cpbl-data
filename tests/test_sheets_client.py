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

    def test_backoff_is_capped_so_a_stall_cannot_outlive_the_job(self):
        client = _StubClient([429] * 20)
        with patch("baseball.sheets.time.sleep") as sleep, patch("builtins.print"):
            with self.assertRaises(APIError):
                client.request("GET", "url")

        waits = [call.args[0] for call in sleep.call_args_list]
        self.assertEqual(waits, [1, 2, 4, 8, 16, 32])
        self.assertLessEqual(sum(waits), LoggingBackOffHTTPClient.MAX_TOTAL_WAIT)


if __name__ == "__main__":
    unittest.main()
