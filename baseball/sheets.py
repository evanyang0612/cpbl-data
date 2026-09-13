import json
import os
import time
from functools import cached_property
from http import HTTPStatus

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from gspread.http_client import HTTPClient


class LoggingBackOffHTTPClient(HTTPClient):
    """Retries rate-limited Sheets calls, out loud.

    gspread's own BackOffHTTPClient sleeps up to 128s per step and prints
    nothing, so a quota stall looks exactly like a hung request: the MLB job
    spent 8-12 minutes inside a single read with no output at all. This one
    reports every wait and caps the total, so a stall shows up in the log and
    cannot quietly eat the job's whole timeout budget.
    """

    MAX_TOTAL_WAIT = 63

    # And a budget for the whole run, because the per-request cap bounds one
    # stall and nothing bounded a run of them. On 2026-09-13 npb_scheduler was
    # cancelled at its ten-minute timeout inside a Sheets 503 retry, part-way
    # through rewriting 投手主客, which left the tab empty. Every request was
    # politely waiting under its own cap; their sum ran the job out of time.
    #
    # Giving up at the budget is the better failure: it raises, so the
    # workflow's alert fires. Being cancelled at the timeout is silent.
    MAX_RUN_WAIT = 180
    _run_waited = 0.0

    @classmethod
    def reset_retry_budget(cls) -> None:
        cls._run_waited = 0.0

    @classmethod
    def retry_budget_spent(cls) -> float:
        return cls._run_waited

    def _send(self, *args, **kwargs):
        return super().request(*args, **kwargs)

    def request(self, *args, **kwargs):
        waited = 0
        wait = 1
        while True:
            try:
                return self._send(*args, **kwargs)
            except APIError as err:
                cls = LoggingBackOffHTTPClient
                if (not self._retryable(err)
                        or waited + wait > self.MAX_TOTAL_WAIT
                        or cls._run_waited + wait > cls.MAX_RUN_WAIT):
                    raise
                print(
                    f"Sheets API {err.code}, retrying in {wait}s "
                    f"({waited + wait}s/{self.MAX_TOTAL_WAIT}s spent waiting)",
                    flush=True,
                )
                time.sleep(wait)
                waited += wait
                LoggingBackOffHTTPClient._run_waited += wait
                wait *= 2

    @staticmethod
    def _retryable(err: APIError) -> bool:
        if err.code in (HTTPStatus.REQUEST_TIMEOUT, HTTPStatus.TOO_MANY_REQUESTS):
            return True
        if err.code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            return True
        # The Drive API reports quota exhaustion as 403 rather than 429.
        errors = err.error.get("errors") if isinstance(err.error, dict) else None
        return bool(
            err.code == HTTPStatus.FORBIDDEN
            and errors
            and errors[0].get("domain") == "usageLimits"
        )


class GoogleSheetsClient:
    """Small gateway for authenticated Google Sheets access."""

    def __init__(
        self,
        *,
        credentials_file: str | None = None,
        credentials_json_env: str = "GOOGLE_CREDENTIALS",
    ):
        self.credentials_file = credentials_file or os.getenv("GOOGLE_CREDENTIALS_FILE")
        self.credentials_json_env = credentials_json_env

    @cached_property
    def client(self):
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_json = os.environ.get(self.credentials_json_env)
        if creds_json:
            creds = Credentials.from_service_account_info(
                json.loads(creds_json), scopes=scope
            )
        else:
            creds = Credentials.from_service_account_file(
                self.credentials_file, scopes=scope
            )
        return gspread.authorize(creds, http_client=LoggingBackOffHTTPClient)

    def spreadsheet(self, spreadsheet_key: str):
        return self.client.open_by_key(spreadsheet_key)

    def worksheet(self, spreadsheet_key: str, sheet_name: str):
        return self.spreadsheet(spreadsheet_key).worksheet(sheet_name)
