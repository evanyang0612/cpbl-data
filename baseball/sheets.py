import json
import os
from functools import cached_property

import gspread
from google.oauth2.service_account import Credentials


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
        return gspread.authorize(creds, http_client=gspread.BackOffHTTPClient)

    def spreadsheet(self, spreadsheet_key: str):
        return self.client.open_by_key(spreadsheet_key)

    def worksheet(self, spreadsheet_key: str, sheet_name: str):
        return self.spreadsheet(spreadsheet_key).worksheet(sheet_name)
