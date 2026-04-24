import os

import requests
from dotenv import load_dotenv


def _get_telegram_config():
    # Load .env lazily so import order does not suppress notifications.
    load_dotenv(override=False)
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
    return bot_token, chat_id


def send_telegram(msg):
    bot_token, chat_id = _get_telegram_config()
    if not bot_token or not chat_id:
        print("Telegram credentials are not configured. Skipping notification.")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        response = requests.post(
            url,
            data={"chat_id": chat_id, "text": msg},
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            print(f"Telegram API error: {payload}")
            return False
        return True
    except requests.RequestException as exc:
        print(f"Telegram notification failed: {exc}")
        return False
