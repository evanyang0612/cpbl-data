"""Find the chat id of the Telegram channel the broadcast should post to.

A channel's id is not shown anywhere in the Telegram app, and it is not the id
already in ``.env`` — that one is a private chat, which is why it is positive.
Channel ids are large and negative (``-100...``).

Reads ``TELEGRAM_BROADCAST_BOT_TOKEN``: the broadcast has a bot of its own so
that subscribers never receive the scraper failure alerts the other bot sends.

Three ways to get the id, all needing that bot to already be an administrator
of the channel:

  # a private channel: right-click any message in Telegram Desktop or Web,
  # Copy Message Link, and pass it — the id is embedded in the link
  uv run python -m migration.find_telegram_chat_id https://t.me/c/2123456789/5

  # a public channel, addressed by its @username
  uv run python -m migration.find_telegram_chat_id @my_channel

  # from the bot's pending updates: post a message in the channel, then run
  uv run python -m migration.find_telegram_chat_id

Prefer the first two. The last reads updates, which carry only events from
after the bot became an administrator and which expire after 24 hours, so it
fails in ways that are hard to tell apart.
"""

import os
import sys

import requests
from dotenv import load_dotenv


def _token() -> str:
    load_dotenv(override=False)
    token = os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN")
    if not token:
        sys.exit("No TELEGRAM_BROADCAST_BOT_TOKEN in the environment or .env.\n"
                 "Create the broadcast bot with @BotFather first — do not reuse "
                 "the bot that sends failure alerts.")
    return token


def _call(token: str, method: str, **params) -> dict:
    resp = requests.get(f"https://api.telegram.org/bot{token}/{method}",
                        params=params, timeout=20)
    return resp.json()


def from_message_link(token: str, link: str) -> None:
    """Derive a private channel's id from one of its message links.

    A private channel has no @username to look up and produces no update until
    something happens in it, but its message links already carry the id:
    ``https://t.me/c/2123456789/5`` belongs to chat ``-1002123456789``. In
    Telegram Desktop or Web, right-click any message in the channel and choose
    Copy Message Link.
    """
    internal = link.rstrip("/").split("/c/", 1)[1].split("/", 1)[0]
    if not internal.isdigit():
        sys.exit(f"Could not read a channel id out of {link!r}")
    chat_id = f"-100{internal}"
    payload = _call(token, "getChat", chat_id=chat_id)
    if not payload.get("ok"):
        sys.exit(f"Derived {chat_id}, but Telegram said: "
                 f"{payload.get('description')}\n"
                 "The id is probably right; the bot cannot see the channel, so "
                 "check it is an administrator there with 'Post Messages'.")
    chat = payload["result"]
    print(f"title  : {chat.get('title')}")
    print(f"type   : {chat.get('type')}")
    print(f"chat_id: {chat.get('id')}")
    print("\nSet this as the TELEGRAM_BROADCAST_CHAT_ID secret.")


def by_username(token: str, username: str) -> None:
    payload = _call(token, "getChat", chat_id=username)
    if not payload.get("ok"):
        sys.exit(f"Telegram said: {payload.get('description')}\n"
                 "Check the @username, and that the broadcast bot is an "
                 "administrator of the channel with 'Post Messages' enabled.")
    chat = payload["result"]
    print(f"title  : {chat.get('title')}")
    print(f"type   : {chat.get('type')}")
    print(f"chat_id: {chat.get('id')}")
    print("\nSet this as the TELEGRAM_BROADCAST_CHAT_ID secret.")


def _diagnose(token: str) -> str:
    """Say why no updates arrived, rather than only that none did.

    The three causes look identical from the caller's side: a webhook consumes
    updates before ``getUpdates`` can see them, the bot is not an administrator
    anywhere so Telegram has nothing to deliver, or the updates arrived and
    were already read (they are handed out once, then expire after 24 hours).
    """
    hook = _call(token, "getWebhookInfo").get("result", {})
    if hook.get("url"):
        return (f"A webhook is set on this bot ({hook['url']}), which consumes "
                "every update before getUpdates can see it. Remove it with "
                "deleteWebhook, or pass the channel's @username instead.")
    if not hook.get("pending_update_count"):
        return ("Telegram has never had an update for this bot, so it is not an "
                "administrator of any channel yet. Add it to the channel under "
                "Administrators with 'Post Messages' enabled — that alone "
                "produces an update, no post needed.")
    return ("The updates were already read; they are handed out once and expire "
            "after 24 hours. Post again in the channel, then re-run.")


def from_updates(token: str) -> None:
    payload = _call(token, "getUpdates", timeout=0)
    if not payload.get("ok"):
        sys.exit(f"Telegram said: {payload.get('description')}")
    seen = {}
    for update in payload.get("result", []):
        post = update.get("channel_post") or update.get("my_chat_member")
        chat = (post or {}).get("chat") or {}
        if chat.get("type") == "channel":
            seen[chat["id"]] = chat.get("title")
    if not seen:
        sys.exit(f"No channel activity in the bot's pending updates.\n"
                 f"{_diagnose(token)}\n"
                 "A public channel needs none of this — pass its @username as "
                 "an argument and the id is looked up directly.")
    for chat_id, title in seen.items():
        print(f"{chat_id}\t{title}")
    print("\nSet the id above as the TELEGRAM_BROADCAST_CHAT_ID secret.")


def main() -> None:
    token = _token()
    if len(sys.argv) > 1:
        argument = sys.argv[1]
        if "/c/" in argument:
            from_message_link(token, argument)
        else:
            by_username(token, argument)
    else:
        from_updates(token)


if __name__ == "__main__":
    main()
