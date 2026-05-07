from __future__ import annotations

import os
import time

from uptime_kuma_api import MonitorType, NotificationType, UptimeKumaApi


KUMA_URL = os.environ.get("KUMA_URL", "http://127.0.0.1:3001")
KUMA_USERNAME = os.environ["KUMA_USERNAME"]
KUMA_PASSWORD = os.environ["KUMA_PASSWORD"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()


MONITORS = [
    ("tgctxbot primary /health", "http://127.0.0.1:8010/health"),
    ("tgctxbot primary /health/ready", "http://127.0.0.1:8010/health/ready"),
    ("tgctxbot secondary /health", "http://127.0.0.1:8011/health"),
    ("tgctxbot secondary /health/ready", "http://127.0.0.1:8011/health/ready"),
]


def wait_for_kuma(api: UptimeKumaApi) -> None:
    last_error: Exception | None = None
    for _ in range(60):
        try:
            api.need_setup()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Uptime Kuma did not become ready: {last_error}")


def ensure_login(api: UptimeKumaApi) -> None:
    if api.need_setup():
        api.setup(KUMA_USERNAME, KUMA_PASSWORD)
    api.login(KUMA_USERNAME, KUMA_PASSWORD)


def ensure_telegram_notification(api: UptimeKumaApi) -> int | None:
    name = "Telegram alerts"
    for notification in api.get_notifications():
        if notification.get("name") == name:
            return int(notification["id"])

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return None

    response = api.add_notification(
        name=name,
        type=NotificationType.TELEGRAM,
        isDefault=True,
        applyExisting=True,
        telegramBotToken=TELEGRAM_BOT_TOKEN,
        telegramChatID=TELEGRAM_CHAT_ID,
        telegramSendSilently=False,
        telegramProtectContent=False,
    )
    return int(response["id"])


def ensure_monitors(api: UptimeKumaApi, notification_id: int | None) -> None:
    existing = {monitor["name"]: monitor for monitor in api.get_monitors()}

    for name, url in MONITORS:
        data = {
            "type": MonitorType.HTTP,
            "name": name,
            "url": url,
            "interval": 60,
            "retryInterval": 30,
            "resendInterval": 3600,
            "maxretries": 2,
            "timeout": 15,
            "accepted_statuscodes": ["200-299"],
        }
        if notification_id is not None:
            data["notificationIDList"] = [notification_id]

        if name in existing:
            api.edit_monitor(existing[name]["id"], **data)
        else:
            api.add_monitor(**data)


def main() -> None:
    api = UptimeKumaApi(KUMA_URL)
    try:
        wait_for_kuma(api)
        ensure_login(api)
        notification_id = ensure_telegram_notification(api)
        ensure_monitors(api, notification_id)
        monitors = sorted(monitor["name"] for monitor in api.get_monitors())
        notifications = sorted(notification["name"] for notification in api.get_notifications())
        print("MONITORS")
        for monitor in monitors:
            print(monitor)
        print("NOTIFICATIONS")
        for notification in notifications:
            print(notification)
    finally:
        api.disconnect()


if __name__ == "__main__":
    main()
