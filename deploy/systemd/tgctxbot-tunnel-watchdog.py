#!/usr/bin/env python3
"""Check the VPS tunnel pair and notify the first admin when both stay down."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path


STATE_DIR = Path(os.environ.get("TGCTXBOT_WATCHDOG_STATE_DIR", "/var/lib/tgctxbot"))
LOG_PATH = Path(os.environ.get("TGCTXBOT_WATCHDOG_LOG_PATH", "/var/log/tgctxbot/tunnel-events.log"))
STATE_PATH = STATE_DIR / "tunnel-watchdog.json"
TELEGRAM_PROXY = "http://127.0.0.1:43128"
NOTEBOOKLM_PROXY = "127.0.0.1:43129"
ALERT_DELAY_SECONDS = 120
ALERT_THROTTLE_SECONDS = 15 * 60
RECOVERY_STREAK = 5


def now() -> int:
    return int(time.time())


def load_state() -> dict[str, object]:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def save_state(state: dict[str, object]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def log_event(payload: dict[str, object]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def run_curl(args: list[str]) -> bool:
    try:
        completed = subprocess.run(args, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError:
        return False
    return completed.returncode == 0


def probe_telegram(bot_token: str) -> bool:
    return run_curl(
        [
            "curl",
            "-fsS",
            "--max-time",
            "20",
            "--proxy",
            TELEGRAM_PROXY,
            f"https://api.telegram.org/bot{bot_token}/getMe",
        ]
    )


def probe_notebooklm() -> bool:
    return run_curl(
        [
            "curl",
            "-fsS",
            "--max-time",
            "20",
            "--socks5-hostname",
            NOTEBOOKLM_PROXY,
            "-I",
            "https://notebooklm.google.com",
        ]
    )


def fetch_journal_excerpt() -> str:
    try:
        completed = subprocess.run(
            [
                "journalctl",
                "-u",
                "workplace-telegram-ssh-tunnel.service",
                "-u",
                "notebooklm-google-ssh-tunnel.service",
                "-n",
                "20",
                "--no-pager",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return ""
    return (completed.stdout or completed.stderr or "").strip()


def send_telegram_dm(bot_token: str, admin_id: str, text: str) -> bool:
    try:
        completed = subprocess.run(
            [
                "curl",
                "-fsS",
                "-X",
                "POST",
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                "--data-urlencode",
                f"chat_id={admin_id}",
                "--data-urlencode",
                f"text={text}",
                "--data-urlencode",
                "disable_web_page_preview=true",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        return False
    return completed.returncode == 0


def main() -> int:
    bot_token = os.environ.get("BOT_TOKEN", "").strip()
    admin_ids = [part.strip() for part in os.environ.get("BOT_ADMIN_USER_IDS", "").split(",") if part.strip()]
    if not bot_token or not admin_ids:
        log_event({"event": "nlm.tunnel.watchdog.skipped", "reason": "missing_bot_token_or_admin"})
        return 0

    telegram_ok = probe_telegram(bot_token)
    notebooklm_ok = probe_notebooklm()
    healthy = telegram_ok and notebooklm_ok
    state = load_state()
    current = now()
    down_since = int(state.get("down_since", 0) or 0)
    last_alert_at = int(state.get("last_alert_at", 0) or 0)
    healthy_streak = int(state.get("healthy_streak", 0) or 0)
    alerted = bool(state.get("alerted", False))

    if healthy:
        healthy_streak += 1
        state["healthy_streak"] = healthy_streak
        state["down_since"] = 0
        if alerted and healthy_streak >= RECOVERY_STREAK:
            admin_id = admin_ids[0]
            journal = fetch_journal_excerpt()
            message = "NotebookLM tunnels recovered after a temporary outage."
            if journal:
                message += "\n\nLast journal lines:\n" + journal[-1200:]
            if send_telegram_dm(bot_token, admin_id, message):
                log_event({"event": "nlm.tunnel.recovered", "admin_id": admin_id, "healthy_streak": healthy_streak})
                state["alerted"] = False
                state["last_recovered_at"] = current
                state["last_alert_at"] = last_alert_at
        save_state(state)
        log_event({"event": "nlm.tunnel.tick", "telegram_ok": telegram_ok, "notebooklm_ok": notebooklm_ok, "healthy": True})
        return 0

    state["healthy_streak"] = 0
    if not down_since:
        down_since = current
        state["down_since"] = down_since
    down_for = current - down_since
    should_alert = down_for >= ALERT_DELAY_SECONDS and (current - last_alert_at >= ALERT_THROTTLE_SECONDS)
    reason = []
    if not telegram_ok:
        reason.append("telegram")
    if not notebooklm_ok:
        reason.append("notebooklm")
    reason_text = "+".join(reason) or "unknown"

    if should_alert:
        admin_id = admin_ids[0]
        journal = fetch_journal_excerpt()
        message = (
            f"NotebookLM tunnel alert: {reason_text} has been down for {down_for} seconds.\n"
            "Please check the VPS tunnel services.\n"
        )
        if journal:
            message += "\nLast journal lines:\n" + journal[-1200:]
        if send_telegram_dm(bot_token, admin_id, message):
            state["alerted"] = True
            state["last_alert_at"] = current
            log_event({"event": "nlm.tunnel.alert", "admin_id": admin_id, "reason": reason_text, "down_for": down_for})

    save_state(state)
    log_event({"event": "nlm.tunnel.tick", "telegram_ok": telegram_ok, "notebooklm_ok": notebooklm_ok, "healthy": False, "reason": reason_text})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
