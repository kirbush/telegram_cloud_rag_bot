from __future__ import annotations

NOTEBOOKLM_LIGHTWEIGHT_FOOTER = ""
NOTEBOOKLM_AUTH_EXPIRED_MESSAGE = (
    "Сейчас ответы временно недоступны. Админу уже отправлено уведомление."
)


def notebooklm_temporarily_unavailable(reason: str, cooldown_minutes: int) -> str:
    return (
        "Сейчас ответы временно недоступны. "
        f"Попробуй ещё раз через {cooldown_minutes} минут."
    )


def bot_handler_error_message(trace_id: str) -> str:
    return f"Извини, что-то сломалось. trace_id: {trace_id}"
