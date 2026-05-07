from functools import lru_cache
from typing import TYPE_CHECKING, Any

from app.core.config import get_settings

if TYPE_CHECKING:
    from aiogram import Bot
    from aiogram.types import Message


def _settings_text(*field_names: str) -> str:
    settings = get_settings()
    for field_name in field_names:
        raw = getattr(settings, field_name, None)
        if isinstance(raw, str):
            value = raw.strip()
            if value:
                return value
    return ""


@lru_cache(maxsize=1)
def _imported_chat_link_map() -> dict[int, str]:
    raw = _settings_text("telegram_imported_chat_link_map")
    if not raw:
        return {}

    mapping: dict[int, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        chat_id_raw, target_raw = item.split(":", 1)
        try:
            chat_id = int(chat_id_raw.strip())
        except ValueError:
            continue

        target = target_raw.strip().removeprefix("https://t.me/c/").strip("/")
        target = target.removeprefix("-100")
        if target.isdigit():
            mapping[chat_id] = target
    return mapping


@lru_cache(maxsize=1)
def _live_chat_history_map() -> dict[int, int]:
    raw = _settings_text("telegram_chat_alias_map", "telegram_imported_chat_context_map")
    if not raw:
        return {}

    mapping: dict[int, int] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        live_chat_raw, imported_chat_raw = item.split(":", 1)
        try:
            live_chat_id = int(live_chat_raw.strip())
            imported_chat_id = int(imported_chat_raw.strip())
        except ValueError:
            continue
        mapping[live_chat_id] = imported_chat_id
        if live_chat_id > 0:
            mapping.setdefault(int(f"-100{live_chat_id}"), imported_chat_id)
    return mapping


def resolve_canonical_chat_id(chat_id: int) -> int:
    return _live_chat_history_map().get(chat_id, chat_id)


def build_answer_kwargs(message: "Message | Any") -> dict[str, int]:
    settings = get_settings()
    if not getattr(settings, "bot_reply_in_direct_messages_topic", False):
        return {}
    direct_messages_topic = getattr(message, "direct_messages_topic", None)
    topic_id = getattr(direct_messages_topic, "topic_id", None)
    if topic_id is None:
        return {}
    try:
        return {"direct_messages_topic_id": int(topic_id)}
    except (TypeError, ValueError):
        return {}


def build_message_url(
    chat_id: int,
    message_id: int,
    chat_username: str | None,
    thread_id: int | None = None,
) -> str:
    thread_suffix = f"?thread={thread_id}" if thread_id else ""
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}{thread_suffix}"
    if chat_id > 0:
        mapped_group_id = _imported_chat_link_map().get(chat_id)
        if mapped_group_id is not None:
            return f"https://t.me/c/{mapped_group_id}/{message_id}{thread_suffix}"
        return f"tg://privatepost?channel=-100{chat_id}&post={message_id}"
    internal = str(abs(chat_id)).replace("100", "", 1)
    return f"https://t.me/c/{internal}/{message_id}{thread_suffix}"


async def is_chat_admin(bot: "Bot", chat_id: int, user_id: int) -> bool:
    member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
    return member.status in {"administrator", "creator"}
