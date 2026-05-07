"""Telegram bot handlers for the lightweight NotebookLM-only runtime.

`/ask` is kept for backward compatibility in existing chats and delegates to
NotebookLM exactly like `/nlm`. Legacy archive-management commands are gone.
"""

import asyncio
import html
import logging
import os
import re
from asyncio import to_thread
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, NamedTuple

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    MessageReactionCountUpdated,
    MessageReactionUpdated,
    PreCheckoutQuery,
)

from app.bot.keyboards import main_keyboard
from app.bot.messages import NOTEBOOKLM_LIGHTWEIGHT_FOOTER, bot_handler_error_message
from app.bot.update_logging_middleware import UpdateLoggingMiddleware
from app.bot.utils import build_answer_kwargs, resolve_canonical_chat_id
from app.core.config import (
    get_settings,
    is_bot_admin_user,
    is_notebooklm_source_sync_enabled,
)
from app.services.notebooklm_lightweight_history import NotebookLMLightweightHistoryStore
from app.services.notebooklm_metrics import inc_handler_exception_total
from app.services.notebooklm_runtime import NotebookLMRuntimeStore, is_notebooklm_enabled
from app.services.access_store import BotAccessStore, STARS_CURRENCY
from app.services.conversation_store import BotConversationStore, ConversationTurn, ConversationUserSummary
from app.services.telegram_stars import (
    TelegramStarsAPIError,
    TelegramStarsClient,
    reconcile_star_transactions,
    sanitize_telegram_text,
)
from app.services.notebooklm_source_sync import (
    NotebookLMSourceSyncError,
    NotebookLMSourceSyncResult,
    NotebookLMSourceSyncService,
)
from app.services.notebooklm_upload_sync import (
    UploadSyncConfigurationError,
    get_notebooklm_upload_sync_manager,
)
from app.services.notebooklm_remote_auth import (
    RemoteAuthConfigurationError,
    get_notebooklm_remote_auth_manager,
)
from app.services.media_storyboard import build_animation_storyboard
from app.services.openai_vision_context import OpenAIVisionContextService

router = Router()
router.message.outer_middleware(UpdateLoggingMiddleware())
logger = logging.getLogger(__name__)

_LIGHTWEIGHT_VIRTUAL_CHAT_ID = 0
_TYPING_ACTION_INTERVAL_SECONDS = 4.0
_media_context_tasks: set[asyncio.Task[None]] = set()

# user_id -> selected canonical chat_id for DMs
_dm_chat_selection: dict[int, int] = {}

# Owner in-bot admin panel. Authorization still requires BOT_ADMIN_USER_IDS.
_OWNER_ADMIN_USER_ID = 123456789
_OWNER_ADMIN_CALLBACK_PREFIX = "adm:"
_OWNER_ADMIN_INPUT_KINDS = frozenset(
    {"free", "stars", "credits", "override", "clear_override", "balance", "grant"}
)
_owner_admin_pending_inputs: dict[int, dict[str, str]] = {}

_CROCKFORD_BASE32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


class _SelectedMediaPayload(NamedTuple):
    media_kind: str
    source_file_id: str
    source_unique_id: str | None
    analysis_file_id: str
    thumbnail_file_id: str | None
    mime_type: str
    analysis_unique_id: str | None
    storyboard_enabled: bool
    fallback_file_id: str | None
    fallback_mime_type: str | None


class _PreparedMediaPayload(NamedTuple):
    media_bytes: bytes
    mime_type: str
    is_storyboard: bool


def _encode_crockford(value: int, length: int) -> str:
    chars = ["0"] * length
    for index in range(length - 1, -1, -1):
        chars[index] = _CROCKFORD_BASE32[value & 31]
        value >>= 5
    return "".join(chars)


def _short_trace_id() -> str:
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    entropy = int.from_bytes(os.urandom(10), byteorder="big", signed=False)
    return _encode_crockford(timestamp_ms, 10) + _encode_crockford(entropy, 16)


def _lightweight_store() -> NotebookLMLightweightHistoryStore:
    return NotebookLMLightweightHistoryStore(settings=get_settings())


def _access_store() -> BotAccessStore:
    return BotAccessStore(settings=get_settings())


def _conversation_store() -> BotConversationStore:
    return BotConversationStore(settings=get_settings())


def _telegram_stars_client(settings=None) -> TelegramStarsClient:
    effective_settings = settings or get_settings()
    proxy_url = ""
    if bool(getattr(effective_settings, "telegram_proxy_enabled", False)):
        proxy_url = str(getattr(effective_settings, "telegram_proxy_url", "") or "").strip()
    return TelegramStarsClient(
        bot_token=str(getattr(effective_settings, "bot_token", "") or ""),
        proxy_url=proxy_url or None,
    )


def _configured_bot_identity(settings=None) -> dict[str, object]:
    effective_settings = settings or get_settings()
    bot_token = str(getattr(effective_settings, "bot_token", "") or "").strip()
    return {
        "instance_name": str(getattr(effective_settings, "bot_instance_name", "") or "").strip(),
        "bot_id_hint": bot_token.split(":", 1)[0] if ":" in bot_token else "",
        "token_configured": bool(bot_token),
    }


def _is_notebooklm_remote_auth_configured(settings) -> bool:
    return bool(
        str(getattr(settings, "notebooklm_remote_auth_base_url", "") or "").strip()
        and str(getattr(settings, "notebooklm_remote_auth_docker_socket", "") or "").strip()
        and str(getattr(settings, "notebooklm_remote_auth_selenium_image", "") or "").strip()
        and getattr(settings, "notebooklm_proxy_enabled", False)
        and str(getattr(settings, "notebooklm_proxy_url", "") or "").strip()
    )


def _source_sync_enabled(settings=None) -> bool:
    return is_notebooklm_source_sync_enabled(settings or get_settings())


def _lightweight_user_kwargs(message: Message) -> dict[str, object | None]:
    return {
        "user_id": message.from_user.id if message.from_user else None,
        "username": message.from_user.username if message.from_user else None,
        "display_name": _extract_display_name(message),
    }


async def _send_typing_action(bot: Bot, chat_id: int, *, action: str = "typing") -> None:
    try:
        await bot.send_chat_action(chat_id, action)
    except Exception:
        logger.debug(
            "bot.typing_action failed chat_id=%s action=%s",
            chat_id,
            action,
            exc_info=True,
        )


async def _typing_keepalive_loop(
    bot: Bot,
    chat_id: int,
    *,
    action: str = "typing",
    interval_s: float = _TYPING_ACTION_INTERVAL_SECONDS,
) -> None:
    while True:
        await asyncio.sleep(interval_s)
        await _send_typing_action(bot, chat_id, action=action)


@asynccontextmanager
async def _typing_keepalive(
    message: Message,
    *,
    bot: Bot | None = None,
    action: str = "typing",
    interval_s: float = _TYPING_ACTION_INTERVAL_SECONDS,
):
    active_bot = bot or getattr(message, "bot", None)
    chat_id = getattr(getattr(message, "chat", None), "id", None)
    if active_bot is None or chat_id is None:
        yield
        return

    await _send_typing_action(active_bot, chat_id, action=action)
    task = asyncio.create_task(
        _typing_keepalive_loop(active_bot, chat_id, action=action, interval_s=interval_s)
    )
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _persist_lightweight_history_message(
    message: Message,
    *,
    canonical_chat_id: int,
    text: str,
    edited: bool = False,
) -> None:
    def _persist() -> None:
        store = _lightweight_store()
        store.upsert_message(
            canonical_chat_id=canonical_chat_id,
            live_chat_id=message.chat.id,
            chat_title=getattr(message.chat, "title", None),
            chat_type=getattr(message.chat, "type", None),
            chat_username=getattr(message.chat, "username", None),
            telegram_message_id=message.message_id,
            text=text,
            message_date=message.date,
            reply_to_message_id=message.reply_to_message.message_id if message.reply_to_message else None,
            thread_id=message.message_thread_id,
            edited=edited,
            **_lightweight_user_kwargs(message),
        )
        store.append_timeline_event(
            canonical_chat_id=canonical_chat_id,
            live_chat_id=message.chat.id,
            event_type="message_edit" if edited else "message_text",
            source_telegram_message_id=message.message_id,
            event_date=message.date,
            text=text,
            media_kind=None,
            file_id=None,
            file_unique_id=None,
            thumbnail_file_id=None,
            reply_to_message_id=message.reply_to_message.message_id if message.reply_to_message else None,
            thread_id=message.message_thread_id,
            **_lightweight_user_kwargs(message),
        )

    await to_thread(_persist)


def _normalize_reaction_label(reaction_type) -> str:
    reaction_kind = getattr(reaction_type, "type", None)
    if reaction_kind == "emoji":
        return str(getattr(reaction_type, "emoji", "") or "").strip()
    if reaction_kind == "custom_emoji":
        custom_emoji_id = str(getattr(reaction_type, "custom_emoji_id", "") or "").strip()
        return f"custom_emoji:{custom_emoji_id}" if custom_emoji_id else "custom_emoji"
    if reaction_kind == "paid":
        return "paid_reaction"
    return str(reaction_kind or "unknown_reaction")


def _reaction_snapshot_from_counts(reactions) -> dict[str, int]:
    snapshot: dict[str, int] = {}
    for reaction in reactions or []:
        label = _normalize_reaction_label(getattr(reaction, "type", None))
        snapshot[label] = int(getattr(reaction, "total_count", 0) or 0)
    return {label: count for label, count in snapshot.items() if count > 0}


def _reaction_actor_kwargs(update: MessageReactionUpdated) -> dict[str, str | int | None]:
    actor_user = getattr(update, "user", None)
    if actor_user is not None:
        username = str(getattr(actor_user, "username", "") or "").strip() or None
        first_name = str(getattr(actor_user, "first_name", "") or "").strip()
        last_name = str(getattr(actor_user, "last_name", "") or "").strip()
        display_name = " ".join(part for part in (first_name, last_name) if part).strip() or None
        return {
            "actor_type": "user",
            "actor_user_id": int(getattr(actor_user, "id", 0) or 0) or None,
            "actor_chat_id": None,
            "username": username,
            "display_name": display_name,
        }

    actor_chat = getattr(update, "actor_chat", None)
    if actor_chat is not None:
        return {
            "actor_type": "chat",
            "actor_user_id": None,
            "actor_chat_id": int(getattr(actor_chat, "id", 0) or 0) or None,
            "username": str(getattr(actor_chat, "username", "") or "").strip() or None,
            "display_name": str(getattr(actor_chat, "title", "") or "").strip() or None,
        }

    return {
        "actor_type": "",
        "actor_user_id": None,
        "actor_chat_id": None,
        "username": None,
        "display_name": None,
    }


def _track_media_context_task(task: asyncio.Task[None]) -> None:
    _media_context_tasks.add(task)
    task.add_done_callback(_media_context_tasks.discard)


async def _download_telegram_file_bytes(bot: Bot, *, file_id: str) -> bytes:
    telegram_file = await bot.get_file(file_id)
    buffer = BytesIO()
    await bot.download_file(telegram_file.file_path, destination=buffer)
    return buffer.getvalue()


def _media_attachment_text(media_kind: str) -> str:
    if media_kind == "sticker":
        return "Sticker attached."
    if media_kind == "animation":
        return "Animation attached."
    return "Photo attached."


def _select_media_payload(message: Message) -> _SelectedMediaPayload | None:
    if message.photo:
        largest = max(message.photo, key=lambda item: int(getattr(item, "file_size", 0) or 0))
        return _SelectedMediaPayload(
            media_kind="photo",
            source_file_id=largest.file_id,
            source_unique_id=getattr(largest, "file_unique_id", None),
            analysis_file_id=largest.file_id,
            thumbnail_file_id=None,
            mime_type="image/jpeg",
            analysis_unique_id=getattr(largest, "file_unique_id", None),
            storyboard_enabled=False,
            fallback_file_id=None,
            fallback_mime_type=None,
        )
    animation = getattr(message, "animation", None)
    if animation is not None:
        thumbnail = getattr(animation, "thumbnail", None)
        thumbnail_file_id = getattr(thumbnail, "file_id", None)
        analysis_file_id = animation.file_id
        analysis_unique_id = getattr(animation, "file_unique_id", None)
        selected_mime_type = getattr(animation, "mime_type", None) or "image/gif"
        return _SelectedMediaPayload(
            media_kind="animation",
            source_file_id=animation.file_id,
            source_unique_id=getattr(animation, "file_unique_id", None),
            analysis_file_id=analysis_file_id,
            thumbnail_file_id=thumbnail_file_id,
            mime_type=selected_mime_type,
            analysis_unique_id=analysis_unique_id,
            storyboard_enabled=True,
            fallback_file_id=thumbnail_file_id,
            fallback_mime_type="image/jpeg" if thumbnail_file_id else None,
        )
    sticker = getattr(message, "sticker", None)
    if sticker is None:
        return None
    thumbnail = getattr(sticker, "thumbnail", None)
    if getattr(sticker, "is_animated", False) or getattr(sticker, "is_video", False):
        thumbnail_file_id = getattr(thumbnail, "file_id", None)
        analysis_file_id = sticker.file_id
        analysis_unique_id = getattr(sticker, "file_unique_id", None)
        selected_mime_type = (
            "video/webm"
            if getattr(sticker, "is_video", False)
            else "application/x-tgsticker"
        )
        storyboard_enabled = True
        fallback_file_id = thumbnail_file_id
        fallback_mime_type = "image/jpeg" if thumbnail_file_id else None
    else:
        analysis_file_id = sticker.file_id
        analysis_unique_id = getattr(sticker, "file_unique_id", None)
        thumbnail_file_id = getattr(thumbnail, "file_id", None)
        selected_mime_type = "image/webp"
        storyboard_enabled = False
        fallback_file_id = None
        fallback_mime_type = None
    return _SelectedMediaPayload(
        media_kind="sticker",
        source_file_id=sticker.file_id,
        source_unique_id=getattr(sticker, "file_unique_id", None),
        analysis_file_id=analysis_file_id,
        thumbnail_file_id=thumbnail_file_id,
        mime_type=selected_mime_type,
        analysis_unique_id=analysis_unique_id,
        storyboard_enabled=storyboard_enabled,
        fallback_file_id=fallback_file_id,
        fallback_mime_type=fallback_mime_type,
    )


async def _prepare_media_analysis_payload(
    bot: Bot,
    *,
    file_id: str,
    mime_type: str,
    storyboard_enabled: bool,
    fallback_file_id: str | None,
    fallback_mime_type: str | None,
) -> _PreparedMediaPayload:
    media_bytes = await _download_telegram_file_bytes(bot, file_id=file_id)
    if not storyboard_enabled:
        return _PreparedMediaPayload(
            media_bytes=media_bytes,
            mime_type=mime_type,
            is_storyboard=False,
        )

    try:
        storyboard_bytes = await to_thread(build_animation_storyboard, media_bytes)
    except Exception:
        logger.exception("bot.media_storyboard render failed file_id=%s", file_id)
        storyboard_bytes = None
    if storyboard_bytes:
        return _PreparedMediaPayload(
            media_bytes=storyboard_bytes,
            mime_type="image/png",
            is_storyboard=True,
        )

    if fallback_file_id and fallback_file_id != file_id:
        fallback_bytes = await _download_telegram_file_bytes(bot, file_id=fallback_file_id)
        return _PreparedMediaPayload(
            media_bytes=fallback_bytes,
            mime_type=fallback_mime_type or "image/jpeg",
            is_storyboard=False,
        )

    return _PreparedMediaPayload(
        media_bytes=media_bytes,
        mime_type=mime_type,
        is_storyboard=False,
    )


async def _process_media_context_job(
    bot: Bot,
    *,
    job_pk: int,
    canonical_chat_id: int,
    live_chat_id: int,
    source_telegram_message_id: int,
    media_kind: str,
    file_id: str,
    mime_type: str,
    thumbnail_file_id: str | None,
    storyboard_enabled: bool,
    fallback_file_id: str | None,
    fallback_mime_type: str | None,
    reply_to_message_id: int | None,
    thread_id: int | None,
    user_id: int | None,
    username: str | None,
    display_name: str | None,
) -> None:
    settings = get_settings()

    def _mark_started():
        return _lightweight_store().mark_media_job_running(job_pk=job_pk)

    job = await to_thread(_mark_started)
    try:
        prepared_payload = await _prepare_media_analysis_payload(
            bot,
            file_id=file_id,
            mime_type=mime_type,
            storyboard_enabled=storyboard_enabled,
            fallback_file_id=fallback_file_id,
            fallback_mime_type=fallback_mime_type,
        )
        result = await OpenAIVisionContextService(settings=settings).analyze_media(
            media_bytes=prepared_payload.media_bytes,
            media_kind=media_kind,
            mime_type=prepared_payload.mime_type,
            is_storyboard=prepared_payload.is_storyboard,
        )

        def _persist_context() -> None:
            store = _lightweight_store()
            store.append_timeline_event(
                canonical_chat_id=canonical_chat_id,
                live_chat_id=live_chat_id,
                event_type="sticker_context" if media_kind == "sticker" else "image_context",
                source_telegram_message_id=source_telegram_message_id,
                event_date=datetime.now(timezone.utc),
                text=result.to_timeline_text(),
                media_kind=media_kind,
                file_id=file_id,
                file_unique_id=None,
                thumbnail_file_id=thumbnail_file_id,
                user_id=user_id,
                username=username,
                display_name=display_name,
                reply_to_message_id=reply_to_message_id,
                thread_id=thread_id,
            )
            store.mark_media_job_completed(job_pk=job_pk)

        await to_thread(_persist_context)
    except Exception as exc:
        retryable = int(job.attempt_count) < int(getattr(settings, "media_context_max_retries", 2) or 2)
        logger.exception(
            "bot.media_context_job failed chat_id=%s message_id=%s media_kind=%s retryable=%s",
            canonical_chat_id,
            source_telegram_message_id,
            media_kind,
            retryable,
        )

        def _mark_failed() -> None:
            if retryable:
                _lightweight_store().mark_media_job_retryable(job_pk=job_pk, error=str(exc))
            else:
                _lightweight_store().mark_media_job_failed(job_pk=job_pk, error=str(exc))

        await to_thread(_mark_failed)


async def _persist_lightweight_media_message(message: Message, *, canonical_chat_id: int) -> None:
    selected_payload = _select_media_payload(message)
    if selected_payload is None:
        return
    settings = get_settings()

    def _persist() -> tuple[
        int | None,
        str,
        str,
        str,
        str | None,
        str | None,
        str | None,
        bool,
        int | None,
        int | None,
        int | None,
        str | None,
        str | None,
    ]:
        store = _lightweight_store()
        store.append_timeline_event(
            canonical_chat_id=canonical_chat_id,
            live_chat_id=message.chat.id,
            event_type="media_attached",
            source_telegram_message_id=message.message_id,
            event_date=message.date,
            text=_media_attachment_text(selected_payload.media_kind),
            media_kind=selected_payload.media_kind,
            file_id=selected_payload.source_file_id,
            file_unique_id=selected_payload.source_unique_id,
            thumbnail_file_id=selected_payload.thumbnail_file_id,
            reply_to_message_id=message.reply_to_message.message_id if message.reply_to_message else None,
            thread_id=message.message_thread_id,
            **_lightweight_user_kwargs(message),
        )
        job_pk = None
        if getattr(settings, "media_context_enabled", False):
            job = store.create_media_job(
                canonical_chat_id=canonical_chat_id,
                source_telegram_message_id=message.message_id,
                media_kind=selected_payload.media_kind,
                file_id=selected_payload.analysis_file_id,
                file_unique_id=selected_payload.analysis_unique_id,
                thumbnail_file_id=selected_payload.thumbnail_file_id,
            )
            job_pk = job.job_pk
        return (
            job_pk,
            selected_payload.media_kind,
            selected_payload.analysis_file_id,
            selected_payload.mime_type,
            selected_payload.thumbnail_file_id,
            selected_payload.fallback_file_id,
            selected_payload.fallback_mime_type,
            selected_payload.storyboard_enabled,
            message.reply_to_message.message_id if message.reply_to_message else None,
            message.message_thread_id,
            message.from_user.id if message.from_user else None,
            message.from_user.username if message.from_user else None,
            _extract_display_name(message),
        )

    (
        job_pk,
        persisted_media_kind,
        persisted_file_id,
        persisted_mime_type,
        persisted_thumbnail_file_id,
        persisted_fallback_file_id,
        persisted_fallback_mime_type,
        persisted_storyboard_enabled,
        reply_to_message_id,
        thread_id,
        user_id,
        username,
        display_name,
    ) = await to_thread(_persist)

    if job_pk is None:
        return

    task = asyncio.create_task(
        _process_media_context_job(
            message.bot,
            job_pk=job_pk,
            canonical_chat_id=canonical_chat_id,
            live_chat_id=message.chat.id,
            source_telegram_message_id=message.message_id,
            media_kind=persisted_media_kind,
            file_id=persisted_file_id,
            mime_type=persisted_mime_type,
            thumbnail_file_id=persisted_thumbnail_file_id,
            storyboard_enabled=persisted_storyboard_enabled,
            fallback_file_id=persisted_fallback_file_id,
            fallback_mime_type=persisted_fallback_mime_type,
            reply_to_message_id=reply_to_message_id,
            thread_id=thread_id,
            user_id=user_id,
            username=username,
            display_name=display_name,
        ),
        name=f"media-context-{canonical_chat_id}-{message.message_id}",
    )
    _track_media_context_task(task)


async def _answer_message(message: Message, text: str, **kwargs) -> None:
    answer_kwargs = build_answer_kwargs(message)
    answer_kwargs.update(kwargs)
    await message.answer(text, **answer_kwargs)


def _strip_bot_mention(text: str, bot_username: str | None) -> str:
    if not bot_username:
        return text.strip()
    cleaned = re.sub(rf"(?i)@{re.escape(bot_username)}\b", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,:;-")
    return cleaned.strip()


async def _extract_triggered_question(message: Message, bot: Bot) -> tuple[str | None, str | None]:
    """Classify an incoming group text as a question for the bot.

    Only explicit @mention triggers an answer. Replies to the bot's own
    messages are intentionally ignored.
    """
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return None, None

    bot_username = None
    try:
        bot_user = await bot.me()
        bot_username = getattr(bot_user, "username", None)
    except Exception:
        logger.exception("bot.identity lookup failed while classifying incoming text")

    mention_token = f"@{bot_username}".lower() if bot_username else None
    if mention_token and mention_token in text.lower():
        cleaned = _strip_bot_mention(text, bot_username)
        return (cleaned or text), "mention"

    return None, None


@router.message(Command("start"))
async def start(message: Message) -> None:
    settings = get_settings()
    manual_mode = not _source_sync_enabled(settings)
    if _is_private_chat(message):
        if manual_mode:
            private_help = (
                "Бот готов. Используй /ask или /nlm, чтобы задать вопрос.\n"
                "База знаний обновляется вручную владельцем бота."
            )
        else:
            private_help = (
                "Бот готов. Выбери чат для поиска: /chats\n"
                "Затем просто пиши вопрос или используй /ask и /nlm."
            )
        await _answer_message(message, private_help, reply_markup=_owner_private_keyboard(message, settings))
        return

    if manual_mode:
        group_help = (
            "Бот готов. Используй /ask, /nlm, /askboth или упомяни меня в сообщении.\n"
            "База знаний обновляется вручную владельцем бота."
        )
    else:
        group_help = "Бот готов. Используй /ask, /nlm, /askboth или упомяни меня в сообщении с вопросом."
    await _answer_message(message, group_help, reply_markup=main_keyboard())


@router.message(Command("help"))
async def help_cmd(message: Message) -> None:
    settings = get_settings()
    if not _source_sync_enabled(settings):
        await _answer_message(
            message,
            "/ask <вопрос> — задать вопрос\n"
            "/nlm <вопрос> — задать вопрос\n"
            "/askboth <вопрос> — задать вопрос\n"
            "/balance, /limits, /buy — лимиты и кредиты Telegram Stars\n"
            "/chats — выбрать чат для поиска\n"
            "@bot <вопрос> — спросить в обычном сообщении группы\n"
            "База знаний обновляется вручную владельцем бота.",
            reply_markup=_owner_private_keyboard(message, settings),
        )
        return

    await _answer_message(
        message,
        "/ask <вопрос> — задать вопрос\n"
        "/nlm <вопрос> — задать вопрос\n"
        "/askboth <вопрос> — задать вопрос\n"
        "/balance, /limits, /buy — лимиты и кредиты Telegram Stars\n"
        "/chats — выбрать чат для поиска в ЛС\n"
        "@bot <вопрос> — спросить в обычном сообщении группы",
        reply_markup=_owner_private_keyboard(message, settings),
    )


@router.message(Command("admin", "settings", "service"))
async def owner_admin_cmd(message: Message, bot: Bot) -> None:
    settings = get_settings()
    if not _is_owner_admin_message(message, settings):
        await _answer_message(message, "Панель доступна только владельцу бота в личном чате.")
        return
    user_id = getattr(getattr(message, "from_user", None), "id", None)
    if user_id is not None:
        _owner_admin_pending_inputs.pop(int(user_id), None)
    text, keyboard = await _owner_admin_home_panel(bot)
    await _answer_message(
        message,
        text,
        reply_markup=keyboard,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def _list_available_chats() -> list[dict]:
    settings = get_settings()
    if not _source_sync_enabled(settings):
        return [
            {
                "chat_id": _LIGHTWEIGHT_VIRTUAL_CHAT_ID,
                "title": "База знаний",
                "message_count": 0,
            }
        ]
    lightweight_chats = await to_thread(
        NotebookLMLightweightHistoryStore(settings=settings).list_chat_summaries
    )
    if lightweight_chats:
        return [
            {
                "chat_id": chat.canonical_chat_id,
                "title": chat.title,
                "message_count": chat.message_count,
            }
            for chat in lightweight_chats
        ]
    return [
        {
            "chat_id": _LIGHTWEIGHT_VIRTUAL_CHAT_ID,
            "title": "База знаний",
            "message_count": 0,
        }
    ]


def _is_private_chat(message: Message) -> bool:
    return getattr(getattr(message, "chat", None), "type", None) == "private"


def _is_owner_user_id(user_id: int | None) -> bool:
    try:
        return int(user_id) == _OWNER_ADMIN_USER_ID
    except (TypeError, ValueError):
        return False


def _is_owner_admin_user(user_id: int | None, settings=None) -> bool:
    return _is_owner_user_id(user_id) and is_bot_admin_user(user_id, settings or get_settings())


def _is_owner_admin_message(message: Message, settings=None) -> bool:
    user_id = getattr(getattr(message, "from_user", None), "id", None)
    return _is_private_chat(message) and _is_owner_admin_user(user_id, settings or get_settings())


def _is_owner_admin_callback(callback: CallbackQuery, settings=None) -> bool:
    user_id = getattr(getattr(callback, "from_user", None), "id", None)
    message = getattr(callback, "message", None)
    chat_type = getattr(getattr(message, "chat", None), "type", None)
    return chat_type == "private" and _is_owner_admin_user(user_id, settings or get_settings())


def _owner_admin_pending_filter(message: Message) -> bool:
    user_id = getattr(getattr(message, "from_user", None), "id", None)
    return (
        _is_private_chat(message)
        and _is_owner_admin_user(user_id, get_settings())
        and int(user_id) in _owner_admin_pending_inputs
    )


def _owner_admin_keyboard(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=text, callback_data=callback_data) for text, callback_data in row]
            for row in rows
        ]
    )


def _owner_private_keyboard(message: Message, settings=None) -> InlineKeyboardMarkup:
    base = main_keyboard().inline_keyboard
    if not _is_owner_admin_message(message, settings or get_settings()):
        return InlineKeyboardMarkup(inline_keyboard=base)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Админка", callback_data="adm:home")],
            *base,
        ]
    )


def _owner_admin_home_keyboard() -> InlineKeyboardMarkup:
    return _owner_admin_keyboard(
        [
            [("Access / Stars", "adm:access"), ("Live Stars", "adm:stars")],
            [("Dialogs", "adm:conv"), ("NotebookLM", "adm:nlm")],
            [("Refresh", "adm:home")],
        ]
    )


def _owner_admin_access_keyboard(enabled: bool) -> InlineKeyboardMarkup:
    toggle_text = "Выключить лимиты" if enabled else "Включить лимиты"
    return _owner_admin_keyboard(
        [
            [(toggle_text, "adm:cfg:toggle"), ("Обновить", "adm:access")],
            [("Лимит / 24ч", "adm:input:free"), ("Цена Stars", "adm:input:stars")],
            [("Кредиты за покупку", "adm:input:credits")],
            [("Override чата", "adm:input:override"), ("Стереть override", "adm:input:clear_override")],
            [("Баланс user", "adm:input:balance"), ("Выдать credits", "adm:input:grant")],
            [("Live Stars", "adm:stars"), ("Назад", "adm:home")],
        ]
    )


def _owner_admin_notebooklm_keyboard(source_sync_enabled: bool) -> InlineKeyboardMarkup:
    rows = [
        [("Auth session", "adm:svc:auth"), ("Monitoring", "adm:svc:monitoring")],
        [("Обновить статус", "adm:nlm")],
    ]
    if source_sync_enabled:
        rows.insert(1, [("Sync выбранного чата", "adm:svc:update")])
    rows.append([("Назад", "adm:home")])
    return _owner_admin_keyboard(rows)


def _owner_admin_conversations_keyboard() -> InlineKeyboardMarkup:
    return _owner_admin_keyboard(
        [
            [("Обновить", "adm:conv")],
            [("Назад", "adm:home")],
        ]
    )


def _owner_admin_cancel_keyboard() -> InlineKeyboardMarkup:
    return _owner_admin_keyboard([[("Отмена", "adm:cancel"), ("Назад", "adm:access")]])


def _html(value: object) -> str:
    return html.escape(str(value if value is not None else ""), quote=False)


def _yes_no(value: object) -> str:
    return "yes" if bool(value) else "no"


def _short_path(value: object, *, max_len: int = 72) -> str:
    text = str(value or "")
    if len(text) <= max_len:
        return text
    return "..." + text[-max_len + 3 :]


def _clip_text(value: object, *, max_len: int = 180) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _input_prompt(kind: str) -> str:
    prompts = {
        "free": "Отправь новый бесплатный лимит вопросов за rolling 24 часа. Например: <code>20</code>",
        "stars": "Отправь цену пакета в Telegram Stars. Например: <code>25</code>",
        "credits": "Отправь количество credits в одном Stars-пакете. Например: <code>10</code>",
        "override": (
            "Отправь override чата в формате:\n"
            "<code>chat_id enabled free stars credits</code>\n"
            "Пример: <code>-1001234567890 on 5 25 10</code>\n"
            "Пиши <code>-</code>, чтобы наследовать поле."
        ),
        "clear_override": "Отправь <code>chat_id</code>, для которого нужно стереть override.",
        "balance": "Отправь <code>telegram_user_id chat_id</code>, чтобы посмотреть баланс.",
        "grant": (
            "Отправь <code>telegram_user_id chat_id delta [note]</code>.\n"
            "Пример: <code>123456789 -1001234567890 5 test grant</code>\n"
            "Для отзыва credits delta может быть отрицательным."
        ),
    }
    return prompts.get(kind, "Отправь значение или /cancel.")


def _parse_int_value(
    raw_value: str,
    *,
    field: str,
    min_value: int,
    max_value: int = 1_000_000,
) -> int:
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}: нужно целое число.") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{field}: допустимый диапазон {min_value}..{max_value}.")
    return value


def _parse_optional_int(raw_value: str, *, field: str, min_value: int) -> int | None:
    if str(raw_value).strip() == "-":
        return None
    return _parse_int_value(raw_value, field=field, min_value=min_value)


def _parse_optional_enabled(raw_value: str) -> bool | None:
    value = str(raw_value).strip().lower()
    if value == "-":
        return None
    if value in {"1", "true", "yes", "on", "enable", "enabled", "вкл"}:
        return True
    if value in {"0", "false", "no", "off", "disable", "disabled", "выкл"}:
        return False
    raise ValueError("enabled: используй on/off или -.")


def _format_admin_balance(balance: dict[str, Any]) -> str:
    reset = balance.get("next_reset_at") or "not scheduled"
    return (
        f"User <code>{_html(balance.get('telegram_user_id'))}</code>, "
        f"chat <code>{_html(balance.get('chat_id'))}</code>\n"
        f"Access enabled: <b>{_yes_no(balance.get('enabled'))}</b>\n"
        f"Free: <b>{_html(balance.get('free_remaining'))}</b> left "
        f"({ _html(balance.get('used_in_window')) }/{ _html(balance.get('free_limit')) } used)\n"
        f"Manual credits: <b>{_html(balance.get('manual_credits'))}</b>\n"
        f"Paid credits: <b>{_html(balance.get('paid_credits'))}</b>\n"
        f"Total remaining: <b>{_html(balance.get('total_remaining'))}</b>\n"
        f"Next reset: <code>{_html(reset)}</code>"
    )


def _get_dm_chat_id(user_id: int, live_chat_id: int) -> int | None:
    if user_id in _dm_chat_selection:
        return _dm_chat_selection[user_id]
    return None


async def _fetch_owner_admin_bot_identity(bot: Bot, settings=None) -> dict[str, object]:
    identity = _configured_bot_identity(settings or get_settings())
    try:
        bot_user = await bot.me()
    except Exception:
        logger.exception("bot.owner_admin getMe failed")
        return identity
    username = str(getattr(bot_user, "username", "") or "").strip()
    identity.update(
        {
            "live_id": getattr(bot_user, "id", None),
            "username": username,
            "username_label": f"@{username}" if username else "",
        }
    )
    return identity


async def _owner_admin_runtime_status(settings=None) -> dict[str, Any]:
    effective_settings = settings or get_settings()
    try:
        return await to_thread(NotebookLMRuntimeStore(settings=effective_settings).get_runtime_status)
    except Exception as exc:
        logger.exception("bot.owner_admin runtime status failed")
        return {
            "enabled": getattr(effective_settings, "notebooklm_enabled", False),
            "auth_ready": False,
            "source": "error",
            "config_error": f"{type(exc).__name__}: {exc}",
        }


async def _owner_admin_home_panel(bot: Bot) -> tuple[str, InlineKeyboardMarkup]:
    settings = get_settings()
    identity = await _fetch_owner_admin_bot_identity(bot, settings)
    runtime = await _owner_admin_runtime_status(settings)
    access_status = await to_thread(_access_store().status)
    global_access = access_status.get("global", {})
    bot_label = identity.get("username_label") or (
        f"id {identity.get('live_id')}" if identity.get("live_id") else "unknown"
    )
    owner_configured = is_bot_admin_user(_OWNER_ADMIN_USER_ID, settings)
    source_sync = _source_sync_enabled(settings)
    background_sync = bool(getattr(settings, "notebooklm_background_sync_enabled", False))
    text = (
        "<b>Админка владельца</b>\n"
        f"Bot: <code>{_html(bot_label)}</code>\n"
        f"Instance: <code>{_html(identity.get('instance_name') or 'default')}</code>\n"
        f"Token id hint: <code>{_html(identity.get('bot_id_hint') or 'unknown')}</code>\n"
        f"Owner 123456789 configured: <b>{_yes_no(owner_configured)}</b>\n\n"
        "<b>NotebookLM</b>\n"
        f"Enabled: <b>{_yes_no(runtime.get('enabled'))}</b>\n"
        f"Auth file ready: <b>{_yes_no(runtime.get('auth_ready'))}</b>\n"
        f"Runtime source: <code>{_html(runtime.get('source') or 'unknown')}</code>\n"
        f"Notebook: <code>{_html(runtime.get('notebook_id') or 'not set')}</code>\n"
        f"Source sync: <b>{_yes_no(source_sync)}</b>, background: <b>{_yes_no(background_sync)}</b>\n\n"
        "<b>Access / Stars</b>\n"
        f"Enabled: <b>{_yes_no(global_access.get('enabled'))}</b>\n"
        f"Free / 24h: <b>{_html(global_access.get('free_questions_per_24h'))}</b>\n"
        f"Package: <b>{_html(global_access.get('credits_per_purchase'))}</b> credits "
        f"for <b>{_html(global_access.get('stars_price'))}</b> XTR"
    )
    config_error = str(runtime.get("config_error") or "").strip()
    if config_error:
        text += f"\nConfig detail: <code>{_html(sanitize_telegram_text(config_error))}</code>"
    return text, _owner_admin_home_keyboard()


async def _owner_admin_access_panel() -> tuple[str, InlineKeyboardMarkup]:
    status = await to_thread(_access_store().status)
    ledger = await to_thread(_access_store().stars_ledger_summary)
    global_access = status.get("global", {})
    totals = status.get("totals", {})
    overrides = status.get("chat_overrides", [])
    override_preview = ", ".join(str(item.get("chat_id")) for item in overrides[:5]) or "none"
    if len(overrides) > 5:
        override_preview += f", +{len(overrides) - 5}"
    text = (
        "<b>Access / Telegram Stars</b>\n"
        f"Access enabled: <b>{_yes_no(global_access.get('enabled'))}</b>\n"
        f"Free questions / 24h: <b>{_html(global_access.get('free_questions_per_24h'))}</b>\n"
        f"Stars price: <b>{_html(global_access.get('stars_price'))}</b> XTR\n"
        f"Credits per purchase: <b>{_html(global_access.get('credits_per_purchase'))}</b>\n"
        f"Currency: <code>{_html(status.get('currency') or STARS_CURRENCY)}</code>\n"
        f"State: <code>{_html(_short_path(status.get('state_path')))}</code>\n\n"
        "<b>Ledger</b>\n"
        f"Usage / orders / payments: <b>{_html(totals.get('usage_count', 0))}</b> / "
        f"<b>{_html(totals.get('order_count', 0))}</b> / "
        f"<b>{_html(totals.get('payment_count', 0))}</b>\n"
        f"Local paid Stars: <b>{_html(ledger.get('total_local_paid_stars_amount', 0))}</b>\n"
        f"Paid credits remaining: <b>{_html((ledger.get('paid_credits') or {}).get('remaining', 0))}</b>\n"
        f"Manual credits remaining: <b>{_html((ledger.get('manual_credits') or {}).get('remaining', 0))}</b>\n"
        f"Chat overrides: <code>{_html(override_preview)}</code>"
    )
    return text, _owner_admin_access_keyboard(bool(global_access.get("enabled")))


def _format_conversation_user(summary: ConversationUserSummary) -> str:
    username = f"@{summary.username}" if summary.username else "no username"
    name = summary.display_name or username
    return (
        f"<code>{summary.telegram_user_id}</code> "
        f"{_html(name)} ({_html(username)})\n"
        f"  turns: <b>{summary.turn_count}</b>, last: <b>{_html(summary.last_status or 'unknown')}</b>\n"
        f"  <i>{_html(_clip_text(summary.last_question, max_len=120))}</i>"
    )


def _format_conversation_turn(turn: ConversationTurn) -> str:
    username = f"@{turn.username}" if turn.username else "no username"
    name = turn.display_name or username
    meta = (
        f"#{turn.turn_id} <b>{_html(turn.status)}</b> "
        f"user <code>{turn.telegram_user_id}</code> {_html(name)}"
    )
    if turn.chat_title:
        meta += f", chat {_html(turn.chat_title)}"
    if turn.latency_ms is not None:
        meta += f", {turn.latency_ms} ms"
    detail = turn.error_text or turn.reason or turn.answer_text or ""
    if detail:
        detail = f"\n  → {_html(_clip_text(detail, max_len=140))}"
    return f"{meta}\n  <i>{_html(_clip_text(turn.question_text, max_len=140))}</i>{detail}"


def _format_conversation_users(users: list[ConversationUserSummary]) -> str:
    if not users:
        return "Пока нет записанных диалогов."
    return "\n\n".join(_format_conversation_user(user) for user in users)


def _format_conversation_turns(turns: list[ConversationTurn]) -> str:
    if not turns:
        return "Пока нет записанных вопросов."
    return "\n\n".join(_format_conversation_turn(turn) for turn in turns)


async def _owner_admin_conversations_panel() -> tuple[str, InlineKeyboardMarkup]:
    store = _conversation_store()
    status, users, turns = await asyncio.gather(
        to_thread(store.status),
        to_thread(store.list_recent_users, limit=5),
        to_thread(store.list_recent_turns, limit=5),
    )
    text = (
        "<b>Dialogs</b>\n"
        f"State: <code>{_html(_short_path(status.get('state_path')))}</code>\n"
        f"Users: <b>{_html(status.get('user_count', 0))}</b>, "
        f"turns: <b>{_html(status.get('turn_count', 0))}</b>, "
        f"answered/denied/failed: <b>{_html(status.get('answered_count', 0))}</b> / "
        f"<b>{_html(status.get('denied_count', 0))}</b> / "
        f"<b>{_html(status.get('failed_count', 0))}</b>\n\n"
        "<b>Commands</b>\n"
        "<code>/users</code>, <code>/last_questions</code>, <code>/history user_id [limit]</code>\n\n"
        "<b>Recent users</b>\n"
        f"{_format_conversation_users(users)}\n\n"
        "<b>Recent questions</b>\n"
        f"{_format_conversation_turns(turns)}"
    )
    return text, _owner_admin_conversations_keyboard()


async def _owner_admin_notebooklm_panel() -> tuple[str, InlineKeyboardMarkup]:
    settings = get_settings()
    runtime = await _owner_admin_runtime_status(settings)
    source_sync = _source_sync_enabled(settings)
    remote_auth = _is_notebooklm_remote_auth_configured(settings)
    selected_chat = _dm_chat_selection.get(_OWNER_ADMIN_USER_ID)
    text = (
        "<b>NotebookLM service</b>\n"
        f"Enabled: <b>{_yes_no(runtime.get('enabled'))}</b>\n"
        f"Auth file ready: <b>{_yes_no(runtime.get('auth_ready'))}</b>\n"
        f"Storage file exists: <b>{_yes_no(runtime.get('storage_state_exists'))}</b>\n"
        f"Runtime configured: <b>{_yes_no(runtime.get('runtime_state_configured'))}</b>\n"
        f"Runtime file exists: <b>{_yes_no(runtime.get('runtime_state_exists'))}</b>\n"
        f"Source: <code>{_html(runtime.get('source') or 'unknown')}</code>\n"
        f"Notebook: <code>{_html(runtime.get('notebook_id') or 'not set')}</code>\n"
        f"Source sync: <b>{_yes_no(source_sync)}</b>\n"
        f"Background sync: <b>{_yes_no(getattr(settings, 'notebooklm_background_sync_enabled', False))}</b>\n"
        f"Remote browser auth: <b>{_yes_no(remote_auth)}</b>\n"
        f"Selected DM chat: <code>{_html(selected_chat or 'virtual runtime')}</code>"
    )
    config_error = str(runtime.get("config_error") or "").strip()
    if config_error:
        text += f"\nConfig detail: <code>{_html(sanitize_telegram_text(config_error))}</code>"
    return text, _owner_admin_notebooklm_keyboard(source_sync)


def _telegram_live_error(client: TelegramStarsClient, exc: Exception) -> str:
    if isinstance(exc, TelegramStarsAPIError):
        message = str(exc)
    else:
        message = f"Unexpected Telegram Stars API error: {type(exc).__name__}"
    return client.sanitize_error(message)


async def _owner_admin_stars_panel() -> tuple[str, InlineKeyboardMarkup]:
    store = _access_store()
    local_summary = await to_thread(store.stars_ledger_summary)
    local_payments = await to_thread(store.star_payments)
    client = _telegram_stars_client()

    errors: list[str] = []
    bot_identity: dict[str, Any] = {"ok": False}
    balance: dict[str, Any] = {"ok": False, "amount": None, "currency": STARS_CURRENCY}
    transactions: list[dict[str, Any]] = []
    transactions_available = False

    try:
        bot_identity = {"ok": True, **dict(await client.fetch_bot_identity())}
    except Exception as exc:
        errors.append(_telegram_live_error(client, exc))

    try:
        balance = {"ok": True, **dict(await client.fetch_balance())}
    except Exception as exc:
        errors.append(_telegram_live_error(client, exc))

    try:
        transactions = await client.fetch_transactions(offset=0, limit=10)
        transactions_available = True
    except Exception as exc:
        errors.append(_telegram_live_error(client, exc))

    reconciliation = reconcile_star_transactions(
        transactions,
        local_payments,
        page_available=transactions_available,
    )
    live_bot = bot_identity.get("username_label") or bot_identity.get("username") or bot_identity.get("id") or "unknown"
    text = (
        "<b>Telegram Stars live</b>\n"
        f"Live bot: <code>{_html(live_bot)}</code> ({'ok' if bot_identity.get('ok') else 'unavailable'})\n"
        f"Live balance: <b>{_html(balance.get('amount') if balance.get('ok') else 'unavailable')}</b> XTR\n"
        f"Fetched transactions: <b>{_html(len(transactions) if transactions_available else 'unavailable')}</b>\n\n"
        "<b>Local ledger</b>\n"
        f"State: <code>{_html(_short_path(local_summary.get('state_path')))}</code>\n"
        f"Orders / payments / usage: <b>{_html(local_summary.get('local_order_count', 0))}</b> / "
        f"<b>{_html(local_summary.get('local_payment_count', 0))}</b> / "
        f"<b>{_html(local_summary.get('usage_count', 0))}</b>\n"
        f"Local paid Stars: <b>{_html(local_summary.get('total_local_paid_stars_amount', 0))}</b>\n"
        f"Reconciliation matched / live-only / local-not-page: "
        f"<b>{_html(reconciliation.get('matched_count', 0))}</b> / "
        f"<b>{_html(reconciliation.get('live_not_found_locally_count', 0))}</b> / "
        f"<b>{_html(reconciliation.get('local_not_in_fetched_page_count', 0))}</b>"
    )
    if errors:
        text += "\nLive API detail: <code>" + _html("; ".join(errors[:3])) + "</code>"
    return text, _owner_admin_keyboard([[("Обновить", "adm:stars"), ("Назад", "adm:home")]])


async def _edit_owner_admin_message(
    callback: CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    message = getattr(callback, "message", None)
    if message is None:
        await callback.answer("Открой /admin в личном чате.", show_alert=True)
        return
    try:
        await message.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        logger.debug("bot.owner_admin edit_text failed, sending a new message", exc_info=True)
        await message.answer(
            text,
            reply_markup=reply_markup,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


@router.message(Command("chats"))
async def chats_cmd(message: Message) -> None:
    chats = await _list_available_chats()
    if not chats:
        await _answer_message(message, "Нет доступных чатов.")
        return

    current = _dm_chat_selection.get(message.from_user.id) if message.from_user else None

    buttons = []
    for chat in chats[:10]:
        marker = "-> " if chat["chat_id"] == current else ""
        label = f"{marker}{chat['title']} ({chat['message_count']} msgs)"
        buttons.append(
            [
                InlineKeyboardButton(
                    text=label[:64],
                    callback_data=f"select_chat:{chat['chat_id']}",
                )
            ]
        )

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    hint = ""
    if current:
        current_title = next((c["title"] for c in chats if c["chat_id"] == current), str(current))
        hint = f"\nТекущий: {current_title}"
    await _answer_message(
        message,
        f"Выбери чат для поиска в личных сообщениях:{hint}",
        reply_markup=keyboard,
    )


@router.callback_query(F.data.startswith("select_chat:"))
async def select_chat_callback(callback: CallbackQuery) -> None:
    try:
        chat_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Ошибка выбора чата")
        return

    user_id = callback.from_user.id
    _dm_chat_selection[user_id] = chat_id

    chats = await _list_available_chats()
    title = next((c["title"] for c in chats if c["chat_id"] == chat_id), str(chat_id))
    logger.info("dm.chat_selected user_id=%s chat_id=%s title=%r", user_id, chat_id, title)

    await callback.message.edit_text(
        f"Выбран чат: {title}\n"
        f"Теперь /ask в ЛС будет искать ответ в контексте этого чата.\n"
        f"Смени чат: /chats"
    )
    await callback.answer(f"Чат: {title}")


def _resolve_chat_id_for_query(message: Message) -> int | None:
    if _is_private_chat(message):
        user_id = message.from_user.id if message.from_user else None
        if user_id and user_id in _dm_chat_selection:
            return _dm_chat_selection[user_id]
        return None
    return message.chat.id


def _resolve_notebooklm_chat_id_for_query(message: Message) -> int | None:
    if _is_private_chat(message):
        selected_chat_id = _resolve_chat_id_for_query(message)
        if selected_chat_id is not None:
            return selected_chat_id
        return _LIGHTWEIGHT_VIRTUAL_CHAT_ID
    return resolve_canonical_chat_id(message.chat.id)


def _resolve_notebooklm_chat_id_for_user_context(message: Message, user_id: int | None) -> int | None:
    if _is_private_chat(message):
        if user_id is not None and user_id in _dm_chat_selection:
            return _dm_chat_selection[user_id]
        return _LIGHTWEIGHT_VIRTUAL_CHAT_ID
    return resolve_canonical_chat_id(message.chat.id)


def _question_participant_id(message: Message) -> int | None:
    user_id = getattr(getattr(message, "from_user", None), "id", None)
    return int(user_id) if user_id is not None else None


def _format_notebooklm_status(balance: dict, *, chat_id: int) -> str:
    reset = balance.get("next_reset_at") or "пока не запланирован"
    return (
        "Баланс вопросов\n"
        f"Осталось бесплатных: {balance['free_remaining']} "
        f"(использовано {balance['used_in_window']} из {balance['free_limit']} за 24 часа)\n"
        f"Подарочные кредиты: {balance['manual_credits']}\n"
        f"Кредиты Telegram Stars: {balance['paid_credits']}\n"
        f"Следующий бесплатный вопрос: {reset}"
    )


def _format_access_denied(balance: dict) -> str:
    reset = balance.get("next_reset_at") or "когда освободится лимит за 24 часа"
    return (
        "Пока лимит вопросов закончился.\n"
        f"Бесплатные вопросы: использовано {balance['used_in_window']} из {balance['free_limit']} за 24 часа.\n"
        f"Кредиты: {balance['manual_credits']} подарочных, {balance['paid_credits']} через Telegram Stars.\n"
        f"Следующий бесплатный вопрос: {reset}\n"
        "Баланс: /balance. Купить кредиты: /buy."
    )


async def _record_conversation_question(
    message: Message,
    *,
    source: str,
    chat_id: int,
    question_key: str,
    question_text: str,
) -> int | None:
    user = getattr(message, "from_user", None)
    user_id = getattr(user, "id", None)
    if user_id is None:
        return None
    chat = getattr(message, "chat", None)
    try:
        turn = await to_thread(
            _conversation_store().record_question,
            source=source,
            telegram_user_id=int(user_id),
            username=getattr(user, "username", None),
            display_name=_extract_display_name(message),
            chat_id=int(getattr(chat, "id", chat_id) or chat_id),
            chat_type=getattr(chat, "type", None),
            chat_title=getattr(chat, "title", None) or getattr(chat, "username", None),
            message_id=getattr(message, "message_id", None),
            thread_id=getattr(message, "message_thread_id", None),
            question_key=question_key,
            question_text=question_text,
        )
    except Exception:
        logger.exception(
            "bot.conversation record_failed chat_id=%s user_id=%s source=%s",
            chat_id,
            user_id,
            source,
        )
        return None
    return int(turn.turn_id)


async def _update_conversation_turn(
    turn_id: int | None,
    *,
    status: str,
    answer_text: str | None = None,
    error_text: str | None = None,
    reason: str | None = None,
    latency_ms: int | None = None,
    notebook_id: str | None = None,
) -> None:
    if turn_id is None:
        return
    try:
        await to_thread(
            _conversation_store().update_turn,
            turn_id=turn_id,
            status=status,
            answer_text=answer_text,
            error_text=error_text,
            reason=reason,
            latency_ms=latency_ms,
            notebook_id=notebook_id,
        )
    except Exception:
        logger.exception("bot.conversation update_failed turn_id=%s status=%s", turn_id, status)


async def _consume_question_access(
    message: Message,
    *,
    chat_id: int,
    question_key: str,
    conversation_turn_id: int | None = None,
) -> bool:
    participant_id = _question_participant_id(message)
    if participant_id is None:
        text = "Чтобы задать вопрос, нужен Telegram-аккаунт."
        await _update_conversation_turn(
            conversation_turn_id,
            status="denied",
            answer_text=text,
            reason="missing_telegram_user",
        )
        await _answer_message(message, text)
        return False
    if is_bot_admin_user(participant_id, get_settings()):
        logger.info(
            "bot.access admin_bypass chat_id=%s user_id=%s",
            chat_id,
            participant_id,
        )
        return True

    def _consume():
        return _access_store().consume_question(
            telegram_user_id=participant_id,
            chat_id=chat_id,
            question_key=question_key,
        )

    consumed = await to_thread(_consume)
    if consumed is not None:
        logger.info(
            "bot.access consumed chat_id=%s user_id=%s source=%s",
            chat_id,
            participant_id,
            consumed.source,
        )
        return True

    balance = await to_thread(
        _access_store().balance,
        telegram_user_id=participant_id,
        chat_id=chat_id,
    )
    text = _format_access_denied(balance)
    await _update_conversation_turn(
        conversation_turn_id,
        status="denied",
        answer_text=text,
        reason="limit_exceeded",
    )
    await _answer_message(message, text)
    return False


@router.message(Command("monitoring", "kuma"))
async def monitoring_cmd(message: Message) -> None:
    settings = get_settings()
    if not _is_owner_admin_message(message, settings):
        await _answer_message(message, "Команда доступна только владельцу бота в личном чате.")
        return

    url = str(getattr(settings, "uptime_kuma_public_url", "") or "").strip().rstrip("/")
    if not url:
        await _answer_message(message, "Uptime Kuma URL не настроен.")
        return

    await _answer_message(
        message,
        f"Uptime Kuma: {url}\nЛогин и пароль смотри в ops-секретах.",
        disable_web_page_preview=True,
    )


def _parse_owner_limit(parts: list[str], *, index: int, default: int = 10) -> int:
    if len(parts) <= index:
        return default
    try:
        return min(50, max(1, int(parts[index])))
    except ValueError as exc:
        raise ValueError("limit должен быть числом от 1 до 50.") from exc


async def _ensure_owner_conversation_command(message: Message) -> bool:
    if _is_owner_admin_message(message, get_settings()):
        return True
    await _answer_message(message, "Команда доступна только владельцу бота в личном чате.")
    return False


@router.message(Command("users"))
async def users_cmd(message: Message) -> None:
    if not await _ensure_owner_conversation_command(message):
        return
    parts = str(getattr(message, "text", "") or "").split()
    try:
        limit = _parse_owner_limit(parts, index=1)
    except ValueError as exc:
        await _answer_message(message, str(exc))
        return
    users = await to_thread(_conversation_store().list_recent_users, limit=limit)
    await _answer_message(
        message,
        "<b>Recent users</b>\n" + _format_conversation_users(users),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@router.message(Command("last_questions"))
async def last_questions_cmd(message: Message) -> None:
    if not await _ensure_owner_conversation_command(message):
        return
    parts = str(getattr(message, "text", "") or "").split()
    try:
        limit = _parse_owner_limit(parts, index=1)
    except ValueError as exc:
        await _answer_message(message, str(exc))
        return
    turns = await to_thread(_conversation_store().list_recent_turns, limit=limit)
    await _answer_message(
        message,
        "<b>Last questions</b>\n" + _format_conversation_turns(turns),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@router.message(Command("history"))
async def history_cmd(message: Message) -> None:
    if not await _ensure_owner_conversation_command(message):
        return
    parts = str(getattr(message, "text", "") or "").split()
    if len(parts) < 2:
        await _answer_message(message, "Формат: /history <telegram_user_id> [limit]")
        return
    try:
        telegram_user_id = int(parts[1])
        limit = _parse_owner_limit(parts, index=2)
    except ValueError:
        await _answer_message(message, "Формат: /history <telegram_user_id> [limit]")
        return
    turns = await to_thread(
        _conversation_store().list_user_history,
        telegram_user_id=telegram_user_id,
        limit=limit,
    )
    await _answer_message(
        message,
        f"<b>History for <code>{telegram_user_id}</code></b>\n" + _format_conversation_turns(turns),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def _is_notebooklm_auth_allowed(message: Message, bot: Bot) -> bool:
    user_id = getattr(getattr(message, "from_user", None), "id", None)
    return _is_private_chat(message) and _is_owner_admin_user(user_id, get_settings())


def _markdown_to_telegram_html(text: str) -> str:
    if not text:
        return ""
    escaped = html.escape(text, quote=False)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped, flags=re.DOTALL)
    escaped = re.sub(r"__(.+?)__", r"<b>\1</b>", escaped, flags=re.DOTALL)
    escaped = re.sub(r"`([^`\n]+?)`", r"<code>\1</code>", escaped)
    escaped = re.sub(
        r"\[([^\]\n]+?)\]\(([^)\s]+)\)",
        lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>',
        escaped,
    )
    return escaped


def _format_nlm_result(result, *, include_footer: bool = False) -> str:
    if result.error:
        return html.escape(result.error, quote=False)
    parts = [_markdown_to_telegram_html(result.answer)]
    if include_footer and NOTEBOOKLM_LIGHTWEIGHT_FOOTER:
        parts.append(f"\n<i>{html.escape(NOTEBOOKLM_LIGHTWEIGHT_FOOTER, quote=False)}</i>")
    if result.sources and get_settings().bot_nlm_show_sources:
        parts.append("\n--- Sources ---")
        for index, source in enumerate(result.sources[:5], 1):
            parts.append(f"{index}. {html.escape(source[:200], quote=False)}")
    return "\n".join(parts)


def _format_triggered_nlm_result(result, *, include_footer: bool = False) -> str:
    if result.error:
        return html.escape(result.error, quote=False)
    rendered = _markdown_to_telegram_html(result.answer)
    if include_footer and NOTEBOOKLM_LIGHTWEIGHT_FOOTER:
        rendered += f"\n\n<i>{html.escape(NOTEBOOKLM_LIGHTWEIGHT_FOOTER, quote=False)}</i>"
    return rendered


def _format_update_result(result: NotebookLMSourceSyncResult) -> str:
    if result.status == "noop":
        prefix = "NotebookLM context is already up to date."
        if result.bootstrap_created:
            prefix = "NotebookLM sync watermark initialized. New messages were not found."
        return (
            f"{prefix}\n"
            f"Notebook: `{result.notebook_id}`\n"
            f"Chat: `{result.canonical_chat_id}`\n"
            f"Watermark: `{result.watermark_after}`"
        )
    return (
        "NotebookLM context updated.\n"
        f"Notebook: `{result.notebook_id}`\n"
        f"Chat: `{result.canonical_chat_id}`\n"
        f"Uploaded messages: `{result.message_count}`\n"
        f"Watermark: `{result.watermark_before}` -> `{result.watermark_after}`\n"
        f"Export: `{result.export_path or ''}`"
    )


async def _ask_notebooklm(message: Message, raw_question: str, *, command_label: str) -> None:
    settings = get_settings()
    if not is_notebooklm_enabled(settings):
        await _answer_message(message, "Сейчас ответы временно недоступны. Попробуй позже.")
        return

    if not raw_question:
        await _answer_message(message, f"Usage: /{command_label} <вопрос>")
        return

    nlm_chat_id = _resolve_notebooklm_chat_id_for_query(message)
    if nlm_chat_id is None:
        await _answer_message(message, "Сначала выбери чат для поиска: /chats")
        return

    question_key = f"{command_label}:{getattr(message, 'message_id', '')}"
    turn_id = await _record_conversation_question(
        message,
        source=f"command:{command_label}",
        chat_id=nlm_chat_id,
        question_key=question_key,
        question_text=raw_question,
    )
    if not await _consume_question_access(
        message,
        chat_id=nlm_chat_id,
        question_key=question_key,
        conversation_turn_id=turn_id,
    ):
        return

    from app.services.notebooklm_service import NotebookLMService

    logger.info(
        "bot.%s_command chat_id=%s nlm_chat_id=%s user_id=%s question=%r",
        command_label,
        getattr(message.chat, "id", None),
        nlm_chat_id,
        getattr(getattr(message, "from_user", None), "id", None),
        raw_question[:200],
    )
    try:
        async with _typing_keepalive(message):
            result = await NotebookLMService().ask(chat_id=nlm_chat_id, question=raw_question)
    except Exception:
        logger.exception("bot.%s_command notebooklm_failed", command_label)
        error_text = "Не получилось подготовить ответ. Попробуй ещё раз чуть позже."
        await _update_conversation_turn(
            turn_id,
            status="failed",
            error_text=error_text,
            reason="exception",
        )
        await _answer_message(message, error_text)
        return

    rendered = _format_nlm_result(result, include_footer=True)
    await _update_conversation_turn(
        turn_id,
        status="failed" if getattr(result, "error", None) else "answered",
        answer_text=None if getattr(result, "error", None) else rendered,
        error_text=rendered if getattr(result, "error", None) else None,
        reason=getattr(result, "reason", None) or ("answer_error" if getattr(result, "error", None) else None),
        latency_ms=getattr(result, "latency_ms", None),
        notebook_id=getattr(result, "notebook_id", None),
    )
    await _answer_message(message, rendered, parse_mode="HTML")


@router.message(Command("ask"))
async def ask(message: Message) -> None:
    raw_question = re.sub(r"^/ask(@\S+)?\s*", "", message.text or "", count=1).strip()
    await _ask_notebooklm(message, raw_question, command_label="ask")


@router.message(Command("nlm"))
async def nlm_cmd(message: Message) -> None:
    raw_question = re.sub(r"^/nlm(@\S+)?\s*", "", message.text or "", count=1).strip()
    await _ask_notebooklm(message, raw_question, command_label="nlm")


@router.message(Command("askboth"))
async def askboth_cmd(message: Message) -> None:
    raw_question = re.sub(r"^/askboth(@\S+)?\s*", "", message.text or "", count=1).strip()
    await _ask_notebooklm(message, raw_question, command_label="askboth")


@router.message(Command("balance"))
async def balance_cmd(message: Message) -> None:
    user_id = _question_participant_id(message)
    chat_id = _resolve_notebooklm_chat_id_for_query(message)
    if user_id is None or chat_id is None:
        await _answer_message(message, "Баланс доступен после выбора чата: /chats.")
        return

    balance = await to_thread(_access_store().balance, telegram_user_id=user_id, chat_id=chat_id)
    await _answer_message(
        message,
        "Баланс вопросов\n"
        f"Бесплатные: осталось {balance['free_remaining']} "
        f"(использовано {balance['used_in_window']} из {balance['free_limit']} за 24 часа)\n"
        f"Подарочные кредиты: {balance['manual_credits']}\n"
        f"Кредиты Telegram Stars: {balance['paid_credits']}",
    )


@router.message(Command("limits"))
async def limits_cmd(message: Message) -> None:
    user_id = _question_participant_id(message)
    chat_id = _resolve_notebooklm_chat_id_for_query(message)
    if user_id is None or chat_id is None:
        await _answer_message(message, "Лимиты доступны после выбора чата: /chats.")
        return

    balance = await to_thread(_access_store().balance, telegram_user_id=user_id, chat_id=chat_id)
    reset = balance.get("next_reset_at") or "пока не запланирован"
    await _answer_message(
        message,
        "Лимиты вопросов\n"
        "Окно лимита: 24 часа\n"
        f"Бесплатный лимит: {balance['free_limit']} вопрос(ов)\n"
        f"Следующий бесплатный вопрос: {reset}\n"
        "Кредиты Telegram Stars используются после бесплатных и подарочных кредитов.",
    )


@router.message(Command("paysupport"))
async def paysupport_cmd(message: Message) -> None:
    await _answer_message(
        message,
        "Telegram Stars покупают дополнительные кредиты для вопросов. "
        "Используй /buy, чтобы создать счёт. Если с оплатой что-то не так, напиши админу id платежа Telegram.",
    )


@router.message(Command("terms"))
async def terms_cmd(message: Message) -> None:
    await _answer_message(
        message,
        "Условия: покупка Telegram Stars выдаёт расходуемые кредиты для вопросов в выбранном чате. "
        "Это не подписка; кредиты не сгорают автоматически и используются после бесплатного лимита и подарочных кредитов.",
    )


@router.message(Command("buy", "stars"))
async def buy_cmd(message: Message) -> None:
    user_id = _question_participant_id(message)
    chat_id = _resolve_notebooklm_chat_id_for_query(message)
    if user_id is None or chat_id is None:
        await _answer_message(message, "Choose a chat first with /chats, then retry /buy.")
        return

    order = await to_thread(
        _access_store().create_stars_order,
        telegram_user_id=user_id,
        chat_id=chat_id,
    )
    await message.answer_invoice(
        title="Кредиты для вопросов",
        description=f"{order.credits} кредитов для вопросов",
        payload=order.payload,
        provider_token="",
        currency=STARS_CURRENCY,
        prices=[
            LabeledPrice(
                label=f"{order.credits} кредитов для вопросов",
                amount=order.stars_amount,
            )
        ],
    )


@router.pre_checkout_query()
async def pre_checkout_query(pre_checkout: PreCheckoutQuery) -> None:
    user_id = getattr(getattr(pre_checkout, "from_user", None), "id", None)
    valid = await to_thread(
        _access_store().validate_stars_order,
        payload=str(getattr(pre_checkout, "invoice_payload", "") or ""),
        currency=str(getattr(pre_checkout, "currency", "") or ""),
        total_amount=int(getattr(pre_checkout, "total_amount", 0) or 0),
        telegram_user_id=int(user_id) if user_id is not None else None,
    )
    if not valid:
        await pre_checkout.answer(
            ok=False,
            error_message="Telegram Stars invoice is no longer valid. Create a new one with /buy.",
        )
        return
    await pre_checkout.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment_message(message: Message) -> None:
    payment = getattr(message, "successful_payment", None)
    user_id = _question_participant_id(message)
    if payment is None or user_id is None:
        return
    try:
        granted, credits = await to_thread(
            _access_store().record_successful_payment,
            payload=str(getattr(payment, "invoice_payload", "") or ""),
            currency=str(getattr(payment, "currency", "") or ""),
            total_amount=int(getattr(payment, "total_amount", 0) or 0),
            telegram_user_id=user_id,
            telegram_payment_charge_id=getattr(payment, "telegram_payment_charge_id", None),
            provider_payment_charge_id=getattr(payment, "provider_payment_charge_id", None),
            raw=getattr(payment, "model_dump", lambda: {})(),
        )
    except Exception:
        logger.exception("bot.successful_payment failed user_id=%s", user_id)
        await _answer_message(message, "Оплата Telegram Stars прошла, но кредиты не начислились. Напиши в поддержку через /paysupport.")
        return

    if granted:
        await _answer_message(message, f"Оплата Telegram Stars прошла. Добавлено кредитов: {credits}.")
    else:
        await _answer_message(message, "Эта оплата уже была обработана; кредиты повторно не начислялись.")


def _create_notebooklm_auth_reply(
    *,
    settings,
    chat_id: int | None,
    user_id: int | None,
    message_thread_id: int | None,
    private_chat: bool,
) -> str:
    session = get_notebooklm_upload_sync_manager().create_session(
        source="telegram-bot",
        requested_by_chat_id=chat_id,
        requested_by_user_id=user_id,
        notify_chat_id=chat_id,
        notify_message_thread_id=None if private_chat else message_thread_id,
    )

    remote_session = None
    if _is_notebooklm_remote_auth_configured(settings):
        try:
            remote_session = get_notebooklm_remote_auth_manager().create_session(
                source="telegram-bot",
                requested_by_chat_id=chat_id,
                requested_by_user_id=user_id,
                notify_chat_id=chat_id,
                notify_message_thread_id=None if private_chat else message_thread_id,
            )
        except (RemoteAuthConfigurationError, ValueError) as exc:
            logger.warning("bot.auth_nlm remote auth unavailable: %s", exc)
        except Exception:
            logger.exception(
                "bot.auth_nlm failed to create remote session chat_id=%s user_id=%s",
                chat_id,
                user_id,
            )

    reply = (
        "NotebookLM auth refresh session создана.\n"
        f"Одноразовая ссылка для телефона, ручного JSON или Windows helper: {session['entry_url']}\n"
        f"Истекает: {session['expires_at']}"
    )
    if session.get("protocol_url"):
        reply += "\nWindows helper тоже доступен на этой странице, если открыт Windows-ПК с установленным local helper."
    reply += "\nPhone/manual import uses the same one-time auth refresh link."
    if remote_session and remote_session.get("auth_url"):
        reply += f"\nVPS browser login link: {remote_session['auth_url']}"
    return reply


@router.message(Command("update"))
async def update_cmd(message: Message, bot: Bot) -> None:
    settings = get_settings()
    if not await _is_notebooklm_auth_allowed(message, bot):
        await _answer_message(
            message,
            "Команда доступна только владельцу бота в личном чате.",
        )
        return
    if not is_notebooklm_enabled(settings):
        await _answer_message(message, "NotebookLM integration is not configured.")
        return
    if not is_notebooklm_source_sync_enabled(settings):
        await _answer_message(
            message,
            "Для этого бота chat-driven sync отключён. Добавляй и обновляй источники вручную через UI NotebookLM.",
        )
        return

    chat_id = _resolve_notebooklm_chat_id_for_query(message)
    if chat_id is None:
        await _answer_message(message, "Сначала выбери чат для поиска: /chats")
        return
    if chat_id == _LIGHTWEIGHT_VIRTUAL_CHAT_ID:
        await _answer_message(
            message,
            "Команда /update требует локальную БД истории и недоступна для virtual runtime chat.",
        )
        return

    try:
        async with _typing_keepalive(message, bot=bot):
            result = await NotebookLMSourceSyncService().sync_chat_delta(chat_id=chat_id)
    except NotebookLMSourceSyncError as exc:
        await _answer_message(message, f"NotebookLM update error: {exc}")
        return
    except Exception as exc:
        if "Authentication expired or invalid" in str(exc):
            await _answer_message(
                message,
                "Авторизация NotebookLM на VPS истекла. Обнови её через `/auth_nlm` или `/admin/notebooklm`, затем повтори `/update`.",
                parse_mode="Markdown",
            )
            return
        logger.exception(
            "bot.update_command failed chat_id=%s user_id=%s resolved_chat_id=%s",
            getattr(message.chat, "id", None),
            getattr(getattr(message, "from_user", None), "id", None),
            chat_id,
        )
        await _answer_message(message, "Не удалось обновить NotebookLM контекст. Проверь логи.")
        return

    await _answer_message(message, _format_update_result(result), parse_mode="Markdown")


@router.message(Command("auth_nlm"))
async def auth_nlm(message: Message, bot: Bot) -> None:
    settings = get_settings()
    if not await _is_notebooklm_auth_allowed(message, bot):
        await _answer_message(
            message,
            "Команда доступна только владельцу бота в личном чате.",
        )
        return

    try:
        reply = _create_notebooklm_auth_reply(
            settings=settings,
            chat_id=getattr(message.chat, "id", None),
            user_id=getattr(getattr(message, "from_user", None), "id", None),
            message_thread_id=getattr(message, "message_thread_id", None),
            private_chat=_is_private_chat(message),
        )
    except UploadSyncConfigurationError as exc:
        await _answer_message(message, str(exc))
        return
    except Exception:
        logger.exception(
            "bot.auth_nlm failed chat_id=%s user_id=%s",
            getattr(message.chat, "id", None),
            getattr(getattr(message, "from_user", None), "id", None),
        )
        await _answer_message(message, "Не удалось создать NotebookLM Windows sync session. Проверь конфигурацию.")
        return
    await _answer_message(message, reply, disable_web_page_preview=True)


async def _apply_owner_admin_input(kind: str, raw_text: str) -> tuple[str, str]:
    text = str(raw_text or "").strip()
    store = _access_store()
    if kind == "free":
        value = _parse_int_value(text, field="Лимит", min_value=0)
        await to_thread(store.set_global_config, free_questions_per_24h=value)
        return f"<b>Сохранено:</b> free / 24h = <code>{value}</code>", "access"
    if kind == "stars":
        value = _parse_int_value(text, field="Цена Stars", min_value=1)
        await to_thread(store.set_global_config, stars_price=value)
        return f"<b>Сохранено:</b> Stars price = <code>{value}</code>", "access"
    if kind == "credits":
        value = _parse_int_value(text, field="Credits", min_value=1)
        await to_thread(store.set_global_config, credits_per_purchase=value)
        return f"<b>Сохранено:</b> credits per purchase = <code>{value}</code>", "access"
    if kind == "override":
        parts = text.split()
        if len(parts) != 5:
            raise ValueError("Нужен формат: chat_id enabled free stars credits.")
        chat_id = _parse_int_value(parts[0], field="chat_id", min_value=-10**16, max_value=10**16)
        enabled = _parse_optional_enabled(parts[1])
        free = _parse_optional_int(parts[2], field="free", min_value=0)
        stars = _parse_optional_int(parts[3], field="stars", min_value=1)
        credits = _parse_optional_int(parts[4], field="credits", min_value=1)
        await to_thread(
            store.set_chat_override,
            chat_id=chat_id,
            enabled=enabled,
            free_questions_per_24h=free,
            stars_price=stars,
            credits_per_purchase=credits,
        )
        return f"<b>Override сохранён:</b> chat <code>{chat_id}</code>", "access"
    if kind == "clear_override":
        chat_id = _parse_int_value(text, field="chat_id", min_value=-10**16, max_value=10**16)
        await to_thread(store.clear_chat_override, chat_id=chat_id)
        return f"<b>Override удалён:</b> chat <code>{chat_id}</code>", "access"
    if kind == "balance":
        parts = text.split()
        if len(parts) != 2:
            raise ValueError("Нужен формат: telegram_user_id chat_id.")
        user_id = _parse_int_value(parts[0], field="telegram_user_id", min_value=1)
        chat_id = _parse_int_value(parts[1], field="chat_id", min_value=-10**16, max_value=10**16)
        balance = await to_thread(store.balance, telegram_user_id=user_id, chat_id=chat_id)
        return "<b>Баланс пользователя</b>\n" + _format_admin_balance(balance), "access"
    if kind == "grant":
        parts = text.split(maxsplit=3)
        if len(parts) < 3:
            raise ValueError("Нужен формат: telegram_user_id chat_id delta [note].")
        user_id = _parse_int_value(parts[0], field="telegram_user_id", min_value=1)
        chat_id = _parse_int_value(parts[1], field="chat_id", min_value=-10**16, max_value=10**16)
        delta = _parse_int_value(parts[2], field="delta", min_value=-1_000_000)
        if delta == 0:
            raise ValueError("delta не должен быть 0.")
        note = parts[3] if len(parts) > 3 else "owner-admin"
        balance = await to_thread(
            store.grant_manual_credits,
            telegram_user_id=user_id,
            chat_id=chat_id,
            delta=delta,
            reason=note,
        )
        return (
            f"<b>Manual credits обновлены:</b> user <code>{user_id}</code>, "
            f"chat <code>{chat_id}</code>, balance <code>{balance}</code>",
            "access",
        )
    raise ValueError("Неизвестный режим ввода.")


@router.callback_query(F.data.startswith(_OWNER_ADMIN_CALLBACK_PREFIX))
async def owner_admin_callback(callback: CallbackQuery, bot: Bot) -> None:
    settings = get_settings()
    if not _is_owner_admin_callback(callback, settings):
        await callback.answer("Недоступно.", show_alert=False)
        return

    data = str(getattr(callback, "data", "") or "")
    user_id = int(getattr(callback.from_user, "id"))
    try:
        if data == "adm:home":
            _owner_admin_pending_inputs.pop(user_id, None)
            text, keyboard = await _owner_admin_home_panel(bot)
        elif data == "adm:access":
            text, keyboard = await _owner_admin_access_panel()
        elif data == "adm:nlm":
            text, keyboard = await _owner_admin_notebooklm_panel()
        elif data == "adm:conv":
            text, keyboard = await _owner_admin_conversations_panel()
        elif data == "adm:stars":
            text, keyboard = await _owner_admin_stars_panel()
        elif data == "adm:cancel":
            _owner_admin_pending_inputs.pop(user_id, None)
            access_text, keyboard = await _owner_admin_access_panel()
            text = "<b>Ввод отменён.</b>\n\n" + access_text
        elif data == "adm:cfg:toggle":
            current = await to_thread(_access_store().get_global_config)
            await to_thread(_access_store().set_global_config, enabled=not current.enabled)
            access_text, keyboard = await _owner_admin_access_panel()
            text = "<b>Сохранено.</b>\n\n" + access_text
        elif data.startswith("adm:input:"):
            kind = data.rsplit(":", 1)[-1]
            if kind not in _OWNER_ADMIN_INPUT_KINDS:
                await callback.answer("Неизвестное действие.", show_alert=True)
                return
            _owner_admin_pending_inputs[user_id] = {"kind": kind}
            text = "<b>Ввод настройки</b>\n" + _input_prompt(kind) + "\n\n/cancel — отменить."
            keyboard = _owner_admin_cancel_keyboard()
        elif data == "adm:svc:auth":
            callback_message = getattr(callback, "message", None)
            reply = await to_thread(
                _create_notebooklm_auth_reply,
                settings=settings,
                chat_id=getattr(getattr(callback_message, "chat", None), "id", None),
                user_id=user_id,
                message_thread_id=None,
                private_chat=True,
            )
            text = "<b>NotebookLM auth</b>\n" + _html(reply)
            keyboard = _owner_admin_notebooklm_keyboard(_source_sync_enabled(settings))
        elif data == "adm:svc:monitoring":
            url = str(getattr(settings, "uptime_kuma_public_url", "") or "").strip().rstrip("/")
            if url:
                text = f"<b>Monitoring</b>\nUptime Kuma: <code>{_html(url)}</code>"
            else:
                text = "<b>Monitoring</b>\nUptime Kuma URL не настроен."
            keyboard = _owner_admin_notebooklm_keyboard(_source_sync_enabled(settings))
        elif data == "adm:svc:update":
            if not is_notebooklm_enabled(settings):
                text = "<b>NotebookLM sync</b>\nNotebookLM integration is not configured."
            elif not _source_sync_enabled(settings):
                text = "<b>NotebookLM sync</b>\nChat-driven sync отключён для этого бота."
            else:
                chat_id = _dm_chat_selection.get(user_id)
                if chat_id is None or chat_id == _LIGHTWEIGHT_VIRTUAL_CHAT_ID:
                    text = "<b>NotebookLM sync</b>\nСначала выбери реальный чат через /chats."
                else:
                    try:
                        result = await NotebookLMSourceSyncService().sync_chat_delta(chat_id=chat_id)
                    except NotebookLMSourceSyncError as exc:
                        text = f"<b>NotebookLM sync error</b>\n<code>{_html(exc)}</code>"
                    except Exception as exc:
                        logger.exception("bot.owner_admin sync failed chat_id=%s", chat_id)
                        text = (
                            "<b>NotebookLM sync error</b>\n"
                            f"<code>{_html(type(exc).__name__)}</code>"
                        )
                    else:
                        text = "<b>NotebookLM sync</b>\n<code>" + _html(_format_update_result(result)) + "</code>"
            keyboard = _owner_admin_notebooklm_keyboard(_source_sync_enabled(settings))
        else:
            await callback.answer("Неизвестное действие.", show_alert=True)
            return
    except (UploadSyncConfigurationError, ValueError) as exc:
        logger.warning("bot.owner_admin callback failed: %s", exc)
        text = f"<b>Ошибка</b>\n<code>{_html(sanitize_telegram_text(exc))}</code>"
        keyboard = _owner_admin_home_keyboard()
    except Exception as exc:
        logger.exception("bot.owner_admin callback crashed action=%s", data)
        text = f"<b>Ошибка</b>\n<code>{_html(type(exc).__name__)}</code>"
        keyboard = _owner_admin_home_keyboard()

    await _edit_owner_admin_message(callback, text, keyboard)
    await callback.answer()


@router.message(_owner_admin_pending_filter)
async def owner_admin_input_message(message: Message, bot: Bot) -> None:
    user_id = int(getattr(message.from_user, "id"))
    state = _owner_admin_pending_inputs.get(user_id)
    if not state:
        return

    text = str(getattr(message, "text", "") or "").strip()
    if text.lower() in {"/cancel", "cancel", "отмена"}:
        _owner_admin_pending_inputs.pop(user_id, None)
        access_text, keyboard = await _owner_admin_access_panel()
        await _answer_message(
            message,
            "<b>Ввод отменён.</b>\n\n" + access_text,
            reply_markup=keyboard,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    try:
        notice, panel = await _apply_owner_admin_input(str(state.get("kind") or ""), text)
    except ValueError as exc:
        await _answer_message(
            message,
            f"<b>Не сохранил.</b>\n<code>{_html(exc)}</code>\n\n"
            f"{_input_prompt(str(state.get('kind') or ''))}",
            reply_markup=_owner_admin_cancel_keyboard(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return
    except Exception as exc:
        logger.exception("bot.owner_admin input failed kind=%s", state.get("kind"))
        _owner_admin_pending_inputs.pop(user_id, None)
        await _answer_message(
            message,
            f"<b>Ошибка.</b>\n<code>{_html(type(exc).__name__)}</code>",
            reply_markup=_owner_admin_home_keyboard(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    _owner_admin_pending_inputs.pop(user_id, None)
    if panel == "access":
        body, keyboard = await _owner_admin_access_panel()
    else:
        body, keyboard = await _owner_admin_home_panel(bot)
    await _answer_message(
        message,
        notice + "\n\n" + body,
        reply_markup=keyboard,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


def _extract_message_text(message: Message) -> str | None:
    return message.text or message.caption or None


def _extract_display_name(message: Message) -> str | None:
    user = message.from_user
    if not user:
        return None
    parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(part for part in parts if part).strip()
    return name or None


@router.message(F.photo | F.sticker | F.animation)
async def ingest_media_message(message: Message) -> None:
    settings = get_settings()
    if _is_private_chat(message) or not _source_sync_enabled(settings):
        return
    canonical_chat_id = resolve_canonical_chat_id(message.chat.id)
    try:
        await _persist_lightweight_media_message(message, canonical_chat_id=canonical_chat_id)
    except Exception:
        logger.exception(
            "bot.media persist failed chat_id=%s message_id=%s",
            getattr(message.chat, "id", None),
            getattr(message, "message_id", None),
        )


@router.message_reaction()
async def handle_message_reaction(update: MessageReactionUpdated) -> None:
    settings = get_settings()
    if not (_source_sync_enabled(settings) and getattr(settings, "reaction_context_enabled", False)):
        return
    canonical_chat_id = resolve_canonical_chat_id(update.chat.id)
    old_labels = [_normalize_reaction_label(item) for item in getattr(update, "old_reaction", [])]
    new_labels = [_normalize_reaction_label(item) for item in getattr(update, "new_reaction", [])]
    actor_kwargs = _reaction_actor_kwargs(update)

    def _persist() -> None:
        store = _lightweight_store()
        existing = store.get_reaction_snapshot(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=update.message_id,
        )
        if actor_kwargs["actor_type"]:
            store.apply_reaction_actor_delta(
                canonical_chat_id=canonical_chat_id,
                source_telegram_message_id=update.message_id,
                old_labels=old_labels,
                new_labels=new_labels,
                changed_at=update.date,
                reply_to_message_id=existing.reply_to_message_id if existing else None,
                thread_id=existing.thread_id if existing else None,
                **actor_kwargs,
            )
            return
        store.apply_reaction_delta(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=update.message_id,
            old_labels=old_labels,
            new_labels=new_labels,
            changed_at=update.date,
            reply_to_message_id=existing.reply_to_message_id if existing else None,
            thread_id=existing.thread_id if existing else None,
        )

    await to_thread(_persist)


@router.message_reaction_count()
async def handle_message_reaction_count(update: MessageReactionCountUpdated) -> None:
    settings = get_settings()
    if not (_source_sync_enabled(settings) and getattr(settings, "reaction_context_enabled", False)):
        return
    canonical_chat_id = resolve_canonical_chat_id(update.chat.id)
    snapshot = _reaction_snapshot_from_counts(getattr(update, "reactions", []))

    def _persist() -> None:
        store = _lightweight_store()
        existing = store.get_reaction_snapshot(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=update.message_id,
        )
        store.upsert_reaction_snapshot(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=update.message_id,
            reply_to_message_id=existing.reply_to_message_id if existing else None,
            thread_id=existing.thread_id if existing else None,
            snapshot=snapshot,
            changed_at=update.date,
            snapshot_origin="count",
        )

    await to_thread(_persist)


@router.message(F.text | F.caption)
async def ingest_text_message(message: Message, bot: Bot) -> None:
    text = _extract_message_text(message)
    if not text or text.startswith("/"):
        return

    settings = get_settings()
    is_dm = _is_private_chat(message)

    if is_dm:
        if not is_notebooklm_enabled(settings):
            await _answer_message(message, "Сейчас ответы временно недоступны. Попробуй позже.")
            return

        notebooklm_chat_id = _resolve_notebooklm_chat_id_for_query(message)
        if notebooklm_chat_id is None:
            await _answer_message(message, "Сначала выбери чат для поиска: /chats")
            return

        question_key = f"dm:{getattr(message, 'message_id', '')}"
        turn_id = await _record_conversation_question(
            message,
            source="dm",
            chat_id=notebooklm_chat_id,
            question_key=question_key,
            question_text=text,
        )
        if not await _consume_question_access(
            message,
            chat_id=notebooklm_chat_id,
            question_key=question_key,
            conversation_turn_id=turn_id,
        ):
            return

        from app.services.notebooklm_service import NotebookLMService

        logger.info(
            "bot.dm_question chat_id=%s notebooklm_chat_id=%s user_id=%s question=%r",
            getattr(message.chat, "id", None),
            notebooklm_chat_id,
            getattr(getattr(message, "from_user", None), "id", None),
            text[:200],
        )
        try:
            async with _typing_keepalive(message):
                result = await NotebookLMService().ask(chat_id=notebooklm_chat_id, question=text)
        except Exception:
            logger.exception(
                "bot.dm_question notebooklm_failed chat_id=%s notebooklm_chat_id=%s",
                getattr(message.chat, "id", None),
                notebooklm_chat_id,
            )
            error_text = "Не получилось подготовить ответ. Попробуй ещё раз чуть позже."
            await _update_conversation_turn(
                turn_id,
                status="failed",
                error_text=error_text,
                reason="exception",
            )
            await _answer_message(message, error_text)
            return
        rendered = _format_nlm_result(result, include_footer=True)
        await _update_conversation_turn(
            turn_id,
            status="failed" if getattr(result, "error", None) else "answered",
            answer_text=None if getattr(result, "error", None) else rendered,
            error_text=rendered if getattr(result, "error", None) else None,
            reason=getattr(result, "reason", None) or ("answer_error" if getattr(result, "error", None) else None),
            latency_ms=getattr(result, "latency_ms", None),
            notebook_id=getattr(result, "notebook_id", None),
        )
        await _answer_message(
            message,
            rendered,
            parse_mode="HTML",
        )
        return

    canonical_chat_id = resolve_canonical_chat_id(message.chat.id)
    logger.info(
        "bot.message received chat_id=%s canonical_chat_id=%s message_id=%s user_id=%s text=%r",
        getattr(message.chat, "id", None),
        canonical_chat_id,
        getattr(message, "message_id", None),
        getattr(getattr(message, "from_user", None), "id", None),
        text[:200],
    )
    try:
        if _source_sync_enabled(settings):
            await _persist_lightweight_history_message(
                message,
                canonical_chat_id=canonical_chat_id,
                text=text,
            )
    except Exception:
        logger.exception(
            "bot.message persist failed chat_id=%s message_id=%s",
            getattr(message.chat, "id", None),
            getattr(message, "message_id", None),
        )
        return

    question, trigger_kind = await _extract_triggered_question(message, bot)
    if not question:
        return

    logger.info(
        "bot.message promoted_to_question chat_id=%s canonical_chat_id=%s message_id=%s trigger=%s question=%r",
        getattr(message.chat, "id", None),
        canonical_chat_id,
        getattr(message, "message_id", None),
        trigger_kind,
        question[:200],
    )

    if not is_notebooklm_enabled(settings):
        await _answer_message(message, "Сейчас ответы временно недоступны. Попробуй позже.")
        return

    notebooklm_chat_id = _resolve_notebooklm_chat_id_for_query(message)
    if notebooklm_chat_id is None:
        await _answer_message(message, "Сначала выбери чат для поиска: /chats")
        return

    question_key = f"{trigger_kind}:{getattr(message, 'message_id', '')}"
    turn_id = await _record_conversation_question(
        message,
        source=f"group:{trigger_kind}",
        chat_id=notebooklm_chat_id,
        question_key=question_key,
        question_text=question,
    )
    if not await _consume_question_access(
        message,
        chat_id=notebooklm_chat_id,
        question_key=question_key,
        conversation_turn_id=turn_id,
    ):
        return

    from app.services.notebooklm_service import NotebookLMService

    try:
        async with _typing_keepalive(message):
            result = await NotebookLMService().ask(chat_id=notebooklm_chat_id, question=question)
    except Exception:
        logger.exception(
            "bot.message triggered_notebooklm_failed chat_id=%s canonical_chat_id=%s message_id=%s trigger=%s",
            getattr(message.chat, "id", None),
            notebooklm_chat_id,
            getattr(message, "message_id", None),
            trigger_kind,
        )
        error_text = "Увидел вопрос, но не получилось подготовить ответ. Попробуй ещё раз чуть позже."
        await _update_conversation_turn(
            turn_id,
            status="failed",
            error_text=error_text,
            reason="exception",
        )
        await _answer_message(message, error_text)
        return

    logger.info(
        "bot.message answered_via_notebooklm chat_id=%s canonical_chat_id=%s message_id=%s trigger=%s notebook_id=%s latency_ms=%s",
        getattr(message.chat, "id", None),
        notebooklm_chat_id,
        getattr(message, "message_id", None),
        trigger_kind,
        getattr(result, "notebook_id", None),
        getattr(result, "latency_ms", None),
    )
    rendered = _format_triggered_nlm_result(result, include_footer=True)
    await _update_conversation_turn(
        turn_id,
        status="failed" if getattr(result, "error", None) else "answered",
        answer_text=None if getattr(result, "error", None) else rendered,
        error_text=rendered if getattr(result, "error", None) else None,
        reason=getattr(result, "reason", None) or ("answer_error" if getattr(result, "error", None) else None),
        latency_ms=getattr(result, "latency_ms", None),
        notebook_id=getattr(result, "notebook_id", None),
    )
    await _answer_message(
        message,
        rendered,
        parse_mode="HTML",
    )


@router.edited_message(F.text | F.caption)
async def handle_edited_message(message: Message, bot: Bot) -> None:
    text = _extract_message_text(message)
    if not text:
        return
    settings = get_settings()
    canonical_chat_id = resolve_canonical_chat_id(message.chat.id)

    logger.info(
        "bot.edited_message chat_id=%s message_id=%s text=%r",
        getattr(message.chat, "id", None),
        getattr(message, "message_id", None),
        text[:200],
    )
    try:
        if _source_sync_enabled(settings):
            await _persist_lightweight_history_message(
                message,
                canonical_chat_id=canonical_chat_id,
                text=text,
                edited=True,
            )
    except Exception:
        logger.exception(
            "bot.edited_message persist failed chat_id=%s message_id=%s",
            getattr(message.chat, "id", None),
            getattr(message, "message_id", None),
        )


@router.callback_query(F.data == "ask")
async def ask_callback(callback: CallbackQuery) -> None:
    await _answer_message(callback.message, "Используй формат: /ask <вопрос>")
    await callback.answer()


@router.callback_query(F.data == "status")
async def status_callback(callback: CallbackQuery) -> None:
    message = getattr(callback, "message", None)
    if message is None:
        await callback.answer("Status is unavailable for this message.", show_alert=True)
        return

    user_id = getattr(getattr(callback, "from_user", None), "id", None)
    if user_id is None:
        await _answer_message(message, "Баланс доступен только для Telegram-пользователя.")
        await callback.answer()
        return

    chat_id = _resolve_notebooklm_chat_id_for_user_context(message, int(user_id))
    if chat_id is None:
        await _answer_message(message, "Сначала выбери чат через /chats, затем открой баланс снова.")
        await callback.answer()
        return

    balance = await to_thread(
        _access_store().balance,
        telegram_user_id=int(user_id),
        chat_id=chat_id,
    )
    await _answer_message(message, _format_notebooklm_status(balance, chat_id=chat_id))
    await callback.answer()


@router.error()
async def top_level_handler_error(event: ErrorEvent) -> bool:
    trace_id = _short_trace_id()
    inc_handler_exception_total()
    logger.exception("bot.handler_exception trace_id=%s", trace_id, exc_info=event.exception)

    message = getattr(event.update, "message", None)
    if message is None:
        callback_query = getattr(event.update, "callback_query", None)
        message = getattr(callback_query, "message", None)

    if message is not None:
        try:
            await _answer_message(message, bot_handler_error_message(trace_id))
        except Exception:
            logger.exception("bot.handler_exception reply_failed trace_id=%s", trace_id)
    return True
