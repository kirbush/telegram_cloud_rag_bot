import asyncio
import re
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from app.bot.handlers import (
    _answer_message,
    _owner_admin_pending_inputs,
    _persist_lightweight_media_message,
    _prepare_media_analysis_payload,
    _select_media_payload,
    _short_trace_id,
    _typing_keepalive,
    ask,
    auth_nlm,
    balance_cmd,
    buy_cmd,
    handle_edited_message,
    history_cmd,
    ingest_text_message,
    last_questions_cmd,
    monitoring_cmd,
    nlm_cmd,
    limits_cmd,
    owner_admin_callback,
    owner_admin_cmd,
    owner_admin_input_message,
    paysupport_cmd,
    pre_checkout_query,
    start,
    status_callback,
    successful_payment_message,
    terms_cmd,
    update_cmd,
    users_cmd,
)
from app.services.telegram_stars import TelegramStarsAPIError


class AnswerMessageTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.build_answer_kwargs", return_value={})
    async def test_omits_direct_messages_topic_id_for_dm_replies_by_default(
        self, _build_answer_kwargs
    ) -> None:
        answer = AsyncMock()
        message = SimpleNamespace(
            direct_messages_topic=SimpleNamespace(topic_id=77),
            answer=answer,
        )

        await _answer_message(message, "hello")

        answer.assert_awaited_once_with("hello")

    @patch(
        "app.bot.handlers.build_answer_kwargs",
        return_value={"direct_messages_topic_id": 77},
    )
    async def test_can_preserve_direct_messages_topic_id_when_opted_in(
        self, _build_answer_kwargs
    ) -> None:
        answer = AsyncMock()
        message = SimpleNamespace(
            direct_messages_topic=SimpleNamespace(topic_id=77),
            answer=answer,
        )

        await _answer_message(message, "hello")

        answer.assert_awaited_once_with("hello", direct_messages_topic_id=77)

    async def test_omits_direct_messages_topic_id_when_not_present(self) -> None:
        answer = AsyncMock()
        message = SimpleNamespace(direct_messages_topic=None, answer=answer)

        await _answer_message(message, "hello", parse_mode="Markdown")

        answer.assert_awaited_once_with("hello", parse_mode="Markdown")


class TraceIdTests(unittest.TestCase):
    def test_trace_id_is_ulid_shaped(self) -> None:
        trace_id = _short_trace_id()

        self.assertEqual(len(trace_id), 26)
        self.assertRegex(trace_id, re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$"))


class TypingKeepaliveTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeats_typing_until_context_exits(self) -> None:
        bot = SimpleNamespace(send_chat_action=AsyncMock())
        message = SimpleNamespace(
            chat=SimpleNamespace(id=42),
            bot=bot,
        )

        async with _typing_keepalive(message, interval_s=0.01):
            await asyncio.sleep(0.035)

        self.assertGreaterEqual(bot.send_chat_action.await_count, 2)
        self.assertTrue(
            all(call.args == (42, "typing") for call in bot.send_chat_action.await_args_list)
        )


def _group_message(
    *,
    text: str,
    reply_to_message=None,
    chat_id: int = -10012345,
    thread_id: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        text=text,
        caption=None,
        chat=SimpleNamespace(type="supergroup", id=chat_id, title="Live Chat", username=None),
        message_id=501,
        from_user=SimpleNamespace(id=42, username="kirill", first_name="Kirill", last_name=None),
        reply_to_message=reply_to_message,
        message_thread_id=thread_id,
        date=datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
        answer=AsyncMock(),
        answer_chat_action=AsyncMock(),
    )


def _private_message(*, text: str) -> SimpleNamespace:
    return SimpleNamespace(
        text=text,
        caption=None,
        chat=SimpleNamespace(type="private", id=42, title=None, username="kirill_bush"),
        message_id=78,
        from_user=SimpleNamespace(
            id=123456789,
            username="kirill_bush",
            first_name="Kirill",
            last_name="Bushmakin",
        ),
        reply_to_message=None,
        message_thread_id=None,
        date=datetime(2026, 4, 24, 12, 53, tzinfo=timezone.utc),
        answer=AsyncMock(),
    )


def _bot() -> SimpleNamespace:
    return SimpleNamespace(
        id=777,
        me=AsyncMock(return_value=SimpleNamespace(id=777, username="testbot")),
        send_chat_action=AsyncMock(),
    )


def _keyboard_texts(markup) -> list[str]:
    return [button.text for row in markup.inline_keyboard for button in row]


def _keyboard_callbacks(markup) -> list[str]:
    return [button.callback_data for row in markup.inline_keyboard for button in row]


class PublicCopyAndKeyboardTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.get_settings")
    async def test_non_admin_start_uses_public_copy_and_buttons(self, get_settings_mock) -> None:
        get_settings_mock.return_value = SimpleNamespace(
            bot_admin_user_ids="123456789",
            notebooklm_source_sync_enabled=False,
        )
        message = _private_message(text="/start")
        message.from_user.id = 42

        await start(message)

        args, kwargs = message.answer.await_args
        self.assertNotIn("NotebookLM", args[0])
        keyboard = kwargs["reply_markup"]
        self.assertNotIn("Админка", _keyboard_texts(keyboard))
        self.assertEqual(_keyboard_callbacks(keyboard), ["ask", "status"])

    @patch("app.bot.handlers.get_settings")
    async def test_admin_start_gets_admin_button(self, get_settings_mock) -> None:
        get_settings_mock.return_value = SimpleNamespace(
            bot_admin_user_ids="123456789",
            notebooklm_source_sync_enabled=False,
        )
        message = _private_message(text="/start")

        await start(message)

        keyboard = message.answer.await_args.kwargs["reply_markup"]
        self.assertIn("Админка", _keyboard_texts(keyboard))
        self.assertEqual(_keyboard_callbacks(keyboard)[0], "adm:home")


def _owner_admin_settings(**overrides) -> SimpleNamespace:
    values = {
        "bot_admin_user_ids": "123456789",
        "bot_token": "777:TEST_TOKEN",
        "bot_instance_name": "secondary",
        "notebooklm_enabled": True,
        "notebooklm_source_sync_enabled": False,
        "notebooklm_background_sync_enabled": False,
        "notebooklm_remote_auth_base_url": "",
        "notebooklm_remote_auth_docker_socket": "/var/run/docker.sock",
        "notebooklm_remote_auth_selenium_image": "selenium/standalone-chromium:4.34.0",
        "notebooklm_proxy_enabled": False,
        "notebooklm_proxy_url": "",
        "telegram_proxy_enabled": False,
        "telegram_proxy_url": "",
        "uptime_kuma_public_url": "https://status.example",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _owner_admin_callback(data: str, *, user_id: int = 123456789, chat_type: str = "private") -> SimpleNamespace:
    return SimpleNamespace(
        data=data,
        from_user=SimpleNamespace(id=user_id),
        message=SimpleNamespace(
            chat=SimpleNamespace(type=chat_type, id=42),
            edit_text=AsyncMock(),
            answer=AsyncMock(),
        ),
        answer=AsyncMock(),
    )


class MediaPayloadSelectionTests(unittest.TestCase):
    def test_animated_sticker_uses_source_for_storyboard_and_keeps_thumbnail_fallback(self) -> None:
        message = SimpleNamespace(
            photo=[],
            animation=None,
            sticker=SimpleNamespace(
                file_id="sticker-source-file",
                file_unique_id="sticker-source-unique",
                is_animated=True,
                is_video=False,
                thumbnail=SimpleNamespace(
                    file_id="sticker-thumb-file",
                    file_unique_id="sticker-thumb-unique",
                ),
            ),
        )

        payload = _select_media_payload(message)

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.media_kind, "sticker")
        self.assertEqual(payload.source_file_id, "sticker-source-file")
        self.assertEqual(payload.source_unique_id, "sticker-source-unique")
        self.assertEqual(payload.analysis_file_id, "sticker-source-file")
        self.assertEqual(payload.analysis_unique_id, "sticker-source-unique")
        self.assertEqual(payload.thumbnail_file_id, "sticker-thumb-file")
        self.assertEqual(payload.mime_type, "application/x-tgsticker")
        self.assertTrue(payload.storyboard_enabled)
        self.assertEqual(payload.fallback_file_id, "sticker-thumb-file")
        self.assertEqual(payload.fallback_mime_type, "image/jpeg")

    def test_video_sticker_uses_source_webm_for_storyboard(self) -> None:
        message = SimpleNamespace(
            photo=[],
            animation=None,
            sticker=SimpleNamespace(
                file_id="video-sticker-source-file",
                file_unique_id="video-sticker-source-unique",
                is_animated=False,
                is_video=True,
                thumbnail=SimpleNamespace(
                    file_id="video-sticker-thumb-file",
                    file_unique_id="video-sticker-thumb-unique",
                ),
            ),
        )

        payload = _select_media_payload(message)

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.analysis_file_id, "video-sticker-source-file")
        self.assertEqual(payload.analysis_unique_id, "video-sticker-source-unique")
        self.assertEqual(payload.thumbnail_file_id, "video-sticker-thumb-file")
        self.assertEqual(payload.mime_type, "video/webm")
        self.assertTrue(payload.storyboard_enabled)
        self.assertEqual(payload.fallback_file_id, "video-sticker-thumb-file")

    def test_animation_uses_source_for_storyboard_and_source_for_history(self) -> None:
        message = SimpleNamespace(
            photo=[],
            sticker=None,
            animation=SimpleNamespace(
                file_id="animation-source-file",
                file_unique_id="animation-source-unique",
                mime_type="video/mp4",
                thumbnail=SimpleNamespace(
                    file_id="animation-thumb-file",
                    file_unique_id="animation-thumb-unique",
                ),
            ),
        )

        payload = _select_media_payload(message)

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.media_kind, "animation")
        self.assertEqual(payload.source_file_id, "animation-source-file")
        self.assertEqual(payload.source_unique_id, "animation-source-unique")
        self.assertEqual(payload.analysis_file_id, "animation-source-file")
        self.assertEqual(payload.analysis_unique_id, "animation-source-unique")
        self.assertEqual(payload.thumbnail_file_id, "animation-thumb-file")
        self.assertEqual(payload.mime_type, "video/mp4")
        self.assertTrue(payload.storyboard_enabled)
        self.assertEqual(payload.fallback_file_id, "animation-thumb-file")
        self.assertEqual(payload.fallback_mime_type, "image/jpeg")

    def test_animation_without_thumbnail_uses_animation_payload_mime(self) -> None:
        message = SimpleNamespace(
            photo=[],
            sticker=None,
            animation=SimpleNamespace(
                file_id="animation-source-file",
                file_unique_id="animation-source-unique",
                mime_type="image/gif",
                thumbnail=None,
            ),
        )

        payload = _select_media_payload(message)

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload.analysis_file_id, "animation-source-file")
        self.assertEqual(payload.analysis_unique_id, "animation-source-unique")
        self.assertIsNone(payload.thumbnail_file_id)
        self.assertEqual(payload.mime_type, "image/gif")
        self.assertTrue(payload.storyboard_enabled)
        self.assertIsNone(payload.fallback_file_id)


class MediaAnalysisPayloadTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.build_animation_storyboard", return_value=b"storyboard-png")
    @patch("app.bot.handlers._download_telegram_file_bytes", new_callable=AsyncMock)
    async def test_storyboard_success_returns_single_png_payload(
        self,
        download_file,
        build_storyboard,
    ) -> None:
        download_file.return_value = b"video-bytes"
        bot = _bot()

        payload = await _prepare_media_analysis_payload(
            bot,
            file_id="source-file",
            mime_type="video/webm",
            storyboard_enabled=True,
            fallback_file_id="thumb-file",
            fallback_mime_type="image/jpeg",
        )

        self.assertEqual(payload.media_bytes, b"storyboard-png")
        self.assertEqual(payload.mime_type, "image/png")
        self.assertTrue(payload.is_storyboard)
        download_file.assert_awaited_once_with(bot, file_id="source-file")
        build_storyboard.assert_called_once_with(b"video-bytes")

    @patch("app.bot.handlers.build_animation_storyboard", return_value=None)
    @patch("app.bot.handlers._download_telegram_file_bytes", new_callable=AsyncMock)
    async def test_storyboard_failure_falls_back_to_thumbnail(
        self,
        download_file,
        _build_storyboard,
    ) -> None:
        download_file.side_effect = [b"video-bytes", b"thumb-bytes"]
        bot = _bot()

        payload = await _prepare_media_analysis_payload(
            bot,
            file_id="source-file",
            mime_type="video/webm",
            storyboard_enabled=True,
            fallback_file_id="thumb-file",
            fallback_mime_type="image/jpeg",
        )

        self.assertEqual(payload.media_bytes, b"thumb-bytes")
        self.assertEqual(payload.mime_type, "image/jpeg")
        self.assertFalse(payload.is_storyboard)
        self.assertEqual(download_file.await_args_list[0].kwargs["file_id"], "source-file")
        self.assertEqual(download_file.await_args_list[1].kwargs["file_id"], "thumb-file")


class _FakeTask:
    def add_done_callback(self, _callback) -> None:
        return None


class _FakeAccessStore:
    def __init__(self, *, consume_allowed: bool = True, access_disabled: bool = False) -> None:
        self.consume_allowed = consume_allowed
        self.access_disabled = access_disabled
        self.consumed = []
        self.valid_pre_checkout = True
        self.recorded_payments = []
        self.global_config = SimpleNamespace(
            enabled=not access_disabled,
            free_questions_per_24h=20,
            stars_price=25,
            credits_per_purchase=10,
        )
        self.chat_overrides = {}
        self.grants = []

    def consume_question(self, **kwargs):
        if self.access_disabled:
            return SimpleNamespace(source="disabled")
        if not self.consume_allowed:
            return None
        self.consumed.append(kwargs)
        return SimpleNamespace(source="free")

    def balance(self, **kwargs):
        return {
            "telegram_user_id": kwargs["telegram_user_id"],
            "chat_id": kwargs["chat_id"],
            "enabled": not self.access_disabled,
            "free_limit": 1,
            "used_in_window": 1 if not self.consume_allowed else 0,
            "free_remaining": 0 if not self.consume_allowed else 1,
            "manual_credits": 0,
            "paid_credits": 2,
            "total_remaining": 2 if self.consume_allowed else 0,
            "next_reset_at": "2026-04-28T12:00:00Z",
        }

    def create_stars_order(self, **kwargs):
        return SimpleNamespace(
            payload="access:order-1",
            credits=10,
            stars_amount=25,
            currency="XTR",
        )

    def validate_stars_order(self, **kwargs):
        return self.valid_pre_checkout

    def record_successful_payment(self, **kwargs):
        self.recorded_payments.append(kwargs)
        return True, 10

    def get_global_config(self):
        return self.global_config

    def set_global_config(self, **kwargs):
        for key, value in kwargs.items():
            if value is not None:
                setattr(self.global_config, key, value)
        return self.global_config

    def set_chat_override(self, **kwargs):
        self.chat_overrides[kwargs["chat_id"]] = kwargs
        return self.global_config

    def clear_chat_override(self, **kwargs):
        self.chat_overrides.pop(kwargs["chat_id"], None)

    def grant_manual_credits(self, **kwargs):
        self.grants.append(kwargs)
        return int(kwargs["delta"])

    def status(self):
        return {
            "state_path": ".state/bot/test-access.sqlite3",
            "currency": "XTR",
            "global": {
                "enabled": self.global_config.enabled,
                "free_questions_per_24h": self.global_config.free_questions_per_24h,
                "stars_price": self.global_config.stars_price,
                "credits_per_purchase": self.global_config.credits_per_purchase,
            },
            "chat_overrides": [
                {
                    "chat_id": chat_id,
                    "enabled": override.get("enabled"),
                    "free_questions_per_24h": override.get("free_questions_per_24h"),
                    "stars_price": override.get("stars_price"),
                    "credits_per_purchase": override.get("credits_per_purchase"),
                }
                for chat_id, override in self.chat_overrides.items()
            ],
            "totals": {
                "usage_count": 2,
                "order_count": 1,
                "payment_count": 1,
            },
        }

    def stars_ledger_summary(self):
        return {
            "state_path": ".state/bot/test-access.sqlite3",
            "currency": "XTR",
            "local_order_count": 1,
            "local_payment_count": 1,
            "usage_count": 2,
            "total_local_paid_stars_amount": 25,
            "paid_credits": {"granted": 10, "consumed": 3, "remaining": 7},
            "manual_credits": {"granted": 5, "consumed": 1, "remaining": 4},
        }

    def star_payments(self):
        return [
            {
                "id": 1,
                "telegram_payment_charge_id": "txn-1",
                "stars_amount": 25,
                "currency": "XTR",
            }
        ]


class _FakeConversationStore:
    def __init__(self) -> None:
        self.recorded = []
        self.updated = []
        self.users = [
            SimpleNamespace(
                telegram_user_id=42,
                username="kirill",
                display_name="Kirill",
                last_seen_at=datetime(2026, 4, 28, 9, 0, tzinfo=timezone.utc),
                turn_count=2,
                last_status="answered",
                last_question="Как включить климат?",
            )
        ]
        self.turns = [
            SimpleNamespace(
                turn_id=1,
                status="answered",
                source="dm",
                telegram_user_id=42,
                username="kirill",
                display_name="Kirill",
                chat_id=42,
                chat_type="private",
                chat_title=None,
                message_id=78,
                thread_id=None,
                question_key="dm:78",
                question_text="Как включить климат?",
                answer_text="Поставьте приложение.",
                error_text=None,
                reason=None,
                latency_ms=42,
                notebook_id="nb-1",
                created_at=datetime(2026, 4, 28, 9, 0, tzinfo=timezone.utc),
                updated_at=datetime(2026, 4, 28, 9, 1, tzinfo=timezone.utc),
            )
        ]

    def record_question(self, **kwargs):
        self.recorded.append(kwargs)
        return SimpleNamespace(turn_id=len(self.recorded))

    def update_turn(self, **kwargs):
        self.updated.append(kwargs)
        return SimpleNamespace(turn_id=kwargs["turn_id"], status=kwargs["status"])

    def list_recent_users(self, *, limit=10):
        return self.users[:limit]

    def list_recent_turns(self, *, limit=10):
        return self.turns[:limit]

    def list_user_history(self, *, telegram_user_id, limit=10):
        return [turn for turn in self.turns if turn.telegram_user_id == telegram_user_id][:limit]

    def status(self):
        return {
            "state_path": ".state/bot/test-conversations.sqlite3",
            "turn_count": len(self.turns),
            "answered_count": 1,
            "denied_count": 0,
            "failed_count": 0,
            "user_count": len(self.users),
        }


class _FakeMediaStore:
    def __init__(self) -> None:
        self.timeline_events = []
        self.media_jobs = []

    def append_timeline_event(self, **kwargs):
        self.timeline_events.append(kwargs)

    def create_media_job(self, **kwargs):
        self.media_jobs.append(kwargs)
        return SimpleNamespace(job_pk=123)


class MediaPersistenceTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.get_settings")
    @patch("app.bot.handlers._lightweight_store")
    @patch("app.bot.handlers._process_media_context_job", new_callable=AsyncMock)
    @patch("app.bot.handlers.asyncio.create_task")
    async def test_persist_keeps_source_ids_but_schedules_storyboard_analysis(
        self,
        create_task,
        process_media_context_job,
        lightweight_store,
        get_settings,
    ) -> None:
        store = _FakeMediaStore()
        lightweight_store.return_value = store
        get_settings.return_value = SimpleNamespace(media_context_enabled=True)

        def _capture_task(coro, *, name=None):
            self.assertEqual(name, "media-context-555000111-501")
            coro.close()
            return _FakeTask()

        create_task.side_effect = _capture_task
        message = _group_message(text="")
        message.photo = []
        message.sticker = SimpleNamespace(
            file_id="sticker-source-file",
            file_unique_id="sticker-source-unique",
            is_animated=True,
            is_video=False,
            thumbnail=SimpleNamespace(
                file_id="sticker-thumb-file",
                file_unique_id="sticker-thumb-unique",
            ),
        )
        message.animation = None
        message.bot = _bot()

        await _persist_lightweight_media_message(message, canonical_chat_id=555000111)

        self.assertEqual(store.timeline_events[0]["media_kind"], "sticker")
        self.assertEqual(store.timeline_events[0]["file_id"], "sticker-source-file")
        self.assertEqual(store.timeline_events[0]["file_unique_id"], "sticker-source-unique")
        self.assertEqual(store.timeline_events[0]["thumbnail_file_id"], "sticker-thumb-file")
        self.assertEqual(store.media_jobs[0]["file_id"], "sticker-source-file")
        self.assertEqual(store.media_jobs[0]["file_unique_id"], "sticker-source-unique")
        self.assertEqual(store.media_jobs[0]["thumbnail_file_id"], "sticker-thumb-file")
        self.assertEqual(process_media_context_job.call_args.kwargs["file_id"], "sticker-source-file")
        self.assertEqual(
            process_media_context_job.call_args.kwargs["mime_type"],
            "application/x-tgsticker",
        )
        self.assertEqual(process_media_context_job.call_args.kwargs["thumbnail_file_id"], "sticker-thumb-file")
        self.assertTrue(process_media_context_job.call_args.kwargs["storyboard_enabled"])
        self.assertEqual(process_media_context_job.call_args.kwargs["fallback_file_id"], "sticker-thumb-file")
        self.assertEqual(process_media_context_job.call_args.kwargs["fallback_mime_type"], "image/jpeg")


class TriggerRoutingTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=False)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_group_mention_bypasses_access_when_disabled(
        self,
        access_store,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        fake_access = _FakeAccessStore(access_disabled=True)
        access_store.return_value = fake_access
        mock_get_settings.return_value = SimpleNamespace(
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=False,
        )
        notebooklm_ask.return_value = SimpleNamespace(
            answer="Notebook answer",
            error=None,
            latency_ms=42,
            notebook_id="nb-1",
            sources=[],
        )
        bot = _bot()
        message = _group_message(text="@testbot who joined?")
        message.bot = bot

        await ingest_text_message(message, bot)

        notebooklm_ask.assert_awaited_once_with(chat_id=555000111, question="who joined?")
        self.assertEqual(fake_access.consumed, [])

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=False)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_group_mention_denied_by_access_does_not_call_notebooklm(
        self,
        access_store,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        access_store.return_value = _FakeAccessStore(consume_allowed=False)
        mock_get_settings.return_value = SimpleNamespace(
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=False,
        )
        bot = _bot()
        message = _group_message(text="@testbot who joined?")
        message.bot = bot

        await ingest_text_message(message, bot)

        notebooklm_ask.assert_not_awaited()
        message.answer.assert_awaited_once()
        args, _ = message.answer.await_args
        self.assertIn("Пока лимит вопросов закончился", args[0])
        self.assertNotIn("NotebookLM", args[0])

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=False)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_group_mention_admin_bypasses_exhausted_access(
        self,
        access_store,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        fake_access = _FakeAccessStore(consume_allowed=False)
        access_store.return_value = fake_access
        mock_get_settings.return_value = SimpleNamespace(
            bot_admin_user_ids="123456789",
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=False,
        )
        notebooklm_ask.return_value = SimpleNamespace(
            answer="Notebook answer",
            error=None,
            latency_ms=42,
            notebook_id="nb-1",
            sources=[],
        )
        bot = _bot()
        message = _group_message(text="@testbot who joined?")
        message.from_user.id = 123456789
        message.bot = bot

        await ingest_text_message(message, bot)

        notebooklm_ask.assert_awaited_once_with(chat_id=555000111, question="who joined?")
        self.assertEqual(fake_access.consumed, [])

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=False)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_group_mention_routes_to_notebooklm(
        self,
        access_store,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        fake_access = _FakeAccessStore()
        access_store.return_value = fake_access
        mock_get_settings.return_value = SimpleNamespace(
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=False,
        )
        notebooklm_ask.return_value = SimpleNamespace(
            answer="Notebook answer",
            error=None,
            latency_ms=42,
            notebook_id="nb-1",
            sources=[],
        )
        bot = _bot()
        message = _group_message(text="@testbot who joined the discussion?")
        message.bot = bot

        await ingest_text_message(message, bot)

        notebooklm_ask.assert_awaited_once_with(
            chat_id=555000111, question="who joined the discussion?"
        )
        self.assertEqual(fake_access.consumed[0]["telegram_user_id"], 42)
        self.assertEqual(fake_access.consumed[0]["chat_id"], 555000111)
        bot.send_chat_action.assert_awaited_once_with(-10012345, "typing")
        message.answer.assert_awaited_once()
        args, kwargs = message.answer.await_args
        self.assertIn("Notebook answer", args[0])
        self.assertEqual(kwargs.get("parse_mode"), "HTML")

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=False)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers._conversation_store")
    @patch("app.bot.handlers._access_store")
    async def test_group_mention_records_answered_conversation_turn(
        self,
        access_store,
        conversation_store,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        access_store.return_value = _FakeAccessStore()
        fake_conversations = _FakeConversationStore()
        conversation_store.return_value = fake_conversations
        mock_get_settings.return_value = SimpleNamespace(
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=False,
        )
        notebooklm_ask.return_value = SimpleNamespace(
            answer="Notebook answer",
            error=None,
            latency_ms=42,
            notebook_id="nb-1",
            sources=[],
        )
        bot = _bot()
        message = _group_message(text="@testbot who joined?")
        message.bot = bot

        await ingest_text_message(message, bot)

        self.assertEqual(fake_conversations.recorded[0]["source"], "group:mention")
        self.assertEqual(fake_conversations.recorded[0]["question_text"], "who joined?")
        self.assertEqual(fake_conversations.updated[0]["status"], "answered")
        self.assertIn("Notebook answer", fake_conversations.updated[0]["answer_text"])
        self.assertEqual(fake_conversations.updated[0]["notebook_id"], "nb-1")

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=False)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers._conversation_store")
    @patch("app.bot.handlers._access_store")
    async def test_group_mention_records_denied_conversation_turn(
        self,
        access_store,
        conversation_store,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        access_store.return_value = _FakeAccessStore(consume_allowed=False)
        fake_conversations = _FakeConversationStore()
        conversation_store.return_value = fake_conversations
        mock_get_settings.return_value = SimpleNamespace(
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=False,
        )
        bot = _bot()
        message = _group_message(text="@testbot who joined?")
        message.bot = bot

        await ingest_text_message(message, bot)

        notebooklm_ask.assert_not_awaited()
        self.assertEqual(fake_conversations.recorded[0]["question_text"], "who joined?")
        self.assertEqual(fake_conversations.updated[0]["status"], "denied")
        self.assertEqual(fake_conversations.updated[0]["reason"], "limit_exceeded")
        self.assertIn("лимит вопросов", fake_conversations.updated[0]["answer_text"])

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=False)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    async def test_plain_group_message_without_mention_does_not_trigger_notebooklm(
        self,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=False,
        )
        bot = _bot()
        message = _group_message(text="no mention here, just chatting")
        message.bot = bot

        await ingest_text_message(message, bot)

        notebooklm_ask.assert_not_awaited()
        bot.send_chat_action.assert_not_awaited()
        message.answer.assert_not_awaited()

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=True)
    @patch("app.bot.handlers._persist_lightweight_history_message", new_callable=AsyncMock)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_group_mention_persists_lightweight_history_when_source_sync_enabled(
        self,
        access_store,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        persist_lightweight_history_message,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        access_store.return_value = _FakeAccessStore()
        mock_get_settings.return_value = SimpleNamespace(
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=True,
        )
        notebooklm_ask.return_value = SimpleNamespace(
            answer="Notebook answer",
            error=None,
            latency_ms=42,
            notebook_id="nb-1",
            sources=[],
        )
        bot = _bot()
        message = _group_message(text="@testbot who joined?")
        message.bot = bot

        await ingest_text_message(message, bot)

        persist_lightweight_history_message.assert_awaited_once_with(
            message,
            canonical_chat_id=555000111,
            text="@testbot who joined?",
        )
        notebooklm_ask.assert_awaited_once_with(chat_id=555000111, question="who joined?")

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers._source_sync_enabled", return_value=False)
    @patch("app.bot.handlers._persist_lightweight_history_message", new_callable=AsyncMock)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_group_mention_skips_lightweight_history_when_source_sync_disabled(
        self,
        access_store,
        _resolve_canonical_chat_id,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        persist_lightweight_history_message,
        _source_sync_enabled,
        _is_enabled,
    ) -> None:
        access_store.return_value = _FakeAccessStore()
        mock_get_settings.return_value = SimpleNamespace(
            notebooklm_enabled=True,
            notebooklm_source_sync_enabled=False,
        )
        notebooklm_ask.return_value = SimpleNamespace(
            answer="Notebook answer",
            error=None,
            latency_ms=42,
            notebook_id="nb-1",
            sources=[],
        )
        bot = _bot()
        message = _group_message(text="@testbot who joined?")
        message.bot = bot

        await ingest_text_message(message, bot)

        persist_lightweight_history_message.assert_not_awaited()
        notebooklm_ask.assert_awaited_once_with(chat_id=555000111, question="who joined?")

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_private_message_routes_directly_to_notebooklm(
        self,
        access_store,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        mock_get_settings,
        _is_enabled,
    ) -> None:
        access_store.return_value = _FakeAccessStore()
        mock_get_settings.return_value = SimpleNamespace(notebooklm_enabled=True)
        notebooklm_ask.return_value = SimpleNamespace(
            answer="Notebook answer",
            error=None,
            latency_ms=42,
            notebook_id="nb-1",
            sources=[],
        )
        bot = _bot()
        message = _private_message(text="какие модели обсуждали в чате?")
        message.bot = bot

        await ingest_text_message(message, bot)

        notebooklm_ask.assert_awaited_once_with(
            chat_id=555000111, question="какие модели обсуждали в чате?"
        )
        bot.send_chat_action.assert_awaited_once_with(42, "typing")

    @patch("app.bot.handlers._source_sync_enabled", return_value=True)
    @patch("app.bot.handlers._persist_lightweight_history_message", new_callable=AsyncMock)
    @patch("app.bot.handlers.resolve_canonical_chat_id", return_value=555000111)
    @patch("app.bot.handlers.get_settings")
    async def test_edited_group_message_persists_to_lightweight_history(
        self,
        mock_get_settings,
        _resolve_canonical_chat_id,
        persist_lightweight_history_message,
        _source_sync_enabled,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(notebooklm_source_sync_enabled=True)
        message = _group_message(text="edited text")
        message.bot = _bot()

        await handle_edited_message(message, message.bot)

        persist_lightweight_history_message.assert_awaited_once_with(
            message,
            canonical_chat_id=555000111,
            text="edited text",
            edited=True,
        )


class AskCommandTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers.get_settings", return_value=SimpleNamespace())
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_ask_delegates_to_notebooklm(
        self,
        access_store,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        _mock_settings,
        _is_enabled,
    ) -> None:
        access_store.return_value = _FakeAccessStore()
        notebooklm_ask.return_value = SimpleNamespace(
            answer="Notebook answer",
            error=None,
            latency_ms=42,
            notebook_id="nb-1",
            sources=[],
        )
        bot = _bot()
        message = _private_message(text="/ask кто участвует в проекте?")
        message.bot = bot

        await ask(message)

        notebooklm_ask.assert_awaited_once_with(
            chat_id=555000111, question="кто участвует в проекте?"
        )

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers.get_settings", return_value=SimpleNamespace())
    @patch("app.services.notebooklm_service.NotebookLMService.ask", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_nlm_and_ask_share_identical_backend(
        self,
        access_store,
        _resolve_notebooklm_chat_id_for_query,
        notebooklm_ask,
        _mock_settings,
        _is_enabled,
    ) -> None:
        access_store.return_value = _FakeAccessStore()
        notebooklm_ask.return_value = SimpleNamespace(
            answer="A",
            error=None,
            latency_ms=1,
            notebook_id="nb",
            sources=[],
        )

        ask_message = _private_message(text="/ask что нового?")
        ask_message.bot = _bot()
        await ask(ask_message)

        nlm_message = _private_message(text="/nlm что нового?")
        nlm_message.bot = _bot()
        await nlm_cmd(nlm_message)

        self.assertEqual(notebooklm_ask.await_count, 2)
        for call in notebooklm_ask.await_args_list:
            self.assertEqual(call.kwargs["chat_id"], 555000111)
            self.assertEqual(call.kwargs["question"], "что нового?")

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=False)
    @patch("app.bot.handlers.get_settings", return_value=SimpleNamespace())
    async def test_ask_reports_disabled_integration(self, _mock_settings, _is_enabled) -> None:
        message = _private_message(text="/ask anything")
        message.bot = _bot()

        await ask(message)

        message.answer.assert_awaited_once()
        args, _ = message.answer.await_args
        self.assertIn("ответы временно недоступны", args[0])
        self.assertNotIn("NotebookLM", args[0])


class OwnerAdminPanelTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        _owner_admin_pending_inputs.clear()

    @patch("app.bot.handlers._owner_admin_runtime_status", new_callable=AsyncMock)
    @patch("app.bot.handlers._access_store")
    @patch("app.bot.handlers.get_settings")
    async def test_owner_private_admin_command_renders_panel(
        self,
        get_settings_mock,
        access_store,
        runtime_status,
    ) -> None:
        get_settings_mock.return_value = _owner_admin_settings()
        access_store.return_value = _FakeAccessStore()
        runtime_status.return_value = {
            "enabled": True,
            "auth_ready": True,
            "source": "runtime",
            "notebook_id": "nb-1",
        }
        message = _private_message(text="/admin")
        bot = _bot()

        await owner_admin_cmd(message, bot)

        message.answer.assert_awaited_once()
        args, kwargs = message.answer.await_args
        self.assertIn("Админка владельца", args[0])
        self.assertIn("secondary", args[0])
        self.assertEqual(kwargs.get("parse_mode"), "HTML")
        self.assertIsNotNone(kwargs.get("reply_markup"))

    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_command_rejects_non_owner_and_groups(self, get_settings_mock) -> None:
        get_settings_mock.return_value = _owner_admin_settings(bot_admin_user_ids="123456789,999")
        non_owner = _private_message(text="/admin")
        non_owner.from_user.id = 999
        group_owner = _group_message(text="/admin")
        group_owner.from_user.id = 123456789

        await owner_admin_cmd(non_owner, _bot())
        await owner_admin_cmd(group_owner, _bot())

        self.assertIn("только владельцу", non_owner.answer.await_args.args[0])
        self.assertIn("только владельцу", group_owner.answer.await_args.args[0])

    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_callback_denies_non_owner_and_group_context(self, get_settings_mock) -> None:
        get_settings_mock.return_value = _owner_admin_settings(bot_admin_user_ids="123456789,999")
        non_owner = _owner_admin_callback("adm:cfg:toggle", user_id=999)
        group_owner = _owner_admin_callback("adm:cfg:toggle", chat_type="supergroup")

        await owner_admin_callback(non_owner, _bot())
        await owner_admin_callback(group_owner, _bot())

        non_owner.answer.assert_awaited_once_with("Недоступно.", show_alert=False)
        non_owner.message.edit_text.assert_not_awaited()
        group_owner.answer.assert_awaited_once_with("Недоступно.", show_alert=False)
        group_owner.message.edit_text.assert_not_awaited()

    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_callback_rejects_invalid_input_payload(self, get_settings_mock) -> None:
        get_settings_mock.return_value = _owner_admin_settings()
        callback = _owner_admin_callback("adm:input:not_real")

        await owner_admin_callback(callback, _bot())

        callback.answer.assert_awaited_once_with("Неизвестное действие.", show_alert=True)
        callback.message.edit_text.assert_not_awaited()
        self.assertNotIn(123456789, _owner_admin_pending_inputs)

    @patch("app.bot.handlers._conversation_store")
    @patch("app.bot.handlers.get_settings")
    async def test_owner_conversation_commands_render_history(
        self,
        get_settings_mock,
        conversation_store,
    ) -> None:
        get_settings_mock.return_value = _owner_admin_settings()
        fake_conversations = _FakeConversationStore()
        conversation_store.return_value = fake_conversations

        users_message = _private_message(text="/users")
        last_message = _private_message(text="/last_questions")
        history_message = _private_message(text="/history 42 5")

        await users_cmd(users_message)
        await last_questions_cmd(last_message)
        await history_cmd(history_message)

        self.assertIn("Recent users", users_message.answer.await_args.args[0])
        self.assertIn("Kirill", users_message.answer.await_args.args[0])
        self.assertIn("Last questions", last_message.answer.await_args.args[0])
        self.assertIn("Как включить климат?", last_message.answer.await_args.args[0])
        self.assertIn("History for", history_message.answer.await_args.args[0])
        self.assertIn("Поставьте приложение", history_message.answer.await_args.args[0])

    @patch("app.bot.handlers.get_settings")
    async def test_owner_conversation_commands_reject_non_owner_and_groups(self, get_settings_mock) -> None:
        get_settings_mock.return_value = _owner_admin_settings(bot_admin_user_ids="123456789,999")
        non_owner = _private_message(text="/users")
        non_owner.from_user.id = 999
        group_owner = _group_message(text="/last_questions")
        group_owner.from_user.id = 123456789

        await users_cmd(non_owner)
        await last_questions_cmd(group_owner)

        self.assertIn("только владельцу", non_owner.answer.await_args.args[0])
        self.assertIn("только владельцу", group_owner.answer.await_args.args[0])

    @patch("app.bot.handlers._conversation_store")
    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_callback_renders_conversation_panel(
        self,
        get_settings_mock,
        conversation_store,
    ) -> None:
        get_settings_mock.return_value = _owner_admin_settings()
        conversation_store.return_value = _FakeConversationStore()
        callback = _owner_admin_callback("adm:conv")

        await owner_admin_callback(callback, _bot())

        rendered = callback.message.edit_text.await_args.args[0]
        self.assertIn("Dialogs", rendered)
        self.assertIn("/history user_id", rendered)
        self.assertIn("Как включить климат?", rendered)

    @patch("app.bot.handlers._access_store")
    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_toggle_mutates_access_config(
        self,
        get_settings_mock,
        access_store,
    ) -> None:
        get_settings_mock.return_value = _owner_admin_settings()
        fake_access = _FakeAccessStore(access_disabled=True)
        access_store.return_value = fake_access
        callback = _owner_admin_callback("adm:cfg:toggle")

        await owner_admin_callback(callback, _bot())

        self.assertTrue(fake_access.global_config.enabled)
        callback.message.edit_text.assert_awaited_once()
        self.assertIn("Access / Telegram Stars", callback.message.edit_text.await_args.args[0])

    @patch("app.bot.handlers._access_store")
    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_numeric_input_updates_free_limit(
        self,
        get_settings_mock,
        access_store,
    ) -> None:
        get_settings_mock.return_value = _owner_admin_settings()
        fake_access = _FakeAccessStore()
        access_store.return_value = fake_access
        _owner_admin_pending_inputs[123456789] = {"kind": "free"}
        message = _private_message(text="7")

        await owner_admin_input_message(message, _bot())

        self.assertEqual(fake_access.global_config.free_questions_per_24h, 7)
        self.assertNotIn(123456789, _owner_admin_pending_inputs)
        self.assertIn("free / 24h", message.answer.await_args.args[0])

    @patch("app.bot.handlers._access_store")
    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_invalid_numeric_input_keeps_pending_state(
        self,
        get_settings_mock,
        access_store,
    ) -> None:
        get_settings_mock.return_value = _owner_admin_settings()
        fake_access = _FakeAccessStore()
        access_store.return_value = fake_access
        _owner_admin_pending_inputs[123456789] = {"kind": "free"}
        message = _private_message(text="not-a-number")

        await owner_admin_input_message(message, _bot())

        self.assertEqual(fake_access.global_config.free_questions_per_24h, 20)
        self.assertIn(123456789, _owner_admin_pending_inputs)
        self.assertIn("Не сохранил", message.answer.await_args.args[0])

    @patch("app.bot.handlers._telegram_stars_client")
    @patch("app.bot.handlers._access_store")
    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_stars_live_failure_falls_back_to_local_ledger(
        self,
        get_settings_mock,
        access_store,
        telegram_stars_client,
    ) -> None:
        class FailingStarsClient:
            def sanitize_error(self, value):
                return str(value).replace("777:TEST_TOKEN", "[redacted]")

            async def fetch_bot_identity(self):
                raise TelegramStarsAPIError("failed for 777:TEST_TOKEN")

            async def fetch_balance(self):
                raise TelegramStarsAPIError("failed for 777:TEST_TOKEN")

            async def fetch_transactions(self, *, offset, limit):
                raise TelegramStarsAPIError("failed for 777:TEST_TOKEN")

        get_settings_mock.return_value = _owner_admin_settings()
        access_store.return_value = _FakeAccessStore()
        telegram_stars_client.return_value = FailingStarsClient()
        callback = _owner_admin_callback("adm:stars")

        await owner_admin_callback(callback, _bot())

        rendered = callback.message.edit_text.await_args.args[0]
        self.assertIn("Local ledger", rendered)
        self.assertIn("[redacted]", rendered)
        self.assertNotIn("TEST_TOKEN", rendered)

    @patch("app.bot.handlers.get_notebooklm_upload_sync_manager")
    @patch("app.bot.handlers.get_settings")
    async def test_owner_admin_auth_callback_creates_notebooklm_session(
        self,
        get_settings_mock,
        get_upload_manager,
    ) -> None:
        get_settings_mock.return_value = _owner_admin_settings()
        manager = SimpleNamespace(
            create_session=Mock(
                return_value={
                    "entry_url": "https://example/auth/abc",
                    "expires_at": "2026-04-24T13:00:00+00:00",
                    "protocol_url": "tgctxbot-notebooklm-sync://abc",
                }
            )
        )
        get_upload_manager.return_value = manager
        callback = _owner_admin_callback("adm:svc:auth")

        await owner_admin_callback(callback, _bot())

        manager.create_session.assert_called_once()
        self.assertEqual(manager.create_session.call_args.kwargs["requested_by_user_id"], 123456789)
        self.assertIn("https://example/auth/abc", callback.message.edit_text.await_args.args[0])


class AccessCommandAndPaymentTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_balance_and_limits_report_access_state(
        self,
        access_store,
        _resolve_notebooklm_chat_id_for_query,
    ) -> None:
        access_store.return_value = _FakeAccessStore()
        message = _private_message(text="/balance")

        await balance_cmd(message)
        await limits_cmd(message)

        self.assertEqual(message.answer.await_count, 2)
        self.assertIn("Кредиты Telegram Stars", message.answer.await_args_list[0].args[0])
        self.assertIn("Окно лимита: 24 часа", message.answer.await_args_list[1].args[0])
        self.assertNotIn("NotebookLM", message.answer.await_args_list[0].args[0])
        self.assertNotIn("NotebookLM", message.answer.await_args_list[1].args[0])

    @patch("app.bot.handlers._access_store")
    async def test_status_button_reports_access_state(self, access_store) -> None:
        access_store.return_value = _FakeAccessStore()
        callback = _owner_admin_callback("status", user_id=42, chat_type="supergroup")
        callback.message.chat.id = -10012345

        await status_callback(callback)

        callback.message.answer.assert_awaited_once()
        rendered = callback.message.answer.await_args.args[0]
        self.assertIn("Баланс вопросов", rendered)
        self.assertIn("Осталось бесплатных", rendered)
        self.assertIn("Кредиты Telegram Stars", rendered)
        self.assertNotIn("NotebookLM", rendered)
        callback.answer.assert_awaited_once()

    async def test_paysupport_and_terms_are_stars_only(self) -> None:
        message = _private_message(text="/paysupport")

        await paysupport_cmd(message)
        await terms_cmd(message)

        combined = "\n".join(call.args[0] for call in message.answer.await_args_list)
        self.assertIn("Telegram Stars", combined)
        self.assertNotIn("RUB", combined)

    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    @patch("app.bot.handlers._access_store")
    async def test_buy_creates_telegram_stars_invoice(
        self,
        access_store,
        _resolve_notebooklm_chat_id_for_query,
    ) -> None:
        access_store.return_value = _FakeAccessStore()
        message = _private_message(text="/buy")
        message.answer_invoice = AsyncMock()

        await buy_cmd(message)

        message.answer_invoice.assert_awaited_once()
        kwargs = message.answer_invoice.await_args.kwargs
        self.assertEqual(kwargs["currency"], "XTR")
        self.assertEqual(kwargs["prices"][0].amount, 25)
        self.assertEqual(kwargs["payload"], "access:order-1")
        self.assertNotIn("NotebookLM", kwargs["title"])
        self.assertNotIn("NotebookLM", kwargs["description"])
        self.assertNotIn("NotebookLM", kwargs["prices"][0].label)

    @patch("app.bot.handlers._access_store")
    async def test_pre_checkout_validates_stars_order(self, access_store) -> None:
        access_store.return_value = _FakeAccessStore()
        query = SimpleNamespace(
            from_user=SimpleNamespace(id=123456789),
            invoice_payload="access:order-1",
            currency="XTR",
            total_amount=25,
            answer=AsyncMock(),
        )

        await pre_checkout_query(query)

        query.answer.assert_awaited_once_with(ok=True)

    @patch("app.bot.handlers._access_store")
    async def test_successful_payment_grants_credits_once(self, access_store) -> None:
        fake_access = _FakeAccessStore()
        access_store.return_value = fake_access
        payment = SimpleNamespace(
            invoice_payload="access:order-1",
            currency="XTR",
            total_amount=25,
            telegram_payment_charge_id="tg-charge-1",
            provider_payment_charge_id="provider-charge-1",
            model_dump=lambda: {"currency": "XTR"},
        )
        message = _private_message(text="")
        message.successful_payment = payment

        await successful_payment_message(message)

        self.assertEqual(fake_access.recorded_payments[0]["telegram_payment_charge_id"], "tg-charge-1")
        args, _ = message.answer.await_args
        self.assertIn("Добавлено кредитов: 10", args[0])
        self.assertNotIn("NotebookLM", args[0])


class NotebookLMAuthCommandTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.is_bot_admin_user", return_value=True)
    @patch("app.bot.handlers.get_settings")
    @patch("app.bot.handlers.get_notebooklm_upload_sync_manager")
    async def test_auth_nlm_creates_session_for_admin(
        self, get_manager, mock_get_settings, _is_admin
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(bot_admin_user_ids="123456789")
        manager = SimpleNamespace(
            create_session=lambda **_: {
                "entry_url": "https://example/auth/abc",
                "expires_at": "2026-04-24T13:00:00+00:00",
                "protocol_url": "tgctxbot-notebooklm-sync://...",
            }
        )
        get_manager.return_value = manager

        message = _private_message(text="/auth_nlm")
        message.bot = _bot()

        await auth_nlm(message, message.bot)

        message.answer.assert_awaited_once()
        args, _ = message.answer.await_args
        self.assertIn("NotebookLM auth refresh session", args[0])
        self.assertIn("телефона, ручного JSON или Windows helper", args[0])
        self.assertIn("https://example/auth/abc", args[0])

    @patch("app.bot.handlers.is_bot_admin_user", return_value=True)
    @patch("app.bot.handlers.get_settings")
    @patch("app.bot.handlers.get_notebooklm_remote_auth_manager")
    @patch("app.bot.handlers.get_notebooklm_upload_sync_manager")
    async def test_auth_nlm_includes_remote_browser_link_when_configured(
        self,
        get_upload_manager,
        get_remote_manager,
        mock_get_settings,
        _is_admin,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            bot_admin_user_ids="123456789",
            notebooklm_remote_auth_base_url="https://example",
            notebooklm_remote_auth_docker_socket="/var/run/docker.sock",
            notebooklm_remote_auth_selenium_image="selenium/standalone-chromium:4.34.0",
            notebooklm_proxy_enabled=True,
            notebooklm_proxy_url="socks5://127.0.0.1:43129",
        )
        get_upload_manager.return_value = SimpleNamespace(
            create_session=lambda **_: {
                "entry_url": "https://example/auth-session/upload-token",
                "expires_at": "2026-04-24T13:00:00+00:00",
                "protocol_url": "tgctxbot-notebooklm-sync://...",
            }
        )
        get_remote_manager.return_value = SimpleNamespace(
            create_session=lambda **_: {
                "auth_url": "https://example/auth-session/remote-auth/remote-token",
                "expires_at": "2026-04-24T13:00:00+00:00",
            }
        )

        message = _private_message(text="/auth_nlm")
        message.bot = _bot()

        await auth_nlm(message, message.bot)

        args, _ = message.answer.await_args
        self.assertIn("https://example/auth-session/upload-token", args[0])
        self.assertIn("VPS browser login link", args[0])
        self.assertIn("/auth-session/remote-auth/remote-token", args[0])

    @patch("app.bot.handlers.is_bot_admin_user", return_value=False)
    @patch("app.bot.handlers.get_settings")
    async def test_auth_nlm_rejects_non_admin(self, mock_get_settings, _is_admin) -> None:
        mock_get_settings.return_value = SimpleNamespace(bot_admin_user_ids="")

        message = _private_message(text="/auth_nlm")
        message.bot = _bot()

        await auth_nlm(message, message.bot)

        message.answer.assert_awaited_once()
        args, _ = message.answer.await_args
        self.assertIn("владельцу", args[0])


class NotebookLMUpdateCommandTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.is_notebooklm_source_sync_enabled", return_value=True)
    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers.is_bot_admin_user", return_value=True)
    @patch("app.bot.handlers.get_settings")
    @patch("app.services.notebooklm_source_sync.NotebookLMSourceSyncService.sync_chat_delta", new_callable=AsyncMock)
    @patch("app.bot.handlers._resolve_notebooklm_chat_id_for_query", return_value=555000111)
    async def test_update_command_triggers_sync_for_admin(
        self,
        _resolve_notebooklm_chat_id_for_query,
        sync_chat_delta,
        mock_get_settings,
        _is_admin,
        _is_enabled,
        _source_sync_enabled,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            bot_admin_user_ids="123456789",
            notebooklm_source_sync_enabled=True,
        )
        sync_chat_delta.return_value = SimpleNamespace(
            status="uploaded",
            canonical_chat_id=555000111,
            notebook_id="nb-chat",
            message_count=10,
            watermark_before="2026-04-24T11:00:00+00:00",
            watermark_after="2026-04-24T12:00:00+00:00",
            export_path=".state/notebooklm/exports/555000111.md",
            bootstrap_created=False,
        )

        message = _private_message(text="/update")
        message.bot = _bot()

        await update_cmd(message, message.bot)

        sync_chat_delta.assert_awaited_once_with(chat_id=555000111)
        message.answer.assert_awaited_once()

    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers.is_bot_admin_user", return_value=False)
    @patch("app.bot.handlers.get_settings")
    async def test_update_command_rejects_non_admin(
        self, mock_get_settings, _is_admin, _is_enabled
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            bot_admin_user_ids="",
            notebooklm_source_sync_enabled=True,
        )

        message = _private_message(text="/update")
        message.bot = _bot()

        await update_cmd(message, message.bot)

        message.answer.assert_awaited_once()
        args, _ = message.answer.await_args
        self.assertIn("владельцу", args[0])

    @patch("app.bot.handlers.is_notebooklm_source_sync_enabled", return_value=False)
    @patch("app.bot.handlers.is_notebooklm_enabled", return_value=True)
    @patch("app.bot.handlers.get_settings")
    async def test_update_command_reports_manual_source_mode(
        self, mock_get_settings, _is_enabled, _source_sync_enabled
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            bot_admin_user_ids="123456789",
            notebooklm_source_sync_enabled=False,
        )

        message = _private_message(text="/update")
        message.bot = _bot()

        await update_cmd(message, message.bot)

        message.answer.assert_awaited_once()
        args, _ = message.answer.await_args
        self.assertIn("chat-driven sync отключ", args[0])


class MonitoringCommandTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.bot.handlers.is_bot_admin_user", return_value=True)
    @patch("app.bot.handlers.get_settings")
    async def test_monitoring_command_returns_public_link_for_admin(
        self, mock_get_settings, _is_admin
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            bot_admin_user_ids="123456789",
            uptime_kuma_public_url="http://203.0.113.10:3001/",
        )

        message = _private_message(text="/monitoring")

        await monitoring_cmd(message)

        message.answer.assert_awaited_once()
        args, kwargs = message.answer.await_args
        self.assertIn("http://203.0.113.10:3001", args[0])
        self.assertTrue(kwargs["disable_web_page_preview"])

    @patch("app.bot.handlers.is_bot_admin_user", return_value=False)
    @patch("app.bot.handlers.get_settings")
    async def test_monitoring_command_rejects_non_admin(self, mock_get_settings, _is_admin) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            bot_admin_user_ids="",
            uptime_kuma_public_url="http://203.0.113.10:3001/",
        )

        message = _private_message(text="/monitoring")

        await monitoring_cmd(message)

        message.answer.assert_awaited_once()
        args, _ = message.answer.await_args
        self.assertIn("владельцу", args[0])

    @patch("app.bot.handlers.is_bot_admin_user", return_value=True)
    @patch("app.bot.handlers.get_settings")
    async def test_monitoring_command_reports_missing_url(self, mock_get_settings, _is_admin) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            bot_admin_user_ids="123456789",
            uptime_kuma_public_url="",
        )

        message = _private_message(text="/kuma")

        await monitoring_cmd(message)

        message.answer.assert_awaited_once()
        args, _ = message.answer.await_args
        self.assertIn("не настроен", args[0])


if __name__ == "__main__":
    unittest.main()
