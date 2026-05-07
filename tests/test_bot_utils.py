import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.bot.utils import (
    _imported_chat_link_map,
    _live_chat_history_map,
    build_answer_kwargs,
    build_message_url,
    is_chat_admin,
    resolve_canonical_chat_id,
)


class _BotUtilsCacheAwareTestCase(unittest.TestCase):
    def tearDown(self) -> None:
        _imported_chat_link_map.cache_clear()
        _live_chat_history_map.cache_clear()


class BuildMessageUrlTests(_BotUtilsCacheAwareTestCase):
    def test_uses_public_username_when_available(self) -> None:
        self.assertEqual(
            build_message_url(chat_id=-1001234567890, message_id=42, chat_username="teamchat"),
            "https://t.me/teamchat/42",
        )

    def test_uses_telegram_app_link_for_imported_private_group_history(self) -> None:
        settings = SimpleNamespace(
            telegram_chat_alias_map=None,
            telegram_imported_chat_context_map=None,
            telegram_imported_chat_link_map=None,
        )
        with patch("app.bot.utils.get_settings", return_value=settings):
            self.assertEqual(
                build_message_url(chat_id=555000111, message_id=253433, chat_username=None),
                "tg://privatepost?channel=-100555000111&post=253433",
            )

    def test_builds_internal_supergroup_url_without_username(self) -> None:
        self.assertEqual(
            build_message_url(chat_id=-1001234567890, message_id=42, chat_username=None),
            "https://t.me/c/1234567890/42",
        )

    def test_uses_settings_backed_imported_chat_link_map_when_present(self) -> None:
        settings = SimpleNamespace(
            telegram_chat_alias_map=None,
            telegram_imported_chat_context_map=None,
            telegram_imported_chat_link_map="555000111:1234567890",
        )
        with patch("app.bot.utils.get_settings", return_value=settings):
            self.assertEqual(
                build_message_url(chat_id=555000111, message_id=253433, chat_username=None),
                "https://t.me/c/1234567890/253433",
            )


class ResolveCanonicalChatIdTests(_BotUtilsCacheAwareTestCase):
    def test_uses_settings_backed_imported_chat_context_map(self) -> None:
        settings = SimpleNamespace(
            telegram_chat_alias_map=None,
            telegram_imported_chat_context_map="-1001234567890:555000111",
            telegram_imported_chat_link_map=None,
        )
        with patch("app.bot.utils.get_settings", return_value=settings):
            self.assertEqual(resolve_canonical_chat_id(-1001234567890), 555000111)

    def test_alias_map_takes_priority_when_present(self) -> None:
        settings = SimpleNamespace(
            telegram_chat_alias_map="-1001234567890:555000111",
            telegram_imported_chat_context_map="-1001111111111:111",
            telegram_imported_chat_link_map=None,
        )
        with patch("app.bot.utils.get_settings", return_value=settings):
            self.assertEqual(resolve_canonical_chat_id(-1001234567890), 555000111)


class BuildAnswerKwargsTests(unittest.TestCase):
    def test_omits_direct_messages_topic_id_by_default(self) -> None:
        message = SimpleNamespace(direct_messages_topic=SimpleNamespace(topic_id=9876543210))

        with patch(
            "app.bot.utils.get_settings",
            return_value=SimpleNamespace(bot_reply_in_direct_messages_topic=False),
        ):
            self.assertEqual(build_answer_kwargs(message), {})

    def test_includes_direct_messages_topic_id_when_opted_in(self) -> None:
        message = SimpleNamespace(direct_messages_topic=SimpleNamespace(topic_id=9876543210))

        with patch(
            "app.bot.utils.get_settings",
            return_value=SimpleNamespace(bot_reply_in_direct_messages_topic=True),
        ):
            self.assertEqual(build_answer_kwargs(message), {"direct_messages_topic_id": 9876543210})

    def test_omits_direct_messages_topic_id_when_missing(self) -> None:
        with patch(
            "app.bot.utils.get_settings",
            return_value=SimpleNamespace(bot_reply_in_direct_messages_topic=True),
        ):
            self.assertEqual(build_answer_kwargs(SimpleNamespace(direct_messages_topic=None)), {})


class IsChatAdminTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_true_for_admin_statuses(self) -> None:
        for status in ("administrator", "creator"):
            with self.subTest(status=status):
                bot = SimpleNamespace(
                    get_chat_member=AsyncMock(return_value=SimpleNamespace(status=status))
                )

                result = await is_chat_admin(bot=bot, chat_id=1, user_id=2)

                self.assertTrue(result)

    async def test_returns_false_for_regular_members(self) -> None:
        bot = SimpleNamespace(get_chat_member=AsyncMock(return_value=SimpleNamespace(status="member")))

        result = await is_chat_admin(bot=bot, chat_id=1, user_id=2)

        self.assertFalse(result)
