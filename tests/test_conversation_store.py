import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from app.services.conversation_store import BotConversationStore


class BotConversationStoreTests(unittest.TestCase):
    def test_records_updates_and_lists_question_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = BotConversationStore(
                settings=SimpleNamespace(
                    bot_conversation_state_path=str(Path(tmpdir) / "conversations.sqlite3")
                )
            )

            turn = store.record_question(
                source="dm",
                telegram_user_id=42,
                username="kirill",
                display_name="Kirill",
                chat_id=100,
                chat_type="private",
                chat_title=None,
                message_id=7,
                thread_id=None,
                question_key="dm:7",
                question_text="Как включить климат?",
            )
            updated = store.update_turn(
                turn_id=turn.turn_id,
                status="answered",
                answer_text="Поставьте приложение.",
                latency_ms=123,
                notebook_id="nb-1",
            )

            self.assertIsNotNone(updated)
            self.assertEqual(updated.status, "answered")
            self.assertEqual(updated.answer_text, "Поставьте приложение.")
            self.assertEqual(updated.latency_ms, 123)

            recent = store.list_recent_turns(limit=5)
            history = store.list_user_history(telegram_user_id=42, limit=5)
            users = store.list_recent_users(limit=5)
            status = store.status()

            self.assertEqual([item.turn_id for item in recent], [turn.turn_id])
            self.assertEqual([item.turn_id for item in history], [turn.turn_id])
            self.assertEqual(users[0].telegram_user_id, 42)
            self.assertEqual(users[0].turn_count, 1)
            self.assertEqual(status["turn_count"], 1)
            self.assertEqual(status["answered_count"], 1)
