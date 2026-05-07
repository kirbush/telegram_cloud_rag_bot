import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from app.services.notebooklm_lightweight_history import NotebookLMLightweightHistoryStore


class NotebookLMLightweightHistoryStoreTests(unittest.TestCase):
    def test_new_rows_are_serialized_in_moscow_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
            )
            store = NotebookLMLightweightHistoryStore(settings=settings)
            store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=42,
                username="zhenya",
                display_name="Zhenya",
                text="test",
                message_date=datetime(2026, 4, 22, 7, 25, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            snapshot, _ = store.upsert_reaction_snapshot(
                canonical_chat_id=555000111,
                source_telegram_message_id=501,
                reply_to_message_id=None,
                thread_id=None,
                snapshot={"🔥": 1},
                changed_at=datetime(2026, 4, 22, 7, 26, tzinfo=timezone.utc),
            )

            with closing(store._connect()) as conn:
                message_row = conn.execute(
                    "SELECT message_date FROM notebooklm_lightweight_messages WHERE telegram_message_id = 501"
                ).fetchone()

            self.assertEqual(message_row["message_date"], "2026-04-22T10:25:00+03:00")
            self.assertEqual(snapshot.last_changed_at, datetime(2026, 4, 22, 7, 26, tzinfo=timezone.utc))
            self.assertIn("2026-04-22T10:26:00+03:00", snapshot.snapshot_text)

    def test_mixed_utc_and_moscow_rows_keep_correct_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
            )
            store = NotebookLMLightweightHistoryStore(settings=settings)
            first = store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=42,
                username="zhenya",
                display_name="Zhenya",
                text="test",
                message_date=datetime(2026, 4, 22, 7, 25, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            second = store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=502,
                user_id=43,
                username="stas",
                display_name="Stas",
                text="new local row",
                message_date=datetime(2026, 4, 22, 7, 26, tzinfo=timezone.utc),
                reply_to_message_id=501,
                thread_id=None,
            )
            store.upsert_reaction_snapshot(
                canonical_chat_id=555000111,
                source_telegram_message_id=502,
                reply_to_message_id=None,
                thread_id=None,
                snapshot={"🔥": 1},
                changed_at=datetime(2026, 4, 22, 7, 27, tzinfo=timezone.utc),
            )

            with closing(store._connect()) as conn, conn:
                conn.execute(
                    """
                    UPDATE notebooklm_lightweight_messages
                    SET message_date = '2026-04-22T07:25:00+00:00',
                        created_at = '2026-04-22T07:25:00+00:00',
                        updated_at = '2026-04-22T07:25:00+00:00'
                    WHERE telegram_message_id = 501
                    """
                )
                conn.execute(
                    """
                    UPDATE notebooklm_lightweight_reaction_snapshots
                    SET last_changed_at = '2026-04-22T07:27:00+00:00',
                        created_at = '2026-04-22T07:27:00+00:00',
                        updated_at = '2026-04-22T07:27:00+00:00',
                        snapshot_text = 'Current reactions for message 502 as of 2026-04-22T07:27:00+00:00: 🔥 x1'
                    WHERE source_telegram_message_id = 502
                    """
                )

            latest = store.get_latest_message_on_or_before(
                canonical_chat_id=555000111,
                cutoff=datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc),
            )
            delta = store.list_delta_messages(
                canonical_chat_id=555000111,
                watermark_date=first.message_date,
                watermark_message_pk=first.message_pk,
                until=datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc),
            )
            snapshots = store.list_reaction_snapshots_between(
                canonical_chat_id=555000111,
                since=datetime(2026, 4, 22, 7, 0, tzinfo=timezone.utc),
                until=datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc),
            )

            self.assertIsNotNone(latest)
            self.assertEqual(latest.telegram_message_id, second.telegram_message_id)
            self.assertEqual([message.telegram_message_id for message in delta], [502])
            self.assertEqual(len(snapshots), 1)
            self.assertEqual(snapshots[0].source_telegram_message_id, 502)
            self.assertEqual(snapshots[0].last_changed_at, datetime(2026, 4, 22, 7, 27, tzinfo=timezone.utc))

    def test_upsert_message_preserves_chain_metadata_and_updates_existing_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
            )
            store = NotebookLMLightweightHistoryStore(settings=settings)

            created = store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=42,
                username="alice",
                display_name="Alice",
                text="Original message",
                message_date=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                reply_to_message_id=500,
                thread_id=77,
            )

            updated = store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=None,
                username=None,
                display_name=None,
                text="Edited message",
                message_date=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
                edited=True,
            )

            self.assertEqual(created.message_pk, updated.message_pk)
            self.assertEqual(updated.text, "Edited message")
            self.assertEqual(updated.reply_to_message_id, 500)
            self.assertEqual(updated.thread_id, 77)
            self.assertTrue(updated.edited)
            self.assertEqual(updated.username, "alice")
            self.assertEqual(updated.display_name, "Alice")

    def test_delta_selection_and_chat_summaries_use_canonical_chat_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
            )
            store = NotebookLMLightweightHistoryStore(settings=settings)
            store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=500,
                user_id=42,
                username="alice",
                display_name="Alice",
                text="Bootstrap",
                message_date=datetime(2026, 4, 10, 18, 30, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            first_delta = store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=43,
                username="bob",
                display_name="Bob",
                text="First delta",
                message_date=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                reply_to_message_id=500,
                thread_id=77,
            )
            second_delta = store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=502,
                user_id=44,
                username="carol",
                display_name="Carol",
                text="Second delta",
                message_date=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                reply_to_message_id=501,
                thread_id=77,
            )

            latest_bootstrap = store.get_latest_message_on_or_before(
                canonical_chat_id=555000111,
                cutoff=datetime(2026, 4, 10, 23, 59, tzinfo=timezone.utc),
            )
            delta = store.list_delta_messages(
                canonical_chat_id=555000111,
                watermark_date=latest_bootstrap.message_date,
                watermark_message_pk=latest_bootstrap.message_pk,
                until=datetime(2026, 4, 12, 0, 0, tzinfo=timezone.utc),
            )
            summaries = store.list_chat_summaries()

            self.assertIsNotNone(latest_bootstrap)
            self.assertEqual([item.message_pk for item in delta], [first_delta.message_pk, second_delta.message_pk])
            self.assertEqual(delta[1].reply_to_message_id, 501)
            self.assertEqual(delta[1].thread_id, 77)
            self.assertEqual(len(summaries), 1)
            self.assertEqual(summaries[0].canonical_chat_id, 555000111)
            self.assertEqual(summaries[0].title, "Live Chat")
            self.assertEqual(summaries[0].message_count, 3)

    def test_reaction_snapshot_keeps_last_changed_at_when_hash_is_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
            )
            store = NotebookLMLightweightHistoryStore(settings=settings)
            created, created_changed = store.upsert_reaction_snapshot(
                canonical_chat_id=555000111,
                source_telegram_message_id=501,
                reply_to_message_id=500,
                thread_id=77,
                snapshot={"👍": 2},
                changed_at=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
            )
            updated, updated_changed = store.upsert_reaction_snapshot(
                canonical_chat_id=555000111,
                source_telegram_message_id=501,
                reply_to_message_id=None,
                thread_id=None,
                snapshot={"👍": 2},
                changed_at=datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc),
            )

            self.assertTrue(created_changed)
            self.assertFalse(updated_changed)
            self.assertEqual(created.last_changed_at, updated.last_changed_at)
            self.assertEqual(updated.reply_to_message_id, 500)
            self.assertEqual(updated.thread_id, 77)

    def test_reaction_actor_delta_persists_known_authors_and_renders_remaining_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
            )
            store = NotebookLMLightweightHistoryStore(settings=settings)
            store.upsert_reaction_snapshot(
                canonical_chat_id=555000111,
                source_telegram_message_id=501,
                reply_to_message_id=500,
                thread_id=77,
                snapshot={"🔥": 3},
                changed_at=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                snapshot_origin="count",
            )

            updated, changed = store.apply_reaction_actor_delta(
                canonical_chat_id=555000111,
                source_telegram_message_id=501,
                actor_type="user",
                actor_user_id=42,
                actor_chat_id=None,
                username="kirill",
                display_name="Kirill Bushmakin",
                old_labels=[],
                new_labels=["🔥"],
                changed_at=datetime(2026, 4, 11, 9, 20, tzinfo=timezone.utc),
                reply_to_message_id=500,
                thread_id=77,
            )

            actors = store.list_reaction_actors(
                canonical_chat_id=555000111,
                source_telegram_message_id=501,
            )

            self.assertTrue(changed)
            self.assertEqual(updated.snapshot, {"🔥": 3})
            self.assertEqual(len(actors), 1)
            self.assertEqual(actors[0].actor_user_id, 42)
            self.assertEqual(actors[0].reactions, {"🔥": 1})
            self.assertIn("Known public reaction authors", updated.snapshot_text)
            self.assertIn("Kirill Bushmakin (@kirill)", updated.snapshot_text)
            self.assertIn("Additional count-only or anonymous reactions", updated.snapshot_text)

    def test_media_job_round_trip_and_status_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
            )
            store = NotebookLMLightweightHistoryStore(settings=settings)
            job = store.create_media_job(
                canonical_chat_id=555000111,
                source_telegram_message_id=501,
                media_kind="photo",
                file_id="file-1",
                file_unique_id="uniq-1",
                thumbnail_file_id=None,
            )
            running = store.mark_media_job_running(job_pk=job.job_pk)
            retryable = store.mark_media_job_retryable(job_pk=job.job_pk, error="boom")
            loaded = store.get_media_job(
                canonical_chat_id=555000111,
                source_telegram_message_id=501,
                media_kind="photo",
            )

            self.assertEqual(job.status, "pending")
            self.assertEqual(running.status, "running")
            self.assertEqual(running.attempt_count, 1)
            self.assertEqual(retryable.status, "retryable")
            self.assertEqual(retryable.last_error, "boom")
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.status, "retryable")
