import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from app.core.notebooklm_time import notebooklm_timezone_name
from app.services.notebooklm_lightweight_history import NotebookLMLightweightHistoryStore
from app.services.notebooklm_source_sync import (
    NotebookLMSourceSyncCheckpoint,
    NotebookLMSourceSyncError,
    NotebookLMSourceSyncService,
    NotebookLMSourceSyncStore,
    NotebookLMSyncMessage,
    NotebookLMRollingSource,
)


class NotebookLMSourceSyncStoreTests(unittest.TestCase):
    def test_store_round_trip_persists_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_source_sync_state_path=str(Path(tmp) / "state.json"),
            )
            store = NotebookLMSourceSyncStore(settings=settings)
            checkpoint = NotebookLMSourceSyncCheckpoint(
                context_key="555000111:nb-1",
                canonical_chat_id=555000111,
                notebook_id="nb-1",
                last_uploaded_message_date="2026-04-10T18:30:00+00:00",
                last_uploaded_message_pk=123,
                last_uploaded_telegram_message_id=456,
                updated_at="2026-04-21T10:00:00+00:00",
                bootstrap_cutoff_date="2026-04-10",
                last_export_path=None,
            )

            store.save_checkpoint(checkpoint)

            saved = store.get_checkpoint(canonical_chat_id=555000111, notebook_id="nb-1")
            self.assertIsNotNone(saved)
            self.assertEqual(saved.last_uploaded_message_pk, 123)
            payload = json.loads(Path(settings.notebooklm_source_sync_state_path).read_text(encoding="utf-8"))
            self.assertEqual(payload["checkpoints"][0]["context_key"], "555000111:nb-1")
            self.assertEqual(payload["checkpoints"][0]["last_uploaded_message_date"], "2026-04-10T21:30:00+03:00")
            self.assertEqual(payload["storage_timezone"], notebooklm_timezone_name())


class _DummySession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class NotebookLMSourceSyncServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_upload_markdown_prefers_notebooklm_sources_add_text_for_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            export_path = Path(tmp) / "delta.md"
            export_path.write_text("# delta\n", encoding="utf-8")
            add_file = AsyncMock()
            add_text = AsyncMock()
            fake_client = SimpleNamespace(
                sources=SimpleNamespace(add_file=add_file, add_text=add_text),
                __aenter__=AsyncMock(return_value=None),
                __aexit__=AsyncMock(return_value=False),
            )

            class _ClientContext:
                def __init__(self, client) -> None:
                    self.sources = client.sources

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb) -> bool:
                    return False

            runtime_store = SimpleNamespace(
                resolve_storage_state_path=lambda: str(Path(tmp) / "storage.json"),
            )
            settings = SimpleNamespace(
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=False,
            )
            service = NotebookLMSourceSyncService(
                settings=settings,
                session_factory=lambda: _DummySession(),
                runtime_store=runtime_store,
                client_factory=AsyncMock(return_value=_ClientContext(fake_client)),
            )

            await service._upload_markdown(notebook_id="nb-1", export_path=export_path)

            add_text.assert_awaited_once_with("nb-1", "delta.md", "# delta\n")
            add_file.assert_not_awaited()

    async def test_upload_markdown_falls_back_to_file_upload_when_add_text_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            export_path = Path(tmp) / "delta.md"
            export_path.write_text("# delta\n", encoding="utf-8")
            add_file = AsyncMock()
            fake_client = SimpleNamespace(
                sources=SimpleNamespace(add_file=add_file),
                __aenter__=AsyncMock(return_value=None),
                __aexit__=AsyncMock(return_value=False),
            )

            class _ClientContext:
                def __init__(self, client) -> None:
                    self.sources = client.sources

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb) -> bool:
                    return False

            runtime_store = SimpleNamespace(
                resolve_storage_state_path=lambda: str(Path(tmp) / "storage.json"),
            )
            settings = SimpleNamespace(
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=False,
            )
            service = NotebookLMSourceSyncService(
                settings=settings,
                session_factory=lambda: _DummySession(),
                runtime_store=runtime_store,
                client_factory=AsyncMock(return_value=_ClientContext(fake_client)),
            )

            await service._upload_markdown(notebook_id="nb-1", export_path=export_path)

            add_file.assert_awaited_once_with("nb-1", str(export_path), mime_type="text/markdown")

    async def test_upload_markdown_raises_when_add_text_is_missing_and_file_upload_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            export_path = Path(tmp) / "delta.md"
            export_path.write_text("# delta\n", encoding="utf-8")
            request = httpx.Request("POST", "https://notebooklm.google.com/upload/_/?authuser=0")
            response = httpx.Response(500, request=request)
            add_file = AsyncMock(side_effect=httpx.HTTPStatusError("upload failed", request=request, response=response))
            fake_client = SimpleNamespace(
                sources=SimpleNamespace(add_file=add_file),
                __aenter__=AsyncMock(return_value=None),
                __aexit__=AsyncMock(return_value=False),
            )

            class _ClientContext:
                def __init__(self, client) -> None:
                    self.sources = client.sources

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb) -> bool:
                    return False

            runtime_store = SimpleNamespace(
                resolve_storage_state_path=lambda: str(Path(tmp) / "storage.json"),
            )
            settings = SimpleNamespace(
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=False,
            )
            service = NotebookLMSourceSyncService(
                settings=settings,
                session_factory=lambda: _DummySession(),
                runtime_store=runtime_store,
                client_factory=AsyncMock(return_value=_ClientContext(fake_client)),
            )

            with self.assertRaises(httpx.HTTPStatusError):
                await service._upload_markdown(notebook_id="nb-1", export_path=export_path)

            add_file.assert_awaited_once_with("nb-1", str(export_path), mime_type="text/markdown")

    # Legacy bootstrap/update tests were removed together with the
    # SQLAlchemy path. The lightweight variants below exercise the same contract
    # against the SQLite-based lightweight history store.

    async def test_sync_chat_delta_uses_lightweight_history_store_in_lightweight_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_timeout=30,
                notebooklm_vps_lightweight_mode=True,
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
                notebooklm_source_sync_state_path=str(Path(tmp) / "state.json"),
                notebooklm_source_sync_export_dir=str(Path(tmp) / "exports"),
                notebooklm_source_sync_bootstrap_cutoff_date="2026-04-10",
            )
            runtime_store = SimpleNamespace(
                resolve_notebook_id=lambda chat_id: "nb-1",
                resolve_storage_state_path=lambda: str(Path(tmp) / "storage.json"),
            )
            lightweight_history_store = NotebookLMLightweightHistoryStore(settings=settings)
            lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=42,
                username="alice",
                display_name="Alice",
                text="bootstrap",
                message_date=datetime(2026, 4, 10, 18, 30, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=502,
                user_id=43,
                username="bob",
                display_name="Bob",
                text="delta reply",
                message_date=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                reply_to_message_id=501,
                thread_id=77,
            )

            service = NotebookLMSourceSyncService(
                settings=settings,
                session_factory=lambda: _DummySession(),
                runtime_store=runtime_store,
                lightweight_history_store=lightweight_history_store,
                now_fn=lambda: datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
            )
            service._replace_active_source_upload = AsyncMock(return_value="src-1")

            result = await service.sync_chat_delta(chat_id=555000111)

            self.assertEqual(result.status, "updated")
            self.assertEqual(result.message_count, 1)
            self.assertTrue(result.bootstrap_created)
            self.assertEqual(result.watermark_after, "2026-04-11T12:15:00+03:00")
            self.assertTrue(Path(result.export_path).exists())
            export_text = Path(result.export_path).read_text(encoding="utf-8")
            self.assertIn("# Telegram Context Source v2", export_text)
            self.assertIn("Timezone: Europe/Moscow GMT+3", export_text)
            self.assertIn("## 2026-04-11 (Europe/Moscow GMT+3)", export_text)
            self.assertIn("### 12:15:00 GMT+3 - Bob (@bob) - message_text - message 502", export_text)
            self.assertIn("delta reply", export_text)
            self.assertIn("Reply to telegram message id: 501", export_text)
            self.assertIn("Thread id: 77", export_text)
            self.assertIn("Author: Bob (@bob)", export_text)
            self.assertIn("Date (Europe/Moscow GMT+3): 2026-04-11T12:15:00+03:00", export_text)
            self.assertIn("First entry (Europe/Moscow GMT+3)", export_text)
            service._replace_active_source_upload.assert_awaited_once()
            payload = json.loads(Path(settings.notebooklm_source_sync_state_path).read_text(encoding="utf-8"))
            self.assertEqual(payload["checkpoints"][0]["rolling_sources"][0]["segment_index"], 1)

    async def test_sync_chat_delta_migrates_legacy_checkpoint_and_exports_reaction_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_timeout=30,
                notebooklm_vps_lightweight_mode=True,
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
                notebooklm_source_sync_state_path=str(Path(tmp) / "state.json"),
                notebooklm_source_sync_export_dir=str(Path(tmp) / "exports"),
                notebooklm_source_sync_bootstrap_cutoff_date="2026-04-10",
            )
            runtime_store = SimpleNamespace(
                resolve_notebook_id=lambda chat_id: "nb-1",
                resolve_storage_state_path=lambda: str(Path(tmp) / "storage.json"),
            )
            lightweight_history_store = NotebookLMLightweightHistoryStore(settings=settings)
            bootstrap = lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=42,
                username="alice",
                display_name="Alice",
                text="bootstrap",
                message_date=datetime(2026, 4, 10, 18, 30, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=502,
                user_id=43,
                username="bob",
                display_name="Bob",
                text="delta reply",
                message_date=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                reply_to_message_id=501,
                thread_id=77,
            )
            lightweight_history_store.backfill_legacy_message_events()
            lightweight_history_store.upsert_reaction_snapshot(
                canonical_chat_id=555000111,
                source_telegram_message_id=502,
                reply_to_message_id=501,
                thread_id=77,
                snapshot={"👍": 3, "❤️": 1},
                changed_at=datetime(2026, 4, 11, 10, 0, tzinfo=timezone.utc),
            )
            store = NotebookLMSourceSyncStore(settings=settings)
            store.save_checkpoint(
                NotebookLMSourceSyncCheckpoint(
                    context_key="555000111:nb-1",
                    canonical_chat_id=555000111,
                    notebook_id="nb-1",
                    last_uploaded_message_date="2026-04-10T18:30:00+00:00",
                    last_uploaded_message_pk=bootstrap.message_pk,
                    last_uploaded_telegram_message_id=501,
                    updated_at="2026-04-21T09:00:00+00:00",
                    bootstrap_cutoff_date="2026-04-10",
                    last_export_path=None,
                )
            )
            service = NotebookLMSourceSyncService(
                settings=settings,
                session_factory=lambda: _DummySession(),
                runtime_store=runtime_store,
                state_store=store,
                lightweight_history_store=lightweight_history_store,
                now_fn=lambda: datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
            )
            service._replace_active_source_upload = AsyncMock(return_value="src-1")

            result = await service.sync_chat_delta(chat_id=555000111)

            self.assertEqual(result.status, "updated")
            self.assertEqual(result.message_count, 2)
            export_text = Path(result.export_path).read_text(encoding="utf-8")
            self.assertIn("Event type: message_text", export_text)
            self.assertIn("Event type: reaction_snapshot", export_text)
            self.assertIn("### 13:00:00 GMT+3 - system - reaction_snapshot - message 502", export_text)
            self.assertIn("Current reactions for message 502", export_text)
            saved = store.get_checkpoint(canonical_chat_id=555000111, notebook_id="nb-1")
            self.assertEqual(saved.last_uploaded_event_stream, "reaction_snapshot")
            self.assertIsNotNone(saved.last_uploaded_event_pk)
            self.assertEqual(saved.rolling_sources[0].notebook_source_id, "src-1")

    async def test_sync_chat_delta_keeps_checkpoint_and_export_unchanged_when_upload_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            export_path = Path(tmp) / "exports" / "segment-001.md"
            settings = SimpleNamespace(
                notebooklm_timeout=30,
                notebooklm_vps_lightweight_mode=True,
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
                notebooklm_source_sync_state_path=str(Path(tmp) / "state.json"),
                notebooklm_source_sync_export_dir=str(Path(tmp) / "exports"),
                notebooklm_source_sync_bootstrap_cutoff_date="2026-04-10",
            )
            runtime_store = SimpleNamespace(
                resolve_notebook_id=lambda chat_id: "nb-1",
                resolve_storage_state_path=lambda: str(Path(tmp) / "storage.json"),
            )
            lightweight_history_store = NotebookLMLightweightHistoryStore(settings=settings)
            bootstrap = lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=42,
                username="alice",
                display_name="Alice",
                text="already uploaded",
                message_date=datetime(2026, 4, 10, 18, 30, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=502,
                user_id=43,
                username="bob",
                display_name="Bob",
                text="must not be committed before upload",
                message_date=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            source = NotebookLMRollingSource(
                segment_index=1,
                status="active",
                title="segment-001.md",
                local_path=str(export_path),
                notebook_source_id="old-src",
                started_event_date="2026-04-10T18:30:00+00:00",
                started_event_stream="timeline",
                started_event_pk=bootstrap.message_pk,
                last_event_date="2026-04-10T18:30:00+00:00",
                last_event_stream="timeline",
                last_event_pk=bootstrap.message_pk,
                word_count=10,
                entry_count=1,
                updated_at="2026-04-21T09:00:00+00:00",
            )
            store = NotebookLMSourceSyncStore(settings=settings)
            store.save_checkpoint(
                NotebookLMSourceSyncCheckpoint(
                    context_key="555000111:nb-1",
                    canonical_chat_id=555000111,
                    notebook_id="nb-1",
                    last_uploaded_message_date="2026-04-10T18:30:00+00:00",
                    last_uploaded_message_pk=bootstrap.message_pk,
                    last_uploaded_telegram_message_id=501,
                    updated_at="2026-04-21T09:00:00+00:00",
                    bootstrap_cutoff_date="2026-04-10",
                    last_export_path=str(export_path),
                    last_uploaded_event_date="2026-04-10T18:30:00+00:00",
                    last_uploaded_event_stream="timeline",
                    last_uploaded_event_pk=bootstrap.message_pk,
                    rolling_sources=[source],
                )
            )
            service = NotebookLMSourceSyncService(
                settings=settings,
                session_factory=lambda: _DummySession(),
                runtime_store=runtime_store,
                state_store=store,
                lightweight_history_store=lightweight_history_store,
                now_fn=lambda: datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
            )
            export_path.parent.mkdir(parents=True, exist_ok=True)
            old_export_text = service._render_rolling_source_markdown(
                canonical_chat_id=555000111,
                notebook_id="nb-1",
                source=source,
                body="## 2026-04-10 (Europe/Moscow GMT+3)\n\nold body\n",
            )
            export_path.write_text(old_export_text, encoding="utf-8")
            service._replace_active_source_upload = AsyncMock(side_effect=TimeoutError("upload timed out"))

            with self.assertRaises(TimeoutError):
                await service.sync_chat_delta(chat_id=555000111)

            self.assertEqual(export_path.read_text(encoding="utf-8"), old_export_text)
            self.assertFalse(list(export_path.parent.glob("*.upload-*.tmp")))
            saved = store.get_checkpoint(canonical_chat_id=555000111, notebook_id="nb-1")
            self.assertEqual(saved.last_uploaded_event_pk, bootstrap.message_pk)
            self.assertEqual(saved.last_uploaded_telegram_message_id, 501)
            self.assertEqual(saved.rolling_sources[0].entry_count, 1)
            self.assertEqual(saved.rolling_sources[0].notebook_source_id, "old-src")

    async def test_sync_chat_delta_rotates_to_new_segment_when_active_source_budget_is_full(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_timeout=30,
                notebooklm_vps_lightweight_mode=True,
                notebooklm_lightweight_history_path=str(Path(tmp) / "history.sqlite3"),
                notebooklm_source_sync_state_path=str(Path(tmp) / "state.json"),
                notebooklm_source_sync_export_dir=str(Path(tmp) / "exports"),
                notebooklm_source_sync_bootstrap_cutoff_date="2026-04-10",
                notebooklm_source_sync_max_words_per_source=5,
                notebooklm_source_sync_max_sources_per_notebook=50,
            )
            runtime_store = SimpleNamespace(
                resolve_notebook_id=lambda chat_id: "nb-1",
                resolve_storage_state_path=lambda: str(Path(tmp) / "storage.json"),
            )
            lightweight_history_store = NotebookLMLightweightHistoryStore(settings=settings)
            bootstrap = lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=501,
                user_id=42,
                username="alice",
                display_name="Alice",
                text="bootstrap",
                message_date=datetime(2026, 4, 10, 18, 30, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=502,
                user_id=43,
                username="bob",
                display_name="Bob",
                text="one two three four",
                message_date=datetime(2026, 4, 11, 9, 15, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            lightweight_history_store.upsert_message(
                canonical_chat_id=555000111,
                live_chat_id=-10012345,
                chat_title="Live Chat",
                chat_type="supergroup",
                chat_username=None,
                telegram_message_id=503,
                user_id=44,
                username="carol",
                display_name="Carol",
                text="five six seven eight",
                message_date=datetime(2026, 4, 11, 9, 16, tzinfo=timezone.utc),
                reply_to_message_id=None,
                thread_id=None,
            )
            store = NotebookLMSourceSyncStore(settings=settings)
            store.save_checkpoint(
                NotebookLMSourceSyncCheckpoint(
                    context_key="555000111:nb-1",
                    canonical_chat_id=555000111,
                    notebook_id="nb-1",
                    last_uploaded_message_date="2026-04-10T18:30:00+00:00",
                    last_uploaded_message_pk=bootstrap.message_pk,
                    last_uploaded_telegram_message_id=501,
                    updated_at="2026-04-21T09:00:00+00:00",
                    bootstrap_cutoff_date="2026-04-10",
                    last_export_path=None,
                )
            )
            service = NotebookLMSourceSyncService(
                settings=settings,
                session_factory=lambda: _DummySession(),
                runtime_store=runtime_store,
                state_store=store,
                lightweight_history_store=lightweight_history_store,
                now_fn=lambda: datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
            )
            service._replace_active_source_upload = AsyncMock(side_effect=["src-1", "src-2"])

            result = await service.sync_chat_delta(chat_id=555000111)

            self.assertEqual(result.status, "updated")
            saved = store.get_checkpoint(canonical_chat_id=555000111, notebook_id="nb-1")
            self.assertEqual(len(saved.rolling_sources), 2)
            self.assertEqual(saved.rolling_sources[0].status, "finalized")
            self.assertEqual(saved.rolling_sources[1].status, "active")

    async def test_sync_chat_delta_errors_without_notebook_mapping(self) -> None:
        service = NotebookLMSourceSyncService(
            settings=SimpleNamespace(notebooklm_timeout=30),
            session_factory=lambda: _DummySession(),
            runtime_store=SimpleNamespace(resolve_notebook_id=lambda chat_id: ""),
        )

        with self.assertRaises(NotebookLMSourceSyncError):
            await service.sync_chat_delta(chat_id=555000111)
