import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

from app.services.notebooklm_background_sync import NotebookLMBackgroundSyncRunner


class NotebookLMBackgroundSyncRunnerTests(unittest.IsolatedAsyncioTestCase):
    def test_next_run_at_uses_moscow_3am_schedule(self) -> None:
        settings = SimpleNamespace(
            notebooklm_background_sync_enabled=True,
            notebooklm_background_sync_timezone="Europe/Moscow",
            notebooklm_background_sync_hour=3,
            notebooklm_background_sync_minute=0,
        )
        runner = NotebookLMBackgroundSyncRunner(
            settings=settings,
            sync_service=SimpleNamespace(),
            history_store=SimpleNamespace(),
            runtime_store=SimpleNamespace(),
        )

        next_run = runner.next_run_at(
            now_utc=datetime(2026, 4, 22, 2, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(next_run.hour, 3)
        self.assertEqual(next_run.minute, 0)
        self.assertEqual(next_run.date().isoformat(), "2026-04-23")
        self.assertEqual(next_run.utcoffset().total_seconds(), 3 * 3600)

    async def test_run_once_syncs_only_mapped_chats(self) -> None:
        sync_service = SimpleNamespace(sync_chat_delta=AsyncMock(side_effect=[
            SimpleNamespace(status="updated"),
            SimpleNamespace(status="noop"),
        ]))
        history_store = SimpleNamespace(
            list_chat_summaries=lambda: [
                SimpleNamespace(canonical_chat_id=1),
                SimpleNamespace(canonical_chat_id=2),
                SimpleNamespace(canonical_chat_id=3),
            ]
        )
        runtime_store = SimpleNamespace(
            is_enabled=lambda: True,
            resolve_notebook_id=lambda chat_id: {1: "nb-1", 2: "", 3: "nb-3"}.get(chat_id, ""),
        )
        runner = NotebookLMBackgroundSyncRunner(
            settings=SimpleNamespace(
                notebooklm_background_sync_enabled=True,
                notebooklm_background_sync_timezone="Europe/Moscow",
                notebooklm_background_sync_hour=3,
                notebooklm_background_sync_minute=0,
            ),
            sync_service=sync_service,
            history_store=history_store,
            runtime_store=runtime_store,
        )

        results = await runner.run_once()

        self.assertEqual(results, [(1, "updated"), (3, "noop")])
        self.assertEqual(sync_service.sync_chat_delta.await_count, 2)

    async def test_run_once_skips_when_source_sync_disabled(self) -> None:
        sync_service = SimpleNamespace(sync_chat_delta=AsyncMock())
        runner = NotebookLMBackgroundSyncRunner(
            settings=SimpleNamespace(
                notebooklm_background_sync_enabled=True,
                notebooklm_source_sync_enabled=False,
                notebooklm_background_sync_timezone="Europe/Moscow",
                notebooklm_background_sync_hour=3,
                notebooklm_background_sync_minute=0,
            ),
            sync_service=sync_service,
            history_store=SimpleNamespace(list_chat_summaries=lambda: [SimpleNamespace(canonical_chat_id=1)]),
            runtime_store=SimpleNamespace(is_enabled=lambda: True),
        )

        results = await runner.run_once()

        self.assertEqual(results, [])
        sync_service.sync_chat_delta.assert_not_awaited()
        self.assertFalse(runner.enabled())
