"""Background daily NotebookLM source sync runner."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.config import get_settings, is_notebooklm_source_sync_enabled
from app.services.notebooklm_events import log_event
from app.services.notebooklm_lightweight_history import NotebookLMLightweightHistoryStore
from app.services.notebooklm_metrics import set_sync_last_success_timestamp
from app.services.notebooklm_runtime import NotebookLMRuntimeStore
from app.services.notebooklm_source_sync import NotebookLMSourceSyncService

logger = logging.getLogger(__name__)


class NotebookLMBackgroundSyncRunner:
    def __init__(
        self,
        *,
        settings=None,
        sync_service: NotebookLMSourceSyncService | None = None,
        history_store: NotebookLMLightweightHistoryStore | None = None,
        runtime_store: NotebookLMRuntimeStore | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._sync_service = sync_service or NotebookLMSourceSyncService(settings=self._settings)
        self._history_store = history_store or NotebookLMLightweightHistoryStore(settings=self._settings)
        self._runtime_store = runtime_store or NotebookLMRuntimeStore(settings=self._settings)
        self._task: asyncio.Task[None] | None = None

    def enabled(self) -> bool:
        return bool(getattr(self._settings, "notebooklm_background_sync_enabled", False)) and is_notebooklm_source_sync_enabled(
            self._settings
        )

    def _timezone(self) -> timezone | ZoneInfo:
        timezone_name = str(
            getattr(self._settings, "notebooklm_background_sync_timezone", "Europe/Moscow") or "Europe/Moscow"
        ).strip()
        try:
            return ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            if timezone_name == "Europe/Moscow":
                return timezone(timedelta(hours=3))
            raise

    def _scheduled_time(self) -> tuple[int, int]:
        return (
            int(getattr(self._settings, "notebooklm_background_sync_hour", 3) or 3),
            int(getattr(self._settings, "notebooklm_background_sync_minute", 0) or 0),
        )

    def next_run_at(self, *, now_utc: datetime | None = None) -> datetime:
        now_local = (now_utc or datetime.now(tz=self._timezone())).astimezone(self._timezone())
        hour, minute = self._scheduled_time()
        target = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now_local:
            target = target + timedelta(days=1)
        return target

    async def run_once(self) -> list[tuple[int, str]]:
        results: list[tuple[int, str]] = []
        if not self._runtime_store.is_enabled():
            log_event(logger, logging.INFO, "nlm.sync.skipped", reason="runtime_disabled")
            return results
        if not is_notebooklm_source_sync_enabled(self._settings):
            log_event(logger, logging.INFO, "nlm.sync.skipped", reason="source_sync_disabled")
            return results
        timeout_seconds = max(int(getattr(self._settings, "notebooklm_timeout", 60) or 60) * 2, 1)
        for summary in self._history_store.list_chat_summaries():
            notebook_id = self._runtime_store.resolve_notebook_id(summary.canonical_chat_id)
            if not notebook_id:
                log_event(
                    logger,
                    logging.INFO,
                    "nlm.sync.skipped",
                    reason="missing_notebook_mapping",
                    canonical_chat_id=summary.canonical_chat_id,
                )
                continue
            try:
                result = await asyncio.wait_for(
                    self._sync_service.sync_chat_delta(chat_id=summary.canonical_chat_id),
                    timeout=timeout_seconds,
                )
                results.append((summary.canonical_chat_id, result.status))
                set_sync_last_success_timestamp(datetime.now(timezone.utc).timestamp())
            except Exception:
                logger.exception(
                    "notebooklm.background_sync chat_failed chat_id=%s",
                    summary.canonical_chat_id,
                )
        return results

    async def run_forever(self) -> None:
        if not self.enabled():
            return
        timezone_name = getattr(self._settings, "notebooklm_background_sync_timezone", "Europe/Moscow")
        while True:
            next_run = self.next_run_at(now_utc=datetime.now(tz=self._timezone()))
            delay_seconds = max((next_run - datetime.now(tz=self._timezone())).total_seconds(), 0.0)
            logger.info(
                "notebooklm.background_sync sleeping until %s (%s)",
                next_run.isoformat(),
                timezone_name,
            )
            await asyncio.sleep(delay_seconds)
            try:
                results = await self.run_once()
                logger.info("notebooklm.background_sync completed chats=%s", results)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("notebooklm.background_sync unexpected_failure")

    def start(self) -> asyncio.Task[None] | None:
        if not self.enabled():
            return None
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run_forever())
        return self._task

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None
