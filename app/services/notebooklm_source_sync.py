"""NotebookLM source delta export and upload orchestration."""

from __future__ import annotations

import copy
import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import httpx
from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession

from app.core.config import get_notebooklm_proxy_url, get_settings
from app.core.notebooklm_time import notebooklm_isoformat, notebooklm_timezone_name, parse_timestamp
from app.services.notebooklm_client import create_notebooklm_client
from app.services.notebooklm_events import log_event
from app.services.notebooklm_lightweight_history import (
    REACTION_STREAM,
    TIMELINE_STREAM,
    NotebookLMLightweightHistoryStore,
    NotebookLMLightweightTimelineEvent,
    ReactionSnapshotState,
)
from app.services.notebooklm_metrics import set_sources_used
from app.services.notebooklm_runtime import NotebookLMRuntimeStore

_DEFAULT_BOOTSTRAP_CUTOFF_DATE = "2026-04-10"
_DEFAULT_SOURCE_SYNC_STATE_PATH = ".state/notebooklm/source_sync_state.json"
_DEFAULT_SOURCE_SYNC_EXPORT_DIR = ".state/notebooklm/exports"
_STATE_STORAGE_TIMEZONE_KEY = "storage_timezone"
_SOURCE_FORMAT_VERSION = 2
_SOURCE_TIMEZONE_LABEL = "Europe/Moscow GMT+3"

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc(value: datetime) -> datetime:
    return parse_timestamp(value.isoformat())


def _isoformat(value: datetime) -> str:
    return notebooklm_isoformat(value)


def _parse_datetime(value: str) -> datetime:
    return parse_timestamp(value)


def _stream_rank(stream: str) -> int:
    return 0 if stream == TIMELINE_STREAM else 1


def _word_count(text: str) -> int:
    return len(re.findall(r"\S+", text))


@dataclass(slots=True)
class NotebookLMRollingSource:
    segment_index: int
    status: str
    title: str
    local_path: str
    notebook_source_id: str | None = None
    started_event_date: str | None = None
    started_event_stream: str | None = None
    started_event_pk: int | None = None
    last_event_date: str | None = None
    last_event_stream: str | None = None
    last_event_pk: int | None = None
    word_count: int = 0
    entry_count: int = 0
    updated_at: str | None = None
    finalized_at: str | None = None
    pending_delete_source_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class NotebookLMSourceSyncCheckpoint:
    context_key: str
    canonical_chat_id: int
    notebook_id: str
    last_uploaded_message_date: str
    last_uploaded_message_pk: int
    last_uploaded_telegram_message_id: int
    updated_at: str
    bootstrap_cutoff_date: str | None = None
    last_export_path: str | None = None
    last_uploaded_event_date: str | None = None
    last_uploaded_event_stream: str | None = None
    last_uploaded_event_pk: int | None = None
    last_budget_alert_at: str | None = None
    rolling_sources: list[NotebookLMRollingSource] = field(default_factory=list)


@dataclass(slots=True)
class NotebookLMSyncMessage:
    message_pk: int
    telegram_message_id: int
    message_date: datetime
    user_id: int | None
    username: str | None
    display_name: str | None
    text: str
    reply_to_message_id: int | None
    thread_id: int | None


@dataclass(slots=True)
class NotebookLMSyncEntry:
    event_pk: int
    event_stream: str
    entry_type: str
    source_telegram_message_id: int
    event_date: datetime
    user_id: int | None
    username: str | None
    display_name: str | None
    text: str
    reply_to_message_id: int | None
    thread_id: int | None


@dataclass(slots=True)
class NotebookLMSourceSyncResult:
    status: str
    canonical_chat_id: int
    notebook_id: str
    message_count: int
    watermark_before: str
    watermark_after: str
    export_path: str | None
    bootstrap_created: bool


class NotebookLMSourceSyncError(RuntimeError):
    """Raised when NotebookLM source sync cannot proceed safely."""


class NotebookLMSourceSyncStore:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _path(self) -> Path:
        configured = (
            getattr(self._settings, "notebooklm_source_sync_state_path", _DEFAULT_SOURCE_SYNC_STATE_PATH)
            or _DEFAULT_SOURCE_SYNC_STATE_PATH
        )
        return Path(str(configured)).expanduser()

    @staticmethod
    def _normalize_timestamp_value(value: Any) -> str:
        raw_value = str(value or "").strip()
        if not raw_value:
            return ""
        return _isoformat(_parse_datetime(raw_value))

    def _normalize_payload(self, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        normalized = dict(payload)
        changed = False
        normalized_checkpoints: list[dict[str, Any]] = []
        for item in normalized.get("checkpoints", []):
            if not isinstance(item, dict):
                changed = True
                continue
            normalized_item = dict(item)
            for field_name in (
                "last_uploaded_message_date",
                "updated_at",
                "last_uploaded_event_date",
                "last_budget_alert_at",
            ):
                if normalized_item.get(field_name):
                    value = self._normalize_timestamp_value(normalized_item.get(field_name))
                    if value != normalized_item.get(field_name):
                        normalized_item[field_name] = value
                        changed = True
            rolling_sources = normalized_item.get("rolling_sources", [])
            normalized_sources: list[dict[str, Any]] = []
            for source in rolling_sources:
                if not isinstance(source, dict):
                    changed = True
                    continue
                normalized_source = dict(source)
                for field_name in ("started_event_date", "last_event_date", "updated_at", "finalized_at"):
                    if normalized_source.get(field_name):
                        value = self._normalize_timestamp_value(normalized_source.get(field_name))
                        if value != normalized_source.get(field_name):
                            normalized_source[field_name] = value
                            changed = True
                normalized_sources.append(normalized_source)
            if normalized_sources != rolling_sources:
                normalized_item["rolling_sources"] = normalized_sources
                changed = True
            normalized_checkpoints.append(normalized_item)
        if normalized_checkpoints != normalized.get("checkpoints", []):
            normalized["checkpoints"] = normalized_checkpoints
            changed = True
        timezone_name = notebooklm_timezone_name()
        if normalized.get(_STATE_STORAGE_TIMEZONE_KEY) != timezone_name:
            normalized[_STATE_STORAGE_TIMEZONE_KEY] = timezone_name
            changed = True
        return normalized, changed

    @staticmethod
    def _write_payload(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temp_path.replace(path)

    @staticmethod
    def context_key(*, canonical_chat_id: int, notebook_id: str) -> str:
        return f"{canonical_chat_id}:{notebook_id}"

    def _load(self) -> list[NotebookLMSourceSyncCheckpoint]:
        path = self._path()
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                logger.warning("notebooklm.source_sync failed to discard orphan tmp state file path=%s", temp_path)
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return []
        checkpoints: list[NotebookLMSourceSyncCheckpoint] = []
        for item in payload.get("checkpoints", []):
            if not isinstance(item, dict):
                continue
            checkpoints.append(
                NotebookLMSourceSyncCheckpoint(
                    context_key=str(item.get("context_key", "") or ""),
                    canonical_chat_id=int(item.get("canonical_chat_id", 0) or 0),
                    notebook_id=str(item.get("notebook_id", "") or ""),
                    last_uploaded_message_date=str(item.get("last_uploaded_message_date", "") or ""),
                    last_uploaded_message_pk=int(item.get("last_uploaded_message_pk", 0) or 0),
                    last_uploaded_telegram_message_id=int(item.get("last_uploaded_telegram_message_id", 0) or 0),
                    updated_at=str(item.get("updated_at", "") or ""),
                    bootstrap_cutoff_date=str(item.get("bootstrap_cutoff_date", "") or "") or None,
                    last_export_path=str(item.get("last_export_path", "") or "") or None,
                    last_uploaded_event_date=str(item.get("last_uploaded_event_date", "") or "") or None,
                    last_uploaded_event_stream=str(item.get("last_uploaded_event_stream", "") or "") or None,
                    last_uploaded_event_pk=int(item.get("last_uploaded_event_pk", 0) or 0) or None,
                    last_budget_alert_at=str(item.get("last_budget_alert_at", "") or "") or None,
                    rolling_sources=[
                        NotebookLMRollingSource(
                            segment_index=int(source.get("segment_index", 0) or 0),
                            status=str(source.get("status", "active") or "active"),
                            title=str(source.get("title", "") or ""),
                            local_path=str(source.get("local_path", "") or ""),
                            notebook_source_id=str(source.get("notebook_source_id", "") or "") or None,
                            started_event_date=str(source.get("started_event_date", "") or "") or None,
                            started_event_stream=str(source.get("started_event_stream", "") or "") or None,
                            started_event_pk=int(source.get("started_event_pk", 0) or 0) or None,
                            last_event_date=str(source.get("last_event_date", "") or "") or None,
                            last_event_stream=str(source.get("last_event_stream", "") or "") or None,
                            last_event_pk=int(source.get("last_event_pk", 0) or 0) or None,
                            word_count=int(source.get("word_count", 0) or 0),
                            entry_count=int(source.get("entry_count", 0) or 0),
                            updated_at=str(source.get("updated_at", "") or "") or None,
                            finalized_at=str(source.get("finalized_at", "") or "") or None,
                            pending_delete_source_ids=[
                                str(value)
                                for value in source.get("pending_delete_source_ids", [])
                                if str(value or "").strip()
                            ],
                        )
                        for source in item.get("rolling_sources", [])
                        if isinstance(source, dict)
                    ],
                )
            )
        return checkpoints

    def _write(self, checkpoints: list[NotebookLMSourceSyncCheckpoint]) -> None:
        path = self._path()
        payload, _ = self._normalize_payload(
            {
                _STATE_STORAGE_TIMEZONE_KEY: notebooklm_timezone_name(),
                "checkpoints": [asdict(checkpoint) for checkpoint in checkpoints],
            }
        )
        self._write_payload(path, payload)

    def get_checkpoint(self, *, canonical_chat_id: int, notebook_id: str) -> NotebookLMSourceSyncCheckpoint | None:
        context_key = self.context_key(canonical_chat_id=canonical_chat_id, notebook_id=notebook_id)
        return next((item for item in self._load() if item.context_key == context_key), None)

    def save_checkpoint(self, checkpoint: NotebookLMSourceSyncCheckpoint) -> NotebookLMSourceSyncCheckpoint:
        checkpoints = self._load()
        for index, existing in enumerate(checkpoints):
            if existing.context_key == checkpoint.context_key:
                checkpoints[index] = checkpoint
                self._write(checkpoints)
                return checkpoint
        checkpoints.append(checkpoint)
        self._write(checkpoints)
        return checkpoint


class NotebookLMSourceSyncService:
    def __init__(
        self,
        *,
        settings=None,
        client_factory: Callable[..., Any] = create_notebooklm_client,
        runtime_store: NotebookLMRuntimeStore | None = None,
        state_store: NotebookLMSourceSyncStore | None = None,
        lightweight_history_store: NotebookLMLightweightHistoryStore | None = None,
        now_fn: Callable[[], datetime] = _utc_now,
        session_factory: Any = None,  # noqa: ARG002 - accepted for test backwards-compat, unused in lightweight mode
    ) -> None:
        self._settings = settings or get_settings()
        self._client_factory = client_factory
        self._runtime_store = runtime_store or NotebookLMRuntimeStore(settings=self._settings)
        self._state_store = state_store or NotebookLMSourceSyncStore(settings=self._settings)
        self._lightweight_history_store = lightweight_history_store or NotebookLMLightweightHistoryStore(
            settings=self._settings
        )
        self._now_fn = now_fn
        # session_factory retained only to keep older unit tests that still pass it
        # from breaking. The lightweight runtime never uses a DB session.
        _ = session_factory

    @staticmethod
    def _first_admin_user_id(settings) -> int | None:
        raw_value = str(getattr(settings, "bot_admin_user_ids", "") or "").strip()
        if not raw_value:
            return None
        for part in raw_value.split(","):
            candidate = part.strip()
            if not candidate:
                continue
            try:
                return int(candidate)
            except ValueError:
                continue
        return None

    def _sync_ticks_path(self) -> Path:
        configured = getattr(self._settings, "notebooklm_sync_ticks_path", ".state/notebooklm/sync_ticks.jsonl")
        return Path(str(configured or ".state/notebooklm/sync_ticks.jsonl")).expanduser()

    def _sync_tick_retention_days(self) -> int:
        return max(int(getattr(self._settings, "notebooklm_sync_tick_retention_days", 30) or 30), 1)

    @staticmethod
    def _total_words(checkpoint: NotebookLMSourceSyncCheckpoint | None) -> int:
        if checkpoint is None:
            return 0
        return sum(max(int(source.word_count or 0), 0) for source in checkpoint.rolling_sources)

    async def _append_sync_tick(
        self,
        *,
        chat_id: int,
        notebook_id: str,
        events_appended: int,
        words_before: int,
        words_after: int,
        active_source_id: str | None,
        rotated: bool,
        duration_ms: int,
        error: str | None,
    ) -> None:
        path = self._sync_ticks_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tick = {
            "logged_at": _isoformat(self._now_fn()),
            "chat_id": chat_id,
            "notebook_id": notebook_id,
            "events_appended": events_appended,
            "words_before": words_before,
            "words_after": words_after,
            "active_source_id": active_source_id,
            "rotated": rotated,
            "duration_ms": duration_ms,
            "error": error,
        }
        lines: list[str] = []
        cutoff = self._now_fn() - timedelta(days=self._sync_tick_retention_days())
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                logged_at = payload.get("logged_at")
                try:
                    logged_at_dt = _parse_datetime(str(logged_at or ""))
                except Exception:
                    continue
                if logged_at_dt >= cutoff:
                    lines.append(json.dumps(payload, ensure_ascii=False))
        lines.append(json.dumps(tick, ensure_ascii=False))
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        os.replace(temp_path, path)

    async def _maybe_notify_source_budget(
        self,
        checkpoint: NotebookLMSourceSyncCheckpoint,
    ) -> NotebookLMSourceSyncCheckpoint:
        if len(checkpoint.rolling_sources) < 48:
            return checkpoint
        last_alert = checkpoint.last_budget_alert_at
        if last_alert:
            try:
                if _parse_datetime(last_alert) >= self._now_fn() - timedelta(hours=24):
                    return checkpoint
            except Exception:
                pass
        admin_user_id = self._first_admin_user_id(self._settings)
        if admin_user_id is None:
            return checkpoint
        text = (
            f"⚠ NotebookLM {checkpoint.notebook_id}: 48/50 источников - пора архивировать старый ноутбук"
        )
        try:
            bot_session = None
            if getattr(self._settings, "telegram_proxy_enabled", False) and getattr(
                self._settings,
                "telegram_proxy_url",
                None,
            ):
                bot_session = AiohttpSession(proxy=self._settings.telegram_proxy_url)
            bot = Bot(token=self._settings.bot_token, session=bot_session)
            await bot.send_message(admin_user_id, text, disable_web_page_preview=True)
            await bot.session.close()
            checkpoint.last_budget_alert_at = _isoformat(self._now_fn())
        except Exception:
            logger.exception("notebooklm.source_sync failed to notify admin about source budget")
        return checkpoint

    def _uses_lightweight_history(self) -> bool:
        return True

    def _export_dir(self) -> Path:
        configured = (
            getattr(self._settings, "notebooklm_source_sync_export_dir", _DEFAULT_SOURCE_SYNC_EXPORT_DIR)
            or _DEFAULT_SOURCE_SYNC_EXPORT_DIR
        )
        return Path(str(configured)).expanduser()

    def _bootstrap_cutoff_date(self) -> date:
        raw_value = (
            getattr(
                self._settings,
                "notebooklm_source_sync_bootstrap_cutoff_date",
                _DEFAULT_BOOTSTRAP_CUTOFF_DATE,
            )
            or _DEFAULT_BOOTSTRAP_CUTOFF_DATE
        )
        return date.fromisoformat(str(raw_value).strip())

    def _max_words_per_source(self) -> int:
        return int(getattr(self._settings, "notebooklm_source_sync_max_words_per_source", 500000) or 500000)

    def _max_sources_per_notebook(self) -> int:
        return int(getattr(self._settings, "notebooklm_source_sync_max_sources_per_notebook", 50) or 50)

    def _rolling_source_title(self, *, canonical_chat_id: int, notebook_id: str, segment_index: int) -> str:
        return f"telegram-context-v2-chat-{canonical_chat_id}-notebook-{notebook_id}-segment-{segment_index:03d}.md"

    def _rolling_source_path(self, *, canonical_chat_id: int, notebook_id: str, segment_index: int) -> Path:
        return self._export_dir() / self._rolling_source_title(
            canonical_chat_id=canonical_chat_id,
            notebook_id=notebook_id,
            segment_index=segment_index,
        )

    @staticmethod
    def _entry_author(entry: NotebookLMSyncEntry) -> str | None:
        author = entry.display_name or entry.username or None
        if entry.username and entry.display_name and entry.username != entry.display_name:
            author = f"{entry.display_name} (@{entry.username})"
        elif entry.username:
            author = f"@{entry.username}"
        elif entry.user_id is not None and entry.display_name:
            author = f"{entry.display_name} (user{entry.user_id})"
        elif entry.user_id is not None:
            author = f"user{entry.user_id}"
        return author

    @staticmethod
    def _entry_local_datetime(entry: NotebookLMSyncEntry) -> datetime:
        return datetime.fromisoformat(_isoformat(entry.event_date))

    @staticmethod
    def _last_body_day(body: str) -> str | None:
        matches = re.findall(r"^## (\d{4}-\d{2}-\d{2}) \(", body, flags=re.MULTILINE)
        return matches[-1] if matches else None

    @staticmethod
    def _extract_rolling_source_body(source_text: str) -> str:
        marker = "\n## "
        marker_index = source_text.find(marker)
        return source_text[marker_index + 1 :].strip() if marker_index >= 0 else ""

    def _render_entry_block(self, *, entry: NotebookLMSyncEntry, index: int, include_day_heading: bool = True) -> str:
        local_dt = self._entry_local_datetime(entry)
        day = local_dt.date().isoformat()
        time_label = local_dt.strftime("%H:%M:%S")
        author = self._entry_author(entry)
        heading_author = author or "system"
        lines = [
            f"## {day} ({_SOURCE_TIMEZONE_LABEL})",
            "",
        ] if include_day_heading else []
        lines.extend(
            [
                (
                    f"### {time_label} GMT+3 - {heading_author} - {entry.entry_type} "
                    f"- message {entry.source_telegram_message_id}"
                ),
                f"Entry index: {index}",
                f"Event type: {entry.entry_type}",
                f"Telegram message id: {entry.source_telegram_message_id}",
                f"Date ({_SOURCE_TIMEZONE_LABEL}): {_isoformat(entry.event_date)}",
            ]
        )
        if author:
            lines.append(f"Author: {author}")
        if entry.thread_id is not None:
            lines.append(f"Thread id: {entry.thread_id}")
        if entry.reply_to_message_id is not None:
            lines.append(f"Reply to telegram message id: {entry.reply_to_message_id}")
        lines.extend(["", "Message:", entry.text.rstrip() or "(empty)", ""])
        return "\n".join(lines).rstrip() + "\n\n"

    def _render_entry_blocks(
        self,
        *,
        entries: list[NotebookLMSyncEntry],
        start_index: int,
        previous_body: str = "",
    ) -> str:
        current_day = self._last_body_day(previous_body)
        blocks: list[str] = []
        for index, entry in enumerate(entries, start=start_index):
            day = self._entry_local_datetime(entry).date().isoformat()
            include_day_heading = day != current_day
            blocks.append(
                self._render_entry_block(
                    entry=entry,
                    index=index,
                    include_day_heading=include_day_heading,
                )
            )
            current_day = day
        return "".join(blocks)

    def _render_rolling_source_markdown(
        self,
        *,
        canonical_chat_id: int,
        notebook_id: str,
        source: NotebookLMRollingSource,
        body: str,
    ) -> str:
        lines = [
            "# Telegram Context Source v2",
            "",
            "Purpose: searchable Telegram chat context for NotebookLM.",
            (
                f"Timezone: {_SOURCE_TIMEZONE_LABEL}. "
                "All timestamps in this source are rendered with +03:00 offset."
            ),
            f"Format version: {_SOURCE_FORMAT_VERSION}",
            f"Canonical chat id: {canonical_chat_id}",
            f"Notebook id: {notebook_id}",
            f"Segment: {source.segment_index}",
            f"Status: {source.status}",
            f"Entry count: {source.entry_count}",
            f"Estimated words: {source.word_count}",
        ]
        if source.started_event_date:
            lines.append(f"First entry ({_SOURCE_TIMEZONE_LABEL}): {source.started_event_date}")
        if source.last_event_date:
            lines.append(f"Last entry ({_SOURCE_TIMEZONE_LABEL}): {source.last_event_date}")
        if source.updated_at:
            lines.append(f"Updated at ({_SOURCE_TIMEZONE_LABEL}): {source.updated_at}")
        if source.finalized_at:
            lines.append(f"Finalized at ({_SOURCE_TIMEZONE_LABEL}): {source.finalized_at}")
        lines.append("Reaction note: reaction snapshots are included as separate timeline entries when available.")
        lines.extend(["", body.rstrip(), ""])
        return "\n".join(lines).rstrip() + "\n"

    async def _find_latest_message_on_or_before(
        self,
        *,
        canonical_chat_id: int,
        cutoff: datetime,
    ) -> NotebookLMSyncMessage | None:
        message = self._lightweight_history_store.get_latest_message_on_or_before(
            canonical_chat_id=canonical_chat_id,
            cutoff=cutoff,
        )
        if message is None:
            return None
        return NotebookLMSyncMessage(
            message_pk=message.message_pk,
            telegram_message_id=message.telegram_message_id,
            message_date=message.message_date,
            user_id=message.user_id,
            username=message.username,
            display_name=message.display_name,
            text=message.text,
            reply_to_message_id=message.reply_to_message_id,
            thread_id=message.thread_id,
        )

    async def _list_delta_messages(
        self,
        *,
        canonical_chat_id: int,
        watermark_date: datetime,
        watermark_message_pk: int,
        until: datetime,
    ) -> list[NotebookLMSyncMessage]:
        messages = self._lightweight_history_store.list_delta_messages(
            canonical_chat_id=canonical_chat_id,
            watermark_date=watermark_date,
            watermark_message_pk=watermark_message_pk,
            until=until,
        )
        return [
            NotebookLMSyncMessage(
                message_pk=message.message_pk,
                telegram_message_id=message.telegram_message_id,
                message_date=message.message_date,
                user_id=message.user_id,
                username=message.username,
                display_name=message.display_name,
                text=message.text,
                reply_to_message_id=message.reply_to_message_id,
                thread_id=message.thread_id,
            )
            for message in messages
        ]

    @staticmethod
    def _row_to_sync_message(row: Any) -> NotebookLMSyncMessage | None:
        if row is None:
            return None
        return NotebookLMSyncMessage(
            message_pk=int(row[0]),
            telegram_message_id=int(row[1]),
            message_date=_to_utc(row[2]),
            user_id=row[3],
            username=row[4],
            display_name=row[5],
            text=row[6],
            reply_to_message_id=row[7],
            thread_id=row[8],
        )

    @staticmethod
    def _entry_key(entry: NotebookLMSyncEntry) -> tuple[datetime, int, int]:
        return (entry.event_date, _stream_rank(entry.event_stream), entry.event_pk)

    @staticmethod
    def _messages_to_entries(messages: list[NotebookLMSyncMessage]) -> list[NotebookLMSyncEntry]:
        return [
            NotebookLMSyncEntry(
                event_pk=message.message_pk,
                event_stream=TIMELINE_STREAM,
                entry_type="message_text",
                source_telegram_message_id=message.telegram_message_id,
                event_date=message.message_date,
                user_id=message.user_id,
                username=message.username,
                display_name=message.display_name,
                text=message.text,
                reply_to_message_id=message.reply_to_message_id,
                thread_id=message.thread_id,
            )
            for message in messages
        ]

    @staticmethod
    def _timeline_event_to_entry(event: NotebookLMLightweightTimelineEvent) -> NotebookLMSyncEntry:
        return NotebookLMSyncEntry(
            event_pk=event.event_pk,
            event_stream=TIMELINE_STREAM,
            entry_type=event.event_type,
            source_telegram_message_id=event.source_telegram_message_id,
            event_date=event.event_date,
            user_id=event.user_id,
            username=event.username,
            display_name=event.display_name,
            text=event.text,
            reply_to_message_id=event.reply_to_message_id,
            thread_id=event.thread_id,
        )

    @staticmethod
    def _reaction_snapshot_to_entry(snapshot: ReactionSnapshotState) -> NotebookLMSyncEntry:
        return NotebookLMSyncEntry(
            event_pk=snapshot.event_pk,
            event_stream=REACTION_STREAM,
            entry_type="reaction_snapshot",
            source_telegram_message_id=snapshot.source_telegram_message_id,
            event_date=snapshot.last_changed_at,
            user_id=None,
            username=None,
            display_name=None,
            text=snapshot.snapshot_text,
            reply_to_message_id=snapshot.reply_to_message_id,
            thread_id=snapshot.thread_id,
        )

    def _checkpoint_watermark(
        self,
        checkpoint: NotebookLMSourceSyncCheckpoint,
    ) -> tuple[datetime, str, int]:
        if checkpoint.last_uploaded_event_date and checkpoint.last_uploaded_event_stream and checkpoint.last_uploaded_event_pk:
            return (
                _parse_datetime(checkpoint.last_uploaded_event_date),
                checkpoint.last_uploaded_event_stream,
                checkpoint.last_uploaded_event_pk,
            )
        return (
            _parse_datetime(checkpoint.last_uploaded_message_date),
            TIMELINE_STREAM,
            checkpoint.last_uploaded_message_pk,
        )

    def _build_initial_checkpoint(
        self,
        *,
        canonical_chat_id: int,
        notebook_id: str,
        entry: NotebookLMSyncEntry,
        bootstrap_cutoff_date: date,
    ) -> NotebookLMSourceSyncCheckpoint:
        now_iso = _isoformat(self._now_fn())
        return NotebookLMSourceSyncCheckpoint(
            context_key=self._state_store.context_key(canonical_chat_id=canonical_chat_id, notebook_id=notebook_id),
            canonical_chat_id=canonical_chat_id,
            notebook_id=notebook_id,
            last_uploaded_message_date=_isoformat(entry.event_date),
            last_uploaded_message_pk=entry.event_pk if entry.event_stream == TIMELINE_STREAM else 0,
            last_uploaded_telegram_message_id=entry.source_telegram_message_id,
            updated_at=now_iso,
            bootstrap_cutoff_date=bootstrap_cutoff_date.isoformat(),
            last_uploaded_event_date=_isoformat(entry.event_date),
            last_uploaded_event_stream=entry.event_stream,
            last_uploaded_event_pk=entry.event_pk,
        )

    @staticmethod
    def _active_rolling_source(checkpoint: NotebookLMSourceSyncCheckpoint) -> NotebookLMRollingSource | None:
        for source in checkpoint.rolling_sources:
            if source.status == "active":
                return source
        return None

    def _create_active_rolling_source(
        self,
        *,
        checkpoint: NotebookLMSourceSyncCheckpoint,
        canonical_chat_id: int,
        notebook_id: str,
    ) -> NotebookLMRollingSource:
        next_segment_index = max((source.segment_index for source in checkpoint.rolling_sources), default=0) + 1
        if len(checkpoint.rolling_sources) >= self._max_sources_per_notebook():
            raise NotebookLMSourceSyncError(
                f"NotebookLM source limit reached for notebook {notebook_id}: "
                f"{len(checkpoint.rolling_sources)} configured sources, max {self._max_sources_per_notebook()}."
            )
        source = NotebookLMRollingSource(
            segment_index=next_segment_index,
            status="active",
            title=self._rolling_source_title(
                canonical_chat_id=canonical_chat_id,
                notebook_id=notebook_id,
                segment_index=next_segment_index,
            ),
            local_path=str(
                self._rolling_source_path(
                    canonical_chat_id=canonical_chat_id,
                    notebook_id=notebook_id,
                    segment_index=next_segment_index,
                )
            ),
            updated_at=_isoformat(self._now_fn()),
        )
        checkpoint.rolling_sources.append(source)
        checkpoint.rolling_sources.sort(key=lambda item: item.segment_index)
        log_event(
            logger,
            logging.INFO,
            "nlm.source.rotated",
            notebook_id=notebook_id,
            canonical_chat_id=canonical_chat_id,
            segment_index=next_segment_index,
            sources_used=len(checkpoint.rolling_sources),
        )
        return source

    def _ensure_active_rolling_source(
        self,
        *,
        checkpoint: NotebookLMSourceSyncCheckpoint,
        canonical_chat_id: int,
        notebook_id: str,
    ) -> NotebookLMRollingSource:
        source = self._active_rolling_source(checkpoint)
        if source is not None:
            return source
        return self._create_active_rolling_source(
            checkpoint=checkpoint,
            canonical_chat_id=canonical_chat_id,
            notebook_id=notebook_id,
        )

    def _finalize_rolling_source(self, *, source: NotebookLMRollingSource) -> None:
        source.status = "finalized"
        source.finalized_at = _isoformat(self._now_fn())
        source.updated_at = source.finalized_at

    async def _with_notebooklm_sources(self) -> Any:
        proxy_url = get_notebooklm_proxy_url(self._settings)
        storage_path = self._runtime_store.resolve_storage_state_path()
        return await self._client_factory(
            storage_path,
            self._settings.notebooklm_timeout,
            proxy_url,
        )

    async def _replace_active_source_upload(
        self,
        *,
        notebook_id: str,
        source: NotebookLMRollingSource,
        export_path: Path,
        delete_pending: bool = True,
    ) -> str | None:
        client = await self._with_notebooklm_sources()
        async with client:
            sources = getattr(client, "sources", None)
            add_text = getattr(sources, "add_text", None)
            add_file = getattr(sources, "add_file", None)
            delete = getattr(sources, "delete", None)
            uploaded = None
            if callable(add_text) and export_path.suffix.lower() in {".md", ".markdown"}:
                uploaded = await add_text(
                    notebook_id,
                    source.title,
                    export_path.read_text(encoding="utf-8"),
                )
            elif callable(add_file):
                try:
                    uploaded = await add_file(notebook_id, str(export_path), mime_type="text/markdown")
                except (httpx.HTTPError, OSError):
                    if not callable(add_text) or export_path.suffix.lower() not in {".md", ".markdown"}:
                        raise
                    uploaded = await add_text(
                        notebook_id,
                        source.title,
                        export_path.read_text(encoding="utf-8"),
                    )
            else:
                raise NotebookLMSourceSyncError("NotebookLM client does not expose a supported source upload method.")

            new_source_id = getattr(uploaded, "id", None)
            if source.notebook_source_id and source.notebook_source_id not in source.pending_delete_source_ids:
                source.pending_delete_source_ids.append(source.notebook_source_id)
            source.notebook_source_id = str(new_source_id) if new_source_id else source.notebook_source_id
            source.updated_at = _isoformat(self._now_fn())

            if delete_pending and callable(delete) and source.pending_delete_source_ids:
                remaining: list[str] = []
                for source_id in source.pending_delete_source_ids:
                    if not source_id or source_id == source.notebook_source_id:
                        continue
                    try:
                        await delete(notebook_id, source_id)
                    except Exception:
                        remaining.append(source_id)
                source.pending_delete_source_ids = remaining
            return source.notebook_source_id

    async def _delete_pending_source_ids(
        self,
        *,
        notebook_id: str,
        sources_to_clean: list[NotebookLMRollingSource],
    ) -> None:
        pending_sources = [source for source in sources_to_clean if source.pending_delete_source_ids]
        if not pending_sources:
            return
        try:
            client = await self._with_notebooklm_sources()
            async with client:
                sources_api = getattr(client, "sources", None)
                delete = getattr(sources_api, "delete", None)
                if not callable(delete):
                    return
                for source in pending_sources:
                    remaining: list[str] = []
                    for source_id in source.pending_delete_source_ids:
                        if not source_id or source_id == source.notebook_source_id:
                            continue
                        try:
                            await delete(notebook_id, source_id)
                        except Exception:
                            remaining.append(source_id)
                    source.pending_delete_source_ids = remaining
        except Exception:
            logger.warning(
                "notebooklm.source_sync cleanup of old NotebookLM sources failed; will retry later",
                exc_info=True,
            )

    def _find_latest_exportable_entry_on_or_before(
        self,
        *,
        canonical_chat_id: int,
        cutoff: datetime,
    ) -> NotebookLMSyncEntry | None:
        self._lightweight_history_store.backfill_legacy_message_events()
        candidates: list[NotebookLMSyncEntry] = []
        if event := self._lightweight_history_store.get_latest_timeline_event_on_or_before(
            canonical_chat_id=canonical_chat_id,
            cutoff=cutoff,
        ):
            candidates.append(self._timeline_event_to_entry(event))
        if snapshot := self._lightweight_history_store.get_latest_reaction_snapshot_on_or_before(
            canonical_chat_id=canonical_chat_id,
            cutoff=cutoff,
        ):
            candidates.append(self._reaction_snapshot_to_entry(snapshot))
        if not candidates:
            return None
        candidates.sort(key=self._entry_key)
        return candidates[-1]

    def _list_lightweight_delta_entries(
        self,
        *,
        canonical_chat_id: int,
        watermark_date: datetime,
        watermark_stream: str,
        watermark_pk: int,
        until: datetime,
    ) -> list[NotebookLMSyncEntry]:
        self._lightweight_history_store.backfill_legacy_message_events()
        watermark_key = (watermark_date, _stream_rank(watermark_stream), watermark_pk)
        entries = [
            self._timeline_event_to_entry(event)
            for event in self._lightweight_history_store.list_timeline_events_between(
                canonical_chat_id=canonical_chat_id,
                since=watermark_date,
                until=until,
            )
        ]
        entries.extend(
            self._reaction_snapshot_to_entry(snapshot)
            for snapshot in self._lightweight_history_store.list_reaction_snapshots_between(
                canonical_chat_id=canonical_chat_id,
                since=watermark_date,
                until=until,
            )
        )
        entries.sort(key=self._entry_key)
        return [entry for entry in entries if self._entry_key(entry) > watermark_key]

    def _append_entries_to_rolling_source(
        self,
        *,
        canonical_chat_id: int,
        notebook_id: str,
        source: NotebookLMRollingSource,
        entries: list[NotebookLMSyncEntry],
        output_path: Path | None = None,
    ) -> Path:
        export_path = Path(source.local_path).expanduser()
        export_path.parent.mkdir(parents=True, exist_ok=True)
        write_path = output_path or export_path
        previous_body = ""
        if export_path.exists():
            previous_text = export_path.read_text(encoding="utf-8")
            previous_body = self._extract_rolling_source_body(previous_text)
        next_index = source.entry_count + 1
        appended_blocks = self._render_entry_blocks(
            entries=entries,
            start_index=next_index,
            previous_body=previous_body,
        ).strip()
        body_parts = [part for part in (previous_body, appended_blocks) if part]
        body = "\n\n".join(body_parts).strip()
        last_entry = entries[-1]
        if source.started_event_date is None:
            source.started_event_date = _isoformat(entries[0].event_date)
            source.started_event_stream = entries[0].event_stream
            source.started_event_pk = entries[0].event_pk
        source.last_event_date = _isoformat(last_entry.event_date)
        source.last_event_stream = last_entry.event_stream
        source.last_event_pk = last_entry.event_pk
        source.entry_count += len(entries)
        source.updated_at = _isoformat(self._now_fn())
        rendered = self._render_rolling_source_markdown(
            canonical_chat_id=canonical_chat_id,
            notebook_id=notebook_id,
            source=source,
            body=body,
        )
        source.word_count = _word_count(rendered)
        rendered = self._render_rolling_source_markdown(
            canonical_chat_id=canonical_chat_id,
            notebook_id=notebook_id,
            source=source,
            body=body,
        )
        write_path.parent.mkdir(parents=True, exist_ok=True)
        write_path.write_text(rendered, encoding="utf-8")
        return write_path

    def _rewrite_rolling_source_header_at_path(
        self,
        *,
        canonical_chat_id: int,
        notebook_id: str,
        source: NotebookLMRollingSource,
        export_path: Path,
    ) -> None:
        if not export_path.exists():
            return
        previous_text = export_path.read_text(encoding="utf-8")
        body = self._extract_rolling_source_body(previous_text)
        rendered = self._render_rolling_source_markdown(
            canonical_chat_id=canonical_chat_id,
            notebook_id=notebook_id,
            source=source,
            body=body,
        )
        source.word_count = _word_count(rendered)
        export_path.write_text(rendered, encoding="utf-8")

    def _rewrite_rolling_source_header(
        self,
        *,
        canonical_chat_id: int,
        notebook_id: str,
        source: NotebookLMRollingSource,
    ) -> None:
        export_path = Path(source.local_path).expanduser()
        self._rewrite_rolling_source_header_at_path(
            canonical_chat_id=canonical_chat_id,
            notebook_id=notebook_id,
            source=source,
            export_path=export_path,
        )

    async def _sync_lightweight_rolling_sources(
        self,
        *,
        checkpoint: NotebookLMSourceSyncCheckpoint,
        canonical_chat_id: int,
        notebook_id: str,
        entries: list[NotebookLMSyncEntry],
    ) -> str:
        max_words = self._max_words_per_source()
        working_checkpoint = copy.deepcopy(checkpoint)
        batches_by_segment: dict[int, list[NotebookLMSyncEntry]] = {}
        projected_words: dict[int, int] = {
            source.segment_index: source.word_count for source in working_checkpoint.rolling_sources
        }
        projected_counts: dict[int, int] = {
            source.segment_index: source.entry_count for source in working_checkpoint.rolling_sources
        }

        for entry in entries:
            while True:
                active = self._ensure_active_rolling_source(
                    checkpoint=working_checkpoint,
                    canonical_chat_id=canonical_chat_id,
                    notebook_id=notebook_id,
                )
                active_projection_words = projected_words.get(active.segment_index, active.word_count)
                active_projection_count = projected_counts.get(active.segment_index, active.entry_count)
                entry_block = self._render_entry_block(entry=entry, index=active_projection_count + 1)
                entry_words = _word_count(entry_block)
                if active_projection_count > 0 and active_projection_words + entry_words > max_words:
                    self._finalize_rolling_source(source=active)
                    working_checkpoint.updated_at = _isoformat(self._now_fn())
                    continue
                batches_by_segment.setdefault(active.segment_index, []).append(entry)
                projected_words[active.segment_index] = active_projection_words + entry_words
                projected_counts[active.segment_index] = active_projection_count + 1
                break

        last_export_path: str | None = None
        staged_exports: list[tuple[Path, Path]] = []
        try:
            for source in working_checkpoint.rolling_sources:
                segment_entries = batches_by_segment.get(source.segment_index)
                if not segment_entries:
                    continue
                final_export_path = Path(source.local_path).expanduser()
                staged_export_path = final_export_path.with_name(
                    f".{final_export_path.name}.upload-{os.getpid()}-{source.segment_index}.tmp"
                )
                export_path = self._append_entries_to_rolling_source(
                    canonical_chat_id=canonical_chat_id,
                    notebook_id=notebook_id,
                    source=source,
                    entries=segment_entries,
                    output_path=staged_export_path,
                )
                staged_exports.append((export_path, final_export_path))
                uploaded_source_id = await self._replace_active_source_upload(
                    notebook_id=notebook_id,
                    source=source,
                    export_path=export_path,
                    delete_pending=False,
                )
                if uploaded_source_id:
                    source.notebook_source_id = uploaded_source_id
                working_checkpoint.updated_at = _isoformat(self._now_fn())
                self._rewrite_rolling_source_header_at_path(
                    canonical_chat_id=canonical_chat_id,
                    notebook_id=notebook_id,
                    source=source,
                    export_path=export_path,
                )
                last_export_path = str(final_export_path)

            for staged_export_path, final_export_path in staged_exports:
                final_export_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staged_export_path, final_export_path)
        except Exception:
            for staged_export_path, _final_export_path in staged_exports:
                try:
                    staged_export_path.unlink(missing_ok=True)
                except OSError:
                    logger.warning(
                        "notebooklm.source_sync failed to discard staged export path=%s",
                        staged_export_path,
                    )
            raise

        checkpoint.rolling_sources = working_checkpoint.rolling_sources
        checkpoint.updated_at = working_checkpoint.updated_at
        await self._delete_pending_source_ids(
            notebook_id=notebook_id,
            sources_to_clean=checkpoint.rolling_sources,
        )

        active = self._active_rolling_source(checkpoint)
        if active is not None:
            last_export_path = last_export_path or active.local_path
        if not last_export_path:
            raise NotebookLMSourceSyncError("Rolling NotebookLM sync produced no export path.")
        return last_export_path

    def _render_markdown(
        self,
        *,
        canonical_chat_id: int,
        notebook_id: str,
        watermark_before: datetime,
        entries: list[NotebookLMSyncEntry],
        exported_at: datetime,
    ) -> tuple[Path, str]:
        export_dir = self._export_dir()
        export_dir.mkdir(parents=True, exist_ok=True)
        timestamp = exported_at.strftime("%Y%m%dT%H%M%SZ")
        path = export_dir / f"chat-{canonical_chat_id}-notebook-{notebook_id}-{timestamp}.md"

        lines = [
            "# Telegram Context Delta Export v2",
            "",
            "Purpose: searchable Telegram chat context delta for NotebookLM.",
            (
                f"Timezone: {_SOURCE_TIMEZONE_LABEL}. "
                "All timestamps in this source are rendered with +03:00 offset."
            ),
            f"Format version: {_SOURCE_FORMAT_VERSION}",
            f"Exported at ({_SOURCE_TIMEZONE_LABEL}): {_isoformat(exported_at)}",
            f"Canonical chat id: {canonical_chat_id}",
            f"Notebook id: {notebook_id}",
            f"Previous watermark ({_SOURCE_TIMEZONE_LABEL}): {_isoformat(watermark_before)}",
            f"Entry count: {len(entries)}",
            "",
        ]
        lines.append(self._render_entry_blocks(entries=entries, start_index=1).rstrip())

        rendered = "\n".join(lines).rstrip() + "\n"
        path.write_text(rendered, encoding="utf-8")
        return path, rendered

    async def _upload_markdown(self, *, notebook_id: str, export_path: Path) -> None:
        proxy_url = get_notebooklm_proxy_url(self._settings)
        storage_path = self._runtime_store.resolve_storage_state_path()
        client = await self._client_factory(
            storage_path,
            self._settings.notebooklm_timeout,
            proxy_url,
        )

        async with client:
            sources = getattr(client, "sources", None)
            add_text = getattr(sources, "add_text", None)
            add_file = getattr(sources, "add_file", None)
            if callable(add_text) and export_path.suffix.lower() in {".md", ".markdown"}:
                await add_text(
                    notebook_id,
                    export_path.name,
                    export_path.read_text(encoding="utf-8"),
                )
                return
            if not callable(add_file):
                raise NotebookLMSourceSyncError("NotebookLM client does not expose sources.add_file().")
            try:
                await add_file(notebook_id, str(export_path), mime_type="text/markdown")
                return
            except (httpx.HTTPError, OSError):
                if not callable(add_text) or export_path.suffix.lower() not in {".md", ".markdown"}:
                    raise
                await add_text(
                    notebook_id,
                    export_path.name,
                    export_path.read_text(encoding="utf-8"),
                )

    async def sync_chat_delta(self, *, chat_id: int) -> NotebookLMSourceSyncResult:
        started_at = self._now_fn()
        notebook_id = self._runtime_store.resolve_notebook_id(chat_id)
        if not notebook_id:
            raise NotebookLMSourceSyncError("No NotebookLM notebook configured for this chat.")

        checkpoint = self._state_store.get_checkpoint(canonical_chat_id=chat_id, notebook_id=notebook_id)
        initial_words = self._total_words(checkpoint)
        initial_source_count = len(checkpoint.rolling_sources) if checkpoint else 0
        initial_active = self._active_rolling_source(checkpoint) if checkpoint else None
        initial_active_source = initial_active.notebook_source_id if initial_active else None
        bootstrap_created = False
        try:
            if checkpoint is None:
                bootstrap_cutoff = self._bootstrap_cutoff_date()
                bootstrap_entry = self._find_latest_exportable_entry_on_or_before(
                    canonical_chat_id=chat_id,
                    cutoff=datetime.combine(bootstrap_cutoff, time.max, tzinfo=timezone.utc),
                )
                if bootstrap_entry is None:
                    raise NotebookLMSourceSyncError(
                        "Cannot initialize NotebookLM sync watermark: "
                        f"no local exportable events found on or before {bootstrap_cutoff.isoformat()} for chat {chat_id}."
                    )
                checkpoint = self._build_initial_checkpoint(
                    canonical_chat_id=chat_id,
                    notebook_id=notebook_id,
                    entry=bootstrap_entry,
                    bootstrap_cutoff_date=bootstrap_cutoff,
                )
                self._state_store.save_checkpoint(checkpoint)
                bootstrap_created = True

            watermark_before, watermark_stream, watermark_pk = self._checkpoint_watermark(checkpoint)
            now_utc = _to_utc(self._now_fn())
            entries = self._list_lightweight_delta_entries(
                canonical_chat_id=chat_id,
                watermark_date=watermark_before,
                watermark_stream=watermark_stream,
                watermark_pk=watermark_pk,
                until=now_utc,
            )

            if not entries:
                result = NotebookLMSourceSyncResult(
                    status="noop",
                    canonical_chat_id=chat_id,
                    notebook_id=notebook_id,
                    message_count=0,
                    watermark_before=_isoformat(watermark_before),
                    watermark_after=_isoformat(watermark_before),
                    export_path=None,
                    bootstrap_created=bootstrap_created,
                )
                await self._append_sync_tick(
                    chat_id=chat_id,
                    notebook_id=notebook_id,
                    events_appended=0,
                    words_before=initial_words,
                    words_after=self._total_words(checkpoint),
                    active_source_id=(
                        self._active_rolling_source(checkpoint).notebook_source_id
                        if checkpoint and self._active_rolling_source(checkpoint)
                        else None
                    ),
                    rotated=False,
                    duration_ms=int((self._now_fn() - started_at).total_seconds() * 1000),
                    error=None,
                )
                set_sources_used(notebook_id, len(checkpoint.rolling_sources))
                return result

            export_path = Path(
                await self._sync_lightweight_rolling_sources(
                    checkpoint=checkpoint,
                    canonical_chat_id=chat_id,
                    notebook_id=notebook_id,
                    entries=entries,
                )
            )

            last_entry = entries[-1]
            updated_checkpoint = NotebookLMSourceSyncCheckpoint(
                context_key=checkpoint.context_key,
                canonical_chat_id=checkpoint.canonical_chat_id,
                notebook_id=checkpoint.notebook_id,
                last_uploaded_message_date=(
                    _isoformat(last_entry.event_date)
                    if last_entry.event_stream == TIMELINE_STREAM
                    else checkpoint.last_uploaded_message_date
                ),
                last_uploaded_message_pk=(
                    last_entry.event_pk if last_entry.event_stream == TIMELINE_STREAM else checkpoint.last_uploaded_message_pk
                ),
                last_uploaded_telegram_message_id=(
                    last_entry.source_telegram_message_id
                    if last_entry.event_stream == TIMELINE_STREAM
                    else checkpoint.last_uploaded_telegram_message_id
                ),
                updated_at=_isoformat(now_utc),
                bootstrap_cutoff_date=checkpoint.bootstrap_cutoff_date,
                last_export_path=str(export_path),
                last_uploaded_event_date=_isoformat(last_entry.event_date),
                last_uploaded_event_stream=last_entry.event_stream,
                last_uploaded_event_pk=last_entry.event_pk,
                last_budget_alert_at=checkpoint.last_budget_alert_at,
                rolling_sources=checkpoint.rolling_sources,
            )
            updated_checkpoint = await self._maybe_notify_source_budget(updated_checkpoint)
            self._state_store.save_checkpoint(updated_checkpoint)
            set_sources_used(notebook_id, len(updated_checkpoint.rolling_sources))
            await self._append_sync_tick(
                chat_id=chat_id,
                notebook_id=notebook_id,
                events_appended=len(entries),
                words_before=initial_words,
                words_after=self._total_words(updated_checkpoint),
                active_source_id=(
                    self._active_rolling_source(updated_checkpoint).notebook_source_id
                    if self._active_rolling_source(updated_checkpoint)
                    else None
                ),
                rotated=len(updated_checkpoint.rolling_sources) > initial_source_count,
                duration_ms=int((self._now_fn() - started_at).total_seconds() * 1000),
                error=None,
            )

            return NotebookLMSourceSyncResult(
                status="updated",
                canonical_chat_id=chat_id,
                notebook_id=notebook_id,
                message_count=len(entries),
                watermark_before=_isoformat(watermark_before),
                watermark_after=_isoformat(last_entry.event_date),
                export_path=str(export_path),
                bootstrap_created=bootstrap_created,
            )
        except Exception as exc:
            current_checkpoint = self._state_store.get_checkpoint(canonical_chat_id=chat_id, notebook_id=notebook_id)
            await self._append_sync_tick(
                chat_id=chat_id,
                notebook_id=notebook_id,
                events_appended=0,
                words_before=initial_words,
                words_after=self._total_words(current_checkpoint),
                active_source_id=initial_active_source,
                rotated=False,
                duration_ms=int((self._now_fn() - started_at).total_seconds() * 1000),
                error=str(exc),
            )
            raise
