"""Lightweight SQLite-backed message, event, and reaction state for NotebookLM."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from app.core.config import get_settings
from app.core.notebooklm_time import notebooklm_isoformat, parse_timestamp

_DEFAULT_LIGHTWEIGHT_HISTORY_PATH = ".state/notebooklm/history.sqlite3"
TIMELINE_STREAM = "timeline"
REACTION_STREAM = "reaction_snapshot"
_TEXT_EVENT_TYPES = {"message_text", "message_edit"}


def _to_utc(value: datetime) -> datetime:
    return parse_timestamp(value.isoformat())


def _isoformat(value: datetime) -> str:
    return notebooklm_isoformat(value)


def _parse_datetime(value: str) -> datetime:
    return parse_timestamp(value)


def _normalize_snapshot(snapshot: dict[str, int]) -> dict[str, int]:
    normalized: dict[str, int] = {}
    for key, value in sorted(snapshot.items()):
        count = int(value or 0)
        if count > 0:
            normalized[str(key)] = count
    return normalized


def _snapshot_hash(snapshot: dict[str, int]) -> str:
    payload = json.dumps(_normalize_snapshot(snapshot), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _display_actor_name(
    *,
    actor_type: str,
    actor_user_id: int | None,
    actor_chat_id: int | None,
    username: str | None,
    display_name: str | None,
) -> str:
    base = (display_name or "").strip()
    handle = (username or "").strip()
    if handle and base and handle != base:
        return f"{base} (@{handle})"
    if handle:
        return f"@{handle}"
    if base:
        return base
    if actor_type == "chat" and actor_chat_id is not None:
        return f"chat{actor_chat_id}"
    if actor_user_id is not None:
        return f"user{actor_user_id}"
    return "unknown actor"


def _normalize_actor_reactions(reactions: dict[str, int]) -> dict[str, int]:
    return _normalize_snapshot(reactions)


def _aggregate_actor_reactions(actor_states: Iterable["ReactionActorState"]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for actor in actor_states:
        counter.update(_normalize_actor_reactions(actor.reactions))
    return {label: count for label, count in counter.items() if count > 0}


def render_reaction_snapshot_text(
    *,
    source_telegram_message_id: int,
    snapshot: dict[str, int],
    changed_at: datetime,
    actor_states: Iterable["ReactionActorState"] = (),
) -> str:
    normalized = _normalize_snapshot(snapshot)
    summary = ", ".join(f"{label} x{count}" for label, count in normalized.items()) if normalized else "(none)"
    lines = [
        f"Current reactions for message {source_telegram_message_id} "
        f"as of {_isoformat(changed_at)}: {summary}"
    ]
    known_actor_states = [actor for actor in actor_states if _normalize_actor_reactions(actor.reactions)]
    if not known_actor_states:
        return lines[0]

    lines.extend(["", "Known public reaction authors:"])
    for actor in known_actor_states:
        actor_summary = ", ".join(
            f"{label} x{count}" for label, count in _normalize_actor_reactions(actor.reactions).items()
        )
        lines.append(
            f"- {_display_actor_name(**actor.identity_kwargs())}: {actor_summary or '(none)'}"
        )

    public_totals = _aggregate_actor_reactions(known_actor_states)
    remaining: dict[str, int] = {}
    for label, count in normalized.items():
        leftover = count - int(public_totals.get(label, 0))
        if leftover > 0:
            remaining[label] = leftover
    if remaining:
        remaining_summary = ", ".join(f"{label} x{count}" for label, count in remaining.items())
        lines.extend(
            [
                "",
                "Additional count-only or anonymous reactions still present:",
                f"- {remaining_summary}",
            ]
        )
    return "\n".join(lines)


@dataclass(slots=True)
class NotebookLMLightweightHistoryMessage:
    message_pk: int
    canonical_chat_id: int
    live_chat_id: int
    telegram_message_id: int
    message_date: datetime
    user_id: int | None
    username: str | None
    display_name: str | None
    text: str
    reply_to_message_id: int | None
    thread_id: int | None
    edited: bool
    chat_title: str | None
    chat_type: str | None
    chat_username: str | None


@dataclass(slots=True)
class NotebookLMLightweightChatSummary:
    canonical_chat_id: int
    title: str
    message_count: int


@dataclass(slots=True)
class NotebookLMLightweightTimelineEvent:
    event_pk: int
    canonical_chat_id: int
    live_chat_id: int
    event_type: str
    source_telegram_message_id: int
    event_date: datetime
    user_id: int | None
    username: str | None
    display_name: str | None
    text: str
    reply_to_message_id: int | None
    thread_id: int | None
    media_kind: str | None
    file_id: str | None
    file_unique_id: str | None
    thumbnail_file_id: str | None


@dataclass(slots=True)
class ReactionSnapshotState:
    event_pk: int
    canonical_chat_id: int
    source_telegram_message_id: int
    reply_to_message_id: int | None
    thread_id: int | None
    snapshot: dict[str, int]
    snapshot_text: str
    snapshot_hash: str
    last_changed_at: datetime
    snapshot_origin: str = "actor"


@dataclass(slots=True)
class ReactionActorState:
    actor_pk: int
    canonical_chat_id: int
    source_telegram_message_id: int
    actor_key: str
    actor_type: str
    actor_user_id: int | None
    actor_chat_id: int | None
    username: str | None
    display_name: str | None
    reactions: dict[str, int]
    last_changed_at: datetime

    def identity_kwargs(self) -> dict[str, str | int | None]:
        return {
            "actor_type": self.actor_type,
            "actor_user_id": self.actor_user_id,
            "actor_chat_id": self.actor_chat_id,
            "username": self.username,
            "display_name": self.display_name,
        }


@dataclass(slots=True)
class MediaContextJob:
    job_pk: int
    canonical_chat_id: int
    source_telegram_message_id: int
    media_kind: str
    file_id: str
    file_unique_id: str | None
    thumbnail_file_id: str | None
    status: str
    attempt_count: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class NotebookLMLightweightHistoryStore:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _path(self) -> Path:
        configured = getattr(
            self._settings,
            "notebooklm_lightweight_history_path",
            _DEFAULT_LIGHTWEIGHT_HISTORY_PATH,
        )
        return Path(str(configured or _DEFAULT_LIGHTWEIGHT_HISTORY_PATH)).expanduser()

    def _connect(self) -> sqlite3.Connection:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        self._ensure_schema(conn)
        return conn

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notebooklm_lightweight_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_chat_id INTEGER NOT NULL,
                live_chat_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                user_id INTEGER,
                username TEXT,
                display_name TEXT,
                text TEXT NOT NULL,
                message_date TEXT NOT NULL,
                reply_to_message_id INTEGER,
                thread_id INTEGER,
                edited INTEGER NOT NULL DEFAULT 0,
                chat_title TEXT,
                chat_type TEXT,
                chat_username TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(canonical_chat_id, telegram_message_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notebooklm_lightweight_messages_chat_date
            ON notebooklm_lightweight_messages (canonical_chat_id, message_date, id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notebooklm_lightweight_timeline_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_chat_id INTEGER NOT NULL,
                live_chat_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                source_telegram_message_id INTEGER NOT NULL,
                event_date TEXT NOT NULL,
                user_id INTEGER,
                username TEXT,
                display_name TEXT,
                text TEXT NOT NULL,
                reply_to_message_id INTEGER,
                thread_id INTEGER,
                media_kind TEXT,
                file_id TEXT,
                file_unique_id TEXT,
                thumbnail_file_id TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notebooklm_lightweight_timeline_chat_date
            ON notebooklm_lightweight_timeline_events (canonical_chat_id, event_date, id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notebooklm_lightweight_reaction_snapshots (
                event_pk INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_chat_id INTEGER NOT NULL,
                source_telegram_message_id INTEGER NOT NULL,
                reply_to_message_id INTEGER,
                thread_id INTEGER,
                snapshot_json TEXT NOT NULL,
                snapshot_text TEXT NOT NULL,
                hash TEXT NOT NULL,
                snapshot_origin TEXT NOT NULL DEFAULT 'actor',
                last_changed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(canonical_chat_id, source_telegram_message_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notebooklm_lightweight_reactions_chat_date
            ON notebooklm_lightweight_reaction_snapshots (canonical_chat_id, last_changed_at, event_pk)
            """
        )
        columns = {
            str(row["name"] or "")
            for row in conn.execute("PRAGMA table_info(notebooklm_lightweight_reaction_snapshots)").fetchall()
        }
        if "snapshot_origin" not in columns:
            conn.execute(
                """
                ALTER TABLE notebooklm_lightweight_reaction_snapshots
                ADD COLUMN snapshot_origin TEXT NOT NULL DEFAULT 'actor'
                """
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notebooklm_lightweight_reaction_actors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_chat_id INTEGER NOT NULL,
                source_telegram_message_id INTEGER NOT NULL,
                actor_key TEXT NOT NULL,
                actor_type TEXT NOT NULL,
                actor_user_id INTEGER,
                actor_chat_id INTEGER,
                username TEXT,
                display_name TEXT,
                reactions_json TEXT NOT NULL,
                last_changed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(canonical_chat_id, source_telegram_message_id, actor_key)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notebooklm_lightweight_reaction_actors_chat_message
            ON notebooklm_lightweight_reaction_actors (canonical_chat_id, source_telegram_message_id, actor_key)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notebooklm_lightweight_media_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_chat_id INTEGER NOT NULL,
                source_telegram_message_id INTEGER NOT NULL,
                media_kind TEXT NOT NULL,
                file_id TEXT NOT NULL,
                file_unique_id TEXT,
                thumbnail_file_id TEXT,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(canonical_chat_id, source_telegram_message_id, media_kind)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notebooklm_lightweight_media_jobs_status
            ON notebooklm_lightweight_media_jobs (status, updated_at, id)
            """
        )
        conn.commit()

    @staticmethod
    def _row_to_message(row: sqlite3.Row | None) -> NotebookLMLightweightHistoryMessage | None:
        if row is None:
            return None
        return NotebookLMLightweightHistoryMessage(
            message_pk=int(row["id"]),
            canonical_chat_id=int(row["canonical_chat_id"]),
            live_chat_id=int(row["live_chat_id"]),
            telegram_message_id=int(row["telegram_message_id"]),
            message_date=_parse_datetime(str(row["message_date"])),
            user_id=int(row["user_id"]) if row["user_id"] is not None else None,
            username=row["username"],
            display_name=row["display_name"],
            text=str(row["text"] or ""),
            reply_to_message_id=int(row["reply_to_message_id"]) if row["reply_to_message_id"] is not None else None,
            thread_id=int(row["thread_id"]) if row["thread_id"] is not None else None,
            edited=bool(row["edited"]),
            chat_title=row["chat_title"],
            chat_type=row["chat_type"],
            chat_username=row["chat_username"],
        )

    @staticmethod
    def _row_to_timeline_event(row: sqlite3.Row | None) -> NotebookLMLightweightTimelineEvent | None:
        if row is None:
            return None
        return NotebookLMLightweightTimelineEvent(
            event_pk=int(row["id"]),
            canonical_chat_id=int(row["canonical_chat_id"]),
            live_chat_id=int(row["live_chat_id"]),
            event_type=str(row["event_type"]),
            source_telegram_message_id=int(row["source_telegram_message_id"]),
            event_date=_parse_datetime(str(row["event_date"])),
            user_id=int(row["user_id"]) if row["user_id"] is not None else None,
            username=row["username"],
            display_name=row["display_name"],
            text=str(row["text"] or ""),
            reply_to_message_id=int(row["reply_to_message_id"]) if row["reply_to_message_id"] is not None else None,
            thread_id=int(row["thread_id"]) if row["thread_id"] is not None else None,
            media_kind=row["media_kind"],
            file_id=row["file_id"],
            file_unique_id=row["file_unique_id"],
            thumbnail_file_id=row["thumbnail_file_id"],
        )

    @staticmethod
    def _row_to_reaction_snapshot(row: sqlite3.Row | None) -> ReactionSnapshotState | None:
        if row is None:
            return None
        raw_snapshot = json.loads(str(row["snapshot_json"] or "{}"))
        snapshot = raw_snapshot if isinstance(raw_snapshot, dict) else {}
        return ReactionSnapshotState(
            event_pk=int(row["event_pk"]),
            canonical_chat_id=int(row["canonical_chat_id"]),
            source_telegram_message_id=int(row["source_telegram_message_id"]),
            reply_to_message_id=int(row["reply_to_message_id"]) if row["reply_to_message_id"] is not None else None,
            thread_id=int(row["thread_id"]) if row["thread_id"] is not None else None,
            snapshot={str(key): int(value) for key, value in snapshot.items()},
            snapshot_text=str(row["snapshot_text"] or ""),
            snapshot_hash=str(row["hash"] or ""),
            last_changed_at=_parse_datetime(str(row["last_changed_at"])),
            snapshot_origin=str(row["snapshot_origin"] or "actor"),
        )

    @staticmethod
    def _row_to_reaction_actor(row: sqlite3.Row | None) -> ReactionActorState | None:
        if row is None:
            return None
        raw_reactions = json.loads(str(row["reactions_json"] or "{}"))
        reactions = raw_reactions if isinstance(raw_reactions, dict) else {}
        return ReactionActorState(
            actor_pk=int(row["id"]),
            canonical_chat_id=int(row["canonical_chat_id"]),
            source_telegram_message_id=int(row["source_telegram_message_id"]),
            actor_key=str(row["actor_key"] or ""),
            actor_type=str(row["actor_type"] or "user"),
            actor_user_id=int(row["actor_user_id"]) if row["actor_user_id"] is not None else None,
            actor_chat_id=int(row["actor_chat_id"]) if row["actor_chat_id"] is not None else None,
            username=row["username"],
            display_name=row["display_name"],
            reactions={str(key): int(value) for key, value in reactions.items() if int(value or 0) > 0},
            last_changed_at=_parse_datetime(str(row["last_changed_at"])),
        )

    @staticmethod
    def _row_to_media_job(row: sqlite3.Row | None) -> MediaContextJob | None:
        if row is None:
            return None
        return MediaContextJob(
            job_pk=int(row["id"]),
            canonical_chat_id=int(row["canonical_chat_id"]),
            source_telegram_message_id=int(row["source_telegram_message_id"]),
            media_kind=str(row["media_kind"]),
            file_id=str(row["file_id"]),
            file_unique_id=row["file_unique_id"],
            thumbnail_file_id=row["thumbnail_file_id"],
            status=str(row["status"]),
            attempt_count=int(row["attempt_count"]),
            last_error=row["last_error"],
            created_at=_parse_datetime(str(row["created_at"])),
            updated_at=_parse_datetime(str(row["updated_at"])),
        )

    def upsert_message(
        self,
        *,
        canonical_chat_id: int,
        live_chat_id: int,
        chat_title: str | None,
        chat_type: str | None,
        chat_username: str | None,
        telegram_message_id: int,
        user_id: int | None,
        username: str | None,
        display_name: str | None,
        text: str,
        message_date: datetime,
        reply_to_message_id: int | None,
        thread_id: int | None,
        edited: bool = False,
    ) -> NotebookLMLightweightHistoryMessage:
        now_iso = _isoformat(datetime.now(timezone.utc))
        message_date_iso = _isoformat(message_date)
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO notebooklm_lightweight_messages (
                    canonical_chat_id,
                    live_chat_id,
                    telegram_message_id,
                    user_id,
                    username,
                    display_name,
                    text,
                    message_date,
                    reply_to_message_id,
                    thread_id,
                    edited,
                    chat_title,
                    chat_type,
                    chat_username,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_chat_id, telegram_message_id) DO UPDATE SET
                    live_chat_id = excluded.live_chat_id,
                    user_id = COALESCE(excluded.user_id, notebooklm_lightweight_messages.user_id),
                    username = COALESCE(excluded.username, notebooklm_lightweight_messages.username),
                    display_name = COALESCE(excluded.display_name, notebooklm_lightweight_messages.display_name),
                    text = excluded.text,
                    message_date = excluded.message_date,
                    reply_to_message_id = COALESCE(
                        excluded.reply_to_message_id,
                        notebooklm_lightweight_messages.reply_to_message_id
                    ),
                    thread_id = COALESCE(excluded.thread_id, notebooklm_lightweight_messages.thread_id),
                    edited = CASE
                        WHEN excluded.edited = 1 OR notebooklm_lightweight_messages.edited = 1 THEN 1
                        ELSE 0
                    END,
                    chat_title = COALESCE(excluded.chat_title, notebooklm_lightweight_messages.chat_title),
                    chat_type = COALESCE(excluded.chat_type, notebooklm_lightweight_messages.chat_type),
                    chat_username = COALESCE(excluded.chat_username, notebooklm_lightweight_messages.chat_username),
                    updated_at = excluded.updated_at
                """,
                (
                    canonical_chat_id,
                    live_chat_id,
                    telegram_message_id,
                    user_id,
                    username,
                    display_name,
                    text,
                    message_date_iso,
                    reply_to_message_id,
                    thread_id,
                    1 if edited else 0,
                    chat_title,
                    chat_type,
                    chat_username,
                    now_iso,
                    now_iso,
                ),
            )
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_messages
                WHERE canonical_chat_id = ? AND telegram_message_id = ?
                """,
                (canonical_chat_id, telegram_message_id),
            ).fetchone()
        message = self._row_to_message(row)
        if message is None:
            raise RuntimeError("Failed to persist lightweight NotebookLM history row.")
        return message

    def get_message(
        self,
        *,
        canonical_chat_id: int,
        telegram_message_id: int,
    ) -> NotebookLMLightweightHistoryMessage | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_messages
                WHERE canonical_chat_id = ? AND telegram_message_id = ?
                """,
                (canonical_chat_id, telegram_message_id),
            ).fetchone()
        return self._row_to_message(row)

    def append_timeline_event(
        self,
        *,
        canonical_chat_id: int,
        live_chat_id: int,
        event_type: str,
        source_telegram_message_id: int,
        event_date: datetime,
        text: str,
        user_id: int | None,
        username: str | None,
        display_name: str | None,
        reply_to_message_id: int | None,
        thread_id: int | None,
        media_kind: str | None = None,
        file_id: str | None = None,
        file_unique_id: str | None = None,
        thumbnail_file_id: str | None = None,
    ) -> NotebookLMLightweightTimelineEvent:
        now_iso = _isoformat(datetime.now(timezone.utc))
        with closing(self._connect()) as conn, conn:
            cursor = conn.execute(
                """
                INSERT INTO notebooklm_lightweight_timeline_events (
                    canonical_chat_id,
                    live_chat_id,
                    event_type,
                    source_telegram_message_id,
                    event_date,
                    user_id,
                    username,
                    display_name,
                    text,
                    reply_to_message_id,
                    thread_id,
                    media_kind,
                    file_id,
                    file_unique_id,
                    thumbnail_file_id,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    canonical_chat_id,
                    live_chat_id,
                    event_type,
                    source_telegram_message_id,
                    _isoformat(event_date),
                    user_id,
                    username,
                    display_name,
                    text,
                    reply_to_message_id,
                    thread_id,
                    media_kind,
                    file_id,
                    file_unique_id,
                    thumbnail_file_id,
                    now_iso,
                ),
            )
            row = conn.execute(
                "SELECT * FROM notebooklm_lightweight_timeline_events WHERE id = ?",
                (cursor.lastrowid,),
            ).fetchone()
        event = self._row_to_timeline_event(row)
        if event is None:
            raise RuntimeError("Failed to persist timeline event.")
        return event

    def backfill_legacy_message_events(self) -> int:
        created = 0
        with closing(self._connect()) as conn, conn:
            rows = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_messages
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM notebooklm_lightweight_timeline_events AS events
                    WHERE
                        events.canonical_chat_id = notebooklm_lightweight_messages.canonical_chat_id
                        AND events.source_telegram_message_id = notebooklm_lightweight_messages.telegram_message_id
                        AND events.event_type IN ('message_text', 'message_edit')
                )
                ORDER BY julianday(message_date) ASC, id ASC
                """
            ).fetchall()
            for row in rows:
                message = self._row_to_message(row)
                if message is None:
                    continue
                conn.execute(
                    """
                    INSERT INTO notebooklm_lightweight_timeline_events (
                        canonical_chat_id,
                        live_chat_id,
                        event_type,
                        source_telegram_message_id,
                        event_date,
                        user_id,
                        username,
                        display_name,
                        text,
                        reply_to_message_id,
                        thread_id,
                        media_kind,
                        file_id,
                        file_unique_id,
                        thumbnail_file_id,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
                    """,
                    (
                        message.canonical_chat_id,
                        message.live_chat_id,
                        "message_edit" if message.edited else "message_text",
                        message.telegram_message_id,
                        _isoformat(message.message_date),
                        message.user_id,
                        message.username,
                        message.display_name,
                        message.text,
                        message.reply_to_message_id,
                        message.thread_id,
                        _isoformat(datetime.now(timezone.utc)),
                    ),
                )
                created += 1
        return created

    def get_latest_timeline_event_on_or_before(
        self,
        *,
        canonical_chat_id: int,
        cutoff: datetime,
    ) -> NotebookLMLightweightTimelineEvent | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_timeline_events
                WHERE canonical_chat_id = ? AND julianday(event_date) <= julianday(?)
                ORDER BY julianday(event_date) DESC, id DESC
                LIMIT 1
                """,
                (canonical_chat_id, _isoformat(cutoff)),
            ).fetchone()
        return self._row_to_timeline_event(row)

    def list_timeline_events_between(
        self,
        *,
        canonical_chat_id: int,
        since: datetime,
        until: datetime,
    ) -> list[NotebookLMLightweightTimelineEvent]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_timeline_events
                WHERE
                    canonical_chat_id = ?
                    AND julianday(event_date) >= julianday(?)
                    AND julianday(event_date) <= julianday(?)
                ORDER BY julianday(event_date) ASC, id ASC
                """,
                (
                    canonical_chat_id,
                    _isoformat(since),
                    _isoformat(until),
                ),
            ).fetchall()
        return [event for row in rows if (event := self._row_to_timeline_event(row)) is not None]

    def get_reaction_snapshot(
        self,
        *,
        canonical_chat_id: int,
        source_telegram_message_id: int,
    ) -> ReactionSnapshotState | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_reaction_snapshots
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ?
                """,
                (canonical_chat_id, source_telegram_message_id),
            ).fetchone()
        return self._row_to_reaction_snapshot(row)

    def list_reaction_actors(
        self,
        *,
        canonical_chat_id: int,
        source_telegram_message_id: int,
    ) -> list[ReactionActorState]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_reaction_actors
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ?
                ORDER BY COALESCE(display_name, username, actor_key) ASC, id ASC
                """,
                (canonical_chat_id, source_telegram_message_id),
            ).fetchall()
        return [actor for row in rows if (actor := self._row_to_reaction_actor(row)) is not None]

    def get_latest_reaction_snapshot_on_or_before(
        self,
        *,
        canonical_chat_id: int,
        cutoff: datetime,
    ) -> ReactionSnapshotState | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_reaction_snapshots
                WHERE canonical_chat_id = ? AND julianday(last_changed_at) <= julianday(?)
                ORDER BY julianday(last_changed_at) DESC, event_pk DESC
                LIMIT 1
                """,
                (canonical_chat_id, _isoformat(cutoff)),
            ).fetchone()
        return self._row_to_reaction_snapshot(row)

    @staticmethod
    def compute_reaction_snapshot(
        *,
        current_snapshot: dict[str, int] | None,
        old_labels: list[str],
        new_labels: list[str],
    ) -> dict[str, int]:
        counter = Counter(_normalize_snapshot(current_snapshot or {}))
        for label in old_labels:
            if not label:
                continue
            counter[str(label)] -= 1
            if counter[str(label)] <= 0:
                counter.pop(str(label), None)
        for label in new_labels:
            if not label:
                continue
            counter[str(label)] += 1
        return {label: count for label, count in counter.items() if count > 0}

    @staticmethod
    def compute_reaction_snapshot_for_actor_replacement(
        *,
        current_snapshot: dict[str, int] | None,
        previous_actor_reactions: dict[str, int] | None,
        new_actor_reactions: dict[str, int] | None,
    ) -> dict[str, int]:
        counter = Counter(_normalize_snapshot(current_snapshot or {}))
        for label, count in _normalize_actor_reactions(previous_actor_reactions or {}).items():
            counter[str(label)] -= int(count)
            if counter[str(label)] <= 0:
                counter.pop(str(label), None)
        for label, count in _normalize_actor_reactions(new_actor_reactions or {}).items():
            counter[str(label)] += int(count)
        return {label: count for label, count in counter.items() if count > 0}

    def upsert_reaction_snapshot(
        self,
        *,
        canonical_chat_id: int,
        source_telegram_message_id: int,
        snapshot: dict[str, int],
        changed_at: datetime,
        reply_to_message_id: int | None = None,
        thread_id: int | None = None,
        snapshot_origin: str = "actor",
    ) -> tuple[ReactionSnapshotState, bool]:
        normalized = _normalize_snapshot(snapshot)
        actor_states = self.list_reaction_actors(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=source_telegram_message_id,
        )
        snapshot_hash = _snapshot_hash(normalized)
        snapshot_text = render_reaction_snapshot_text(
            source_telegram_message_id=source_telegram_message_id,
            snapshot=normalized,
            changed_at=changed_at,
            actor_states=actor_states,
        )
        snapshot_json = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
        changed_at_iso = _isoformat(changed_at)
        now_iso = _isoformat(datetime.now(timezone.utc))
        with closing(self._connect()) as conn, conn:
            existing = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_reaction_snapshots
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ?
                """,
                (canonical_chat_id, source_telegram_message_id),
            ).fetchone()
            if existing is None:
                cursor = conn.execute(
                    """
                    INSERT INTO notebooklm_lightweight_reaction_snapshots (
                        canonical_chat_id,
                        source_telegram_message_id,
                        reply_to_message_id,
                        thread_id,
                        snapshot_json,
                        snapshot_text,
                        hash,
                        snapshot_origin,
                        last_changed_at,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        canonical_chat_id,
                        source_telegram_message_id,
                        reply_to_message_id,
                        thread_id,
                        snapshot_json,
                        snapshot_text,
                        snapshot_hash,
                        snapshot_origin,
                        changed_at_iso,
                        now_iso,
                        now_iso,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM notebooklm_lightweight_reaction_snapshots WHERE event_pk = ?",
                    (cursor.lastrowid,),
                ).fetchone()
                state = self._row_to_reaction_snapshot(row)
                if state is None:
                    raise RuntimeError("Failed to persist reaction snapshot.")
                return state, True

            existing_hash = str(existing["hash"] or "")
            existing_text = str(existing["snapshot_text"] or "")
            existing_origin = str(existing["snapshot_origin"] or "actor")
            existing_changed_at = str(existing["last_changed_at"])
            comparable_snapshot_text = render_reaction_snapshot_text(
                source_telegram_message_id=source_telegram_message_id,
                snapshot=normalized,
                changed_at=_parse_datetime(existing_changed_at),
                actor_states=actor_states,
            )
            changed = (
                existing_hash != snapshot_hash
                or existing_text != comparable_snapshot_text
                or existing_origin != snapshot_origin
            )
            conn.execute(
                """
                UPDATE notebooklm_lightweight_reaction_snapshots
                SET
                    reply_to_message_id = COALESCE(?, reply_to_message_id),
                    thread_id = COALESCE(?, thread_id),
                    snapshot_json = ?,
                    snapshot_text = ?,
                    hash = ?,
                    snapshot_origin = ?,
                    last_changed_at = ?,
                    updated_at = ?
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ?
                """,
                (
                    reply_to_message_id,
                    thread_id,
                    snapshot_json,
                    snapshot_text,
                    snapshot_hash,
                    snapshot_origin,
                    changed_at_iso if changed else existing_changed_at,
                    now_iso,
                    canonical_chat_id,
                    source_telegram_message_id,
                ),
            )
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_reaction_snapshots
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ?
                """,
                (canonical_chat_id, source_telegram_message_id),
            ).fetchone()
        state = self._row_to_reaction_snapshot(row)
        if state is None:
            raise RuntimeError("Failed to load reaction snapshot after update.")
        return state, changed

    def apply_reaction_actor_delta(
        self,
        *,
        canonical_chat_id: int,
        source_telegram_message_id: int,
        actor_type: str,
        actor_user_id: int | None,
        actor_chat_id: int | None,
        username: str | None,
        display_name: str | None,
        old_labels: list[str],
        new_labels: list[str],
        changed_at: datetime,
        reply_to_message_id: int | None = None,
        thread_id: int | None = None,
    ) -> tuple[ReactionSnapshotState, bool]:
        actor_key = (
            f"user:{int(actor_user_id)}"
            if actor_type == "user" and actor_user_id is not None
            else f"chat:{int(actor_chat_id)}"
            if actor_type == "chat" and actor_chat_id is not None
            else ""
        )
        if not actor_key:
            return self.apply_reaction_delta(
                canonical_chat_id=canonical_chat_id,
                source_telegram_message_id=source_telegram_message_id,
                old_labels=old_labels,
                new_labels=new_labels,
                changed_at=changed_at,
                reply_to_message_id=reply_to_message_id,
                thread_id=thread_id,
            )

        new_actor_reactions = self.compute_reaction_snapshot(
            current_snapshot=None,
            old_labels=[],
            new_labels=new_labels,
        )
        with closing(self._connect()) as conn, conn:
            actor_row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_reaction_actors
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ? AND actor_key = ?
                """,
                (canonical_chat_id, source_telegram_message_id, actor_key),
            ).fetchone()
            existing_actor = self._row_to_reaction_actor(actor_row)
            existing_snapshot = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_reaction_snapshots
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ?
                """,
                (canonical_chat_id, source_telegram_message_id),
            ).fetchone()
            snapshot_state = self._row_to_reaction_snapshot(existing_snapshot)
            if snapshot_state and snapshot_state.snapshot_origin == "count":
                merged_snapshot = snapshot_state.snapshot
                snapshot_origin = "count"
            else:
                merged_snapshot = self.compute_reaction_snapshot_for_actor_replacement(
                    current_snapshot=snapshot_state.snapshot if snapshot_state else None,
                    previous_actor_reactions=existing_actor.reactions if existing_actor else None,
                    new_actor_reactions=new_actor_reactions,
                )
                snapshot_origin = "actor"
            now_iso = _isoformat(datetime.now(timezone.utc))
            if new_actor_reactions:
                conn.execute(
                    """
                    INSERT INTO notebooklm_lightweight_reaction_actors (
                        canonical_chat_id,
                        source_telegram_message_id,
                        actor_key,
                        actor_type,
                        actor_user_id,
                        actor_chat_id,
                        username,
                        display_name,
                        reactions_json,
                        last_changed_at,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(canonical_chat_id, source_telegram_message_id, actor_key) DO UPDATE SET
                        actor_type = excluded.actor_type,
                        actor_user_id = COALESCE(excluded.actor_user_id, notebooklm_lightweight_reaction_actors.actor_user_id),
                        actor_chat_id = COALESCE(excluded.actor_chat_id, notebooklm_lightweight_reaction_actors.actor_chat_id),
                        username = COALESCE(excluded.username, notebooklm_lightweight_reaction_actors.username),
                        display_name = COALESCE(excluded.display_name, notebooklm_lightweight_reaction_actors.display_name),
                        reactions_json = excluded.reactions_json,
                        last_changed_at = excluded.last_changed_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        canonical_chat_id,
                        source_telegram_message_id,
                        actor_key,
                        actor_type,
                        actor_user_id,
                        actor_chat_id,
                        username,
                        display_name,
                        json.dumps(new_actor_reactions, ensure_ascii=False, sort_keys=True),
                        _isoformat(changed_at),
                        now_iso,
                        now_iso,
                    ),
                )
            else:
                conn.execute(
                    """
                    DELETE FROM notebooklm_lightweight_reaction_actors
                    WHERE canonical_chat_id = ? AND source_telegram_message_id = ? AND actor_key = ?
                    """,
                    (canonical_chat_id, source_telegram_message_id, actor_key),
                )

        existing = self.get_reaction_snapshot(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=source_telegram_message_id,
        )
        return self.upsert_reaction_snapshot(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=source_telegram_message_id,
            snapshot=merged_snapshot,
            changed_at=changed_at,
            reply_to_message_id=reply_to_message_id if reply_to_message_id is not None else (existing.reply_to_message_id if existing else None),
            thread_id=thread_id if thread_id is not None else (existing.thread_id if existing else None),
            snapshot_origin=snapshot_origin,
        )

    def apply_reaction_delta(
        self,
        *,
        canonical_chat_id: int,
        source_telegram_message_id: int,
        old_labels: list[str],
        new_labels: list[str],
        changed_at: datetime,
        reply_to_message_id: int | None = None,
        thread_id: int | None = None,
    ) -> tuple[ReactionSnapshotState, bool]:
        existing = self.get_reaction_snapshot(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=source_telegram_message_id,
        )
        snapshot = self.compute_reaction_snapshot(
            current_snapshot=existing.snapshot if existing else None,
            old_labels=old_labels,
            new_labels=new_labels,
        )
        return self.upsert_reaction_snapshot(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=source_telegram_message_id,
            snapshot=snapshot,
            changed_at=changed_at,
            reply_to_message_id=reply_to_message_id if reply_to_message_id is not None else (existing.reply_to_message_id if existing else None),
            thread_id=thread_id if thread_id is not None else (existing.thread_id if existing else None),
            snapshot_origin="actor",
        )

    def list_reaction_snapshots_between(
        self,
        *,
        canonical_chat_id: int,
        since: datetime,
        until: datetime,
    ) -> list[ReactionSnapshotState]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_reaction_snapshots
                WHERE
                    canonical_chat_id = ?
                    AND julianday(last_changed_at) >= julianday(?)
                    AND julianday(last_changed_at) <= julianday(?)
                ORDER BY julianday(last_changed_at) ASC, event_pk ASC
                """,
                (
                    canonical_chat_id,
                    _isoformat(since),
                    _isoformat(until),
                ),
            ).fetchall()
        return [state for row in rows if (state := self._row_to_reaction_snapshot(row)) is not None]

    def upsert_media_job(
        self,
        *,
        canonical_chat_id: int,
        source_telegram_message_id: int,
        media_kind: str,
        file_id: str,
        file_unique_id: str | None,
        thumbnail_file_id: str | None,
        status: str = "pending",
    ) -> MediaContextJob:
        now_iso = _isoformat(datetime.now(timezone.utc))
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO notebooklm_lightweight_media_jobs (
                    canonical_chat_id,
                    source_telegram_message_id,
                    media_kind,
                    file_id,
                    file_unique_id,
                    thumbnail_file_id,
                    status,
                    attempt_count,
                    last_error,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?)
                ON CONFLICT(canonical_chat_id, source_telegram_message_id, media_kind) DO UPDATE SET
                    file_id = excluded.file_id,
                    file_unique_id = COALESCE(excluded.file_unique_id, notebooklm_lightweight_media_jobs.file_unique_id),
                    thumbnail_file_id = COALESCE(
                        excluded.thumbnail_file_id,
                        notebooklm_lightweight_media_jobs.thumbnail_file_id
                    ),
                    status = excluded.status,
                    updated_at = excluded.updated_at
                """,
                (
                    canonical_chat_id,
                    source_telegram_message_id,
                    media_kind,
                    file_id,
                    file_unique_id,
                    thumbnail_file_id,
                    status,
                    now_iso,
                    now_iso,
                ),
            )
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_media_jobs
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ? AND media_kind = ?
                """,
                (canonical_chat_id, source_telegram_message_id, media_kind),
            ).fetchone()
        job = self._row_to_media_job(row)
        if job is None:
            raise RuntimeError("Failed to persist media job.")
        return job

    def create_media_job(
        self,
        *,
        canonical_chat_id: int,
        source_telegram_message_id: int,
        media_kind: str,
        file_id: str,
        file_unique_id: str | None,
        thumbnail_file_id: str | None,
    ) -> MediaContextJob:
        return self.upsert_media_job(
            canonical_chat_id=canonical_chat_id,
            source_telegram_message_id=source_telegram_message_id,
            media_kind=media_kind,
            file_id=file_id,
            file_unique_id=file_unique_id,
            thumbnail_file_id=thumbnail_file_id,
            status="pending",
        )

    def get_media_job(
        self,
        *,
        canonical_chat_id: int,
        source_telegram_message_id: int,
        media_kind: str,
    ) -> MediaContextJob | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_media_jobs
                WHERE canonical_chat_id = ? AND source_telegram_message_id = ? AND media_kind = ?
                """,
                (canonical_chat_id, source_telegram_message_id, media_kind),
            ).fetchone()
        return self._row_to_media_job(row)

    def mark_media_job_running(self, *, job_pk: int) -> MediaContextJob:
        now_iso = _isoformat(datetime.now(timezone.utc))
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                UPDATE notebooklm_lightweight_media_jobs
                SET
                    status = 'running',
                    attempt_count = attempt_count + 1,
                    last_error = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now_iso, job_pk),
            )
            row = conn.execute(
                "SELECT * FROM notebooklm_lightweight_media_jobs WHERE id = ?",
                (job_pk,),
            ).fetchone()
        job = self._row_to_media_job(row)
        if job is None:
            raise RuntimeError("Failed to mark media job as running.")
        return job

    def mark_media_job_completed(self, *, job_pk: int) -> MediaContextJob:
        return self._update_media_job_status(job_pk=job_pk, status="completed", last_error=None)

    def mark_media_job_retryable(self, *, job_pk: int, error: str) -> MediaContextJob:
        return self._update_media_job_status(job_pk=job_pk, status="retryable", last_error=error[:2000])

    def mark_media_job_failed(self, *, job_pk: int, error: str) -> MediaContextJob:
        return self._update_media_job_status(job_pk=job_pk, status="failed", last_error=error[:2000])

    def _update_media_job_status(self, *, job_pk: int, status: str, last_error: str | None) -> MediaContextJob:
        now_iso = _isoformat(datetime.now(timezone.utc))
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                UPDATE notebooklm_lightweight_media_jobs
                SET
                    status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, last_error, now_iso, job_pk),
            )
            row = conn.execute(
                "SELECT * FROM notebooklm_lightweight_media_jobs WHERE id = ?",
                (job_pk,),
            ).fetchone()
        job = self._row_to_media_job(row)
        if job is None:
            raise RuntimeError("Failed to update media job.")
        return job

    def get_latest_message_on_or_before(
        self,
        *,
        canonical_chat_id: int,
        cutoff: datetime,
    ) -> NotebookLMLightweightHistoryMessage | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_messages
                WHERE canonical_chat_id = ? AND julianday(message_date) <= julianday(?)
                ORDER BY julianday(message_date) DESC, id DESC
                LIMIT 1
                """,
                (canonical_chat_id, _isoformat(cutoff)),
            ).fetchone()
        return self._row_to_message(row)

    def list_delta_messages(
        self,
        *,
        canonical_chat_id: int,
        watermark_date: datetime,
        watermark_message_pk: int,
        until: datetime,
    ) -> list[NotebookLMLightweightHistoryMessage]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_messages
                WHERE
                    canonical_chat_id = ?
                    AND julianday(message_date) <= julianday(?)
                    AND (
                        julianday(message_date) > julianday(?)
                        OR (julianday(message_date) = julianday(?) AND id > ?)
                    )
                ORDER BY julianday(message_date) ASC, id ASC
                """,
                (
                    canonical_chat_id,
                    _isoformat(until),
                    _isoformat(watermark_date),
                    _isoformat(watermark_date),
                    watermark_message_pk,
                ),
            ).fetchall()
        return [message for row in rows if (message := self._row_to_message(row)) is not None]

    def list_messages_on_or_after(
        self,
        *,
        canonical_chat_id: int,
        watermark_date: datetime,
        until: datetime,
    ) -> list[NotebookLMLightweightHistoryMessage]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM notebooklm_lightweight_messages
                WHERE
                    canonical_chat_id = ?
                    AND julianday(message_date) >= julianday(?)
                    AND julianday(message_date) <= julianday(?)
                ORDER BY julianday(message_date) ASC, id ASC
                """,
                (
                    canonical_chat_id,
                    _isoformat(watermark_date),
                    _isoformat(until),
                ),
            ).fetchall()
        return [message for row in rows if (message := self._row_to_message(row)) is not None]

    def list_chat_summaries(self) -> list[NotebookLMLightweightChatSummary]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT
                    base.canonical_chat_id,
                    COALESCE(
                        (
                            SELECT latest.chat_title
                            FROM notebooklm_lightweight_messages AS latest
                            WHERE
                                latest.canonical_chat_id = base.canonical_chat_id
                                AND latest.chat_title IS NOT NULL
                                AND latest.chat_title != ''
                            ORDER BY julianday(latest.message_date) DESC, latest.id DESC
                            LIMIT 1
                        ),
                        'Chat ' || base.canonical_chat_id
                    ) AS title,
                    COUNT(*) AS message_count
                FROM notebooklm_lightweight_messages AS base
                GROUP BY base.canonical_chat_id
                ORDER BY COUNT(*) DESC, base.canonical_chat_id ASC
                """
            ).fetchall()
        return [
            NotebookLMLightweightChatSummary(
                canonical_chat_id=int(row["canonical_chat_id"]),
                title=str(row["title"]),
                message_count=int(row["message_count"]),
            )
            for row in rows
        ]
