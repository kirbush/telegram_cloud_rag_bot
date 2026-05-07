"""SQLite-backed owner inbox for bot question turns."""

from __future__ import annotations

import os
import stat
import sqlite3
from contextlib import closing, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from app.core.config import get_settings
from app.core.notebooklm_time import notebooklm_isoformat, parse_timestamp

DEFAULT_CONVERSATION_STATE_PATH = ".state/bot/conversations.sqlite3"
_CONTAINER_APP_USER = "app"


def _now() -> datetime:
    return datetime.now(UTC)


def _isoformat(value: datetime) -> str:
    return notebooklm_isoformat(value)


def _parse(value: str) -> datetime:
    return parse_timestamp(value)


def _prepare_sqlite_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    targets = [(path.parent, stat.S_IRWXU | stat.S_IRWXG | stat.S_IXOTH | stat.S_IROTH)]
    if path.exists():
        targets.append((path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH))
    for target, minimum_mode in targets:
        with suppress(OSError):
            target.chmod(stat.S_IMODE(target.stat().st_mode) | minimum_mode)
    if os.name == "posix" and hasattr(os, "geteuid") and os.geteuid() == 0:
        try:
            import pwd

            app_user = pwd.getpwnam(_CONTAINER_APP_USER)
        except (ImportError, KeyError, OSError):
            return
        for target in (path.parent, path):
            if target.exists():
                with suppress(OSError):
                    os.chown(target, app_user.pw_uid, app_user.pw_gid)


@dataclass(slots=True)
class ConversationTurn:
    turn_id: int
    created_at: datetime
    updated_at: datetime
    status: str
    source: str
    telegram_user_id: int
    username: str | None
    display_name: str | None
    chat_id: int
    chat_type: str | None
    chat_title: str | None
    message_id: int | None
    thread_id: int | None
    question_key: str | None
    question_text: str
    answer_text: str | None
    error_text: str | None
    reason: str | None
    latency_ms: int | None
    notebook_id: str | None


@dataclass(slots=True)
class ConversationUserSummary:
    telegram_user_id: int
    username: str | None
    display_name: str | None
    last_seen_at: datetime
    turn_count: int
    last_status: str | None
    last_question: str | None


class BotConversationStore:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _path(self) -> Path:
        configured = getattr(self._settings, "bot_conversation_state_path", DEFAULT_CONVERSATION_STATE_PATH)
        return Path(str(configured or DEFAULT_CONVERSATION_STATE_PATH)).expanduser()

    def _connect(self) -> sqlite3.Connection:
        path = self._path()
        _prepare_sqlite_path(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        self._ensure_schema(conn)
        _prepare_sqlite_path(path)
        return conn

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_conversation_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                status TEXT NOT NULL,
                source TEXT NOT NULL,
                telegram_user_id INTEGER NOT NULL,
                username TEXT,
                display_name TEXT,
                chat_id INTEGER NOT NULL,
                chat_type TEXT,
                chat_title TEXT,
                message_id INTEGER,
                thread_id INTEGER,
                question_key TEXT,
                question_text TEXT NOT NULL,
                answer_text TEXT,
                error_text TEXT,
                reason TEXT,
                latency_ms INTEGER,
                notebook_id TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bot_conversation_turns_created
            ON bot_conversation_turns (created_at DESC, id DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bot_conversation_turns_user_created
            ON bot_conversation_turns (telegram_user_id, created_at DESC, id DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bot_conversation_turns_status_created
            ON bot_conversation_turns (status, created_at DESC, id DESC)
            """
        )

    @staticmethod
    def _row_to_turn(row: sqlite3.Row | None) -> ConversationTurn | None:
        if row is None:
            return None
        return ConversationTurn(
            turn_id=int(row["id"]),
            created_at=_parse(str(row["created_at"])),
            updated_at=_parse(str(row["updated_at"])),
            status=str(row["status"]),
            source=str(row["source"]),
            telegram_user_id=int(row["telegram_user_id"]),
            username=row["username"],
            display_name=row["display_name"],
            chat_id=int(row["chat_id"]),
            chat_type=row["chat_type"],
            chat_title=row["chat_title"],
            message_id=int(row["message_id"]) if row["message_id"] is not None else None,
            thread_id=int(row["thread_id"]) if row["thread_id"] is not None else None,
            question_key=row["question_key"],
            question_text=str(row["question_text"]),
            answer_text=row["answer_text"],
            error_text=row["error_text"],
            reason=row["reason"],
            latency_ms=int(row["latency_ms"]) if row["latency_ms"] is not None else None,
            notebook_id=row["notebook_id"],
        )

    @staticmethod
    def _row_to_user_summary(row: sqlite3.Row | None) -> ConversationUserSummary | None:
        if row is None:
            return None
        return ConversationUserSummary(
            telegram_user_id=int(row["telegram_user_id"]),
            username=row["username"],
            display_name=row["display_name"],
            last_seen_at=_parse(str(row["last_seen_at"])),
            turn_count=int(row["turn_count"]),
            last_status=row["last_status"],
            last_question=row["last_question"],
        )

    def record_question(
        self,
        *,
        source: str,
        telegram_user_id: int,
        username: str | None,
        display_name: str | None,
        chat_id: int,
        chat_type: str | None,
        chat_title: str | None,
        message_id: int | None,
        thread_id: int | None,
        question_key: str | None,
        question_text: str,
        at: datetime | None = None,
    ) -> ConversationTurn:
        now_iso = _isoformat(at or _now())
        with closing(self._connect()) as conn, conn:
            cursor = conn.execute(
                """
                INSERT INTO bot_conversation_turns (
                    created_at,
                    updated_at,
                    status,
                    source,
                    telegram_user_id,
                    username,
                    display_name,
                    chat_id,
                    chat_type,
                    chat_title,
                    message_id,
                    thread_id,
                    question_key,
                    question_text
                ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_iso,
                    now_iso,
                    str(source),
                    int(telegram_user_id),
                    username,
                    display_name,
                    int(chat_id),
                    chat_type,
                    chat_title,
                    int(message_id) if message_id is not None else None,
                    int(thread_id) if thread_id is not None else None,
                    question_key,
                    question_text,
                ),
            )
            row = conn.execute(
                "SELECT * FROM bot_conversation_turns WHERE id = ?",
                (int(cursor.lastrowid),),
            ).fetchone()
        turn = self._row_to_turn(row)
        if turn is None:
            raise RuntimeError("Failed to persist bot conversation turn.")
        return turn

    def update_turn(
        self,
        *,
        turn_id: int,
        status: str,
        answer_text: str | None = None,
        error_text: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
        notebook_id: str | None = None,
        at: datetime | None = None,
    ) -> ConversationTurn | None:
        now_iso = _isoformat(at or _now())
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                UPDATE bot_conversation_turns
                SET updated_at = ?,
                    status = ?,
                    answer_text = COALESCE(?, answer_text),
                    error_text = COALESCE(?, error_text),
                    reason = COALESCE(?, reason),
                    latency_ms = COALESCE(?, latency_ms),
                    notebook_id = COALESCE(?, notebook_id)
                WHERE id = ?
                """,
                (
                    now_iso,
                    str(status),
                    answer_text,
                    error_text,
                    reason,
                    int(latency_ms) if latency_ms is not None else None,
                    notebook_id,
                    int(turn_id),
                ),
            )
            row = conn.execute(
                "SELECT * FROM bot_conversation_turns WHERE id = ?",
                (int(turn_id),),
            ).fetchone()
        return self._row_to_turn(row)

    def list_recent_turns(self, *, limit: int = 10) -> list[ConversationTurn]:
        effective_limit = min(50, max(1, int(limit)))
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT * FROM bot_conversation_turns
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (effective_limit,),
            ).fetchall()
        return [turn for row in rows if (turn := self._row_to_turn(row)) is not None]

    def list_user_history(self, *, telegram_user_id: int, limit: int = 10) -> list[ConversationTurn]:
        effective_limit = min(50, max(1, int(limit)))
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT * FROM bot_conversation_turns
                WHERE telegram_user_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (int(telegram_user_id), effective_limit),
            ).fetchall()
        return [turn for row in rows if (turn := self._row_to_turn(row)) is not None]

    def list_recent_users(self, *, limit: int = 10) -> list[ConversationUserSummary]:
        effective_limit = min(50, max(1, int(limit)))
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY telegram_user_id
                            ORDER BY datetime(created_at) DESC, id DESC
                        ) AS rn,
                        COUNT(*) OVER (PARTITION BY telegram_user_id) AS turn_count
                    FROM bot_conversation_turns
                )
                SELECT
                    telegram_user_id,
                    username,
                    display_name,
                    created_at AS last_seen_at,
                    turn_count,
                    status AS last_status,
                    question_text AS last_question
                FROM ranked
                WHERE rn = 1
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (effective_limit,),
            ).fetchall()
        return [summary for row in rows if (summary := self._row_to_user_summary(row)) is not None]

    def status(self) -> dict[str, object]:
        path = self._path()
        with closing(self._connect()) as conn:
            totals = conn.execute(
                """
                SELECT
                    COUNT(*) AS turn_count,
                    SUM(CASE WHEN status = 'answered' THEN 1 ELSE 0 END) AS answered_count,
                    SUM(CASE WHEN status = 'denied' THEN 1 ELSE 0 END) AS denied_count,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
                    COUNT(DISTINCT telegram_user_id) AS user_count
                FROM bot_conversation_turns
                """
            ).fetchone()
        return {
            "state_path": str(path),
            "turn_count": int(totals["turn_count"] if totals else 0),
            "answered_count": int(totals["answered_count"] if totals and totals["answered_count"] is not None else 0),
            "denied_count": int(totals["denied_count"] if totals and totals["denied_count"] is not None else 0),
            "failed_count": int(totals["failed_count"] if totals and totals["failed_count"] is not None else 0),
            "user_count": int(totals["user_count"] if totals and totals["user_count"] is not None else 0),
        }
