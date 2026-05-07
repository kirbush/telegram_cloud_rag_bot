"""SQLite-backed question access, credits, and Telegram Stars payment state."""

from __future__ import annotations

import json
import os
import stat
import sqlite3
import uuid
from contextlib import closing, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.core.config import get_settings
from app.core.notebooklm_time import notebooklm_isoformat, parse_timestamp

DEFAULT_ACCESS_STATE_PATH = ".state/bot/access.sqlite3"
DEFAULT_ACCESS_ENABLED = False
DEFAULT_FREE_QUESTIONS_PER_24H = 20
DEFAULT_STARS_PRICE = 25
DEFAULT_CREDITS_PER_PURCHASE = 10
ROLLING_WINDOW_HOURS = 24
STARS_CURRENCY = "XTR"
ORDER_PAYLOAD_PREFIX = "access:"
_CONTAINER_APP_USER = "app"


def _now() -> datetime:
    return datetime.now(UTC)


def _isoformat(value: datetime) -> str:
    return notebooklm_isoformat(value)


def _parse(value: str) -> datetime:
    return parse_timestamp(value)


def _cooperative_sqlite_mode(path: Path) -> None:
    """Keep root-initialized Access DBs writable by the non-root bot process."""
    targets = [(path.parent, stat.S_IRWXU | stat.S_IRWXG | stat.S_IXOTH | stat.S_IROTH)]
    if path.exists():
        targets.append((path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH))
    for target, minimum_mode in targets:
        with suppress(OSError):
            current_mode = stat.S_IMODE(target.stat().st_mode)
            target.chmod(current_mode | minimum_mode)


def _chown_to_container_app_user(path: Path) -> None:
    if os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() != 0:
        return
    try:
        import pwd

        app_user = pwd.getpwnam(_CONTAINER_APP_USER)
    except (ImportError, KeyError, OSError):
        return
    for target in (path.parent, path):
        if target.exists():
            with suppress(OSError):
                os.chown(target, app_user.pw_uid, app_user.pw_gid)


def _prepare_sqlite_access_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _cooperative_sqlite_mode(path)
    _chown_to_container_app_user(path)


@dataclass(slots=True)
class AccessConfig:
    enabled: bool
    free_questions_per_24h: int
    stars_price: int
    credits_per_purchase: int


@dataclass(slots=True)
class AccessCheck:
    allowed: bool
    reason: str
    chat_id: int
    telegram_user_id: int
    free_limit: int
    used_in_window: int
    free_remaining: int
    manual_credits: int
    paid_credits: int
    next_reset_at: datetime | None

    @property
    def total_remaining(self) -> int:
        return self.free_remaining + self.manual_credits + self.paid_credits


@dataclass(slots=True)
class AccessConsumption:
    check: AccessCheck
    source: str


@dataclass(slots=True)
class StarsOrder:
    order_id: str
    payload: str
    telegram_user_id: int
    chat_id: int
    credits: int
    stars_amount: int
    currency: str


class BotAccessStore:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _path(self) -> Path:
        configured = getattr(self._settings, "bot_access_state_path", DEFAULT_ACCESS_STATE_PATH)
        return Path(str(configured or DEFAULT_ACCESS_STATE_PATH)).expanduser()

    def _connect(self) -> sqlite3.Connection:
        path = self._path()
        _prepare_sqlite_access_path(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        self._ensure_schema(conn)
        _prepare_sqlite_access_path(path)
        return conn

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS access_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS access_chat_overrides (
                chat_id INTEGER PRIMARY KEY,
                enabled INTEGER,
                free_questions_per_24h INTEGER,
                stars_price INTEGER,
                credits_per_purchase INTEGER,
                updated_at TEXT NOT NULL
            )
            """
        )
        override_columns = {
            str(row["name"] or "")
            for row in conn.execute("PRAGMA table_info(access_chat_overrides)").fetchall()
        }
        if "enabled" not in override_columns:
            conn.execute("ALTER TABLE access_chat_overrides ADD COLUMN enabled INTEGER")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS access_credit_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                credit_type TEXT NOT NULL,
                delta INTEGER NOT NULL,
                reason TEXT,
                order_id TEXT,
                payment_id INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_access_credit_ledger_user_chat
            ON access_credit_ledger (telegram_user_id, chat_id, credit_type, id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS access_question_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                source TEXT NOT NULL,
                question_key TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_access_question_usage_window
            ON access_question_usage (telegram_user_id, chat_id, created_at, id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS access_orders (
                order_id TEXT PRIMARY KEY,
                telegram_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                credits INTEGER NOT NULL,
                stars_amount INTEGER NOT NULL,
                currency TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS access_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                telegram_user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                credits INTEGER NOT NULL,
                stars_amount INTEGER NOT NULL,
                currency TEXT NOT NULL,
                telegram_payment_charge_id TEXT,
                provider_payment_charge_id TEXT,
                created_at TEXT NOT NULL,
                raw_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_access_payments_tg_charge
            ON access_payments (telegram_payment_charge_id)
            WHERE telegram_payment_charge_id IS NOT NULL AND telegram_payment_charge_id != ''
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_access_payments_provider_charge
            ON access_payments (provider_payment_charge_id)
            WHERE provider_payment_charge_id IS NOT NULL AND provider_payment_charge_id != ''
            """
        )
        conn.commit()

    def _global_config_locked(self, conn: sqlite3.Connection) -> AccessConfig:
        values = {
            str(row["key"]): str(row["value"])
            for row in conn.execute("SELECT key, value FROM access_settings").fetchall()
        }
        return AccessConfig(
            enabled=str(values.get("enabled", "true" if DEFAULT_ACCESS_ENABLED else "false")).lower()
            in {"1", "true", "yes", "on"},
            free_questions_per_24h=max(
                0,
                int(values.get("free_questions_per_24h", DEFAULT_FREE_QUESTIONS_PER_24H)),
            ),
            stars_price=max(1, int(values.get("stars_price", DEFAULT_STARS_PRICE))),
            credits_per_purchase=max(
                1,
                int(values.get("credits_per_purchase", DEFAULT_CREDITS_PER_PURCHASE)),
            ),
        )

    def get_global_config(self) -> AccessConfig:
        with closing(self._connect()) as conn:
            return self._global_config_locked(conn)

    def set_global_config(
        self,
        *,
        enabled: bool | None = None,
        free_questions_per_24h: int | None = None,
        stars_price: int | None = None,
        credits_per_purchase: int | None = None,
    ) -> AccessConfig:
        current = self.get_global_config()
        next_config = AccessConfig(
            enabled=current.enabled if enabled is None else bool(enabled),
            free_questions_per_24h=max(
                0,
                int(current.free_questions_per_24h if free_questions_per_24h is None else free_questions_per_24h),
            ),
            stars_price=max(1, int(current.stars_price if stars_price is None else stars_price)),
            credits_per_purchase=max(
                1,
                int(current.credits_per_purchase if credits_per_purchase is None else credits_per_purchase),
            ),
        )
        now_iso = _isoformat(_now())
        with closing(self._connect()) as conn, conn:
            for key, value in (
                ("enabled", "true" if next_config.enabled else "false"),
                ("free_questions_per_24h", next_config.free_questions_per_24h),
                ("stars_price", next_config.stars_price),
                ("credits_per_purchase", next_config.credits_per_purchase),
            ):
                conn.execute(
                    """
                    INSERT INTO access_settings (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (key, str(value), now_iso),
                )
        return next_config

    def set_chat_override(
        self,
        *,
        chat_id: int,
        enabled: bool | None = None,
        free_questions_per_24h: int | None = None,
        stars_price: int | None = None,
        credits_per_purchase: int | None = None,
    ) -> AccessConfig:
        now_iso = _isoformat(_now())
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO access_chat_overrides (
                    chat_id,
                    enabled,
                    free_questions_per_24h,
                    stars_price,
                    credits_per_purchase,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    free_questions_per_24h = excluded.free_questions_per_24h,
                    stars_price = excluded.stars_price,
                    credits_per_purchase = excluded.credits_per_purchase,
                    updated_at = excluded.updated_at
                """,
                (
                    int(chat_id),
                    None if enabled is None else int(bool(enabled)),
                    None if free_questions_per_24h is None else max(0, int(free_questions_per_24h)),
                    None if stars_price is None else max(1, int(stars_price)),
                    None if credits_per_purchase is None else max(1, int(credits_per_purchase)),
                    now_iso,
                ),
            )
            return self._effective_config_locked(conn, int(chat_id))

    def clear_chat_override(self, *, chat_id: int) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute("DELETE FROM access_chat_overrides WHERE chat_id = ?", (int(chat_id),))

    def _effective_config_locked(self, conn: sqlite3.Connection, chat_id: int) -> AccessConfig:
        global_config = self._global_config_locked(conn)
        row = conn.execute(
            "SELECT * FROM access_chat_overrides WHERE chat_id = ?",
            (int(chat_id),),
        ).fetchone()
        if row is None:
            return global_config
        return AccessConfig(
            enabled=(
                bool(int(row["enabled"]))
                if row["enabled"] is not None
                else global_config.enabled
            ),
            free_questions_per_24h=(
                int(row["free_questions_per_24h"])
                if row["free_questions_per_24h"] is not None
                else global_config.free_questions_per_24h
            ),
            stars_price=(
                int(row["stars_price"]) if row["stars_price"] is not None else global_config.stars_price
            ),
            credits_per_purchase=(
                int(row["credits_per_purchase"])
                if row["credits_per_purchase"] is not None
                else global_config.credits_per_purchase
            ),
        )

    def get_effective_config(self, *, chat_id: int) -> AccessConfig:
        with closing(self._connect()) as conn:
            return self._effective_config_locked(conn, int(chat_id))

    @staticmethod
    def _ledger_balance_locked(
        conn: sqlite3.Connection,
        *,
        telegram_user_id: int,
        chat_id: int,
        credit_type: str,
    ) -> int:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(delta), 0) AS balance
            FROM access_credit_ledger
            WHERE telegram_user_id = ? AND chat_id = ? AND credit_type = ?
            """,
            (int(telegram_user_id), int(chat_id), credit_type),
        ).fetchone()
        return max(0, int(row["balance"] if row else 0))

    def _used_in_window_locked(
        self,
        conn: sqlite3.Connection,
        *,
        telegram_user_id: int,
        chat_id: int,
        at: datetime,
    ) -> tuple[int, datetime | None]:
        since = at - timedelta(hours=ROLLING_WINDOW_HOURS)
        rows = conn.execute(
            """
            SELECT created_at
            FROM access_question_usage
            WHERE telegram_user_id = ?
              AND chat_id = ?
              AND source = 'free'
              AND julianday(created_at) > julianday(?)
              AND julianday(created_at) <= julianday(?)
            ORDER BY julianday(created_at) ASC, id ASC
            """,
            (int(telegram_user_id), int(chat_id), _isoformat(since), _isoformat(at)),
        ).fetchall()
        if not rows:
            return 0, None
        oldest = _parse(str(rows[0]["created_at"]))
        return len(rows), oldest + timedelta(hours=ROLLING_WINDOW_HOURS)

    def check_question(
        self,
        *,
        telegram_user_id: int,
        chat_id: int,
        at: datetime | None = None,
    ) -> AccessCheck:
        effective_at = at or _now()
        with closing(self._connect()) as conn:
            config = self._effective_config_locked(conn, int(chat_id))
            used, next_reset_at = self._used_in_window_locked(
                conn,
                telegram_user_id=int(telegram_user_id),
                chat_id=int(chat_id),
                at=effective_at,
            )
            manual = self._ledger_balance_locked(
                conn,
                telegram_user_id=int(telegram_user_id),
                chat_id=int(chat_id),
                credit_type="manual",
            )
            paid = self._ledger_balance_locked(
                conn,
                telegram_user_id=int(telegram_user_id),
                chat_id=int(chat_id),
                credit_type="paid",
            )
            if not config.enabled:
                return AccessCheck(
                    allowed=True,
                    reason="disabled",
                    chat_id=int(chat_id),
                    telegram_user_id=int(telegram_user_id),
                    free_limit=config.free_questions_per_24h,
                    used_in_window=used,
                    free_remaining=max(0, config.free_questions_per_24h - used),
                    manual_credits=manual,
                    paid_credits=paid,
                    next_reset_at=next_reset_at,
                )
            free_remaining = max(0, config.free_questions_per_24h - used)
        reason = "allowed" if free_remaining + manual + paid > 0 else "limit_exceeded"
        return AccessCheck(
            allowed=reason == "allowed",
            reason=reason,
            chat_id=int(chat_id),
            telegram_user_id=int(telegram_user_id),
            free_limit=config.free_questions_per_24h,
            used_in_window=used,
            free_remaining=free_remaining,
            manual_credits=manual,
            paid_credits=paid,
            next_reset_at=next_reset_at,
        )

    def consume_question(
        self,
        *,
        telegram_user_id: int,
        chat_id: int,
        question_key: str | None = None,
        at: datetime | None = None,
    ) -> AccessConsumption | None:
        effective_at = at or _now()
        now_iso = _isoformat(effective_at)
        with closing(self._connect()) as conn, conn:
            config = self._effective_config_locked(conn, int(chat_id))
            if not config.enabled:
                check = AccessCheck(
                    allowed=True,
                    reason="disabled",
                    chat_id=int(chat_id),
                    telegram_user_id=int(telegram_user_id),
                    free_limit=config.free_questions_per_24h,
                    used_in_window=0,
                    free_remaining=config.free_questions_per_24h,
                    manual_credits=0,
                    paid_credits=0,
                    next_reset_at=None,
                )
                return AccessConsumption(check=check, source="disabled")
            used, next_reset_at = self._used_in_window_locked(
                conn,
                telegram_user_id=int(telegram_user_id),
                chat_id=int(chat_id),
                at=effective_at,
            )
            free_remaining = max(0, config.free_questions_per_24h - used)
            manual = self._ledger_balance_locked(
                conn,
                telegram_user_id=int(telegram_user_id),
                chat_id=int(chat_id),
                credit_type="manual",
            )
            paid = self._ledger_balance_locked(
                conn,
                telegram_user_id=int(telegram_user_id),
                chat_id=int(chat_id),
                credit_type="paid",
            )
            if free_remaining > 0:
                source = "free"
            elif manual > 0:
                source = "manual"
            elif paid > 0:
                source = "paid"
            else:
                return None

            conn.execute(
                """
                INSERT INTO access_question_usage (
                    telegram_user_id,
                    chat_id,
                    source,
                    question_key,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (int(telegram_user_id), int(chat_id), source, question_key, now_iso),
            )
            if source in {"manual", "paid"}:
                conn.execute(
                    """
                    INSERT INTO access_credit_ledger (
                        telegram_user_id,
                        chat_id,
                        credit_type,
                        delta,
                        reason,
                        created_at
                    ) VALUES (?, ?, ?, -1, ?, ?)
                    """,
                    (int(telegram_user_id), int(chat_id), source, "question", now_iso),
                )

            check = AccessCheck(
                allowed=True,
                reason="allowed",
                chat_id=int(chat_id),
                telegram_user_id=int(telegram_user_id),
                free_limit=config.free_questions_per_24h,
                used_in_window=used + 1 if source == "free" else used,
                free_remaining=max(0, free_remaining - 1) if source == "free" else 0,
                manual_credits=max(0, manual - 1) if source == "manual" else manual,
                paid_credits=max(0, paid - 1) if source == "paid" else paid,
                next_reset_at=next_reset_at,
            )
        return AccessConsumption(check=check, source=source)

    def grant_manual_credits(
        self,
        *,
        telegram_user_id: int,
        chat_id: int,
        delta: int,
        reason: str | None = None,
    ) -> int:
        now_iso = _isoformat(_now())
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO access_credit_ledger (
                    telegram_user_id,
                    chat_id,
                    credit_type,
                    delta,
                    reason,
                    created_at
                ) VALUES (?, ?, 'manual', ?, ?, ?)
                """,
                (int(telegram_user_id), int(chat_id), int(delta), reason, now_iso),
            )
            return self._ledger_balance_locked(
                conn,
                telegram_user_id=int(telegram_user_id),
                chat_id=int(chat_id),
                credit_type="manual",
            )

    def create_stars_order(self, *, telegram_user_id: int, chat_id: int) -> StarsOrder:
        order_id = uuid.uuid4().hex
        payload = f"{ORDER_PAYLOAD_PREFIX}{order_id}"
        config = self.get_effective_config(chat_id=int(chat_id))
        now_iso = _isoformat(_now())
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO access_orders (
                    order_id,
                    telegram_user_id,
                    chat_id,
                    credits,
                    stars_amount,
                    currency,
                    status,
                    payload,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    order_id,
                    int(telegram_user_id),
                    int(chat_id),
                    config.credits_per_purchase,
                    config.stars_price,
                    STARS_CURRENCY,
                    payload,
                    now_iso,
                    now_iso,
                ),
            )
        return StarsOrder(
            order_id=order_id,
            payload=payload,
            telegram_user_id=int(telegram_user_id),
            chat_id=int(chat_id),
            credits=config.credits_per_purchase,
            stars_amount=config.stars_price,
            currency=STARS_CURRENCY,
        )

    def validate_stars_order(
        self,
        *,
        payload: str,
        currency: str,
        total_amount: int,
        telegram_user_id: int | None = None,
    ) -> bool:
        if currency != STARS_CURRENCY or not payload.startswith(ORDER_PAYLOAD_PREFIX):
            return False
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM access_orders WHERE payload = ?",
                (payload,),
            ).fetchone()
            if row is None or str(row["status"]) != "pending":
                return False
            if telegram_user_id is not None and int(row["telegram_user_id"]) != int(telegram_user_id):
                return False
            return int(row["stars_amount"]) == int(total_amount) and str(row["currency"]) == STARS_CURRENCY

    def record_successful_payment(
        self,
        *,
        payload: str,
        currency: str,
        total_amount: int,
        telegram_user_id: int,
        telegram_payment_charge_id: str | None,
        provider_payment_charge_id: str | None,
        raw: dict | None = None,
    ) -> tuple[bool, int]:
        if currency != STARS_CURRENCY:
            raise ValueError("Only Telegram Stars payments are supported.")
        now_iso = _isoformat(_now())
        with closing(self._connect()) as conn, conn:
            if telegram_payment_charge_id:
                existing = conn.execute(
                    "SELECT credits FROM access_payments WHERE telegram_payment_charge_id = ?",
                    (telegram_payment_charge_id,),
                ).fetchone()
                if existing is not None:
                    return False, int(existing["credits"])
            if provider_payment_charge_id:
                existing = conn.execute(
                    "SELECT credits FROM access_payments WHERE provider_payment_charge_id = ?",
                    (provider_payment_charge_id,),
                ).fetchone()
                if existing is not None:
                    return False, int(existing["credits"])

            order = conn.execute(
                "SELECT * FROM access_orders WHERE payload = ?",
                (payload,),
            ).fetchone()
            if order is None:
                raise ValueError("Payment order was not found.")
            if str(order["status"]) == "paid":
                return False, int(order["credits"])
            if int(order["telegram_user_id"]) != int(telegram_user_id):
                raise ValueError("Payment user does not match the order.")
            if str(order["currency"]) != STARS_CURRENCY or int(order["stars_amount"]) != int(total_amount):
                raise ValueError("Payment amount does not match the order.")

            credits = int(order["credits"])
            payment_cursor = conn.execute(
                """
                INSERT INTO access_payments (
                    order_id,
                    telegram_user_id,
                    chat_id,
                    credits,
                    stars_amount,
                    currency,
                    telegram_payment_charge_id,
                    provider_payment_charge_id,
                    created_at,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(order["order_id"]),
                    int(telegram_user_id),
                    int(order["chat_id"]),
                    credits,
                    int(total_amount),
                    STARS_CURRENCY,
                    telegram_payment_charge_id,
                    provider_payment_charge_id,
                    now_iso,
                    json.dumps(raw or {}, ensure_ascii=False, sort_keys=True),
                ),
            )
            payment_id = int(payment_cursor.lastrowid)
            conn.execute(
                """
                INSERT INTO access_credit_ledger (
                    telegram_user_id,
                    chat_id,
                    credit_type,
                    delta,
                    reason,
                    order_id,
                    payment_id,
                    created_at
                ) VALUES (?, ?, 'paid', ?, 'telegram_stars', ?, ?, ?)
                """,
                (
                    int(telegram_user_id),
                    int(order["chat_id"]),
                    credits,
                    str(order["order_id"]),
                    payment_id,
                    now_iso,
                ),
            )
            conn.execute(
                """
                UPDATE access_orders
                SET status = 'paid', updated_at = ?
                WHERE order_id = ?
                """,
                (now_iso, str(order["order_id"])),
            )
        return True, credits

    def balance(self, *, telegram_user_id: int, chat_id: int, at: datetime | None = None) -> dict:
        check = self.check_question(telegram_user_id=telegram_user_id, chat_id=chat_id, at=at)
        return {
            "telegram_user_id": check.telegram_user_id,
            "chat_id": check.chat_id,
            "enabled": check.reason != "disabled",
            "free_limit": check.free_limit,
            "used_in_window": check.used_in_window,
            "free_remaining": check.free_remaining,
            "manual_credits": check.manual_credits,
            "paid_credits": check.paid_credits,
            "total_remaining": check.total_remaining,
            "next_reset_at": _isoformat(check.next_reset_at) if check.next_reset_at else None,
        }

    def status(self) -> dict:
        with closing(self._connect()) as conn:
            config = self._global_config_locked(conn)
            overrides = [
                {
                    "chat_id": int(row["chat_id"]),
                    "enabled": bool(int(row["enabled"])) if row["enabled"] is not None else None,
                    "free_questions_per_24h": row["free_questions_per_24h"],
                    "stars_price": row["stars_price"],
                    "credits_per_purchase": row["credits_per_purchase"],
                }
                for row in conn.execute(
                    "SELECT * FROM access_chat_overrides ORDER BY chat_id ASC"
                ).fetchall()
            ]
            totals = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM access_question_usage) AS usage_count,
                    (SELECT COUNT(*) FROM access_orders) AS order_count,
                    (SELECT COUNT(*) FROM access_payments) AS payment_count
                """
            ).fetchone()
        return {
            "state_path": str(self._path()),
            "currency": STARS_CURRENCY,
            "global": {
                "enabled": config.enabled,
                "free_questions_per_24h": config.free_questions_per_24h,
                "stars_price": config.stars_price,
                "credits_per_purchase": config.credits_per_purchase,
            },
            "chat_overrides": overrides,
            "totals": {
                "usage_count": int(totals["usage_count"] if totals else 0),
                "order_count": int(totals["order_count"] if totals else 0),
                "payment_count": int(totals["payment_count"] if totals else 0),
            },
        }

    def stars_ledger_summary(self) -> dict:
        with closing(self._connect()) as conn:
            totals = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM access_orders) AS order_count,
                    (SELECT COUNT(*) FROM access_payments) AS payment_count,
                    (SELECT COUNT(*) FROM access_question_usage) AS usage_count,
                    (SELECT COALESCE(SUM(stars_amount), 0) FROM access_payments) AS paid_stars
                """
            ).fetchone()
            credit_rows = conn.execute(
                """
                SELECT
                    credit_type,
                    COALESCE(SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END), 0) AS granted,
                    COALESCE(SUM(CASE WHEN delta < 0 THEN -delta ELSE 0 END), 0) AS consumed,
                    COALESCE(SUM(delta), 0) AS remaining
                FROM access_credit_ledger
                WHERE credit_type IN ('paid', 'manual')
                GROUP BY credit_type
                """
            ).fetchall()

        credits = {
            "paid": {"granted": 0, "consumed": 0, "remaining": 0},
            "manual": {"granted": 0, "consumed": 0, "remaining": 0},
        }
        for row in credit_rows:
            credit_type = str(row["credit_type"])
            if credit_type not in credits:
                continue
            credits[credit_type] = {
                "granted": int(row["granted"]),
                "consumed": int(row["consumed"]),
                "remaining": int(row["remaining"]),
            }

        return {
            "state_path": str(self._path()),
            "currency": STARS_CURRENCY,
            "local_order_count": int(totals["order_count"] if totals else 0),
            "local_payment_count": int(totals["payment_count"] if totals else 0),
            "usage_count": int(totals["usage_count"] if totals else 0),
            "total_local_paid_stars_amount": int(totals["paid_stars"] if totals else 0),
            "paid_credits": credits["paid"],
            "manual_credits": credits["manual"],
        }

    def star_payments(self) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT
                    id,
                    order_id,
                    telegram_user_id,
                    chat_id,
                    credits,
                    stars_amount,
                    currency,
                    telegram_payment_charge_id,
                    provider_payment_charge_id,
                    created_at
                FROM access_payments
                ORDER BY id ASC
                """
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "order_id": row["order_id"],
                "telegram_user_id": int(row["telegram_user_id"]),
                "chat_id": int(row["chat_id"]),
                "credits": int(row["credits"]),
                "stars_amount": int(row["stars_amount"]),
                "currency": row["currency"],
                "telegram_payment_charge_id": row["telegram_payment_charge_id"],
                "provider_payment_charge_id": row["provider_payment_charge_id"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]
