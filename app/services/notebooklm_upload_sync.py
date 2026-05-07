"""Windows-first NotebookLM auth sync via one-time upload sessions."""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
from uuid import uuid4

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession

from app.core.config import get_notebooklm_proxy_url, get_settings
from app.services.notebooklm_cookie_import import normalize_notebooklm_cookie_import
from app.services.notebooklm_client import load_notebooklm_auth
from app.services.notebooklm_runtime import NotebookLMRuntimeStore

logger = logging.getLogger(__name__)

_DEFAULT_UPLOAD_SESSION_STATE = ".state/notebooklm/upload_sync_state.json"
_DEFAULT_PROTOCOL_SCHEME = "tgctxbot-notebooklm-sync"
_TERMINAL_SESSION_STATUSES = {"completed", "expired", "failed", "cancelled"}
_TERMINAL_DEVICE_STATUSES = {"expired", "revoked"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class NotebookLMSyncDevice:
    device_id: str
    token_hash: str
    created_at: str
    expires_at: str
    status: str
    browser_preference: str = "auto"
    profile_preference: str = "auto"
    last_uploaded_at: str | None = None
    upload_count: int = 0
    last_metadata: dict[str, Any] | None = None
    revoked_at: str | None = None
    error: str | None = None


@dataclass(slots=True)
class NotebookLMUploadSession:
    session_id: str
    token_hash: str
    source: str
    created_at: str
    expires_at: str
    status: str
    requested_by_user_id: int | None = None
    requested_by_chat_id: int | None = None
    notify_chat_id: int | None = None
    notify_message_thread_id: int | None = None
    uploaded_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    helper_metadata: dict[str, Any] | None = None
    notified_at: str | None = None
    device_id: str | None = None


class UploadSyncConfigurationError(RuntimeError):
    """Raised when the Windows upload-sync flow is not configured."""


class NotebookLMUploadSyncStore:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _path(self) -> Path:
        configured = (
            getattr(self._settings, "notebooklm_upload_session_state_path", _DEFAULT_UPLOAD_SESSION_STATE)
            or _DEFAULT_UPLOAD_SESSION_STATE
        )
        return Path(str(configured)).expanduser()

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _load(self) -> tuple[list[NotebookLMUploadSession], list[NotebookLMSyncDevice]]:
        path = self._path()
        if not path.exists():
            return [], []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return [], []

        sessions: list[NotebookLMUploadSession] = []
        for item in payload.get("sessions", []):
            if not isinstance(item, dict):
                continue
            helper_metadata = item.get("helper_metadata")
            if not isinstance(helper_metadata, dict):
                helper_metadata = None
            sessions.append(
                NotebookLMUploadSession(
                    session_id=str(item.get("session_id", "") or ""),
                    token_hash=str(item.get("token_hash", "") or ""),
                    source=str(item.get("source", "") or ""),
                    created_at=str(item.get("created_at", "") or ""),
                    expires_at=str(item.get("expires_at", "") or ""),
                    status=str(item.get("status", "pending") or "pending"),
                    requested_by_user_id=_safe_int(item.get("requested_by_user_id")),
                    requested_by_chat_id=_safe_int(item.get("requested_by_chat_id")),
                    notify_chat_id=_safe_int(item.get("notify_chat_id")),
                    notify_message_thread_id=_safe_int(item.get("notify_message_thread_id")),
                    uploaded_at=str(item.get("uploaded_at", "") or "") or None,
                    completed_at=str(item.get("completed_at", "") or "") or None,
                    error=str(item.get("error", "") or "") or None,
                    helper_metadata=helper_metadata,
                    notified_at=str(item.get("notified_at", "") or "") or None,
                    device_id=str(item.get("device_id", "") or "") or None,
                )
            )

        devices: list[NotebookLMSyncDevice] = []
        for item in payload.get("devices", []):
            if not isinstance(item, dict):
                continue
            last_metadata = item.get("last_metadata")
            if not isinstance(last_metadata, dict):
                last_metadata = None
            devices.append(
                NotebookLMSyncDevice(
                    device_id=str(item.get("device_id", "") or ""),
                    token_hash=str(item.get("token_hash", "") or ""),
                    created_at=str(item.get("created_at", "") or ""),
                    expires_at=str(item.get("expires_at", "") or ""),
                    status=str(item.get("status", "active") or "active"),
                    browser_preference=str(item.get("browser_preference", "auto") or "auto"),
                    profile_preference=str(item.get("profile_preference", "auto") or "auto"),
                    last_uploaded_at=str(item.get("last_uploaded_at", "") or "") or None,
                    upload_count=int(item.get("upload_count", 0) or 0),
                    last_metadata=last_metadata,
                    revoked_at=str(item.get("revoked_at", "") or "") or None,
                    error=str(item.get("error", "") or "") or None,
                )
            )
        return sessions, devices

    def _write(self, sessions: list[NotebookLMUploadSession], devices: list[NotebookLMSyncDevice]) -> None:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "sessions": [asdict(session) for session in sessions],
            "devices": [asdict(device) for device in devices],
        }
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(path)

    def create_session(
        self,
        *,
        token_hash: str,
        source: str,
        requested_by_user_id: int | None,
        requested_by_chat_id: int | None,
        notify_chat_id: int | None,
        notify_message_thread_id: int | None,
        ttl_seconds: int,
    ) -> NotebookLMUploadSession:
        session = NotebookLMUploadSession(
            session_id=str(uuid4()),
            token_hash=token_hash,
            source=source,
            created_at=_isoformat(_utc_now()) or "",
            expires_at=_isoformat(_utc_now() + timedelta(seconds=ttl_seconds)) or "",
            status="pending",
            requested_by_user_id=requested_by_user_id,
            requested_by_chat_id=requested_by_chat_id,
            notify_chat_id=notify_chat_id,
            notify_message_thread_id=notify_message_thread_id,
        )
        sessions, devices = self._load()
        sessions.append(session)
        self._write(sessions, devices)
        return session

    def create_device(
        self,
        *,
        token_hash: str,
        browser_preference: str,
        profile_preference: str,
        ttl_seconds: int,
        last_metadata: dict[str, Any] | None = None,
    ) -> NotebookLMSyncDevice:
        device = NotebookLMSyncDevice(
            device_id=str(uuid4()),
            token_hash=token_hash,
            created_at=_isoformat(_utc_now()) or "",
            expires_at=_isoformat(_utc_now() + timedelta(seconds=ttl_seconds)) or "",
            status="active",
            browser_preference=browser_preference or "auto",
            profile_preference=profile_preference or "auto",
            last_metadata=last_metadata,
        )
        sessions, devices = self._load()
        devices.append(device)
        self._write(sessions, devices)
        return device

    def list_sessions(self) -> list[NotebookLMUploadSession]:
        sessions, _ = self._load()
        return sessions

    def get_session_by_id(self, session_id: str) -> NotebookLMUploadSession | None:
        return next((item for item in self.list_sessions() if item.session_id == session_id), None)

    def get_session_by_token(self, token: str) -> NotebookLMUploadSession | None:
        token_hash = self._token_hash(token)
        now = _utc_now()
        sessions, devices = self._load()
        changed = False
        found = None
        for session in sessions:
            expires_at = _parse_dt(session.expires_at)
            if expires_at and expires_at <= now and session.status not in _TERMINAL_SESSION_STATUSES:
                session.status = "expired"
                session.error = session.error or "Upload session expired."
                changed = True
            if session.token_hash == token_hash:
                found = session
        if changed:
            self._write(sessions, devices)
        return found

    def update_session(self, updated: NotebookLMUploadSession) -> NotebookLMUploadSession:
        sessions, devices = self._load()
        for index, session in enumerate(sessions):
            if session.session_id == updated.session_id:
                sessions[index] = updated
                self._write(sessions, devices)
                return updated
        raise KeyError(f"Upload session not found: {updated.session_id}")

    def get_device_by_id(self, device_id: str) -> NotebookLMSyncDevice | None:
        _, devices = self._load()
        return next((item for item in devices if item.device_id == device_id), None)

    def get_device_by_token(self, token: str) -> NotebookLMSyncDevice | None:
        token_hash = self._token_hash(token)
        now = _utc_now()
        sessions, devices = self._load()
        changed = False
        found = None
        for device in devices:
            expires_at = _parse_dt(device.expires_at)
            if expires_at and expires_at <= now and device.status not in _TERMINAL_DEVICE_STATUSES:
                device.status = "expired"
                device.error = device.error or "Scheduled sync token expired."
                changed = True
            if device.token_hash == token_hash:
                found = device
        if changed:
            self._write(sessions, devices)
        return found

    def update_device(self, updated: NotebookLMSyncDevice) -> NotebookLMSyncDevice:
        sessions, devices = self._load()
        for index, device in enumerate(devices):
            if device.device_id == updated.device_id:
                devices[index] = updated
                self._write(sessions, devices)
                return updated
        raise KeyError(f"Sync device not found: {updated.device_id}")

    def expire_stale_items(self) -> tuple[list[NotebookLMUploadSession], list[NotebookLMSyncDevice]]:
        now = _utc_now()
        sessions, devices = self._load()
        expired_sessions: list[NotebookLMUploadSession] = []
        expired_devices: list[NotebookLMSyncDevice] = []
        changed = False
        for session in sessions:
            expires_at = _parse_dt(session.expires_at)
            if expires_at and expires_at <= now and session.status not in _TERMINAL_SESSION_STATUSES:
                session.status = "expired"
                session.error = session.error or "Upload session expired."
                expired_sessions.append(session)
                changed = True
        for device in devices:
            expires_at = _parse_dt(device.expires_at)
            if expires_at and expires_at <= now and device.status not in _TERMINAL_DEVICE_STATUSES:
                device.status = "expired"
                device.error = device.error or "Scheduled sync token expired."
                expired_devices.append(device)
                changed = True
        if changed:
            self._write(sessions, devices)
        return expired_sessions, expired_devices


class NotebookLMUploadSyncManager:
    def __init__(self, *, settings=None, store: NotebookLMUploadSyncStore | None = None, runtime_store=None) -> None:
        self._settings = settings or get_settings()
        self._store = store or NotebookLMUploadSyncStore(settings=self._settings)
        self._runtime_store = runtime_store or NotebookLMRuntimeStore(settings=self._settings)

    def _session_ttl_seconds(self) -> int:
        value = int(getattr(self._settings, "notebooklm_upload_session_ttl_seconds", 900) or 900)
        return max(60, value)

    def _device_ttl_seconds(self) -> int:
        value = int(
            getattr(self._settings, "notebooklm_upload_refresh_ttl_seconds", 30 * 24 * 60 * 60)
            or 30 * 24 * 60 * 60
        )
        return max(3600, value)

    def _protocol_scheme(self) -> str:
        value = str(
            getattr(self._settings, "notebooklm_windows_helper_protocol_scheme", _DEFAULT_PROTOCOL_SCHEME)
            or _DEFAULT_PROTOCOL_SCHEME
        ).strip()
        if not value:
            raise UploadSyncConfigurationError("NotebookLM Windows helper protocol scheme is not configured.")
        return value

    def _resolve_public_base_url(self, fallback_base_url: str | None = None) -> str:
        configured = getattr(self._settings, "notebooklm_remote_auth_base_url", "") or ""
        value = configured.strip()
        if value:
            return value.rstrip("/")
        if fallback_base_url:
            return fallback_base_url.rstrip("/")
        raise UploadSyncConfigurationError(
            "NotebookLM Windows upload-sync requires NOTEBOOKLM_REMOTE_AUTH_BASE_URL for helper links."
        )

    def _build_entry_url(self, token: str, fallback_base_url: str | None = None) -> str:
        return f"{self._resolve_public_base_url(fallback_base_url)}/auth-session/{token}"

    def _build_status_url(self, token: str, fallback_base_url: str | None = None) -> str:
        return f"{self._resolve_public_base_url(fallback_base_url)}/api/public/notebooklm/upload-sessions/{token}"

    def _build_upload_url(self, token: str, fallback_base_url: str | None = None) -> str:
        return f"{self._resolve_public_base_url(fallback_base_url)}/api/public/notebooklm/upload-sessions/{token}"

    def _build_refresh_url(self, token: str, fallback_base_url: str | None = None) -> str:
        return f"{self._resolve_public_base_url(fallback_base_url)}/api/public/notebooklm/upload-refresh/{token}"

    async def _normalize_and_validate_storage_state(
        self,
        storage_state_json: str,
    ) -> tuple[str, dict[str, Any]]:
        storage_state, import_metadata = normalize_notebooklm_cookie_import(storage_state_json)
        normalized_json = json.dumps(storage_state, ensure_ascii=False)
        with tempfile.TemporaryDirectory() as tmp:
            temp_path = Path(tmp) / "storage_state.json"
            temp_path.write_text(normalized_json, encoding="utf-8")
            await load_notebooklm_auth(
                temp_path,
                float(getattr(self._settings, "notebooklm_timeout", 30.0) or 30.0),
                get_notebooklm_proxy_url(self._settings),
            )
        return normalized_json, import_metadata

    def _build_protocol_url(
        self,
        *,
        upload_url: str,
        status_url: str,
        entry_url: str,
        browser: str = "auto",
        profile: str = "auto",
    ) -> str:
        scheme = self._protocol_scheme()
        query = (
            f"upload_url={quote_plus(upload_url)}"
            f"&status_url={quote_plus(status_url)}"
            f"&entry_url={quote_plus(entry_url)}"
            f"&browser={quote_plus(browser)}"
            f"&profile={quote_plus(profile)}"
        )
        return f"{scheme}://sync?{query}"

    def create_session(
        self,
        *,
        source: str,
        requested_by_user_id: int | None,
        requested_by_chat_id: int | None,
        notify_chat_id: int | None,
        notify_message_thread_id: int | None,
        fallback_base_url: str | None = None,
    ) -> dict[str, Any]:
        session_token = secrets.token_urlsafe(32)
        session = self._store.create_session(
            token_hash=self._store._token_hash(session_token),  # noqa: SLF001
            source=source,
            requested_by_user_id=requested_by_user_id,
            requested_by_chat_id=requested_by_chat_id,
            notify_chat_id=notify_chat_id,
            notify_message_thread_id=notify_message_thread_id,
            ttl_seconds=self._session_ttl_seconds(),
        )
        return self._session_public_payload(
            session,
            session_token=session_token,
            fallback_base_url=fallback_base_url,
        )

    def get_latest_session(self) -> dict[str, Any] | None:
        sessions = self._store.list_sessions()
        if not sessions:
            return None
        latest = max(sessions, key=lambda item: item.created_at or "")
        return self._session_public_payload(latest)

    def get_session_status(self, token: str, *, fallback_base_url: str | None = None) -> dict[str, Any]:
        session = self._store.get_session_by_token(token)
        if session is None:
            raise KeyError("NotebookLM upload session not found.")
        return self._session_public_payload(
            session,
            session_token=token,
            fallback_base_url=fallback_base_url,
        )

    async def complete_upload(
        self,
        token: str,
        storage_state_json: str,
        *,
        helper_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        session = self._store.get_session_by_token(token)
        if session is None:
            raise KeyError("NotebookLM upload session not found.")
        if session.status == "completed":
            raise ValueError("This NotebookLM upload session has already been used.")
        if session.status in {"expired", "cancelled", "failed"}:
            raise ValueError(f"This NotebookLM upload session is no longer active ({session.status}).")

        helper_metadata = dict(helper_metadata) if isinstance(helper_metadata, dict) else {}
        browser = str((helper_metadata or {}).get("browser", "") or "").strip() or "auto"
        profile = str((helper_metadata or {}).get("profile", "") or "").strip() or "auto"
        uploaded_at = _isoformat(_utc_now())
        try:
            normalized_json, import_metadata = await self._normalize_and_validate_storage_state(storage_state_json)
        except Exception as exc:
            session.status = "failed"
            session.uploaded_at = uploaded_at
            session.completed_at = uploaded_at
            session.error = str(exc)
            session.helper_metadata = helper_metadata
            self._store.update_session(session)
            return {
                "auth_ready": False,
                "auth_check": "expired",
                "auth_error": str(exc),
                "refresh_url": None,
            }

        helper_metadata["import"] = import_metadata
        self._runtime_store.replace_storage_state(normalized_json)
        from app.services.notebooklm_service import NotebookLMService

        await NotebookLMService.invalidate_cached_client()
        refresh_token = secrets.token_urlsafe(32)
        device = self._store.create_device(
            token_hash=self._store._token_hash(refresh_token),  # noqa: SLF001
            browser_preference=browser,
            profile_preference=profile,
            ttl_seconds=self._device_ttl_seconds(),
            last_metadata=helper_metadata,
        )
        session.status = "completed"
        session.uploaded_at = uploaded_at
        session.completed_at = session.uploaded_at
        session.error = None
        session.helper_metadata = helper_metadata
        session.device_id = device.device_id
        self._store.update_session(session)
        device.last_uploaded_at = session.uploaded_at
        device.upload_count += 1
        self._store.update_device(device)

        await self._notify(session, "Авторизация NotebookLM обновлена через Windows sync.")
        return {
            **self._runtime_store.get_runtime_status(),
            "refresh_url": self._build_refresh_url(refresh_token),
            "import": import_metadata,
        }

    async def refresh_from_device(
        self,
        token: str,
        storage_state_json: str,
        *,
        helper_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        device = self._store.get_device_by_token(token)
        if device is None:
            raise KeyError("NotebookLM refresh token not found.")
        if device.status != "active":
            raise ValueError(f"This NotebookLM refresh token is not active ({device.status}).")

        normalized_json, import_metadata = await self._normalize_and_validate_storage_state(storage_state_json)
        self._runtime_store.replace_storage_state(normalized_json)
        from app.services.notebooklm_service import NotebookLMService

        await NotebookLMService.invalidate_cached_client()
        device.last_uploaded_at = _isoformat(_utc_now())
        device.upload_count += 1
        device.error = None
        if isinstance(helper_metadata, dict):
            browser = str(helper_metadata.get("browser", "") or "").strip()
            profile = str(helper_metadata.get("profile", "") or "").strip()
            if browser:
                device.browser_preference = browser
            if profile:
                device.profile_preference = profile
            helper_metadata["import"] = import_metadata
            device.last_metadata = helper_metadata
        else:
            device.last_metadata = {"import": import_metadata}
        self._store.update_device(device)
        return {**self._runtime_store.get_runtime_status(), "import": import_metadata}

    async def cancel_session(self, token: str) -> dict[str, Any]:
        session = self._store.get_session_by_token(token)
        if session is None:
            raise KeyError("NotebookLM upload session not found.")
        session.status = "cancelled"
        session.error = "Upload session cancelled by operator."
        session.completed_at = session.completed_at or _isoformat(_utc_now())
        self._store.update_session(session)
        return self._session_public_payload(session, session_token=token)

    async def reconcile(self) -> None:
        self._store.expire_stale_items()

    async def _notify(self, session: NotebookLMUploadSession, text: str) -> None:
        if session.notify_chat_id is None:
            return
        try:
            bot_session = None
            if getattr(self._settings, "telegram_proxy_enabled", False) and getattr(
                self._settings, "telegram_proxy_url", None
            ):
                bot_session = AiohttpSession(proxy=self._settings.telegram_proxy_url)
            bot = Bot(token=self._settings.bot_token, session=bot_session)
            await bot.send_message(
                session.notify_chat_id,
                text,
                message_thread_id=session.notify_message_thread_id,
                disable_web_page_preview=True,
            )
            await bot.session.close()
            session.notified_at = _isoformat(_utc_now())
            self._store.update_session(session)
        except Exception:
            logger.exception("notebooklm upload-sync notify failed session_id=%s", session.session_id)

    def _session_public_payload(
        self,
        session: NotebookLMUploadSession,
        *,
        session_token: str | None = None,
        fallback_base_url: str | None = None,
    ) -> dict[str, Any]:
        device = self._store.get_device_by_id(session.device_id or "")
        entry_url = (
            self._build_entry_url(session_token, fallback_base_url) if session_token else None
        )
        status_url = (
            self._build_status_url(session_token, fallback_base_url) if session_token else None
        )
        upload_url = (
            self._build_upload_url(session_token, fallback_base_url) if session_token else None
        )
        protocol_url = None
        if upload_url and status_url and entry_url:
            protocol_url = self._build_protocol_url(
                upload_url=upload_url,
                status_url=status_url,
                entry_url=entry_url,
                browser=device.browser_preference if device else "auto",
                profile=device.profile_preference if device else "auto",
            )
        return {
            "session_id": session.session_id,
            "status": session.status,
            "source": session.source,
            "expires_at": session.expires_at,
            "created_at": session.created_at,
            "uploaded_at": session.uploaded_at,
            "completed_at": session.completed_at,
            "error": session.error,
            "entry_url": entry_url,
            "auth_url": entry_url,
            "status_url": status_url,
            "upload_url": upload_url,
            "refresh_url": None,
            "protocol_url": protocol_url,
            "requested_via": session.source,
            "helper_metadata": session.helper_metadata,
            "device": {
                "device_id": device.device_id,
                "status": device.status,
                "expires_at": device.expires_at,
                "browser_preference": device.browser_preference,
                "profile_preference": device.profile_preference,
                "last_uploaded_at": device.last_uploaded_at,
                "upload_count": device.upload_count,
            }
            if device is not None
            else None,
        }


_upload_sync_manager: NotebookLMUploadSyncManager | None = None


def get_notebooklm_upload_sync_manager() -> NotebookLMUploadSyncManager:
    global _upload_sync_manager
    if _upload_sync_manager is None:
        _upload_sync_manager = NotebookLMUploadSyncManager()
    return _upload_sync_manager
