"""Remote NotebookLM auth sessions for a VPS-hosted lightweight instance."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import socket
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession

from app.core.config import get_notebooklm_proxy_url, get_settings
from app.services.notebooklm_events import log_event
from app.services.notebooklm_client import refresh_notebooklm_google_keepalive
from app.services.notebooklm_runtime import NotebookLMRuntimeStore

logger = logging.getLogger(__name__)

_DEFAULT_REMOTE_AUTH_STATE = ".tmp/notebooklm/auth_sessions.json"
_NOTEBOOKLM_URL = "https://notebooklm.google.com/"
_GOOGLE_ACCOUNTS_URL = "https://accounts.google.com/"
_REMOTE_AUTH_WEBDRIVER_PORT = 4444
_AUTH_COOKIE_NAMES = {
    "SID",
    "HSID",
    "SSID",
    "SAPISID",
    "__Secure-1PSID",
    "__Secure-3PSID",
}
_TERMINAL_SESSION_STATUSES = {"completed", "expired", "failed", "cancelled"}


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


def _find_free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class NotebookLMRemoteAuthBrowser:
    container_id: str
    container_name: str
    webdriver_port: int
    novnc_port: int
    vnc_password: str
    webdriver_session_id: str
    browser_url: str
    started_at: str


@dataclass(slots=True)
class NotebookLMRemoteAuthSession:
    session_id: str
    token_hash: str
    source: str
    created_at: str
    expires_at: str
    status: str
    auth_url: str = ""
    requested_by_user_id: int | None = None
    requested_by_chat_id: int | None = None
    notify_chat_id: int | None = None
    notify_message_thread_id: int | None = None
    launched_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    browser: NotebookLMRemoteAuthBrowser | None = None
    notified_at: str | None = None


class NotebookLMRemoteAuthStore:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _path(self) -> Path:
        configured = (
            getattr(self._settings, "notebooklm_remote_auth_state_path", _DEFAULT_REMOTE_AUTH_STATE)
            or _DEFAULT_REMOTE_AUTH_STATE
        )
        return Path(str(configured)).expanduser()

    def _load(self) -> list[NotebookLMRemoteAuthSession]:
        path = self._path()
        if not path.exists():
            return []
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Remote auth state JSON must be an object.")
        raw_sessions = payload.get("sessions", [])
        if not isinstance(raw_sessions, list):
            raise ValueError("Remote auth state JSON must contain a sessions list.")
        sessions: list[NotebookLMRemoteAuthSession] = []
        for item in raw_sessions:
            browser_payload = item.get("browser")
            browser = None
            if isinstance(browser_payload, dict):
                browser = NotebookLMRemoteAuthBrowser(
                    container_id=str(browser_payload.get("container_id", "") or ""),
                    container_name=str(browser_payload.get("container_name", "") or ""),
                    webdriver_port=int(browser_payload.get("webdriver_port", 0) or 0),
                    novnc_port=int(browser_payload.get("novnc_port", 0) or 0),
                    vnc_password=str(browser_payload.get("vnc_password", "") or ""),
                    webdriver_session_id=str(browser_payload.get("webdriver_session_id", "") or ""),
                    browser_url=str(browser_payload.get("browser_url", "") or ""),
                    started_at=str(browser_payload.get("started_at", "") or ""),
                )
            sessions.append(
                NotebookLMRemoteAuthSession(
                    session_id=str(item.get("session_id", "") or ""),
                    token_hash=str(item.get("token_hash", "") or ""),
                    source=str(item.get("source", "") or "unknown"),
                    created_at=str(item.get("created_at", "") or ""),
                    expires_at=str(item.get("expires_at", "") or ""),
                    status=str(item.get("status", "") or "pending"),
                    auth_url="",
                    requested_by_user_id=_safe_int(item.get("requested_by_user_id")),
                    requested_by_chat_id=_safe_int(item.get("requested_by_chat_id")),
                    notify_chat_id=_safe_int(item.get("notify_chat_id")),
                    notify_message_thread_id=_safe_int(item.get("notify_message_thread_id")),
                    launched_at=str(item.get("launched_at", "") or "") or None,
                    completed_at=str(item.get("completed_at", "") or "") or None,
                    error=str(item.get("error", "") or "") or None,
                    browser=browser,
                    notified_at=str(item.get("notified_at", "") or "") or None,
                )
            )
        return sessions

    def _write(self, sessions: list[NotebookLMRemoteAuthSession]) -> None:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"sessions": [asdict(session) for session in sessions]}
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(path)

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

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
    ) -> NotebookLMRemoteAuthSession:
        session = NotebookLMRemoteAuthSession(
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
        sessions = self._load()
        sessions.append(session)
        self._write(sessions)
        return session

    def list_sessions(self) -> list[NotebookLMRemoteAuthSession]:
        return self._load()

    def get_by_id(self, session_id: str) -> NotebookLMRemoteAuthSession | None:
        return next((item for item in self._load() if item.session_id == session_id), None)

    def get_by_token(self, token: str) -> NotebookLMRemoteAuthSession | None:
        token_hash = self._token_hash(token)
        now = _utc_now()
        sessions = self._load()
        changed = False
        found = None
        for session in sessions:
            expires_at = _parse_dt(session.expires_at)
            if expires_at and expires_at <= now and session.status not in {
                "completed",
                "expired",
                "failed",
                "cancelled",
            }:
                session.status = "expired"
                session.error = session.error or "Auth session expired."
                changed = True
            if session.token_hash == token_hash:
                found = session
        if changed:
            self._write(sessions)
        return found

    def update_session(self, updated: NotebookLMRemoteAuthSession) -> NotebookLMRemoteAuthSession:
        sessions = self._load()
        for index, session in enumerate(sessions):
            if session.session_id == updated.session_id:
                sessions[index] = updated
                self._write(sessions)
                return updated
        raise KeyError(f"Remote auth session not found: {updated.session_id}")

    def expire_stale_sessions(self) -> list[NotebookLMRemoteAuthSession]:
        now = _utc_now()
        sessions = self._load()
        expired: list[NotebookLMRemoteAuthSession] = []
        changed = False
        for session in sessions:
            expires_at = _parse_dt(session.expires_at)
            if expires_at and expires_at <= now and session.status not in {
                "completed",
                "expired",
                "failed",
                "cancelled",
            }:
                session.status = "expired"
                session.error = session.error or "Auth session expired."
                expired.append(session)
                changed = True
        if changed:
            self._write(sessions)
        return expired


class RemoteAuthConfigurationError(RuntimeError):
    """Raised when remote auth prerequisites are not configured."""


class DockerRemoteBrowserLauncher:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _socket_path(self) -> str:
        configured = getattr(self._settings, "notebooklm_remote_auth_docker_socket", "") or ""
        value = configured.strip()
        if not value:
            raise RemoteAuthConfigurationError(
                "NotebookLM remote auth is not configured: NOTEBOOKLM_REMOTE_AUTH_DOCKER_SOCKET is empty."
            )
        return value

    def _image(self) -> str:
        configured = getattr(self._settings, "notebooklm_remote_auth_selenium_image", "") or ""
        value = configured.strip()
        if not value:
            raise RemoteAuthConfigurationError(
                "NotebookLM remote auth is not configured: NOTEBOOKLM_REMOTE_AUTH_SELENIUM_IMAGE is empty."
            )
        return value

    def _novnc_port(self) -> int:
        value = int(getattr(self._settings, "notebooklm_remote_auth_novnc_port", 47900) or 47900)
        if value <= 0:
            raise RemoteAuthConfigurationError(
                "NotebookLM remote auth noVNC port must be a positive integer."
            )
        return value

    def _memory_limit_bytes(self) -> int:
        value_mb = int(getattr(self._settings, "notebooklm_remote_auth_memory_limit_mb", 1024) or 1024)
        return max(256, value_mb) * 1024 * 1024

    def _memory_swap_limit_bytes(self, memory_limit: int) -> int:
        value_mb = int(getattr(self._settings, "notebooklm_remote_auth_memory_swap_limit_mb", 1024) or 1024)
        return max(memory_limit, max(256, value_mb) * 1024 * 1024)

    def _proxy_url(self) -> str:
        proxy_url = get_notebooklm_proxy_url(self._settings)
        if proxy_url:
            return proxy_url
        raise RemoteAuthConfigurationError(
            "NotebookLM remote auth requires NOTEBOOKLM_PROXY_ENABLED=true and NOTEBOOKLM_PROXY_URL."
        )

    async def _docker_client(self) -> httpx.AsyncClient:
        socket_path = self._socket_path()
        transport = httpx.AsyncHTTPTransport(uds=socket_path)
        return httpx.AsyncClient(transport=transport, base_url="http://docker")

    async def _ensure_image(self, client: httpx.AsyncClient) -> None:
        image = self._image()
        response = await client.post("/images/create", params={"fromImage": image}, timeout=120.0)
        if response.status_code not in {200, 201, 204}:
            raise RuntimeError(f"Failed to pull Selenium image {image}: {response.text[:500]}")

    async def launch(self, *, public_base_url: str, session_id: str) -> NotebookLMRemoteAuthBrowser:
        webdriver_port = _REMOTE_AUTH_WEBDRIVER_PORT
        vnc_port = _find_free_port()
        novnc_port = self._novnc_port()
        vnc_password = secrets.token_urlsafe(12)
        container_name = f"tgctxbot-nlm-auth-{session_id[:8]}"
        image = self._image()
        proxy_url = self._proxy_url()
        memory_limit = self._memory_limit_bytes()
        memory_swap_limit = self._memory_swap_limit_bytes(memory_limit)

        async with await self._docker_client() as client:
            create_payload = {
                "Image": image,
                "Env": [
                    f"SE_VNC_PASSWORD={vnc_password}",
                    "SE_VNC_NO_PASSWORD=false",
                    "SE_START_VNC=true",
                    "SE_START_NO_VNC=true",
                    f"SE_VNC_PORT={vnc_port}",
                    f"SE_NO_VNC_PORT={novnc_port}",
                    "SE_NODE_MAX_SESSIONS=1",
                    "SE_NODE_OVERRIDE_MAX_SESSIONS=true",
                    f"SE_BROWSER_ARGS_PROXY=--proxy-server={proxy_url}",
                ],
                "Labels": {
                    "app.telegram-context-search-bot": "notebooklm-remote-auth",
                    "app.telegram-context-search-bot.session-id": session_id,
                },
                "HostConfig": {
                    "AutoRemove": True,
                    "ShmSize": 2147483648,
                    "NetworkMode": "host",
                    "Memory": memory_limit,
                    "MemorySwap": memory_swap_limit,
                },
            }

            response = await client.post(
                "/containers/create",
                params={"name": container_name},
                json=create_payload,
                timeout=30.0,
            )
            if response.status_code == 404:
                await self._ensure_image(client)
                response = await client.post(
                    "/containers/create",
                    params={"name": container_name},
                    json=create_payload,
                    timeout=30.0,
                )
            if response.status_code not in {201}:
                raise RuntimeError(f"Failed to create remote auth browser: {response.text[:500]}")
            container_id = str(response.json().get("Id", "") or "")
            start_response = await client.post(f"/containers/{container_id}/start", timeout=30.0)
            if start_response.status_code not in {204}:
                raise RuntimeError(f"Failed to start remote auth browser: {start_response.text[:500]}")

        webdriver_base = f"http://127.0.0.1:{webdriver_port}/wd/hub"
        await self._wait_for_webdriver(webdriver_base)
        webdriver_session_id = await self._create_webdriver_session(webdriver_base)
        await self.navigate(webdriver_port, webdriver_session_id, _NOTEBOOKLM_URL)
        browser_url = self._build_browser_url(public_base_url, novnc_port, vnc_password)
        return NotebookLMRemoteAuthBrowser(
            container_id=container_id,
            container_name=container_name,
            webdriver_port=webdriver_port,
            novnc_port=novnc_port,
            vnc_password=vnc_password,
            webdriver_session_id=webdriver_session_id,
            browser_url=browser_url,
            started_at=_isoformat(_utc_now()) or "",
        )

    async def _wait_for_webdriver(self, webdriver_base: str) -> None:
        deadline = _utc_now() + timedelta(seconds=60)
        async with httpx.AsyncClient() as client:
            while _utc_now() < deadline:
                try:
                    response = await client.get(f"{webdriver_base}/status", timeout=5.0)
                    if response.status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(1)
        raise RuntimeError("Timed out waiting for remote auth browser to become ready.")

    async def _create_webdriver_session(self, webdriver_base: str) -> str:
        payload = {
            "capabilities": {
                "alwaysMatch": {
                    "browserName": "chrome",
                    "goog:chromeOptions": {
                        "args": [
                            "--disable-dev-shm-usage",
                            "--no-sandbox",
                            "--password-store=basic",
                        ]
                    },
                }
            }
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{webdriver_base}/session", json=payload, timeout=30.0)
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"Failed to create WebDriver session: {response.text[:500]}")
        body = response.json()
        session_id = body.get("sessionId") or body.get("value", {}).get("sessionId")
        if not session_id:
            raise RuntimeError("WebDriver session response did not contain sessionId.")
        return str(session_id)

    async def navigate(self, webdriver_port: int, webdriver_session_id: str, url: str) -> None:
        webdriver_base = f"http://127.0.0.1:{webdriver_port}/wd/hub"
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{webdriver_base}/session/{webdriver_session_id}/url",
                json={"url": url},
                timeout=30.0,
            )
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"Failed to navigate remote auth browser: {response.text[:500]}")

    async def get_current_url(self, webdriver_port: int, webdriver_session_id: str) -> str:
        webdriver_base = f"http://127.0.0.1:{webdriver_port}/wd/hub"
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{webdriver_base}/session/{webdriver_session_id}/url",
                timeout=15.0,
            )
        if response.status_code not in {200}:
            raise RuntimeError(f"Failed to read remote auth browser URL: {response.text[:500]}")
        return str(response.json().get("value", "") or "")

    async def get_cookies(self, webdriver_port: int, webdriver_session_id: str) -> list[dict[str, Any]]:
        webdriver_base = f"http://127.0.0.1:{webdriver_port}/wd/hub"
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{webdriver_base}/session/{webdriver_session_id}/cookie",
                timeout=15.0,
            )
        if response.status_code not in {200}:
            raise RuntimeError(f"Failed to read remote auth browser cookies: {response.text[:500]}")
        value = response.json().get("value", [])
        if not isinstance(value, list):
            return []
        return [item for item in value if isinstance(item, dict)]

    async def cleanup(self, browser: NotebookLMRemoteAuthBrowser | None) -> None:
        if browser is None:
            return
        webdriver_base = f"http://127.0.0.1:{browser.webdriver_port}/wd/hub"
        try:
            async with httpx.AsyncClient() as client:
                await client.delete(
                    f"{webdriver_base}/session/{browser.webdriver_session_id}",
                    timeout=15.0,
                )
        except Exception:
            logger.exception("remote-auth webdriver cleanup failed session_id=%s", browser.webdriver_session_id)
        try:
            async with await self._docker_client() as client:
                await client.delete(
                    f"/containers/{browser.container_id}",
                    params={"force": "1"},
                    timeout=30.0,
                )
        except Exception:
            logger.exception("remote-auth container cleanup failed container_id=%s", browser.container_id)

    async def list_remote_auth_containers(self) -> list[dict[str, Any]]:
        filters = json.dumps({"label": ["app.telegram-context-search-bot=notebooklm-remote-auth"]})
        async with await self._docker_client() as client:
            response = await client.get(
                "/containers/json",
                params={"all": "1", "filters": filters},
                timeout=30.0,
            )
        if response.status_code != 200:
            raise RuntimeError(f"Failed to list remote auth containers: {response.text[:500]}")
        payload = response.json()
        if not isinstance(payload, list):
            return []
        return [item for item in payload if isinstance(item, dict)]

    async def remove_container(self, container_id: str) -> None:
        async with await self._docker_client() as client:
            response = await client.delete(
                f"/containers/{container_id}",
                params={"force": "1"},
                timeout=30.0,
            )
        if response.status_code not in {204, 404}:
            raise RuntimeError(f"Failed to remove remote auth container {container_id}: {response.text[:500]}")

    @staticmethod
    def _build_browser_url(public_base_url: str, novnc_port: int, vnc_password: str) -> str:
        parsed = urlparse(public_base_url)
        if not parsed.scheme or not parsed.hostname:
            raise RemoteAuthConfigurationError(
                "NotebookLM remote auth requires a valid NOTEBOOKLM_REMOTE_AUTH_BASE_URL."
            )
        host = parsed.hostname
        scheme = parsed.scheme
        return (
            f"{scheme}://{host}:{novnc_port}/?autoconnect=1&resize=scale"
            f"&password={vnc_password}&view_only=0"
        )


class NotebookLMRemoteAuthManager:
    def __init__(
        self,
        *,
        settings=None,
        store: NotebookLMRemoteAuthStore | None = None,
        runtime_store: NotebookLMRuntimeStore | None = None,
        launcher: DockerRemoteBrowserLauncher | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._store = store or NotebookLMRemoteAuthStore(settings=self._settings)
        self._runtime_store = runtime_store or NotebookLMRuntimeStore(settings=self._settings)
        self._launcher = launcher or DockerRemoteBrowserLauncher(settings=self._settings)
        self._monitor_tasks: dict[str, asyncio.Task[None]] = {}
        self._janitor_task: asyncio.Task[None] | None = None
        self._last_keepalive_monotonic: float | None = None

    def _ttl_seconds(self) -> int:
        value = int(getattr(self._settings, "notebooklm_remote_auth_ttl_seconds", 900) or 900)
        return max(60, value)

    def _poll_interval(self) -> float:
        value = float(getattr(self._settings, "notebooklm_remote_auth_poll_seconds", 5) or 5)
        return max(1.0, value)

    def _janitor_enabled(self) -> bool:
        return bool(
            getattr(self._settings, "notebooklm_enabled", False)
            and getattr(self._settings, "notebooklm_vps_lightweight_mode", False)
            and getattr(self._settings, "notebooklm_janitor_enabled", True)
        )

    def _janitor_interval_seconds(self) -> float:
        value = float(getattr(self._settings, "notebooklm_janitor_interval_seconds", 60) or 60)
        return max(5.0, value)

    def _keepalive_interval_seconds(self) -> float:
        value = float(getattr(self._settings, "notebooklm_cookie_keepalive_interval_seconds", 420) or 420)
        return max(60.0, value)

    def _resolve_public_base_url(self, fallback_base_url: str | None = None) -> str:
        configured = getattr(self._settings, "notebooklm_remote_auth_base_url", "") or ""
        value = configured.strip()
        if value:
            return value.rstrip("/")
        if fallback_base_url:
            return fallback_base_url.rstrip("/")
        raise RemoteAuthConfigurationError(
            "NotebookLM remote auth requires NOTEBOOKLM_REMOTE_AUTH_BASE_URL for bot-generated links."
        )

    def _build_auth_url(self, token: str, fallback_base_url: str | None = None) -> str:
        base_url = self._resolve_public_base_url(fallback_base_url)
        return f"{base_url}/auth-session/remote-auth/{token}"

    def _safe_auth_url(self, token: str, fallback_base_url: str | None = None) -> str | None:
        try:
            return self._build_auth_url(token, fallback_base_url)
        except RemoteAuthConfigurationError:
            return None

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
        token = secrets.token_urlsafe(32)
        session = self._store.create_session(
            token_hash=self._store._token_hash(token),  # noqa: SLF001
            source=source,
            requested_by_user_id=requested_by_user_id,
            requested_by_chat_id=requested_by_chat_id,
            notify_chat_id=notify_chat_id,
            notify_message_thread_id=notify_message_thread_id,
            ttl_seconds=self._ttl_seconds(),
        )
        auth_url = self._build_auth_url(token, fallback_base_url)
        return self._session_public_payload(
            session,
            include_browser=True,
            auth_url=auth_url,
        )

    def get_session_by_token(self, token: str) -> NotebookLMRemoteAuthSession:
        session = self._store.get_by_token(token)
        if session is None:
            raise KeyError("NotebookLM auth session not found.")
        return session

    async def _cancel_other_launched_sessions(self, keep_session_id: str) -> None:
        for existing in self._store.list_sessions():
            if existing.session_id == keep_session_id:
                continue
            if existing.status != "launched" or existing.browser is None:
                continue
            await self._cleanup_session(existing)
            existing.status = "cancelled"
            existing.error = "Auth session replaced by a newer login link."
            existing.completed_at = existing.completed_at or _isoformat(_utc_now())
            self._store.update_session(existing)

    async def ensure_session_started(self, token: str, *, fallback_base_url: str | None = None) -> dict[str, Any]:
        session = self.get_session_by_token(token)
        if session.status == "pending":
            await self._cancel_other_launched_sessions(session.session_id)
            public_base_url = self._resolve_public_base_url(fallback_base_url)
            browser = await self._launcher.launch(
                public_base_url=public_base_url,
                session_id=session.session_id,
            )
            session.browser = browser
            session.status = "launched"
            session.launched_at = _isoformat(_utc_now())
            session.error = None
            self._store.update_session(session)
            self._schedule_monitor(session.session_id)
        elif session.status == "launched":
            self._schedule_monitor(session.session_id)
        return self._session_public_payload(
            self.get_session_by_token(token),
            include_browser=True,
            auth_url=self._safe_auth_url(token, fallback_base_url),
        )

    def get_session_status(self, token: str) -> dict[str, Any]:
        session = self.get_session_by_token(token)
        return self._session_public_payload(
            session,
            include_browser=True,
            auth_url=self._safe_auth_url(token),
        )

    async def cancel_session(self, token: str) -> dict[str, Any]:
        session = self.get_session_by_token(token)
        await self._cleanup_session(session)
        session.status = "cancelled"
        session.error = "Auth session cancelled by operator."
        session.completed_at = session.completed_at or _isoformat(_utc_now())
        self._store.update_session(session)
        return self._session_public_payload(
            session,
            include_browser=True,
            auth_url=self._safe_auth_url(token),
        )

    def get_latest_session(self) -> dict[str, Any] | None:
        sessions = self._store.list_sessions()
        if not sessions:
            return None
        latest = max(sessions, key=lambda item: item.created_at or "")
        return self._session_public_payload(latest, include_browser=True)

    async def reconcile(self) -> None:
        expired = self._store.expire_stale_sessions()
        for session in expired:
            await self._cleanup_session(session)
        for session in self._store.list_sessions():
            if session.status == "launched" and session.browser:
                self._schedule_monitor(session.session_id)
        await self.run_janitor_pass()

    async def run_janitor_pass(self) -> int:
        if not self._janitor_enabled():
            return 0

        reaped = 0
        removed_container_ids: set[str] = set()
        sessions = self._store.list_sessions()
        sessions_by_id = {session.session_id: session for session in sessions}
        cutoff = _utc_now() - timedelta(seconds=self._ttl_seconds())

        for session in sessions:
            if session.browser is None or not session.browser.container_id:
                continue
            container_id = session.browser.container_id
            launched_at = _parse_dt(session.launched_at) or _parse_dt(session.created_at)
            ttl_expired = launched_at is not None and launched_at <= cutoff
            terminal = session.status in _TERMINAL_SESSION_STATUSES
            if not terminal and not ttl_expired:
                continue
            reason = "terminal_session" if terminal else "ttl_exceeded"
            await self._cleanup_session(session)
            removed_container_ids.add(container_id)
            if not terminal:
                session.status = "expired"
                session.error = session.error or "Auth session expired."
                session.completed_at = session.completed_at or _isoformat(_utc_now())
                self._store.update_session(session)
            log_event(
                logger,
                logging.INFO,
                "nlm.auth.container.reaped",
                session_id=session.session_id,
                container_id=container_id,
                reason=reason,
            )
            reaped += 1

        try:
            containers = await self._launcher.list_remote_auth_containers()
        except (AttributeError, RemoteAuthConfigurationError):
            await self._maybe_run_cookie_keepalive()
            return reaped

        for container in containers:
            container_id = str(container.get("Id", "") or "")
            if not container_id or container_id in removed_container_ids:
                continue
            labels = container.get("Labels")
            labels = labels if isinstance(labels, dict) else {}
            session_id = str(labels.get("app.telegram-context-search-bot.session-id", "") or "")
            session = sessions_by_id.get(session_id)
            terminal = session is not None and session.status in _TERMINAL_SESSION_STATUSES
            created_raw = container.get("Created")
            created_at = None
            if isinstance(created_raw, (int, float)):
                created_at = datetime.fromtimestamp(float(created_raw), tz=timezone.utc)
            ttl_expired = created_at is not None and created_at <= cutoff
            if not terminal and not ttl_expired:
                continue
            reason = "terminal_session" if terminal else "ttl_exceeded"
            await self._launcher.remove_container(container_id)
            log_event(
                logger,
                logging.INFO,
                "nlm.auth.container.reaped",
                session_id=session_id or None,
                container_id=container_id,
                reason=reason,
            )
            reaped += 1

        await self._maybe_run_cookie_keepalive()
        return reaped

    def start_janitor(self) -> None:
        if not self._janitor_enabled():
            return
        if self._janitor_task is not None and not self._janitor_task.done():
            return
        self._janitor_task = asyncio.create_task(
            self._run_janitor_loop(),
            name="notebooklm-auth-janitor",
        )

    async def stop_janitor(self) -> None:
        task = self._janitor_task
        self._janitor_task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run_janitor_loop(self) -> None:
        try:
            while True:
                try:
                    await self.run_janitor_pass()
                except Exception:
                    logger.exception("remote-auth janitor pass failed")
                await asyncio.sleep(self._janitor_interval_seconds())
        except asyncio.CancelledError:
            raise

    async def _maybe_run_cookie_keepalive(self) -> bool:
        resolve_storage_state_path = getattr(self._runtime_store, "resolve_storage_state_path", None)
        replace_storage_state = getattr(self._runtime_store, "replace_storage_state", None)
        if not callable(resolve_storage_state_path) or not callable(replace_storage_state):
            return False

        now = time.monotonic()
        last_run = self._last_keepalive_monotonic
        if last_run is not None and (now - last_run) < self._keepalive_interval_seconds():
            return False
        self._last_keepalive_monotonic = now

        storage_path = str(resolve_storage_state_path() or "").strip()
        if not storage_path:
            return False
        if not Path(storage_path).expanduser().exists():
            logger.debug("remote-auth keepalive skipped: storage state missing path=%s", storage_path)
            return False

        try:
            storage_state = await refresh_notebooklm_google_keepalive(
                storage_path,
                float(getattr(self._settings, "notebooklm_timeout", 30.0) or 30.0),
                get_notebooklm_proxy_url(self._settings),
            )
        except Exception as exc:
            logger.warning("remote-auth keepalive refresh failed: %s", exc)
            return False

        replace_storage_state(json.dumps(storage_state, ensure_ascii=False))
        log_event(
            logger,
            logging.INFO,
            "nlm.auth.keepalive.persisted",
            storage_state_path=storage_path,
            cookie_count=len(storage_state.get("cookies", [])),
        )
        return True

    def _schedule_monitor(self, session_id: str) -> None:
        existing = self._monitor_tasks.get(session_id)
        if existing and not existing.done():
            return
        task = asyncio.create_task(self._monitor_session(session_id), name=f"notebooklm-auth-{session_id}")
        self._monitor_tasks[session_id] = task

    async def _monitor_session(self, session_id: str) -> None:
        try:
            while True:
                session = self._store.get_by_id(session_id)
                if session is None or session.status != "launched" or session.browser is None:
                    return
                expires_at = _parse_dt(session.expires_at)
                if expires_at and expires_at <= _utc_now():
                    session.status = "expired"
                    session.error = "Auth session expired before NotebookLM login completed."
                    self._store.update_session(session)
                    await self._cleanup_session(session)
                    await self._notify(session, "Авторизация NotebookLM истекла до завершения входа.")
                    return

                try:
                    if await self._maybe_capture_authenticated_state(session):
                        return
                except Exception as exc:
                    logger.exception("remote-auth monitor failed session_id=%s", session_id)
                    session.status = "failed"
                    session.error = str(exc)
                    self._store.update_session(session)
                    await self._cleanup_session(session)
                    await self._notify(
                        session,
                        "Не удалось обновить авторизацию NotebookLM. Проверь админку и попробуй ещё раз.",
                    )
                    return

                await asyncio.sleep(self._poll_interval())
        finally:
            self._monitor_tasks.pop(session_id, None)

    async def _maybe_capture_authenticated_state(self, session: NotebookLMRemoteAuthSession) -> bool:
        assert session.browser is not None
        current_url = await self._launcher.get_current_url(
            session.browser.webdriver_port,
            session.browser.webdriver_session_id,
        )
        parsed = urlparse(current_url)
        if parsed.hostname != "notebooklm.google.com":
            return False

        await self._launcher.navigate(
            session.browser.webdriver_port,
            session.browser.webdriver_session_id,
            _GOOGLE_ACCOUNTS_URL,
        )
        google_cookies = await self._launcher.get_cookies(
            session.browser.webdriver_port,
            session.browser.webdriver_session_id,
        )
        await self._launcher.navigate(
            session.browser.webdriver_port,
            session.browser.webdriver_session_id,
            _NOTEBOOKLM_URL,
        )
        notebook_cookies = await self._launcher.get_cookies(
            session.browser.webdriver_port,
            session.browser.webdriver_session_id,
        )
        merged_cookies = self._merge_cookies(google_cookies + notebook_cookies)
        cookie_names = {cookie["name"] for cookie in merged_cookies if "name" in cookie}
        if not (_AUTH_COOKIE_NAMES & cookie_names):
            return False

        storage_state = {"cookies": merged_cookies, "origins": []}
        self._runtime_store.replace_storage_state(json.dumps(storage_state, ensure_ascii=False))

        from app.services.notebooklm_service import NotebookLMService

        await NotebookLMService.invalidate_cached_client()
        session.status = "completed"
        session.completed_at = _isoformat(_utc_now())
        session.error = None
        self._store.update_session(session)
        await self._cleanup_session(session)
        await self._notify(session, "Авторизация NotebookLM обновлена.")
        return True

    @staticmethod
    def _merge_cookies(raw_cookies: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[tuple[str, str, str], dict[str, Any]] = {}
        for cookie in raw_cookies:
            name = str(cookie.get("name", "") or "")
            domain = str(cookie.get("domain", "") or "")
            path = str(cookie.get("path", "/") or "/")
            if not name or not domain:
                continue
            same_site = str(cookie.get("sameSite", "Lax") or "Lax")
            if same_site not in {"Strict", "Lax", "None"}:
                same_site = "Lax"
            merged[(name, domain, path)] = {
                "name": name,
                "value": str(cookie.get("value", "") or ""),
                "domain": domain,
                "path": path,
                "expires": float(cookie.get("expiry", cookie.get("expires", -1)) or -1),
                "httpOnly": bool(cookie.get("httpOnly", False)),
                "secure": bool(cookie.get("secure", False)),
                "sameSite": same_site,
            }
        return list(merged.values())

    async def _cleanup_session(self, session: NotebookLMRemoteAuthSession) -> None:
        await self._launcher.cleanup(session.browser)
        if session.browser is not None:
            session.browser = None
            self._store.update_session(session)

    async def _notify(self, session: NotebookLMRemoteAuthSession, text: str) -> None:
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
            logger.exception("remote-auth telegram notification failed session_id=%s", session.session_id)

    @staticmethod
    def _session_public_payload(
        session: NotebookLMRemoteAuthSession,
        *,
        include_browser: bool,
        auth_url: str | None = None,
    ) -> dict[str, Any]:
        browser_url = session.browser.browser_url if session.browser and include_browser else None
        return {
            "session_id": session.session_id,
            "source": session.source,
            "requested_via": session.source,
            "status": session.status,
            "auth_url": auth_url or session.auth_url or None,
            "entry_url": auth_url or session.auth_url or None,
            "browser_url": browser_url,
            "viewer_url": browser_url,
            "expires_at": session.expires_at,
            "launched_at": session.launched_at,
            "completed_at": session.completed_at,
            "error": session.error,
        }


_remote_auth_manager: NotebookLMRemoteAuthManager | None = None


def get_notebooklm_remote_auth_manager() -> NotebookLMRemoteAuthManager:
    global _remote_auth_manager
    if _remote_auth_manager is None:
        _remote_auth_manager = NotebookLMRemoteAuthManager()
    return _remote_auth_manager
