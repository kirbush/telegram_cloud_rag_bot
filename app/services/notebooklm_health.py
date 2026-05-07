from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from app.core.config import get_notebooklm_proxy_url, get_settings, is_notebooklm_source_sync_enabled
from app.services.notebooklm_client import load_notebooklm_auth
from app.services.notebooklm_events import log_event
from app.services.notebooklm_metrics import set_readiness, set_storage_state_age_seconds, set_tunnel_up
from app.services.notebooklm_runtime import NotebookLMRuntimeStore

logger = logging.getLogger(__name__)

_DEFAULT_TELEGRAM_PROXY_URL = "http://127.0.0.1:43128"

@dataclass(slots=True)
class NotebookLMHealthSnapshot:
    live: bool
    ready: bool
    checked_at: str
    reason: str | None = None
    settings_loaded_at: str | None = None
    storage_state_path: str | None = None
    storage_state_age_seconds: int | None = None
    sync_state_age_seconds: int | None = None
    telegram_tunnel_up: bool = False
    notebooklm_tunnel_up: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class NotebookLMHealthService:
    _cache_lock = asyncio.Lock()
    _cached_snapshot: NotebookLMHealthSnapshot | None = None
    _cached_at: datetime | None = None

    async def readiness(self, *, force: bool = False) -> NotebookLMHealthSnapshot:
        settings = get_settings()
        cache_ttl_s = max(int(getattr(settings, "notebooklm_health_cache_seconds", 30) or 30), 1)
        async with self._cache_lock:
            if (
                not force
                and self._cached_snapshot is not None
                and self._cached_at is not None
                and (datetime.now(UTC) - self._cached_at) < timedelta(seconds=cache_ttl_s)
            ):
                return self._cached_snapshot
            snapshot = await self._build_snapshot()
            self.__class__._cached_snapshot = snapshot
            self.__class__._cached_at = datetime.now(UTC)
            return snapshot

    async def _build_snapshot(self) -> NotebookLMHealthSnapshot:
        settings = get_settings()
        runtime_store = NotebookLMRuntimeStore(settings=settings)
        runtime_status = runtime_store.get_runtime_status()
        checked_at = datetime.now(UTC).isoformat()
        snapshot = NotebookLMHealthSnapshot(
            live=True,
            ready=False,
            checked_at=checked_at,
            settings_loaded_at=str(getattr(settings, "settings_loaded_at", "") or "") or None,
            storage_state_path=str(runtime_status.get("storage_state_path", "") or "") or None,
        )

        if not bool(runtime_status.get("enabled", False)):
            snapshot.reason = "not_enabled"
            self._record_metrics(snapshot)
            return snapshot

        storage_path = Path(str(runtime_status.get("storage_state_path", "") or "")).expanduser()
        if not storage_path.exists():
            snapshot.reason = "storage_missing"
            self._record_metrics(snapshot)
            return snapshot

        storage_mtime = datetime.fromtimestamp(storage_path.stat().st_mtime, tz=UTC)
        storage_age_seconds = max(int((datetime.now(UTC) - storage_mtime).total_seconds()), 0)
        snapshot.storage_state_age_seconds = storage_age_seconds
        max_storage_age_days = max(int(getattr(settings, "notebooklm_ready_storage_max_age_days", 14) or 14), 1)
        if storage_age_seconds > max_storage_age_days * 24 * 60 * 60:
            snapshot.reason = "auth_expired"
            self._record_metrics(snapshot)
            return snapshot

        telegram_tunnel_up = await self._probe_telegram_tunnel(settings)
        notebooklm_tunnel_up = await self._probe_notebooklm_tunnel(settings)
        snapshot.telegram_tunnel_up = telegram_tunnel_up
        snapshot.notebooklm_tunnel_up = notebooklm_tunnel_up

        if not telegram_tunnel_up:
            snapshot.reason = "tunnel_down:telegram"
            log_event(logger, logging.WARNING, "nlm.tunnel.flap", which="telegram", state="down")
            self._record_metrics(snapshot)
            return snapshot
        if not notebooklm_tunnel_up:
            snapshot.reason = "tunnel_down:notebooklm"
            log_event(logger, logging.WARNING, "nlm.tunnel.flap", which="notebooklm", state="down")
            self._record_metrics(snapshot)
            return snapshot

        if is_notebooklm_source_sync_enabled(settings):
            sync_state_path = Path(
                str(getattr(settings, "notebooklm_source_sync_state_path", ".state/notebooklm/source_sync_state.json") or "")
            ).expanduser()
            if not sync_state_path.exists():
                snapshot.reason = "sync_stale"
                self._record_metrics(snapshot)
                return snapshot
            sync_mtime = datetime.fromtimestamp(sync_state_path.stat().st_mtime, tz=UTC)
            sync_age_seconds = max(int((datetime.now(UTC) - sync_mtime).total_seconds()), 0)
            snapshot.sync_state_age_seconds = sync_age_seconds
            max_sync_age_hours = max(int(getattr(settings, "notebooklm_ready_sync_max_age_hours", 36) or 36), 1)
            if sync_age_seconds > max_sync_age_hours * 60 * 60:
                snapshot.reason = "sync_stale"
                self._record_metrics(snapshot)
                return snapshot

        try:
            await load_notebooklm_auth(
                storage_path,
                float(getattr(settings, "notebooklm_timeout", 30.0) or 30.0),
                get_notebooklm_proxy_url(settings),
            )
        except Exception:
            snapshot.reason = "auth_expired"
            self._record_metrics(snapshot)
            return snapshot

        snapshot.ready = True
        self._record_metrics(snapshot)
        return snapshot

    @staticmethod
    def cooldown_minutes(reason: str | None, settings=None) -> int:
        effective_settings = settings or get_settings()
        default_cooldown = max(
            int(getattr(effective_settings, "notebooklm_bot_unavailable_cooldown_minutes", 5) or 5),
            1,
        )
        if reason == "sync_stale":
            return max(default_cooldown, 15)
        if reason == "auth_expired":
            return max(default_cooldown, 10)
        if reason == "storage_missing":
            return max(default_cooldown, 10)
        return default_cooldown

    async def _probe_telegram_tunnel(self, settings) -> bool:
        bot_token = str(getattr(settings, "bot_token", "") or "").strip()
        if not bot_token:
            return False
        proxy_url = str(getattr(settings, "telegram_proxy_url", "") or "").strip() or _DEFAULT_TELEGRAM_PROXY_URL
        return await self._probe_url(
            f"https://api.telegram.org/bot{bot_token}/getMe",
            proxy_url=proxy_url,
        )

    async def _probe_notebooklm_tunnel(self, settings) -> bool:
        proxy_url = get_notebooklm_proxy_url(settings)
        if not proxy_url:
            return False
        return await self._probe_url("https://notebooklm.google.com/", proxy_url=proxy_url)

    @staticmethod
    async def _probe_url(url: str, *, proxy_url: str) -> bool:
        try:
            async with httpx.AsyncClient(proxy=proxy_url, follow_redirects=False, timeout=10.0) as client:
                response = await client.get(url)
        except Exception:
            return False
        return response.status_code < 500

    @staticmethod
    def _record_metrics(snapshot: NotebookLMHealthSnapshot) -> None:
        set_storage_state_age_seconds(float(snapshot.storage_state_age_seconds or 0))
        set_tunnel_up("telegram", snapshot.telegram_tunnel_up)
        set_tunnel_up("notebooklm", snapshot.notebooklm_tunnel_up)
        set_readiness("ok" if snapshot.ready else (snapshot.reason or "storage_missing"))
