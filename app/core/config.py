import os
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = os.getenv("APP_ENV_FILE", ".env")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=_ENV_FILE, env_file_encoding="utf-8", extra="ignore")
    settings_loaded_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())

    bot_token: str
    bot_instance_name: str = ""
    openai_api_key: str = ""
    openai_base_url: str = "https://api.openai.com/v1"
    openai_model: str = "gpt-5.4-mini"

    # NotebookLM integration
    notebooklm_enabled: bool = True
    notebooklm_storage_state: str = "~/.notebooklm/storage_state.json"
    notebooklm_notebook_map: dict[str, str] = {}
    notebooklm_default_notebook: str = ""
    notebooklm_timeout: int = 60
    notebooklm_runtime_state_path: str = ""
    notebooklm_runtime_storage_state: str = ".tmp/notebooklm/storage_state.json"
    notebooklm_auth_account_key: str = ""
    notebooklm_shared_storage_state: str = ""
    notebooklm_admin_username: str = "admin"
    notebooklm_admin_password: str = ""
    notebooklm_admin_bind_host: str = "127.0.0.1"
    notebooklm_proxy_enabled: bool = False
    notebooklm_proxy_url: str | None = None
    notebooklm_refresh_cmd: str = ""
    media_context_enabled: bool = False
    reaction_context_enabled: bool = False
    openai_vision_model: str = "gpt-5.4-nano"
    media_context_timeout_seconds: float = 60.0
    media_context_max_retries: int = 2
    notebooklm_remote_auth_base_url: str = ""
    notebooklm_remote_auth_state_path: str = ".state/notebooklm/auth_sessions.json"
    notebooklm_remote_auth_ttl_seconds: int = 900
    notebooklm_remote_auth_poll_seconds: float = 5.0
    notebooklm_remote_auth_docker_socket: str = "/var/run/docker.sock"
    notebooklm_remote_auth_selenium_image: str = "selenium/standalone-chromium:4.34.0"
    notebooklm_remote_auth_novnc_port: int = 47900
    notebooklm_remote_auth_memory_limit_mb: int = 1024
    notebooklm_remote_auth_memory_swap_limit_mb: int = 1024
    notebooklm_upload_session_state_path: str = ".state/notebooklm/upload_sync_state.json"
    notebooklm_upload_session_ttl_seconds: int = 900
    notebooklm_upload_refresh_ttl_seconds: int = 2592000
    notebooklm_windows_helper_protocol_scheme: str = "tgctxbot-notebooklm-sync"
    notebooklm_source_sync_state_path: str = ".state/notebooklm/source_sync_state.json"
    notebooklm_source_sync_export_dir: str = ".state/notebooklm/exports"
    notebooklm_source_sync_bootstrap_cutoff_date: str = "2026-04-10"
    notebooklm_source_sync_max_words_per_source: int = 500000
    notebooklm_source_sync_max_sources_per_notebook: int = 50
    notebooklm_source_sync_enabled: bool = True
    notebooklm_background_sync_enabled: bool = False
    notebooklm_background_sync_timezone: str = "Europe/Moscow"
    notebooklm_background_sync_hour: int = 3
    notebooklm_background_sync_minute: int = 0
    notebooklm_lightweight_history_path: str = ".state/notebooklm/history.sqlite3"
    bot_access_state_path: str = ".state/bot/access.sqlite3"
    bot_conversation_state_path: str = ".state/bot/conversations.sqlite3"
    notebooklm_health_cache_seconds: int = 30
    notebooklm_ready_storage_max_age_days: int = 14
    notebooklm_ready_sync_max_age_hours: int = 36
    notebooklm_sync_ticks_path: str = ".state/notebooklm/sync_ticks.jsonl"
    notebooklm_sync_tick_retention_days: int = 30
    notebooklm_bot_unavailable_cooldown_minutes: int = 5
    notebooklm_janitor_enabled: bool = True
    notebooklm_janitor_interval_seconds: int = 60
    notebooklm_cookie_keepalive_interval_seconds: int = 420
    # Append a "--- Sources ---" block to NotebookLM answers sent via the bot.
    # The admin UI always shows sources regardless.
    bot_nlm_show_sources: bool = False
    bot_reply_in_direct_messages_topic: bool = False
    bot_admin_user_ids: str | None = None
    uptime_kuma_public_url: str = ""
    telegram_proxy_enabled: bool = False
    telegram_proxy_url: str | None = None

    use_webhook: bool = False
    webhook_base_url: str | None = None
    telegram_chat_alias_map: str | None = None
    telegram_imported_chat_context_map: str | None = None
    telegram_imported_chat_link_map: str | None = None
    log_level: str = "INFO"

    def model_post_init(self, __context) -> None:
        admin_password = str(getattr(self, "notebooklm_admin_password", "") or "").strip()
        if not admin_password:
            raise ValueError("NotebookLM runtime requires NOTEBOOKLM_ADMIN_PASSWORD.")
        runtime_overlay_path = str(getattr(self, "notebooklm_runtime_state_path", "") or "").strip()
        storage_state_path = Path(
            str(getattr(self, "notebooklm_storage_state", "~/.notebooklm/storage_state.json") or "")
        ).expanduser()
        if not storage_state_path.exists() and not runtime_overlay_path:
            raise ValueError(
                "NotebookLM runtime requires an existing NOTEBOOKLM_STORAGE_STATE "
                "or NOTEBOOKLM_RUNTIME_STATE_PATH."
            )


def _settings_cache_signature() -> tuple[str, int, int]:
    env_path = Path(_ENV_FILE)
    try:
        stat = env_path.stat()
    except OSError:
        return (_ENV_FILE, -1, -1)
    return (_ENV_FILE, int(stat.st_mtime_ns), int(stat.st_size))


@lru_cache(maxsize=1)
def _load_settings(_cache_key: tuple[str, int, int]) -> Settings:
    return Settings()


def get_settings() -> Settings:
    return _load_settings(_settings_cache_signature())


def _clear_settings_cache() -> None:
    _load_settings.cache_clear()


get_settings.cache_clear = _clear_settings_cache  # type: ignore[attr-defined]


def is_notebooklm_source_sync_enabled(settings: Settings | None = None) -> bool:
    effective_settings = settings or get_settings()
    return bool(getattr(effective_settings, "notebooklm_source_sync_enabled", True))


def get_notebooklm_proxy_url(settings: Settings | None = None) -> str | None:
    effective_settings = settings or get_settings()
    if not getattr(effective_settings, "notebooklm_proxy_enabled", False):
        return None
    proxy_url = getattr(effective_settings, "notebooklm_proxy_url", None)
    if not isinstance(proxy_url, str) or not proxy_url.strip():
        return None
    return proxy_url.strip()


def is_bot_admin_user(user_id: int | None, settings: Settings | None = None) -> bool:
    if user_id is None:
        return False
    effective_settings = settings or get_settings()
    raw_value = getattr(effective_settings, "bot_admin_user_ids", None)
    if not isinstance(raw_value, str) or not raw_value.strip():
        return False
    for part in raw_value.split(","):
        candidate = part.strip()
        if not candidate:
            continue
        try:
            if int(candidate) == int(user_id):
                return True
        except ValueError:
            continue
    return False
