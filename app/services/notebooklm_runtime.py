"""Persisted NotebookLM runtime state for a separate VPS-targeted instance."""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.core.config import get_settings

_DEFAULT_RUNTIME_STORAGE_STATE = ".tmp/notebooklm/storage_state.json"
_NOTEBOOK_PATH_RE = re.compile(r"/notebook/([A-Za-z0-9-]+)")
_RAW_NOTEBOOK_ID_RE = re.compile(r"^[A-Za-z0-9-]{8,}$")


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    os.replace(temp_path, path)


def _ensure_private_storage_state_permissions(path: Path) -> None:
    try:
        if path.exists():
            # The lightweight VPS slice shares managed NotebookLM auth state
            # between the root-run admin/api container and the non-root bot
            # container via a bind-mounted .state directory. Keep the file
            # writable only by the owner, but readable by sibling processes.
            os.chmod(path, 0o644)
    except OSError:
        pass


def _safe_account_key(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    return normalized.strip(".-")[:80]


@dataclass(slots=True)
class NotebookLMRuntimeConfig:
    enabled: bool
    notebook_id: str
    notebook_url: str
    storage_state_path: str
    storage_state_exists: bool
    instance_storage_state_path: str
    shared_storage_state_path: str
    shared_storage_state_exists: bool
    auth_ready: bool
    source: str
    runtime_state_path: str
    runtime_state_configured: bool
    runtime_state_exists: bool
    updated_at: str | None = None
    config_error: str | None = None


@dataclass(slots=True)
class PersistedNotebookLMRuntimeState:
    enabled: bool
    default_notebook_id: str
    default_notebook_url: str
    storage_state_path: str
    updated_at: str


class NotebookLMRuntimeStore:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _runtime_state_path(self) -> Path | None:
        configured = getattr(self._settings, "notebooklm_runtime_state_path", "") or ""
        configured = configured.strip()
        if not configured:
            return None
        return Path(configured).expanduser()

    def _managed_storage_state_path(self) -> Path:
        configured = getattr(
            self._settings,
            "notebooklm_runtime_storage_state",
            _DEFAULT_RUNTIME_STORAGE_STATE,
        )
        return Path(configured).expanduser()

    def _env_storage_state_path(self) -> Path:
        configured = getattr(
            self._settings,
            "notebooklm_storage_state",
            _DEFAULT_RUNTIME_STORAGE_STATE,
        )
        return Path(configured).expanduser()

    def _shared_storage_state_path(self) -> Path | None:
        account_key = _safe_account_key(str(getattr(self._settings, "notebooklm_auth_account_key", "") or ""))
        if not account_key:
            return None
        configured = getattr(self._settings, "notebooklm_shared_storage_state", "")
        configured = str(configured or "").strip()
        if not configured:
            configured = f".state/notebooklm/shared-auth/{account_key}/storage_state.json"
        return Path(configured).expanduser()

    def _effective_storage_state_path(self, instance_path: Path) -> Path:
        shared_path = self._shared_storage_state_path()
        candidates = [instance_path]
        if shared_path is not None and shared_path != instance_path:
            candidates.append(shared_path)

        existing = [path for path in candidates if path.exists()]
        for path in existing:
            _ensure_private_storage_state_permissions(path)
        if not existing:
            return instance_path
        return max(enumerate(existing), key=lambda item: (item[1].stat().st_mtime_ns, item[0]))[1]

    def _mirror_storage_state_to_shared(self, *, instance_path: Path, content: str) -> Path | None:
        shared_path = self._shared_storage_state_path()
        if shared_path is None or shared_path == instance_path:
            return None
        shared_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_text(shared_path, content)
        _ensure_private_storage_state_permissions(shared_path)
        return shared_path

    def _load_persisted_state(
        self,
    ) -> tuple[PersistedNotebookLMRuntimeState | None, str | None]:
        path = self._runtime_state_path()
        if path is None or not path.exists():
            return None, None

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return None, f"Runtime state JSON is invalid: {exc.msg}."
        except OSError as exc:
            return None, f"Runtime state file could not be read: {exc}."

        if not isinstance(payload, dict):
            return None, "Runtime state JSON must be an object."

        return (
            PersistedNotebookLMRuntimeState(
                enabled=bool(payload.get("enabled", False)),
                default_notebook_id=str(payload.get("default_notebook_id", "") or ""),
                default_notebook_url=str(payload.get("default_notebook_url", "") or ""),
                storage_state_path=str(payload.get("storage_state_path", "") or ""),
                updated_at=str(payload.get("updated_at", "") or ""),
            ),
            None,
        )

    def _write_persisted_state(self, state: PersistedNotebookLMRuntimeState) -> None:
        path = self._runtime_state_path()
        if path is None:
            raise ValueError("Set NOTEBOOKLM_RUNTIME_STATE_PATH for the VPS instance before editing runtime state.")

        _atomic_write_text(
            path,
            json.dumps(asdict(state), ensure_ascii=False, indent=2) + "\n",
        )

    @staticmethod
    def normalize_notebook_reference(notebook_ref: str) -> tuple[str, str]:
        value = notebook_ref.strip()
        if not value:
            return "", ""

        if _RAW_NOTEBOOK_ID_RE.fullmatch(value):
            return value, f"https://notebooklm.google.com/notebook/{value}"

        parsed = urlparse(value)
        if parsed.scheme and parsed.netloc:
            match = _NOTEBOOK_PATH_RE.search(parsed.path)
            if match:
                notebook_id = match.group(1)
                return notebook_id, f"https://notebooklm.google.com/notebook/{notebook_id}"

            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                notebook_id = parts[-1]
                if _RAW_NOTEBOOK_ID_RE.fullmatch(notebook_id):
                    return notebook_id, f"https://notebooklm.google.com/notebook/{notebook_id}"

        raise ValueError("Provide a raw NotebookLM notebook id or a full notebook URL.")

    def get_runtime_config(self) -> NotebookLMRuntimeConfig:
        persisted, config_error = self._load_persisted_state()
        runtime_state_path = self._runtime_state_path()
        runtime_state_configured = runtime_state_path is not None
        runtime_state_exists = bool(runtime_state_path and runtime_state_path.exists())
        env_default_notebook = str(getattr(self._settings, "notebooklm_default_notebook", "") or "")
        env_notebook_url = (
            f"https://notebooklm.google.com/notebook/{env_default_notebook}"
            if env_default_notebook
            else ""
        )

        if persisted is None:
            instance_path = self._env_storage_state_path()
            effective_path = self._effective_storage_state_path(instance_path)
            shared_path = self._shared_storage_state_path()
            storage_state_path = str(effective_path)
            storage_exists = effective_path.exists()
            enabled = bool(getattr(self._settings, "notebooklm_enabled", False))
            return NotebookLMRuntimeConfig(
                enabled=enabled,
                notebook_id=env_default_notebook,
                notebook_url=env_notebook_url,
                storage_state_path=storage_state_path,
                storage_state_exists=storage_exists,
                instance_storage_state_path=str(instance_path),
                shared_storage_state_path=str(shared_path or ""),
                shared_storage_state_exists=bool(shared_path and shared_path.exists()),
                auth_ready=storage_exists,
                source="env",
                runtime_state_path=str(runtime_state_path or ""),
                runtime_state_configured=runtime_state_configured,
                runtime_state_exists=runtime_state_exists,
                updated_at=None,
                config_error=config_error,
            )

        instance_path = Path(persisted.storage_state_path or str(self._managed_storage_state_path())).expanduser()
        effective_path = self._effective_storage_state_path(instance_path)
        shared_path = self._shared_storage_state_path()
        storage_state_path = str(effective_path)
        storage_exists = effective_path.exists()
        return NotebookLMRuntimeConfig(
            enabled=persisted.enabled,
            notebook_id=persisted.default_notebook_id,
            notebook_url=persisted.default_notebook_url,
            storage_state_path=storage_state_path,
            storage_state_exists=storage_exists,
            instance_storage_state_path=str(instance_path),
            shared_storage_state_path=str(shared_path or ""),
            shared_storage_state_exists=bool(shared_path and shared_path.exists()),
            auth_ready=storage_exists,
            source="runtime",
            runtime_state_path=str(runtime_state_path or ""),
            runtime_state_configured=runtime_state_configured,
            runtime_state_exists=runtime_state_exists,
            updated_at=persisted.updated_at or None,
            config_error=config_error,
        )

    def get_runtime_status(self) -> dict[str, Any]:
        config = self.get_runtime_config()
        return {
            "enabled": config.enabled,
            "notebook_id": config.notebook_id,
            "notebook_url": config.notebook_url,
            "storage_state_path": config.storage_state_path,
            "storage_state_exists": config.storage_state_exists,
            "instance_storage_state_path": config.instance_storage_state_path,
            "shared_storage_state_path": config.shared_storage_state_path,
            "shared_storage_state_exists": config.shared_storage_state_exists,
            "auth_ready": config.auth_ready,
            "source": config.source,
            "updated_at": config.updated_at,
            "managed_storage_state_path": str(self._managed_storage_state_path()),
            "runtime_state_path": config.runtime_state_path,
            "runtime_state_configured": config.runtime_state_configured,
            "runtime_state_exists": config.runtime_state_exists,
            "config_error": config.config_error,
        }

    def resolve_notebook_id(self, chat_id: int) -> str | None:
        config = self.get_runtime_config()
        if config.source == "runtime" and config.notebook_id:
            return config.notebook_id

        notebook_map = getattr(self._settings, "notebooklm_notebook_map", {}) or {}
        mapped_notebook_id = notebook_map.get(str(chat_id))
        if mapped_notebook_id:
            return mapped_notebook_id
        if config.notebook_id:
            return config.notebook_id
        return None

    def resolve_storage_state_path(self) -> str:
        return self.get_runtime_config().storage_state_path

    def is_enabled(self) -> bool:
        return self.get_runtime_config().enabled

    def update_runtime_config(self, *, enabled: bool, notebook_ref: str) -> dict[str, Any]:
        if self._runtime_state_path() is None:
            raise ValueError(
                "Set NOTEBOOKLM_RUNTIME_STATE_PATH for the VPS instance before using the NotebookLM admin UI."
            )

        notebook_id, notebook_url = self.normalize_notebook_reference(notebook_ref)
        current, _ = self._load_persisted_state()
        storage_state_path = (
            current.storage_state_path
            if current and current.storage_state_path
            else str(self._managed_storage_state_path())
        )
        persisted = PersistedNotebookLMRuntimeState(
            enabled=enabled,
            default_notebook_id=notebook_id,
            default_notebook_url=notebook_url,
            storage_state_path=storage_state_path,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self._write_persisted_state(persisted)
        return self.get_runtime_status()

    def replace_storage_state(self, storage_state_json: str) -> dict[str, Any]:
        if self._runtime_state_path() is None:
            raise ValueError(
                "Set NOTEBOOKLM_RUNTIME_STATE_PATH for the VPS instance before replacing storage state."
            )

        stripped = storage_state_json.strip()
        if not stripped:
            raise ValueError("Storage-state JSON is required.")

        parsed = json.loads(stripped)
        if not isinstance(parsed, (dict, list)):
            raise ValueError("Storage-state JSON must be an object or array.")

        current, _ = self._load_persisted_state()
        runtime = self.get_runtime_config()
        storage_path = (
            current.storage_state_path
            if current and current.storage_state_path
            else str(self._managed_storage_state_path())
        )
        target = Path(storage_path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(parsed, ensure_ascii=False, indent=2) + "\n"
        _atomic_write_text(target, serialized)
        _ensure_private_storage_state_permissions(target)
        self._mirror_storage_state_to_shared(instance_path=target, content=serialized)

        persisted = PersistedNotebookLMRuntimeState(
            enabled=current.enabled if current else runtime.enabled,
            default_notebook_id=current.default_notebook_id if current else runtime.notebook_id,
            default_notebook_url=current.default_notebook_url if current else runtime.notebook_url,
            storage_state_path=str(target),
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self._write_persisted_state(persisted)
        return self.get_runtime_status()


def is_notebooklm_enabled(settings=None) -> bool:
    effective_settings = settings or get_settings()
    try:
        return NotebookLMRuntimeStore(settings=effective_settings).is_enabled()
    except ValueError:
        return bool(getattr(effective_settings, "notebooklm_enabled", False))
