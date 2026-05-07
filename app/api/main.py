import html
import io
import json
import logging
import secrets
import zipfile
from hashlib import sha256
from datetime import UTC, datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field

from app.core.config import (
    get_notebooklm_proxy_url,
    get_settings,
    is_notebooklm_source_sync_enabled,
)
from app.services.access_store import BotAccessStore, STARS_CURRENCY
from app.services.notebooklm_client import load_notebooklm_auth
from app.services.notebooklm_background_sync import NotebookLMBackgroundSyncRunner
from app.services.notebooklm_events import log_event
from app.services.notebooklm_health import NotebookLMHealthService
from app.services.notebooklm_metrics import render_prometheus_text
from app.services.notebooklm_remote_auth import (
    RemoteAuthConfigurationError,
    get_notebooklm_remote_auth_manager,
)
from app.services.notebooklm_upload_sync import (
    UploadSyncConfigurationError,
    get_notebooklm_upload_sync_manager,
)
from app.services.notebooklm_runtime import NotebookLMRuntimeStore
from app.services.telegram_stars import (
    TelegramStarsAPIError,
    TelegramStarsClient,
    reconcile_star_transactions,
    sanitize_telegram_text,
)

app = FastAPI(title="telegram-context-search-bot")
_admin_security = HTTPBasic()
logger = logging.getLogger(__name__)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_background_sync_runner: NotebookLMBackgroundSyncRunner | None = None
_remote_auth_janitor = None
_WINDOWS_HELPER_BUNDLE_FILES: tuple[tuple[str, Path], ...] = (
    ("notebooklm_windows_sync_helper.py", _REPO_ROOT / "scripts" / "windows" / "notebooklm_windows_sync_helper.py"),
    ("invoke_notebooklm_sync.ps1", _REPO_ROOT / "scripts" / "windows" / "invoke_notebooklm_sync.ps1"),
    ("register_notebooklm_sync_protocol.ps1", _REPO_ROOT / "scripts" / "windows" / "register_notebooklm_sync_protocol.ps1"),
    ("install_notebooklm_sync_refresh_task.ps1", _REPO_ROOT / "scripts" / "windows" / "install_notebooklm_sync_refresh_task.ps1"),
    ("install_notebooklm_windows_helper.ps1", _REPO_ROOT / "scripts" / "windows" / "install_notebooklm_windows_helper.ps1"),
    ("windows_chromium_auth.py", _REPO_ROOT / "app" / "services" / "windows_chromium_auth.py"),
)
_ANDROID_EXTENSION_BUNDLE_FILES: tuple[tuple[str, Path], ...] = (
    ("manifest.json", _REPO_ROOT / "browser-extension" / "notebooklm-auth-sync" / "manifest.json"),
    ("background.js", _REPO_ROOT / "browser-extension" / "notebooklm-auth-sync" / "background.js"),
    ("content-script.js", _REPO_ROOT / "browser-extension" / "notebooklm-auth-sync" / "content-script.js"),
    ("README.md", _REPO_ROOT / "browser-extension" / "notebooklm-auth-sync" / "README.md"),
)


@app.on_event("startup")
async def _startup_reconcile_notebooklm_upload_sync() -> None:
    global _remote_auth_janitor
    settings = get_settings()
    try:
        await _upload_sync_manager().reconcile()
    except Exception:
        logger.exception("notebooklm.upload_sync startup reconcile failed")
    docker_socket = Path(
        str(getattr(settings, "notebooklm_remote_auth_docker_socket", "/var/run/docker.sock") or "/var/run/docker.sock")
    ).expanduser()
    if docker_socket.exists():
        try:
            remote_auth_manager = get_notebooklm_remote_auth_manager()
            await remote_auth_manager.reconcile()
            remote_auth_manager.start_janitor()
            _remote_auth_janitor = remote_auth_manager
        except Exception:
            logger.exception("notebooklm.remote_auth startup reconcile failed")
    else:
        logger.info("notebooklm.remote_auth startup skipped; docker socket missing at %s", docker_socket)
    global _background_sync_runner
    try:
        runner = NotebookLMBackgroundSyncRunner(settings=settings)
        _background_sync_runner = runner
        runner.start()
    except Exception:
        logger.exception("notebooklm.background_sync startup failed")


@app.on_event("shutdown")
async def _shutdown_background_notebooklm_sync() -> None:
    global _background_sync_runner
    global _remote_auth_janitor
    if _remote_auth_janitor is not None:
        try:
            await _remote_auth_janitor.stop_janitor()
        except Exception:
            logger.exception("notebooklm.remote_auth shutdown failed")
        finally:
            _remote_auth_janitor = None
    if _background_sync_runner is None:
        return
    try:
        await _background_sync_runner.stop()
    except Exception:
        logger.exception("notebooklm.background_sync shutdown failed")
    finally:
        _background_sync_runner = None


def _raise_admin_internal_error(detail: str, exc: Exception) -> None:
    logger.exception("notebooklm.admin unexpected_error detail=%s", detail, exc_info=exc)
    raise HTTPException(status_code=500, detail=detail) from exc


def _windows_helper_install_script_url(request: Request) -> str:
    return f"{str(request.base_url).rstrip('/')}/api/public/notebooklm/windows-helper/install.ps1"


def _windows_helper_package_url(request: Request) -> str:
    return f"{str(request.base_url).rstrip('/')}/api/public/notebooklm/windows-helper/package.zip"


def _android_extension_package_url(request: Request) -> str:
    return f"{str(request.base_url).rstrip('/')}/api/public/notebooklm/android-extension/package.zip"


def _windows_helper_install_script_sha256(script_text: str) -> str:
    return sha256(script_text.encode("utf-8")).hexdigest()


def _build_windows_helper_bootstrap_script(request: Request) -> str:
    package_url = _windows_helper_package_url(request)
    protocol_scheme = getattr(get_settings(), "notebooklm_windows_helper_protocol_scheme", "tgctxbot-notebooklm-sync")
    return f"""param(
    [string]$InstallRoot = "",
    [switch]$RegisterRefreshTask,
    [int]$EveryHours = 6
)

$ErrorActionPreference = "Stop"
$packageUrl = "{package_url}"
$protocolScheme = "{protocol_scheme}"
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("tgctxbot-notebooklm-helper-" + [guid]::NewGuid().ToString())
$packageRoot = Join-Path $tempRoot "package"
$zipPath = Join-Path $tempRoot "notebooklm-windows-helper.zip"

New-Item -ItemType Directory -Path $packageRoot -Force | Out-Null
Invoke-WebRequest -Uri $packageUrl -OutFile $zipPath -UseBasicParsing
Expand-Archive -Path $zipPath -DestinationPath $packageRoot -Force

$installer = Join-Path $packageRoot "install_notebooklm_windows_helper.ps1"
$arguments = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $installer,
    "-ProtocolScheme", $protocolScheme
)
if ($InstallRoot) {{
    $arguments += @("-InstallRoot", $InstallRoot)
}}
if ($RegisterRefreshTask) {{
    $arguments += @("-RegisterRefreshTask")
    $arguments += @("-EveryHours", $EveryHours)
}}

& powershell.exe @arguments
"""


def _build_windows_helper_package() -> io.BytesIO:
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for archive_name, source_path in _WINDOWS_HELPER_BUNDLE_FILES:
            archive.writestr(archive_name, source_path.read_bytes())
    archive_buffer.seek(0)
    return archive_buffer


def _build_android_extension_package() -> io.BytesIO:
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for archive_name, source_path in _ANDROID_EXTENSION_BUNDLE_FILES:
            archive.writestr(archive_name, source_path.read_bytes())
    archive_buffer.seek(0)
    return archive_buffer


async def _augment_notebooklm_status_with_auth_probe(settings, status: dict) -> dict:
    result = dict(status)
    proxy_url = get_notebooklm_proxy_url(settings)
    result["notebooklm_proxy_enabled"] = bool(proxy_url)
    result["notebooklm_proxy_url"] = proxy_url or ""

    storage_path_value = result.get("storage_state_path") or ""
    storage_path = Path(storage_path_value).expanduser() if storage_path_value else None
    if storage_path and storage_path.exists():
        modified_at = datetime.fromtimestamp(storage_path.stat().st_mtime, tz=UTC)
        result["storage_state_mtime"] = modified_at.isoformat().replace("+00:00", "Z")
    else:
        result["storage_state_mtime"] = None

    if storage_path is None or not storage_path.exists():
        result["auth_ready"] = False
        result["auth_check"] = "missing"
        result["auth_error"] = "storage_state.json is missing."
        return result

    try:
        await load_notebooklm_auth(
            storage_path,
            float(getattr(settings, "notebooklm_timeout", 30.0)),
            proxy_url,
        )
    except Exception as exc:
        result["auth_ready"] = False
        result["auth_check"] = "expired"
        result["auth_error"] = str(exc)
    else:
        result["auth_ready"] = True
        result["auth_check"] = "valid"
        result["auth_error"] = None

    return result


class NlmRequest(BaseModel):
    chat_id: int
    question: str = Field(min_length=1)


class NotebookLMAdminConfigRequest(BaseModel):
    enabled: bool
    notebook_ref: str = Field(min_length=1)


class NotebookLMStorageStateRequest(BaseModel):
    storage_state_json: str = Field(min_length=2)


class NotebookLMAuthSessionCreateRequest(BaseModel):
    notify_in_telegram: bool = True


class NotebookLMUploadSessionRequest(BaseModel):
    storage_state_json: str = Field(min_length=2)
    helper_metadata: dict[str, object] | None = None


class AccessConfigValues(BaseModel):
    enabled: bool | None = None
    free_questions_per_24h: int | None = Field(default=None, ge=0)
    stars_price: int | None = Field(default=None, ge=1)
    credits_per_purchase: int | None = Field(default=None, ge=1)


class AccessChatOverrideRequest(AccessConfigValues):
    chat_id: int
    clear: bool = False


class AccessAdminConfigRequest(BaseModel):
    global_config: AccessConfigValues | None = None
    chat_overrides: list[AccessChatOverrideRequest] = Field(default_factory=list)


class AccessUserGrantRequest(BaseModel):
    chat_id: int
    credits_delta: int
    reason: str | None = None


def _require_notebooklm_admin(
    credentials: HTTPBasicCredentials = Depends(_admin_security),
) -> str:
    settings = get_settings()
    expected_username = getattr(settings, "notebooklm_admin_username", "admin")
    expected_password = getattr(settings, "notebooklm_admin_password", "")
    if not expected_password:
        raise HTTPException(
            status_code=503,
            detail="NotebookLM admin is not configured. Set NOTEBOOKLM_ADMIN_PASSWORD.",
        )

    username_ok = secrets.compare_digest(credentials.username, expected_username)
    password_ok = secrets.compare_digest(credentials.password, expected_password)
    if not (username_ok and password_ok):
        log_event(
            logger,
            logging.WARNING,
            "nlm.admin.login.failed",
            username=credentials.username,
            client=getattr(getattr(credentials, "__dict__", {}), "client", None),
        )
        raise HTTPException(
            status_code=401,
            detail="NotebookLM admin authentication failed.",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def _upload_sync_manager():
    return get_notebooklm_upload_sync_manager()


def _remote_auth_manager():
    return get_notebooklm_remote_auth_manager()


def _access_store() -> BotAccessStore:
    return BotAccessStore(settings=get_settings())


def _telegram_stars_client() -> TelegramStarsClient:
    settings = get_settings()
    proxy_url = ""
    if bool(getattr(settings, "telegram_proxy_enabled", False)):
        proxy_url = str(getattr(settings, "telegram_proxy_url", "") or "").strip()
    return TelegramStarsClient(
        bot_token=getattr(settings, "bot_token", ""),
        proxy_url=proxy_url or None,
    )


def _configured_bot_identity() -> dict:
    settings = get_settings()
    bot_token = str(getattr(settings, "bot_token", "") or "").strip()
    bot_id_hint = bot_token.split(":", 1)[0] if ":" in bot_token else ""
    return {
        "instance_name": str(getattr(settings, "bot_instance_name", "") or "").strip(),
        "bot_id_hint": bot_id_hint,
        "token_configured": bool(bot_token),
    }


def _admin_utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _telegram_live_error(client: object, exc: Exception) -> str:
    if isinstance(exc, TelegramStarsAPIError):
        message = str(exc)
    else:
        message = f"Unexpected Telegram Stars API error: {type(exc).__name__}"
    sanitizer = getattr(client, "sanitize_error", None)
    if callable(sanitizer):
        return sanitizer(message)
    return sanitize_telegram_text(message)


async def _load_live_telegram_stars(client: object, *, offset: int, limit: int) -> tuple[dict, list[dict], bool]:
    errors: list[str] = []
    bot_fetched_at = _admin_utc_iso()
    try:
        fetch_bot_identity = getattr(client, "fetch_bot_identity")
        bot_identity = await fetch_bot_identity()
        bot = {
            "ok": True,
            "fetched_at": bot_fetched_at,
            **dict(bot_identity),
        }
    except Exception as exc:
        error = _telegram_live_error(client, exc)
        errors.append(error)
        bot = {
            "ok": False,
            "fetched_at": bot_fetched_at,
            "error": error,
        }

    balance_fetched_at = _admin_utc_iso()
    try:
        balance_data = await client.fetch_balance()
        balance = {
            "ok": True,
            "fetched_at": balance_fetched_at,
            **dict(balance_data),
            "currency": STARS_CURRENCY,
        }
    except Exception as exc:
        error = _telegram_live_error(client, exc)
        errors.append(error)
        balance = {
            "ok": False,
            "amount": None,
            "currency": STARS_CURRENCY,
            "fetched_at": balance_fetched_at,
            "error": error,
        }

    transactions_fetched_at = _admin_utc_iso()
    try:
        transactions = await client.fetch_transactions(offset=offset, limit=limit)
        transaction_page = {
            "ok": True,
            "offset": offset,
            "limit": limit,
            "fetched_at": transactions_fetched_at,
            "count": len(transactions),
            "items": transactions,
        }
        transactions_available = True
    except Exception as exc:
        error = _telegram_live_error(client, exc)
        errors.append(error)
        transactions = []
        transaction_page = {
            "ok": False,
            "offset": offset,
            "limit": limit,
            "fetched_at": transactions_fetched_at,
            "count": 0,
            "items": [],
            "error": error,
        }
        transactions_available = False

    return (
        {
            "ok": bool(bot.get("ok") and balance.get("ok") and transaction_page.get("ok")),
            "error": "; ".join(errors) if errors else None,
            "bot": bot,
            "balance": balance,
            "transactions": transaction_page,
        },
        transactions,
        transactions_available,
    )


@app.get("/api/admin/access/status")
async def access_admin_status(_admin_user: str = Depends(_require_notebooklm_admin)) -> dict:
    try:
        status = _access_store().status()
        status["bot"] = _configured_bot_identity()
        return status
    except Exception as exc:
        _raise_admin_internal_error("Failed to load access status.", exc)


@app.get("/api/admin/access/stars")
async def access_admin_stars(
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100),
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    effective_limit = min(100, max(1, int(limit)))
    store = _access_store()
    try:
        local_summary = store.stars_ledger_summary()
        local_payments = store.star_payments()
    except Exception as exc:
        _raise_admin_internal_error("Failed to load local Telegram Stars ledger.", exc)

    live, live_transactions, page_available = await _load_live_telegram_stars(
        _telegram_stars_client(),
        offset=int(offset),
        limit=effective_limit,
    )
    reconciliation = reconcile_star_transactions(
        live_transactions,
        local_payments,
        page_available=page_available,
    )
    return {
        "currency": STARS_CURRENCY,
        "bot": _configured_bot_identity(),
        "pagination": {
            "offset": int(offset),
            "limit": effective_limit,
            "requested_limit": int(limit),
        },
        "live": live,
        "local": {
            "summary": local_summary,
        },
        "reconciliation": reconciliation,
    }


@app.post("/api/admin/access/config")
async def access_admin_config(
    request: AccessAdminConfigRequest,
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    store = _access_store()
    try:
        if request.global_config is not None:
            store.set_global_config(
                enabled=request.global_config.enabled,
                free_questions_per_24h=request.global_config.free_questions_per_24h,
                stars_price=request.global_config.stars_price,
                credits_per_purchase=request.global_config.credits_per_purchase,
            )
        for override in request.chat_overrides:
            if override.clear:
                store.clear_chat_override(chat_id=override.chat_id)
                continue
            store.set_chat_override(
                chat_id=override.chat_id,
                enabled=override.enabled,
                free_questions_per_24h=override.free_questions_per_24h,
                stars_price=override.stars_price,
                credits_per_purchase=override.credits_per_purchase,
            )
        return store.status()
    except Exception as exc:
        _raise_admin_internal_error("Failed to save access config.", exc)


@app.get("/api/admin/access/users/{telegram_user_id}")
async def access_admin_user(
    telegram_user_id: int,
    chat_id: int,
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    try:
        return _access_store().balance(telegram_user_id=telegram_user_id, chat_id=chat_id)
    except Exception as exc:
        _raise_admin_internal_error("Failed to load access user balance.", exc)


@app.post("/api/admin/access/users/{telegram_user_id}")
async def access_admin_user_grant(
    telegram_user_id: int,
    request: AccessUserGrantRequest,
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    try:
        balance = _access_store().grant_manual_credits(
            telegram_user_id=telegram_user_id,
            chat_id=request.chat_id,
            delta=request.credits_delta,
            reason=request.reason,
        )
        return {
            "telegram_user_id": telegram_user_id,
            "chat_id": request.chat_id,
            "manual_credits": balance,
        }
    except Exception as exc:
        _raise_admin_internal_error("Failed to update access user credits.", exc)


@app.get("/admin/notebooklm", response_class=HTMLResponse)
async def notebooklm_admin_page(
    request: Request,
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> str:
    install_script_url = html.escape(_windows_helper_install_script_url(request), quote=True)
    package_url = html.escape(_windows_helper_package_url(request), quote=True)
    install_script_sha256 = html.escape(
        _windows_helper_install_script_sha256(_build_windows_helper_bootstrap_script(request)),
        quote=True,
    )
    page = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NotebookLM Admin</title>
  <style>
    :root {
      --bg: #f5efe4;
      --panel: #fffaf2;
      --border: #d8c8b0;
      --text: #1f2937;
      --muted: #6b7280;
      --accent: #8a3b12;
      --accent-soft: #f7dfcf;
      --success: #0f766e;
      --danger: #b42318;
      --shadow: 0 22px 60px rgba(58, 42, 23, 0.12);
      --sans: "Segoe UI", "Trebuchet MS", sans-serif;
      --mono: "Cascadia Code", "Consolas", monospace;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: var(--sans);
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(138, 59, 18, 0.14), transparent 30%),
        linear-gradient(140deg, #f9f4ec, #efe2cf);
      min-height: 100vh;
    }
    .shell {
      max-width: 960px;
      margin: 0 auto;
      padding: 28px 18px 40px;
    }
    .hero, .card {
      background: rgba(255, 250, 242, 0.92);
      border: 1px solid var(--border);
      border-radius: 22px;
      box-shadow: var(--shadow);
    }
    .hero {
      padding: 24px;
      margin-bottom: 18px;
    }
    .hero h1 {
      margin: 0 0 8px;
      font-size: clamp(30px, 4vw, 46px);
      line-height: 0.98;
    }
    .hero p {
      margin: 0;
      color: var(--muted);
      line-height: 1.5;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
    }
    .card {
      padding: 18px;
    }
    h2 {
      margin: 0 0 12px;
      font-size: 20px;
    }
    p, li {
      line-height: 1.5;
    }
    label {
      display: block;
      margin: 0 0 14px;
      font-weight: 700;
      font-size: 14px;
    }
    input, textarea, select, button {
      width: 100%;
      font: inherit;
    }
    input, textarea, select {
      margin-top: 6px;
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: 12px;
      background: #fffdfa;
      color: var(--text);
    }
    textarea {
      min-height: 180px;
      resize: vertical;
      font-family: var(--mono);
      font-size: 13px;
    }
    .checkbox {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
      font-weight: 700;
    }
    .checkbox input {
      width: auto;
      margin: 0;
    }
    button {
      border: 0;
      border-radius: 14px;
      padding: 13px 16px;
      cursor: pointer;
      color: white;
      background: linear-gradient(135deg, #8a3b12, #b45309);
      box-shadow: 0 10px 24px rgba(138, 59, 18, 0.24);
      font-weight: 700;
    }
    .secondary {
      background: linear-gradient(135deg, #0f766e, #155e75);
      box-shadow: 0 10px 24px rgba(15, 118, 110, 0.24);
    }
    .status-box {
      display: grid;
      gap: 10px;
      padding: 16px;
      border-radius: 16px;
      background: #fffdf8;
      border: 1px solid rgba(216, 200, 176, 0.85);
    }
    .status-line {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding-bottom: 8px;
      border-bottom: 1px dashed rgba(216, 200, 176, 0.8);
    }
    .status-line:last-child {
      border-bottom: 0;
      padding-bottom: 0;
    }
    .status-line span:first-child {
      color: var(--muted);
    }
    .compact-list {
      display: grid;
      gap: 8px;
      margin-top: 8px;
    }
    .compact-row {
      padding: 8px 0;
      border-top: 1px dashed rgba(216, 200, 176, 0.8);
      font-size: 13px;
      line-height: 1.45;
      word-break: break-word;
    }
    .ok { color: var(--success); }
    .error { color: var(--danger); }
    .hint {
      color: var(--muted);
      font-size: 14px;
    }
    .flash {
      min-height: 22px;
      margin-top: 10px;
      font-size: 14px;
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>NotebookLM Admin</h1>
      <p>Управление отдельным NotebookLM runtime для VPS. Здесь можно вручную переключить активный notebook и заменить storage-state без редактирования боевого <code>.env</code>.</p>
    </section>

    <div class="grid">
      <section class="card">
        <h2>Текущий статус</h2>
        <div class="status-box" id="statusBox">
          <div class="hint">Загружаю статус...</div>
        </div>
        <div class="flash" id="statusFlash"></div>
      </section>

      <section class="card">
        <h2>Конфигурация runtime</h2>
        <div class="checkbox">
          <input id="enabledInput" type="checkbox">
          <label for="enabledInput" style="margin:0">Включить NotebookLM для этого инстанса</label>
        </div>
        <label>
          Notebook link or id
          <input id="notebookInput" type="text" placeholder="https://notebooklm.google.com/notebook/...">
        </label>
        <button id="saveConfigBtn" type="button">Сохранить runtime config</button>
        <div class="flash" id="configFlash"></div>
      </section>
    </div>

    <section class="card" style="margin-top:16px">
      <h2>Access / Telegram Stars</h2>
      <div class="status-box" id="accessStatusBox">
        <div class="hint">Loading access status...</div>
      </div>
      <div style="margin-top:16px">
        <h3 style="margin:0 0 10px;font-size:16px">Telegram Stars stats</h3>
        <div class="status-box" id="telegramStarsStatsBox">
          <div class="hint">Live Stars stats are not loaded yet.</div>
        </div>
        <div class="grid" style="margin-top:12px">
          <label>
            Transactions offset
            <input id="telegramStarsOffsetInput" type="number" min="0" step="1" value="0">
          </label>
          <label>
            Transactions limit
            <input id="telegramStarsLimitInput" type="number" min="1" max="100" step="1" value="100">
          </label>
        </div>
        <button class="secondary" id="refreshTelegramStarsBtn" type="button">Refresh Telegram Stars stats</button>
      </div>
      <div class="checkbox" style="margin-top:12px">
        <input id="accessEnabledInput" type="checkbox">
        <label for="accessEnabledInput" style="margin:0">Enable NotebookLM access limits and Telegram Stars credits</label>
      </div>
      <div class="grid" style="margin-top:12px">
        <label>
          Free questions per rolling 24h
          <input id="accessFreeInput" type="number" min="0" step="1">
        </label>
        <label>
          Stars price
          <input id="accessStarsInput" type="number" min="1" step="1">
        </label>
        <label>
          Credits per Stars purchase
          <input id="accessCreditsInput" type="number" min="1" step="1">
        </label>
      </div>
      <button class="secondary" id="saveAccessConfigBtn" type="button">Save Telegram Stars access config</button>
      <div class="grid" style="margin-top:12px">
        <label>
          Override chat ID
          <input id="accessOverrideChatInput" type="number" step="1" placeholder="-1001234567890">
        </label>
        <label>
          Enabled override
          <select id="accessOverrideEnabledInput">
            <option value="">inherit</option>
            <option value="true">enabled</option>
            <option value="false">disabled</option>
          </select>
        </label>
        <label>
          Override free / 24h
          <input id="accessOverrideFreeInput" type="number" min="0" step="1" placeholder="inherit">
        </label>
        <label>
          Override Stars price
          <input id="accessOverrideStarsInput" type="number" min="1" step="1" placeholder="inherit">
        </label>
        <label>
          Override credits per purchase
          <input id="accessOverrideCreditsInput" type="number" min="1" step="1" placeholder="inherit">
        </label>
      </div>
      <div class="actions">
        <button id="saveAccessOverrideBtn" type="button">Save chat override</button>
        <button class="secondary" id="clearAccessOverrideBtn" type="button">Clear chat override</button>
      </div>
      <div class="grid" style="margin-top:12px">
        <label>
          Chat ID
          <input id="accessGrantChatInput" type="number" step="1" placeholder="-1001234567890">
        </label>
        <label>
          Telegram user ID
          <input id="accessGrantUserInput" type="number" step="1">
        </label>
        <label>
          Manual credits delta
          <input id="accessGrantDeltaInput" type="number" step="1" value="1">
        </label>
      </div>
      <button id="grantAccessCreditsBtn" type="button">Grant manual credits</button>
      <div class="flash" id="accessFlash"></div>
    </section>

    <section class="card" style="margin-top:16px">
      <h2>Phone / Windows Cookie Import</h2>
      <p class="hint">Если это другой Windows-ПК, helper нужно установить именно на нём один раз, иначе кнопка запуска ничего не сделает.</p>
      <div class="actions">
        <a class="secondary" href="__INSTALL_SCRIPT_URL__">Скачать installer (.ps1)</a>
        <a class="secondary" href="__PACKAGE_URL__">Скачать helper package (.zip)</a>
      </div>
      <p class="hint">SHA-256 installer: <code>__INSTALL_SCRIPT_SHA256__</code></p>
      <p class="hint">Создаёт одноразовую Windows sync session. После одноразовой установки локального protocol handler браузер на твоём Windows-ПК сможет прямо из этой админки запустить helper, забрать живые Google cookies из Chrome/Edge/Chromium и автоматически загрузить свежий <code>storage_state.json</code> на VPS.</p>
      <button id="createAuthSessionBtn" type="button">Запустить Windows sync</button>
      <div class="status-box" id="authSessionBox" style="margin-top:12px">
        <div class="hint">Активная Windows sync session ещё не создана.</div>
      </div>
      <div class="flash" id="authSessionFlash"></div>
    </section>

    <section class="card" style="margin-top:16px">
      <h2>VPS Browser Login</h2>
      <p class="hint">Start a remote noVNC browser session on the VPS when Docker remote auth is configured. Complete Google login in the browser; the service captures allowed Google/NotebookLM cookies and closes the session.</p>
      <button class="secondary" id="createRemoteAuthSessionBtn" type="button">Create VPS browser login</button>
      <div class="status-box" id="remoteAuthSessionBox" style="margin-top:12px">
        <div class="hint">No VPS browser login session has been created yet.</div>
      </div>
      <div class="flash" id="remoteAuthSessionFlash"></div>
    </section>

    <section class="card" style="margin-top:16px">
      <h2>Ручное обновление авторизации</h2>
      <p class="hint">Вставьте содержимое отдельного <code>storage_state.json</code> для VPS-инстанса. После сохранения кэш клиента будет сброшен.</p>
      <label>
        storage_state.json
        <textarea id="storageStateInput" placeholder='{"cookies": [...], "origins": [...]} or upstream storage JSON'></textarea>
      </label>
      <button class="secondary" id="saveStorageBtn" type="button">Обновить storage state</button>
      <div class="flash" id="storageFlash"></div>
    </section>
  </div>

  <script>
    const statusBox = document.getElementById("statusBox");
    const statusFlash = document.getElementById("statusFlash");
    const configFlash = document.getElementById("configFlash");
    const storageFlash = document.getElementById("storageFlash");
    const authSessionFlash = document.getElementById("authSessionFlash");
    const remoteAuthSessionFlash = document.getElementById("remoteAuthSessionFlash");
    const accessFlash = document.getElementById("accessFlash");
    const enabledInput = document.getElementById("enabledInput");
    const notebookInput = document.getElementById("notebookInput");
    const storageStateInput = document.getElementById("storageStateInput");
    const saveConfigBtn = document.getElementById("saveConfigBtn");
    const saveStorageBtn = document.getElementById("saveStorageBtn");
    const createAuthSessionBtn = document.getElementById("createAuthSessionBtn");
    const createRemoteAuthSessionBtn = document.getElementById("createRemoteAuthSessionBtn");
    const authSessionBox = document.getElementById("authSessionBox");
    const remoteAuthSessionBox = document.getElementById("remoteAuthSessionBox");
    const accessStatusBox = document.getElementById("accessStatusBox");
    const telegramStarsStatsBox = document.getElementById("telegramStarsStatsBox");
    const telegramStarsOffsetInput = document.getElementById("telegramStarsOffsetInput");
    const telegramStarsLimitInput = document.getElementById("telegramStarsLimitInput");
    const refreshTelegramStarsBtn = document.getElementById("refreshTelegramStarsBtn");
    const accessEnabledInput = document.getElementById("accessEnabledInput");
    const accessFreeInput = document.getElementById("accessFreeInput");
    const accessStarsInput = document.getElementById("accessStarsInput");
    const accessCreditsInput = document.getElementById("accessCreditsInput");
    const accessOverrideChatInput = document.getElementById("accessOverrideChatInput");
    const accessOverrideEnabledInput = document.getElementById("accessOverrideEnabledInput");
    const accessOverrideFreeInput = document.getElementById("accessOverrideFreeInput");
    const accessOverrideStarsInput = document.getElementById("accessOverrideStarsInput");
    const accessOverrideCreditsInput = document.getElementById("accessOverrideCreditsInput");
    const accessGrantChatInput = document.getElementById("accessGrantChatInput");
    const accessGrantUserInput = document.getElementById("accessGrantUserInput");
    const accessGrantDeltaInput = document.getElementById("accessGrantDeltaInput");
    const saveAccessConfigBtn = document.getElementById("saveAccessConfigBtn");
    const saveAccessOverrideBtn = document.getElementById("saveAccessOverrideBtn");
    const clearAccessOverrideBtn = document.getElementById("clearAccessOverrideBtn");
    const grantAccessCreditsBtn = document.getElementById("grantAccessCreditsBtn");

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    function setFlash(target, text, isError) {
      target.textContent = text;
      target.className = `flash ${isError ? "error" : "ok"}`;
    }

    function renderStatus(status) {
      enabledInput.checked = Boolean(status.enabled);
      notebookInput.value = status.notebook_url || status.notebook_id || "";
      const authCheck = status.auth_check || (status.auth_ready ? "valid" : "missing");
      const authClass = authCheck === "valid" ? "ok" : "error";
      const authLabel = authCheck === "valid" ? "valid" : authCheck;
      const storageLabel = status.storage_state_exists ? "present" : "missing";
      statusBox.innerHTML = `
        <div class="status-line"><span>Source</span><strong>${escapeHtml(status.source)}</strong></div>
        <div class="status-line"><span>Enabled</span><strong>${status.enabled ? "true" : "false"}</strong></div>
        <div class="status-line"><span>Notebook</span><strong>${escapeHtml(status.notebook_id || "not set")}</strong></div>
        <div class="status-line"><span>Storage path</span><strong>${escapeHtml(status.storage_state_path)}</strong></div>
        <div class="status-line"><span>Storage state</span><strong class="${status.storage_state_exists ? "ok" : "error"}">${storageLabel}</strong></div>
        <div class="status-line"><span>Storage updated</span><strong>${escapeHtml(status.storage_state_mtime || "unknown")}</strong></div>
        <div class="status-line"><span>NotebookLM proxy</span><strong class="${status.notebooklm_proxy_enabled ? "ok" : "error"}">${escapeHtml(status.notebooklm_proxy_url || "disabled")}</strong></div>
        <div class="status-line"><span>Auth check</span><strong class="${authClass}">${escapeHtml(authLabel)}</strong></div>
        <div class="status-line"><span>Auth detail</span><strong class="${status.auth_error ? "error" : "ok"}">${escapeHtml(status.auth_error || "none")}</strong></div>
        <div class="status-line"><span>Runtime file</span><strong>${escapeHtml(status.runtime_state_path || "not configured")}</strong></div>
        <div class="status-line"><span>Runtime configured</span><strong>${status.runtime_state_configured ? "true" : "false"}</strong></div>
        <div class="status-line"><span>Config error</span><strong class="${status.config_error ? "error" : "ok"}">${escapeHtml(status.config_error || "none")}</strong></div>
      `;
    }

    function renderAuthSession(session) {
      if (!session) {
        authSessionBox.innerHTML = '<div class="hint">Активная Windows sync session ещё не создана.</div>';
        return;
      }
      const launchLine = session.protocol_url
        ? `<div class="status-line"><span>Windows helper</span><strong><a href="${escapeHtml(session.protocol_url)}">launch local helper</a></strong></div>`
        : `<div class="status-line"><span>Windows helper</span><strong class="error">protocol handler not available</strong></div>`;
      const entryLine = session.entry_url
        ? `<div class="status-line"><span>Status page</span><strong><a href="${escapeHtml(session.entry_url)}" target="_blank" rel="noreferrer">open auth hub</a></strong></div><div class="status-line"><span>Copy import link</span><strong><button class="secondary copy-link" data-copy="${escapeHtml(session.entry_url)}" type="button">copy</button></strong></div>`
        : `<div class="status-line"><span>Status page</span><strong class="error">not available</strong></div>`;
      const refreshLine = session.device && session.device.last_uploaded_at
        ? `<div class="status-line"><span>Last sync</span><strong>${escapeHtml(session.device.last_uploaded_at)}</strong></div>`
        : "";
      authSessionBox.innerHTML = `
        <div class="status-line"><span>Status</span><strong>${escapeHtml(session.status || "unknown")}</strong></div>
        <div class="status-line"><span>Expires</span><strong>${escapeHtml(session.expires_at || "n/a")}</strong></div>
        ${entryLine}
        ${launchLine}
        ${refreshLine}
        <div class="status-line"><span>Via</span><strong>${escapeHtml(session.requested_via || "unknown")}</strong></div>
      `;
    }

    function renderRemoteAuthSession(session) {
      if (!session) {
        remoteAuthSessionBox.innerHTML = '<div class="hint">No VPS browser login session has been created yet.</div>';
        return;
      }
      const loginLine = session.auth_url
        ? `<div class="status-line"><span>Login page</span><strong><a href="${escapeHtml(session.auth_url)}" target="_blank" rel="noreferrer">open VPS browser login</a></strong></div><div class="status-line"><span>Copy login link</span><strong><button class="secondary copy-link" data-copy="${escapeHtml(session.auth_url)}" type="button">copy</button></strong></div>`
        : `<div class="status-line"><span>Login page</span><strong class="error">not available</strong></div>`;
      const browserLine = session.browser_url
        ? `<div class="status-line"><span>Browser</span><strong><a href="${escapeHtml(session.browser_url)}" target="_blank" rel="noreferrer">open noVNC</a></strong></div>`
        : "";
      const cancelLine = !["completed", "failed", "expired", "cancelled"].includes(session.status)
        ? `<div class="status-line"><span>Session</span><strong><button class="secondary remote-cancel" data-token="${escapeHtml((session.auth_url || "").split("/").pop() || "")}" type="button">cancel</button></strong></div>`
        : "";
      remoteAuthSessionBox.innerHTML = `
        <div class="status-line"><span>Status</span><strong>${escapeHtml(session.status || "unknown")}</strong></div>
        <div class="status-line"><span>Expires</span><strong>${escapeHtml(session.expires_at || "n/a")}</strong></div>
        ${loginLine}
        ${browserLine}
        ${cancelLine}
        <div class="status-line"><span>Via</span><strong>${escapeHtml(session.requested_via || "unknown")}</strong></div>
        <div class="status-line"><span>Detail</span><strong class="${session.error ? "error" : "ok"}">${escapeHtml(session.error || "none")}</strong></div>
      `;
    }

    function renderAccessStatus(status) {
      const config = status.global || {};
      const bot = status.bot || {};
      accessEnabledInput.checked = Boolean(config.enabled);
      accessFreeInput.value = config.free_questions_per_24h ?? 20;
      accessStarsInput.value = config.stars_price ?? 25;
      accessCreditsInput.value = config.credits_per_purchase ?? 10;
      const totals = status.totals || {};
      const overrides = status.chat_overrides || [];
      const overrideSummary = overrides.length
        ? overrides.map((item) => `${item.chat_id}: ${item.enabled === null ? "inherit" : (item.enabled ? "enabled" : "disabled")}, free=${item.free_questions_per_24h ?? "inherit"}, stars=${item.stars_price ?? "inherit"}, credits=${item.credits_per_purchase ?? "inherit"}`).join("; ")
        : "none";
      accessStatusBox.innerHTML = `
        <div class="status-line"><span>Current bot instance</span><strong>${escapeHtml(bot.instance_name || "not labeled")}</strong></div>
        <div class="status-line"><span>Bot token id</span><strong>${escapeHtml(bot.bot_id_hint || "unknown")}</strong></div>
        <div class="status-line"><span>Currency</span><strong>${escapeHtml(status.currency || "XTR")}</strong></div>
        <div class="status-line"><span>State path</span><strong>${escapeHtml(status.state_path || "")}</strong></div>
        <div class="status-line"><span>Access enabled</span><strong>${config.enabled ? "true" : "false"}</strong></div>
        <div class="status-line"><span>Free / 24h</span><strong>${escapeHtml(config.free_questions_per_24h ?? 20)}</strong></div>
        <div class="status-line"><span>Stars package</span><strong>${escapeHtml(config.credits_per_purchase ?? 10)} credits for ${escapeHtml(config.stars_price ?? 25)} Stars</strong></div>
        <div class="status-line"><span>Chat overrides</span><strong>${escapeHtml(overrideSummary)}</strong></div>
        <div class="status-line"><span>Usage / payments</span><strong>${escapeHtml(totals.usage_count || 0)} / ${escapeHtml(totals.payment_count || 0)}</strong></div>
      `;
    }

    function starsAmount(value) {
      if (!value || value.amount === null || value.amount === undefined) {
        return "n/a";
      }
      const nano = value.nanostar_amount === undefined ? "" : ` + ${escapeHtml(value.nanostar_amount)} nanostars`;
      return `${escapeHtml(value.amount)} ${escapeHtml(value.currency || "XTR")}${nano}`;
    }

    function renderTelegramStarsStats(stats) {
      const live = stats.live || {};
      const liveBot = live.bot || {};
      const balance = live.balance || {};
      const transactions = (live.transactions && live.transactions.items) || [];
      const summary = (stats.local && stats.local.summary) || {};
      const reconciliation = stats.reconciliation || {};
      const txRows = transactions.length
        ? transactions.map((tx) => `
            <div class="compact-row">
              <strong>${escapeHtml(tx.id || "(no id)")}</strong>
              <div>${escapeHtml(tx.amount)} ${escapeHtml(tx.currency || "XTR")} · ${escapeHtml(tx.date_iso || tx.date || "unknown date")}</div>
              <div class="hint">source=${escapeHtml(tx.source?.type || "unknown")} receiver=${escapeHtml(tx.receiver?.type || "unknown")}</div>
            </div>
          `).join("")
        : '<div class="hint">No live transactions in this fetched page.</div>';
      const mismatchRows = []
        .concat((reconciliation.live_not_found_locally || []).map((item) => `live ${item.id}: ${item.condition}`))
        .concat((reconciliation.local_not_in_fetched_page || []).map((item) => `local ${item.id}: ${item.condition}`));
      const mismatchHtml = mismatchRows.length
        ? mismatchRows.map((item) => `<div class="compact-row">${escapeHtml(item)}</div>`).join("")
        : '<div class="hint">No reconciliation mismatches in the fetched page.</div>';
      telegramStarsStatsBox.innerHTML = `
        <div class="status-line"><span>Live bot</span><strong class="${liveBot.ok ? "ok" : "error"}">${liveBot.ok ? escapeHtml(liveBot.username_label || liveBot.username || liveBot.id || "unknown") : escapeHtml(liveBot.error || "unavailable")}</strong></div>
        <div class="status-line"><span>Live balance</span><strong class="${balance.ok ? "ok" : "error"}">${balance.ok ? starsAmount(balance) : escapeHtml(balance.error || live.error || "unavailable")}</strong></div>
        <div class="status-line"><span>Fetched</span><strong>${escapeHtml(balance.fetched_at || "not loaded")}</strong></div>
        <div class="status-line"><span>Local DB</span><strong>${escapeHtml(summary.state_path || "")}</strong></div>
        <div class="status-line"><span>Local orders / payments / usage</span><strong>${escapeHtml(summary.local_order_count ?? 0)} / ${escapeHtml(summary.local_payment_count ?? 0)} / ${escapeHtml(summary.usage_count ?? 0)}</strong></div>
        <div class="status-line"><span>Local paid Stars</span><strong>${escapeHtml(summary.total_local_paid_stars_amount ?? 0)} ${escapeHtml(summary.currency || "XTR")}</strong></div>
        <div class="status-line"><span>Paid credits</span><strong>${escapeHtml(summary.paid_credits?.granted ?? 0)} granted, ${escapeHtml(summary.paid_credits?.consumed ?? 0)} used, ${escapeHtml(summary.paid_credits?.remaining ?? 0)} left</strong></div>
        <div class="status-line"><span>Manual credits</span><strong>${escapeHtml(summary.manual_credits?.granted ?? 0)} granted, ${escapeHtml(summary.manual_credits?.consumed ?? 0)} used, ${escapeHtml(summary.manual_credits?.remaining ?? 0)} left</strong></div>
        <div class="status-line"><span>Reconciliation</span><strong>${escapeHtml(reconciliation.matched_count ?? 0)} matched, ${escapeHtml(reconciliation.live_not_found_locally_count ?? 0)} live-only, ${escapeHtml(reconciliation.local_not_in_fetched_page_count ?? 0)} local not in fetched page</strong></div>
        <div class="compact-list"><strong>Recent live transactions</strong>${txRows}</div>
        <div class="compact-list"><strong>Reconciliation mismatches</strong>${mismatchHtml}</div>
      `;
    }

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          ...(options.headers || {}),
        },
      });
      const contentType = response.headers.get("content-type") || "";
      const rawText = await response.text();
      let body = {};
      if (rawText) {
        if (contentType.includes("application/json")) {
          try {
            body = JSON.parse(rawText);
          } catch (error) {
            body = { detail: rawText };
          }
        } else {
          body = { detail: rawText };
        }
      }
      if (!response.ok) {
        throw new Error(body.detail || "Request failed");
      }
      return body;
    }

    async function refreshStatus() {
      try {
        const body = await fetchJson("/api/admin/notebooklm/status");
        renderStatus(body);
        setFlash(statusFlash, "Статус обновлён.", false);
      } catch (error) {
        setFlash(statusFlash, error.message || "Не удалось загрузить статус.", true);
      }
    }

    async function refreshAuthSession() {
      try {
        const body = await fetchJson("/api/admin/notebooklm/auth-sessions/current");
        renderAuthSession(body);
      } catch (error) {
        renderAuthSession(null);
        if (error.message && !error.message.includes("No NotebookLM upload session")) {
          setFlash(authSessionFlash, error.message || "Не удалось загрузить Windows sync session.", true);
        }
      }
    }

    async function refreshRemoteAuthSession() {
      try {
        const body = await fetchJson("/api/admin/notebooklm/remote-auth-sessions/current");
        renderRemoteAuthSession(body);
      } catch (error) {
        renderRemoteAuthSession(null);
        if (error.message && !error.message.includes("No NotebookLM remote auth session")) {
          setFlash(remoteAuthSessionFlash, error.message || "Failed to load VPS browser login session.", true);
        }
      }
    }

    async function refreshAccessStatus() {
      try {
        const body = await fetchJson("/api/admin/access/status");
        renderAccessStatus(body);
      } catch (error) {
        setFlash(accessFlash, error.message || "Failed to load access status.", true);
      }
    }

    async function refreshTelegramStarsStats() {
      try {
        const params = new URLSearchParams({
          offset: String(Number(telegramStarsOffsetInput.value || 0)),
          limit: String(Number(telegramStarsLimitInput.value || 100)),
        });
        const body = await fetchJson(`/api/admin/access/stars?${params.toString()}`);
        renderTelegramStarsStats(body);
        setFlash(accessFlash, body.live?.ok ? "Telegram Stars stats refreshed." : (body.live?.error || "Local ledger loaded; live Telegram data unavailable."), !body.live?.ok);
      } catch (error) {
        setFlash(accessFlash, error.message || "Failed to load Telegram Stars stats.", true);
      }
    }

    saveConfigBtn.addEventListener("click", async () => {
      try {
        const body = await fetchJson("/api/admin/notebooklm/config", {
          method: "POST",
          body: JSON.stringify({
            enabled: enabledInput.checked,
            notebook_ref: notebookInput.value.trim(),
          }),
        });
        renderStatus(body);
        setFlash(configFlash, "Runtime config сохранён.", false);
      } catch (error) {
        setFlash(configFlash, error.message || "Не удалось сохранить config.", true);
      }
    });

    saveStorageBtn.addEventListener("click", async () => {
      try {
        const body = await fetchJson("/api/admin/notebooklm/storage-state", {
          method: "POST",
          body: JSON.stringify({
            storage_state_json: storageStateInput.value,
          }),
        });
        renderStatus(body);
        storageStateInput.value = "";
        setFlash(storageFlash, "Storage state обновлён.", false);
      } catch (error) {
        setFlash(storageFlash, error.message || "Не удалось обновить storage state.", true);
      }
    });

    createAuthSessionBtn.addEventListener("click", async () => {
      try {
        const body = await fetchJson("/api/admin/notebooklm/auth-sessions", {
          method: "POST",
          body: JSON.stringify({ notify_in_telegram: false }),
        });
        renderAuthSession(body);
        setFlash(authSessionFlash, "Windows sync session создана.", false);
      } catch (error) {
        setFlash(authSessionFlash, error.message || "Не удалось создать Windows sync session.", true);
      }
    });

    createRemoteAuthSessionBtn.addEventListener("click", async () => {
      try {
        const body = await fetchJson("/api/admin/notebooklm/remote-auth-sessions", {
          method: "POST",
          body: JSON.stringify({ notify_in_telegram: false }),
        });
        renderRemoteAuthSession(body);
        setFlash(remoteAuthSessionFlash, "VPS browser login session created.", false);
      } catch (error) {
        setFlash(remoteAuthSessionFlash, error.message || "Failed to create VPS browser login session.", true);
      }
    });

    refreshTelegramStarsBtn.addEventListener("click", refreshTelegramStarsStats);

    saveAccessConfigBtn.addEventListener("click", async () => {
      try {
        const body = await fetchJson("/api/admin/access/config", {
          method: "POST",
          body: JSON.stringify({
            global_config: {
              enabled: accessEnabledInput.checked,
              free_questions_per_24h: Number(accessFreeInput.value || 0),
              stars_price: Number(accessStarsInput.value || 1),
              credits_per_purchase: Number(accessCreditsInput.value || 1),
            },
          }),
        });
        renderAccessStatus(body);
        setFlash(accessFlash, "Telegram Stars access config saved.", false);
      } catch (error) {
        setFlash(accessFlash, error.message || "Failed to save access config.", true);
      }
    });

    function nullableNumber(value) {
      const trimmed = String(value ?? "").trim();
      return trimmed === "" ? null : Number(trimmed);
    }

    function nullableBool(value) {
      if (value === "true") {
        return true;
      }
      if (value === "false") {
        return false;
      }
      return null;
    }

    saveAccessOverrideBtn.addEventListener("click", async () => {
      try {
        const body = await fetchJson("/api/admin/access/config", {
          method: "POST",
          body: JSON.stringify({
            chat_overrides: [{
              chat_id: Number(accessOverrideChatInput.value || 0),
              enabled: nullableBool(accessOverrideEnabledInput.value),
              free_questions_per_24h: nullableNumber(accessOverrideFreeInput.value),
              stars_price: nullableNumber(accessOverrideStarsInput.value),
              credits_per_purchase: nullableNumber(accessOverrideCreditsInput.value),
            }],
          }),
        });
        renderAccessStatus(body);
        setFlash(accessFlash, "Chat access override saved.", false);
      } catch (error) {
        setFlash(accessFlash, error.message || "Failed to save chat override.", true);
      }
    });

    clearAccessOverrideBtn.addEventListener("click", async () => {
      try {
        const body = await fetchJson("/api/admin/access/config", {
          method: "POST",
          body: JSON.stringify({
            chat_overrides: [{
              chat_id: Number(accessOverrideChatInput.value || 0),
              clear: true,
            }],
          }),
        });
        renderAccessStatus(body);
        setFlash(accessFlash, "Chat access override cleared.", false);
      } catch (error) {
        setFlash(accessFlash, error.message || "Failed to clear chat override.", true);
      }
    });

    grantAccessCreditsBtn.addEventListener("click", async () => {
      try {
        const userId = accessGrantUserInput.value.trim();
        const body = await fetchJson(`/api/admin/access/users/${encodeURIComponent(userId)}`, {
          method: "POST",
          body: JSON.stringify({
            chat_id: Number(accessGrantChatInput.value || 0),
            credits_delta: Number(accessGrantDeltaInput.value || 0),
            reason: "admin-ui",
          }),
        });
        setFlash(accessFlash, `Manual credits: ${body.manual_credits}`, false);
      } catch (error) {
        setFlash(accessFlash, error.message || "Failed to grant manual credits.", true);
      }
    });

    document.addEventListener("click", async (event) => {
      const copyTarget = event.target.closest(".copy-link");
      if (copyTarget && copyTarget.dataset.copy) {
        await navigator.clipboard.writeText(copyTarget.dataset.copy);
        return;
      }
      const cancelTarget = event.target.closest(".remote-cancel");
      if (cancelTarget && cancelTarget.dataset.token) {
        const body = await fetchJson(`/api/admin/notebooklm/remote-auth-sessions/${encodeURIComponent(cancelTarget.dataset.token)}/cancel`, {
          method: "POST",
          body: "{}",
        });
        renderRemoteAuthSession(body);
      }
    });

    refreshStatus();
    refreshAuthSession();
    refreshRemoteAuthSession();
    refreshAccessStatus();
  </script>
</body>
</html>"""
    return (
        page.replace("__INSTALL_SCRIPT_URL__", install_script_url)
        .replace("__PACKAGE_URL__", package_url)
        .replace("__INSTALL_SCRIPT_SHA256__", install_script_sha256)
    )


@app.get("/auth-session/{token}", response_class=HTMLResponse)
async def notebooklm_auth_hub_page(token: str, request: Request) -> str:
    try:
        session = _upload_sync_manager().get_session_status(
            token,
            fallback_base_url=str(request.base_url).rstrip("/"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM upload session was not found.") from exc
    except UploadSyncConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to initialize NotebookLM auth session.", exc)

    escaped_token = html.escape(token, quote=True)
    initial_protocol_url = html.escape(session.get("protocol_url") or "", quote=True)
    android_extension_url = html.escape(_android_extension_package_url(request), quote=True)
    install_script_url = html.escape(_windows_helper_install_script_url(request), quote=True)
    package_url = html.escape(_windows_helper_package_url(request), quote=True)
    install_script_sha256 = html.escape(
        _windows_helper_install_script_sha256(_build_windows_helper_bootstrap_script(request)),
        quote=True,
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NotebookLM Auth Refresh</title>
  <style>
    :root {{
      --panel: #fbfbf8;
      --border: #d6d3ca;
      --text: #1d2430;
      --muted: #667085;
      --accent: #27548a;
      --success: #0f766e;
      --danger: #b42318;
      --shadow: 0 18px 44px rgba(29, 36, 48, 0.10);
      --sans: "Segoe UI", "Trebuchet MS", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: var(--sans);
      color: var(--text);
      background: linear-gradient(140deg, #f5f7f4, #e7ece9);
      min-height: 100vh;
    }}
    .shell {{ width: min(980px, 100%); margin: 0 auto; padding: 18px; display: grid; gap: 16px; }}
    .panel {{
      background: rgba(251, 251, 248, 0.96);
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 18px;
    }}
    .actions, .tabs {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px; }}
    .btn, button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 0;
      border-radius: 8px;
      padding: 12px 14px;
      cursor: pointer;
      color: white;
      text-decoration: none;
      background: var(--accent);
      font: inherit;
      font-weight: 700;
    }}
    .secondary {{ background: var(--success); }}
    .tab {{ width: auto; background: #eceff1; color: var(--text); box-shadow: none; }}
    .tab.active {{ background: var(--accent); color: white; }}
    .pane {{ display: none; }}
    .pane.active {{ display: block; }}
    textarea {{
      width: 100%;
      min-height: 220px;
      margin-top: 10px;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px;
      font: 13px "Cascadia Code", Consolas, monospace;
      color: var(--text);
      background: white;
    }}
    .status-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-top: 16px; }}
    .status-card {{ border: 1px solid var(--border); border-radius: 8px; padding: 12px; background: white; overflow-wrap: anywhere; }}
    .status-card span {{ display: block; color: var(--muted); font-size: 13px; margin-bottom: 4px; }}
    .ok {{ color: var(--success); }}
    .error {{ color: var(--danger); }}
    .hint {{ color: var(--muted); line-height: 1.6; }}
    .step {{ border: 1px solid var(--border); border-radius: 8px; padding: 14px; background: white; margin-top: 12px; }}
    code {{ font-family: "Cascadia Code", Consolas, monospace; background: rgba(29, 36, 48, 0.06); padding: 2px 6px; border-radius: 6px; }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="panel">
      <h1>NotebookLM Auth Refresh</h1>
      <p class="hint">Refresh the managed Google cookies on the VPS. Android import accepts Cookie-Editor JSON, Netscape cookies.txt, or Playwright storage_state.json.</p>
      <div class="tabs">
        <button class="tab active" data-pane="android" type="button">Android / Manual Import</button>
        <button class="tab" data-pane="windows" type="button">Windows Helper</button>
      </div>

      <div id="pane-android" class="pane active">
        <div class="step">
          <h2>Firefox Android quick sync</h2>
          <p class="hint">Install the NotebookLM Auth Sync extension source package after it is signed or loaded in a developer Firefox build. When installed, a sync button appears below and uploads allowed Firefox cookies without manual JSON export.</p>
          <div class="actions">
            <a class="btn secondary" href="{android_extension_url}">Download extension source (.zip)</a>
          </div>
          <div class="actions" id="androidExtensionBridge" data-auth-token="{escaped_token}"></div>
        </div>
        <div class="step">
          <h2>Phone import</h2>
          <p class="hint">Recommended phone path: Firefox Android, open NotebookLM, export cookies for Google/NotebookLM with Cookie-Editor, paste the export here, then submit.</p>
          <textarea id="cookieInput" placeholder='Paste Cookie-Editor JSON, Netscape cookies.txt, or {{"cookies": [...], "origins": []}}'></textarea>
          <div class="actions"><button id="uploadCookieBtn" class="secondary" type="button">Upload cookies</button></div>
        </div>
      </div>

      <div id="pane-windows" class="pane">
        <div class="step">
          <h2>Windows helper</h2>
          <p class="hint">Install the helper once on the Windows PC where Chrome/Edge/Chromium is logged into NotebookLM.</p>
          <div class="actions">
            <a class="btn secondary" href="{install_script_url}">Download installer (.ps1)</a>
            <a class="btn secondary" href="{package_url}">Download helper package (.zip)</a>
          </div>
          <p class="hint">SHA-256 installer: <code>{install_script_sha256}</code></p>
          <div class="actions"><a id="launchLink" class="btn" href="{initial_protocol_url}">Launch local helper</a></div>
        </div>
      </div>

      <p class="hint" id="syncHint">Waiting for an auth refresh upload.</p>
      <div class="status-grid" id="statusGrid"><div class="status-card"><span>Status</span><strong>pending</strong></div></div>
    </section>
  </div>

  <script>
    const token = "{escaped_token}";
    const statusGrid = document.getElementById("statusGrid");
    const syncHint = document.getElementById("syncHint");
    const launchLink = document.getElementById("launchLink");
    const cookieInput = document.getElementById("cookieInput");
    const uploadCookieBtn = document.getElementById("uploadCookieBtn");

    function escapeHtml(value) {{
      return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");
    }}

    function renderStatus(status) {{
      const stateClass = status.status === "completed" ? "ok" : (["failed", "expired", "cancelled"].includes(status.status) ? "error" : "");
      statusGrid.innerHTML = `
        <div class="status-card"><span>Status</span><strong class="${{stateClass}}">${{escapeHtml(status.status || "unknown")}}</strong></div>
        <div class="status-card"><span>Expires</span><strong>${{escapeHtml(status.expires_at || "n/a")}}</strong></div>
        <div class="status-card"><span>Uploaded</span><strong>${{escapeHtml(status.uploaded_at || "pending")}}</strong></div>
        <div class="status-card"><span>Completed</span><strong>${{escapeHtml(status.completed_at || "pending")}}</strong></div>
        <div class="status-card"><span>Browser</span><strong>${{escapeHtml(status.helper_metadata?.browser || status.device?.browser_preference || "auto")}}</strong></div>
        <div class="status-card"><span>Profile</span><strong>${{escapeHtml(status.helper_metadata?.profile || status.device?.profile_preference || "auto")}}</strong></div>
      `;
      if (status.refresh_url) {{
        const card = document.createElement("div");
        card.className = "status-card";
        card.innerHTML = `<span>Refresh URL</span><strong>${{escapeHtml(status.refresh_url)}}</strong>`;
        statusGrid.appendChild(card);
      }}
      if (status.protocol_url) {{
        launchLink.href = status.protocol_url;
      }}
      if (status.error) {{
        syncHint.textContent = status.error;
        syncHint.className = "hint error";
      }} else if (status.status === "completed") {{
        syncHint.textContent = "Auth refresh completed. Managed storage_state.json on the VPS was updated.";
        syncHint.className = "hint";
      }} else {{
        syncHint.textContent = "Waiting for an auth refresh upload.";
        syncHint.className = "hint";
      }}
    }}

    document.querySelectorAll(".tab").forEach((tab) => {{
      tab.addEventListener("click", () => {{
        document.querySelectorAll(".tab").forEach((item) => item.classList.remove("active"));
        document.querySelectorAll(".pane").forEach((item) => item.classList.remove("active"));
        tab.classList.add("active");
        document.getElementById(`pane-${{tab.dataset.pane}}`).classList.add("active");
      }});
    }});

    uploadCookieBtn.addEventListener("click", async () => {{
      try {{
        syncHint.textContent = "Uploading cookies...";
        syncHint.className = "hint";
        const response = await fetch(`/api/public/notebooklm/upload-sessions/${{encodeURIComponent(token)}}`, {{
          method: "POST",
          headers: {{"Content-Type": "application/json"}},
          body: JSON.stringify({{
            storage_state_json: cookieInput.value,
            helper_metadata: {{browser: "firefox-android", profile: "cookie-editor", mode: "android-import"}},
          }}),
        }});
        const body = await response.json();
        if (!response.ok) {{
          throw new Error(body.detail || "Upload failed");
        }}
        renderStatus(body);
      }} catch (error) {{
        syncHint.textContent = error.message || "Upload failed";
        syncHint.className = "hint error";
      }}
    }});

    async function refresh() {{
      const response = await fetch(`/api/public/notebooklm/upload-sessions/${{encodeURIComponent(token)}}`);
      const body = await response.json();
      if (!response.ok) {{
        throw new Error(body.detail || "Request failed");
      }}
      renderStatus(body);
      if (!["completed", "failed", "expired", "cancelled"].includes(body.status)) {{
        setTimeout(refresh, 3000);
      }}
    }}

    refresh().catch((error) => {{
      syncHint.textContent = error.message || "Failed to load NotebookLM auth session.";
      syncHint.className = "hint error";
    }});
  </script>
</body>
</html>"""


@app.get("/auth-session/remote-auth/{token}", response_class=HTMLResponse)
@app.get("/admin/notebooklm/remote-auth/{token}", response_class=HTMLResponse)
async def notebooklm_remote_auth_page(token: str, request: Request) -> str:
    try:
        session = await _remote_auth_manager().ensure_session_started(
            token,
            fallback_base_url=str(request.base_url).rstrip("/"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM remote auth session was not found.") from exc
    except RemoteAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to start NotebookLM remote auth session.", exc)

    escaped_token = html.escape(token, quote=True)
    initial_browser_url = html.escape(session.get("browser_url") or "", quote=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NotebookLM VPS Browser Login</title>
  <style>
    :root {{
      --panel: #fbfbf8;
      --border: #d6d3ca;
      --text: #1d2430;
      --muted: #667085;
      --accent: #27548a;
      --success: #0f766e;
      --danger: #b42318;
      --shadow: 0 18px 44px rgba(29, 36, 48, 0.10);
      --sans: "Segoe UI", "Trebuchet MS", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: var(--sans);
      color: var(--text);
      background: linear-gradient(140deg, #f5f7f4, #e7ece9);
      min-height: 100vh;
    }}
    .shell {{ width: min(980px, 100%); margin: 0 auto; padding: 18px; display: grid; gap: 16px; }}
    .panel {{
      background: rgba(251, 251, 248, 0.96);
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 18px;
    }}
    .actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 14px; }}
    .btn, button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 0;
      border-radius: 8px;
      padding: 12px 14px;
      cursor: pointer;
      color: white;
      text-decoration: none;
      background: var(--accent);
      font: inherit;
      font-weight: 700;
    }}
    button.secondary {{ background: var(--success); }}
    .status-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-top: 16px; }}
    .status-card {{ border: 1px solid var(--border); border-radius: 8px; padding: 12px; background: white; overflow-wrap: anywhere; }}
    .status-card span {{ display: block; color: var(--muted); font-size: 13px; margin-bottom: 4px; }}
    .ok {{ color: var(--success); }}
    .error {{ color: var(--danger); }}
    .hint {{ color: var(--muted); line-height: 1.6; }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="panel">
      <h1>NotebookLM VPS Browser Login</h1>
      <p class="hint">Use the noVNC browser to sign in to Google and open NotebookLM. The VPS will capture allowed Google/NotebookLM cookies after login and close the browser session.</p>
      <div class="actions">
        <a id="browserLink" class="btn" href="{initial_browser_url}" target="_blank" rel="noreferrer">Open remote browser</a>
        <button id="copyBrowserBtn" class="secondary" type="button">Copy browser link</button>
        <button id="cancelBtn" class="secondary" type="button">Cancel session</button>
      </div>
      <p class="hint" id="remoteHint">Waiting for NotebookLM login in the VPS browser.</p>
      <div class="status-grid" id="statusGrid"><div class="status-card"><span>Status</span><strong>launched</strong></div></div>
    </section>
  </div>
  <script>
    const token = "{escaped_token}";
    const statusGrid = document.getElementById("statusGrid");
    const remoteHint = document.getElementById("remoteHint");
    const browserLink = document.getElementById("browserLink");
    const copyBrowserBtn = document.getElementById("copyBrowserBtn");
    const cancelBtn = document.getElementById("cancelBtn");

    function escapeHtml(value) {{
      return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");
    }}

    function renderStatus(status) {{
      const stateClass = status.status === "completed" ? "ok" : (["failed", "expired", "cancelled"].includes(status.status) ? "error" : "");
      statusGrid.innerHTML = `
        <div class="status-card"><span>Status</span><strong class="${{stateClass}}">${{escapeHtml(status.status || "unknown")}}</strong></div>
        <div class="status-card"><span>Expires</span><strong>${{escapeHtml(status.expires_at || "n/a")}}</strong></div>
        <div class="status-card"><span>Launched</span><strong>${{escapeHtml(status.launched_at || "pending")}}</strong></div>
        <div class="status-card"><span>Completed</span><strong>${{escapeHtml(status.completed_at || "pending")}}</strong></div>
      `;
      if (status.browser_url) {{
        browserLink.href = status.browser_url;
      }}
      if (status.error) {{
        remoteHint.textContent = status.error;
        remoteHint.className = "hint error";
      }} else if (status.status === "completed") {{
        remoteHint.textContent = "Auth refresh completed. Managed storage_state.json on the VPS was updated.";
        remoteHint.className = "hint";
      }} else {{
        remoteHint.textContent = "Waiting for NotebookLM login in the VPS browser.";
        remoteHint.className = "hint";
      }}
    }}

    copyBrowserBtn.addEventListener("click", async () => {{
      await navigator.clipboard.writeText(browserLink.href);
    }});

    cancelBtn.addEventListener("click", async () => {{
      const response = await fetch(`/api/public/notebooklm/remote-auth-sessions/${{encodeURIComponent(token)}}/cancel`, {{
        method: "POST",
        headers: {{"Content-Type": "application/json"}},
        body: "{{}}",
      }});
      const body = await response.json();
      if (!response.ok) {{
        throw new Error(body.detail || "Cancel failed");
      }}
      renderStatus(body);
    }});

    async function refresh() {{
      const response = await fetch(`/api/public/notebooklm/remote-auth-sessions/${{encodeURIComponent(token)}}`);
      const body = await response.json();
      if (!response.ok) {{
        throw new Error(body.detail || "Request failed");
      }}
      renderStatus(body);
      if (!["completed", "failed", "expired", "cancelled"].includes(body.status)) {{
        setTimeout(refresh, 3000);
      }}
    }}

    refresh().catch((error) => {{
      remoteHint.textContent = error.message || "Failed to load NotebookLM remote auth session.";
      remoteHint.className = "hint error";
    }});
  </script>
</body>
</html>"""


@app.get("/admin/notebooklm/auth/{token}", response_class=HTMLResponse)
@app.get("/admin/notebooklm/sync/{token}", response_class=HTMLResponse)
async def notebooklm_sync_session_page(token: str, request: Request) -> str:
    try:
        session = _upload_sync_manager().get_session_status(
            token,
            fallback_base_url=str(request.base_url).rstrip("/"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM upload session was not found.") from exc
    except UploadSyncConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to initialize NotebookLM sync session.", exc)

    escaped_token = html.escape(token, quote=True)
    initial_protocol_url = html.escape(session.get("protocol_url") or "", quote=True)
    install_script_url = html.escape(_windows_helper_install_script_url(request), quote=True)
    package_url = html.escape(_windows_helper_package_url(request), quote=True)
    install_script_sha256 = html.escape(
        _windows_helper_install_script_sha256(_build_windows_helper_bootstrap_script(request)),
        quote=True,
    )
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NotebookLM Windows Sync</title>
  <style>
    :root {{
      --bg: #f2efe9;
      --panel: #fffaf5;
      --border: #d7cbb8;
      --text: #1d2430;
      --muted: #667085;
      --accent: #8a3b12;
      --success: #0f766e;
      --danger: #b42318;
      --shadow: 0 18px 44px rgba(58, 42, 23, 0.12);
      --sans: "Segoe UI", "Trebuchet MS", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: var(--sans);
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(138, 59, 18, 0.12), transparent 30%),
        linear-gradient(140deg, #f8f3eb, #eee1cf);
      min-height: 100vh;
    }}
    .shell {{
      width: min(980px, 100%);
      margin: 0 auto;
      padding: 18px;
      display: grid;
      gap: 16px;
    }}
    .panel {{
      background: rgba(255, 250, 245, 0.95);
      border: 1px solid var(--border);
      border-radius: 22px;
      box-shadow: var(--shadow);
      padding: 18px;
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 16px;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 0;
      border-radius: 14px;
      padding: 13px 16px;
      cursor: pointer;
      color: white;
      text-decoration: none;
      background: linear-gradient(135deg, #8a3b12, #b45309);
      box-shadow: 0 10px 24px rgba(138, 59, 18, 0.24);
      font-weight: 700;
    }}
    .btn.secondary {{
      background: linear-gradient(135deg, #0f766e, #155e75);
      box-shadow: 0 10px 24px rgba(15, 118, 110, 0.24);
    }}
    .status-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .status-card {{
      border: 1px solid rgba(215, 203, 184, 0.9);
      border-radius: 16px;
      padding: 12px;
      background: #fffdf8;
    }}
    .status-card span {{
      display: block;
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 4px;
    }}
    .status-card strong.ok {{ color: var(--success); }}
    .status-card strong.error {{ color: var(--danger); }}
    .hint {{ color: var(--muted); line-height: 1.6; }}
    .hint.error {{ color: var(--danger); }}
    .steps {{
      display: grid;
      gap: 12px;
      margin-top: 18px;
      margin-bottom: 18px;
    }}
    .step {{
      border: 1px solid rgba(215, 203, 184, 0.9);
      border-radius: 16px;
      padding: 14px;
      background: #fffdf8;
    }}
    .step h2 {{
      margin: 0 0 8px;
      font-size: 18px;
    }}
    code {{
      font-family: "Cascadia Mono", "Consolas", monospace;
      background: rgba(29, 36, 48, 0.06);
      padding: 2px 6px;
      border-radius: 6px;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="panel">
      <h1>NotebookLM Windows Sync</h1>
      <div class="steps">
        <div class="step">
          <h2>Шаг 1. Установить helper на этот Windows ПК</h2>
          <p class="hint">Этот sync работает только на том Windows-ПК, где helper уже установлен. Если ты открыл UI с другого ноутбука или ПК, сначала скачай и запусти installer именно там.</p>
          <div class="actions">
            <a class="btn secondary" href="{install_script_url}">Скачать installer (.ps1)</a>
            <a class="btn secondary" href="{package_url}">Скачать helper package (.zip)</a>
          </div>
          <p class="hint">SHA-256 installer: <code>{install_script_sha256}</code></p>
        </div>
      </div>
      <p class="hint">Эта одноразовая страница запускает локальный Windows helper. Helper читает живые Google cookies из Chrome/Edge/Chromium на твоём ПК, сохраняет локальный <code>storage_state.json</code> и автоматически загружает его в managed NotebookLM runtime на VPS.</p>
      <div class="actions">
        <a id="launchLink" class="btn" href="{initial_protocol_url}">Запустить локальный helper</a>
      </div>
      <p class="hint" id="syncHint">Если helper уже установлен как custom protocol handler, браузер попробует запустить его автоматически. После завершения sync эта страница обновится сама.</p>
      <div class="status-grid" id="statusGrid">
        <div class="status-card"><span>Status</span><strong>pending</strong></div>
      </div>
    </section>
  </div>

  <script>
    const token = "{escaped_token}";
    const statusGrid = document.getElementById("statusGrid");
    const syncHint = document.getElementById("syncHint");
    const launchLink = document.getElementById("launchLink");
    let autostartAttempted = false;

    function escapeHtml(value) {{
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }}

    function renderStatus(status) {{
      const stateClass = status.status === "completed" ? "ok" : (status.status === "failed" || status.status === "expired" || status.status === "cancelled" ? "error" : "");
      statusGrid.innerHTML = `
        <div class="status-card"><span>Status</span><strong class="${{stateClass}}">${{escapeHtml(status.status || "unknown")}}</strong></div>
        <div class="status-card"><span>Expires</span><strong>${{escapeHtml(status.expires_at || "n/a")}}</strong></div>
        <div class="status-card"><span>Uploaded</span><strong>${{escapeHtml(status.uploaded_at || "pending")}}</strong></div>
        <div class="status-card"><span>Completed</span><strong>${{escapeHtml(status.completed_at || "pending")}}</strong></div>
        <div class="status-card"><span>Browser</span><strong>${{escapeHtml(status.helper_metadata?.browser || status.device?.browser_preference || "auto")}}</strong></div>
        <div class="status-card"><span>Profile</span><strong>${{escapeHtml(status.helper_metadata?.profile || status.device?.profile_preference || "auto")}}</strong></div>
      `;
      if (status.protocol_url) {{
        launchLink.href = status.protocol_url;
      }}
      if (status.error) {{
        syncHint.textContent = status.error;
        syncHint.className = "hint error";
      }} else if (status.status === "completed") {{
        syncHint.textContent = "Sync завершён. Managed storage_state.json на VPS обновлён.";
        syncHint.className = "hint";
      }} else {{
        syncHint.textContent = "Ожидаю upload от локального Windows helper…";
        syncHint.className = "hint";
      }}
      if (!autostartAttempted && status.protocol_url && status.status === "pending") {{
        autostartAttempted = true;
        window.location.href = status.protocol_url;
      }}
    }}

    async function refresh() {{
      const response = await fetch(`/api/public/notebooklm/upload-sessions/${{encodeURIComponent(token)}}`);
      const body = await response.json();
      if (!response.ok) {{
        throw new Error(body.detail || "Request failed");
      }}
      renderStatus(body);
      if (!["completed", "failed", "expired", "cancelled"].includes(body.status)) {{
        setTimeout(refresh, 3000);
      }}
    }}

    refresh().catch((error) => {{
      syncHint.textContent = error.message || "Не удалось загрузить Windows sync session.";
      syncHint.className = "hint error";
    }});
  </script>
</body>
</html>"""


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/health/live")
async def health_live() -> dict:
    return {"status": "ok"}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    snapshot = await NotebookLMHealthService().readiness(force=True)
    payload = {
        "status": "ok" if snapshot.ready else "degraded",
        "reason": snapshot.reason,
        "checked_at": snapshot.checked_at,
        "storage_state_age_seconds": snapshot.storage_state_age_seconds,
        "sync_state_age_seconds": snapshot.sync_state_age_seconds,
        "telegram_tunnel_up": snapshot.telegram_tunnel_up,
        "notebooklm_tunnel_up": snapshot.notebooklm_tunnel_up,
    }
    return JSONResponse(status_code=200 if snapshot.ready else 503, content=payload)


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics() -> PlainTextResponse:
    return PlainTextResponse(render_prometheus_text(), media_type="text/plain; version=0.0.4")


@app.post("/api/nlm")
async def nlm_ask(request: NlmRequest) -> dict:
    settings = get_settings()
    runtime = NotebookLMRuntimeStore(settings=settings)
    try:
        enabled = runtime.is_enabled()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not enabled:
        raise HTTPException(
            status_code=400,
            detail="NotebookLM integration is not enabled for this runtime.",
        )

    from app.services.notebooklm_service import NotebookLMService

    result = await NotebookLMService().ask(chat_id=request.chat_id, question=request.question)
    return {
        "mode": "nlm",
        "answer": result.answer,
        "error": result.error,
        "latency_ms": result.latency_ms,
        "notebook_id": result.notebook_id,
        "sources": result.sources,
        "retrieved": [
            {"text": source, "username": "NotebookLM", "score": None, "url": None}
            for source in result.sources
        ],
    }


@app.get("/api/admin/notebooklm/status")
async def notebooklm_admin_status(_admin_user: str = Depends(_require_notebooklm_admin)) -> dict:
    settings = get_settings()
    try:
        status = NotebookLMRuntimeStore(settings=settings).get_runtime_status()
        augmented = await _augment_notebooklm_status_with_auth_probe(settings, status)
        readiness = await NotebookLMHealthService().readiness()
        augmented["readiness"] = readiness.as_dict()
        augmented["settings_loaded_at"] = getattr(settings, "settings_loaded_at", None)
        augmented["source_sync_enabled"] = is_notebooklm_source_sync_enabled(settings)
        augmented["background_sync_enabled"] = bool(getattr(settings, "notebooklm_background_sync_enabled", False))
        return augmented
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to load NotebookLM admin status.", exc)


@app.post("/api/admin/notebooklm/config")
async def notebooklm_admin_config(
    request: NotebookLMAdminConfigRequest,
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    settings = get_settings()
    try:
        status = NotebookLMRuntimeStore(settings=settings).update_runtime_config(
            enabled=request.enabled,
            notebook_ref=request.notebook_ref,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to save NotebookLM runtime config.", exc)

    from app.services.notebooklm_service import NotebookLMService

    await NotebookLMService.invalidate_cached_client()
    return status


@app.get("/api/admin/notebooklm/auth-sessions/current")
async def notebooklm_admin_current_auth_session(
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    session = _upload_sync_manager().get_latest_session()
    if session is None:
        raise HTTPException(status_code=404, detail="No NotebookLM upload session has been created yet.")
    return session


@app.get("/api/admin/notebooklm/auth-sessions/latest")
async def notebooklm_admin_latest_auth_session(
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    return {"session": _upload_sync_manager().get_latest_session()}


@app.post("/api/admin/notebooklm/auth-sessions")
async def notebooklm_admin_create_auth_session(
    request: NotebookLMAuthSessionCreateRequest,
    http_request: Request,
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    try:
        return _upload_sync_manager().create_session(
            source="admin-ui",
            requested_by_user_id=None,
            requested_by_chat_id=None,
            notify_chat_id=None if not request.notify_in_telegram else None,
            notify_message_thread_id=None,
            fallback_base_url=str(http_request.base_url).rstrip("/"),
        )
    except UploadSyncConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to create NotebookLM upload session.", exc)


@app.get("/api/admin/notebooklm/remote-auth-sessions/current")
async def notebooklm_admin_current_remote_auth_session(
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    session = _remote_auth_manager().get_latest_session()
    if session is None:
        raise HTTPException(status_code=404, detail="No NotebookLM remote auth session has been created yet.")
    return session


@app.get("/api/admin/notebooklm/remote-auth-sessions/latest")
async def notebooklm_admin_latest_remote_auth_session(
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    return {"session": _remote_auth_manager().get_latest_session()}


@app.post("/api/admin/notebooklm/remote-auth-sessions")
async def notebooklm_admin_create_remote_auth_session(
    request: NotebookLMAuthSessionCreateRequest,
    http_request: Request,
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    try:
        return _remote_auth_manager().create_session(
            source="admin-ui",
            requested_by_user_id=None,
            requested_by_chat_id=None,
            notify_chat_id=None if not request.notify_in_telegram else None,
            notify_message_thread_id=None,
            fallback_base_url=str(http_request.base_url).rstrip("/"),
        )
    except RemoteAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to create NotebookLM remote auth session.", exc)


@app.get("/api/admin/notebooklm/remote-auth-sessions/{token}")
async def notebooklm_admin_remote_auth_session_status(token: str) -> dict:
    try:
        return _remote_auth_manager().get_session_status(token)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM remote auth session was not found.") from exc
    except RemoteAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to load NotebookLM remote auth session status.", exc)


@app.post("/api/admin/notebooklm/remote-auth-sessions/{token}/start")
async def notebooklm_admin_start_remote_auth_session(token: str, http_request: Request) -> dict:
    try:
        return await _remote_auth_manager().ensure_session_started(
            token,
            fallback_base_url=str(http_request.base_url).rstrip("/"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM remote auth session was not found.") from exc
    except RemoteAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to start NotebookLM remote auth session.", exc)


@app.post("/api/admin/notebooklm/remote-auth-sessions/{token}/cancel")
async def notebooklm_admin_cancel_remote_auth_session(token: str) -> dict:
    try:
        return await _remote_auth_manager().cancel_session(token)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM remote auth session was not found.") from exc
    except RemoteAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to cancel NotebookLM remote auth session.", exc)


@app.get("/api/public/notebooklm/remote-auth-sessions/{token}")
async def notebooklm_public_remote_auth_session_status(token: str) -> dict:
    try:
        return _remote_auth_manager().get_session_status(token)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM remote auth session was not found.") from exc
    except RemoteAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to load NotebookLM remote auth session status.", exc)


@app.post("/api/public/notebooklm/remote-auth-sessions/{token}/start")
async def notebooklm_public_start_remote_auth_session(token: str, http_request: Request) -> dict:
    try:
        return await _remote_auth_manager().ensure_session_started(
            token,
            fallback_base_url=str(http_request.base_url).rstrip("/"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM remote auth session was not found.") from exc
    except RemoteAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to start NotebookLM remote auth session.", exc)


@app.post("/api/public/notebooklm/remote-auth-sessions/{token}/cancel")
async def notebooklm_public_cancel_remote_auth_session(token: str) -> dict:
    try:
        return await _remote_auth_manager().cancel_session(token)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM remote auth session was not found.") from exc
    except RemoteAuthConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to cancel NotebookLM remote auth session.", exc)


@app.get("/api/public/notebooklm/auth-sessions/{token}")
async def notebooklm_public_auth_session_status(token: str) -> dict:
    try:
        return _upload_sync_manager().get_session_status(token)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM upload session was not found.") from exc
    except UploadSyncConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to load NotebookLM upload session status.", exc)


@app.get("/api/public/notebooklm/upload-sessions/{token}")
async def notebooklm_public_upload_session_status(token: str) -> dict:
    return await notebooklm_public_auth_session_status(token)


@app.post("/api/public/notebooklm/upload-sessions/{token}")
async def notebooklm_public_complete_upload_session(
    token: str,
    request: NotebookLMUploadSessionRequest,
) -> dict:
    try:
        return await _upload_sync_manager().complete_upload(
            token,
            request.storage_state_json,
            helper_metadata=request.helper_metadata,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM upload session was not found.") from exc
    except UploadSyncConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid storage-state JSON: {exc.msg}") from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to complete NotebookLM upload session.", exc)


@app.post("/api/admin/notebooklm/storage-state")
async def notebooklm_admin_storage_state(
    request: NotebookLMStorageStateRequest,
    _admin_user: str = Depends(_require_notebooklm_admin),
) -> dict:
    settings = get_settings()
    try:
        status = NotebookLMRuntimeStore(settings=settings).replace_storage_state(
            request.storage_state_json
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid storage-state JSON: {exc.msg}") from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to update NotebookLM storage state.", exc)

    from app.services.notebooklm_service import NotebookLMService

    await NotebookLMService.invalidate_cached_client()
    return status


@app.post("/api/public/notebooklm/auth-sessions/{token}/cancel")
async def notebooklm_public_auth_session_cancel(token: str) -> dict:
    try:
        return await _upload_sync_manager().cancel_session(token)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM upload session was not found.") from exc
    except UploadSyncConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to cancel NotebookLM upload session.", exc)


@app.post("/api/public/notebooklm/upload-refresh/{token}")
async def notebooklm_public_refresh_upload_session(
    token: str,
    request: NotebookLMUploadSessionRequest,
) -> dict:
    try:
        return await _upload_sync_manager().refresh_from_device(
            token,
            request.storage_state_json,
            helper_metadata=request.helper_metadata,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="NotebookLM refresh token was not found.") from exc
    except UploadSyncConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid storage-state JSON: {exc.msg}") from exc
    except Exception as exc:
        _raise_admin_internal_error("Failed to refresh NotebookLM upload sync.", exc)


@app.get("/api/public/notebooklm/windows-helper/install.ps1", response_class=PlainTextResponse)
async def notebooklm_windows_helper_install_script(request: Request) -> str:
    return _build_windows_helper_bootstrap_script(request)


@app.get("/api/public/notebooklm/windows-helper/install.ps1.sha256", response_class=PlainTextResponse)
async def notebooklm_windows_helper_install_script_sha256(request: Request) -> str:
    script = _build_windows_helper_bootstrap_script(request)
    return _windows_helper_install_script_sha256(script) + "\n"


@app.get("/api/public/notebooklm/windows-helper/package.zip")
async def notebooklm_windows_helper_package() -> StreamingResponse:
    archive_buffer = _build_windows_helper_package()
    headers = {
        "Content-Disposition": 'attachment; filename="notebooklm-windows-helper.zip"',
    }
    return StreamingResponse(archive_buffer, media_type="application/zip", headers=headers)


@app.get("/api/public/notebooklm/android-extension/package.zip")
async def notebooklm_android_extension_package() -> StreamingResponse:
    archive_buffer = _build_android_extension_package()
    headers = {
        "Content-Disposition": 'attachment; filename="notebooklm-auth-sync-extension.zip"',
    }
    return StreamingResponse(archive_buffer, media_type="application/zip", headers=headers)
