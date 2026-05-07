"""NotebookLM CLI — login/list/test/windows-sync helpers for the lightweight bot."""

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

from app.core.config import get_notebooklm_proxy_url, get_settings
from app.services.windows_chromium_auth import (
    extract_browser_storage_state,
    helper_config_path,
    parse_sync_launch_uri,
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dump(payload: dict) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        print(rendered)
    except UnicodeEncodeError:
        buffer = getattr(sys.stdout, "buffer", None)
        if buffer is None:
            raise
        buffer.write((rendered + "\n").encode("utf-8", errors="replace"))
        buffer.flush()


def _resolve_notebooklm_executable() -> str | None:
    candidates = [
        Path(sys.executable).with_name("notebooklm.exe"),
        Path(sys.executable).with_name("notebooklm"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return shutil.which("notebooklm")


async def _list_notebooklm_notebooks(storage_path: str):
    from app.services.notebooklm_client import create_notebooklm_client

    settings = get_settings()
    client = await create_notebooklm_client(
        storage_path,
        settings.notebooklm_timeout,
        get_notebooklm_proxy_url(settings),
    )
    async with client:
        return await client.notebooks.list()


async def _ask_notebooklm_question(storage_path: str, notebook_id: str, question: str):
    from app.services.notebooklm_client import create_notebooklm_client

    settings = get_settings()
    client = await create_notebooklm_client(
        storage_path,
        settings.notebooklm_timeout,
        get_notebooklm_proxy_url(settings),
    )
    async with client:
        return await client.chat.ask(notebook_id, question)


def _format_notebooklm_exception(exc: Exception) -> str:
    error_text = str(exc)
    lowered = error_text.lower()
    if "missing required cookies" in lowered:
        return error_text
    if "401" in error_text or "auth" in lowered or "login" in lowered:
        return "Auth expired. Run: python -m app.cli notebooklm-login"
    return f"Error: {exc}"


def _run_notebooklm_login(args: argparse.Namespace) -> int:
    """Direct local NotebookLM login is disabled — use the VPS remote-auth flow."""
    print(
        "Direct `python -m app.cli notebooklm-login` is disabled because it can bypass "
        "the enforced NotebookLM Google proxy policy.\n"
        "Use the VPS remote-auth flow (/auth_nlm or /admin/notebooklm) instead."
    )
    return 1


def _run_notebooklm_notebooks(args: argparse.Namespace) -> int:
    settings = get_settings()
    storage_path = str(Path(settings.notebooklm_storage_state).expanduser())

    try:
        from notebooklm import NotebookLMClient  # noqa: F401
    except ImportError:
        print("Error: notebooklm-py is not installed.")
        return 1

    try:
        notebooks = asyncio.run(_list_notebooklm_notebooks(storage_path))
    except FileNotFoundError:
        print(f"Storage state not found at {storage_path}. Run: python -m app.cli notebooklm-login")
        return 1
    except Exception as exc:
        print(_format_notebooklm_exception(exc))
        return 1

    print(f"Notebooks ({len(notebooks)}):")
    for notebook in notebooks:
        notebook_id = getattr(notebook, "id", "?")
        notebook_title = getattr(notebook, "title", getattr(notebook, "name", "Untitled"))
        print(f"  {notebook_id}  {notebook_title}")
    return 0


def _run_notebooklm_test(args: argparse.Namespace) -> int:
    settings = get_settings()
    storage_path = str(Path(settings.notebooklm_storage_state).expanduser())

    try:
        from notebooklm import NotebookLMClient  # noqa: F401
    except ImportError:
        print("Error: notebooklm-py is not installed.")
        return 1

    try:
        response = asyncio.run(
            _ask_notebooklm_question(storage_path, args.notebook_id, args.question)
        )
    except FileNotFoundError:
        print(f"Storage state not found at {storage_path}. Run: python -m app.cli notebooklm-login")
        return 1
    except Exception as exc:
        print(_format_notebooklm_exception(exc))
        return 1

    answer_text = getattr(response, "answer", None) or getattr(response, "text", None) or str(response)
    sources = [
        getattr(reference, "cited_text", None) or getattr(reference, "source_id", str(reference))
        for reference in (getattr(response, "references", None) or [])
    ]
    _json_dump(
        {
            "notebook_id": args.notebook_id,
            "question": args.question,
            "answer": answer_text,
            "sources": sources,
        }
    )
    return 0


def _load_windows_helper_config() -> dict:
    path = helper_config_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_windows_helper_config(payload: dict) -> None:
    path = helper_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _save_local_notebooklm_storage_state(storage_state: dict) -> str:
    settings = get_settings()
    target = Path(settings.notebooklm_storage_state).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(storage_state, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return str(target)


def _load_local_notebooklm_storage_state() -> tuple[dict, str]:
    settings = get_settings()
    target = Path(settings.notebooklm_storage_state).expanduser()
    payload = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Local NotebookLM storage_state.json does not contain an object payload.")
    return payload, str(target)


def _login_notebooklm_locally_for_sync() -> tuple[dict, dict, str]:
    storage_path = Path(get_settings().notebooklm_storage_state).expanduser()
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    notebooklm_exe = _resolve_notebooklm_executable()
    if notebooklm_exe is None:
        raise RuntimeError("notebooklm CLI is not installed for local login fallback.")
    completed = subprocess.run(
        [notebooklm_exe, "login", "--storage", str(storage_path)],
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("Local NotebookLM login fallback did not complete successfully.")
    storage_state, local_storage_path = _load_local_notebooklm_storage_state()
    helper_metadata = {
        "browser": "notebooklm-cli",
        "profile": "managed-storage",
        "cookie_count": len(storage_state.get("cookies", [])),
        "mode": "login-fallback",
        "synced_at": _iso_now(),
    }
    return storage_state, helper_metadata, local_storage_path


def _post_storage_state(url: str, storage_state: dict, helper_metadata: dict) -> dict:
    response = httpx.post(
        url,
        json={
            "storage_state_json": json.dumps(storage_state, ensure_ascii=False),
            "helper_metadata": helper_metadata,
        },
        timeout=60.0,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("NotebookLM sync endpoint returned a non-object payload.")
    return payload


def _run_notebooklm_windows_sync(args: argparse.Namespace) -> int:
    if os.name != "nt":
        print("NotebookLM Windows sync helper is supported only on Windows.")
        return 1

    settings = get_settings()
    helper_config = _load_windows_helper_config()
    browser = (args.browser or "auto").strip().lower() or "auto"
    profile = (args.profile or "auto").strip() or "auto"

    upload_url = args.upload_url
    refresh_url = args.refresh_url
    status_url = args.status_url
    entry_url = args.entry_url

    if args.launch_uri:
        try:
            launch_payload = parse_sync_launch_uri(
                args.launch_uri,
                expected_scheme=settings.notebooklm_windows_helper_protocol_scheme,
            )
        except ValueError as exc:
            print(f"Invalid NotebookLM sync launch URI: {exc}")
            return 1
        upload_url = upload_url or launch_payload.get("upload_url")
        status_url = status_url or launch_payload.get("status_url")
        entry_url = entry_url or launch_payload.get("entry_url")
        browser = browser if browser != "auto" else (launch_payload.get("browser") or "auto").strip().lower()
        profile = profile if profile != "auto" else (launch_payload.get("profile") or "auto").strip()

    target_url = upload_url
    mode = "upload"
    if args.scheduled:
        mode = "refresh"
        target_url = refresh_url or str(helper_config.get("refresh_url", "") or "").strip()
        browser = browser if browser != "auto" else str(helper_config.get("browser", "auto") or "auto").strip().lower()
        profile = profile if profile != "auto" else str(helper_config.get("profile", "auto") or "auto").strip()
        status_url = status_url or str(helper_config.get("status_url", "") or "").strip() or None
        entry_url = entry_url or str(helper_config.get("entry_url", "") or "").strip() or None

    if not target_url:
        print("NotebookLM sync target URL is missing. Use --launch-uri, --upload-url, or --scheduled.")
        return 1

    try:
        try:
            storage_state, metadata = extract_browser_storage_state(
                browser_preference=browser,
                profile_preference=profile,
            )
            helper_metadata = {
                **metadata,
                "browser": metadata.get("browser", browser),
                "profile": metadata.get("profile", profile),
                "mode": mode,
                "synced_at": _iso_now(),
            }
            local_storage_path = _save_local_notebooklm_storage_state(storage_state)
        except Exception as extraction_exc:
            if args.scheduled:
                raise
            try:
                storage_state, local_storage_path = _load_local_notebooklm_storage_state()
                helper_metadata = {
                    "browser": "existing-storage-state",
                    "profile": "managed-storage",
                    "cookie_count": len(storage_state.get("cookies", [])),
                    "mode": "existing-storage-fallback",
                    "synced_at": _iso_now(),
                    "extraction_error": str(extraction_exc),
                }
            except Exception:
                storage_state, helper_metadata, local_storage_path = _login_notebooklm_locally_for_sync()
                helper_metadata["extraction_error"] = str(extraction_exc)
        payload = _post_storage_state(target_url, storage_state, helper_metadata)
    except Exception as exc:
        print(f"NotebookLM Windows sync failed: {exc}")
        return 1

    config_payload = {
        "refresh_url": payload.get("refresh_url") or helper_config.get("refresh_url"),
        "status_url": status_url or helper_config.get("status_url"),
        "entry_url": entry_url or helper_config.get("entry_url"),
        "browser": helper_metadata.get("browser", browser),
        "profile": helper_metadata.get("profile", profile),
        "last_sync_at": helper_metadata["synced_at"],
        "last_cookie_count": helper_metadata.get("cookie_count"),
        "local_storage_state_path": local_storage_path,
    }
    _save_windows_helper_config(config_payload)

    _json_dump(
        {
            "timestamp": _iso_now(),
            "command": "notebooklm-windows-sync",
            "mode": mode,
            "browser": helper_metadata.get("browser"),
            "profile": helper_metadata.get("profile"),
            "cookie_count": helper_metadata.get("cookie_count"),
            "local_storage_state_path": local_storage_path,
            "status_url": status_url,
            "entry_url": entry_url,
            "response": payload,
        }
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="NotebookLM CLI for the lightweight Telegram bot"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "notebooklm-login",
        help="Disabled: use VPS remote-auth flow (/auth_nlm or /admin/notebooklm)",
    )

    sub.add_parser(
        "notebooklm-notebooks",
        help="List all NotebookLM notebooks with IDs",
    )

    nlm_test = sub.add_parser(
        "notebooklm-test",
        help="Test a query against a NotebookLM notebook",
    )
    nlm_test.add_argument("--notebook-id", required=True, help="NotebookLM notebook ID")
    nlm_test.add_argument("--question", required=True, help="Question to ask")

    windows_sync = sub.add_parser(
        "notebooklm-windows-sync",
        help="Extract live Chromium cookies on Windows and sync storage_state.json to the VPS NotebookLM runtime",
    )
    windows_sync.add_argument("--launch-uri", default=None, help="Custom protocol launch URI from the admin UI")
    windows_sync.add_argument("--upload-url", default=None, help="Explicit one-time upload URL")
    windows_sync.add_argument("--refresh-url", default=None, help="Explicit refresh URL for scheduled sync")
    windows_sync.add_argument("--status-url", default=None, help="Status page URL for the active sync session")
    windows_sync.add_argument("--entry-url", default=None, help="One-time human-facing sync page URL")
    windows_sync.add_argument("--browser", default="auto", help="chrome, edge, chromium, or auto")
    windows_sync.add_argument("--profile", default="auto", help="Browser profile name, e.g. Default or Profile 1")
    windows_sync.add_argument(
        "--scheduled",
        action="store_true",
        default=False,
        help="Use the saved refresh token from the helper config and perform a background refresh",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "notebooklm-login":
        return _run_notebooklm_login(args)
    if args.command == "notebooklm-notebooks":
        return _run_notebooklm_notebooks(args)
    if args.command == "notebooklm-test":
        return _run_notebooklm_test(args)
    if args.command == "notebooklm-windows-sync":
        return _run_notebooklm_windows_sync(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
