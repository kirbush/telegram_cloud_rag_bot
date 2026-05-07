import asyncio
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

try:
    from windows_chromium_auth import (
        extract_browser_storage_state,
        helper_config_path,
        parse_sync_launch_uri,
    )
except ImportError:
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from app.services.windows_chromium_auth import (  # type: ignore
        extract_browser_storage_state,
        helper_config_path,
        parse_sync_launch_uri,
    )

_DEFAULT_PROTOCOL_SCHEME = "tgctxbot-notebooklm-sync"


def _iso_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _load_helper_config() -> dict:
    path = helper_config_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_helper_config(payload: dict) -> None:
    path = helper_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _helper_log_path() -> Path:
    return helper_config_path().with_name("last-sync.log")


def _local_storage_state_path() -> Path:
    return helper_config_path().with_name("storage_state.json")


def _save_local_storage_state(storage_state: dict) -> str:
    target = _local_storage_state_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(storage_state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(target)


def _candidate_local_storage_state_paths(helper_config: dict) -> list[Path]:
    candidates: list[Path] = []
    configured = str(helper_config.get("local_storage_state_path", "") or "").strip()
    if configured:
        candidates.append(Path(configured).expanduser())
    candidates.append(_local_storage_state_path())
    candidates.append(Path.home() / ".notebooklm" / "storage_state.json")

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _load_local_storage_state(helper_config: dict) -> tuple[dict, str]:
    for target in _candidate_local_storage_state_paths(helper_config):
        if not target.exists():
            continue
        payload = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError(f"Local storage_state.json does not contain an object payload: {target}")
        return payload, str(target)
    raise FileNotFoundError(
        "Local NotebookLM storage_state.json was not found in any known helper path."
    )


def _post_storage_state(url: str, storage_state: dict, helper_metadata: dict) -> dict:
    response = httpx.post(
        url,
        json={
            "storage_state_json": json.dumps(storage_state, ensure_ascii=False),
            "helper_metadata": helper_metadata,
        },
        timeout=90.0,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("NotebookLM sync endpoint returned a non-object payload.")
    return payload


async def _validate_notebooklm_storage_state(storage_path: Path) -> None:
    from notebooklm._url_utils import is_google_auth_redirect
    from notebooklm.auth import load_httpx_cookies

    cookies = load_httpx_cookies(storage_path)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, cookies=cookies) as http_client:
        response = await http_client.get("https://notebooklm.google.com/")
        response.raise_for_status()

    final_url = str(response.url)
    if is_google_auth_redirect(final_url):
        raise ValueError(
            "NotebookLM local auth is expired or invalid. "
            f"Redirected to: {final_url}"
        )


def _preferred_login_storage_path(helper_config: dict) -> Path:
    configured = str(helper_config.get("local_storage_state_path", "") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".notebooklm" / "storage_state.json"


def _run_browser_login(storage_path: Path) -> None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright Python package is not installed for NotebookLM browser login."
        ) from exc

    try:
        from notebooklm.cli.session import NOTEBOOKLM_URL, _ensure_chromium_installed, _windows_playwright_event_loop
    except ImportError as exc:
        raise RuntimeError("NotebookLM browser login helpers are not available.") from exc

    storage_path.parent.mkdir(parents=True, exist_ok=True)
    browser_profile = storage_path.parent / "browser_profile"
    browser_profile.mkdir(parents=True, exist_ok=True)
    _ensure_chromium_installed()

    print("Opening browser for Google login...")
    print(f"Using persistent profile: {browser_profile}")

    deadline = time.time() + 15 * 60
    last_validation_error = None

    with _windows_playwright_event_loop(), sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(browser_profile),
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--password-store=basic",
            ],
            ignore_default_args=["--enable-automation"],
        )
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(NOTEBOOKLM_URL)
            print("Finish the Google login in the browser window. The helper will save auth automatically.")

            while time.time() < deadline:
                current_url = ""
                try:
                    current_url = page.url or ""
                except Exception:
                    current_url = ""

                if "notebooklm.google.com" in current_url and "accounts.google.com" not in current_url:
                    context.storage_state(path=str(storage_path))
                    try:
                        asyncio.run(_validate_notebooklm_storage_state(storage_path))
                        print("NotebookLM browser login completed successfully.")
                        return
                    except Exception as exc:
                        last_validation_error = str(exc)

                page.wait_for_timeout(2000)
        finally:
            context.close()

    detail = f" Last validation error: {last_validation_error}" if last_validation_error else ""
    raise RuntimeError(
        "NotebookLM browser login timed out before a valid auth state was detected." + detail
    )


def _run_sync(args: argparse.Namespace) -> int:
    if os.name != "nt":
        print("NotebookLM Windows sync helper is supported only on Windows.")
        return 1

    helper_config = _load_helper_config()
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
                expected_scheme=args.protocol_scheme or _DEFAULT_PROTOCOL_SCHEME,
            )
        except ValueError as exc:
            print(f"Invalid NotebookLM sync launch URI: {exc}")
            return 1
        upload_url = upload_url or launch_payload.get("upload_url")
        refresh_url = refresh_url or launch_payload.get("refresh_url")
        status_url = status_url or launch_payload.get("status_url")
        entry_url = entry_url or launch_payload.get("entry_url")
        browser = browser if browser != "auto" else (launch_payload.get("browser") or "auto").strip().lower()
        profile = profile if profile != "auto" else (launch_payload.get("profile") or "auto").strip()

    mode = "upload"
    target_url = upload_url
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
            local_storage_path = _save_local_storage_state(storage_state)
        except Exception as extraction_exc:
            if args.scheduled:
                raise
            local_validation_error = None
            try:
                storage_state, local_storage_path = _load_local_storage_state(helper_config)
                asyncio.run(_validate_notebooklm_storage_state(Path(local_storage_path)))
                helper_metadata = {
                    "browser": "existing-storage-state",
                    "profile": "managed-storage",
                    "cookie_count": len(storage_state.get("cookies", [])),
                    "mode": "existing-storage-fallback",
                    "synced_at": _iso_now(),
                    "extraction_error": str(extraction_exc),
                }
            except Exception as local_state_exc:
                local_validation_error = str(local_state_exc)
                login_storage_path = _preferred_login_storage_path(helper_config)
                _run_browser_login(login_storage_path)
                asyncio.run(_validate_notebooklm_storage_state(login_storage_path))
                storage_state, local_storage_path = _load_local_storage_state(
                    {
                        **helper_config,
                        "local_storage_state_path": str(login_storage_path),
                    }
                )
                helper_metadata = {
                    "browser": "notebooklm-browser-login",
                    "profile": "managed-storage",
                    "cookie_count": len(storage_state.get("cookies", [])),
                    "mode": "browser-login-refresh",
                    "synced_at": _iso_now(),
                    "extraction_error": str(extraction_exc),
                    "local_validation_error": local_validation_error,
                }

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
    _save_helper_config(config_payload)

    print(
        json.dumps(
            {
                "timestamp": _iso_now(),
                "command": "notebooklm-windows-sync-helper",
                "mode": mode,
                "target_url": target_url,
                "status_url": status_url,
                "entry_url": entry_url,
                "helper_metadata": helper_metadata,
                "refresh_url": payload.get("refresh_url"),
                "local_storage_state_path": local_storage_path,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NotebookLM Windows sync helper")
    parser.add_argument("--launch-uri", default="")
    parser.add_argument("--upload-url", default="")
    parser.add_argument("--refresh-url", default="")
    parser.add_argument("--status-url", default="")
    parser.add_argument("--entry-url", default="")
    parser.add_argument("--browser", default="auto")
    parser.add_argument("--profile", default="auto")
    parser.add_argument("--protocol-scheme", default=_DEFAULT_PROTOCOL_SCHEME)
    parser.add_argument("--scheduled", action="store_true")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        return _run_sync(args)
    except Exception as exc:  # pragma: no cover - final guard for silent helper exits
        log_path = _helper_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            f"{_iso_now()} unhandled helper error: {exc}\n",
            encoding="utf-8",
        )
        print(f"NotebookLM Windows sync failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
