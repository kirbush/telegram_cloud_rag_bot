import argparse
import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx


def _load_helper_module():
    module_name = "notebooklm_windows_sync_helper_test_module"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing

    helper_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "windows"
        / "notebooklm_windows_sync_helper.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load notebooklm_windows_sync_helper.py for tests.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class WindowsSyncHelperTests(unittest.TestCase):
    def test_validate_notebooklm_storage_state_uses_cookie_jar_without_raw_cookie_header(self) -> None:
        helper = _load_helper_module()
        captured_init_kwargs = {}
        captured_get_kwargs = {}
        fake_cookies = httpx.Cookies()
        fake_cookies.set("SID", "sid-cookie", domain="google.com", path="/")

        class _FakeResponse:
            def __init__(self, url: str) -> None:
                self.url = url

            def raise_for_status(self) -> None:
                return None

        class _FakeAsyncClient:
            def __init__(self, **kwargs) -> None:
                captured_init_kwargs.update(kwargs)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb) -> bool:
                return False

            async def get(self, url: str, **kwargs):
                captured_get_kwargs.update(kwargs)
                return _FakeResponse(url)

        with (
            patch("notebooklm.auth.load_httpx_cookies", return_value=fake_cookies) as load_httpx_cookies,
            patch("notebooklm._url_utils.is_google_auth_redirect", return_value=False),
            patch.object(helper.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            import asyncio

            asyncio.run(helper._validate_notebooklm_storage_state(Path("C:/storage_state.json")))

        load_httpx_cookies.assert_called_once()
        self.assertIs(captured_init_kwargs["cookies"], fake_cookies)
        self.assertTrue(captured_init_kwargs["follow_redirects"])
        self.assertNotIn("headers", captured_get_kwargs)

    def test_run_sync_uses_browser_login_when_local_auth_is_expired(self) -> None:
        helper = _load_helper_module()
        args = argparse.Namespace(
            launch_uri="",
            upload_url="https://example.test/upload",
            refresh_url="",
            status_url="https://example.test/status",
            entry_url="https://example.test/entry",
            browser="auto",
            profile="auto",
            protocol_scheme="tgctxbot-notebooklm-sync",
            scheduled=False,
        )
        expired_storage = ({"cookies": [], "origins": []}, "C:\\expired-storage-state.json")
        refreshed_storage = (
            {"cookies": [{"name": "SID", "value": "fresh"}], "origins": []},
            "C:\\fresh-storage-state.json",
        )

        with (
            patch.object(helper, "os", SimpleNamespace(name="nt")),
            patch.object(helper, "_load_helper_config", return_value={}),
            patch.object(helper, "extract_browser_storage_state", side_effect=RuntimeError("chrome cookies unavailable")),
            patch.object(helper, "_load_local_storage_state", side_effect=[expired_storage, refreshed_storage]),
            patch.object(
                helper,
                "_validate_notebooklm_storage_state",
                side_effect=[ValueError("Authentication expired or invalid."), None],
            ),
            patch.object(helper, "_run_browser_login") as run_browser_login,
            patch.object(helper, "_post_storage_state", return_value={"refresh_url": "https://example.test/refresh"}),
            patch.object(helper, "_save_helper_config") as save_helper_config,
        ):
            result = helper._run_sync(args)

        self.assertEqual(result, 0)
        run_browser_login.assert_called_once()
        save_helper_config.assert_called_once()
        saved_payload = save_helper_config.call_args.args[0]
        self.assertEqual(saved_payload["browser"], "notebooklm-browser-login")
        self.assertEqual(saved_payload["profile"], "managed-storage")
        self.assertEqual(saved_payload["local_storage_state_path"], "C:\\fresh-storage-state.json")

    def test_run_sync_returns_failure_when_browser_login_cannot_complete(self) -> None:
        helper = _load_helper_module()
        args = argparse.Namespace(
            launch_uri="",
            upload_url="https://example.test/upload",
            refresh_url="",
            status_url="https://example.test/status",
            entry_url="https://example.test/entry",
            browser="auto",
            profile="auto",
            protocol_scheme="tgctxbot-notebooklm-sync",
            scheduled=False,
        )

        with (
            patch.object(helper, "os", SimpleNamespace(name="nt")),
            patch.object(helper, "_load_helper_config", return_value={}),
            patch.object(helper, "extract_browser_storage_state", side_effect=RuntimeError("chrome cookies unavailable")),
            patch.object(helper, "_load_local_storage_state", side_effect=FileNotFoundError("missing local state")),
            patch.object(helper, "_run_browser_login", side_effect=RuntimeError("NotebookLM browser login failed")),
            patch.object(helper, "_post_storage_state", new=Mock()),
        ):
            result = helper._run_sync(args)

        self.assertEqual(result, 1)
