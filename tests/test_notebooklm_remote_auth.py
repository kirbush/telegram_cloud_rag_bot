import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from app.services.notebooklm_remote_auth import (
    DockerRemoteBrowserLauncher,
    NotebookLMRemoteAuthBrowser,
    NotebookLMRemoteAuthManager,
    NotebookLMRemoteAuthStore,
)


class NotebookLMRemoteAuthStoreTests(unittest.TestCase):
    def test_store_persists_only_token_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "auth_sessions.json"
            settings = SimpleNamespace(notebooklm_remote_auth_state_path=str(state_path))
            store = NotebookLMRemoteAuthStore(settings=settings)

            store.create_session(
                token_hash="hashed-token",
                source="telegram-bot",
                requested_by_user_id=42,
                requested_by_chat_id=24,
                notify_chat_id=24,
                notify_message_thread_id=None,
                ttl_seconds=900,
            )

            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["sessions"][0]["token_hash"], "hashed-token")
            self.assertNotIn("token-plain", state_path.read_text(encoding="utf-8"))


class _FakeLauncher:
    def __init__(self) -> None:
        self.launch_calls: list[str] = []
        self.browser = NotebookLMRemoteAuthBrowser(
            container_id="container-1",
            container_name="tgctxbot-nlm-auth-test",
            webdriver_port=4444,
            novnc_port=7900,
            vnc_password="secret",
            webdriver_session_id="webdriver-1",
            browser_url="http://example.test:7900/",
            started_at="2026-04-17T10:00:00+00:00",
        )
        self.cleanup = AsyncMock()
        self.list_remote_auth_containers = AsyncMock(return_value=[])
        self.remove_container = AsyncMock()
        self.navigate = AsyncMock()
        self.get_current_url = AsyncMock(return_value="https://notebooklm.google.com/")
        self.get_cookies = AsyncMock(
            side_effect=[
                [
                    {
                        "name": "__Secure-1PSID",
                        "value": "cookie-value",
                        "domain": ".google.com",
                        "path": "/",
                        "expiry": -1,
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    }
                ],
                [
                    {
                        "name": "SID",
                        "value": "cookie-value-2",
                        "domain": ".google.com",
                        "path": "/",
                        "expiry": -1,
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    }
                ],
            ]
        )

    async def launch(self, *, public_base_url: str, session_id: str) -> NotebookLMRemoteAuthBrowser:
        self.launch_calls.append(session_id)
        return self.browser


class NotebookLMRemoteAuthManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_session_cancels_existing_launched_browser(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "auth_sessions.json"
            settings = SimpleNamespace(
                notebooklm_remote_auth_state_path=str(state_path),
                notebooklm_remote_auth_ttl_seconds=900,
                notebooklm_remote_auth_poll_seconds=1.0,
                notebooklm_remote_auth_base_url="http://example.test:8010",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            launcher = _FakeLauncher()
            manager = NotebookLMRemoteAuthManager(
                settings=settings,
                runtime_store=SimpleNamespace(replace_storage_state=unittest.mock.Mock()),
                launcher=launcher,
            )

            created_first = manager.create_session(
                source="telegram-bot",
                requested_by_user_id=42,
                requested_by_chat_id=24,
                notify_chat_id=24,
                notify_message_thread_id=None,
            )
            self.assertIn("/auth-session/remote-auth/", created_first["auth_url"])
            token_first = created_first["auth_url"].rsplit("/", 1)[-1]
            await manager.ensure_session_started(token_first)

            created_second = manager.create_session(
                source="telegram-bot",
                requested_by_user_id=42,
                requested_by_chat_id=24,
                notify_chat_id=24,
                notify_message_thread_id=None,
            )
            token_second = created_second["auth_url"].rsplit("/", 1)[-1]
            await manager.ensure_session_started(token_second)

            first_session = manager.get_session_by_token(token_first)
            second_session = manager.get_session_by_token(token_second)

            self.assertEqual(first_session.status, "cancelled")
            self.assertEqual(first_session.error, "Auth session replaced by a newer login link.")
            self.assertIsNotNone(first_session.completed_at)
            self.assertIsNone(first_session.browser)
            self.assertEqual(second_session.status, "launched")
            self.assertEqual(len(launcher.launch_calls), 2)
            launcher.cleanup.assert_awaited_once()

    @patch("app.services.notebooklm_service.NotebookLMService.invalidate_cached_client", new_callable=AsyncMock)
    async def test_capture_authenticated_state_updates_runtime_and_marks_completed(
        self,
        invalidate_cached_client,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "auth_sessions.json"
            settings = SimpleNamespace(
                notebooklm_remote_auth_state_path=str(state_path),
                notebooklm_remote_auth_ttl_seconds=900,
                notebooklm_remote_auth_poll_seconds=1.0,
                notebooklm_remote_auth_base_url="http://example.test:8010",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            runtime_store = SimpleNamespace(replace_storage_state=unittest.mock.Mock(return_value={"auth_ready": True}))
            launcher = _FakeLauncher()
            manager = NotebookLMRemoteAuthManager(
                settings=settings,
                runtime_store=runtime_store,
                launcher=launcher,
            )
            manager._notify = AsyncMock()  # type: ignore[method-assign]

            created = manager.create_session(
                source="telegram-bot",
                requested_by_user_id=42,
                requested_by_chat_id=24,
                notify_chat_id=24,
                notify_message_thread_id=None,
            )
            token = created["auth_url"].rsplit("/", 1)[-1]
            await manager.ensure_session_started(token)
            session = manager.get_session_by_token(token)

            captured = await manager._maybe_capture_authenticated_state(session)  # noqa: SLF001

            self.assertTrue(captured)
            stored = manager.get_session_by_token(token)
            self.assertEqual(stored.status, "completed")
            runtime_store.replace_storage_state.assert_called_once()
            stored_json = runtime_store.replace_storage_state.call_args.args[0]
            payload = json.loads(stored_json)
            self.assertIn("cookies", payload)
            invalidate_cached_client.assert_awaited_once()
            manager._notify.assert_awaited_once()  # type: ignore[attr-defined]
            launcher.cleanup.assert_awaited_once()

    async def test_janitor_reaps_terminal_session_and_stale_orphan_container(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "auth_sessions.json"
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_vps_lightweight_mode=True,
                notebooklm_janitor_enabled=True,
                notebooklm_janitor_interval_seconds=60,
                notebooklm_remote_auth_state_path=str(state_path),
                notebooklm_remote_auth_ttl_seconds=900,
                notebooklm_remote_auth_poll_seconds=1.0,
                notebooklm_remote_auth_base_url="http://example.test:8010",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            launcher = _FakeLauncher()
            old_epoch = int((datetime.now(timezone.utc) - timedelta(seconds=3600)).timestamp())
            launcher.list_remote_auth_containers.return_value = [
                {
                    "Id": "orphan-container",
                    "Created": old_epoch,
                    "Labels": {"app.telegram-context-search-bot.session-id": "ghost-session"},
                }
            ]
            manager = NotebookLMRemoteAuthManager(
                settings=settings,
                runtime_store=SimpleNamespace(replace_storage_state=unittest.mock.Mock()),
                launcher=launcher,
            )

            created = manager.create_session(
                source="telegram-bot",
                requested_by_user_id=42,
                requested_by_chat_id=24,
                notify_chat_id=24,
                notify_message_thread_id=None,
            )
            token = created["auth_url"].rsplit("/", 1)[-1]
            await manager.ensure_session_started(token)
            session = manager.get_session_by_token(token)
            session.status = "completed"
            session.completed_at = datetime.now(timezone.utc).isoformat()
            manager._store.update_session(session)  # noqa: SLF001

            reaped = await manager.run_janitor_pass()

            self.assertEqual(reaped, 2)
            launcher.cleanup.assert_awaited_once()
            launcher.remove_container.assert_awaited_once_with("orphan-container")
            stored = manager.get_session_by_token(token)
            self.assertIsNone(stored.browser)

    async def test_janitor_persists_cookie_keepalive_once_per_interval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "auth_sessions.json"
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text('{"cookies": [], "origins": []}', encoding="utf-8")
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_vps_lightweight_mode=True,
                notebooklm_janitor_enabled=True,
                notebooklm_janitor_interval_seconds=60,
                notebooklm_cookie_keepalive_interval_seconds=420,
                notebooklm_timeout=30.0,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
                notebooklm_remote_auth_state_path=str(state_path),
                notebooklm_remote_auth_ttl_seconds=900,
                notebooklm_remote_auth_poll_seconds=1.0,
                notebooklm_remote_auth_base_url="http://example.test:8010",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            launcher = _FakeLauncher()
            runtime_store = SimpleNamespace(
                resolve_storage_state_path=Mock(return_value=str(storage_path)),
                replace_storage_state=Mock(return_value={"auth_ready": True}),
            )
            manager = NotebookLMRemoteAuthManager(
                settings=settings,
                runtime_store=runtime_store,
                launcher=launcher,
            )

            with (
                patch(
                    "app.services.notebooklm_remote_auth.refresh_notebooklm_google_keepalive",
                    new=AsyncMock(return_value={"cookies": [{"name": "SID"}], "origins": []}),
                ) as keepalive_refresh,
                patch("app.services.notebooklm_remote_auth.time.monotonic", side_effect=[1000.0, 1060.0]),
            ):
                first_reaped = await manager.run_janitor_pass()
                second_reaped = await manager.run_janitor_pass()

            self.assertEqual(first_reaped, 0)
            self.assertEqual(second_reaped, 0)
            keepalive_refresh.assert_awaited_once_with(
                str(storage_path),
                30.0,
                "socks5://127.0.0.1:43129",
            )
            runtime_store.replace_storage_state.assert_called_once()
            stored_payload = json.loads(runtime_store.replace_storage_state.call_args.args[0])
            self.assertEqual(stored_payload["cookies"][0]["name"], "SID")

    async def test_janitor_keepalive_failure_does_not_write_or_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "auth_sessions.json"
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text('{"cookies": [], "origins": []}', encoding="utf-8")
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_vps_lightweight_mode=True,
                notebooklm_janitor_enabled=True,
                notebooklm_janitor_interval_seconds=60,
                notebooklm_cookie_keepalive_interval_seconds=420,
                notebooklm_timeout=30.0,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
                notebooklm_remote_auth_state_path=str(state_path),
                notebooklm_remote_auth_ttl_seconds=900,
                notebooklm_remote_auth_poll_seconds=1.0,
                notebooklm_remote_auth_base_url="http://example.test:8010",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            launcher = _FakeLauncher()
            runtime_store = SimpleNamespace(
                resolve_storage_state_path=Mock(return_value=str(storage_path)),
                replace_storage_state=Mock(return_value={"auth_ready": True}),
            )
            manager = NotebookLMRemoteAuthManager(
                settings=settings,
                runtime_store=runtime_store,
                launcher=launcher,
            )

            with (
                patch(
                    "app.services.notebooklm_remote_auth.refresh_notebooklm_google_keepalive",
                    new=AsyncMock(side_effect=ValueError("expired")),
                ) as keepalive_refresh,
                patch("app.services.notebooklm_remote_auth.time.monotonic", return_value=1000.0),
            ):
                reaped = await manager.run_janitor_pass()

            self.assertEqual(reaped, 0)
            keepalive_refresh.assert_awaited_once()
            runtime_store.replace_storage_state.assert_not_called()


class DockerRemoteBrowserLauncherTests(unittest.IsolatedAsyncioTestCase):
    async def test_launch_uses_host_network_and_browser_proxy_args(self) -> None:
        settings = SimpleNamespace(
            notebooklm_remote_auth_docker_socket="/var/run/docker.sock",
            notebooklm_remote_auth_selenium_image="selenium/standalone-chromium:latest",
            notebooklm_remote_auth_novnc_port=47900,
            notebooklm_remote_auth_memory_limit_mb=1536,
            notebooklm_remote_auth_memory_swap_limit_mb=1536,
            notebooklm_proxy_enabled=True,
            notebooklm_proxy_url="socks5://127.0.0.1:43129",
        )
        launcher = DockerRemoteBrowserLauncher(settings=settings)
        captured_create_payload = {}
        started_container_ids: list[str] = []

        class _FakeDockerClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb) -> bool:
                return False

            async def post(self, url: str, **kwargs):
                if url == "/containers/create":
                    captured_create_payload.update(kwargs["json"])
                    return SimpleNamespace(status_code=201, json=lambda: {"Id": "container-123"}, text="")
                if url == "/containers/container-123/start":
                    started_container_ids.append("container-123")
                    return SimpleNamespace(status_code=204, text="")
                raise AssertionError(f"Unexpected Docker API call: {url}")

        with (
            patch.object(launcher, "_docker_client", new=AsyncMock(return_value=_FakeDockerClient())),
            patch.object(launcher, "_wait_for_webdriver", new=AsyncMock()),
            patch.object(launcher, "_create_webdriver_session", new=AsyncMock(return_value="webdriver-123")),
            patch.object(launcher, "navigate", new=AsyncMock()),
            patch("app.services.notebooklm_remote_auth._find_free_port", side_effect=[5901]),
        ):
            browser = await launcher.launch(public_base_url="http://example.test:8010", session_id="session-123")

        self.assertEqual(browser.webdriver_port, 4444)
        self.assertEqual(browser.novnc_port, 47900)
        self.assertEqual(started_container_ids, ["container-123"])
        self.assertEqual(captured_create_payload["HostConfig"]["NetworkMode"], "host")
        self.assertEqual(captured_create_payload["HostConfig"]["Memory"], 1536 * 1024 * 1024)
        self.assertEqual(captured_create_payload["HostConfig"]["MemorySwap"], 1536 * 1024 * 1024)
        self.assertNotIn("PortBindings", captured_create_payload["HostConfig"])
        self.assertIn("SE_VNC_PORT=5901", captured_create_payload["Env"])
        self.assertIn("SE_NO_VNC_PORT=47900", captured_create_payload["Env"])
        self.assertIn(
            "SE_BROWSER_ARGS_PROXY=--proxy-server=socks5://127.0.0.1:43129",
            captured_create_payload["Env"],
        )
