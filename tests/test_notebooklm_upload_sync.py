import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from app.services.notebooklm_upload_sync import (
    NotebookLMUploadSyncManager,
    NotebookLMUploadSyncStore,
)


class NotebookLMUploadSyncStoreTests(unittest.TestCase):
    def test_store_persists_only_token_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "upload_sync_state.json"
            settings = SimpleNamespace(notebooklm_upload_session_state_path=str(state_path))
            store = NotebookLMUploadSyncStore(settings=settings)

            store.create_session(
                token_hash="hashed-session-token",
                source="admin-ui",
                requested_by_user_id=42,
                requested_by_chat_id=24,
                notify_chat_id=None,
                notify_message_thread_id=None,
                ttl_seconds=900,
            )
            store.create_device(
                token_hash="hashed-refresh-token",
                browser_preference="chrome",
                profile_preference="Default",
                ttl_seconds=86400,
            )

            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["sessions"][0]["token_hash"], "hashed-session-token")
            self.assertEqual(payload["devices"][0]["token_hash"], "hashed-refresh-token")
            self.assertNotIn("plain-session-token", state_path.read_text(encoding="utf-8"))
            self.assertNotIn("plain-refresh-token", state_path.read_text(encoding="utf-8"))


class NotebookLMUploadSyncManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_session_returns_windows_launch_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "upload_sync_state.json"
            settings = SimpleNamespace(
                notebooklm_upload_session_state_path=str(state_path),
                notebooklm_upload_session_ttl_seconds=900,
                notebooklm_upload_refresh_ttl_seconds=86400,
                notebooklm_windows_helper_protocol_scheme="tgctxbot-notebooklm-sync",
                notebooklm_remote_auth_base_url="http://example.test:8010",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            manager = NotebookLMUploadSyncManager(
                settings=settings,
                runtime_store=SimpleNamespace(replace_storage_state=Mock(return_value={"auth_ready": True})),
            )

            session = manager.create_session(
                source="admin-ui",
                requested_by_user_id=42,
                requested_by_chat_id=None,
                notify_chat_id=None,
                notify_message_thread_id=None,
            )

            self.assertEqual(session["status"], "pending")
            self.assertIn("/auth-session/", session["entry_url"])
            self.assertIn("/api/public/notebooklm/upload-sessions/", session["upload_url"])
            self.assertTrue(session["protocol_url"].startswith("tgctxbot-notebooklm-sync://sync?"))
            self.assertIsNone(session["refresh_url"])

    @patch("app.services.notebooklm_upload_sync.load_notebooklm_auth", new_callable=AsyncMock)
    @patch("app.services.notebooklm_service.NotebookLMService.invalidate_cached_client", new_callable=AsyncMock)
    async def test_complete_upload_updates_runtime_and_issues_refresh_url(
        self,
        invalidate_cached_client,
        load_notebooklm_auth,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "upload_sync_state.json"
            settings = SimpleNamespace(
                notebooklm_upload_session_state_path=str(state_path),
                notebooklm_upload_session_ttl_seconds=900,
                notebooklm_upload_refresh_ttl_seconds=86400,
                notebooklm_windows_helper_protocol_scheme="tgctxbot-notebooklm-sync",
                notebooklm_remote_auth_base_url="http://example.test:8010",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            runtime_store = SimpleNamespace(
                replace_storage_state=Mock(return_value={"auth_ready": True, "storage_state_exists": True}),
                get_runtime_status=Mock(return_value={"auth_ready": True, "storage_state_exists": True}),
            )
            manager = NotebookLMUploadSyncManager(settings=settings, runtime_store=runtime_store)
            manager._notify = AsyncMock()  # type: ignore[method-assign]
            load_notebooklm_auth.return_value = {"SID": "x"}

            created = manager.create_session(
                source="telegram-bot",
                requested_by_user_id=42,
                requested_by_chat_id=24,
                notify_chat_id=24,
                notify_message_thread_id=None,
            )
            token = created["entry_url"].rsplit("/", 1)[-1]

            result = await manager.complete_upload(
                token,
                '{"cookies": [{"name": "SID", "value": "x", "domain": ".google.com", "path": "/"}], "origins": []}',
                helper_metadata={"browser": "chrome", "profile": "Default", "cookie_count": 7},
            )

            self.assertTrue(result["auth_ready"])
            self.assertIn("/api/public/notebooklm/upload-refresh/", result["refresh_url"])
            runtime_store.replace_storage_state.assert_called_once()
            invalidate_cached_client.assert_awaited_once()
            load_notebooklm_auth.assert_awaited_once()
            manager._notify.assert_awaited_once()  # type: ignore[attr-defined]

            stored = manager.get_session_status(token)
            self.assertEqual(stored["status"], "completed")
            self.assertEqual(stored["device"]["upload_count"], 1)

    @patch("app.services.notebooklm_upload_sync.load_notebooklm_auth", new_callable=AsyncMock)
    @patch("app.services.notebooklm_service.NotebookLMService.invalidate_cached_client", new_callable=AsyncMock)
    async def test_refresh_from_device_reuses_saved_refresh_token(
        self,
        invalidate_cached_client,
        load_notebooklm_auth,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "upload_sync_state.json"
            settings = SimpleNamespace(
                notebooklm_upload_session_state_path=str(state_path),
                notebooklm_upload_session_ttl_seconds=900,
                notebooklm_upload_refresh_ttl_seconds=86400,
                notebooklm_windows_helper_protocol_scheme="tgctxbot-notebooklm-sync",
                notebooklm_remote_auth_base_url="http://example.test:8010",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            runtime_store = SimpleNamespace(
                replace_storage_state=Mock(return_value={"auth_ready": True, "storage_state_exists": True}),
                get_runtime_status=Mock(return_value={"auth_ready": True, "storage_state_exists": True}),
            )
            manager = NotebookLMUploadSyncManager(settings=settings, runtime_store=runtime_store)
            manager._notify = AsyncMock()  # type: ignore[method-assign]
            load_notebooklm_auth.return_value = {"SID": "x"}

            created = manager.create_session(
                source="telegram-bot",
                requested_by_user_id=42,
                requested_by_chat_id=24,
                notify_chat_id=24,
                notify_message_thread_id=None,
            )
            token = created["entry_url"].rsplit("/", 1)[-1]
            first_result = await manager.complete_upload(
                token,
                '{"cookies": [{"name": "SID", "value": "x", "domain": ".google.com", "path": "/"}], "origins": []}',
                helper_metadata={"browser": "edge", "profile": "Profile 1", "cookie_count": 4},
            )
            refresh_token = first_result["refresh_url"].rsplit("/", 1)[-1]

            refreshed = await manager.refresh_from_device(
                refresh_token,
                '{"cookies": [{"name": "SID", "value": "y", "domain": ".google.com", "path": "/"}], "origins": []}',
                helper_metadata={"browser": "edge", "profile": "Profile 1", "cookie_count": 5},
            )

            self.assertTrue(refreshed["auth_ready"])
            self.assertEqual(runtime_store.replace_storage_state.call_count, 2)
            invalidate_cached_client.assert_awaited()

    @patch("app.services.notebooklm_upload_sync.load_notebooklm_auth", new_callable=AsyncMock)
    @patch("app.services.notebooklm_service.NotebookLMService.invalidate_cached_client", new_callable=AsyncMock)
    async def test_complete_upload_marks_session_failed_when_uploaded_auth_is_invalid(
        self,
        invalidate_cached_client,
        load_notebooklm_auth,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "upload_sync_state.json"
            settings = SimpleNamespace(
                notebooklm_upload_session_state_path=str(state_path),
                notebooklm_upload_session_ttl_seconds=900,
                notebooklm_upload_refresh_ttl_seconds=86400,
                notebooklm_windows_helper_protocol_scheme="tgctxbot-notebooklm-sync",
                notebooklm_remote_auth_base_url="http://example.test:8010",
                notebooklm_timeout=30.0,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
                telegram_proxy_enabled=False,
                telegram_proxy_url=None,
                bot_token="123:token",
            )
            runtime_store = SimpleNamespace(
                replace_storage_state=Mock(
                    return_value={
                        "auth_ready": True,
                        "storage_state_exists": True,
                        "storage_state_path": str(Path(tmp) / "storage_state.json"),
                    }
                ),
                get_runtime_status=Mock(return_value={"auth_ready": True, "storage_state_exists": True}),
            )
            manager = NotebookLMUploadSyncManager(settings=settings, runtime_store=runtime_store)
            manager._notify = AsyncMock()  # type: ignore[method-assign]
            load_notebooklm_auth.side_effect = ValueError("Authentication expired or invalid.")

            created = manager.create_session(
                source="admin-ui",
                requested_by_user_id=42,
                requested_by_chat_id=None,
                notify_chat_id=None,
                notify_message_thread_id=None,
            )
            token = created["entry_url"].rsplit("/", 1)[-1]

            result = await manager.complete_upload(
                token,
                '{"cookies": [{"name": "SID", "value": "x", "domain": ".google.com", "path": "/"}], "origins": []}',
                helper_metadata={"browser": "chrome", "profile": "Default"},
            )

            self.assertFalse(result["auth_ready"])
            self.assertEqual(result["auth_check"], "expired")
            self.assertIn("expired", result["auth_error"].lower())
            self.assertIsNone(result["refresh_url"])
            runtime_store.replace_storage_state.assert_not_called()
            invalidate_cached_client.assert_not_awaited()
            load_notebooklm_auth.assert_awaited_once()
            manager._notify.assert_not_awaited()  # type: ignore[attr-defined]

            stored = manager.get_session_status(token)
            self.assertEqual(stored["status"], "failed")
            self.assertIn("expired", (stored["error"] or "").lower())
            self.assertIsNone(stored["device"])
