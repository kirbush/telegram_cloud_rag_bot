import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.services.notebooklm_health import NotebookLMHealthService


class NotebookLMHealthServiceTests(unittest.IsolatedAsyncioTestCase):
    @patch("app.services.notebooklm_health.load_notebooklm_auth", new_callable=AsyncMock)
    @patch("app.services.notebooklm_health.NotebookLMRuntimeStore.get_runtime_status")
    @patch("app.services.notebooklm_health.NotebookLMHealthService._probe_notebooklm_tunnel", new_callable=AsyncMock)
    @patch("app.services.notebooklm_health.NotebookLMHealthService._probe_telegram_tunnel", new_callable=AsyncMock)
    @patch("app.services.notebooklm_health.get_settings")
    async def test_readiness_does_not_require_sync_state_when_source_sync_disabled(
        self,
        mock_get_settings,
        probe_telegram,
        probe_notebooklm,
        get_runtime_status,
        load_notebooklm_auth,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_state = Path(tmp) / "storage_state.json"
            storage_state.write_text("{}", encoding="utf-8")
            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_source_sync_enabled=False,
                notebooklm_ready_storage_max_age_days=14,
                notebooklm_ready_sync_max_age_hours=36,
                notebooklm_health_cache_seconds=1,
                notebooklm_timeout=30.0,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
                settings_loaded_at="2026-04-23T10:00:00Z",
                bot_token="test-token",
                telegram_proxy_url="http://127.0.0.1:43128",
            )
            probe_telegram.return_value = True
            probe_notebooklm.return_value = True
            get_runtime_status.return_value = {
                "enabled": True,
                "storage_state_path": str(storage_state),
            }
            load_notebooklm_auth.return_value = object()

            snapshot = await NotebookLMHealthService().readiness(force=True)

            self.assertTrue(snapshot.ready)
            self.assertIsNone(snapshot.reason)
            self.assertIsNone(snapshot.sync_state_age_seconds)
