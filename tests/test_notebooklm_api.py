import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi.testclient import TestClient

from app.api import main
from app.services.telegram_stars import TelegramStarsAPIError


class NotebookLMApiRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        main.app.dependency_overrides[main._require_notebooklm_admin] = lambda: "admin"  # noqa: SLF001

    def tearDown(self) -> None:
        main.app.dependency_overrides.clear()

    def test_generic_auth_session_hub_keeps_windows_compatibility_and_import_ui(self) -> None:
        upload_manager = Mock()
        upload_manager.get_session_status.return_value = {
            "status": "pending",
            "expires_at": "2026-04-24T13:00:00+00:00",
            "protocol_url": "tgctxbot-notebooklm-sync://sync?upload_url=x",
        }

        with patch("app.api.main._upload_sync_manager", return_value=upload_manager):
            response = TestClient(main.app).get("/auth-session/token-redacted")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Android / Manual Import", response.text)
        self.assertIn("Windows Helper", response.text)
        upload_manager.get_session_status.assert_called_once()

    def test_admin_page_exposes_import_and_remote_browser_actions(self) -> None:
        response = TestClient(main.app).get("/admin/notebooklm")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Phone / Windows Cookie Import", response.text)
        self.assertIn("VPS Browser Login", response.text)
        self.assertIn("Access / Telegram Stars", response.text)
        self.assertIn("/api/admin/access/status", response.text)
        self.assertIn("/api/admin/access/stars", response.text)
        self.assertIn("accessOverrideChatInput", response.text)
        self.assertIn("clearAccessOverrideBtn", response.text)
        self.assertIn("telegramStarsStatsBox", response.text)
        self.assertIn("refreshTelegramStarsBtn", response.text)
        self.assertIn("createRemoteAuthSessionBtn", response.text)
        self.assertIn("Copy import link", response.text)

    def test_access_admin_api_status_config_and_grant(self) -> None:
        access_store = Mock()
        access_store.status.return_value = {
            "currency": "XTR",
            "global": {
                "enabled": True,
                "free_questions_per_24h": 3,
                "stars_price": 25,
                "credits_per_purchase": 10,
            },
            "chat_overrides": [{"chat_id": 200, "enabled": False}],
            "totals": {"usage_count": 0, "order_count": 0, "payment_count": 0},
        }
        access_store.set_global_config.return_value = None
        access_store.set_chat_override.return_value = None
        access_store.clear_chat_override.return_value = None
        access_store.balance.return_value = {
            "telegram_user_id": 100,
            "chat_id": 200,
            "manual_credits": 2,
        }
        access_store.grant_manual_credits.return_value = 5

        settings = SimpleNamespace(bot_token="777777:token", bot_instance_name="secondary")
        with (
            patch("app.api.main._access_store", return_value=access_store),
            patch("app.api.main.get_settings", return_value=settings),
        ):
            client = TestClient(main.app)
            status = client.get("/api/admin/access/status")
            config = client.post(
                "/api/admin/access/config",
                json={
                    "global_config": {
                        "enabled": True,
                        "free_questions_per_24h": 4,
                        "stars_price": 30,
                        "credits_per_purchase": 12,
                    },
                    "chat_overrides": [{"chat_id": 200, "enabled": False, "free_questions_per_24h": 2}],
                },
            )
            clear = client.post(
                "/api/admin/access/config",
                json={"chat_overrides": [{"chat_id": 200, "clear": True}]},
            )
            user = client.get("/api/admin/access/users/100?chat_id=200")
            grant = client.post(
                "/api/admin/access/users/100",
                json={"chat_id": 200, "credits_delta": 3, "reason": "test"},
            )

        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["currency"], "XTR")
        self.assertEqual(status.json()["bot"]["instance_name"], "secondary")
        self.assertEqual(status.json()["bot"]["bot_id_hint"], "777777")
        self.assertEqual(config.status_code, 200)
        access_store.set_global_config.assert_called_once_with(
            enabled=True,
            free_questions_per_24h=4,
            stars_price=30,
            credits_per_purchase=12,
        )
        access_store.set_chat_override.assert_called_once()
        self.assertEqual(
            access_store.set_chat_override.call_args.kwargs["enabled"],
            False,
        )
        self.assertEqual(clear.status_code, 200)
        access_store.clear_chat_override.assert_called_once_with(chat_id=200)
        self.assertEqual(user.json()["manual_credits"], 2)
        self.assertEqual(grant.json()["manual_credits"], 5)

    def test_access_admin_api_requires_notebooklm_admin(self) -> None:
        main.app.dependency_overrides.clear()
        client = TestClient(main.app)
        response = client.get("/api/admin/access/status")
        stars = client.get("/api/admin/access/stars")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(stars.status_code, 401)

    def test_access_admin_stars_api_success_summary_reconciliation_and_limit_clamp(self) -> None:
        access_store = Mock()
        access_store.stars_ledger_summary.return_value = {
            "state_path": ".state/bot/access.sqlite3",
            "currency": "XTR",
            "local_order_count": 2,
            "local_payment_count": 2,
            "usage_count": 3,
            "total_local_paid_stars_amount": 55,
            "paid_credits": {"granted": 20, "consumed": 4, "remaining": 16},
            "manual_credits": {"granted": 5, "consumed": 1, "remaining": 4},
        }
        access_store.star_payments.return_value = [
            {
                "id": 1,
                "telegram_payment_charge_id": "tg-charge-1",
                "stars_amount": 25,
                "credits": 10,
            },
            {
                "id": 2,
                "telegram_payment_charge_id": "local-only",
                "stars_amount": 30,
                "credits": 10,
            },
        ]
        stars_client = Mock()
        stars_client.fetch_bot_identity = AsyncMock(
            return_value={
                "id": 777777,
                "is_bot": True,
                "username": "secondary_bot",
                "username_label": "@secondary_bot",
            }
        )
        stars_client.fetch_balance = AsyncMock(
            return_value={"amount": 120, "nanostar_amount": 7, "currency": "XTR"}
        )
        stars_client.fetch_transactions = AsyncMock(
            return_value=[
                {"id": "tg-charge-1", "amount": 25, "currency": "XTR", "date": 1777291200},
                {"id": "live-only", "amount": 40, "currency": "XTR", "date": 1777291300},
            ]
        )
        stars_client.sanitize_error.side_effect = lambda value: str(value)

        with (
            patch("app.api.main._access_store", return_value=access_store),
            patch("app.api.main._telegram_stars_client", return_value=stars_client),
        ):
            response = TestClient(main.app).get("/api/admin/access/stars?offset=2&limit=500")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["pagination"], {"offset": 2, "limit": 100, "requested_limit": 500})
        self.assertEqual(body["live"]["bot"]["username_label"], "@secondary_bot")
        self.assertTrue(body["live"]["ok"])
        self.assertEqual(body["live"]["balance"]["amount"], 120)
        self.assertEqual(body["live"]["balance"]["nanostar_amount"], 7)
        self.assertEqual(body["live"]["transactions"]["count"], 2)
        self.assertEqual(body["local"]["summary"]["total_local_paid_stars_amount"], 55)
        self.assertEqual(body["reconciliation"]["matched_count"], 1)
        self.assertEqual(body["reconciliation"]["live_not_found_locally_count"], 1)
        self.assertEqual(body["reconciliation"]["local_not_in_fetched_page_count"], 1)
        self.assertEqual(
            body["reconciliation"]["local_not_in_fetched_page"][0]["condition"],
            "not in fetched page",
        )
        stars_client.fetch_transactions.assert_awaited_once_with(offset=2, limit=100)

    def test_access_admin_stars_api_telegram_failure_still_returns_local_ledger(self) -> None:
        access_store = Mock()
        access_store.stars_ledger_summary.return_value = {
            "state_path": ".state/bot/access.sqlite3",
            "currency": "XTR",
            "local_order_count": 0,
            "local_payment_count": 0,
            "usage_count": 0,
            "total_local_paid_stars_amount": 0,
            "paid_credits": {"granted": 0, "consumed": 0, "remaining": 0},
            "manual_credits": {"granted": 0, "consumed": 0, "remaining": 0},
        }
        access_store.star_payments.return_value = []
        stars_client = Mock()
        stars_client.fetch_bot_identity = AsyncMock(side_effect=TelegramStarsAPIError("Unauthorized"))
        stars_client.fetch_balance = AsyncMock(side_effect=TelegramStarsAPIError("Unauthorized"))
        stars_client.fetch_transactions = AsyncMock(
            side_effect=TelegramStarsAPIError("Telegram API request failed.")
        )
        stars_client.sanitize_error.side_effect = lambda value: str(value)

        with (
            patch("app.api.main._access_store", return_value=access_store),
            patch("app.api.main._telegram_stars_client", return_value=stars_client),
        ):
            response = TestClient(main.app).get("/api/admin/access/stars?limit=0")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["pagination"]["limit"], 1)
        self.assertFalse(body["live"]["ok"])
        self.assertFalse(body["live"]["balance"]["ok"])
        self.assertFalse(body["live"]["transactions"]["ok"])
        self.assertIn("Unauthorized", body["live"]["error"])
        self.assertEqual(body["local"]["summary"]["state_path"], ".state/bot/access.sqlite3")
        self.assertFalse(body["reconciliation"]["page_available"])

    def test_access_admin_stars_api_rejects_negative_offset(self) -> None:
        response = TestClient(main.app).get("/api/admin/access/stars?offset=-1")

        self.assertEqual(response.status_code, 422)

    def test_legacy_windows_session_routes_remain_available(self) -> None:
        upload_manager = Mock()
        upload_manager.get_session_status.return_value = {
            "status": "pending",
            "expires_at": "2026-04-24T13:00:00+00:00",
            "protocol_url": "tgctxbot-notebooklm-sync://sync?upload_url=x",
        }

        with patch("app.api.main._upload_sync_manager", return_value=upload_manager):
            response = TestClient(main.app).get("/admin/notebooklm/sync/token-redacted")

        self.assertEqual(response.status_code, 200)
        self.assertIn("NotebookLM Windows Sync", response.text)

    def test_remote_auth_admin_api_create_status_start_cancel(self) -> None:
        remote_manager = Mock()
        remote_manager.create_session.return_value = {
            "status": "pending",
            "auth_url": "http://testserver/auth-session/remote-auth/token-redacted",
            "expires_at": "2026-04-24T13:00:00+00:00",
            "requested_via": "admin-ui",
        }
        remote_manager.get_session_status.return_value = {
            "status": "launched",
            "auth_url": "http://testserver/auth-session/remote-auth/token-redacted",
            "browser_url": "http://testserver:47900/",
            "expires_at": "2026-04-24T13:00:00+00:00",
        }
        remote_manager.ensure_session_started = AsyncMock(
            return_value={
                "status": "launched",
                "auth_url": "http://testserver/auth-session/remote-auth/token-redacted",
                "browser_url": "http://testserver:47900/",
                "expires_at": "2026-04-24T13:00:00+00:00",
            }
        )
        remote_manager.cancel_session = AsyncMock(
            return_value={
                "status": "cancelled",
                "auth_url": "http://testserver/auth-session/remote-auth/token-redacted",
                "expires_at": "2026-04-24T13:00:00+00:00",
            }
        )

        with patch("app.api.main._remote_auth_manager", return_value=remote_manager):
            client = TestClient(main.app)
            created = client.post(
                "/api/admin/notebooklm/remote-auth-sessions",
                json={"notify_in_telegram": False},
            )
            status = client.get("/api/admin/notebooklm/remote-auth-sessions/token-redacted")
            started = client.post("/api/admin/notebooklm/remote-auth-sessions/token-redacted/start")
            cancelled = client.post("/api/admin/notebooklm/remote-auth-sessions/token-redacted/cancel")
            admin_page = client.get("/admin/notebooklm/remote-auth/token-redacted")
            public_page = client.get("/auth-session/remote-auth/token-redacted")
            public_status = client.get("/api/public/notebooklm/remote-auth-sessions/token-redacted")
            public_started = client.post("/api/public/notebooklm/remote-auth-sessions/token-redacted/start")
            public_cancelled = client.post("/api/public/notebooklm/remote-auth-sessions/token-redacted/cancel")

        self.assertEqual(created.status_code, 200)
        self.assertIn("/auth-session/remote-auth/", created.json()["auth_url"])
        self.assertEqual(status.json()["status"], "launched")
        self.assertEqual(started.json()["browser_url"], "http://testserver:47900/")
        self.assertEqual(cancelled.json()["status"], "cancelled")
        self.assertEqual(admin_page.status_code, 200)
        self.assertIn("NotebookLM VPS Browser Login", admin_page.text)
        self.assertEqual(public_page.status_code, 200)
        self.assertIn("/api/public/notebooklm/remote-auth-sessions/", public_page.text)
        self.assertEqual(public_status.json()["status"], "launched")
        self.assertEqual(public_started.json()["browser_url"], "http://testserver:47900/")
        self.assertEqual(public_cancelled.json()["status"], "cancelled")
        self.assertEqual(remote_manager.ensure_session_started.await_count, 4)
        self.assertEqual(remote_manager.cancel_session.await_count, 2)
