import json
import unittest

import httpx

from app.services.telegram_stars import (
    TelegramStarsAPIError,
    TelegramStarsClient,
    reconcile_star_transactions,
    sanitize_telegram_text,
)


class TelegramStarsClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_balance_and_transactions_parses_bot_api_responses(self) -> None:
        requests: list[tuple[str, dict]] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8") or "{}")
            requests.append((request.url.path, body))
            if request.url.path.endswith("/getMyStarBalance"):
                return httpx.Response(
                    200,
                    json={"ok": True, "result": {"amount": 12, "nanostar_amount": 345}},
                )
            if request.url.path.endswith("/getMe"):
                return httpx.Response(
                    200,
                    json={
                        "ok": True,
                        "result": {
                            "id": 123456,
                            "is_bot": True,
                            "username": "secondary_bot",
                            "first_name": "Secondary",
                        },
                    },
                )
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": {
                        "transactions": [
                            {
                                "id": "tg-charge-1",
                                "amount": 25,
                                "nanostar_amount": 7,
                                "date": 1777291200,
                                "source": {
                                    "type": "user",
                                    "user": {"id": 100, "username": "alice"},
                                },
                                "receiver": {"type": "bot"},
                            }
                        ]
                    },
                },
            )

        client = TelegramStarsClient(
            bot_token="123456:ABCDEF1234567890abcdef",
            base_url="https://api.telegram.test",
            transport=httpx.MockTransport(handler),
        )

        identity = await client.fetch_bot_identity()
        balance = await client.fetch_balance()
        transactions = await client.fetch_transactions(offset=2, limit=10)

        self.assertEqual(identity["id"], 123456)
        self.assertEqual(identity["username_label"], "@secondary_bot")
        self.assertEqual(balance["amount"], 12)
        self.assertEqual(balance["nanostar_amount"], 345)
        self.assertEqual(transactions[0]["id"], "tg-charge-1")
        self.assertEqual(transactions[0]["amount"], 25)
        self.assertEqual(transactions[0]["nanostar_amount"], 7)
        self.assertEqual(transactions[0]["date_iso"], "2026-04-27T12:00:00Z")
        self.assertEqual(transactions[0]["source"]["type"], "user")
        self.assertEqual(requests[2][1], {"offset": 2, "limit": 10})

    async def test_fetch_failure_raises_sanitized_error(self) -> None:
        async def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                401,
                json={
                    "ok": False,
                    "description": (
                        "Unauthorized: https://api.telegram.org/"
                        "bot123456:ABCDEF1234567890abcdef/getMyStarBalance"
                    ),
                },
            )

        client = TelegramStarsClient(
            bot_token="123456:ABCDEF1234567890abcdef",
            transport=httpx.MockTransport(handler),
        )

        with self.assertRaises(TelegramStarsAPIError) as raised:
            await client.fetch_balance()

        message = str(raised.exception)
        self.assertIn("Telegram API HTTP 401", message)
        self.assertNotIn("123456:ABCDEF1234567890abcdef", message)
        self.assertIn("bot[redacted]", message)

    def test_proxy_url_is_preserved_for_live_vps_calls(self) -> None:
        client = TelegramStarsClient(
            bot_token="123456:ABCDEF1234567890abcdef",
            proxy_url="http://127.0.0.1:43128",
        )

        self.assertEqual(client.proxy_url, "http://127.0.0.1:43128")


class TelegramStarsReconciliationTests(unittest.TestCase):
    def test_reconciles_live_and_local_transaction_ids(self) -> None:
        result = reconcile_star_transactions(
            [
                {"id": "tg-charge-1", "amount": 25},
                {"id": "live-only", "amount": 30},
            ],
            [
                {"id": 1, "telegram_payment_charge_id": "tg-charge-1"},
                {"id": 2, "telegram_payment_charge_id": "local-only"},
                {"id": 3, "telegram_payment_charge_id": ""},
            ],
            page_available=True,
        )

        self.assertEqual(result["matched_count"], 1)
        self.assertEqual(result["live_not_found_locally_count"], 1)
        self.assertEqual(result["local_not_in_fetched_page_count"], 1)
        self.assertEqual(result["local_without_telegram_charge_id_count"], 1)
        self.assertEqual(result["local_not_in_fetched_page"][0]["condition"], "not in fetched page")

    def test_sanitize_telegram_text_redacts_bot_api_tokens(self) -> None:
        sanitized = sanitize_telegram_text(
            "failed https://api.telegram.org/bot123456:ABCDEF1234567890abcdef/getStarTransactions",
            bot_token="123456:ABCDEF1234567890abcdef",
        )

        self.assertNotIn("123456:ABCDEF1234567890abcdef", sanitized)
        self.assertIn("bot[redacted]", sanitized)


if __name__ == "__main__":
    unittest.main()
