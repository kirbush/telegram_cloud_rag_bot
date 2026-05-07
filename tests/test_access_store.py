import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.services.access_store import BotAccessStore, STARS_CURRENCY
from app.services.telegram_stars import reconcile_star_transactions


class BotAccessStoreTests(unittest.TestCase):
    def _store(self, path: Path) -> BotAccessStore:
        return BotAccessStore(settings=SimpleNamespace(bot_access_state_path=str(path)))

    def test_connect_prepares_access_sqlite_path_for_shared_api_bot_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "nested" / "access.sqlite3"
            store = self._store(db_path)

            with patch("app.services.access_store._cooperative_sqlite_mode") as cooperative_mode:
                status = store.status()

            self.assertEqual(status["state_path"], str(db_path))
            self.assertGreaterEqual(cooperative_mode.call_count, 2)
            self.assertTrue(all(call.args[0] == db_path for call in cooperative_mode.call_args_list))

    def test_global_defaults_chat_override_and_manual_credits_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(Path(tmp) / "access.sqlite3")
            self.assertFalse(store.get_global_config().enabled)
            store.set_global_config(enabled=True, free_questions_per_24h=1, stars_price=30, credits_per_purchase=5)
            store.set_chat_override(chat_id=200, enabled=False, free_questions_per_24h=2)
            store.grant_manual_credits(telegram_user_id=100, chat_id=200, delta=3, reason="test")

            balance = store.balance(telegram_user_id=100, chat_id=200)

            self.assertFalse(balance["enabled"])
            self.assertEqual(balance["free_limit"], 2)
            self.assertEqual(balance["free_remaining"], 2)
            self.assertEqual(balance["manual_credits"], 3)
            self.assertFalse(store.get_effective_config(chat_id=200).enabled)
            self.assertTrue(store.get_effective_config(chat_id=201).enabled)
            self.assertEqual(store.get_effective_config(chat_id=201).free_questions_per_24h, 1)

    def test_disabled_access_allows_without_recording_usage_or_consuming_credits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(Path(tmp) / "access.sqlite3")
            store.set_global_config(enabled=False, free_questions_per_24h=0)
            store.grant_manual_credits(telegram_user_id=100, chat_id=200, delta=1)

            consumed = store.consume_question(telegram_user_id=100, chat_id=200)
            balance = store.balance(telegram_user_id=100, chat_id=200)

            self.assertEqual(consumed.source, "disabled")
            self.assertFalse(balance["enabled"])
            self.assertEqual(balance["used_in_window"], 0)
            self.assertEqual(balance["manual_credits"], 1)

    def test_rolling_24h_window_expires_old_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(Path(tmp) / "access.sqlite3")
            store.set_global_config(enabled=True, free_questions_per_24h=1)
            base = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)

            first = store.consume_question(telegram_user_id=100, chat_id=200, at=base)
            denied = store.consume_question(
                telegram_user_id=100,
                chat_id=200,
                at=base + timedelta(hours=23, minutes=59),
            )
            allowed = store.consume_question(
                telegram_user_id=100,
                chat_id=200,
                at=base + timedelta(hours=24, seconds=1),
            )

            self.assertIsNotNone(first)
            self.assertIsNone(denied)
            self.assertIsNotNone(allowed)

    def test_credit_usage_does_not_extend_free_window_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(Path(tmp) / "access.sqlite3")
            store.set_global_config(enabled=True, free_questions_per_24h=1)
            store.grant_manual_credits(telegram_user_id=100, chat_id=200, delta=2)
            at = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)

            self.assertEqual(
                store.consume_question(telegram_user_id=100, chat_id=200, at=at).source,
                "free",
            )
            self.assertEqual(
                store.consume_question(
                    telegram_user_id=100,
                    chat_id=200,
                    at=at + timedelta(minutes=1),
                ).source,
                "manual",
            )
            balance = store.balance(
                telegram_user_id=100,
                chat_id=200,
                at=at + timedelta(minutes=2),
            )

            self.assertEqual(balance["used_in_window"], 1)

    def test_successful_payment_grants_paid_credits_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(Path(tmp) / "access.sqlite3")
            order = store.create_stars_order(telegram_user_id=100, chat_id=200)

            self.assertTrue(
                store.validate_stars_order(
                    payload=order.payload,
                    currency=STARS_CURRENCY,
                    total_amount=order.stars_amount,
                    telegram_user_id=100,
                )
            )
            granted = store.record_successful_payment(
                payload=order.payload,
                currency=STARS_CURRENCY,
                total_amount=order.stars_amount,
                telegram_user_id=100,
                telegram_payment_charge_id="tg-charge-1",
                provider_payment_charge_id="provider-charge-1",
                raw={"ok": True},
            )
            duplicate = store.record_successful_payment(
                payload=order.payload,
                currency=STARS_CURRENCY,
                total_amount=order.stars_amount,
                telegram_user_id=100,
                telegram_payment_charge_id="tg-charge-1",
                provider_payment_charge_id="provider-charge-1",
                raw={"ok": True},
            )
            duplicate_order = store.record_successful_payment(
                payload=order.payload,
                currency=STARS_CURRENCY,
                total_amount=order.stars_amount,
                telegram_user_id=100,
                telegram_payment_charge_id="tg-charge-2",
                provider_payment_charge_id="provider-charge-2",
                raw={"ok": True},
            )

            self.assertEqual(granted, (True, order.credits))
            self.assertEqual(duplicate, (False, order.credits))
            self.assertEqual(duplicate_order, (False, order.credits))
            self.assertEqual(
                store.balance(telegram_user_id=100, chat_id=200)["paid_credits"],
                order.credits,
            )

    def test_stars_ledger_summary_and_reconciliation_are_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(Path(tmp) / "access.sqlite3")
            store.set_global_config(enabled=True, free_questions_per_24h=0)
            order = store.create_stars_order(telegram_user_id=100, chat_id=200)
            store.record_successful_payment(
                payload=order.payload,
                currency=STARS_CURRENCY,
                total_amount=order.stars_amount,
                telegram_user_id=100,
                telegram_payment_charge_id="tg-charge-1",
                provider_payment_charge_id="provider-charge-1",
                raw={"ok": True},
            )
            self.assertEqual(
                store.consume_question(telegram_user_id=100, chat_id=200).source,
                "paid",
            )
            store.grant_manual_credits(telegram_user_id=100, chat_id=200, delta=3, reason="test")
            self.assertEqual(
                store.consume_question(telegram_user_id=100, chat_id=200).source,
                "manual",
            )

            before = store.status()["totals"]
            summary = store.stars_ledger_summary()
            payments = store.star_payments()
            reconciliation = reconcile_star_transactions(
                [
                    {"id": "tg-charge-1", "amount": order.stars_amount},
                    {"id": "live-only", "amount": 40},
                ],
                payments,
                page_available=True,
            )
            after = store.status()["totals"]

            self.assertEqual(before, after)
            self.assertEqual(summary["local_order_count"], 1)
            self.assertEqual(summary["local_payment_count"], 1)
            self.assertEqual(summary["usage_count"], 2)
            self.assertEqual(summary["total_local_paid_stars_amount"], order.stars_amount)
            self.assertEqual(summary["paid_credits"]["granted"], order.credits)
            self.assertEqual(summary["paid_credits"]["consumed"], 1)
            self.assertEqual(summary["paid_credits"]["remaining"], order.credits - 1)
            self.assertEqual(summary["manual_credits"], {"granted": 3, "consumed": 1, "remaining": 2})
            self.assertEqual(payments[0]["telegram_payment_charge_id"], "tg-charge-1")
            self.assertEqual(reconciliation["matched_count"], 1)
            self.assertEqual(reconciliation["live_not_found_locally_count"], 1)


if __name__ == "__main__":
    unittest.main()
