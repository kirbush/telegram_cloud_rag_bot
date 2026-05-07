"""Telegram Bot API helpers for read-only Stars admin stats."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import httpx

from app.services.access_store import STARS_CURRENCY

_BOT_API_TOKEN_RE = re.compile(r"(https?://api\.telegram\.org)/bot[^/\s]+", re.IGNORECASE)
_BOT_TOKEN_RE = re.compile(r"\b[0-9]{4,}:[A-Za-z0-9_-]{16,}\b")
_SECRET_KEY_PARTS = ("token", "password", "secret", "authorization", "api_key")


class TelegramStarsAPIError(RuntimeError):
    """Sanitized Telegram Stars API failure safe to show to admins."""


def _utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def sanitize_telegram_text(value: object, *, bot_token: str | None = None) -> str:
    text = str(value or "")
    if bot_token:
        token = str(bot_token)
        text = text.replace(f"bot{token}", "bot[redacted]")
        text = text.replace(token, "[redacted-token]")
    text = _BOT_API_TOKEN_RE.sub(r"\1/bot[redacted]", text)
    text = _BOT_TOKEN_RE.sub("[redacted-token]", text)
    return text


def sanitize_telegram_payload(value: Any, *, bot_token: str | None = None) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if any(part in key_text.lower() for part in _SECRET_KEY_PARTS):
                sanitized[key_text] = "[redacted]"
            else:
                sanitized[key_text] = sanitize_telegram_payload(item, bot_token=bot_token)
        return sanitized
    if isinstance(value, list):
        return [sanitize_telegram_payload(item, bot_token=bot_token) for item in value]
    if isinstance(value, tuple):
        return [sanitize_telegram_payload(item, bot_token=bot_token) for item in value]
    if isinstance(value, str):
        return sanitize_telegram_text(value, bot_token=bot_token)
    return value


def _as_int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def parse_star_amount(value: Any) -> dict[str, Any]:
    nanostar_amount: int | None = None
    if isinstance(value, dict):
        amount = _as_int(value.get("amount"), 0)
        if value.get("nanostar_amount") is not None:
            nanostar_amount = _as_int(value.get("nanostar_amount"), 0)
    else:
        amount = _as_int(value, 0)

    result: dict[str, Any] = {
        "amount": amount,
        "currency": STARS_CURRENCY,
    }
    if nanostar_amount is not None:
        result["nanostar_amount"] = nanostar_amount
    return result


def _date_iso(value: object) -> str | None:
    try:
        timestamp = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _partner_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    summary: dict[str, Any] = {"type": value.get("type")}
    user = value.get("user")
    if isinstance(user, dict):
        summary["user"] = {
            "id": user.get("id"),
            "username": user.get("username"),
            "first_name": user.get("first_name"),
            "last_name": user.get("last_name"),
        }
    invoice_payload = value.get("invoice_payload")
    if invoice_payload is not None:
        summary["invoice_payload"] = invoice_payload
    return sanitize_telegram_payload(summary)


def parse_bot_identity(value: Any, *, bot_token: str | None = None) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    username = str(raw.get("username") or "").strip()
    return sanitize_telegram_payload(
        {
            "id": raw.get("id"),
            "is_bot": raw.get("is_bot"),
            "username": username,
            "username_label": f"@{username}" if username else "",
            "first_name": raw.get("first_name"),
        },
        bot_token=bot_token,
    )


def parse_star_transaction(value: Any, *, bot_token: str | None = None) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    amount = parse_star_amount(raw.get("amount"))
    if "nanostar_amount" in raw and raw.get("nanostar_amount") is not None:
        amount["nanostar_amount"] = _as_int(raw.get("nanostar_amount"), 0)
    source = raw.get("source")
    receiver = raw.get("receiver")
    return {
        "id": str(raw.get("id") or ""),
        "amount": amount["amount"],
        **({"nanostar_amount": amount["nanostar_amount"]} if "nanostar_amount" in amount else {}),
        "currency": STARS_CURRENCY,
        "date": raw.get("date"),
        "date_iso": _date_iso(raw.get("date")),
        "source": _partner_summary(source),
        "receiver": _partner_summary(receiver),
        "raw_source": sanitize_telegram_payload(source, bot_token=bot_token),
        "raw_receiver": sanitize_telegram_payload(receiver, bot_token=bot_token),
    }


def reconcile_star_transactions(
    live_transactions: list[dict[str, Any]],
    local_payments: list[dict[str, Any]],
    *,
    page_available: bool,
) -> dict[str, Any]:
    local_by_charge_id: dict[str, dict[str, Any]] = {}
    local_without_charge_id: list[dict[str, Any]] = []
    for payment in local_payments:
        charge_id = str(payment.get("telegram_payment_charge_id") or "").strip()
        if charge_id:
            local_by_charge_id[charge_id] = payment
        else:
            local_without_charge_id.append(payment)

    live_by_id: dict[str, dict[str, Any]] = {}
    for transaction in live_transactions if page_available else []:
        transaction_id = str(transaction.get("id") or "").strip()
        if transaction_id:
            live_by_id[transaction_id] = transaction

    matched = [
        {
            "id": charge_id,
            "condition": "matched",
            "live_transaction": live_by_id[charge_id],
            "local_payment": payment,
        }
        for charge_id, payment in local_by_charge_id.items()
        if charge_id in live_by_id
    ]
    live_not_found_locally = [
        {
            "id": transaction_id,
            "condition": "not found locally",
            "live_transaction": transaction,
        }
        for transaction_id, transaction in live_by_id.items()
        if transaction_id not in local_by_charge_id
    ]
    local_not_in_fetched_page = [
        {
            "id": charge_id,
            "condition": "not in fetched page",
            "local_payment": payment,
        }
        for charge_id, payment in local_by_charge_id.items()
        if page_available and charge_id not in live_by_id
    ]

    return {
        "page_available": page_available,
        "matched_count": len(matched),
        "live_not_found_locally_count": len(live_not_found_locally),
        "local_not_in_fetched_page_count": len(local_not_in_fetched_page),
        "local_without_telegram_charge_id_count": len(local_without_charge_id),
        "matched": matched,
        "live_not_found_locally": live_not_found_locally,
        "local_not_in_fetched_page": local_not_in_fetched_page,
        "local_without_telegram_charge_id": local_without_charge_id,
    }


class TelegramStarsClient:
    def __init__(
        self,
        *,
        bot_token: str,
        base_url: str = "https://api.telegram.org",
        timeout: float = 10.0,
        proxy_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._bot_token = str(bot_token or "").strip()
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._proxy_url = str(proxy_url or "").strip() or None
        self._transport = transport

    @property
    def bot_token(self) -> str:
        return self._bot_token

    @property
    def proxy_url(self) -> str | None:
        return self._proxy_url

    def sanitize_error(self, value: object) -> str:
        return sanitize_telegram_text(value, bot_token=self._bot_token)

    async def _post(self, method: str, payload: dict[str, Any] | None = None) -> Any:
        if not self._bot_token:
            raise TelegramStarsAPIError("Telegram bot token is not configured.")
        try:
            client_kwargs: dict[str, Any] = {
                "base_url": self._base_url,
                "timeout": self._timeout,
            }
            if self._transport is not None:
                client_kwargs["transport"] = self._transport
            elif self._proxy_url:
                client_kwargs["proxy"] = self._proxy_url
            async with httpx.AsyncClient(
                **client_kwargs,
            ) as client:
                response = await client.post(f"/bot{self._bot_token}/{method}", json=payload or {})
        except httpx.RequestError as exc:
            raise TelegramStarsAPIError("Telegram API request failed.") from exc

        try:
            body = response.json()
        except ValueError:
            body = {}

        description = ""
        if isinstance(body, dict):
            description = str(body.get("description") or "")
        description = self.sanitize_error(description or response.reason_phrase or "unknown error")

        if response.status_code >= 400:
            raise TelegramStarsAPIError(f"Telegram API HTTP {response.status_code}: {description}")
        if not isinstance(body, dict) or not bool(body.get("ok")):
            raise TelegramStarsAPIError(f"Telegram API error: {description}")
        return body.get("result")

    async def fetch_balance(self) -> dict[str, Any]:
        return parse_star_amount(await self._post("getMyStarBalance"))

    async def fetch_bot_identity(self) -> dict[str, Any]:
        return parse_bot_identity(await self._post("getMe"), bot_token=self._bot_token)

    async def fetch_transactions(self, *, offset: int, limit: int) -> list[dict[str, Any]]:
        result = await self._post(
            "getStarTransactions",
            {"offset": int(offset), "limit": int(limit)},
        )
        if isinstance(result, dict):
            raw_transactions = result.get("transactions") or []
        elif isinstance(result, list):
            raw_transactions = result
        else:
            raw_transactions = []
        if not isinstance(raw_transactions, list):
            raw_transactions = []
        return [
            parse_star_transaction(transaction, bot_token=self._bot_token)
            for transaction in raw_transactions
        ]
