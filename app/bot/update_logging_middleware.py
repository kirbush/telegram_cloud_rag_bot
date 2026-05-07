import logging
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject

logger = logging.getLogger(__name__)


class UpdateLoggingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        if isinstance(event, Message):
            logger.info(
                "bot.update.incoming chat_id=%s message_id=%s user_id=%s text=%r entities=%s",
                getattr(getattr(event, "chat", None), "id", None),
                getattr(event, "message_id", None),
                getattr(getattr(event, "from_user", None), "id", None),
                (event.text or event.caption or "")[:200],
                [
                    {
                        "type": entity.type,
                        "offset": entity.offset,
                        "length": entity.length,
                    }
                    for entity in (event.entities or [])
                ],
            )
        return await handler(event, data)
