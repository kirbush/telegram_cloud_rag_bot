"""Unified entry point: runs Telegram polling and FastAPI admin/API in a single process.

Uses a PID lockfile to prevent multiple instances from running simultaneously,
which would cause Telegram polling conflicts and duplicate UI bindings.
"""

import asyncio
import ctypes
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace

from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession

try:
    import uvicorn
except ModuleNotFoundError:
    uvicorn = SimpleNamespace(Config=None, Server=None)

from app.bot.handlers import router
from app.core.config import get_settings
from app.core.logging import setup_logging

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = REPO_ROOT / ".tmp" / "app.pid"


def _format_fatal_boot_error(exc: Exception) -> str:
    if hasattr(exc, "errors"):
        try:
            errors = exc.errors()
        except Exception:
            errors = []
        if errors:
            message = str(errors[0].get("msg", "") or "").strip()
            if message.lower().startswith("value error, "):
                message = message.split(", ", 1)[1]
            if message:
                return message
    return str(exc).splitlines()[0].strip() or "Application startup failed."


def _pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if sys.platform == "win32":
        PROCESS_QUERY_INFORMATION = 0x0400
        STILL_ACTIVE = 259
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, 0, pid)
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)) == 0:
                return False
            return exit_code.value == STILL_ACTIVE
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def _acquire_lock() -> None:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists():
        try:
            old_pid = int(LOCK_PATH.read_text().strip())
        except (ValueError, OSError):
            old_pid = 0
        if old_pid and old_pid != os.getpid() and _pid_running(old_pid):
            print(
                f"[FATAL] Another app instance is already running (PID {old_pid}).\n"
                f"        Stop it first or delete {LOCK_PATH} if the PID is stale.",
                file=sys.stderr,
            )
            sys.exit(1)
    LOCK_PATH.write_text(str(os.getpid()))


def _release_lock() -> None:
    try:
        if LOCK_PATH.exists():
            current = LOCK_PATH.read_text().strip()
            if current == str(os.getpid()):
                LOCK_PATH.unlink()
    except OSError:
        pass


async def run() -> None:
    try:
        settings = get_settings()
    except Exception as exc:
        print(f"[FATAL] {_format_fatal_boot_error(exc)}", file=sys.stderr)
        raise SystemExit(1) from None
    setup_logging(settings.log_level)

    bot_session = None
    if settings.telegram_proxy_enabled and settings.telegram_proxy_url:
        bot_session = AiohttpSession(proxy=settings.telegram_proxy_url)
        logger.info("app.main telegram proxy enabled via %s", settings.telegram_proxy_url)

    bot = Bot(token=settings.bot_token, session=bot_session)
    dp = Dispatcher()
    dp.include_router(router)

    ui_host = os.environ.get("UI_HOST", "127.0.0.1")
    ui_port = int(os.environ.get("UI_PORT", "8000"))
    uvicorn_config = uvicorn.Config(
        "app.api.main:app",
        host=ui_host,
        port=ui_port,
        log_level=settings.log_level.lower(),
        access_log=False,
    )
    server = uvicorn.Server(uvicorn_config)

    logger.info(
        "app.main starting pid=%s ui=http://%s:%d telegram_polling=on",
        os.getpid(),
        ui_host,
        ui_port,
    )

    polling_task = asyncio.create_task(dp.start_polling(bot), name="telegram-polling")
    server_task = asyncio.create_task(server.serve(), name="uvicorn-serve")

    done, pending = await asyncio.wait(
        {polling_task, server_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()
    for task in pending:
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    for task in done:
        exc = task.exception()
        if exc is not None:
            logger.error("app.main task %s failed: %s", task.get_name(), exc)
            raise exc


def main() -> None:
    _acquire_lock()
    try:
        asyncio.run(run())
    finally:
        _release_lock()


if __name__ == "__main__":
    main()
