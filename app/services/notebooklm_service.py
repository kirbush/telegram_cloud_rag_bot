"""NotebookLM bridge service — wraps notebooklm-py for the Telegram bot."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import PIPE

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession

from app.bot.messages import NOTEBOOKLM_AUTH_EXPIRED_MESSAGE, notebooklm_temporarily_unavailable
from app.core.config import get_notebooklm_proxy_url, get_settings
from app.services.notebooklm_events import log_event
from app.services.notebooklm_health import NotebookLMHealthService
from app.services.notebooklm_client import create_notebooklm_client, prime_notebooklm_client
from app.services.notebooklm_metrics import inc_auth_expired_total
from app.services.notebooklm_runtime import NotebookLMRuntimeStore
from app.services.notebooklm_upload_sync import get_notebooklm_upload_sync_manager

logger = logging.getLogger(__name__)

_INLINE_CITATION_RE = re.compile(r"\[(\s*\d+(?:\s*(?:,|[-\u2013\u2014])\s*\d+)*)\]")
_AUTO_REFRESH_TIMEOUT_S = 300.0
_AUTO_REFRESH_FAILURE_PREFIX = "NotebookLM auto-refresh command failed."


@dataclass
class NotebookLMResult:
    answer: str
    sources: list[str] = field(default_factory=list)
    notebook_id: str = ""
    latency_ms: int = 0
    error: str | None = None
    reason: str | None = None


class NotebookLMService:
    """Singleton-style service wrapping notebooklm-py client."""

    _instance: NotebookLMService | None = None
    _client = None
    _auth_alert_sent = False

    def __new__(cls) -> NotebookLMService:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    async def invalidate_cached_client(cls) -> None:
        await cls()._discard_client()

    @staticmethod
    def _first_admin_user_id(settings) -> int | None:
        raw_value = str(getattr(settings, "bot_admin_user_ids", "") or "").strip()
        if not raw_value:
            return None
        for part in raw_value.split(","):
            candidate = part.strip()
            if not candidate:
                continue
            try:
                return int(candidate)
            except ValueError:
                continue
        return None

    async def _discard_client(self) -> None:
        """Drop the cached client so the next request rebuilds auth state."""
        client = self._client
        self._client = None
        if client is None:
            return

        core = getattr(client, "_core", None)
        http_client = getattr(core, "_http_client", None)
        if http_client is None:
            return

        try:
            await http_client.aclose()
        except Exception:
            logger.debug("notebooklm client close failed during discard", exc_info=True)
        finally:
            core._http_client = None

    async def _get_client(self):
        """Lazily initialize the NotebookLM client from stored cookies."""
        settings = get_settings()
        proxy_url = get_notebooklm_proxy_url(settings)
        runtime = NotebookLMRuntimeStore(settings=settings)
        if self._client is not None:
            prime_notebooklm_client(self._client, settings.notebooklm_timeout, proxy_url)
            return self._client

        storage_path = str(Path(runtime.resolve_storage_state_path()).expanduser())

        if not Path(storage_path).exists():
            raise FileNotFoundError(
                f"NotebookLM storage state not found at {storage_path}. "
                "Refresh it in /admin/notebooklm or run: python -m app.cli notebooklm-login"
            )

        self._client = await create_notebooklm_client(
            storage_path,
            settings.notebooklm_timeout,
            proxy_url,
        )
        return self._client

    async def _ask_once(self, notebook_id: str, question: str, timeout_s: int):
        client = await self._get_client()
        async with client:
            return await asyncio.wait_for(
                client.chat.ask(notebook_id, question),
                timeout=timeout_s,
            )

    @staticmethod
    def _should_retry_with_fresh_client(exc: Exception) -> bool:
        error_text = str(exc).lower()
        retry_markers = (
            "returned null result data",
            "rpc rlm1ne",
            "authentication expired or invalid",
            "missing required cookies",
            "redirected to:",
            "login expired",
            "session expired",
            "unauthorized",
            "401",
        )
        if any(marker in error_text for marker in retry_markers):
            return True
        return "auth" in error_text and "notebooklm" in error_text

    @staticmethod
    def _is_auth_expired_error(exc: Exception) -> bool:
        error_text = str(exc).lower()
        auth_markers = (
            "authentication expired or invalid",
            "missing required cookies",
            "redirected to:",
            "login expired",
            "session expired",
            "unauthorized",
            "401",
        )
        if any(marker in error_text for marker in auth_markers):
            return True
        return "auth" in error_text and ("google" in error_text or "notebooklm" in error_text)

    @staticmethod
    def _configured_refresh_command(settings) -> str:
        return str(getattr(settings, "notebooklm_refresh_cmd", "") or "").strip()

    @staticmethod
    def _format_refresh_command_detail(stdout: bytes, stderr: bytes) -> str:
        parts: list[str] = []
        stdout_text = stdout.decode("utf-8", errors="replace").strip()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        if stdout_text:
            parts.append(f"stdout: {stdout_text[-400:]}")
        if stderr_text:
            parts.append(f"stderr: {stderr_text[-400:]}")
        return "\n".join(parts)

    async def _run_refresh_command(self, command: str) -> None:
        logger.warning("notebooklm.ask running auto-refresh command")
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=_AUTO_REFRESH_TIMEOUT_S)
        except asyncio.TimeoutError as exc:
            process.kill()
            stdout, stderr = await process.communicate()
            detail = self._format_refresh_command_detail(stdout, stderr)
            suffix = f"\n{detail}" if detail else ""
            raise RuntimeError(
                f"{_AUTO_REFRESH_FAILURE_PREFIX} Timed out after {_AUTO_REFRESH_TIMEOUT_S:.0f}s."
                f"{suffix}"
            ) from exc

        if process.returncode != 0:
            detail = self._format_refresh_command_detail(stdout, stderr)
            suffix = f"\n{detail}" if detail else ""
            raise RuntimeError(
                f"{_AUTO_REFRESH_FAILURE_PREFIX} Exit code {process.returncode}.{suffix}"
            )

        logger.info("notebooklm.ask auto-refresh command completed successfully")

    def _resolve_notebook_id(self, chat_id: int) -> str | None:
        """Resolve notebook_id from chat_id mapping or default."""
        settings = get_settings()
        return NotebookLMRuntimeStore(settings=settings).resolve_notebook_id(chat_id)

    @staticmethod
    def _strip_inline_citation_markers(answer: str, citation_numbers: set[int]) -> str:
        """Remove NotebookLM inline source markers while keeping normal bracketed text."""
        if not answer or not citation_numbers:
            return answer

        def _expand_citation_group(group: str) -> list[int] | None:
            expanded: list[int] = []
            for token in group.split(","):
                part = token.strip()
                if not part:
                    return None
                range_match = re.fullmatch(r"(\d+)\s*[-\u2013\u2014]\s*(\d+)", part)
                if range_match:
                    start = int(range_match.group(1))
                    end = int(range_match.group(2))
                    if end < start or end - start > 20:
                        return None
                    expanded.extend(range(start, end + 1))
                    continue
                if part.isdigit():
                    expanded.append(int(part))
                    continue
                return None
            return expanded or None

        def _replace(match: re.Match[str]) -> str:
            numbers = _expand_citation_group(match.group(1))
            if numbers is not None and all(number in citation_numbers for number in numbers):
                return ""
            return match.group(0)

        cleaned = _INLINE_CITATION_RE.sub(_replace, answer)
        cleaned = re.sub(r"[ \t]+([,.;:!?])", r"\1", cleaned)
        cleaned = re.sub(r"([(\[{])[ \t]+", r"\1", cleaned)
        cleaned = re.sub(r"[ \t]+([)\]}])", r"\1", cleaned)
        cleaned = re.sub(r"\(\)", "", cleaned)
        cleaned = re.sub(r"\[\]", "", cleaned)
        cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def _result_from_exception(
        self,
        notebook_id: str,
        started_at: float,
        timeout_s: int,
        exc: Exception,
    ) -> NotebookLMResult:
        settings = get_settings()
        if isinstance(exc, FileNotFoundError):
            error_msg = notebooklm_temporarily_unavailable(
                "storage_missing",
                NotebookLMHealthService.cooldown_minutes("storage_missing", settings),
            )
            reason = "storage_missing"
        elif isinstance(exc, asyncio.TimeoutError):
            error_msg = notebooklm_temporarily_unavailable(
                "tunnel_down:notebooklm",
                NotebookLMHealthService.cooldown_minutes("tunnel_down:notebooklm", settings),
            )
            reason = "tunnel_down:notebooklm"
        else:
            logger.exception("notebooklm.ask failed notebook_id=%s", notebook_id)
            error_msg = str(exc)
            lowered = error_msg.lower()
            reason = None
            if _AUTO_REFRESH_FAILURE_PREFIX.lower() in lowered:
                error_msg = notebooklm_temporarily_unavailable(
                    "auth_expired",
                    NotebookLMHealthService.cooldown_minutes("auth_expired", settings),
                )
                reason = "auth_expired"
            elif "missing required cookies" in lowered:
                error_msg = NOTEBOOKLM_AUTH_EXPIRED_MESSAGE
                reason = "auth_expired"
            elif "401" in error_msg or "auth" in lowered or "login" in lowered:
                error_msg = NOTEBOOKLM_AUTH_EXPIRED_MESSAGE
                reason = "auth_expired"
            else:
                error_msg = notebooklm_temporarily_unavailable(
                    "tunnel_down:notebooklm",
                    NotebookLMHealthService.cooldown_minutes("tunnel_down:notebooklm", settings),
                )
                reason = "tunnel_down:notebooklm"

        return NotebookLMResult(
            answer="",
            notebook_id=notebook_id,
            latency_ms=int((time.monotonic() - started_at) * 1000),
            error=error_msg,
            reason=reason,
        )

    async def _send_admin_auth_expired_notification(self, settings, *, notebook_id: str) -> bool:
        if self._auth_alert_sent:
            return False
        admin_user_id = self._first_admin_user_id(settings)
        if admin_user_id is None:
            return False

        entry_url: str | None = None
        try:
            session = get_notebooklm_upload_sync_manager().create_session(
                source="notebooklm-auth-expired",
                requested_by_user_id=admin_user_id,
                requested_by_chat_id=admin_user_id,
                notify_chat_id=None,
                notify_message_thread_id=None,
            )
            entry_url = str(session.get("entry_url", "") or "").strip() or None
        except Exception:
            logger.exception("notebooklm.ask failed to create auth refresh session")

        lines = [
            "NotebookLM сессия на VPS устарела и требует обновления.",
            f"Notebook: {notebook_id or 'unknown'}",
        ]
        if entry_url:
            lines.append(f"Одноразовая ссылка на обновление: {entry_url}")
        else:
            lines.append("Обнови авторизацию через /auth_nlm или /admin/notebooklm.")
        message = "\n".join(lines)

        try:
            bot_session = None
            if getattr(settings, "telegram_proxy_enabled", False) and getattr(settings, "telegram_proxy_url", None):
                bot_session = AiohttpSession(proxy=settings.telegram_proxy_url)
            bot = Bot(token=settings.bot_token, session=bot_session)
            await bot.send_message(admin_user_id, message, disable_web_page_preview=True)
            await bot.session.close()
            self.__class__._auth_alert_sent = True
            return True
        except Exception:
            logger.exception("notebooklm.ask failed to notify admin about expired auth")
            return False

    async def ask(self, chat_id: int, question: str) -> NotebookLMResult:
        """Send a question to NotebookLM and return the result."""
        t0 = time.monotonic()

        notebook_id = self._resolve_notebook_id(chat_id)
        if not notebook_id:
            return NotebookLMResult(
                answer="",
                error="Для этого чата пока не подключена база знаний. Напиши админу, чтобы он настроил поиск.",
            )

        settings = get_settings()
        if bool(getattr(settings, "notebooklm_vps_lightweight_mode", False)):
            readiness = await NotebookLMHealthService().readiness()
            if not readiness.ready and readiness.reason not in {None, "auth_expired"}:
                cooldown = NotebookLMHealthService.cooldown_minutes(readiness.reason, settings)
                return NotebookLMResult(
                    answer="",
                    notebook_id=notebook_id,
                    latency_ms=int((time.monotonic() - t0) * 1000),
                    error=notebooklm_temporarily_unavailable(readiness.reason or "storage_missing", cooldown),
                    reason=readiness.reason,
                )

        response = None
        error: Exception | None = None
        try:
            response = await self._ask_once(
                notebook_id,
                question,
                settings.notebooklm_timeout,
            )
        except Exception as exc:
            error = exc
            if self._should_retry_with_fresh_client(exc):
                logger.warning(
                    "notebooklm.ask retrying with fresh client notebook_id=%s",
                    notebook_id,
                )
                await self._discard_client()
                retry_allowed = True
                refresh_command = self._configured_refresh_command(settings)
                if refresh_command and self._is_auth_expired_error(exc):
                    try:
                        await self._run_refresh_command(refresh_command)
                    except Exception as refresh_exc:
                        error = refresh_exc
                        retry_allowed = False

                if retry_allowed:
                    try:
                        response = await self._ask_once(
                            notebook_id,
                            question,
                            settings.notebooklm_timeout,
                        )
                    except Exception as retry_exc:
                        error = retry_exc
                    else:
                        error = None

        if error is not None:
            if self._is_auth_expired_error(error):
                admin_notified = await self._send_admin_auth_expired_notification(
                    settings,
                    notebook_id=notebook_id,
                )
                inc_auth_expired_total()
                log_event(
                    logger,
                    logging.WARNING,
                    "nlm.auth.expired",
                    notebook_id=notebook_id,
                    admin_notified=admin_notified,
                )
                return NotebookLMResult(
                    answer="",
                    notebook_id=notebook_id,
                    latency_ms=int((time.monotonic() - t0) * 1000),
                    error=NOTEBOOKLM_AUTH_EXPIRED_MESSAGE,
                    reason="auth_expired",
                )
            return self._result_from_exception(
                notebook_id,
                t0,
                settings.notebooklm_timeout,
                error,
            )

        if self._auth_alert_sent:
            log_event(logger, logging.INFO, "nlm.auth.refreshed", notebook_id=notebook_id)
        self.__class__._auth_alert_sent = False
        references = getattr(response, "references", None) or []
        citation_numbers = {
            citation_number
            for citation_number in (
                getattr(reference, "citation_number", index)
                for index, reference in enumerate(references, start=1)
            )
            if isinstance(citation_number, int)
        }
        answer_text = getattr(response, "answer", None) or getattr(response, "text", None) or str(response)
        answer_text = self._strip_inline_citation_markers(answer_text, citation_numbers)
        sources = [
            getattr(reference, "cited_text", None) or getattr(reference, "source_id", str(reference))
            for reference in references
        ]
        latency_ms = int((time.monotonic() - t0) * 1000)
        return NotebookLMResult(
            answer=answer_text,
            sources=sources,
            notebook_id=notebook_id,
            latency_ms=latency_ms,
        )
