import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.services.notebooklm_service import NotebookLMService


class _FakeNotebookLMClient:
    def __init__(self, response=None, exc: Exception | None = None) -> None:
        if exc is None:
            ask = AsyncMock(return_value=response)
        else:
            ask = AsyncMock(side_effect=exc)
        self.chat = SimpleNamespace(ask=ask)
        self._core = SimpleNamespace(_http_client=None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeProcess:
    def __init__(self, *, returncode: int, stdout: bytes = b"", stderr: bytes = b"") -> None:
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr
        self.kill_called = False

    async def communicate(self):
        return self._stdout, self._stderr

    def kill(self) -> None:
        self.kill_called = True


class NotebookLMServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        NotebookLMService._instance = None
        NotebookLMService._client = None

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_uses_async_upstream_client_and_extracts_references(
        self, mock_get_settings
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={},
                notebooklm_default_notebook="nb-1",
                notebooklm_timeout=30,
            )

            response = SimpleNamespace(
                answer="Notebook answer",
                references=[
                    SimpleNamespace(cited_text="Source excerpt", source_id="src-1"),
                    SimpleNamespace(cited_text=None, source_id="src-2"),
                ],
            )
            fake_client = _FakeNotebookLMClient(response)

            with patch(
                "app.services.notebooklm_service.create_notebooklm_client",
                new=AsyncMock(return_value=fake_client),
            ) as mock_create_client:
                service = NotebookLMService()
                result = await service.ask(chat_id=123, question="What happened?")

            self.assertEqual(result.answer, "Notebook answer")
            self.assertEqual(result.sources, ["Source excerpt", "src-2"])
            self.assertEqual(result.notebook_id, "nb-1")
            fake_client.chat.ask.assert_awaited_once_with("nb-1", "What happened?")
            mock_create_client.assert_awaited_once()

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_strips_single_grouped_and_adjacent_citations(
        self, mock_get_settings
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={},
                notebooklm_default_notebook="nb-1",
                notebooklm_timeout=30,
            )

            response = SimpleNamespace(
                answer="Fact [1], another fact [2]. Group [1, 2]. Range [1-2]. Glue[1][2]",
                references=[
                    SimpleNamespace(cited_text="Source excerpt", source_id="src-1", citation_number=1),
                    SimpleNamespace(cited_text="Another excerpt", source_id="src-2", citation_number=2),
                ],
            )
            fake_client = _FakeNotebookLMClient(response)

            with patch(
                "app.services.notebooklm_service.create_notebooklm_client",
                new=AsyncMock(return_value=fake_client),
            ):
                service = NotebookLMService()
                result = await service.ask(chat_id=123, question="What happened?")

            self.assertEqual(result.answer, "Fact, another fact. Group. Range. Glue")
            self.assertEqual(result.sources, ["Source excerpt", "Another excerpt"])

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_keeps_non_citation_bracket_text(self, mock_get_settings) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={},
                notebooklm_default_notebook="nb-1",
                notebooklm_timeout=30,
            )

            response = SimpleNamespace(
                answer="Window [2023] and stage [9] without inline citation markers.",
                references=[
                    SimpleNamespace(cited_text="Source excerpt", source_id="src-1", citation_number=1),
                    SimpleNamespace(cited_text="Another excerpt", source_id="src-2", citation_number=2),
                ],
            )
            fake_client = _FakeNotebookLMClient(response)

            with patch(
                "app.services.notebooklm_service.create_notebooklm_client",
                new=AsyncMock(return_value=fake_client),
            ):
                service = NotebookLMService()
                result = await service.ask(chat_id=123, question="What happened?")

            self.assertEqual(result.answer, "Window [2023] and stage [9] without inline citation markers.")

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_leaves_answer_without_citations_unchanged(self, mock_get_settings) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={},
                notebooklm_default_notebook="nb-1",
                notebooklm_timeout=30,
            )

            response = SimpleNamespace(
                answer="Plain answer without citations.",
                references=[
                    SimpleNamespace(cited_text="Source excerpt", source_id="src-1", citation_number=1),
                ],
            )
            fake_client = _FakeNotebookLMClient(response)

            with patch(
                "app.services.notebooklm_service.create_notebooklm_client",
                new=AsyncMock(return_value=fake_client),
            ):
                service = NotebookLMService()
                result = await service.ask(chat_id=123, question="What happened?")

            self.assertEqual(result.answer, "Plain answer without citations.")

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_prefers_explicit_chat_to_notebook_mapping(self, mock_get_settings) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={"555000111": "nb-chat"},
                notebooklm_default_notebook="nb-default",
                notebooklm_timeout=30,
            )

            response = SimpleNamespace(answer="Mapped answer", references=[])
            fake_client = _FakeNotebookLMClient(response)

            with patch(
                "app.services.notebooklm_service.create_notebooklm_client",
                new=AsyncMock(return_value=fake_client),
            ):
                service = NotebookLMService()
                result = await service.ask(chat_id=555000111, question="What happened?")

            self.assertEqual(result.notebook_id, "nb-chat")
            fake_client.chat.ask.assert_awaited_once_with("nb-chat", "What happened?")

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_retries_with_fresh_client_after_rpc_null_result(
        self, mock_get_settings
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={"555000111": "nb-chat"},
                notebooklm_default_notebook="nb-default",
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
            )

            stale_client = _FakeNotebookLMClient(
                exc=RuntimeError(
                    "RPC rLM1Ne returned null result data (possible server error or parameter mismatch)"
                )
            )
            fresh_client = _FakeNotebookLMClient(
                response=SimpleNamespace(answer="Recovered answer", references=[])
            )

            service = NotebookLMService()
            service._client = stale_client

            with patch(
                "app.services.notebooklm_service.create_notebooklm_client",
                new=AsyncMock(return_value=fresh_client),
            ) as mock_create_client:
                result = await service.ask(chat_id=555000111, question="Hello")

            self.assertEqual(result.answer, "Recovered answer")
            self.assertEqual(result.error, None)
            stale_client.chat.ask.assert_awaited_once_with("nb-chat", "Hello")
            fresh_client.chat.ask.assert_awaited_once_with("nb-chat", "Hello")
            mock_create_client.assert_awaited_once()

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_retries_with_fresh_client_after_auth_expired_error(
        self, mock_get_settings
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={"555000111": "nb-chat"},
                notebooklm_default_notebook="nb-default",
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
            )

            stale_client = _FakeNotebookLMClient(
                exc=ValueError(
                    "Authentication expired or invalid. Redirected to: https://accounts.google.com/"
                )
            )
            fresh_client = _FakeNotebookLMClient(
                response=SimpleNamespace(answer="Recovered after login", references=[])
            )

            service = NotebookLMService()
            service._client = stale_client

            with patch(
                "app.services.notebooklm_service.create_notebooklm_client",
                new=AsyncMock(return_value=fresh_client),
            ) as mock_create_client:
                result = await service.ask(chat_id=555000111, question="Hello")

            self.assertEqual(result.answer, "Recovered after login")
            self.assertEqual(result.error, None)
            stale_client.chat.ask.assert_awaited_once_with("nb-chat", "Hello")
            fresh_client.chat.ask.assert_awaited_once_with("nb-chat", "Hello")
            mock_create_client.assert_awaited_once()

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_runs_auto_refresh_command_before_retry_on_auth_expiry(
        self,
        mock_get_settings,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={"555000111": "nb-chat"},
                notebooklm_default_notebook="nb-default",
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
                notebooklm_refresh_cmd="python -m app.cli notebooklm-windows-sync --scheduled",
            )

            stale_client = _FakeNotebookLMClient(
                exc=ValueError(
                    "Authentication expired or invalid. Redirected to: https://accounts.google.com/"
                )
            )
            fresh_client = _FakeNotebookLMClient(
                response=SimpleNamespace(answer="Recovered after refresh", references=[])
            )

            service = NotebookLMService()
            service._client = stale_client

            with (
                patch(
                    "app.services.notebooklm_service.create_notebooklm_client",
                    new=AsyncMock(return_value=fresh_client),
                ) as mock_create_client,
                patch.object(service, "_run_refresh_command", new=AsyncMock()) as mock_refresh,
            ):
                result = await service.ask(chat_id=555000111, question="Hello")

            self.assertEqual(result.answer, "Recovered after refresh")
            self.assertEqual(result.error, None)
            mock_refresh.assert_awaited_once_with("python -m app.cli notebooklm-windows-sync --scheduled")
            mock_create_client.assert_awaited_once()

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_reports_auto_refresh_command_failure_without_retrying(
        self,
        mock_get_settings,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={"555000111": "nb-chat"},
                notebooklm_default_notebook="nb-default",
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
                notebooklm_refresh_cmd="python -m app.cli notebooklm-windows-sync --scheduled",
            )

            stale_client = _FakeNotebookLMClient(
                exc=ValueError(
                    "Authentication expired or invalid. Redirected to: https://accounts.google.com/"
                )
            )

            service = NotebookLMService()
            service._client = stale_client

            with (
                patch(
                    "app.services.notebooklm_service.create_notebooklm_client",
                    new=AsyncMock(),
                ) as mock_create_client,
                patch.object(
                    service,
                    "_run_refresh_command",
                    new=AsyncMock(side_effect=RuntimeError("NotebookLM auto-refresh command failed. Exit code 1.")),
                ),
            ):
                result = await service.ask(chat_id=555000111, question="Hello")

            self.assertIn("ответы временно недоступны", result.error)
            self.assertNotIn("NotebookLM", result.error)
            self.assertEqual(result.reason, "auth_expired")
            mock_create_client.assert_not_awaited()

    @patch("app.services.notebooklm_service.get_settings")
    async def test_ask_rpc_null_retry_does_not_run_auto_refresh_command(
        self,
        mock_get_settings,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={"555000111": "nb-chat"},
                notebooklm_default_notebook="nb-default",
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
                notebooklm_refresh_cmd="python -m app.cli notebooklm-windows-sync --scheduled",
            )

            stale_client = _FakeNotebookLMClient(
                exc=RuntimeError(
                    "RPC rLM1Ne returned null result data (possible server error or parameter mismatch)"
                )
            )
            fresh_client = _FakeNotebookLMClient(
                response=SimpleNamespace(answer="Recovered answer", references=[])
            )

            service = NotebookLMService()
            service._client = stale_client

            with (
                patch(
                    "app.services.notebooklm_service.create_notebooklm_client",
                    new=AsyncMock(return_value=fresh_client),
                ) as mock_create_client,
                patch.object(service, "_run_refresh_command", new=AsyncMock()) as mock_refresh,
            ):
                result = await service.ask(chat_id=555000111, question="Hello")

            self.assertEqual(result.answer, "Recovered answer")
            self.assertEqual(result.error, None)
            mock_refresh.assert_not_awaited()
            mock_create_client.assert_awaited_once()

    @patch("app.services.notebooklm_service.get_settings")
    async def test_get_client_passes_dedicated_notebooklm_proxy_to_client_factory(
        self,
        mock_get_settings,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text("{}", encoding="utf-8")

            mock_get_settings.return_value = SimpleNamespace(
                notebooklm_storage_state=str(storage_path),
                notebooklm_notebook_map={},
                notebooklm_default_notebook="nb-1",
                notebooklm_timeout=30,
                notebooklm_proxy_enabled=True,
                notebooklm_proxy_url="socks5://127.0.0.1:43129",
            )

            fake_client = _FakeNotebookLMClient(response=SimpleNamespace(answer="answer", references=[]))

            with patch(
                "app.services.notebooklm_service.create_notebooklm_client",
                new=AsyncMock(return_value=fake_client),
            ) as mock_create_client:
                await NotebookLMService()._get_client()

            mock_create_client.assert_awaited_once_with(
                str(storage_path),
                30,
                "socks5://127.0.0.1:43129",
            )

    async def test_run_refresh_command_raises_with_process_output_on_nonzero_exit(self) -> None:
        service = NotebookLMService()
        fake_process = _FakeProcess(returncode=1, stdout=b"refresh stdout", stderr=b"refresh stderr")

        with patch("app.services.notebooklm_service.asyncio.create_subprocess_shell", new=AsyncMock(return_value=fake_process)):
            with self.assertRaises(RuntimeError) as exc_info:
                await service._run_refresh_command("refresh-command")

        self.assertIn("Exit code 1", str(exc_info.exception))
        self.assertIn("refresh stderr", str(exc_info.exception))

    async def test_run_refresh_command_accepts_zero_exit_code(self) -> None:
        service = NotebookLMService()
        fake_process = _FakeProcess(returncode=0, stdout=b"ok", stderr=b"")

        with patch("app.services.notebooklm_service.asyncio.create_subprocess_shell", new=AsyncMock(return_value=fake_process)):
            await service._run_refresh_command("refresh-command")
