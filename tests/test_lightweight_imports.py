import builtins
import importlib
import unittest
from unittest.mock import patch


def _blocked_import_factory(blocked_modules: set[str]):
    real_import = builtins.__import__

    def _guard(name, globals=None, locals=None, fromlist=(), level=0):
        if name in blocked_modules:
            raise AssertionError(f"unexpected import during lightweight reload: {name}")
        return real_import(name, globals, locals, fromlist, level)

    return _guard


def _legacy_module(*parts: str) -> str:
    return ".".join(parts)


class LightweightImportTests(unittest.TestCase):
    def test_api_main_can_reload_without_legacy_service_import(self) -> None:
        module = importlib.import_module("app.api.main")

        with patch(
            "builtins.__import__",
            side_effect=_blocked_import_factory(
                {_legacy_module("app", "services", "rag_" + "service")}
            ),
        ):
            importlib.reload(module)

        importlib.reload(module)

    def test_bot_handlers_can_reload_without_rag_ingest_or_celery_imports(self) -> None:
        module = importlib.import_module("app.bot.handlers")

        with patch(
            "builtins.__import__",
            side_effect=_blocked_import_factory(
                {
                    _legacy_module("app", "services", "rag_" + "service"),
                    _legacy_module("app", "ingest", "service"),
                    _legacy_module("app", "workers", "celery_app"),
                }
            ),
        ):
            importlib.reload(module)

        importlib.reload(module)
