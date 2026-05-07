import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from app.services.notebooklm_runtime import NotebookLMRuntimeStore


class NotebookLMRuntimeStoreTests(unittest.TestCase):
    def test_empty_runtime_path_keeps_env_mode_and_does_not_require_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_default_notebook="env-notebook",
                notebooklm_storage_state=str(Path(tmp) / "env-storage.json"),
                notebooklm_runtime_state_path="",
                notebooklm_runtime_storage_state=str(Path(tmp) / "managed-storage.json"),
            )
            Path(settings.notebooklm_storage_state).write_text("{}", encoding="utf-8")

            runtime = NotebookLMRuntimeStore(settings=settings).get_runtime_config()

            self.assertTrue(runtime.enabled)
            self.assertEqual(runtime.source, "env")
            self.assertFalse(runtime.runtime_state_configured)
            self.assertIsNone(runtime.config_error)

    def test_falls_back_to_env_settings_when_runtime_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_default_notebook="env-notebook",
                notebooklm_storage_state=str(Path(tmp) / "env-storage.json"),
                notebooklm_runtime_state_path=str(Path(tmp) / "runtime-state.json"),
                notebooklm_runtime_storage_state=str(Path(tmp) / "managed-storage.json"),
            )
            Path(settings.notebooklm_storage_state).write_text("{}", encoding="utf-8")

            runtime = NotebookLMRuntimeStore(settings=settings).get_runtime_config()

            self.assertTrue(runtime.enabled)
            self.assertEqual(runtime.notebook_id, "env-notebook")
            self.assertEqual(runtime.source, "env")
            self.assertTrue(runtime.auth_ready)

    def test_invalid_runtime_file_reports_config_error_and_falls_back_to_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runtime_state = Path(tmp) / "runtime-state.json"
            runtime_state.write_text("{invalid json", encoding="utf-8")
            env_storage = Path(tmp) / "env-storage.json"
            env_storage.write_text("{}", encoding="utf-8")
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_default_notebook="env-notebook",
                notebooklm_storage_state=str(env_storage),
                notebooklm_runtime_state_path=str(runtime_state),
                notebooklm_runtime_storage_state=str(Path(tmp) / "managed-storage.json"),
            )

            runtime = NotebookLMRuntimeStore(settings=settings).get_runtime_config()

            self.assertEqual(runtime.source, "env")
            self.assertEqual(runtime.notebook_id, "env-notebook")
            self.assertIsNotNone(runtime.config_error)

    def test_update_runtime_config_persists_normalized_notebook_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_enabled=False,
                notebooklm_default_notebook="",
                notebooklm_storage_state=str(Path(tmp) / "env-storage.json"),
                notebooklm_runtime_state_path=str(Path(tmp) / "runtime-state.json"),
                notebooklm_runtime_storage_state=str(Path(tmp) / "managed-storage.json"),
            )

            status = NotebookLMRuntimeStore(settings=settings).update_runtime_config(
                enabled=True,
                notebook_ref="https://notebooklm.google.com/notebook/nb-runtime-123?pli=1",
            )

            self.assertTrue(status["enabled"])
            self.assertEqual(status["notebook_id"], "nb-runtime-123")
            self.assertEqual(
                status["notebook_url"],
                "https://notebooklm.google.com/notebook/nb-runtime-123",
            )
            self.assertEqual(status["source"], "runtime")

    def test_replace_storage_state_writes_to_managed_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runtime_state_path = Path(tmp) / "runtime-state.json"
            managed_storage_path = Path(tmp) / "managed-storage.json"
            settings = SimpleNamespace(
                notebooklm_enabled=False,
                notebooklm_default_notebook="",
                notebooklm_storage_state=str(Path(tmp) / "env-storage.json"),
                notebooklm_runtime_state_path=str(runtime_state_path),
                notebooklm_runtime_storage_state=str(managed_storage_path),
            )
            store = NotebookLMRuntimeStore(settings=settings)

            store.update_runtime_config(enabled=True, notebook_ref="nb-storage-1")
            status = store.replace_storage_state('{"cookies": [], "origins": []}')

            self.assertTrue(status["storage_state_exists"])
            self.assertTrue(status["auth_ready"])
            self.assertEqual(status["storage_state_path"], str(managed_storage_path))
            self.assertTrue(managed_storage_path.exists())

    def test_shared_storage_is_disabled_without_account_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_storage = Path(tmp) / "env-storage.json"
            shared_storage = Path(tmp) / "shared" / "storage_state.json"
            env_storage.write_text('{"cookies": [{"name": "SID", "value": "local"}]}', encoding="utf-8")
            shared_storage.parent.mkdir(parents=True)
            shared_storage.write_text('{"cookies": [{"name": "SID", "value": "shared"}]}', encoding="utf-8")
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_default_notebook="env-notebook",
                notebooklm_storage_state=str(env_storage),
                notebooklm_runtime_state_path="",
                notebooklm_runtime_storage_state=str(Path(tmp) / "managed-storage.json"),
                notebooklm_auth_account_key="",
                notebooklm_shared_storage_state=str(shared_storage),
            )

            runtime = NotebookLMRuntimeStore(settings=settings).get_runtime_config()

            self.assertEqual(runtime.storage_state_path, str(env_storage))
            self.assertEqual(runtime.shared_storage_state_path, "")

    def test_prefers_newer_shared_storage_for_same_account_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_storage = Path(tmp) / "env-storage.json"
            env_storage.write_text('{"cookies": [{"name": "SID", "value": "local"}]}', encoding="utf-8")
            shared_storage = Path(tmp) / "shared" / "same-google" / "storage_state.json"
            shared_storage.parent.mkdir(parents=True)
            shared_storage.write_text('{"cookies": [{"name": "SID", "value": "shared"}]}', encoding="utf-8")
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_default_notebook="env-notebook",
                notebooklm_storage_state=str(env_storage),
                notebooklm_runtime_state_path="",
                notebooklm_runtime_storage_state=str(Path(tmp) / "managed-storage.json"),
                notebooklm_auth_account_key="same-google",
                notebooklm_shared_storage_state=str(shared_storage),
            )

            runtime = NotebookLMRuntimeStore(settings=settings).get_runtime_config()

            self.assertEqual(runtime.storage_state_path, str(shared_storage))
            self.assertEqual(runtime.instance_storage_state_path, str(env_storage))
            self.assertTrue(runtime.shared_storage_state_exists)

    def test_replace_storage_state_mirrors_to_shared_storage_for_account_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runtime_state_path = Path(tmp) / "runtime-state.json"
            managed_storage_path = Path(tmp) / "managed-storage.json"
            shared_storage = Path(tmp) / "shared" / "same-google" / "storage_state.json"
            settings = SimpleNamespace(
                notebooklm_enabled=False,
                notebooklm_default_notebook="",
                notebooklm_storage_state=str(Path(tmp) / "env-storage.json"),
                notebooklm_runtime_state_path=str(runtime_state_path),
                notebooklm_runtime_storage_state=str(managed_storage_path),
                notebooklm_auth_account_key="same-google",
                notebooklm_shared_storage_state=str(shared_storage),
            )
            store = NotebookLMRuntimeStore(settings=settings)

            store.update_runtime_config(enabled=True, notebook_ref="nb-storage-1")
            status = store.replace_storage_state('{"cookies": [{"name": "SID", "value": "fresh"}], "origins": []}')

            self.assertTrue(managed_storage_path.exists())
            self.assertTrue(shared_storage.exists())
            self.assertEqual(
                managed_storage_path.read_text(encoding="utf-8"),
                shared_storage.read_text(encoding="utf-8"),
            )
            self.assertEqual(status["shared_storage_state_path"], str(shared_storage))

    def test_resolve_notebook_id_prefers_runtime_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = SimpleNamespace(
                notebooklm_enabled=True,
                notebooklm_default_notebook="env-default",
                notebooklm_notebook_map={"123": "mapped-notebook"},
                notebooklm_storage_state=str(Path(tmp) / "env-storage.json"),
                notebooklm_runtime_state_path=str(Path(tmp) / "runtime-state.json"),
                notebooklm_runtime_storage_state=str(Path(tmp) / "managed-storage.json"),
            )
            store = NotebookLMRuntimeStore(settings=settings)
            store.update_runtime_config(enabled=True, notebook_ref="runtime-notebook")

            self.assertEqual(store.resolve_notebook_id(123), "runtime-notebook")
