import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def pytest_configure() -> None:
    os.environ.setdefault("BOT_TOKEN", "123456:test-token")
    os.environ.setdefault("NOTEBOOKLM_ADMIN_PASSWORD", "test-password")
    os.environ.setdefault("NOTEBOOKLM_RUNTIME_STATE_PATH", ".state/test-runtime-state.json")
