import json
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from app.api import main


EXTENSION_DIR = Path(__file__).resolve().parents[1] / "browser-extension" / "notebooklm-auth-sync"
REPO_ROOT = Path(__file__).resolve().parents[1]


def test_android_extension_manifest_is_domain_scoped() -> None:
    manifest = json.loads((EXTENSION_DIR / "manifest.json").read_text(encoding="utf-8"))

    assert manifest["manifest_version"] == 2
    assert "cookies" in manifest["permissions"]
    assert "https://*.google.com/*" in manifest["permissions"]
    assert "https://notebooklm.google.com/*" in manifest["permissions"]
    assert "https://*.googleusercontent.com/*" in manifest["permissions"]
    assert "https://usercontent.google.com/*" in manifest["permissions"]
    assert "<all_urls>" not in manifest["permissions"]
    assert manifest["content_scripts"][0]["matches"] == [
        "http://*/auth-session/*",
        "https://*/auth-session/*",
    ]


def test_android_extension_accepts_secure_google_auth_cookies_without_sid() -> None:
    background = (EXTENSION_DIR / "background.js").read_text(encoding="utf-8")

    assert "AUTH_COOKIE_NAMES" in background
    assert '"__Secure-1PSID"' in background
    assert '"__Secure-3PSID"' in background
    assert '"OSID"' in background
    assert 'new Set(["SID"])' not in background


def test_docker_image_includes_android_extension_source() -> None:
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY browser-extension /app/browser-extension" in dockerfile


def test_android_extension_package_endpoint_contains_source_files() -> None:
    response = TestClient(main.app).get("/api/public/notebooklm/android-extension/package.zip")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        assert sorted(archive.namelist()) == [
            "README.md",
            "background.js",
            "content-script.js",
            "manifest.json",
        ]
        manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
        assert manifest["name"] == "NotebookLM Auth Sync"


def test_auth_hub_exposes_android_extension_bridge_and_package_link() -> None:
    upload_manager = Mock()
    upload_manager.get_session_status.return_value = {
        "status": "pending",
        "expires_at": "2026-04-24T13:00:00+00:00",
        "protocol_url": "tgctxbot-notebooklm-sync://sync?upload_url=x",
    }

    with patch("app.api.main._upload_sync_manager", return_value=upload_manager):
        response = TestClient(main.app).get("/auth-session/token-redacted")

    assert response.status_code == 200
    assert "/api/public/notebooklm/android-extension/package.zip" in response.text
    assert 'id="androidExtensionBridge"' in response.text
    assert 'data-auth-token="token-redacted"' in response.text
    assert "Android / Manual Import" in response.text
    assert "Windows Helper" in response.text
