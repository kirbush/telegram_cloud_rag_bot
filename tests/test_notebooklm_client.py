import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from app.services.notebooklm_client import (
    _COOKIE_JAR_ATTR,
    _load_auth_state_from_storage,
    load_notebooklm_auth,
    prime_notebooklm_client,
    refresh_notebooklm_google_keepalive,
    serialize_notebooklm_auth_to_storage_state,
)


class _FakeResponse:
    def __init__(self, *, text: str, url: str) -> None:
        self.text = text
        self.url = url

    def raise_for_status(self) -> None:
        return None


class NotebookLMClientAuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_notebooklm_auth_requires_proxy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text(
                json.dumps(
                    {
                        "cookies": [
                            {"name": "SID", "value": "sid-google", "domain": ".google.com", "path": "/"},
                        ],
                        "origins": [],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "NOTEBOOKLM_PROXY_ENABLED=true"):
                await load_notebooklm_auth(storage_path, 30.0, proxy_url=None)

    async def test_load_notebooklm_auth_uses_cookie_jar_and_preserves_accounts_google_cookies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text(
                json.dumps(
                    {
                        "cookies": [
                            {"name": "SID", "value": "sid-google", "domain": ".google.com", "path": "/"},
                            {
                                "name": "__Secure-1PSIDRTS",
                                "value": "accounts-cookie",
                                "domain": "accounts.google.com",
                                "path": "/",
                            },
                        ],
                        "origins": [],
                    }
                ),
                encoding="utf-8",
            )

            captured_init_kwargs = {}
            captured_get_kwargs = {}

            class _FakeAsyncClient:
                def __init__(self, **kwargs) -> None:
                    captured_init_kwargs.update(kwargs)

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb) -> bool:
                    return False

                async def get(self, url: str, **kwargs):
                    captured_get_kwargs.update(kwargs)
                    return _FakeResponse(text="<html>ok</html>", url=url)

            with (
                patch("app.services.notebooklm_client.httpx.AsyncClient", _FakeAsyncClient),
                patch("notebooklm.auth.extract_csrf_from_html", return_value="csrf"),
                patch("notebooklm.auth.extract_session_id_from_html", return_value="session"),
                patch("notebooklm._url_utils.is_google_auth_redirect", return_value=False),
            ):
                auth = await load_notebooklm_auth(storage_path, 30.0, proxy_url="socks5://127.0.0.1:43129")

            self.assertIsInstance(captured_init_kwargs["cookies"], httpx.Cookies)
            self.assertNotIn("Cookie", captured_init_kwargs["headers"])
            self.assertNotIn("headers", captured_get_kwargs)
            self.assertEqual(auth.cookies["SID"], "sid-google")
            self.assertEqual(auth.cookies["__Secure-1PSIDRTS"], "accounts-cookie")
            self.assertTrue(hasattr(auth, _COOKIE_JAR_ATTR))

    async def test_storage_state_without_sid_accepts_secure_google_auth_cookies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text(
                json.dumps(
                    {
                        "cookies": [
                            {
                                "name": "__Secure-1PSID",
                                "value": "secure-1psid",
                                "domain": ".google.com",
                                "path": "/",
                            },
                            {
                                "name": "__Secure-3PSID",
                                "value": "secure-3psid",
                                "domain": ".google.com",
                                "path": "/",
                            },
                            {
                                "name": "OSID",
                                "value": "osid",
                                "domain": "notebooklm.google.com",
                                "path": "/",
                            },
                        ],
                        "origins": [],
                    }
                ),
                encoding="utf-8",
            )

            auth_state = _load_auth_state_from_storage(storage_path)

            self.assertNotIn("SID", auth_state.header_cookies)
            self.assertEqual(auth_state.header_cookies["__Secure-1PSID"], "secure-1psid")
            self.assertEqual(auth_state.header_cookies["__Secure-3PSID"], "secure-3psid")
            self.assertEqual(auth_state.header_cookies["OSID"], "osid")
            self.assertEqual(auth_state.cookie_jar.get("OSID", domain="notebooklm.google.com", path="/"), "osid")

    async def test_prime_notebooklm_client_builds_cookie_jar_backed_http_client(self) -> None:
        cookie_jar = httpx.Cookies()
        cookie_jar.set("SID", "sid-google", domain="google.com", path="/")

        auth = SimpleNamespace(cookie_header="SID=sid-google")
        setattr(auth, _COOKIE_JAR_ATTR, cookie_jar)
        client = SimpleNamespace(_core=SimpleNamespace(_connect_timeout=None, auth=auth, _http_client=None))

        captured_init_kwargs = {}

        class _FakeAsyncClient:
            def __init__(self, **kwargs) -> None:
                captured_init_kwargs.update(kwargs)

        with patch("app.services.notebooklm_client.httpx.AsyncClient", _FakeAsyncClient):
            prime_notebooklm_client(client, 45.0, proxy_url="socks5://127.0.0.1:43129")

        self.assertIsInstance(client._core._http_client, _FakeAsyncClient)
        self.assertEqual(client._core._connect_timeout, 45.0)
        self.assertIs(captured_init_kwargs["cookies"], cookie_jar)
        self.assertTrue(captured_init_kwargs["follow_redirects"])
        self.assertNotIn("Cookie", captured_init_kwargs["headers"])

    async def test_prime_notebooklm_client_removes_raw_cookie_header_from_existing_client(self) -> None:
        cookie_jar = httpx.Cookies()
        cookie_jar.set("SID", "sid-google", domain="google.com", path="/")
        cookie_jar.set("__Secure-1PSIDRTS", "accounts-cookie", domain="accounts.google.com", path="/")

        auth = SimpleNamespace(
            cookie_header="SID=stale-header",
            cookies={"SID": "sid-google", "__Secure-1PSIDRTS": "accounts-cookie"},
        )
        setattr(auth, _COOKIE_JAR_ATTR, cookie_jar)

        existing_http_client = SimpleNamespace(
            headers={"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8", "Cookie": "SID=stale-header"},
            cookies=httpx.Cookies(),
        )
        existing_http_client.cookies.set("SID", "stale-header", domain="google.com", path="/")

        client = SimpleNamespace(
            _core=SimpleNamespace(
                _connect_timeout=None,
                auth=auth,
                _http_client=existing_http_client,
            )
        )

        prime_notebooklm_client(client, 45.0, proxy_url="socks5://127.0.0.1:43129")

        self.assertEqual(client._core._connect_timeout, 45.0)
        self.assertNotIn("Cookie", existing_http_client.headers)
        self.assertEqual(existing_http_client.cookies.get("SID", domain="google.com", path="/"), "sid-google")

    async def test_update_auth_headers_keeps_cookie_jar_and_removes_raw_cookie_header(self) -> None:
        cookie_jar = httpx.Cookies()
        cookie_jar.set("SID", "sid-old", domain="google.com", path="/")

        auth = SimpleNamespace(cookies={"SID": "sid-new"})
        setattr(auth, _COOKIE_JAR_ATTR, cookie_jar)
        http_client = SimpleNamespace(headers={"Cookie": "SID=sid-old"}, cookies=cookie_jar)
        client = SimpleNamespace(_core=SimpleNamespace(_connect_timeout=None, auth=auth, _http_client=http_client))

        prime_notebooklm_client(client, 30.0, proxy_url="socks5://127.0.0.1:43129")
        client._core.update_auth_headers()

        self.assertNotIn("Cookie", http_client.headers)
        self.assertEqual(http_client.cookies.get("SID", domain="google.com", path="/"), "sid-new")

    async def test_serialize_notebooklm_auth_storage_state_round_trips_live_cookie_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text(
                json.dumps(
                    {
                        "cookies": [
                            {"name": "SID", "value": "sid-google", "domain": ".google.com", "path": "/"},
                            {
                                "name": "__Secure-1PSIDRTS",
                                "value": "accounts-cookie",
                                "domain": "accounts.google.com",
                                "path": "/",
                                "httpOnly": True,
                                "secure": True,
                                "sameSite": "None",
                            },
                        ],
                        "origins": [{"origin": "https://notebooklm.google.com", "localStorage": []}],
                    }
                ),
                encoding="utf-8",
            )

            class _FakeAsyncClient:
                def __init__(self, **kwargs) -> None:
                    self._cookies = kwargs["cookies"]

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb) -> bool:
                    return False

                async def get(self, url: str, **kwargs):
                    return _FakeResponse(text="<html>ok</html>", url=url)

            with (
                patch("app.services.notebooklm_client.httpx.AsyncClient", _FakeAsyncClient),
                patch("notebooklm.auth.extract_csrf_from_html", return_value="csrf"),
                patch("notebooklm.auth.extract_session_id_from_html", return_value="session"),
                patch("notebooklm._url_utils.is_google_auth_redirect", return_value=False),
            ):
                auth = await load_notebooklm_auth(storage_path, 30.0, proxy_url="socks5://127.0.0.1:43129")

            cookie_jar = getattr(auth, _COOKIE_JAR_ATTR)
            cookie_jar.set("SID", "sid-refreshed", domain="google.com", path="/")
            cookie_jar.set("__Secure-1PSIDRTS", "rts-refreshed", domain="accounts.google.com", path="/")

            serialized = serialize_notebooklm_auth_to_storage_state(auth)
            cookies_by_name = {cookie["name"]: cookie for cookie in serialized["cookies"]}

            self.assertEqual(cookies_by_name["SID"]["value"], "sid-refreshed")
            self.assertEqual(cookies_by_name["__Secure-1PSIDRTS"]["value"], "rts-refreshed")
            self.assertEqual(cookies_by_name["__Secure-1PSIDRTS"]["sameSite"], "None")
            self.assertTrue(cookies_by_name["__Secure-1PSIDRTS"]["secure"])
            self.assertEqual(serialized["origins"], [{"origin": "https://notebooklm.google.com", "localStorage": []}])

            refreshed_path = Path(tmp) / "refreshed-storage.json"
            refreshed_path.write_text(json.dumps(serialized), encoding="utf-8")

            with (
                patch("app.services.notebooklm_client.httpx.AsyncClient", _FakeAsyncClient),
                patch("notebooklm.auth.extract_csrf_from_html", return_value="csrf"),
                patch("notebooklm.auth.extract_session_id_from_html", return_value="session"),
                patch("notebooklm._url_utils.is_google_auth_redirect", return_value=False),
            ):
                refreshed_auth = await load_notebooklm_auth(
                    refreshed_path,
                    30.0,
                    proxy_url="socks5://127.0.0.1:43129",
                )

            self.assertEqual(refreshed_auth.cookies["SID"], "sid-refreshed")
            self.assertEqual(refreshed_auth.cookies["__Secure-1PSIDRTS"], "rts-refreshed")

    async def test_refresh_notebooklm_google_keepalive_uses_cookie_jar_client_for_google_pings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage_path = Path(tmp) / "storage_state.json"
            storage_path.write_text(
                json.dumps(
                    {
                        "cookies": [
                            {"name": "SID", "value": "sid-google", "domain": ".google.com", "path": "/"},
                            {
                                "name": "__Secure-1PSIDRTS",
                                "value": "accounts-cookie",
                                "domain": "accounts.google.com",
                                "path": "/",
                            },
                        ],
                        "origins": [],
                    }
                ),
                encoding="utf-8",
            )

            init_kwargs: list[dict] = []
            requested_urls: list[str] = []

            class _FakeAsyncClient:
                def __init__(self, **kwargs) -> None:
                    init_kwargs.append(kwargs)
                    self._cookies = kwargs["cookies"]

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb) -> bool:
                    return False

                async def get(self, url: str, **kwargs):
                    requested_urls.append(url)
                    if url == "https://notebooklm.google.com/" and len(requested_urls) > 1:
                        self._cookies.set(
                            "__Secure-1PSIDRTS",
                            "rotated-rts",
                            domain="accounts.google.com",
                            path="/",
                        )
                    return _FakeResponse(text="<html>ok</html>", url=url)

            with (
                patch("app.services.notebooklm_client.httpx.AsyncClient", _FakeAsyncClient),
                patch("notebooklm.auth.extract_csrf_from_html", return_value="csrf"),
                patch("notebooklm.auth.extract_session_id_from_html", return_value="session"),
                patch("notebooklm._url_utils.is_google_auth_redirect", return_value=False),
            ):
                refreshed_state = await refresh_notebooklm_google_keepalive(
                    storage_path,
                    30.0,
                    proxy_url="socks5://127.0.0.1:43129",
                )

            self.assertEqual(
                requested_urls,
                [
                    "https://notebooklm.google.com/",
                    "https://notebooklm.google.com/",
                    "https://myaccount.google.com/",
                ],
            )
            self.assertTrue(all(isinstance(kwargs["cookies"], httpx.Cookies) for kwargs in init_kwargs))
            self.assertTrue(all("Cookie" not in kwargs["headers"] for kwargs in init_kwargs))
            cookies_by_name = {cookie["name"]: cookie for cookie in refreshed_state["cookies"]}
            self.assertEqual(cookies_by_name["__Secure-1PSIDRTS"]["value"], "rotated-rts")
