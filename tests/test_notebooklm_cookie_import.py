import json
import unittest

from app.services.notebooklm_cookie_import import (
    NotebookLMCookieImportError,
    normalize_notebooklm_cookie_import,
)


class NotebookLMCookieImportTests(unittest.TestCase):
    def test_cookie_editor_json_filters_allowed_domains(self) -> None:
        storage_state, metadata = normalize_notebooklm_cookie_import(
            json.dumps(
                [
                    {"domain": ".google.com", "name": "SID", "value": "sid", "path": "/"},
                    {
                        "domain": "accounts.google.com",
                        "name": "__Secure-1PSIDRTS",
                        "value": "rts",
                        "path": "/",
                        "sameSite": "None",
                        "secure": True,
                        "httpOnly": True,
                    },
                    {"domain": "example.com", "name": "SID", "value": "leak", "path": "/"},
                    {"domain": "google.evil.com", "name": "BAD", "value": "evil", "path": "/"},
                    {"domain": "foo.google.evil.com", "name": "BAD2", "value": "evil2", "path": "/"},
                    {"domain": "notebooklm.google.com", "name": "NLM", "value": "nlm", "path": "/"},
                    {
                        "domain": "googleusercontent.com",
                        "name": "GUC",
                        "value": "guc",
                        "path": "/",
                    },
                    {
                        "domain": "usercontent.google.com",
                        "name": "UC",
                        "value": "uc",
                        "path": "/",
                    },
                ]
            )
        )

        domains = {cookie["domain"] for cookie in storage_state["cookies"]}
        self.assertEqual(
            domains,
            {
                ".google.com",
                "accounts.google.com",
                "notebooklm.google.com",
                "googleusercontent.com",
                "usercontent.google.com",
            },
        )
        self.assertEqual(metadata["source_format"], "json")
        self.assertEqual(metadata["cookie_count"], 5)
        self.assertIn("SID", metadata["auth_cookie_names"])

    def test_cookie_editor_json_without_sid_accepts_secure_google_auth_cookies(self) -> None:
        storage_state, metadata = normalize_notebooklm_cookie_import(
            json.dumps(
                [
                    {
                        "domain": ".google.com",
                        "name": "SAPISID",
                        "value": "redacted-sapisid",
                        "path": "/",
                        "sameSite": None,
                        "secure": True,
                    },
                    {
                        "domain": ".google.com",
                        "name": "__Secure-1PSID",
                        "value": "redacted-1psid",
                        "path": "/",
                        "sameSite": None,
                        "secure": True,
                        "httpOnly": True,
                    },
                    {
                        "domain": ".google.com",
                        "name": "__Secure-3PSID",
                        "value": "redacted-3psid",
                        "path": "/",
                        "sameSite": "no_restriction",
                        "secure": True,
                        "httpOnly": True,
                    },
                    {
                        "domain": ".google.com",
                        "name": "SSID",
                        "value": "redacted-ssid",
                        "path": "/",
                        "secure": True,
                        "httpOnly": True,
                    },
                    {
                        "domain": "notebooklm.google.com",
                        "name": "OSID",
                        "value": "redacted-osid",
                        "path": "/",
                        "secure": True,
                        "httpOnly": True,
                    },
                    {
                        "domain": "notebooklm.google.com",
                        "name": "__Secure-OSID",
                        "value": "redacted-secure-osid",
                        "path": "/",
                        "secure": True,
                        "httpOnly": True,
                    },
                    {
                        "domain": ".google.com",
                        "name": "AEC",
                        "value": "redacted-aec",
                        "path": "/",
                        "sameSite": "lax",
                        "secure": True,
                        "httpOnly": True,
                    },
                ]
            )
        )

        cookies_by_name = {cookie["name"]: cookie for cookie in storage_state["cookies"]}
        self.assertNotIn("SID", cookies_by_name)
        self.assertEqual(cookies_by_name["__Secure-3PSID"]["sameSite"], "None")
        self.assertEqual(cookies_by_name["AEC"]["sameSite"], "Lax")
        self.assertEqual(
            metadata["auth_cookie_names"],
            ["OSID", "SAPISID", "SSID", "__Secure-1PSID", "__Secure-3PSID", "__Secure-OSID"],
        )

    def test_playwright_storage_state_preserves_origins(self) -> None:
        payload = {
            "cookies": [
                {"domain": ".google.com", "name": "SID", "value": "sid", "path": "/"},
                {
                    "domain": "notebooklm.google.com",
                    "name": "NLM",
                    "value": "nlm",
                    "path": "/",
                },
            ],
            "origins": [{"origin": "https://notebooklm.google.com", "localStorage": []}],
        }

        storage_state, _ = normalize_notebooklm_cookie_import(json.dumps(payload))

        self.assertEqual(storage_state["origins"], payload["origins"])
        self.assertEqual(len(storage_state["cookies"]), 2)

    def test_netscape_cookies_txt_is_normalized(self) -> None:
        raw_text = "\n".join(
            [
                "# Netscape HTTP Cookie File",
                ".google.com\tTRUE\t/\tTRUE\t1893456000\tSID\tsid",
                "accounts.google.com\tFALSE\t/\tTRUE\t1893456000\t__Secure-1PSIDRTS\trts",
                "example.com\tFALSE\t/\tFALSE\t1893456000\tSID\tleak",
                "",
            ]
        )

        storage_state, metadata = normalize_notebooklm_cookie_import(raw_text)

        names = {cookie["name"] for cookie in storage_state["cookies"]}
        domains = {cookie["domain"] for cookie in storage_state["cookies"]}
        self.assertEqual(names, {"SID", "__Secure-1PSIDRTS"})
        self.assertEqual(domains, {".google.com", "accounts.google.com"})
        self.assertEqual(metadata["source_format"], "netscape")

    def test_missing_required_auth_cookie_is_rejected(self) -> None:
        with self.assertRaises(NotebookLMCookieImportError):
            normalize_notebooklm_cookie_import(
                json.dumps([{"domain": ".google.com", "name": "OTHER", "value": "x", "path": "/"}])
            )
