import unittest
from pathlib import Path

from app.services.windows_chromium_auth import (
    BrowserCandidate,
    ChromiumCookie,
    build_storage_state,
    choose_best_candidate,
    parse_sync_launch_uri,
)


class WindowsChromiumAuthTests(unittest.TestCase):
    def test_parse_sync_launch_uri_extracts_expected_payload(self) -> None:
        payload = parse_sync_launch_uri(
            "tgctxbot-notebooklm-sync://sync?upload_url=https%3A%2F%2Fexample.test%2Fupload"
            "&status_url=https%3A%2F%2Fexample.test%2Fstatus"
            "&entry_url=https%3A%2F%2Fexample.test%2Fentry"
            "&browser=edge&profile=Profile+1"
        )

        self.assertEqual(payload["upload_url"], "https://example.test/upload")
        self.assertEqual(payload["status_url"], "https://example.test/status")
        self.assertEqual(payload["entry_url"], "https://example.test/entry")
        self.assertEqual(payload["browser"], "edge")
        self.assertEqual(payload["profile"], "Profile 1")

    def test_build_storage_state_preserves_cookie_flags(self) -> None:
        state = build_storage_state(
            [
                ChromiumCookie(
                    host_key=".google.com",
                    name="SID",
                    value="cookie-value",
                    path="/",
                    secure=True,
                    http_only=True,
                    same_site="Lax",
                    expires=-1,
                    last_access_utc=1,
                )
            ]
        )

        self.assertEqual(state["cookies"][0]["name"], "SID")
        self.assertTrue(state["cookies"][0]["httpOnly"])
        self.assertTrue(state["cookies"][0]["secure"])
        self.assertEqual(state["origins"], [])

    def test_choose_best_candidate_prefers_auth_richer_profile(self) -> None:
        weak_candidate = BrowserCandidate(
            browser="chrome",
            profile="Default",
            local_state_path=Path("Local State"),
            cookies_path=Path("Cookies"),
        )
        strong_candidate = BrowserCandidate(
            browser="edge",
            profile="Profile 1",
            local_state_path=Path("Local State"),
            cookies_path=Path("Cookies"),
        )
        chosen_candidate, chosen_cookies = choose_best_candidate(
            [
                (
                    weak_candidate,
                    [
                        ChromiumCookie(
                            host_key=".google.com",
                            name="NID",
                            value="1",
                            path="/",
                            secure=True,
                            http_only=True,
                            same_site="Lax",
                            expires=-1,
                            last_access_utc=5,
                        )
                    ],
                ),
                (
                    strong_candidate,
                    [
                        ChromiumCookie(
                            host_key=".google.com",
                            name="SID",
                            value="1",
                            path="/",
                            secure=True,
                            http_only=True,
                            same_site="Lax",
                            expires=-1,
                            last_access_utc=10,
                        ),
                        ChromiumCookie(
                            host_key=".google.com",
                            name="HSID",
                            value="2",
                            path="/",
                            secure=True,
                            http_only=True,
                            same_site="Lax",
                            expires=-1,
                            last_access_utc=9,
                        ),
                    ],
                ),
            ]
        )

        self.assertEqual(chosen_candidate.browser, "edge")
        self.assertEqual(len(chosen_cookies), 2)
