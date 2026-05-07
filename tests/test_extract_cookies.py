import importlib.util
import unittest
from pathlib import Path


def _load_extract_cookies_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "extract_cookies.py"
    spec = importlib.util.spec_from_file_location("extract_cookies", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


extract_cookies = _load_extract_cookies_module()


class ExtractCookiesTests(unittest.TestCase):
    def test_parse_cookie_editor_json_normalizes_fields(self) -> None:
        cookies = extract_cookies.parse_cookie_editor_json(
            [
                {
                    "domain": ".google.com",
                    "name": "__Secure-3PAPISID",
                    "path": "/",
                    "sameSite": "no_restriction",
                    "secure": True,
                    "httpOnly": False,
                    "expirationDate": 1807947880.9,
                    "value": "abc",
                },
                {
                    "domain": "notebooklm.google.com",
                    "name": "OSID",
                    "path": "/",
                    "sameSite": None,
                    "secure": True,
                    "httpOnly": True,
                    "value": "def",
                },
            ]
        )

        self.assertEqual(cookies[0]["sameSite"], "None")
        self.assertEqual(cookies[0]["expires"], 1807947880.9)
        self.assertEqual(cookies[1]["sameSite"], "Lax")
        self.assertTrue(cookies[1]["httpOnly"])

    def test_missing_required_cookies_reports_missing_sid(self) -> None:
        missing = extract_cookies.missing_required_cookies(
            [{"name": "SAPISID", "value": "x"}]
        )
        self.assertEqual(missing, {"SID"})
