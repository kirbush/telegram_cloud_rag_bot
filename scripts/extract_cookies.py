"""Convert Google auth cookies into NotebookLM storage_state.json.

Supported inputs:
  1. A raw cookie string copied from browser DevTools (`document.cookie`)
  2. A Cookie Editor JSON export (array of cookie objects)
  3. An existing Playwright storage_state.json object

Usage:
  python scripts/extract_cookies.py "<cookie string or JSON>"
  python scripts/extract_cookies.py path\\to\\cookies.json

Or pass via environment:
  set COOKIE_STRING=SID=...; HSID=...
  python scripts/extract_cookies.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

REQUIRED_COOKIES = {"SID"}


def parse_cookie_string(cookie_str: str) -> list[dict[str, Any]]:
    """Parse a browser cookie string into storage_state cookie objects."""
    cookies: list[dict[str, Any]] = []
    for pair in cookie_str.split(";"):
        pair = pair.strip()
        if "=" not in pair:
            continue
        name, _, value = pair.partition("=")
        cookies.append(
            {
                "name": name.strip(),
                "value": value.strip(),
                "domain": ".google.com",
                "path": "/",
                "secure": True,
                "httpOnly": False,
                "sameSite": "Lax",
            }
        )
    return cookies


def parse_cookie_editor_json(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert Cookie Editor export entries into storage_state cookie objects."""
    cookies: list[dict[str, Any]] = []
    for item in items:
        cookie = {
            "name": item["name"],
            "value": item["value"],
            "domain": item.get("domain", ".google.com"),
            "path": item.get("path", "/"),
            "secure": item.get("secure", True),
            "httpOnly": item.get("httpOnly", False),
            "sameSite": normalize_same_site(item.get("sameSite")),
        }
        if item.get("expirationDate"):
            cookie["expires"] = item["expirationDate"]
        cookies.append(cookie)
    return cookies


def normalize_same_site(value: Any) -> str:
    if value is None or value == "":
        return "Lax"

    normalized = str(value).strip().lower()
    if normalized in {"no_restriction", "none"}:
        return "None"
    if normalized == "strict":
        return "Strict"
    return "Lax"


def load_cookies(raw_input: str) -> list[dict[str, Any]]:
    """Auto-detect and load cookies from text, JSON, or a file path."""
    input_text = raw_input.strip().lstrip("\ufeff")
    if not input_text:
        return []

    path = Path(input_text)
    if path.exists():
        input_text = path.read_text(encoding="utf-8").lstrip("\ufeff")

    if input_text.startswith("[") or input_text.startswith("{"):
        payload = json.loads(input_text)
        if isinstance(payload, dict):
            if "cookies" not in payload:
                raise ValueError("JSON object must contain a 'cookies' key.")
            return parse_cookie_editor_json(payload["cookies"])
        if isinstance(payload, list):
            return parse_cookie_editor_json(payload)
        raise ValueError("Unsupported JSON payload for cookie import.")

    return parse_cookie_string(input_text)


def build_storage_state(cookies: list[dict[str, Any]]) -> dict[str, Any]:
    return {"cookies": cookies, "origins": []}


def missing_required_cookies(cookies: list[dict[str, Any]]) -> set[str]:
    return REQUIRED_COOKIES - {cookie.get("name") for cookie in cookies}


def main() -> int:
    raw_input = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else os.environ.get("COOKIE_STRING", "")
    if not raw_input:
        print(__doc__)
        return 1

    try:
        cookies = load_cookies(raw_input)
    except Exception as exc:
        print(f"Failed to parse cookies: {exc}")
        return 1

    if not cookies:
        print("No cookies parsed from the input.")
        return 1

    storage_state = build_storage_state(cookies)

    storage_dir = Path.home() / ".notebooklm"
    storage_dir.mkdir(parents=True, exist_ok=True)
    path = storage_dir / "storage_state.json"
    path.write_text(json.dumps(storage_state, indent=2), encoding="utf-8")

    key_names = [
        cookie["name"]
        for cookie in cookies
        if any(token in cookie["name"] for token in ["SID", "HSID", "SSID", "APISID", "SAPISID", "OSID", "PSID"])
    ]
    print(f"Saved {len(cookies)} cookies to {path}")
    print(f"Key auth cookies: {key_names}")

    missing = missing_required_cookies(cookies)
    if missing:
        print(f"Warning: missing required cookies for notebooklm-py auth: {sorted(missing)}")
        print("Export cookies from a Google page that includes SID, or run: notebooklm login")

    print("\nNow set in your .env:")
    print("  NOTEBOOKLM_ENABLED=true")
    print("  NOTEBOOKLM_DEFAULT_NOTEBOOK=<NOTEBOOK_ID>")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
