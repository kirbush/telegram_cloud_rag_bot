"""Normalize user-supplied NotebookLM cookie exports into Playwright storage state."""

from __future__ import annotations

import json
from http.cookiejar import MozillaCookieJar
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

MAX_COOKIE_IMPORT_BYTES = 512 * 1024
AUTH_COOKIE_NAMES = {
    "SID",
    "HSID",
    "SSID",
    "SAPISID",
    "__Secure-1PSID",
    "__Secure-3PSID",
    "__Secure-1PSIDTS",
    "__Secure-3PSIDTS",
    "__Secure-1PSIDRTS",
    "__Secure-3PSIDRTS",
    "__Secure-1PSIDCC",
    "__Secure-3PSIDCC",
    "__Secure-OSID",
    "OSID",
}
_ALLOWED_COOKIE_DOMAIN_ROOTS = (
    "google.com",
    "googleusercontent.com",
    "usercontent.google.com",
)


class NotebookLMCookieImportError(ValueError):
    """Raised when a cookie import payload cannot be normalized safely."""


def _normalize_domain(domain: object) -> str:
    value = str(domain or "").strip()
    if value.startswith("#HttpOnly_"):
        value = value.removeprefix("#HttpOnly_")
    return value.lower()


def _is_allowed_domain(domain: object) -> bool:
    normalized = _normalize_domain(domain).lstrip(".")
    if not normalized:
        return False
    return any(
        normalized == root or normalized.endswith(f".{root}")
        for root in _ALLOWED_COOKIE_DOMAIN_ROOTS
    )


def _same_site(value: object) -> str:
    candidate = str(value or "Lax")
    lowered = candidate.lower()
    if lowered in {"no_restriction", "none"}:
        return "None"
    if lowered == "lax":
        return "Lax"
    if lowered == "strict":
        return "Strict"
    if candidate in {"Strict", "Lax", "None"}:
        return candidate
    return "Lax"


def _cookie_expires(cookie: dict[str, Any]) -> float:
    for key in ("expires", "expirationDate", "expiry", "expiration"):
        value = cookie.get(key)
        if value in {None, ""}:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return -1


def _storage_cookie(cookie: dict[str, Any]) -> dict[str, Any] | None:
    name = str(cookie.get("name", "") or "").strip()
    value = str(cookie.get("value", "") or "")
    domain = str(cookie.get("domain", cookie.get("host", "")) or "").strip()
    if domain.startswith("#HttpOnly_"):
        domain = domain.removeprefix("#HttpOnly_")
    if not name or not value or not _is_allowed_domain(domain):
        return None
    return {
        "name": name,
        "value": value,
        "domain": domain,
        "path": str(cookie.get("path", "") or "/") or "/",
        "expires": _cookie_expires(cookie),
        "httpOnly": bool(cookie.get("httpOnly", cookie.get("http_only", False))),
        "secure": bool(cookie.get("secure", True)),
        "sameSite": _same_site(cookie.get("sameSite", cookie.get("same_site", "Lax"))),
    }


def _normalize_json_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("cookies"), list):
        raw_cookies = payload["cookies"]
        origins = payload.get("origins", [])
        if not isinstance(origins, list):
            origins = []
    elif isinstance(payload, list):
        raw_cookies = payload
        origins = []
    else:
        raise NotebookLMCookieImportError(
            "Cookie import JSON must be a Playwright storage object or a Cookie-Editor cookie array."
        )

    cookies = []
    for item in raw_cookies:
        if not isinstance(item, dict):
            continue
        normalized = _storage_cookie(item)
        if normalized is not None:
            cookies.append(normalized)
    return {"cookies": _dedupe_cookies(cookies), "origins": origins}


def _normalize_netscape_payload(raw_text: str) -> dict[str, Any]:
    with NamedTemporaryFile("w", encoding="utf-8", delete=False) as temp_file:
        temp_file.write(raw_text)
        temp_path = Path(temp_file.name)
    try:
        jar = MozillaCookieJar(str(temp_path))
        jar.load(ignore_discard=True, ignore_expires=True)
    except Exception as exc:
        raise NotebookLMCookieImportError(f"Netscape cookies.txt could not be parsed: {exc}") from exc
    finally:
        try:
            temp_path.unlink()
        except OSError:
            pass

    cookies = []
    for cookie in jar:
        normalized = _storage_cookie(
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "expires": cookie.expires if cookie.expires is not None else -1,
                "httpOnly": str(cookie.domain or "").startswith("#HttpOnly_"),
                "secure": cookie.secure,
                "sameSite": "Lax",
            }
        )
        if normalized is not None:
            cookies.append(normalized)
    return {"cookies": _dedupe_cookies(cookies), "origins": []}


def _dedupe_cookies(cookies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for cookie in cookies:
        key = (
            str(cookie.get("name", "")),
            _normalize_domain(cookie.get("domain", "")),
            str(cookie.get("path", "/") or "/"),
        )
        merged[key] = cookie
    return list(merged.values())


def normalize_notebooklm_cookie_import(raw_value: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return Playwright storage-state JSON plus import metadata.

    Accepted inputs:
    - Playwright storage_state JSON: {"cookies": [...], "origins": [...]}
    - Cookie-Editor JSON: [{"domain": ".google.com", "name": "__Secure-1PSID", ...}]
    - Netscape cookies.txt
    """
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        raise NotebookLMCookieImportError("Cookie import payload is empty.")
    if len(raw_value.encode("utf-8")) > MAX_COOKIE_IMPORT_BYTES:
        raise NotebookLMCookieImportError("Cookie import payload is too large.")

    source_format = "json"
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        source_format = "netscape"
        storage_state = _normalize_netscape_payload(raw_value)
    else:
        storage_state = _normalize_json_payload(payload)

    cookie_names = {str(cookie.get("name", "")) for cookie in storage_state["cookies"]}
    auth_cookie_names = cookie_names & AUTH_COOKIE_NAMES
    if not auth_cookie_names:
        raise NotebookLMCookieImportError(
            f"Cookie import is missing NotebookLM auth cookies. Expected one of: {sorted(AUTH_COOKIE_NAMES)}."
        )

    metadata = {
        "source_format": source_format,
        "cookie_count": len(storage_state["cookies"]),
        "domains": sorted({_normalize_domain(cookie.get("domain", "")) for cookie in storage_state["cookies"]}),
        "auth_cookie_names": sorted(auth_cookie_names),
    }
    return storage_state, metadata
