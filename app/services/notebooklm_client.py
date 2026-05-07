"""NotebookLM client helpers with browser-like cookie jar behavior."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import MethodType

import httpx

_AUTH_COOKIE_NAMES = {
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
_COOKIE_JAR_ATTR = "_tgctxbot_cookie_jar"
_STORAGE_STATE_ATTR = "_tgctxbot_storage_state"
_NOTEBOOKLM_URL = "https://notebooklm.google.com/"
_MYACCOUNT_URL = "https://myaccount.google.com/"
_PROXY_REQUIRED_MESSAGE = (
    "NotebookLM Google traffic requires NOTEBOOKLM_PROXY_ENABLED=true and NOTEBOOKLM_PROXY_URL."
)
_ALLOWED_COOKIE_DOMAIN_ROOTS = (
    "google.com",
    "googleusercontent.com",
    "usercontent.google.com",
)


@dataclass(slots=True)
class _NotebookLMAuthState:
    cookie_jar: httpx.Cookies
    header_cookies: dict[str, str]
    storage_state: dict


def _normalize_cookie_domain(domain: str) -> str:
    return str(domain or "").strip().lstrip(".").lower()


def _is_allowed_google_cookie_domain(domain: str) -> bool:
    normalized = _normalize_cookie_domain(domain)
    if not normalized:
        return False
    return any(
        normalized == root or normalized.endswith(f".{root}")
        for root in _ALLOWED_COOKIE_DOMAIN_ROOTS
    )


def _cookie_priority(domain: str) -> int:
    normalized = _normalize_cookie_domain(domain)
    if normalized == "google.com":
        return 0
    if normalized == "accounts.google.com":
        return 1
    if normalized == "notebooklm.google.com":
        return 2
    if normalized.endswith(".google.com"):
        return 3
    return 4


def _load_storage_state(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"Storage file not found: {path}\n"
            "Run 'python -m app.cli notebooklm-login' to authenticate first."
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("NotebookLM storage_state.json must contain an object payload.")
    return payload


def _load_auth_state_from_storage(path: Path) -> _NotebookLMAuthState:
    storage_state = _load_storage_state(path)
    cookie_jar = httpx.Cookies()
    header_cookies: dict[str, str] = {}
    cookie_priorities: dict[str, int] = {}
    seen_domains: set[str] = set()

    for cookie in storage_state.get("cookies", []):
        if not isinstance(cookie, dict):
            continue
        name = str(cookie.get("name", "") or "").strip()
        value = str(cookie.get("value", "") or "")
        domain = str(cookie.get("domain", "") or "")
        path_value = str(cookie.get("path", "") or "/")
        normalized_domain = _normalize_cookie_domain(domain)
        if not name or not value or not _is_allowed_google_cookie_domain(domain):
            continue

        cookie_jar.set(name, value, domain=normalized_domain, path=path_value or "/")
        seen_domains.add(normalized_domain)

        priority = _cookie_priority(domain)
        current_priority = cookie_priorities.get(name)
        if current_priority is None or priority < current_priority:
            header_cookies[name] = value
            cookie_priorities[name] = priority

    auth_cookie_names = set(header_cookies) & _AUTH_COOKIE_NAMES
    if not auth_cookie_names:
        detail = ""
        if seen_domains:
            detail = f"\nGoogle domains in storage: {sorted(seen_domains)}"
        raise ValueError(
            f"Missing required cookies: expected one of {sorted(_AUTH_COOKIE_NAMES)}{detail}\n"
            "Run 'python -m app.cli notebooklm-login' to authenticate."
        )

    return _NotebookLMAuthState(
        cookie_jar=cookie_jar,
        header_cookies=header_cookies,
        storage_state=storage_state,
    )


def _build_timeout(timeout_s: float) -> httpx.Timeout:
    return httpx.Timeout(
        connect=timeout_s,
        read=timeout_s,
        write=timeout_s,
        pool=timeout_s,
    )


def _require_proxy_url(proxy_url: str | None) -> str:
    value = str(proxy_url or "").strip()
    if not value:
        raise ValueError(_PROXY_REQUIRED_MESSAGE)
    return value


def _build_http_client(
    cookie_jar: httpx.Cookies,
    timeout_s: float,
    proxy_url: str | None = None,
) -> httpx.AsyncClient:
    proxy_url = _require_proxy_url(proxy_url)
    client_kwargs = {
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        },
        "cookies": cookie_jar,
        "follow_redirects": True,
        "timeout": _build_timeout(timeout_s),
        "trust_env": False,
        "proxy": proxy_url,
    }
    return httpx.AsyncClient(**client_kwargs)


def _cookie_slot_key(name: str, domain: str, path_value: str) -> tuple[str, str, str]:
    return (str(name or ""), _normalize_cookie_domain(domain), str(path_value or "/") or "/")


def _normalize_same_site(value: object) -> str:
    same_site = str(value or "Lax")
    if same_site not in {"Strict", "Lax", "None"}:
        return "Lax"
    return same_site


def _cookie_to_storage_cookie(cookie, template: dict | None = None) -> dict[str, object]:
    template = dict(template or {})
    rest = getattr(cookie, "_rest", None)
    rest_lookup = {str(key).lower(): value for key, value in (rest or {}).items()}
    expires = cookie.expires if cookie.expires is not None else template.get("expires", -1)
    if expires in {None, ""}:
        expires = -1
    return {
        **template,
        "name": str(cookie.name or ""),
        "value": str(cookie.value or ""),
        "domain": str(template.get("domain", "") or cookie.domain or ""),
        "path": str(template.get("path", "") or cookie.path or "/"),
        "expires": float(expires),
        "httpOnly": bool(template.get("httpOnly", "httponly" in rest_lookup)),
        "secure": bool(template.get("secure", cookie.secure)),
        "sameSite": _normalize_same_site(template.get("sameSite", rest_lookup.get("samesite", "Lax"))),
    }


def serialize_cookie_jar_to_storage_state(
    cookie_jar: httpx.Cookies,
    *,
    template_state: dict | None = None,
) -> dict:
    template_payload = template_state if isinstance(template_state, dict) else {}
    serialized: dict = {
        key: value
        for key, value in template_payload.items()
        if key != "cookies"
    }
    serialized.setdefault("origins", [])

    cookies: list[dict[str, object]] = []
    template_google_cookies: dict[tuple[str, str, str], dict] = {}
    for raw_cookie in template_payload.get("cookies", []):
        if not isinstance(raw_cookie, dict):
            continue
        domain = str(raw_cookie.get("domain", "") or "")
        key = _cookie_slot_key(raw_cookie.get("name", ""), domain, raw_cookie.get("path", "/"))
        if _is_allowed_google_cookie_domain(domain):
            template_google_cookies[key] = raw_cookie
            continue
        cookies.append(dict(raw_cookie))

    for cookie in cookie_jar.jar:
        domain = str(cookie.domain or "")
        if not cookie.name or not _is_allowed_google_cookie_domain(domain):
            continue
        key = _cookie_slot_key(cookie.name, domain, cookie.path or "/")
        cookies.append(_cookie_to_storage_cookie(cookie, template_google_cookies.get(key)))

    serialized["cookies"] = cookies
    return serialized


def serialize_notebooklm_auth_to_storage_state(auth) -> dict:
    cookie_jar = getattr(auth, _COOKIE_JAR_ATTR, None)
    if cookie_jar is None:
        raise ValueError("NotebookLM auth does not carry a cookie jar.")
    template_state = getattr(auth, _STORAGE_STATE_ATTR, None)
    storage_state = serialize_cookie_jar_to_storage_state(cookie_jar, template_state=template_state)
    setattr(auth, _STORAGE_STATE_ATTR, storage_state)
    return storage_state


def _sync_cookie_jar_from_auth(auth, cookie_jar: httpx.Cookies) -> None:
    auth_cookies = getattr(auth, "cookies", None)
    if not isinstance(auth_cookies, dict):
        return

    existing_by_name: dict[str, list[tuple[str, str]]] = {}
    for cookie in cookie_jar.jar:
        existing_by_name.setdefault(cookie.name, []).append((cookie.domain, cookie.path))

    for name, value in auth_cookies.items():
        existing_slots = existing_by_name.get(name)
        if not existing_slots:
            cookie_jar.set(name, value, domain="google.com", path="/")
            continue
        for domain, path_value in existing_slots:
            cookie_jar.set(name, value, domain=domain, path=path_value or "/")


def _install_cookie_jar_update_auth_headers(core) -> None:
    def _update_auth_headers(self) -> None:
        if not self._http_client:
            raise RuntimeError("Client not initialized. Use 'async with' context.")

        auth = getattr(self, "auth", None)
        if auth is None:
            return

        cookie_jar = getattr(auth, _COOKIE_JAR_ATTR, None)
        if cookie_jar is not None:
            _sync_cookie_jar_from_auth(auth, cookie_jar)
            self._http_client.headers.pop("Cookie", None)
            refreshed_cookie_jar = httpx.Cookies()
            refreshed_cookie_jar.update(cookie_jar)
            self._http_client.cookies.clear()
            self._http_client.cookies.update(refreshed_cookie_jar)
            return

        self._http_client.headers.pop("Cookie", None)

    core.update_auth_headers = MethodType(_update_auth_headers, core)


async def load_notebooklm_auth(
    storage_path: str | Path,
    timeout_s: float,
    proxy_url: str | None = None,
):
    """Load cookies from storage and fetch fresh CSRF/session tokens."""
    from notebooklm._url_utils import is_google_auth_redirect
    from notebooklm.auth import (
        AuthTokens,
        extract_csrf_from_html,
        extract_session_id_from_html,
    )

    path = Path(storage_path).expanduser()
    auth_state = _load_auth_state_from_storage(path)
    proxy_url = _require_proxy_url(proxy_url)

    client_kwargs = {
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        },
        "cookies": auth_state.cookie_jar,
        "timeout": _build_timeout(timeout_s),
        "trust_env": False,
        "follow_redirects": True,
        "proxy": proxy_url,
    }

    async with httpx.AsyncClient(**client_kwargs) as http_client:
        response = await http_client.get("https://notebooklm.google.com/")
        response.raise_for_status()

    final_url = str(response.url)
    if is_google_auth_redirect(final_url):
        raise ValueError(
            "Authentication expired or invalid. "
            f"Redirected to: {final_url}\n"
            "Run 'python -m app.cli notebooklm-login' to re-authenticate."
        )

    csrf_token = extract_csrf_from_html(response.text, final_url)
    session_id = extract_session_id_from_html(response.text, final_url)
    auth = AuthTokens(cookies=auth_state.header_cookies, csrf_token=csrf_token, session_id=session_id)
    setattr(auth, _COOKIE_JAR_ATTR, auth_state.cookie_jar)
    setattr(auth, _STORAGE_STATE_ATTR, auth_state.storage_state)
    return auth


async def refresh_notebooklm_google_keepalive(
    storage_path: str | Path,
    timeout_s: float,
    proxy_url: str | None = None,
) -> dict:
    from notebooklm._url_utils import is_google_auth_redirect

    auth = await load_notebooklm_auth(storage_path, timeout_s, proxy_url)
    cookie_jar = getattr(auth, _COOKIE_JAR_ATTR, None)
    if cookie_jar is None:
        raise ValueError("NotebookLM auth does not carry a cookie jar.")

    async with _build_http_client(cookie_jar, timeout_s, proxy_url) as http_client:
        for url in (_NOTEBOOKLM_URL, _MYACCOUNT_URL):
            response = await http_client.get(url)
            response.raise_for_status()
            final_url = str(response.url)
            if is_google_auth_redirect(final_url):
                raise ValueError(
                    "Authentication expired or invalid. "
                    f"Redirected to: {final_url}\n"
                    "Run 'python -m app.cli notebooklm-login' to re-authenticate."
                )

    return serialize_notebooklm_auth_to_storage_state(auth)


def prime_notebooklm_client(
    client,
    timeout_s: float,
    proxy_url: str | None = None,
) -> None:
    """Prepare the client's internal httpx session for this environment."""
    proxy_url = _require_proxy_url(proxy_url)
    core = client._core
    core._connect_timeout = timeout_s
    auth = getattr(core, "auth", None)
    if auth is None:
        return
    cookie_jar = getattr(auth, _COOKIE_JAR_ATTR, None)
    if cookie_jar is None:
        return
    _install_cookie_jar_update_auth_headers(core)
    if core._http_client is not None:
        core.update_auth_headers()
    if core._http_client is None:
        core._http_client = _build_http_client(cookie_jar, timeout_s, proxy_url)


async def create_notebooklm_client(
    storage_path: str | Path,
    timeout_s: float,
    proxy_url: str | None = None,
):
    """Create a NotebookLM client with explicit network settings."""
    from notebooklm import NotebookLMClient

    auth = await load_notebooklm_auth(storage_path, timeout_s, proxy_url)
    client = NotebookLMClient(auth, timeout=timeout_s)
    prime_notebooklm_client(client, timeout_s, proxy_url)
    return client
