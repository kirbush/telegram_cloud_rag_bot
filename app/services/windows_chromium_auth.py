"""Windows Chromium-family cookie extraction for NotebookLM auth sync."""

from __future__ import annotations

import base64
import ctypes
import ctypes.wintypes
import json
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

_CHROME_EPOCH_OFFSET = 11644473600
_REQUIRED_AUTH_COOKIE_NAMES = {
    "SID",
    "HSID",
    "SSID",
    "SAPISID",
    "__Secure-1PSID",
    "__Secure-3PSID",
}
_DOMAIN_SUFFIXES = (
    ".google.com",
    "google.com",
    ".notebooklm.google.com",
    "notebooklm.google.com",
    ".accounts.google.com",
    "accounts.google.com",
)
_BROWSER_ROOTS = {
    "chrome": ("Google", "Chrome", "User Data"),
    "edge": ("Microsoft", "Edge", "User Data"),
    "chromium": ("Chromium", "User Data"),
}


@dataclass(slots=True)
class ChromiumCookie:
    host_key: str
    name: str
    value: str
    path: str
    secure: bool
    http_only: bool
    same_site: str
    expires: float
    last_access_utc: int


@dataclass(slots=True)
class BrowserCandidate:
    browser: str
    profile: str
    local_state_path: Path
    cookies_path: Path


def helper_config_path() -> Path:
    appdata = os.getenv("APPDATA")
    if appdata:
        return Path(appdata) / "tgctxbot-notebooklm" / "windows-helper.json"
    return Path.home() / ".tgctxbot-notebooklm" / "windows-helper.json"


def parse_sync_launch_uri(value: str, *, expected_scheme: str | None = None) -> dict[str, str]:
    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    normalized_expected = (expected_scheme or "tgctxbot-notebooklm-sync").strip().lower()
    if scheme != normalized_expected:
        raise ValueError("Unsupported NotebookLM sync launch URI.")
    raw = parse_qs(parsed.query, keep_blank_values=True)
    payload: dict[str, str] = {}
    for key, items in raw.items():
        if not items:
            continue
        payload[key] = str(items[-1] or "")
    return payload


def build_storage_state(cookies: list[ChromiumCookie]) -> dict[str, Any]:
    return {
        "cookies": [
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.host_key,
                "path": cookie.path or "/",
                "secure": cookie.secure,
                "httpOnly": cookie.http_only,
                "sameSite": cookie.same_site,
                "expires": cookie.expires,
            }
            for cookie in cookies
        ],
        "origins": [],
    }


def choose_best_candidate(candidates: list[tuple[BrowserCandidate, list[ChromiumCookie]]]) -> tuple[BrowserCandidate, list[ChromiumCookie]]:
    if not candidates:
        raise ValueError("No supported Chromium browser profile with Google auth cookies was found.")

    def score(item: tuple[BrowserCandidate, list[ChromiumCookie]]) -> tuple[int, int, int]:
        _, cookies = item
        auth_names = {cookie.name for cookie in cookies}
        auth_score = len(_REQUIRED_AUTH_COOKIE_NAMES & auth_names)
        cookie_count = len(cookies)
        last_access = max((cookie.last_access_utc for cookie in cookies), default=0)
        return auth_score, cookie_count, last_access

    return max(candidates, key=score)


def extract_browser_storage_state(
    *,
    browser_preference: str = "auto",
    profile_preference: str = "auto",
) -> tuple[dict[str, Any], dict[str, Any]]:
    browser_preference = (browser_preference or "auto").strip().lower()
    profile_preference = (profile_preference or "auto").strip()
    discovered: list[tuple[BrowserCandidate, list[ChromiumCookie]]] = []
    for candidate in iter_browser_candidates(browser_preference, profile_preference):
        cookies = read_candidate_cookies(candidate)
        if not cookies:
            continue
        discovered.append((candidate, cookies))

    chosen_candidate, chosen_cookies = choose_best_candidate(discovered)
    state = build_storage_state(chosen_cookies)
    metadata = {
        "browser": chosen_candidate.browser,
        "profile": chosen_candidate.profile,
        "cookie_count": len(chosen_cookies),
        "auth_cookie_names": sorted({cookie.name for cookie in chosen_cookies} & _REQUIRED_AUTH_COOKIE_NAMES),
        "cookies_path": str(chosen_candidate.cookies_path),
    }
    return state, metadata


def iter_browser_candidates(
    browser_preference: str = "auto",
    profile_preference: str = "auto",
) -> list[BrowserCandidate]:
    local_appdata = os.getenv("LOCALAPPDATA")
    if not local_appdata:
        raise RuntimeError("LOCALAPPDATA is not available; Chromium cookie extraction is supported only on Windows.")

    browsers = [browser_preference] if browser_preference != "auto" else ["chrome", "edge", "chromium"]
    profiles = [profile_preference] if profile_preference != "auto" else None
    candidates: list[BrowserCandidate] = []
    for browser in browsers:
        root_parts = _BROWSER_ROOTS.get(browser)
        if root_parts is None:
            continue
        user_data_dir = Path(local_appdata).joinpath(*root_parts)
        local_state_path = user_data_dir / "Local State"
        if not local_state_path.exists():
            continue
        if profiles is None:
            discovered_profiles = ["Default"] + sorted(
                part.name
                for part in user_data_dir.glob("Profile *")
                if part.is_dir()
            )
        else:
            discovered_profiles = profiles
        for profile in discovered_profiles:
            profile_dir = user_data_dir / profile
            cookies_path = profile_dir / "Network" / "Cookies"
            if not cookies_path.exists():
                cookies_path = profile_dir / "Cookies"
            if not cookies_path.exists():
                continue
            candidates.append(
                BrowserCandidate(
                    browser=browser,
                    profile=profile,
                    local_state_path=local_state_path,
                    cookies_path=cookies_path,
                )
            )
    return candidates


def read_candidate_cookies(candidate: BrowserCandidate) -> list[ChromiumCookie]:
    master_key = _load_chromium_master_key(candidate.local_state_path)
    try:
        db_uri = f"file:{candidate.cookies_path.resolve().as_posix()}?mode=ro"
        with sqlite3.connect(db_uri, uri=True) as conn:
            rows = conn.execute(
                """
                SELECT host_key, name, value, path, is_secure, is_httponly,
                       expires_utc, encrypted_value, samesite, last_access_utc
                FROM cookies
                """
            ).fetchall()
    except sqlite3.Error:
        with tempfile.TemporaryDirectory() as tmp:
            temp_db = Path(tmp) / "Cookies"
            shutil.copy2(candidate.cookies_path, temp_db)
            with sqlite3.connect(temp_db) as conn:
                rows = conn.execute(
                    """
                    SELECT host_key, name, value, path, is_secure, is_httponly,
                           expires_utc, encrypted_value, samesite, last_access_utc
                    FROM cookies
                    """
                ).fetchall()

    cookies: list[ChromiumCookie] = []
    for row in rows:
        host_key = str(row[0] or "")
        if not _is_relevant_domain(host_key):
            continue
        name = str(row[1] or "")
        value = str(row[2] or "")
        if not value:
            encrypted_value = bytes(row[7] or b"")
            value = _decrypt_cookie_value(encrypted_value, master_key)
        if not value:
            continue
        cookies.append(
            ChromiumCookie(
                host_key=host_key,
                name=name,
                value=value,
                path=str(row[3] or "/") or "/",
                secure=bool(row[4]),
                http_only=bool(row[5]),
                expires=_chrome_time_to_unix_seconds(int(row[6] or 0)),
                same_site=_normalize_same_site(row[8]),
                last_access_utc=int(row[9] or 0),
            )
        )
    return _dedupe_cookies(cookies)


def _dedupe_cookies(cookies: list[ChromiumCookie]) -> list[ChromiumCookie]:
    merged: dict[tuple[str, str, str], ChromiumCookie] = {}
    for cookie in cookies:
        key = (cookie.host_key, cookie.name, cookie.path)
        existing = merged.get(key)
        if existing is None or cookie.last_access_utc >= existing.last_access_utc:
            merged[key] = cookie
    return list(merged.values())


def _is_relevant_domain(host_key: str) -> bool:
    normalized = host_key.strip().lower()
    return any(normalized.endswith(suffix) for suffix in _DOMAIN_SUFFIXES)


def _chrome_time_to_unix_seconds(value: int) -> float:
    if value <= 0:
        return -1
    return max(-1, float(value) / 1_000_000 - _CHROME_EPOCH_OFFSET)


def _normalize_same_site(value: Any) -> str:
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return "Lax"
    if raw == 2:
        return "Strict"
    if raw == 0:
        return "None"
    return "Lax"


def _load_chromium_master_key(local_state_path: Path) -> bytes:
    payload = json.loads(local_state_path.read_text(encoding="utf-8"))
    encrypted_key = payload.get("os_crypt", {}).get("encrypted_key")
    if not encrypted_key:
        raise RuntimeError(f"Chromium Local State does not contain os_crypt.encrypted_key: {local_state_path}")
    raw = base64.b64decode(encrypted_key)
    if raw.startswith(b"DPAPI"):
        raw = raw[5:]
    return _crypt_unprotect_data(raw)


def _decrypt_cookie_value(encrypted_value: bytes, master_key: bytes) -> str:
    if not encrypted_value:
        return ""
    if encrypted_value.startswith((b"v10", b"v11")):
        nonce = encrypted_value[3:15]
        ciphertext = encrypted_value[15:]
        return _decrypt_aes_gcm(master_key, nonce, ciphertext)
    return _crypt_unprotect_data(encrypted_value).decode("utf-8", errors="ignore")


def _decrypt_aes_gcm(master_key: bytes, nonce: bytes, ciphertext: bytes) -> str:
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as exc:
        raise RuntimeError(
            "The Windows NotebookLM helper requires the 'cryptography' package for Chromium cookie decryption."
        ) from exc

    plaintext = AESGCM(master_key).decrypt(nonce, ciphertext, None)
    return plaintext.decode("utf-8", errors="ignore")


class _DataBlob(ctypes.Structure):
    _fields_ = [
        ("cbData", ctypes.wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_char)),
    ]


def _crypt_unprotect_data(encrypted_bytes: bytes) -> bytes:
    if os.name != "nt":
        raise RuntimeError("Chromium cookie decryption is supported only on Windows.")
    if not encrypted_bytes:
        return b""

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32

    buffer = ctypes.create_string_buffer(encrypted_bytes, len(encrypted_bytes))
    blob_in = _DataBlob(len(encrypted_bytes), buffer)
    blob_out = _DataBlob()

    if not crypt32.CryptUnprotectData(
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(blob_out),
    ):
        raise ctypes.WinError()

    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        kernel32.LocalFree(blob_out.pbData)
