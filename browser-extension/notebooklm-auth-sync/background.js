const ALLOWED_COOKIE_URLS = [
  "https://google.com/",
  "https://accounts.google.com/",
  "https://myaccount.google.com/",
  "https://notebooklm.google.com/",
  "https://googleusercontent.com/",
  "https://usercontent.google.com/",
];

const AUTH_COOKIE_NAMES = new Set([
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
]);

function normalizeSameSite(value) {
  if (value === "strict") return "Strict";
  if (value === "no_restriction" || value === "none") return "None";
  return "Lax";
}

function toStorageCookie(cookie) {
  return {
    name: String(cookie.name || ""),
    value: String(cookie.value || ""),
    domain: String(cookie.domain || ""),
    path: String(cookie.path || "/") || "/",
    expires: typeof cookie.expirationDate === "number" ? cookie.expirationDate : -1,
    httpOnly: Boolean(cookie.httpOnly),
    secure: Boolean(cookie.secure),
    sameSite: normalizeSameSite(cookie.sameSite),
  };
}

function dedupeCookies(cookies) {
  const merged = new Map();
  for (const cookie of cookies) {
    if (!cookie.name || !cookie.value || !cookie.domain) continue;
    const key = `${cookie.name}\n${cookie.domain.toLowerCase()}\n${cookie.path || "/"}`;
    merged.set(key, cookie);
  }
  return Array.from(merged.values());
}

async function collectNotebookLmCookies() {
  const rawCookies = [];
  for (const url of ALLOWED_COOKIE_URLS) {
    const cookies = await browser.cookies.getAll({ url });
    rawCookies.push(...cookies);
  }
  const cookies = dedupeCookies(rawCookies.map(toStorageCookie));
  const names = new Set(cookies.map((cookie) => cookie.name));
  const authCookieNames = Array.from(AUTH_COOKIE_NAMES).filter((name) => names.has(name));
  if (!authCookieNames.length) {
    throw new Error(`Missing NotebookLM auth cookies. Expected one of: ${Array.from(AUTH_COOKIE_NAMES).join(", ")}`);
  }
  return {
    cookies,
    origins: [],
  };
}

browser.runtime.onMessage.addListener((message) => {
  if (!message || message.type !== "collectNotebookLmCookies") {
    return false;
  }
  return collectNotebookLmCookies().then((storageState) => ({
    ok: true,
    storageState,
    cookieCount: storageState.cookies.length,
  })).catch((error) => ({
    ok: false,
    error: error && error.message ? error.message : "Cookie collection failed.",
  }));
});
