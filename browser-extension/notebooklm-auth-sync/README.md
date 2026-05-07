# NotebookLM Auth Sync Firefox Extension

This is a small Firefox Android WebExtension source package for syncing
NotebookLM auth cookies from Firefox to the existing tgctxbot auth refresh page.

The extension is intentionally narrow:

- It runs only on `/auth-session/*` pages.
- It reads cookies only for Google, NotebookLM, and googleusercontent domains.
- It accepts current Google auth cookie sets even when `SID` is absent, including
  `__Secure-*`, `SAPISID`, `SSID`, `OSID`, and `__Secure-OSID`.
- It posts Playwright-style `storage_state` JSON to the existing public upload
  endpoint for the current one-time token.

Stable Firefox Android normally installs extensions through addons.mozilla.org.
Use this source for AMO submission/signing or developer testing in Firefox
Nightly/Beta workflows. Until it is signed/installable on the phone, use the
documented Cookie-Editor fallback.
