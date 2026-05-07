(function () {
  const tokenMatch = window.location.pathname.match(/^\/auth-session\/([^/]+)$/);
  if (!tokenMatch) return;

  const bridge = document.getElementById("androidExtensionBridge");
  if (!bridge || bridge.dataset.notebooklmExtensionReady === "1") return;
  bridge.dataset.notebooklmExtensionReady = "1";

  const status = document.createElement("p");
  status.className = "hint";
  status.textContent = "Firefox Android extension is ready.";

  const button = document.createElement("button");
  button.className = "secondary";
  button.type = "button";
  button.textContent = "Sync cookies from Firefox";

  bridge.appendChild(status);
  bridge.appendChild(button);

  function setStatus(text, isError) {
    status.textContent = text;
    status.className = isError ? "hint error" : "hint";
  }

  button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      setStatus("Reading Firefox cookies...", false);
      const result = await browser.runtime.sendMessage({ type: "collectNotebookLmCookies" });
      if (!result || !result.ok) {
        throw new Error((result && result.error) || "Cookie collection failed.");
      }

      setStatus(`Uploading ${result.cookieCount} cookies...`, false);
      const token = encodeURIComponent(tokenMatch[1]);
      const response = await fetch(`/api/public/notebooklm/upload-sessions/${token}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          storage_state_json: JSON.stringify(result.storageState),
          helper_metadata: {
            browser: "firefox-android",
            profile: "notebooklm-auth-sync-extension",
            mode: "android-extension",
            cookie_count: result.cookieCount,
          },
        }),
      });
      const body = await response.json();
      if (!response.ok) {
        throw new Error(body.detail || "Upload failed.");
      }
      setStatus("NotebookLM auth cookies uploaded. This page will refresh status automatically.", false);
      window.dispatchEvent(new CustomEvent("notebooklm-auth-sync-uploaded", { detail: body }));
    } catch (error) {
      setStatus(error && error.message ? error.message : "Sync failed.", true);
    } finally {
      button.disabled = false;
    }
  });
}());
