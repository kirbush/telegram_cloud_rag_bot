param(
    [string]$LaunchUri = "",
    [switch]$Scheduled,
    [string]$HelperRoot = "",
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"

if (-not $HelperRoot) {
    $HelperRoot = $PSScriptRoot
}

if (-not $PythonExe) {
    $pythonCandidates = @(
        (Join-Path $HelperRoot ".venv\\Scripts\\python.exe"),
        (Join-Path ([System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\\.."))) ".venv\\Scripts\\python.exe")
    )
    foreach ($candidate in $pythonCandidates) {
        if (Test-Path $candidate) {
            $PythonExe = $candidate
            break
        }
    }
}

if (-not $PythonExe) {
    $PythonExe = "python"
}

$helperScript = Join-Path $HelperRoot "notebooklm_windows_sync_helper.py"
if (-not (Test-Path $helperScript)) {
    throw "NotebookLM helper script not found: $helperScript"
}
$logRoot = Join-Path $env:APPDATA "tgctxbot-notebooklm"
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$stdoutLog = Join-Path $logRoot "last-sync.stdout.log"
$stderrLog = Join-Path $logRoot "last-sync.stderr.log"

$arguments = @($helperScript)
if ($Scheduled) {
    $arguments += "--scheduled"
} elseif ($LaunchUri) {
    $arguments += "--launch-uri"
    $arguments += $LaunchUri
}

Start-Process `
    -FilePath $PythonExe `
    -ArgumentList $arguments `
    -WorkingDirectory $HelperRoot `
    -WindowStyle Minimized `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog
