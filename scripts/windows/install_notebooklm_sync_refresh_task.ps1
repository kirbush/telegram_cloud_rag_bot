param(
    [int]$EveryHours = 6,
    [string]$TaskName = "tgctxbot-notebooklm-sync-refresh",
    [string]$HelperRoot = "",
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"

if ($EveryHours -lt 1) {
    throw "EveryHours must be >= 1."
}

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

$invokeScript = Join-Path $HelperRoot "invoke_notebooklm_sync.ps1"
$taskCommand = "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$invokeScript`" -HelperRoot `"$HelperRoot`" -PythonExe `"$PythonExe`" -Scheduled"

schtasks /Create /TN $TaskName /SC HOURLY /MO $EveryHours /TR $taskCommand /F | Out-Null

Write-Host "Registered scheduled task $TaskName to refresh NotebookLM auth every $EveryHours hour(s)."
Write-Host "Command: $taskCommand"
