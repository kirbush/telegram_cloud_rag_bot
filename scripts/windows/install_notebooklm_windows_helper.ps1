param(
    [string]$InstallRoot = "",
    [string]$ProtocolScheme = "tgctxbot-notebooklm-sync",
    [switch]$RegisterRefreshTask,
    [int]$EveryHours = 6,
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"

if (-not $InstallRoot) {
    if (-not $env:LOCALAPPDATA) {
        throw "LOCALAPPDATA is not available."
    }
    $InstallRoot = Join-Path $env:LOCALAPPDATA "tgctxbot-notebooklm-helper"
}

if ($EveryHours -lt 1) {
    throw "EveryHours must be >= 1."
}

$sourceRoot = $PSScriptRoot
$installPath = [System.IO.Path]::GetFullPath($InstallRoot)
New-Item -ItemType Directory -Path $installPath -Force | Out-Null

$filesToCopy = @(
    "notebooklm_windows_sync_helper.py",
    "invoke_notebooklm_sync.ps1",
    "register_notebooklm_sync_protocol.ps1",
    "install_notebooklm_sync_refresh_task.ps1"
)

foreach ($fileName in $filesToCopy) {
    $sourcePath = Join-Path $sourceRoot $fileName
    if (-not (Test-Path $sourcePath)) {
        throw "Missing helper file: $sourcePath"
    }
    Copy-Item -Path $sourcePath -Destination (Join-Path $installPath $fileName) -Force
}

$cookieModuleSource = Join-Path $sourceRoot "windows_chromium_auth.py"
if (-not (Test-Path $cookieModuleSource)) {
    $repoModuleSource = Join-Path $sourceRoot "..\\..\\app\\services\\windows_chromium_auth.py"
    $cookieModuleSource = [System.IO.Path]::GetFullPath($repoModuleSource)
}
if (-not (Test-Path $cookieModuleSource)) {
    throw "Could not find windows_chromium_auth.py"
}
Copy-Item -Path $cookieModuleSource -Destination (Join-Path $installPath "windows_chromium_auth.py") -Force

if (-not $PythonExe) {
    $pythonCandidates = @()
    $repoVenvPython = Join-Path ([System.IO.Path]::GetFullPath((Join-Path $sourceRoot "..\\.."))) ".venv\\Scripts\\python.exe"
    if (Test-Path $repoVenvPython) {
        $pythonCandidates += @(
            @{
                Label = "repo-venv"
                Command = $repoVenvPython
                Kind = "direct"
            }
        )
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        $pythonCandidates += @(
            @{
                Label = "python"
                Command = "python"
                Kind = "direct"
            }
        )
    }
    if (Get-Command py -ErrorAction SilentlyContinue) {
        $pythonCandidates += @(
            @{
                Label = "py-3.11"
                Command = "py -3.11"
                Kind = "launcher"
            }
        )
    }

    foreach ($candidate in $pythonCandidates) {
        try {
            if ($candidate.Kind -eq "launcher") {
                $null = & py -3.11 --version 2>$null
            } else {
                $null = & $candidate.Command --version 2>$null
            }
            if ($LASTEXITCODE -eq 0) {
                $PythonExe = $candidate.Command
                break
            }
        } catch {
        }
    }

    if (-not $PythonExe) {
        throw "Python 3.11+ is required. Install Python first, then rerun the helper installer."
    }
}

$venvPath = Join-Path $installPath ".venv"
$venvPython = Join-Path $venvPath "Scripts\\python.exe"
if (-not (Test-Path $venvPython)) {
    if ($PythonExe -eq "py -3.11") {
        & py -3.11 -m venv $venvPath
    } else {
        & $PythonExe -m venv $venvPath
    }
}
if (-not (Test-Path $venvPython)) {
    throw "Failed to create helper virtual environment."
}

& $venvPython -m pip install --disable-pip-version-check --upgrade pip | Out-Null
& $venvPython -m pip install --disable-pip-version-check httpx cryptography notebooklm-py playwright | Out-Null
& $venvPython -m playwright install chromium | Out-Null

$registerScript = Join-Path $installPath "register_notebooklm_sync_protocol.ps1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File $registerScript -ProtocolScheme $ProtocolScheme -HelperRoot $installPath -PythonExe $venvPython

if ($RegisterRefreshTask) {
    $refreshScript = Join-Path $installPath "install_notebooklm_sync_refresh_task.ps1"
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File $refreshScript -EveryHours $EveryHours -HelperRoot $installPath -PythonExe $venvPython
}

Write-Host "NotebookLM Windows helper installed."
Write-Host "Install root: $installPath"
Write-Host "Python:       $venvPython"
