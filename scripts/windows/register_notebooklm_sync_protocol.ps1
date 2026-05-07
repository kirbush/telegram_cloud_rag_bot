param(
    [string]$ProtocolScheme = "tgctxbot-notebooklm-sync",
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

$invokeScript = Join-Path $HelperRoot "invoke_notebooklm_sync.ps1"
$protocolRoot = "HKCU:\\Software\\Classes\\$ProtocolScheme"
$commandValue = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$invokeScript`" -HelperRoot `"$HelperRoot`" -PythonExe `"$PythonExe`" -LaunchUri `"%1`""

New-Item -Path $protocolRoot -Force | Out-Null
Set-Item -Path $protocolRoot -Value "URL:$ProtocolScheme Protocol"
New-ItemProperty -Path $protocolRoot -Name "URL Protocol" -Value "" -PropertyType String -Force | Out-Null
New-Item -Path "$protocolRoot\\DefaultIcon" -Force | Out-Null
Set-Item -Path "$protocolRoot\\DefaultIcon" -Value "`"$PythonExe`",0"
New-Item -Path "$protocolRoot\\shell\\open\\command" -Force | Out-Null
Set-Item -Path "$protocolRoot\\shell\\open\\command" -Value $commandValue

Write-Host "Registered protocol handler $ProtocolScheme for NotebookLM Windows sync."
Write-Host "Helper root: $HelperRoot"
Write-Host "Python:    $PythonExe"
