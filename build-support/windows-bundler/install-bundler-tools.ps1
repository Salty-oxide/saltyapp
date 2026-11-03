# Seeds Tauri's Windows bundler cache from files committed in this folder, so
# `tauri build` never has to download them (some networks block those downloads).
#
# Tauri looks in %LOCALAPPDATA%\tauri and downloads only what is missing:
#   NSIS\                      <- nsis.zip (nsis-3.11), for the setup .exe
#   NSIS\Plugins\x86-unicode\additional\nsis_tauri_utils.dll
#   WixTools314\               <- wix.zip (wix314-binaries), for the .msi
#
# Sources (refresh from these when Tauri pins newer versions):
#   https://github.com/tauri-apps/binary-releases/releases/download/nsis-3.11/nsis-3.11.zip
#   https://github.com/tauri-apps/nsis-tauri-utils/releases/download/nsis_tauri_utils-v0.5.3/nsis_tauri_utils.dll
#   https://github.com/wixtoolset/wix3/releases/download/wix3141rtm/wix314-binaries.zip
#
# Run once before `npm run build`:   pwsh build-support/windows-bundler/install-bundler-tools.ps1
# Add -Force to replace an existing cache.
param([switch]$Force)
$ErrorActionPreference = 'Stop'

$here = $PSScriptRoot
$cache = Join-Path $env:LOCALAPPDATA 'tauri'
New-Item -ItemType Directory -Force -Path $cache | Out-Null

# Refuse to install anything that does not match the recorded hashes.
foreach ($line in Get-Content (Join-Path $here 'SHA256SUMS')) {
  $expected, $name = $line -split '\s+', 2
  $actual = (Get-FileHash -Algorithm SHA256 (Join-Path $here $name.Trim('* '))).Hash
  if ($actual -ne $expected.ToUpper()) { throw "Hash mismatch for $name" }
}

$nsis = Join-Path $cache 'NSIS'
$wix = Join-Path $cache 'WixTools314'
if ($Force) { Remove-Item -Recurse -Force $nsis, $wix -ErrorAction SilentlyContinue }

if (-not (Test-Path $nsis)) {
  $tmp = Join-Path $cache 'nsis-extract'
  Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
  Expand-Archive (Join-Path $here 'nsis.zip') -DestinationPath $tmp
  Move-Item (Join-Path $tmp 'nsis-3.11') $nsis
  Remove-Item -Recurse -Force $tmp
}
$plugin = Join-Path $nsis 'Plugins\x86-unicode\additional'
New-Item -ItemType Directory -Force -Path $plugin | Out-Null
Copy-Item (Join-Path $here 'nsis_tauri_utils.dll') $plugin -Force

if (-not (Test-Path $wix)) {
  Expand-Archive (Join-Path $here 'wix.zip') -DestinationPath $wix
}
Write-Host "Bundler tools ready in $cache"
