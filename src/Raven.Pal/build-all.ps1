param (
    [switch]$zeroversion = $false,
    # rebuild at the current pal.ver, e.g. after fixing a toolchain that failed on the previous run
    [switch]$keepversion = $false
)

$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {

# The names DynamicNativeLibraryResolver loads and Raven.Server.csproj ships (libs/librvnpal).
# A partial set must never be copied: a binary left at the previous version fails RavenDB startup on its platform.
$Expected = @(
    'librvnpal.linux.x64.so', 'librvnpal.arm.32.so', 'librvnpal.arm.64.so',
    'librvnpal.mac.x64.dylib', 'librvnpal.mac.arm64.dylib',
    'librvnpal.win.x86.dll', 'librvnpal.win.x64.dll', 'librvnpal.win.arm64.dll',
    'librvnpal.win7.x86.dll', 'librvnpal.win7.x64.dll'
)

$PalVerStr = (Get-Content pal.ver)
[int]$PalVer = [convert]::ToInt32($PalVerStr, 10)

if ( $zeroversion -eq $true ) {
    $PalVer = 0
}
elseif ( $keepversion -eq $false ) {
    $PalVer++
    Set-Content pal.ver -Value $PalVer
}

$GenCode = "#include <sys/types.h>`n#include ""rvn.h""`nEXPORT int32_t rvn_get_pal_ver() { return  $($PalVer) ; }"
Set-Content src/rvngetpalver.c -Value $GenCode

# Sparrow.Server compares PAL_VER with rvn_get_pal_ver() at startup; version 0 skips that check, so PAL_VER stays as it is then
if ( $zeroversion -eq $false ) {
    $PalCs = Join-Path $PSScriptRoot '..\Sparrow.Server\Platform\Pal.cs'
    $Text = [IO.File]::ReadAllText($PalCs)
    $Pattern = 'public const int PAL_VER = \d+;'
    if ($Text -notmatch $Pattern) {
        throw "PAL_VER was not found in $PalCs"
    }
    [IO.File]::WriteAllText($PalCs, [regex]::Replace($Text, $Pattern, "public const int PAL_VER = $PalVer;"))
}

if (Test-Path artifacts) {
    Remove-Item -Recurse .\artifacts\
}

$Failed = @()

if (-not (Test-Path osxcross)) {
    wsl bash -c './build-all-posix.sh setup'
    if ($LASTEXITCODE -ne 0) { $Failed += "build-all-posix.sh setup (exit code $LASTEXITCODE)" }
}

wsl bash -c './build-all-posix.sh build'
if ($LASTEXITCODE -ne 0) { $Failed += "build-all-posix.sh build (exit code $LASTEXITCODE)" }

# .\ so it also runs where NoDefaultCurrentDirectoryInExePath is set
cmd.exe /c .\build-all-windows.bat
if ($LASTEXITCODE -ne 0) { $Failed += "build-all-windows.bat (exit code $LASTEXITCODE)" }

$Missing = @($Expected | Where-Object { -not (Test-Path (Join-Path artifacts $_)) })
$Unexpected = @(Get-ChildItem artifacts -File -ErrorAction SilentlyContinue | Where-Object { $Expected -notcontains $_.Name } | ForEach-Object Name)

if ($Failed.Count -gt 0 -or $Missing.Count -gt 0 -or $Unexpected.Count -gt 0) {
    if ($Failed.Count -gt 0) { Write-Host "Failed: $($Failed -join '; ')" }
    if ($Missing.Count -gt 0) { Write-Host "Not built: $($Missing -join ', ')" }
    if ($Unexpected.Count -gt 0) { Write-Host "Unexpected names (nothing loads them): $($Unexpected -join ', ')" }
    throw "PAL $PalVer was not built for every platform, nothing was copied to libs\librvnpal. Fix the build and rerun with -keepversion."
}

Copy-Item ($Expected | ForEach-Object { Join-Path artifacts $_ }) ..\..\libs\librvnpal\
Write-Host "PAL $PalVer built for all $($Expected.Count) platforms and copied to libs\librvnpal."
Write-Host "Commit libs\librvnpal, pal.ver, src\rvngetpalver.c and src\Sparrow.Server\Platform\Pal.cs together."
}
finally {
    Pop-Location
}
