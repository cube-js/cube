# Cube CLI installer for Windows (x86_64).
#
#   irm https://raw.githubusercontent.com/cube-js/cube/master/install-cli.ps1 | iex
#
# Environment overrides:
#   CUBE_INSTALL_DIR   install directory (default: %LOCALAPPDATA%\cube\bin)
#   CUBE_VERSION       release tag to install, e.g. v1.7.5 (default: latest)
$ErrorActionPreference = "Stop"

$Repo = "cube-js/cube"
$Target = "x86_64-pc-windows-msvc"

$Version = if ($env:CUBE_VERSION) { $env:CUBE_VERSION } else { "latest" }
$Url = if ($Version -eq "latest") {
    "https://github.com/$Repo/releases/latest/download/cube-$Target.tar.gz"
} else {
    "https://github.com/$Repo/releases/download/$Version/cube-$Target.tar.gz"
}

$Dir = if ($env:CUBE_INSTALL_DIR) { $env:CUBE_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "cube\bin" }
New-Item -ItemType Directory -Force -Path $Dir | Out-Null

$Tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())
New-Item -ItemType Directory -Force -Path $Tmp | Out-Null
try {
    Write-Host "Downloading cube ($Target) from $Url…"
    $Archive = Join-Path $Tmp "cube.tar.gz"
    Invoke-WebRequest -Uri $Url -OutFile $Archive -UseBasicParsing

    # tar ships with Windows 10 1803+.
    tar -xzf $Archive -C $Tmp

    # Run the new binary before it replaces an existing install.
    $Extracted = Join-Path $Tmp "cube.exe"
    $InstalledVersion = & $Extracted --version
    if ($LASTEXITCODE -ne 0) {
        throw "The downloaded cube.exe failed to run (exit code $LASTEXITCODE, 0x$('{0:X8}' -f $LASTEXITCODE)); $Dir was left unchanged."
    }

    $Exe = Join-Path $Dir "cube.exe"
    Copy-Item -Force $Extracted $Exe
    Write-Host "Installed $InstalledVersion to $Exe"

    $UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if (($UserPath -split ";") -notcontains $Dir) {
        [Environment]::SetEnvironmentVariable("Path", "$Dir;$UserPath", "User")
        Write-Host "Added $Dir to your user PATH (restart your terminal to pick it up)."
    }
} finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
