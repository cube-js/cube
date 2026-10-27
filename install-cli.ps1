# Cube CLI installer for Windows (x86_64).
#
#   irm https://raw.githubusercontent.com/cube-js/cube/master/install-cli.ps1 | iex
#
# Environment overrides:
#   CUBE_INSTALL_DIR   install directory (default: %LOCALAPPDATA%\cube\bin)
#   CUBE_VERSION       release tag to install, e.g. v1.7.5 (default: the newest
#                      release that ships a Windows binary)
#   CUBE_RELEASES_URL  GitHub releases API endpoint to pick the version from
#                      (default: the cube-js/cube releases, used by tests)
#   GITHUB_TOKEN       sent to api.github.com only, to avoid its
#                      unauthenticated rate limit on shared CI runners
$ErrorActionPreference = "Stop"

$Repo = "cube-js/cube"
$Target = "x86_64-pc-windows-msvc"

$Asset = "cube-$Target.tar.gz"
$ReleasesUrl = if ($env:CUBE_RELEASES_URL) { $env:CUBE_RELEASES_URL } else { "https://api.github.com/repos/$Repo/releases?per_page=30" }
# How many releases to try before giving up when downloads keep failing.
$MaxAttempts = 3

# The download URL of our asset for every stable release that lists it,
# highest version first. Releases from older maintenance lines are interleaved
# by publish date, so order by version.
function Get-ReleaseUrls {
    $Headers = @{}
    # Only GitHub gets the token, never a CUBE_RELEASES_URL mirror.
    if ($env:GITHUB_TOKEN -and $ReleasesUrl -like 'https://api.github.com/*') { $Headers.Authorization = "Bearer $env:GITHUB_TOKEN" }
    # Assign before piping: Windows PowerShell 5.1 would otherwise pass the
    # whole JSON array down the pipeline as a single object.
    $Releases = Invoke-RestMethod -Uri $ReleasesUrl -Headers $Headers -UseBasicParsing
    $Releases | Where-Object { -not $_.prerelease -and -not $_.draft } | ForEach-Object {
        $Version = $null
        $Match = $_.assets | Where-Object { $_.name -eq $Asset } | Select-Object -First 1
        if ($Match -and [version]::TryParse(($_.tag_name -replace '^v', ''), [ref]$Version)) {
            [pscustomobject]@{ Version = $Version; Url = $Match.browser_download_url }
        }
    } | Sort-Object Version -Descending | Select-Object -First $MaxAttempts -ExpandProperty Url
}

$Urls = if ($env:CUBE_VERSION -and $env:CUBE_VERSION -ne "latest") {
    @("https://github.com/$Repo/releases/download/$($env:CUBE_VERSION)/$Asset")
} else {
    # Not releases/latest: its binaries upload after publishing and can be
    # missing (v1.7.14).
    $Found = @(try { Get-ReleaseUrls } catch { })
    if ($Found.Count -eq 0) {
        Write-Warning "could not list releases, falling back to the latest one"
        $Found = @("https://github.com/$Repo/releases/latest/download/$Asset")
    }
    $Found
}

$Dir = if ($env:CUBE_INSTALL_DIR) { $env:CUBE_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "cube\bin" }
New-Item -ItemType Directory -Force -Path $Dir | Out-Null

$Tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())
New-Item -ItemType Directory -Force -Path $Tmp | Out-Null
try {
    $Archive = Join-Path $Tmp "cube.tar.gz"
    $Downloaded = $false
    foreach ($Url in $Urls) {
        Write-Host "Downloading cube ($Target) from $Url…"
        try {
            Invoke-WebRequest -Uri $Url -OutFile $Archive -UseBasicParsing
            $Downloaded = $true
            break
        } catch {
            Write-Warning "download failed, trying an older release"
        }
    }
    if (-not $Downloaded) { throw "could not download $Asset" }

    # tar ships with Windows 10 1803+.
    tar -xzf $Archive -C $Tmp
    Copy-Item -Force (Join-Path $Tmp "cube.exe") (Join-Path $Dir "cube.exe")

    $Exe = Join-Path $Dir "cube.exe"
    Write-Host "Installed $(& $Exe --version) to $Exe"

    $UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if (($UserPath -split ";") -notcontains $Dir) {
        [Environment]::SetEnvironmentVariable("Path", "$Dir;$UserPath", "User")
        Write-Host "Added $Dir to your user PATH (restart your terminal to pick it up)."
    }
} finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
