#Requires -Version 5.1
<#
.SYNOPSIS
    Install pq from pq.eorlov.org.
.DESCRIPTION
    Downloads and installs the pq binary for Windows.
.PARAMETER Version
    Install specific version (e.g. v1.0.0). Default: latest.
.PARAMETER To
    Install to directory. Default: $env:LOCALAPPDATA\pq.
.EXAMPLE
    irm https://pq.eorlov.org/install.ps1 | iex
.EXAMPLE
    & ./install.ps1 -Version v1.0.0 -To C:\tools
#>
param(
    [string]$Version,
    [string]$To
)

$ErrorActionPreference = 'Stop'

$BaseUrl = "https://pq.eorlov.org"
$Binary = "pq"
$Target = "x86_64-pc-windows-msvc"

function Get-LatestVersion {
    $json = Invoke-RestMethod -Uri "$BaseUrl/dist/latest.json"
    return $json.version
}

function Install-Pq {
    if (-not $Version) {
        Write-Host "Fetching latest version..."
        $Version = Get-LatestVersion
    }

    if (-not $To) {
        $To = Join-Path $env:LOCALAPPDATA $Binary
    }

    $fileName = "$Binary-$Version-$Target.zip"
    $url = "$BaseUrl/dist/$Version/$fileName"
    $shaUrl = "$url.sha256"

    Write-Host "Installing $Binary $Version ($Target)..."
    Write-Host "  from: $url"
    Write-Host "  to:   $To"

    $tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())
    New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

    try {
        $zipPath = Join-Path $tmpDir $fileName

        try {
            Invoke-WebRequest -Uri $url -OutFile $zipPath -UseBasicParsing
        } catch {
            Write-Error "Failed to download $url`nCheck that version '$Version' exists."
            return
        }

        # Verify SHA256 checksum
        try {
            $expectedHash = (Invoke-RestMethod -Uri $shaUrl).Trim().Split()[0]
            $actualHash = (Get-FileHash -Path $zipPath -Algorithm SHA256).Hash.ToLower()
            if ($actualHash -ne $expectedHash) {
                Write-Error "Checksum mismatch`n  expected: $expectedHash`n  actual:   $actualHash"
                return
            }
            Write-Host "  checksum verified"
        } catch {
            Write-Warning "Could not verify checksum, skipping."
        }

        # Extract
        Expand-Archive -Path $zipPath -DestinationPath $tmpDir -Force

        # Install
        New-Item -ItemType Directory -Path $To -Force | Out-Null
        Copy-Item -Path (Join-Path $tmpDir "$Binary.exe") -Destination (Join-Path $To "$Binary.exe") -Force

        Write-Host ""
        Write-Host "$Binary $Version installed successfully!"

        # Add to PATH if needed
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        if ($userPath -notlike "*$To*") {
            [Environment]::SetEnvironmentVariable('Path', "$To;$userPath", 'User')
            $env:Path = "$To;$env:Path"
            Write-Host ""
            Write-Host "Added $To to your User PATH."
            Write-Host "Restart your terminal for PATH changes to take effect."
        }
    } finally {
        Remove-Item -Path $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Install-Pq
