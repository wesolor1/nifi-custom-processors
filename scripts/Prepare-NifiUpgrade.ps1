<#
.SYNOPSIS
    Bump custom processor POMs to a target NiFi version, rebuild NARs, and copy them into a NiFi lib directory.

.DESCRIPTION
    Custom NARs must be rebuilt against the NiFi version they will run on. Parent NAR coordinates
    (nifi-standard-nar, nifi-aws-nar, nifi-smb-nar, nifi-azure-nar, ...) are embedded at build time.
    Copying 2.9.0 custom NARs into a 2.12.0 lib directory will fail classloading because those parent
    NARs no longer exist at the old version.

    Do NOT copy the whole old lib/ into the new NiFi. The new zip already contains Apache NARs.
    This script only produces the *-extended-* / websocket custom NARs.

.PARAMETER NifiVersion
    Target Apache NiFi version, e.g. 2.12.0

.PARAMETER NifiApiVersion
    org.apache.nifi:nifi-api version. Often one minor behind NiFi (2.9.0 used 2.8.0).
    Check https://repo.maven.apache.org/maven2/org/apache/nifi/nifi-api/ and the NiFi release notes.

.PARAMETER FromVersion
    Extra literal versions to rewrite in pom.xml (project/parent/dependency <version> tags).
    Defaults include the current repo literals 2.9.0 and 2.3.0 (websocket bundles).

.PARAMETER NifiHome
    Optional unpacked NiFi directory. When set, collected NARs are copied to lib or extensions.

.PARAMETER NarDestination
    Subfolder under NifiHome: lib (same folder as Apache NARs) or extensions (autoload). Default: lib.

.PARAMETER SkipBump
    Do not rewrite pom.xml files.

.PARAMETER SkipBuild
    Do not run Maven. Collect whatever NARs already exist under *-nar/target.

.PARAMETER SkipTests
    Pass -DskipTests to Maven.

.PARAMETER DryRun
    Show POM replacements without writing files.

.EXAMPLE
    .\scripts\Prepare-NifiUpgrade.ps1 -NifiVersion 2.12.0 -NifiApiVersion 2.12.0 -DryRun

.EXAMPLE
    .\scripts\Prepare-NifiUpgrade.ps1 -NifiVersion 2.12.0 -NifiApiVersion 2.12.0 -SkipTests `
        -NifiHome 'D:\nifi-2.12.0'

.EXAMPLE
    .\scripts\Prepare-NifiUpgrade.ps1 -NifiVersion 2.12.0 -SkipBump -SkipBuild `
        -StageForAnsible 'C:\Users\RWESOLO\Projects\nifibnl-deployment\files\custom-nars'
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d+\.\d+\.\d+(-.*)?$')]
    [string] $NifiVersion,

    [string] $NifiApiVersion,

    [string[]] $FromVersion = @('2.9.0', '2.3.0'),

    [string] $NifiHome,

    [ValidateSet('lib', 'extensions')]
    [string] $NarDestination = 'lib',

    [string] $OutputDirectory,

    [string] $StageForAnsible,

    [switch] $SkipBump,
    [switch] $SkipBuild,
    [switch] $SkipTests,
    [switch] $DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($NifiApiVersion)) {
    $NifiApiVersion = $NifiVersion
}
if ($NifiApiVersion -notmatch '^\d+\.\d+\.\d+(-.*)?$') {
    throw "NifiApiVersion '$NifiApiVersion' is not a valid version."
}
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $repoRoot 'dist\nars'
}

$utf8NoBom = New-Object System.Text.UTF8Encoding $false

$expectedNarArtifacts = @(
    'nifi-listsftp-extended-nar',
    'nifi-fetchsftp-extended-nar',
    'nifi-putsftp-extended-nar',
    'nifi-deletesftp-extended-nar',
    'nifi-s3-extended-nar',
    'nifi-putsmbfile-extended-nar',
    'nifi-smb-extended-nar',
    'nifi-azure-datalake-extended-nar',
    'nifi-finxact-websocket-nar',
    'nifi-websocket-listener-nar'
)

function Get-PomFiles {
    Get-ChildItem -Path $repoRoot -Filter 'pom.xml' -Recurse -File |
        Where-Object { $_.FullName -notmatch '[\\/]target[\\/]' }
}

function Update-NifiPoms {
    param(
        [string] $TargetVersion,
        [string] $ApiVersion,
        [string[]] $OldVersions,
        [switch] $WhatIf
    )

    $uniqueOld = @($OldVersions | Where-Object { $_ -and $_ -ne $TargetVersion } | Select-Object -Unique)
    $changed = 0

    foreach ($pom in Get-PomFiles) {
        $original = [System.IO.File]::ReadAllText($pom.FullName)
        $updated = $original

        $updated = [regex]::Replace(
            $updated,
            '<nifi\.version>[^<]+</nifi\.version>',
            "<nifi.version>$TargetVersion</nifi.version>"
        )
        $updated = [regex]::Replace(
            $updated,
            '<nifi\.api\.version>[^<]+</nifi\.api\.version>',
            "<nifi.api.version>$ApiVersion</nifi.api.version>"
        )
        $updated = [regex]::Replace(
            $updated,
            '(<artifactId>nifi-extension-bundles</artifactId>\s*<version>)[^<]+(</version>)',
            "`${1}$TargetVersion`${2}"
        )

        foreach ($old in $uniqueOld) {
            $escaped = [regex]::Escape($old)
            $updated = [regex]::Replace($updated, "<version>$escaped</version>", "<version>$TargetVersion</version>")
        }

        if ($updated -eq $original) {
            continue
        }

        $rel = $pom.FullName.Substring($repoRoot.Length).TrimStart('\', '/')
        if ($WhatIf) {
            Write-Host "[dry-run] would update $rel"
        }
        else {
            [System.IO.File]::WriteAllText($pom.FullName, $updated, $utf8NoBom)
            Write-Host "Updated $rel"
        }
        $changed++
    }

    Write-Host "POM files touched: $changed"
}

function Invoke-NifiBuild {
    $mvn = Get-Command mvn -ErrorAction SilentlyContinue
    if (-not $mvn) {
        throw 'Maven (mvn) is not on PATH. Install Maven 3.9+ and Java 21, then retry.'
    }

    $mvnArgs = @('-f', (Join-Path $repoRoot 'pom.xml'), 'clean', 'package')
    if ($SkipTests) {
        $mvnArgs += '-DskipTests'
    }

    Write-Host "Running: mvn $($mvnArgs -join ' ')"
    Push-Location $repoRoot
    try {
        & mvn @mvnArgs
        if ($LASTEXITCODE -ne 0) {
            throw "Maven failed with exit code $LASTEXITCODE. Fix compile/test errors before copying NARs into NiFi $NifiVersion."
        }
    }
    finally {
        Pop-Location
    }
}

function Export-BuiltNars {
    if (Test-Path -LiteralPath $OutputDirectory) {
        Remove-Item -LiteralPath $OutputDirectory -Recurse -Force
    }
    New-Item -ItemType Directory -Path $OutputDirectory | Out-Null

    $nars = @(Get-ChildItem -Path $repoRoot -Filter '*.nar' -Recurse -File |
        Where-Object {
            $_.FullName -match '[\\/]nifi-[^\\/]+-nar[\\/]target[\\/]' -and
            $_.Name -notlike 'original-*'
        })

    if ($nars.Count -eq 0) {
        throw "No NAR files found under *-nar/target. Build first (omit -SkipBuild)."
    }

    foreach ($nar in $nars) {
        Copy-Item -LiteralPath $nar.FullName -Destination (Join-Path $OutputDirectory $nar.Name) -Force
        Write-Host "Collected $($nar.Name)"
    }

    $missing = @()
    foreach ($artifact in $expectedNarArtifacts) {
        $match = @(Get-ChildItem -Path $OutputDirectory -Filter "$artifact-*.nar" -ErrorAction SilentlyContinue)
        if ($match.Count -eq 0) {
            $missing += $artifact
        }
    }
    if ($missing.Count -gt 0) {
        throw "Missing NAR artifacts: $($missing -join ', ')"
    }

    Write-Host "Collected $($nars.Count) NAR(s) into $OutputDirectory"
    return @(Get-ChildItem -Path $OutputDirectory -Filter '*.nar' -File)
}

function Copy-NarsToDestination {
    param(
        [System.IO.FileInfo[]] $Nars,
        [string] $DestinationDir
    )

    if (-not (Test-Path -LiteralPath $DestinationDir)) {
        New-Item -ItemType Directory -Path $DestinationDir | Out-Null
    }

    foreach ($nar in $Nars) {
        $dest = Join-Path $DestinationDir $nar.Name
        Copy-Item -LiteralPath $nar.FullName -Destination $dest -Force
        Write-Host "Copied $($nar.Name) -> $dest"
    }
}

Write-Host "NiFi target version : $NifiVersion"
Write-Host "nifi-api version    : $NifiApiVersion"
Write-Host "Replace literals    : $($FromVersion -join ', ')"

if (-not $SkipBump) {
    Update-NifiPoms -TargetVersion $NifiVersion -ApiVersion $NifiApiVersion -OldVersions $FromVersion -WhatIf:$DryRun
    if ($DryRun) {
        Write-Host 'Dry-run complete. Re-run without -DryRun to write POM changes.'
        return
    }
}

if ($DryRun -and $SkipBump) {
    Write-Host 'Nothing to do in dry-run without POM bump.'
    return
}

if (-not $SkipBuild) {
    Invoke-NifiBuild
}

$collected = Export-BuiltNars

if ($NifiHome) {
    $dest = Join-Path $NifiHome $NarDestination
    if (-not (Test-Path -LiteralPath $NifiHome)) {
        throw "NifiHome does not exist: $NifiHome"
    }
    Copy-NarsToDestination -Nars $collected -DestinationDir $dest
}

if ($StageForAnsible) {
    Copy-NarsToDestination -Nars $collected -DestinationDir $StageForAnsible
    Write-Host "Staged for rolling upgrade. Ansible copies these into nifi-$NifiVersion/$NarDestination"
}
