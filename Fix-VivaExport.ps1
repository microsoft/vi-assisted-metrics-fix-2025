<#!
.SYNOPSIS
Applies Microsoft’s workaround to Viva Insights exports affected by the September–October 2025 Copilot meeting-hours underreporting issue.

.DESCRIPTION
Reads a Viva Insights advanced reporting CSV, computes the August multiplier (from 2025-07-27 through 2025-08-30 by default), and rewrites the affected September–October 2025 rows so that “Total meeting hours summarized or recapped by Copilot”, “Copilot assisted hours”, and “Copilot assisted value” reflect the corrected values. All other columns pass through untouched. The script mirrors the fix_viva_export.py CLI exactly and delegates execution to the Python implementation after ensuring Python (and required modules) are present.

.PARAMETER Input
Path to the Viva Insights CSV export file.

.PARAMETER Output
Optional destination path for the corrected CSV. Defaults to <input>_corrected_<timestamp>.csv in the input directory.

.PARAMETER Granularity
Aggregation mode. Accepts Weekly (default, Sunday–Saturday weeks), Monthly, or Daily.

.PARAMETER SourceStart
Reference window start date (ISO). Default: 2025-07-27.

.PARAMETER SourceEnd
Reference window end date (ISO). Default: 2025-08-30.

.PARAMETER TargetStart
Affected window start date (ISO). Default: 2025-08-31.

.PARAMETER TargetEnd
Affected window end date (ISO). Default: 2025-11-01.

.PARAMETER Rate
Hourly rate used to recompute Copilot assisted value. Default: 72.

.PARAMETER Overwrite
Overwrite the destination file without prompting.

.PARAMETER Quiet
Suppress summary output.

.PARAMETER Test
Runs the validation workflow instead of applying corrections. Requires -Original and -Corrected.

.PARAMETER Original
Path to the unmodified Viva Insights export when using -Test.

.PARAMETER Corrected
Path to the corrected Viva Insights export when using -Test.

.PARAMETER Tolerance
Maximum allowed delta for validation comparisons. Default: 1e-6.

.PARAMETER Help
Displays usage details identical to the Python script and exits.

.EXAMPLE
PS> .\Fix-VivaExport.ps1 -Input viva_export.csv

.EXAMPLE
PS> .\Fix-VivaExport.ps1 -Input viva_export.csv -Granularity monthly -Overwrite
#>

[CmdletBinding(DefaultParameterSetName = 'Run')]
param(
    [Parameter(Mandatory = $false, ParameterSetName = 'Run')]
    [Alias('Input')]
    [string]$InputPath,

    [Parameter(ParameterSetName = 'Run')]
    [string]$Output,

    [Parameter(ParameterSetName = 'Run')]
    [ValidateSet('weekly', 'monthly', 'daily')]
    [string]$Granularity = 'weekly',

    [Parameter(ParameterSetName = 'Run')]
    [string]$SourceStart,
    [Parameter(ParameterSetName = 'Run')]
    [string]$SourceEnd,
    [Parameter(ParameterSetName = 'Run')]
    [string]$TargetStart,
    [Parameter(ParameterSetName = 'Run')]
    [string]$TargetEnd,

    [Parameter(ParameterSetName = 'Run')]
    [double]$Rate = 72,

    [Parameter(ParameterSetName = 'Run')]
    [switch]$Overwrite,

    [Parameter(ParameterSetName = 'Run')]
    [switch]$Quiet,

    [Parameter(ParameterSetName = 'Run')]
    [switch]$Test,

    [Parameter(ParameterSetName = 'Run')]
    [string]$Original,

    [Parameter(ParameterSetName = 'Run')]
    [string]$Corrected,

    [Parameter(ParameterSetName = 'Run')]
    [double]$Tolerance = 1e-6,

    [Parameter(Mandatory = $true, ParameterSetName = 'Help')]
    [switch]$Help,

    [Parameter(ParameterSetName = 'Run')]
    [switch]$AcceptPartial
)

$helpText = @"
Usage:
    python fix_viva_export.py --input INPUT.csv [--output OUTPUT.csv] [options]

Summary:
  Applies Microsoft’s workaround for the September–October 2025 underreporting
  of “Total meeting hours summarized or recapped by Copilot” (and related
  assisted-hours metrics) in Viva Insights exports. The script keeps the original
  CSV structure intact—only the affected metric values are adjusted.

Defaults:
  Granularity    : weekly (Sunday–Saturday weeks)
  Source window  : 2025-07-27 to 2025-08-30 (reference weeks used to compute
                    the multiplier)
  Target window  : 2025-08-31 to 2025-11-01 (affected weeks to be corrected)
  Hourly rate    : 72 USD (used to recompute Copilot assisted value)

Options:
    --granularity {weekly,monthly,daily}
  --source-start YYYY-MM-DD
  --source-end   YYYY-MM-DD
  --target-start YYYY-MM-DD
  --target-end   YYYY-MM-DD
    --rate FLOAT
    --overwrite
    --quiet
    --accept-partial
    --test (requires --original, --corrected)
    --original PATH
    --corrected PATH
    --tolerance FLOAT
    --help

Important:
    "Copilot assisted hours" remains an estimate, because Microsoft's official
    workaround extrapolates meeting hours from the August 2025 reference window
    (or whichever window you override via --source-start/--source-end). All
    non-impacted rows and columns flow through unchanged. Columns are never
    renamed, removed, or reordered.
"@

if ($PSCmdlet.ParameterSetName -eq 'Help') {
    Write-Host $helpText
    return
}

$usingOriginal = $PSBoundParameters.ContainsKey('Original')
$usingCorrected = $PSBoundParameters.ContainsKey('Corrected')

if ($Test) {
    if (-not $usingOriginal -or -not $usingCorrected) {
        Write-Host "Error: -Test requires both -Original and -Corrected paths." -ForegroundColor Red
        return
    }
} else {
    if ($usingOriginal -or $usingCorrected) {
        Write-Host "Error: -Original and -Corrected can only be used together with -Test." -ForegroundColor Red
        return
    }
    if (-not $PSBoundParameters.ContainsKey('InputPath') -or [string]::IsNullOrWhiteSpace($InputPath)) {
        Write-Host "Error: -Input is required unless -Test is specified." -ForegroundColor Red
        return
    }
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvRoot = Join-Path $scriptDir '.venv'
$venvScripts = Join-Path $venvRoot 'Scripts'
$venvPython = Join-Path $venvScripts 'python.exe'

$defaultSourceStart = '2025-07-27'
$defaultSourceEnd = '2025-08-30'
$defaultTargetStart = '2025-08-31'
$defaultTargetEnd = '2025-11-01'

$hasSourceStart = $PSBoundParameters.ContainsKey('SourceStart')
$hasSourceEnd = $PSBoundParameters.ContainsKey('SourceEnd')

if ($hasSourceStart -xor $hasSourceEnd) {
    Write-Host "Error: --source-start and --source-end must be supplied together." -ForegroundColor Red
    exit 1
}

if (-not $hasSourceStart) {
    $SourceStart = $defaultSourceStart
    $SourceEnd = $defaultSourceEnd
}

if (-not $PSBoundParameters.ContainsKey('TargetStart')) {
    $TargetStart = $defaultTargetStart
}
if (-not $PSBoundParameters.ContainsKey('TargetEnd')) {
    $TargetEnd = $defaultTargetEnd
}

$culture = [System.Globalization.CultureInfo]::InvariantCulture
try {
    $sourceStartDate = [DateTime]::ParseExact($SourceStart, 'yyyy-MM-dd', $culture)
    $sourceEndDate = [DateTime]::ParseExact($SourceEnd, 'yyyy-MM-dd', $culture)
} catch {
    Write-Host "Error: Unable to parse source window dates. Use ISO format YYYY-MM-DD." -ForegroundColor Red
    exit 1
}

if ($sourceEndDate -lt $sourceStartDate) {
    Write-Host "Error: Source window end date ($SourceEnd) cannot be earlier than start date ($SourceStart)." -ForegroundColor Red
    exit 1
}

function Get-PythonLauncher {
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        return @{ Path = $python.Source; PreArgs = @() }
    }

    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        return @{ Path = $py.Source; PreArgs = @('-3') }
    }

    return $null
}

function Get-PreferredPythonLauncher {
    param()

    if (Test-Path $venvPython) {
        return @{ Path = (Resolve-Path -LiteralPath $venvPython).Path; PreArgs = @() }
    }

    return Get-PythonLauncher
}

function Install-PythonIfNeeded {
    $launcher = Get-PreferredPythonLauncher
    if ($launcher) {
        return $launcher
    }

    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        throw "Python is not installed and winget is unavailable. Please install Python 3.11 or later manually."
    }

    Write-Host "Python not detected. Attempting installation via winget..."
    $null = winget install -e --id Python.Python.3.11 --accept-package-agreements --accept-source-agreements

    $launcher = Get-PreferredPythonLauncher
    if (-not $launcher) {
        throw "Python installation did not complete successfully."
    }
    return $launcher
}

$pythonLauncher = Install-PythonIfNeeded
$pythonPath = $pythonLauncher.Path
$pythonPreArgs = $pythonLauncher.PreArgs

$pythonScript = Join-Path $scriptDir 'fix_viva_export.py'
if (-not (Test-Path $pythonScript)) {
    throw "Python script not found at expected location: $pythonScript"
}

$arguments = @($pythonScript)

if ($Test) {
    try {
        $resolvedOriginal = (Resolve-Path -LiteralPath $Original -ErrorAction Stop).Path
    } catch {
        $resolvedOriginal = $Original
    }

    try {
        $resolvedCorrected = (Resolve-Path -LiteralPath $Corrected -ErrorAction Stop).Path
    } catch {
        $resolvedCorrected = $Corrected
    }

    $toleranceString = $Tolerance.ToString([System.Globalization.CultureInfo]::InvariantCulture)

    $arguments += @(
        '--test',
        '--original', $resolvedOriginal,
        '--corrected', $resolvedCorrected,
        '--granularity', $Granularity,
        '--source-start', $SourceStart,
        '--source-end', $SourceEnd,
        '--target-start', $TargetStart,
        '--target-end', $TargetEnd,
        '--tolerance', $toleranceString
    )

    if ($Quiet) {
        $arguments += '--quiet'
    }
} else {
    $resolvedInput = (Resolve-Path -LiteralPath $InputPath).Path
    $rateString = $Rate.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    $toleranceString = $Tolerance.ToString([System.Globalization.CultureInfo]::InvariantCulture)

    $arguments += @(
        '--input', $resolvedInput,
        '--granularity', $Granularity,
        '--source-start', $SourceStart,
        '--source-end', $SourceEnd,
        '--target-start', $TargetStart,
        '--target-end', $TargetEnd,
        '--rate', $rateString,
        '--tolerance', $toleranceString
    )

    if ($PSBoundParameters.ContainsKey('Output') -and $Output) {
        try {
            $resolvedOutput = (Resolve-Path -Path $Output -ErrorAction Stop).Path
        } catch {
            $resolvedOutput = $Output
        }
        $arguments += @('--output', $resolvedOutput)
    }
    if ($Overwrite) {
        $arguments += '--overwrite'
    }
    if ($Quiet) {
        $arguments += '--quiet'
    }
    if ($AcceptPartial) {
        $arguments += '--accept-partial'
    }
}

$fullArgs = @($pythonPreArgs + $arguments) | Where-Object { $_ -ne $null }

$LASTEXITCODE = 0
& $pythonPath @fullArgs
$exitCode = $LASTEXITCODE
exit $exitCode
