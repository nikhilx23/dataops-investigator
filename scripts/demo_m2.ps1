# Milestone 2 (lightweight): simulated month of daily operations, incremental dbt,
# full-refresh equivalence and reconciliation. -Reset empties the SYNTHETIC environment first.
param([int]$Seed = 42, [string]$DataProfile = 'small', [int]$DbtEvery = 5, [switch]$Reset)
. (Join-Path $PSScriptRoot '_env.ps1')
Push-Location $script:Root
try {
    $argv = @('scripts/demo_m2.py', '--seed', $Seed, '--profile', $DataProfile, '--dbt-every', $DbtEvery)
    if ($Reset) { $argv += '--reset' }
    python @argv
    exit $LASTEXITCODE
} finally { Pop-Location }
