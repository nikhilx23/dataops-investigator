# Readiness check for every database role. -Docker runs it in the app container.
param([switch]$Docker)
. (Join-Path $PSScriptRoot '_env.ps1')
Push-Location $script:Root
try {
    if ($Docker) { docker compose --profile lightweight run --rm app health }
    else { python -m dataops health }
    exit $LASTEXITCODE
} finally { Pop-Location }
