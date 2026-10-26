# Start the Docker Compose stack and apply migrations.  Usage: scripts/up.ps1 [-StackProfile lightweight|full]
param([ValidateSet('lightweight', 'full')][string]$StackProfile = 'lightweight')
. (Join-Path $PSScriptRoot '_env.ps1')
Push-Location $script:Root
try {
    docker compose --profile $StackProfile up -d --build --wait postgres
    if ($LASTEXITCODE -ne 0) { throw "compose up failed" }
    docker compose --profile $StackProfile run --rm app db migrate
    if ($LASTEXITCODE -ne 0) { throw "migrations failed" }
    docker compose --profile $StackProfile run --rm app health
} finally { Pop-Location }
