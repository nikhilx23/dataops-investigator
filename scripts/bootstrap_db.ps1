# Create databases, roles and grants (idempotent), then apply migrations. Requires psql >= 15.
. (Join-Path $PSScriptRoot '_env.ps1')
Write-Host "Bootstrapping databases and roles on $($env:DATAOPS_PG_HOST):$($env:DATAOPS_PG_PORT) ..."
$env:PGPASSWORD = $env:DATAOPS_PG_ADMIN_PASSWORD
try {
    & psql -X -q -h $env:DATAOPS_PG_HOST -p $env:DATAOPS_PG_PORT -U $env:DATAOPS_PG_ADMIN_USER -d postgres `
        -f (Join-Path $script:Root 'infra/postgres/bootstrap.sql')
    if ($LASTEXITCODE -ne 0) { throw "bootstrap.sql failed" }
} finally { Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue }
python -m dataops db migrate
if ($LASTEXITCODE -ne 0) { throw "migrations failed" }
Write-Host "Bootstrap complete."
