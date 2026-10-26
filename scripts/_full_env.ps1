# Dot-sourced: full-profile environment on top of _env.ps1 (CLI targets the Compose PostgreSQL).
. (Join-Path $PSScriptRoot '_env.ps1')
$env:DATAOPS_PG_PORT = if ($env:DATAOPS_PG_HOST_PORT) { $env:DATAOPS_PG_HOST_PORT } else { '5433' }
function Invoke-Checked { param([scriptblock]$Block, [string]$What)
    & $Block; if ($LASTEXITCODE -ne 0) { throw "$What failed (exit $LASTEXITCODE)" } }
