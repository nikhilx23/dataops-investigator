# Full profile: start PostgreSQL (logical WAL), Kafka, Debezium Connect; migrate; register CDC.
. (Join-Path $PSScriptRoot '_full_env.ps1')
Push-Location $script:Root
try {
    Invoke-Checked { docker compose --profile full up -d --wait postgres kafka connect } 'compose up'
    Invoke-Checked { python -m dataops db migrate } 'migrate'
    Invoke-Checked { python -m dataops health } 'health'
    Invoke-Checked { python -m dataops cdc register } 'cdc register'
} finally { Pop-Location }
