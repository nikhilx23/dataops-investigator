# Milestone 1 vertical slice: generate -> load source -> batch ingest -> dbt -> reconcile.
# Default runs in the local virtualenv; -Docker runs every step in the Compose app container.
param([int]$Seed = 42, [ValidateSet('tiny', 'small', 'medium', 'large')][string]$DataProfile = 'small',
      [switch]$Docker)
. (Join-Path $PSScriptRoot '_env.ps1')
Push-Location $script:Root

# ConvertFrom-Json turns ISO-8601 strings into local DateTime values; convert them back to UTC ISO.
function ConvertTo-UtcIso($value) {
    if ($value -is [datetime]) { return $value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssK") }
    return [string]$value
}

function Invoke-DataOps {
    if ($Docker) { $out = docker compose --profile lightweight run --rm -T app --log-level WARNING @args }
    else { $out = python -m dataops --log-level WARNING @args }
    if ($LASTEXITCODE -ne 0) { Write-Host ($out -join "`n"); throw "dataops $($args -join ' ') failed" }
    return ($out -join "`n") | ConvertFrom-Json
}

try {
    Write-Host "== 1/5 generate synthetic data (seed=$Seed profile=$DataProfile) and load source"
    $gen = Invoke-DataOps generate --seed $Seed --profile $DataProfile --load --replace
    $gen | Select-Object simulation_start, simulation_end, dataset_checksum | Format-List
    $start = ConvertTo-UtcIso $gen.simulation_start; $end = ConvertTo-UtcIso $gen.simulation_end
    $ledger = "var/ledger/$DataProfile-seed$Seed.json"

    Write-Host "== 2/5 catch-up ingestion of history before $start"
    $r = Invoke-DataOps ingest --until $start
    Write-Host "batches=$($r.batches) failed=$($r.failed) inserted=$($r.inserted_rows) duplicates_skipped=$($r.duplicate_rows)"

    Write-Host "== 3/5 daily incremental windows $start .. $end"
    $r = Invoke-DataOps ingest --start $start --end $end --step 1d
    Write-Host "batches=$($r.batches) failed=$($r.failed) inserted=$($r.inserted_rows) duplicates_skipped=$($r.duplicate_rows)"

    Write-Host "== 4/5 dbt build (staging views, revenue mart, tests)"
    if ($Docker) { docker compose --profile lightweight run --rm -T --entrypoint dbt app build --project-dir dbt --profiles-dir dbt }
    else { dbt build --project-dir dbt --profiles-dir dbt }
    if ($LASTEXITCODE -ne 0) { throw "dbt build failed" }

    Write-Host "== 5/5 reconciliation (source vs raw counts, source vs mart, ledger vs mart)"
    $rec = Invoke-DataOps reconcile --ledger $ledger
    $rec.results | ForEach-Object { "{0,-40} {1,-5} rows={2} mismatches={3}" -f $_.check_id, $_.status, $_.compared_rows, $_.mismatched_rows }
} finally { Pop-Location }
