# Full-profile demonstration (see demo_full.sh for what each step proves).
param([string]$Until = '2026-03-05T00:00:00+00:00')
. (Join-Path $PSScriptRoot '_full_env.ps1')
Push-Location $script:Root
try {
    Invoke-Checked { python -m dataops --log-level WARNING simulate --until $Until | Out-Null } 'simulate'
    Invoke-Checked { python scripts/verify_cdc.py } 'verify_cdc'
    Invoke-Checked { python scripts/verify_streaming.py } 'verify_streaming'
    Invoke-Checked { python -m dataops --log-level WARNING publish } 'publish'
    Invoke-Checked { dbt build --project-dir dbt --profiles-dir dbt --vars '{source_path: cdc}' } 'dbt build'
    Invoke-Checked { python -m dataops --log-level WARNING reconcile --source-path cdc } 'reconcile'
} finally { Pop-Location }
