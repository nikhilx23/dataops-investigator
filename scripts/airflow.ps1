# Run Airflow 3.3.2 from var/airflow-venv (Windows: Airflow itself is supported on Linux/WSL2;
# this wrapper is for WSL2 or Linux PowerShell). Usage: scripts/airflow.ps1 standalone
. (Join-Path $PSScriptRoot '_full_env.ps1')
$venv = Join-Path $script:Root 'var/airflow-venv'
$env:AIRFLOW_HOME = Join-Path $script:Root 'var/airflow'
$env:AIRFLOW__CORE__DAGS_FOLDER = Join-Path $script:Root 'orchestration/dags'
$env:AIRFLOW__CORE__LOAD_EXAMPLES = 'False'
$env:AIRFLOW__CORE__EXECUTOR = 'LocalExecutor'
$env:AIRFLOW__CORE__PARALLELISM = '4'
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "postgresql+psycopg2://airflow:$($env:DATAOPS_AIRFLOW_DB_PASSWORD)@$($env:DATAOPS_PG_HOST):$($env:DATAOPS_PG_PORT)/airflow"
$env:AIRFLOW__API__HOST = '127.0.0.1'; $env:AIRFLOW__API__PORT = '8080'
$env:DATAOPS_PROJECT_DIR = $script:Root
$env:DATAOPS_PYTHON = Join-Path $script:Root '.venv/bin/python'
$env:DATAOPS_DBT = Join-Path $script:Root '.venv/bin/dbt'
if (-not $env:DATAOPS_SOURCE_PATH) { $env:DATAOPS_SOURCE_PATH = 'cdc' }
$env:PATH = "$(Join-Path $venv 'bin')$([IO.Path]::PathSeparator)$env:PATH"
& (Join-Path $venv 'bin/airflow') @args
exit $LASTEXITCODE
