#!/usr/bin/env bash
# Run Airflow 3.3.2 for the full profile, from its own virtualenv (var/airflow-venv).
#   scripts/airflow.sh install          create the venv with the official constraints
#   scripts/airflow.sh standalone       api-server (127.0.0.1:8080) + scheduler + dag-processor + triggerer
#   scripts/airflow.sh <airflow args>   any airflow CLI command, e.g. dags list-import-errors
# Metadata lives in the `airflow` database on the Compose PostgreSQL (role airflow).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_env.sh"
VENV="$ROOT/var/airflow-venv"
if [ "${1:-}" = "install" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install "apache-airflow[postgres]==3.3.2" \
    -c "$ROOT/orchestration/constraints-airflow-3.3.2-py3.13.txt"
  exit 0
fi
PG_PORT="${DATAOPS_AIRFLOW_PG_PORT:-5433}"
export AIRFLOW_HOME="$ROOT/var/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/orchestration/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=4
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:${DATAOPS_AIRFLOW_DB_PASSWORD}@${DATAOPS_PG_HOST:-127.0.0.1}:${PG_PORT}/${DATAOPS_AIRFLOW_DB:-airflow}"
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__API__PORT=8080
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=30
# DAG tasks call the project CLI in the project virtualenv against the same databases.
export DATAOPS_PROJECT_DIR="$ROOT"
export DATAOPS_PYTHON="$ROOT/.venv/bin/python"
export DATAOPS_DBT="$ROOT/.venv/bin/dbt"
export DATAOPS_PG_PORT="$PG_PORT"
export DATAOPS_SOURCE_PATH="${DATAOPS_SOURCE_PATH:-cdc}"
# standalone spawns its components as `airflow ...`, so the venv must be on PATH.
export PATH="$VENV/bin:$PATH"
exec "$VENV/bin/airflow" "$@"
