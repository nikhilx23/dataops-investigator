#!/usr/bin/env bash
# Full-profile demonstration: simulate source days, verify CDC, run the lakehouse jobs,
# publish Silver, build dbt on the CDC path and reconcile. Requires scripts/full_up.sh.
# Uses (and advances) the synthetic source; run `python -m dataops simulate --reset` first
# for a clean start (destructive for the synthetic demo source only).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_env.sh"
cd "$ROOT"
export DATAOPS_PG_PORT="${DATAOPS_PG_HOST_PORT:-5433}"
UNTIL="${DATAOPS_DEMO_UNTIL:-2026-03-05T00:00:00+00:00}"
python -m dataops --log-level WARNING simulate --until "$UNTIL" >/dev/null
python scripts/verify_cdc.py
python scripts/verify_streaming.py
python -m dataops --log-level WARNING publish
dbt build --project-dir dbt --profiles-dir dbt --vars '{source_path: cdc}'
python -m dataops --log-level WARNING reconcile --source-path cdc
