#!/usr/bin/env bash
# Milestone 1 vertical slice: generate -> load source -> batch ingest -> dbt -> reconcile.
# DATAOPS_RUNTIME=local (default) uses the local virtualenv against DATAOPS_PG_HOST;
# DATAOPS_RUNTIME=docker runs every step in the Compose app container.
# Reloading the synthetic source (--replace) is destructive for the demo source tables only.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_env.sh"
cd "$ROOT"
SEED="${DATAOPS_SEED:-42}"
PROFILE="${DATAOPS_PROFILE:-small}"

dataops() {
  if [ "${DATAOPS_RUNTIME:-local}" = "docker" ]; then
    docker compose --profile lightweight run --rm -T app --log-level WARNING "$@"
  else
    python -m dataops --log-level WARNING "$@"
  fi
}
dbt_build() {
  if [ "${DATAOPS_RUNTIME:-local}" = "docker" ]; then
    docker compose --profile lightweight run --rm -T --entrypoint dbt app \
      build --project-dir dbt --profiles-dir dbt
  else
    dbt build --project-dir dbt --profiles-dir dbt
  fi
}

echo "== 1/5 generate synthetic data (seed=$SEED profile=$PROFILE) and load source"
GEN="$(dataops generate --seed "$SEED" --profile "$PROFILE" --load --replace)"
echo "$GEN" | python -c "import json,sys; d=json.load(sys.stdin); print(json.dumps({k: d[k] for k in ('simulation_start','simulation_end','dataset_checksum','row_counts')}, indent=2))"
START="$(echo "$GEN" | python -c "import json,sys; print(json.load(sys.stdin)['simulation_start'])")"
END="$(echo "$GEN" | python -c "import json,sys; print(json.load(sys.stdin)['simulation_end'])")"
LEDGER="var/ledger/${PROFILE}-seed${SEED}.json"

echo "== 2/5 catch-up ingestion of history before $START"
dataops ingest --until "$START" | python -c "import json,sys; d=json.load(sys.stdin); print(f\"batches={d['batches']} failed={d['failed']} inserted={d['inserted_rows']} duplicates_skipped={d['duplicate_rows']}\")"
echo "== 3/5 daily incremental windows $START .. $END"
dataops ingest --start "$START" --end "$END" --step 1d | python -c "import json,sys; d=json.load(sys.stdin); print(f\"batches={d['batches']} failed={d['failed']} inserted={d['inserted_rows']} duplicates_skipped={d['duplicate_rows']}\")"
echo "== 4/5 dbt build (staging views, revenue mart, tests)"
dbt_build | tail -4
echo "== 5/5 reconciliation (source vs raw counts, source vs mart, ledger vs mart)"
dataops reconcile --ledger "$LEDGER" | python -c "import json,sys; d=json.load(sys.stdin); [print(f\"{r['check_id']:40} {r['status']:5} rows={r['compared_rows']} mismatches={r['mismatched_rows']}\") for r in d['results']]"
