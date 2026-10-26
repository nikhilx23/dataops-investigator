#!/usr/bin/env bash
# Full profile: start PostgreSQL (logical WAL), Kafka, Debezium Connect; migrate; register CDC.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_env.sh"
cd "$ROOT"
export DATAOPS_PG_PORT="${DATAOPS_PG_HOST_PORT:-5433}"
docker compose --profile full up -d --wait postgres kafka connect
python -m dataops db migrate
python -m dataops health
python -m dataops cdc register
