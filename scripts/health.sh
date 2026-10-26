#!/usr/bin/env bash
# Readiness check for every database role.  DATAOPS_RUNTIME=docker runs it in the app container.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_env.sh"
cd "$ROOT"
if [ "${DATAOPS_RUNTIME:-local}" = "docker" ]; then
  docker compose --profile lightweight run --rm app health
else
  python -m dataops health
fi
