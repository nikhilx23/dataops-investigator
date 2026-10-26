#!/usr/bin/env bash
# Stop containers. Data volumes are KEPT; pass --delete-volumes to remove them (destructive).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [ "${1:-}" = "--delete-volumes" ]; then
  docker compose --profile full down --volumes
else
  docker compose --profile full down
fi
