#!/usr/bin/env bash
# Start the Docker Compose stack and apply migrations.  Usage: scripts/up.sh [lightweight|full]
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_env.sh"
PROFILE="${1:-lightweight}"
cd "$ROOT"
docker compose --profile "$PROFILE" up -d --build --wait postgres
docker compose --profile "$PROFILE" run --rm app db migrate
docker compose --profile "$PROFILE" run --rm app health
