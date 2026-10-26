#!/usr/bin/env bash
# Create databases, roles and grants (idempotent), then apply schema migrations.
# Requires: psql >= 15 on PATH, a reachable PostgreSQL server, and a populated .env.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/_env.sh
source "$ROOT/scripts/_env.sh"

echo "Bootstrapping databases and roles on ${DATAOPS_PG_HOST}:${DATAOPS_PG_PORT} ..."
PGPASSWORD="$DATAOPS_PG_ADMIN_PASSWORD" psql -X -q \
  -h "$DATAOPS_PG_HOST" -p "$DATAOPS_PG_PORT" -U "$DATAOPS_PG_ADMIN_USER" -d postgres \
  -f "$ROOT/infra/postgres/bootstrap.sql"

echo "Applying schema migrations ..."
python -m dataops db migrate
echo "Bootstrap complete."
