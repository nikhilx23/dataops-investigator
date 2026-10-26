#!/usr/bin/env bash
# Compose init hook (runs once, on an empty data volume, inside the postgres container).
# The DATAOPS_* variables are passed to the container from .env by docker-compose.yml.
set -euo pipefail
psql -X -q -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -f /bootstrap/bootstrap.sql
