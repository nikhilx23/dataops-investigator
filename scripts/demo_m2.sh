#!/usr/bin/env bash
# Milestone 2 (lightweight): simulated month of daily operations, incremental dbt,
# full-refresh equivalence and reconciliation. Pass --reset to empty the SYNTHETIC
# environment first (destructive for demo data only).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_env.sh"
cd "$ROOT"
python scripts/demo_m2.py "$@"
