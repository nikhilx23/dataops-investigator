# ADR 0001: One installable Python package instead of per-service directories

Status: accepted (Milestone 1)

## Context
The suggested layout lists `services/generator`, `services/ingestion`, ... as separate
folders. Every service shares configuration, database access, secret redaction and the
table catalog.

## Decision
Ship one package, `src/dataops/`, with a subpackage per service (`generator`, `ingestion`,
`reconciliation`, later `collectors`, `incidents`, `investigator`, `recovery`) and one CLI
entry point. Containers run the same image with different commands.

## Consequences
- Shared code is imported, not copied; one dependency set to pin and scan.
- Service boundaries are enforced by database roles (each component connects with its own
  least-privilege role), not by separate deployables.
- If a service later needs an incompatible dependency (e.g. Spark jobs), it gets its own
  image and requirements file.
