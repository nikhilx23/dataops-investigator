# Project status

All data is synthetic; this is a demonstration system, not a production deployment. Every
number in the docs comes from a run recorded in [PROGRESS.md](PROGRESS.md) or a results file in
this repository.

## Completed and verified

| Area | Evidence |
|---|---|
| Batch ingestion, dbt models, reconciliation | Milestones 1–2a in PROGRESS; integration tests |
| CDC, Kafka, Spark/Delta, publication, Airflow | Milestone 2b (`scripts/verify_cdc.py`, `scripts/verify_streaming.py`) |
| Collectors, lineage, incident engine | Milestone 3; [reliability_platform.md](reliability_platform.md) |
| Investigator plumbing, retrieval, validator, eval harness | Milestone 4, **fixture mode only** |
| Recovery service, 8 scenarios (6 run end to end here, scenario 1 in M3/M5, scenario 4 in M3) | Milestone 5; [results/scenarios](results/scenarios/README.md) |
| API, dashboard, Prometheus/Grafana, container image | Milestone 5; [screenshots](screenshots/) |
| CI on GitHub (lint, types, unit, integration, Spark, DAGs, compose, image, gitleaks) | green run recorded in PROGRESS |
| Benchmarks | [benchmarks.md](benchmarks.md) |

## Unfinished work (clearly not done)

- **Stronger models and a held-out test set**: only Claude Haiku 5.5 has been evaluated, on the
  same 35 cases used for tuning; no Sonnet/Opus run, no repeated runs per configuration.
- **Agent behaviour gaps seen with Haiku**: rarely searches runbooks or proposes recovery.
- **Human rubric scoring** of explanations (rubric written in `evaluations/README.md`).
- **Neural embeddings**: implemented provider, not verified (Hugging Face unreachable here);
  default retrieval is lexical feature hashing.
- **Terraform (Azure)**: sketch only; never validated or applied.
- **Full-profile items not repeated in M5**: recovery of the consumer-lag incident still open in
  the Compose database; continuous streaming as a supervised long-running service.
- **Security beyond a local demo**: per-user authentication, TLS, secret manager, rate limits.
- **Batch deletes**: batch extraction cannot see hard deletes (CDC can); scenario cleanups use a
  documented harness step.
- **Benchmarks**: single machine, single run per step; no load or concurrency testing.
