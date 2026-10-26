# Dependency baseline

Selected on 2026-10-25. "Verified" means exercised in this repository's checks; "resolved"
means the version set was resolved by pip from published package metadata (official
`requires_dist` / `requires_python`), without running the component.

## Python packages (pinned in pyproject.toml)

| Package | Version | Status | Notes |
|---|---|---|---|
| Python | 3.13 (local), 3.12 (container, CI) | verified 3.13 | package declares `>=3.12,<3.14` |
| psycopg[binary] | 3.3.6 | verified | |
| pydantic | 2.14.1 | installed | used from Milestone 3 (API schemas) |
| dbt-core | 1.12.6 | verified | `dbt build` 40/40 against PostgreSQL 16 |
| dbt-postgres | 1.11.0 | verified | requires `dbt-core>=1.8,<2`, `dbt-adapters>=1.24.1` |
| pytest / ruff / mypy | 9.1.1 / 0.17.2 / 2.4.0 | verified | |

## Data platform (full profile)

| Component | Version | Status | Evidence |
|---|---|---|---|
| PostgreSQL | 16 | verified (16.15, Ubuntu package) | local server used by all M1 checks |
| pgvector | 0.6.0 locally, 0.8.0 image | extension created locally (0.6.0) | image tag not pulled (see blockers) |
| Apache Spark (pyspark) | 4.2.0 | resolved | `delta-spark 4.4.1` requires `pyspark>=4.0.1,<=4.2.0` |
| Delta Lake (delta-spark) | 4.4.1 | resolved | Scala 2.13 artifact `io.delta:delta-spark_2.13:4.4.1` |
| Java | 21 | available locally (OpenJDK 21.0.12) | Spark 4.x supports Java 17 and 21 |
| Apache Kafka, Debezium, Airflow | to pin in Milestone 2 | not yet selected | each will be pinned with its documented compatibility before use |

## Container images

| Image | Tag | Status |
|---|---|---|
| pgvector/pgvector | 0.8.0-pg16 | **not pull-verified**: Docker Hub is blocked in the build sandbox |
| python | 3.12.11-slim-bookworm | **not pull-verified** (same reason) |

Tags are pinned; digests will be pinned once the images can be pulled (CI job
`compose-and-scripts` builds the app image on GitHub-hosted runners).

## Why not newer/older

- Spark/Delta: the newest Delta release whose declared PySpark range includes the newest PySpark.
- dbt: latest dbt-core with the latest dbt-postgres adapter whose declared range accepts it.
