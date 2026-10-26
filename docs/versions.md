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
| pgvector | 0.6.0 (native), 0.8.0 (image) | verified (extension created on both) | full profile runs on the 0.8.0 image |
| Apache Spark (pyspark) | 4.2.0 | **verified** (streaming jobs, tests) | `delta-spark 4.4.1` requires `pyspark>=4.0.1,<=4.2.0`; its POM is built against Spark 4.2.0 |
| Delta Lake (delta-spark) | 4.4.1 | **verified** | Scala 2.13 artifacts from Maven Central |
| spark-sql-kafka-0-10_2.13 | 4.2.0 | **verified** | matches Spark |
| Java | 21 | **verified** (OpenJDK 21.0.12) | Spark 4.x supports Java 17 and 21 |
| Kafka | 3.9.0 (`quay.io/debezium/kafka:3.0.8.Final`, KRaft) | **verified** | image bundles kafka_2.13-3.9.0 |
| Debezium PostgreSQL connector | 3.0.8.Final (`quay.io/debezium/connect:3.0.8.Final`) | **verified** | pgoutput against PostgreSQL 16 |
| confluent-kafka (Python) | 2.16.0 | **verified** | |
| Airflow | 3.3.2 + official constraints for Python 3.13 (`orchestration/`) | **verified** (scheduler runs) | providers: standard 1.19.0, postgres 7.0.2 |

## Container images

| Image | Tag | Status |
|---|---|---|
| pgvector/pgvector | 0.8.0-pg16 | **verified** (pulled, runs the full profile) |
| quay.io/debezium/kafka | 3.0.8.Final | **verified** |
| quay.io/debezium/connect | 3.0.8.Final | **verified** |
| python | 3.12.11-slim-bookworm | **not verified**: Docker Hub answered HTTP 429 (anonymous pull rate limit) on every attempt from the build network |

Images that were pulled and run here are pinned by digest in docker-compose.yml. The python
base image stays tag-pinned until it can be pulled (CI job `compose-and-scripts` builds it).

## Why not newer/older

- Spark/Delta: the newest Delta release whose declared PySpark range includes the newest PySpark.
- dbt: latest dbt-core with the latest dbt-postgres adapter whose declared range accepts it.
