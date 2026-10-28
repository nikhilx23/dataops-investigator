# Local and cloud cost considerations

Nothing in this project has been deployed to a cloud, and no paid resources were created.

## Local (measured on the build machine: 2 vCPU, 7.8 GiB RAM)

| Profile | What runs | Memory observed |
|---|---|---|
| lightweight (native or Compose) | PostgreSQL, CLI, API, dashboard | PostgreSQL 56–117 MiB; API/dashboard each a single Python process |
| full | + Kafka, Debezium Connect, Spark (local), Airflow | peak ≈ 3 GiB total when run in stages (Kafka 390–450 MiB, Connect 415–430 MiB, Airflow 833 MiB RSS, Spark driver up to 1.2 GiB) |
| monitoring | + Prometheus, Grafana | small (not separately measured) |

## AI cost

- Fixture mode and the evaluation harness in fixture mode cost nothing.
- Model mode: cost = tokens × your provider price. Every investigation records input/output
  tokens; the evaluation report computes cost only when `DATAOPS_LLM_PRICE_INPUT_PER_MTOK` and
  `DATAOPS_LLM_PRICE_OUTPUT_PER_MTOK` are set (no prices are hard-coded). Tool results are capped
  at 12k characters each and the context at 90k characters, which bounds tokens per run. No
  real-model runs have been made yet, so no measured cost exists.

## If moved to a cloud (estimate inputs, not quotes)

`infra/terraform/azure` (optional, not applied) sketches: Azure Database for PostgreSQL
Flexible Server (pgvector), a Container Apps environment for the API/dashboard, Log Analytics,
and Key Vault for secrets. Cost drivers to check against the provider's current price list:
database compute tier and storage, always-on container replicas, log ingestion volume, and
LLM tokens. Kafka/Spark would be replaced by managed services (e.g. Event Hubs, Databricks),
each a significant line item; the lightweight batch path avoids them.
