# Troubleshooting

Problems actually hit while building and running this project, with the fix.

| Symptom | Cause | Fix |
|---|---|---|
| `connection refused` on 127.0.0.1:5432 | PostgreSQL not running (native install) | `sudo pg_ctlcluster 16 main start` (or `docker compose --profile lightweight up -d postgres`, host port 5433) |
| `psql: \getenv: invalid command` | psql older than 15 | install postgresql-client ≥ 15; in Compose the postgres container bootstraps itself |
| Migration applied but tables missing | (old bug) migrations inside an uncommitted transaction | fixed: migrations run in autocommit; rerun `dataops db migrate` |
| `another batch for <table> is running` | a loader crashed and left a `running` batch | wait for the 30-minute lease, or `dataops recovery propose --action retry_failed_batch` (abandons it only if no loader session is alive) |
| Orphan `relationships` test failures after extraction | a parent table was not extracted for the window | `bounded_backfill` the parent table(s), then `rebuild_models` |
| Reconciliation fails but no extraction error | changes committed with timestamps behind the watermark (back-dated) | `bounded_backfill` over the commit window + `rebuild_models --full-refresh` |
| Incremental facts still show a removed/reverted row | incremental `delete+insert` never retracts rows absent from the new batch | `rebuild_models` with `full_refresh: true` |
| Schema collector reports every column `removed` | the reader lost SELECT on the table (fixed: such tables are skipped) | restore the grant; the failing extraction reports the access problem |
| Incident engine crash `incident_evidence_pkey` | (old bug) evidence id was a global key | migration 0006 |
| Spark `CONCURRENT_STREAM_LOG_UPDATE` | two stream runs on one checkpoint | the lakehouse lock prevents it; do not start a second runner |
| Debezium deletes quarantined | delete images carry placeholders for non-key columns | fixed: deletes are validated on the key only |
| `airflow standalone` cannot find `airflow` | PATH inside the standalone launcher | use `scripts/airflow.sh` |
| Docker Hub `429 Too Many Requests` | anonymous pull limit | `docker login`, or retry later |
| Image build fails at `apt-get` / pip TLS errors behind a proxy | no Debian mirror / TLS inspection | the image needs no OS packages; pass the proxy CA as `--secret id=pipca,src=<ca.pem>` |
| API returns 403 on writes | `DATAOPS_API_TOKEN` not set | run `scripts/init_env.py` (or add the token to `.env`) |
| Dashboard: "API unreachable" | API not running or wrong `DATAOPS_API_URL` | start uvicorn / the `api` service |
| `investigate` (model mode) → 409 / ProviderUnavailable | no `ANTHROPIC_API_KEY` | add it to `.env`; fixture mode is a test harness, not AI |
| Grafana dashboard blank in headless screenshots | panels render after layout; take the screenshot after a short wait | — |
| Prometheus target down on macOS/Windows | monitoring profile uses host networking (Linux) | scrape `host.docker.internal:8000` instead |
| `scenario run` refuses: "no pending payment is behind the watermark" | candidates were touched by the last cleanup | run a pipeline cycle and wait ~20 minutes |
