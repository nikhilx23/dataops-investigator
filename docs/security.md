# Security and access design

Scope: a local, single-user portfolio system on synthetic data. This describes what is
implemented and tested, and what is not.

## Secrets

- All credentials live in `.env` (git-ignored, created with mode 0600 by
  `scripts/init_env.py`, which generates random values for every `CHANGE_ME`). Nothing secret
  is committed; CI runs gitleaks on the full history (allowlist only for the deliberately fake
  test values listed in `.gitleaks.toml`).
- PostgreSQL role passwords are read by `bootstrap.sql` with `\getenv`, so they never appear on
  a command line or in a file. The Debezium password is resolved inside the Connect worker
  (`EnvVarConfigProvider`); it is not stored in the connector config in Kafka.
- `redact()` removes passwords, connection-string credentials, API keys (`sk-…`) and the values
  of every `*_PASSWORD` environment variable from task messages, tool results and errors before
  they are stored or shown (unit-tested).
- `ANTHROPIC_API_KEY` is read from the environment only; it is never logged or stored.
- Docker image: `.dockerignore` excludes `.env`, `var/`, dbt targets; the image contains no
  credentials. A CA bundle needed behind a TLS-inspecting proxy is passed as a BuildKit
  secret, not a layer.

## Least privilege (separate databases and roles)

| Role | Can | Cannot |
|---|---|---|
| `src_app` | own `shop.*` (synthetic source) | read the warehouse |
| `src_reader` | SELECT on `shop.*` | write anything |
| `cdc_reader` | replication from the publication | write anything |
| `wh_loader` | write `raw.*`, `cdc_pub.*` | create analytics objects |
| `wh_transform` | dbt: read raw, own analytics schemas | write raw |
| `wh_reader` | SELECT on analytics | write anything |
| `rel_app` | write `reliability.*` | touch source or warehouse |
| `rel_reader` | SELECT on `reliability.*` (investigator, API reads, metrics) | write anything (tested) |

`PUBLIC` privileges are revoked; reader roles have statement timeouts (15–120 s); integration tests assert
the privilege boundaries.

## AI investigator

- Read-only by construction: tools run as `rel_reader`; there is no SQL, shell, file or URL
  parameter; arguments are validated (`extra=forbid`, patterns) before any query.
- Untrusted text (logs, messages, traces, runbooks) is wrapped as `untrusted_text`; evidence ids
  are only taken from evidence fields, so a planted id in a log cannot become a citation.
- Output validation removes unresolvable citations, ungrounded numbers, forbidden operations and
  numeric confidence; the evaluation suite includes five prompt-injection cases (logs, test
  messages, connector trace, poisoned runbook).
- Model output never executes anything: recovery actions are an allowlist with typed scopes.

## Recovery and API

- Recovery needs a named human actor (service identities are refused) and a reason; preconditions
  are re-checked at approval and execution; every transition is in an append-only audit table;
  one action at a time (advisory lock). No destructive actions exist.
- API writes require `Authorization: Bearer $DATAOPS_API_TOKEN` (constant-time comparison) and
  `X-Actor`; with no token configured, writes are disabled. Reads are unauthenticated and the
  API binds to 127.0.0.1 by default (Compose publishes on 127.0.0.1 only).
- Grafana: admin password from `.env`, anonymous access off, bound to 127.0.0.1.

## Not implemented (would be needed beyond a local demo)

- Per-user authentication/authorization (SSO, roles) on the API and dashboard; the shared bearer
  token plus a self-declared `X-Actor` is not identity proof.
- TLS between services, encryption at rest, secret manager integration (e.g. Key Vault), audit
  log shipping, rate limiting.
- Network policies; the monitoring profile uses host networking on Linux.
