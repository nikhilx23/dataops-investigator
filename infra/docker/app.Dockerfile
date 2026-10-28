# syntax=docker/dockerfile:1
# Application image: CLI, ingestion, reconciliation and dbt (lightweight profile).
# Base image pinned by digest (pulled and built 2026-10-28).
FROM python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

# No OS packages: in Compose the database is bootstrapped by the postgres container's init
# script (it has psql); this image only needs Python.

RUN useradd --create-home --uid 10001 dataops
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
# Optional build secret `pipca`: a CA bundle for builds behind a TLS-inspecting proxy.
RUN --mount=type=secret,id=pipca,required=false \
    if [ -f /run/secrets/pipca ]; then export PIP_CERT=/run/secrets/pipca SSL_CERT_FILE=/run/secrets/pipca; fi; \
    pip install ".[dbt,app]"
COPY dbt ./dbt
COPY apps ./apps
COPY catalog ./catalog
COPY contracts ./contracts
COPY runbooks ./runbooks
COPY .streamlit ./.streamlit
COPY infra/postgres ./infra/postgres
COPY scripts ./scripts
RUN mkdir -p /app/var && chown -R dataops /app/var /app/dbt
USER dataops
ENTRYPOINT ["python", "-m", "dataops"]
CMD ["health"]
