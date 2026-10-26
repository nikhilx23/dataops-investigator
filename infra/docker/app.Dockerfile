# Application image: CLI, ingestion, reconciliation and dbt (lightweight profile).
# Base image tag pinned; see docs/versions.md for why it is not digest-pinned yet.
FROM python:3.12.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

# postgresql-client 15 (bookworm) provides psql >= 15, needed for \getenv in bootstrap.sql.
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 dataops
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[dbt]"
COPY dbt ./dbt
COPY infra/postgres ./infra/postgres
COPY scripts ./scripts
RUN mkdir -p /app/var && chown -R dataops /app/var /app/dbt
USER dataops
ENTRYPOINT ["python", "-m", "dataops"]
CMD ["health"]
