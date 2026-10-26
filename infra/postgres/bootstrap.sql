-- DataOps Investigator: database and role bootstrap (idempotent).
--
-- Run as a superuser with psql (>= 15, for \getenv). All names and passwords come from
-- DATAOPS_* environment variables (see .env.example), read with \getenv so that secrets
-- never appear in this file, in process arguments, or in logs.
-- Entry points: scripts/bootstrap_db.sh, scripts/bootstrap_db.ps1, and the Compose init hook.
--
-- Layout: one PostgreSQL server, separate logical databases and least-privilege roles.
--   commerce_source : operational transactions (schema shop)
--   warehouse       : raw landing (schema raw) + dbt-owned analytics schemas
--   reliability     : reliability + AI metadata (schema reliability, pgvector)
--   airflow         : Airflow metadata (full profile)

\set ON_ERROR_STOP on
\set QUIET on

\getenv source_db       DATAOPS_SOURCE_DB
\getenv warehouse_db    DATAOPS_WAREHOUSE_DB
\getenv reliability_db  DATAOPS_RELIABILITY_DB
\getenv airflow_db      DATAOPS_AIRFLOW_DB
\getenv src_app_pw      DATAOPS_SRC_APP_PASSWORD
\getenv src_reader_pw   DATAOPS_SRC_READER_PASSWORD
\getenv wh_loader_pw    DATAOPS_WH_LOADER_PASSWORD
\getenv wh_transform_pw DATAOPS_WH_TRANSFORM_PASSWORD
\getenv wh_reader_pw    DATAOPS_WH_READER_PASSWORD
\getenv rel_app_pw      DATAOPS_REL_APP_PASSWORD
\getenv rel_reader_pw   DATAOPS_REL_READER_PASSWORD
\getenv airflow_pw      DATAOPS_AIRFLOW_DB_PASSWORD

-- ---------------------------------------------------------------- roles
SELECT format('CREATE ROLE %I LOGIN', r) FROM unnest(ARRAY[
  'src_app','src_reader','wh_loader','wh_transform','wh_reader','rel_app','rel_reader','airflow'
]) AS r WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) \gexec

ALTER ROLE src_app      WITH LOGIN PASSWORD :'src_app_pw';
ALTER ROLE src_reader   WITH LOGIN PASSWORD :'src_reader_pw';
ALTER ROLE wh_loader    WITH LOGIN PASSWORD :'wh_loader_pw';
ALTER ROLE wh_transform WITH LOGIN PASSWORD :'wh_transform_pw';
ALTER ROLE wh_reader    WITH LOGIN PASSWORD :'wh_reader_pw';
ALTER ROLE rel_app      WITH LOGIN PASSWORD :'rel_app_pw';
ALTER ROLE rel_reader   WITH LOGIN PASSWORD :'rel_reader_pw';
ALTER ROLE airflow      WITH LOGIN PASSWORD :'airflow_pw';

-- Bound runaway queries from application roles.
ALTER ROLE src_reader   SET statement_timeout = '120s';
ALTER ROLE wh_reader    SET statement_timeout = '60s';
ALTER ROLE rel_reader   SET statement_timeout = '15s';

-- ------------------------------------------------------------ databases
SELECT format('CREATE DATABASE %I OWNER %I', d.name, d.owner)
FROM (VALUES (:'source_db','src_app'), (:'warehouse_db','wh_transform'),
             (:'reliability_db','rel_app'), (:'airflow_db','airflow')) AS d(name, owner)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = d.name) \gexec

-- No database is open to PUBLIC.
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', d)
FROM unnest(ARRAY[:'source_db', :'warehouse_db', :'reliability_db', :'airflow_db']) AS d \gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO src_app, src_reader', :'source_db') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO wh_loader, wh_transform, wh_reader', :'warehouse_db') \gexec
-- Loader stages pages in a temp table; dbt-postgres uses temp tables for incremental models.
SELECT format('GRANT TEMPORARY ON DATABASE %I TO wh_loader, wh_transform', :'warehouse_db') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO rel_app, rel_reader', :'reliability_db') \gexec

-- ------------------------------------------------------ commerce_source
\connect :source_db
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA IF NOT EXISTS shop AUTHORIZATION src_app;
GRANT USAGE ON SCHEMA shop TO src_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO src_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE src_app IN SCHEMA shop GRANT SELECT ON TABLES TO src_reader;

-- ------------------------------------------------------------ warehouse
\connect :warehouse_db
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
-- raw: append-only landing zone written only by the loader.
CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION wh_loader;
GRANT USAGE ON SCHEMA raw TO wh_transform, wh_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO wh_transform, wh_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE wh_loader IN SCHEMA raw GRANT SELECT ON TABLES TO wh_transform, wh_reader;
-- dbt (wh_transform owns the database) creates analytics schemas; readers get SELECT on them.
ALTER DEFAULT PRIVILEGES FOR ROLE wh_transform GRANT USAGE ON SCHEMAS TO wh_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE wh_transform GRANT SELECT ON TABLES TO wh_reader;

-- ---------------------------------------------------------- reliability
\connect :reliability_db
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS reliability AUTHORIZATION rel_app;
GRANT USAGE ON SCHEMA reliability TO rel_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA reliability TO rel_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE rel_app IN SCHEMA reliability GRANT SELECT ON TABLES TO rel_reader;
