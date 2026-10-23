#!/bin/bash
# One PostgreSQL server, two isolated databases with their own owners:
#   airflow   -> Airflow's metadata
#   lakehouse -> the platform's operational metadata (runs, batches, files, quality, publications)
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;
    CREATE USER lakehouse WITH PASSWORD '${LAKEHOUSE_DB_PASSWORD}';
    CREATE DATABASE lakehouse OWNER lakehouse;
    -- Read-only role for the API: it never writes metadata.
    CREATE USER lakehouse_reader WITH PASSWORD '${LAKEHOUSE_READER_PASSWORD}';
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname lakehouse <<-SQL
    GRANT CONNECT ON DATABASE lakehouse TO lakehouse_reader;
    GRANT USAGE ON SCHEMA public TO lakehouse_reader;
    ALTER DEFAULT PRIVILEGES FOR ROLE lakehouse IN SCHEMA public GRANT SELECT ON TABLES TO lakehouse_reader;
SQL
