# 0009 — Operational metadata in PostgreSQL via SQLAlchemy Core

## Context
Runs, batches, file registrations, schema events, quality results, publications and locks need
transactions, unique constraints and ad-hoc SQL for investigation.

## Decision
* PostgreSQL (database `lakehouse`, owner `lakehouse`; read-only role `lakehouse_reader` for the API).
* SQLAlchemy Core with portable constructs only, so the fast test suite runs the same repository code on
  SQLite (including the partial unique index and savepoints).
* Short transactions only: no transaction is held while Spark works.
* Schema created by `lakehouse init` (`create_all` + `schema_version` guard that refuses to run older code
  against a newer schema).

## Consequences
* No migration tool yet: the next schema change should introduce Alembic (the `schema_version` table is
  the hook for it).
* Integration tests run the repository against a real PostgreSQL in a throw-away database.
