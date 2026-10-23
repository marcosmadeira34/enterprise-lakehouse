# 0001 — Medallion layers on Delta Lake

## Context
Raw files arrive late, duplicated, malformed or with changed layouts. Consumers need typed, deduplicated
data and stable analytical tables, and every published number must be traceable to source files.

## Decision
Three layers, each a set of Delta tables with one responsibility:

* **Bronze** — raw values as text, append-only (`delta.appendOnly=true`), one table per entity, partitioned
  by ingestion date, with lineage columns. It is the immutable source of every replay.
* **Silver** — typed, normalized, validated, deduplicated current state per entity (MERGE on business
  key), plus one rejects table.
* **Gold** — star schema and aggregates for consumption, rebuilt from a pinned Silver snapshot.

Delta Lake provides what the design depends on: atomic commits per write, optimistic concurrency, MERGE,
time travel (pinned reads and rollback-free publication), commit metadata (`userMetadata`), table
properties and enforced schemas.

## Consequences
* Storage is multiplied (raw + conformed + modeled); acceptable for auditability.
* Delta on S3 is safe for a **single writer per table** with the default log store; the platform enforces it
  with per-entity locks (ADR-0004). Multi-writer would need a DynamoDB-style log store.
* Alternatives considered: plain Parquet (no atomic multi-file commits, no MERGE); Iceberg (equally valid,
  but needs a catalog service for the same guarantees, adding an extra component to the first version).
