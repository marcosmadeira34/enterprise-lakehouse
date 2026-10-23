# 0003 — File identity is the content hash

## Context
Sources re-send files under new names, overwrite files keeping the name, and legitimately repeat records
inside one file. Name-based tracking either reloads duplicates or skips corrections.

## Decision
* A file is identified by `(entity, SHA-256 of its bytes)`, computed by the ingestion layer (streamed,
  no Spark).
* Same bytes under another name → registered as `DUPLICATE` (pointing at the original) and not loaded.
* Same name with new bytes → a new file.
* `(uri, size, modified time)` is only a cache to avoid re-hashing files already registered.
* The database enforces at most one *live* registration (`PENDING` or `COMMITTED`) per content with a
  partial unique index, so two concurrent ingestions cannot both register the same file.
* Record-level duplicates inside a file are preserved in Bronze; Silver resolves them by business key.

## Consequences
* Every new file is read once to hash it (cost proportional to new data only).
* A byte-identical re-delivery that is *meant* to be reloaded must go through `lakehouse reprocess`
  (Bronze already holds its records), not through re-upload.
