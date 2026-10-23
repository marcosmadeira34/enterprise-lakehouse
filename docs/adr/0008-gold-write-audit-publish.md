# 0008 — Gold rebuild with write-audit-publish and version pinning

## Context
Gold has several tables that must be mutually consistent, must pass blocking checks before anyone sees
them, and must never be exposed half-written.

## Decision
* Gold reads every Silver input at a **pinned version** taken at the start of the run.
* Each Gold table is fully rebuilt and overwritten (one atomic commit per table).
* Blocking rules (`GLD-*`, `REC-GOLD-*`) run against exactly the versions just written.
* Only then a row is inserted in `gold_publications` with the table versions, the source versions, row
  counts and status `PUBLISHED` or `BLOCKED`.
* Consumers (the API) always read the versions of the latest `PUBLISHED` row via Delta time travel.

## Consequences
* Full rebuild cost grows with history. At the measured scales it is a few seconds (`docs/benchmark.md`).
  The evolution path is incremental rebuild of affected `sale_date` partitions using Silver's change data
  feed (already enabled on Silver tables).
* `VACUUM` must keep files of the published versions (default retention 7 days; no automatic vacuum runs).
* Cost uses the current product cost (SCD type 1); historical cost needs an SCD2 product dimension.
