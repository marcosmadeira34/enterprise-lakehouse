# 0004 — Delta/PostgreSQL consistency by commit tagging and recovery

## Context
Writing data (Delta on object storage) and recording it (PostgreSQL) are two systems; there is no
atomic transaction across them. A process can die between the two.

## Decision
No distributed transaction is claimed. Instead each step is designed so that re-running it after any
crash converges to the correct state:

* **Bronze**: one Delta commit per batch, tagged with `{"batch_id": ...}` in `userMetadata`. Before
  writing, the job looks for that commit in the table history; if present (crash after commit), it skips
  the write and only completes the metadata (counts are read back from the table, not from memory).
* **Silver**: idempotent by construction (ADR-0005), so a re-run after a crash simply repeats the work.
* **Gold**: tables are written first, the publication row last; a crash leaves the previous publication
  in force (ADR-0008).
* **Locks**: lease rows in PostgreSQL (`processing_locks`) per layer and entity; an expired lease (crashed
  holder) can be taken over.
* **`lakehouse recover`** marks runs left `RUNNING` as `ABANDONED` and their batches retryable.
* Each boundary has a named failure checkpoint (`LAKEHOUSE_FAIL_AT=...`) exercised by the resilience tests.

## Consequences
* The history lookup is bounded by Delta log retention (30 days by default); a batch older than that
  cannot be in a half-committed state in practice.
* Correctness relies on single-writer-per-table, guaranteed by the locks.
