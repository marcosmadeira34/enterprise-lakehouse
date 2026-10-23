# 0005 — Idempotent Silver: guarded MERGE and per-batch reject replacement

## Context
Silver must survive re-runs, retries, late batches, out-of-order updates and historical replays without
duplicating or regressing data.

## Decision
* Within the processed batches, valid records are deduplicated per business key, keeping the latest by
  `(updated_at, batch_seq, source file, record hash)`; the others are rejected as `DUPLICATE_BUSINESS_KEY`.
* Winners are merged with
  `WHEN MATCHED AND (s.updated_at > t.updated_at OR (s.updated_at = t.updated_at AND s._batch_seq > t._batch_seq)) UPDATE`
  and `WHEN NOT MATCHED INSERT`. Re-running a batch matches every row with an equal version: no change.
  An older (late or replayed) version can never overwrite a newer one.
* Rejects are written with `replaceWhere entity = X AND _batch_id IN (...)`: a re-run replaces the batch's
  rejects instead of appending.
* A batch whose reject ratio exceeds `quality.max_reject_ratio` is blocked (nothing merged). If blocked and
  healthy batches were processed together, the healthy ones are re-evaluated without the blocked ones so a
  blocked record cannot demote a valid one to "duplicate".
* Historical reprocessing replays Bronze batches (upsert semantics) or rebuilds an entity (`--full-refresh`)
  when accepted records must be removed.

## Consequences
* Deletes at the source are not modeled (no tombstones); documented limitation.
* Silver equals "latest valid version per key over all processed batches", which is exactly what the
  `REC-*-SILVER-STATE` reconciliation re-derives from Bronze and compares.
