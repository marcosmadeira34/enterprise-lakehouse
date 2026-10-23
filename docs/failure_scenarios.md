# Failure scenarios and recovery

Each scenario lists what happens, what is left behind, how to recover, and the test that proves it.
Failure checkpoints are armed with `LAKEHOUSE_FAIL_AT=<name>` (comma-separated; see
`src/lakehouse/common/faults.py`).

| # | Scenario | Behaviour | Recovery | Evidence |
|---|---|---|---|---|
| 1 | **Crash after the Bronze Delta commit, before PostgreSQL is updated** (`bronze.after_delta_commit`) | Data is in Delta; batch is `FAILED`, files `PENDING`, run `FAILED` with error row | Re-run `bronze`: finds the commit by `userMetadata.batch_id` (or the rows by `_batch_id`), skips the write, counts from the table, marks files `COMMITTED` | `tests/resilience/test_failures.py::test_crash_between_bronze_commit_and_metadata_update` |
| 2 | **Crash after the Silver MERGE, before rejects are written** (`silver.after_merge`) | Silver has the merged rows, rejects missing, batch `FAILED` | Re-run `silver`: MERGE is a no-op for equal versions, rejects written once; reconciliation passes | `test_crash_after_silver_merge_then_retry_is_consistent` |
| 3 | **Crash after Gold tables are written, before publication** (`gold.after_tables_written`) | New Gold versions exist but are not published; API keeps serving the previous publication | Re-run `gold` | `test_crash_after_gold_write_keeps_previous_publication` |
| 4 | **Same batch processed twice** (retry, manual re-run, replay) | No new rows anywhere; counts identical | — | `tests/integration/test_pipeline_local.py::test_rerun_is_idempotent`, `test_replaying_a_processed_batch_changes_nothing` |
| 5 | **Duplicate file** (same bytes, new name) | Registered `DUPLICATE` pointing at the original; not loaded | — | `test_drop2_*` (re-sent sales file), `tests/unit/test_discovery.py`, `test_docker_services.py::test_duplicate_detection_on_s3` |
| 6 | **Same name, corrected content** | New file → new batch; Silver keeps the newest version per key | — | `tests/unit/test_discovery.py` |
| 7 | **Incompatible schema** (required column missing) | File `QUARANTINED`, `schema_events MISSING_REQUIRED`; other files of the batch load | Fix the source and deliver a new file; or extend the contract | `test_schema_change_is_captured_and_incompatible_file_quarantined`, `test_unreadable_and_incompatible_files_do_not_block_the_batch` |
| 8 | **Compatible schema change** (new/removed optional/reordered columns) | Loaded; new values in `_extra_columns`; events `ADDED_COLUMNS` / `MISSING_OPTIONAL` | Promote the column via contract change + replay | same as above |
| 9 | **Unreadable file** (corrupt Parquet) | File `QUARANTINED` with the reader error; error row in `run_errors`; batch continues | Replace the file | `test_unreadable_and_incompatible_files_do_not_block_the_batch` |
| 10 | **Invalid records** (bad dates, negatives, orphans, missing fields, malformed lines) | Rejected with reason codes; batch succeeds if below the reject threshold | Query `_rejects`; fix source or rules; replay | `test_every_injected_anomaly_is_rejected_with_its_reason` |
| 11 | **Batch mostly invalid** (> `max_reject_ratio`) | Batch blocked: rejects stored, **nothing** merged, step exits 2, `SLV-*-REJECT-RATIO` FAILED | Investigate, fix, `reprocess --batch-id` | `test_batch_above_reject_threshold_is_blocked_not_published` |
| 12 | **Late reference data** (products arrive after their sales lines were rejected) | `REC-SALE-ITEMS-SILVER-STATE` fails (Silver ≠ what Bronze now implies) | `lakehouse reprocess --entity sale_items --since …` | `test_late_reference_data_is_detected_and_fixed_by_historical_reprocessing` |
| 13 | **Out-of-order / stale update** | Older `updated_at` never overwrites newer data | — | `test_updates_applied_and_stale_versions_ignored` |
| 14 | **Late batch** (old transactions arriving weeks later) | Accepted; Gold rebuild includes them in their historical dates | — | `test_drop2_state_and_gold_match_oracle` |
| 15 | **Out-of-band change to Silver** | `REC-*-SILVER-STATE` and `REC-GOLD-*` fail | `reprocess --full-refresh` | `test_tampering_with_silver_is_detected_and_repaired` |
| 16 | **Concurrent run on the same entity** | Second process cannot take the lease; run `FAILED` (`LockNotAcquiredError`), CLI exit 3 (Airflow retries) | Automatic retry; expired leases are taken over | `test_lock_held_by_another_process_is_refused_and_recorded`, `tests/unit/test_metadata.py` |
| 17 | **Process killed (no exception handler ran)** | Run stays `RUNNING`, batch layer `RUNNING`, lease expires | `lakehouse recover` (first task of `lakehouse_pipeline`) marks them `ABANDONED`/`FAILED`; next run retries | covered by the same mechanisms as 1–3; `recover` logic in `pipelines/runner.py` |
| 18 | **Gold audit fails** | Publication `BLOCKED`, step exits 2, API keeps the previous version | Fix and re-run `gold` | gate in `pipelines/gold/job.py`; API pinning in `tests/unit/test_api.py` |
| 19 | **Quality check itself crashes** | Status `ERROR`; a BLOCKING rule in `ERROR` fails the step | Fix the rule/table | `tests/unit/test_quality_framework.py::test_crashing_check_is_an_error_not_a_pass` |

## What is *not* atomic

Writing to Delta and to PostgreSQL are separate operations. The platform never claims otherwise: it
guarantees that re-running any step after any crash converges, and reconciliation detects divergence
that recovery did not fix (ADR-0004).

## Known gaps

* Source deletes (tombstones) are not modeled.
* If a Bronze commit is older than the Delta log retention *and* metadata says it never happened, the data
  fallback (`_batch_id` lookup) still prevents a double append; the version number is then unknown.
* A crash in the middle of the `reprocess --full-refresh` reset leaves the entity empty in Silver until the
  replay completes (re-running the same command finishes it).
