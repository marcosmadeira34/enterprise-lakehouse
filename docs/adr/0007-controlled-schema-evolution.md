# 0007 — Controlled schema evolution

## Context
Source layouts change: columns are added, removed or reordered. Automatic schema merging silently widens
tables and can map values to the wrong columns.

## Decision
* Every table is created up front with an explicit schema; `spark.databricks.delta.schema.autoMerge.enabled`
  is `false` and no write uses `mergeSchema`.
* CSV files are read one by one with a schema built from **their own header** (Spark maps CSV columns by
  position), so reordered layouts land correctly.
* Per file, against the entity contract:
  * extra columns → kept in `_extra_columns` (JSON) and recorded as `ADDED_COLUMNS / CAPTURED`;
  * missing optional columns → NULL, recorded as `MISSING_OPTIONAL / NULL_FILLED`;
  * missing required columns → the file is `QUARANTINED`, recorded as `MISSING_REQUIRED`; the rest of the
    batch is loaded.
* Promoting a new column is a reviewed change: add it to the contract (bumping `version`), alter the tables,
  and replay Bronze (the values are already preserved in `_extra_columns`).

## Consequences
* No data is lost to drift, and no table changes shape without a code review.
* Type changes inside an existing column surface as `INVALID_FORMAT:<col>` rejects in Silver.
