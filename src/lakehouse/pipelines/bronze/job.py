"""Bronze: land planned batches as raw, auditable Delta tables (one table per entity).

Per batch and entity there is exactly **one** Delta commit, tagged with the batch id in
``userMetadata``. Coordination with the metadata database is by recovery, not by a distributed
transaction:

1. mark the batch RUNNING;
2. if a commit for the batch already exists (a previous attempt crashed after committing), skip the
   write; otherwise read the files, apply the schema policy and append them in a single commit;
3. count what is *in the table* for the batch and only then mark files and batch as committed.

A crash between steps 2 and 3 leaves the batch RUNNING/FAILED with its data committed; the next
attempt detects the commit and only finishes step 3, so data is never appended twice.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from functools import reduce
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.common import faults
from lakehouse.common.spark import commit_metadata, ensure_table, find_commit, read_delta, spark_path_for
from lakehouse.config import Settings
from lakehouse.contracts import EntityContract, SourceFormat, get_contract
from lakehouse.metadata.repository import MetadataRepository
from lakehouse.observability.logging import bind, log_event
from lakehouse.pipelines.runtime import RunHandle
from lakehouse.quality.framework import Category, DatasetRule, Severity, volume_outcome

log = logging.getLogger(__name__)

CORRUPT_COLUMN = "_corrupt_record"
LINEAGE_FIELDS = [
    StructField("_extra_columns", StringType()),
    StructField(CORRUPT_COLUMN, StringType()),
    StructField("_record_hash", StringType()),
    StructField("_batch_id", StringType()),
    StructField("_batch_seq", LongType()),
    StructField("_run_id", StringType()),
    StructField("_source_file", StringType()),
    StructField("_source_file_sha256", StringType()),
    StructField("_source_format", StringType()),
    StructField("_source_system", StringType()),
    StructField("_ingested_at", TimestampType()),
    StructField("_ingestion_date", DateType()),
]


def bronze_schema(contract: EntityContract) -> StructType:
    """Business columns are kept as text: Bronze preserves values exactly as received."""
    return StructType([StructField(name, StringType()) for name in contract.column_names] + LINEAGE_FIELDS)


@dataclass(frozen=True)
class SchemaDecision:
    present: tuple[str, ...]
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]
    extra: tuple[str, ...]

    @property
    def compatible(self) -> bool:
        return not self.missing_required


def normalize_column_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def evaluate_schema(contract: EntityContract, source_columns: list[str]) -> SchemaDecision:
    """Schema policy: extra columns are captured, missing optional ones become NULL, a missing
    required column makes the file incompatible (quarantined). Nothing is evolved automatically."""
    present = {normalize_column_name(c) for c in source_columns if c != CORRUPT_COLUMN}
    expected = set(contract.column_names)
    required = set(contract.required_columns)
    return SchemaDecision(
        present=tuple(sorted(present & expected)),
        missing_required=tuple(sorted(required - present)),
        missing_optional=tuple(sorted((expected - required) - present)),
        extra=tuple(sorted(present - expected)),
    )


def read_source_file(spark: SparkSession, contract: EntityContract, uri: str) -> DataFrame:
    path = spark_path_for(uri)
    fmt = contract.source_format
    if fmt is SourceFormat.CSV:
        header = spark.read.option("header", "true").option("inferSchema", "false").csv(path).columns
        # Explicit all-text schema built from this file's own header (Spark maps CSV columns by
        # position, so one read per file is what keeps reordered layouts correct).
        schema = StructType(
            [StructField(c, StringType()) for c in header] + [StructField(CORRUPT_COLUMN, StringType())]
        )
        return (
            spark.read.schema(schema)
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
            .option("encoding", "UTF-8")
            .csv(path)
        )
    if fmt is SourceFormat.JSON:
        return (
            spark.read.option("primitivesAsString", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
            .json(path)
        )
    return spark.read.parquet(path)


def _as_text(df: DataFrame, column: str) -> Column:
    dtype = df.schema[column].dataType.typeName()
    if dtype in {"struct", "array", "map"}:
        return F.to_json(F.col(f"`{column}`"))
    if dtype == "timestamp":
        return F.date_format(F.col(f"`{column}`"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    return F.col(f"`{column}`").cast("string")


def to_bronze_frame(
    df: DataFrame,
    contract: EntityContract,
    decision: SchemaDecision,
    file_row: dict[str, Any],
    batch: dict[str, Any],
    run_id: str,
    source_system: str,
) -> DataFrame:
    by_normalized = {normalize_column_name(c): c for c in df.columns if c != CORRUPT_COLUMN}
    business = [
        (_as_text(df, by_normalized[name]) if name in by_normalized else F.lit(None).cast("string")).alias(
            name
        )
        for name in contract.column_names
    ]
    if decision.extra:
        pairs = [x for name in decision.extra for x in (F.lit(name), _as_text(df, by_normalized[name]))]
        extra = F.to_json(F.create_map(*pairs))
    else:
        extra = F.lit(None).cast("string")
    corrupt = F.col(CORRUPT_COLUMN) if CORRUPT_COLUMN in df.columns else F.lit(None).cast("string")
    record_hash = F.sha2(
        F.concat_ws(
            "\u0001", *[F.coalesce(c, F.lit("\u0000")) for c in business], F.coalesce(extra, F.lit(""))
        ),
        256,
    )
    return df.select(
        *business,
        extra.alias("_extra_columns"),
        corrupt.alias(CORRUPT_COLUMN),
        record_hash.alias("_record_hash"),
        F.lit(batch["batch_id"]).alias("_batch_id"),
        F.lit(int(batch["batch_seq"])).cast("long").alias("_batch_seq"),
        F.lit(run_id).alias("_run_id"),
        F.lit(file_row["uri"]).alias("_source_file"),
        F.lit(file_row["content_sha256"]).alias("_source_file_sha256"),
        F.lit(contract.source_format.value).alias("_source_format"),
        F.lit(source_system).alias("_source_system"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(batch["ingestion_date"]).cast("date").alias("_ingestion_date"),
    )


@dataclass
class BronzeBatchResult:
    entity: str
    batch_id: str
    records: int = 0
    files_committed: int = 0
    files_quarantined: int = 0
    delta_version: int | None = None
    recovered: bool = False
    quality: list[Any] = field(default_factory=list)


def bronze_table_path(settings: Settings, entity: str) -> str:
    return spark_path_for(settings.table_uri("bronze", entity))


def batch_rows(spark: SparkSession, settings: Settings, entity: str, batch: dict[str, Any]) -> DataFrame:
    """Rows of one batch, pruned by the ingestion-date partition."""
    df = read_delta(spark, bronze_table_path(settings, entity))
    return df.where((F.col("_ingestion_date") == F.lit(batch["ingestion_date"]).cast("date"))
                    & (F.col("_batch_id") == batch["batch_id"]))  # fmt: skip


def _quarantine(
    repo: MetadataRepository, contract: EntityContract, file_row: dict[str, Any], batch_id: str, reason: str
) -> None:
    repo.update_file(int(file_row["file_id"]), status="QUARANTINED", error_message=reason[:4000])
    log_event(log, "file_quarantined", logging.WARNING, uri=file_row["uri"], reason=reason[:500])


def _apply_schema_policy(
    repo: MetadataRepository,
    contract: EntityContract,
    file_row: dict[str, Any],
    batch_id: str,
    columns: list[str],
) -> SchemaDecision:
    decision = evaluate_schema(contract, columns)
    file_id = int(file_row["file_id"])
    repo.update_file(
        file_id, schema_columns=[normalize_column_name(c) for c in columns if c != CORRUPT_COLUMN]
    )
    events = [
        ("ADDED_COLUMNS", decision.extra, "CAPTURED"),
        ("MISSING_OPTIONAL", decision.missing_optional, "NULL_FILLED"),
        ("MISSING_REQUIRED", decision.missing_required, "QUARANTINED"),
    ]
    for event_type, cols, action in events:
        if cols:
            repo.record_schema_event(
                contract.name, file_id, batch_id, event_type, list(cols), action, contract.version
            )
            log_event(log, "schema_change_detected", logging.WARNING, uri=file_row["uri"],
                      event_type=event_type, columns=list(cols), action=action)  # fmt: skip
    return decision


def process_batch(
    spark: SparkSession, run: RunHandle, contract: EntityContract, batch: dict[str, Any]
) -> BronzeBatchResult:
    settings, repo = run.settings, run.repo
    batch_id = batch["batch_id"]
    path = bronze_table_path(settings, contract.name)
    result = BronzeBatchResult(contract.name, batch_id)
    repo.set_layer_status(batch_id, "bronze", "RUNNING", run.run_id)
    run.step = f"bronze:{contract.name}:{batch_id}"

    ensure_table(
        spark,
        path,
        bronze_schema(contract),
        partition_by=["_ingestion_date"],
        # Raw data is immutable: updates and deletes are refused by Delta itself.
        properties={
            "delta.appendOnly": "true",
            "lakehouse.layer": "bronze",
            "lakehouse.contract_version": str(contract.version),
        },
        comment=f"Bronze {contract.name}: raw records as received, with ingestion lineage.",
    )
    version = find_commit(spark, path, "batch_id", batch_id)
    # Fallback for a history already truncated by log retention: the rows themselves carry the batch id
    # (pruned by the ingestion-date partition, so this reads at most one partition).
    already_written = (
        version is not None or batch_rows(spark, settings, contract.name, batch).limit(1).count() > 0
    )
    if already_written:
        result.recovered = True
        log_event(log, "bronze_commit_recovered", logging.WARNING, batch_id=batch_id, delta_version=version)
    else:
        frames = []
        for file_row in repo.batch_files(batch_id):
            if file_row["status"] != "PENDING":
                continue
            try:
                raw = read_source_file(spark, contract, file_row["uri"])
                columns = raw.columns
            except Exception as exc:  # unreadable or corrupt file: isolate it, keep the batch going
                _quarantine(
                    repo, contract, file_row, batch_id, f"unreadable file: {type(exc).__name__}: {exc}"
                )
                repo.record_error(run.run_id, run.step, exc, entity=contract.name, batch_id=batch_id)
                continue
            decision = _apply_schema_policy(repo, contract, file_row, batch_id, columns)
            if not decision.compatible:
                _quarantine(repo, contract, file_row, batch_id,
                            f"incompatible schema: missing required columns {list(decision.missing_required)}")  # fmt: skip
                continue
            frames.append(
                to_bronze_frame(
                    raw, contract, decision, file_row, batch, run.run_id, settings.pipeline.source_system
                )
            )
        if frames:
            union = reduce(DataFrame.unionByName, frames)
            (
                union.write.format("delta")
                .mode("append")
                .option(
                    "userMetadata",
                    commit_metadata(batch_id=batch_id, run_id=run.run_id, entity=contract.name),
                )
                .save(path)
            )
            version = find_commit(spark, path, "batch_id", batch_id)
            faults.checkpoint("bronze.after_delta_commit")

    counts = {
        row["_source_file_sha256"]: int(row["count"])
        for row in batch_rows(spark, settings, contract.name, batch)
        .groupBy("_source_file_sha256")
        .count()
        .collect()
    }
    files = repo.batch_files(batch_id)
    pending = [f for f in files if f["status"] == "PENDING"]
    repo.mark_files_committed(
        batch_id, {f["content_sha256"]: counts.get(f["content_sha256"], 0) for f in pending}
    )
    result.records = sum(counts.values())
    result.files_committed = len(pending) + sum(1 for f in files if f["status"] == "COMMITTED")
    result.files_quarantined = sum(1 for f in files if f["status"] == "QUARANTINED")
    result.delta_version = version
    repo.set_layer_status(
        batch_id,
        "bronze",
        "SUCCEEDED",
        run.run_id,
        records_read=result.records,
        records_accepted=result.records,
        records_rejected=0,
        delta_version=version,
        metrics={
            "files_committed": result.files_committed,
            "files_quarantined": result.files_quarantined,
            "recovered": result.recovered,
        },
    )
    repo.set_layer_status(batch_id, "silver", "PENDING")
    result.quality = [volume_rule(settings, repo, contract.name, batch, result.records)]
    repo.save_quality_results([r.to_record(run.run_id) for r in result.quality])
    run.add(read=result.records, written=result.records)
    log_event(log, "bronze_batch_committed", batch_id=batch_id, records=result.records,
              files_committed=result.files_committed, files_quarantined=result.files_quarantined,
              delta_version=version, recovered=result.recovered)  # fmt: skip
    return result


def volume_rule(
    settings: Settings, repo: MetadataRepository, entity: str, batch: dict[str, Any], records: int
) -> Any:
    cfg = settings.quality.volume_anomaly
    history = repo.recent_batch_volumes(entity, int(batch["batch_seq"]), cfg.lookback_batches)
    rule = DatasetRule(
        rule_id=f"BRZ-{entity.upper()}-VOLUME",
        name=f"{entity} ingestion volume",
        description=(
            f"Records in the batch must be within [{cfg.min_ratio}, {cfg.max_ratio}] x the mean of the "
            f"last {cfg.lookback_batches} batches (needs {cfg.min_history} batches of history)."
        ),
        category=Category.VOLUME,
        layer="bronze",
        table_name=f"bronze.{entity}",
        severity=Severity.WARNING,
        check=lambda _ctx: volume_outcome(records, history, cfg.min_history, cfg.min_ratio, cfg.max_ratio),
    )
    return rule.evaluate(None, batch_id=batch["batch_id"])


def run_bronze(
    spark: SparkSession, run: RunHandle, entities: list[str], batch_ids: list[str] | None = None
) -> list[BronzeBatchResult]:
    results = []
    for entity in entities:
        contract = get_contract(entity)
        with bind(entity=entity), run.repo.lock(f"bronze:{entity}", run.settings.pipeline.lock_ttl_seconds):
            for batch in run.repo.pending_batches("bronze", entity):
                if batch_ids and batch["batch_id"] not in batch_ids:
                    continue
                with bind(batch_id=batch["batch_id"]):
                    try:
                        results.append(process_batch(spark, run, contract, batch))
                    except Exception as exc:
                        run.repo.set_layer_status(batch["batch_id"], "bronze", "FAILED", run.run_id,
                                                  error_message=f"{type(exc).__name__}: {exc}")  # fmt: skip
                        raise
    return results
