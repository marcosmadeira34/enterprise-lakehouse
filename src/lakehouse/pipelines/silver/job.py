"""Silver: conform pending Bronze batches into one Delta table per entity, plus a shared rejects table.

Idempotency by construction (no reliance on the metadata update succeeding):

* accepted records are MERGEd on the business key and only overwrite a target row when they are
  strictly newer by ``(updated_at, batch_seq)`` — re-running a batch is a no-op, and a late or replayed
  older batch can never regress newer data;
* rejects of the processed batches are written with ``replaceWhere`` on ``(entity, batch_id)``, so a
  re-run replaces them instead of appending a second copy.

All pending batches of an entity are processed together (one MERGE), and metrics are still kept per
batch. A batch whose reject ratio exceeds ``quality.max_reject_ratio`` is *blocked*: its rejects are
stored for investigation, none of its records are merged and the step fails.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.common import faults
from lakehouse.common.spark import ensure_table, read_delta, spark_path_for, table_exists
from lakehouse.contracts import EntityContract, get_contract
from lakehouse.observability.logging import bind, log_event
from lakehouse.pipelines.bronze.job import bronze_table_path
from lakehouse.pipelines.runtime import RunHandle
from lakehouse.pipelines.silver.transform import (
    MONEY_COLUMN,
    REJECTS_SCHEMA,
    accepted_rows,
    conform,
    rejected_rows,
    row_rule_catalog,
    silver_schema,
)
from lakehouse.quality.framework import QualityGateError, RuleResult, Severity

log = logging.getLogger(__name__)

DIMENSIONS = {"categories", "products", "customers", "stores"}
REJECTS_TABLE = "_rejects"


@dataclass
class SilverBatchResult:
    entity: str
    batch_id: str
    records_read: int = 0
    accepted: int = 0
    rejected: int = 0
    blocked: bool = False
    reasons: dict[str, int] = field(default_factory=dict)


def silver_path(settings: Any, entity: str) -> str:
    return spark_path_for(settings.table_uri("silver", entity))


def rejects_path(settings: Any) -> str:
    return spark_path_for(settings.table_uri("silver", REJECTS_TABLE))


def ensure_silver_tables(spark: SparkSession, settings: Any, contract: EntityContract) -> None:
    ensure_table(
        spark,
        silver_path(settings, contract.name),
        silver_schema(contract),
        partition_by=[contract.silver_partition] if contract.silver_partition else [],
        properties={
            "lakehouse.layer": "silver",
            "lakehouse.contract_version": str(contract.version),
            "delta.enableChangeDataFeed": "true",
        },
        comment=f"Silver {contract.name}: {contract.description}",
    )
    ensure_table(
        spark,
        rejects_path(settings),
        REJECTS_SCHEMA,
        partition_by=["entity"],
        properties={"lakehouse.layer": "silver"},
        comment="Records rejected in Silver, with the reasons and the raw source values.",
    )


def parent_keys(spark: SparkSession, settings: Any, contract: EntityContract) -> dict[str, DataFrame | None]:
    keys: dict[str, DataFrame | None] = {}
    for fk in contract.foreign_keys:
        path = silver_path(settings, fk.parent)
        keys[fk.parent] = (
            read_delta(spark, path).select(fk.parent_column) if table_exists(spark, path) else None
        )
    return keys


def bronze_input(spark: SparkSession, settings: Any, entity: str, batches: list[dict[str, Any]]) -> DataFrame:
    dates = sorted({b["ingestion_date"] for b in batches})
    ids = [b["batch_id"] for b in batches]
    df = read_delta(spark, bronze_table_path(settings, entity))
    return df.where(
        F.col("_ingestion_date").isin([F.lit(d).cast("date") for d in dates]) & F.col("_batch_id").isin(ids)
    )


def _sql_list(values: list[str]) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def merge_accepted(
    spark: SparkSession, path: str, source: DataFrame, contract: EntityContract
) -> dict[str, Any]:
    from delta.tables import DeltaTable

    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in contract.business_key)
    vc = contract.version_column
    newer = f"s.`{vc}` > t.`{vc}` OR (s.`{vc}` = t.`{vc}` AND s._batch_seq > t._batch_seq)"
    target = DeltaTable.forPath(spark, path)
    target.alias("t").merge(source.alias("s"), on).whenMatchedUpdateAll(
        condition=newer
    ).whenNotMatchedInsertAll().execute()
    last = target.history(1).select("version", "operation", "operationMetrics").first()
    metrics = dict(last["operationMetrics"] or {}) if last and last["operation"] == "MERGE" else {}
    return {"delta_version": int(last["version"]) if last else None, **{k: metrics.get(k) for k in (
        "numSourceRows", "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsCopied")}}  # fmt: skip


def write_rejects(
    spark: SparkSession, path: str, rejects: DataFrame, entity: str, batch_ids: list[str]
) -> None:
    predicate = f"entity = '{entity}' AND _batch_id IN ({_sql_list(batch_ids)})"
    (rejects.write.format("delta").mode("overwrite").option("replaceWhere", predicate).save(path))


@dataclass
class _Evaluation:
    conformed: DataFrame
    stats: dict[str, dict[str, Any]]
    reasons: dict[str, dict[str, int]]


def _evaluate(
    spark: SparkSession, settings: Any, contract: EntityContract, batches: list[dict[str, Any]]
) -> _Evaluation:
    conformed = conform(
        bronze_input(spark, settings, contract.name, batches),
        contract,
        settings.business_rules,
        parent_keys(spark, settings, contract),
        broadcast=DIMENSIONS,
    ).persist()
    money = MONEY_COLUMN.get(contract.name)
    valid = F.size("_reasons") == 0
    aggs = [F.count(F.lit(1)).alias("read"), F.sum(valid.cast("long")).alias("accepted")]
    if money:
        aggs += [F.sum(F.when(valid, F.col(money))).alias("accepted_amount"),
                 F.sum(F.when(~valid, F.col(money))).alias("rejected_amount")]  # fmt: skip
    stats = {r["_batch_id"]: r.asDict() for r in conformed.groupBy("_batch_id").agg(*aggs).collect()}
    reasons: dict[str, dict[str, int]] = {}
    exploded = conformed.select("_batch_id", F.explode("_reasons").alias("reason")).groupBy(
        "_batch_id", "reason"
    )
    for row in exploded.count().collect():
        reasons.setdefault(row["_batch_id"], {})[row["reason"]] = int(row["count"])
    return _Evaluation(conformed, stats, reasons)


def process_entity(
    spark: SparkSession, run: RunHandle, contract: EntityContract, batches: list[dict[str, Any]]
) -> list[SilverBatchResult]:
    settings, repo = run.settings, run.repo
    entity = contract.name
    run.step = f"silver:{entity}"
    for batch in batches:
        repo.set_layer_status(batch["batch_id"], "silver", "RUNNING", run.run_id)
    ensure_silver_tables(spark, settings, contract)
    max_ratio = settings.quality.max_reject_ratio

    def is_blocked(evaluation: _Evaluation, batch_id: str) -> bool:
        s = evaluation.stats.get(batch_id, {"read": 0, "accepted": 0})
        read, accepted = int(s["read"] or 0), int(s["accepted"] or 0)
        return read > 0 and (read - accepted) / read > max_ratio

    first = _evaluate(spark, settings, contract, batches)
    evaluations = [first]
    try:
        blocked = {b["batch_id"] for b in batches if is_blocked(first, b["batch_id"])}
        mergeable = [b for b in batches if b["batch_id"] not in blocked]
        final = first
        if blocked and mergeable:
            # Dedup ran across all batches: a blocked batch may hold the newest version of a key and
            # have demoted a valid older one to "duplicate". Re-evaluate without the blocked batches.
            final = _evaluate(spark, settings, contract, mergeable)
            evaluations.append(final)

        def source_of(batch_id: str) -> _Evaluation:
            return first if batch_id in blocked else final

        merge_info: dict[str, Any] = {}
        if mergeable:
            ids = [b["batch_id"] for b in mergeable]
            source = accepted_rows(final.conformed.where(F.col("_batch_id").isin(ids)), contract, run.run_id)
            merge_info = merge_accepted(spark, silver_path(settings, entity), source, contract)
        faults.checkpoint("silver.after_merge")
        rejects = rejected_rows(final.conformed, contract, run.run_id) if mergeable else None
        if blocked:
            blocked_rejects = rejected_rows(first.conformed.where(F.col("_batch_id").isin(sorted(blocked))),
                                            contract, run.run_id)  # fmt: skip
            rejects = blocked_rejects if rejects is None else rejects.unionByName(blocked_rejects)
        assert rejects is not None
        write_rejects(spark, rejects_path(settings), rejects, entity, [b["batch_id"] for b in batches])
        faults.checkpoint("silver.after_rejects")

        money = MONEY_COLUMN.get(entity)
        results: list[SilverBatchResult] = []
        quality: list[RuleResult] = []
        for batch in batches:
            batch_id = batch["batch_id"]
            evaluation = source_of(batch_id)
            s = evaluation.stats.get(batch_id, {"read": 0, "accepted": 0})
            read, accepted = int(s["read"] or 0), int(s["accepted"] or 0)
            result = SilverBatchResult(entity, batch_id, read, accepted, read - accepted,
                                       blocked=batch_id in blocked, reasons=evaluation.reasons.get(batch_id, {}))  # fmt: skip
            results.append(result)
            quality += batch_quality_results(contract, settings, result)
            metrics: dict[str, Any] = {"reasons": result.reasons, "blocked": result.blocked, **merge_info}
            if money:
                metrics["accepted_amount"] = str(s.get("accepted_amount") or Decimal("0"))
                metrics["rejected_amount"] = str(s.get("rejected_amount") or Decimal("0"))
            repo.set_layer_status(
                batch_id,
                "silver",
                "FAILED" if result.blocked else "SUCCEEDED",
                run.run_id,
                records_read=result.records_read,
                records_accepted=0 if result.blocked else result.accepted,
                records_rejected=result.rejected,
                delta_version=merge_info.get("delta_version"),
                metrics=metrics,
                error_message=f"reject ratio above {max_ratio:.0%}: batch blocked"
                if result.blocked
                else None,
            )
            run.add(read=result.records_read, written=0 if result.blocked else result.accepted,
                    rejected=result.rejected)  # fmt: skip
            log_event(log, "silver_batch_processed", batch_id=batch_id, records_read=result.records_read,
                      accepted=result.accepted, rejected=result.rejected, blocked=result.blocked,
                      reasons=result.reasons)  # fmt: skip
        repo.save_quality_results([q.to_record(run.run_id) for q in quality])
    finally:
        for evaluation in evaluations:
            evaluation.conformed.unpersist()

    failures = [q for q in quality if q.is_blocking_failure]
    if failures:
        raise QualityGateError(failures)
    return results


def batch_quality_results(
    contract: EntityContract, settings: Any, result: SilverBatchResult
) -> list[RuleResult]:
    """Row-rule counts for the batch (every rule, including those with zero violations) plus the
    blocking reject-ratio gate."""
    out = []
    for reason, (rule_id, rule) in row_rule_catalog(contract, settings.business_rules).items():
        out.append(
            RuleResult(
                rule_id=rule_id,
                rule_name=reason,
                description=rule.description + " Violating records are rejected.",
                category=rule.category,
                layer="silver",
                table_name=f"silver.{contract.name}",
                severity=Severity.WARNING,
                records_evaluated=result.records_read,
                violations=result.reasons.get(reason, 0),
                batch_id=result.batch_id,
            )
        )
    out.append(
        RuleResult(
            rule_id=f"SLV-{contract.name.upper()}-REJECT-RATIO",
            rule_name=f"{contract.name} reject ratio",
            description=f"At most {settings.quality.max_reject_ratio:.0%} of a batch may be rejected; "
            "above that the batch is blocked and not published to Silver.",
            category="validity",
            layer="silver",
            table_name=f"silver.{contract.name}",
            severity=Severity.BLOCKING,
            records_evaluated=result.records_read,
            violations=result.rejected,
            threshold_pct=settings.quality.max_reject_ratio * 100,
            batch_id=result.batch_id,
        )
    )
    return out


def run_silver(
    spark: SparkSession, run: RunHandle, entities: list[str], batch_ids: list[str] | None = None
) -> list[SilverBatchResult]:
    results: list[SilverBatchResult] = []
    for entity in entities:
        contract = get_contract(entity)
        with bind(entity=entity), run.repo.lock(f"silver:{entity}", run.settings.pipeline.lock_ttl_seconds):
            batches = [
                b
                for b in run.repo.pending_batches("silver", entity)
                if not batch_ids or b["batch_id"] in batch_ids
            ]
            if not batches:
                log_event(log, "silver_nothing_pending", entity=entity)
                continue
            try:
                results.extend(process_entity(spark, run, contract, batches))
            except QualityGateError:
                raise
            except Exception as exc:
                for batch in batches:
                    run.repo.set_layer_status(batch["batch_id"], "silver", "FAILED", run.run_id,
                                              error_message=f"{type(exc).__name__}: {exc}")  # fmt: skip
                raise
    return results


def reset_for_reprocess(
    spark: SparkSession, run: RunHandle, entity: str, batch_ids: list[str] | None, full_refresh: bool
) -> list[str]:
    """Mark Bronze-committed batches as pending for Silver again.

    * targeted (``batch_ids`` or date range resolved by the caller): replays those batches; the MERGE
      guard keeps newer data intact, so replaying old batches is safe;
    * ``full_refresh``: empties the Silver table and the entity's rejects, then replays every batch,
      rebuilding the entity from Bronze (needed when a rule change must *remove* accepted records).
    """
    from delta.tables import DeltaTable

    settings, repo = run.settings, run.repo
    contract = get_contract(entity)
    with repo.lock(f"silver:{entity}", settings.pipeline.lock_ttl_seconds):
        batches = repo.batches_for_reprocess(entity, None if full_refresh else batch_ids)
        if full_refresh:
            ensure_silver_tables(spark, settings, contract)
            DeltaTable.forPath(spark, silver_path(settings, entity)).delete()
            DeltaTable.forPath(spark, rejects_path(settings)).delete(F.col("entity") == entity)
        for batch in batches:
            repo.set_layer_status(batch["batch_id"], "silver", "PENDING", run.run_id)
        log_event(
            log, "silver_reprocess_scheduled", entity=entity, batches=len(batches), full_refresh=full_refresh
        )
        return [b["batch_id"] for b in batches]
