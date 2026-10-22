"""Cross-layer reconciliation.

1. **Batch counts** (per entity): what the metadata says each batch read/accepted/rejected must match
   what is physically in Delta — Bronze rows of the batch and rejects of the batch.
2. **Silver state** (per entity): Silver is re-derived from Bronze with the same conformance code and
   compared on row count, monetary total and a content fingerprint. Any lost/duplicated merge,
   partial failure or manual change to Silver shows up here. A divergence can also mean reference
   data arrived late (e.g. a product registered after its sales were rejected): the remedy is
   ``lakehouse reprocess``.
3. **Gold vs Silver**: the Gold reconciliation rules, re-evaluated against the published versions.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.contracts import EntityContract, get_contract
from lakehouse.metadata.repository import MetadataRepository
from lakehouse.pipelines.silver.job import DIMENSIONS, REJECTS_TABLE, bronze_input, parent_keys
from lakehouse.pipelines.silver.transform import MONEY_COLUMN, conform
from lakehouse.quality.framework import Category, DatasetRule, Outcome, Severity
from lakehouse.quality.rules import QualityContext


def _batch_counts_check(repo: MetadataRepository, entity: str) -> Any:
    def check(ctx: QualityContext) -> Outcome:
        layers = [r for r in repo.layer_rows("silver", entity) if r["status"] in {"SUCCEEDED", "FAILED"}]
        bronze_meta = {r["batch_id"]: r for r in repo.layer_rows("bronze", entity)}
        if not layers:
            return Outcome(0, 0, details={"reason": "no processed batches"})
        ids = [r["batch_id"] for r in layers]
        bronze = {
            r["_batch_id"]: int(r["count"])
            for r in ctx.table("bronze", entity)
            .where(F.col("_batch_id").isin(ids))
            .groupBy("_batch_id")
            .count()
            .collect()
        }
        rejects = {
            r["_batch_id"]: int(r["count"])
            for r in ctx.table("silver", REJECTS_TABLE)
            .where((F.col("entity") == entity) & F.col("_batch_id").isin(ids))
            .groupBy("_batch_id")
            .count()
            .collect()
        }
        mismatches = []
        for layer in layers:
            batch_id = layer["batch_id"]
            in_bronze, in_rejects = bronze.get(batch_id, 0), rejects.get(batch_id, 0)
            problems = []
            if int(bronze_meta[batch_id]["records_read"] or 0) != in_bronze:
                problems.append(
                    f"bronze metadata {bronze_meta[batch_id]['records_read']} != delta {in_bronze}"
                )
            if int(layer["records_read"] or 0) != in_bronze:
                problems.append(f"silver read {layer['records_read']} != bronze delta {in_bronze}")
            if int(layer["records_rejected"] or 0) != in_rejects:
                problems.append(f"rejected {layer['records_rejected']} != rejects delta {in_rejects}")
            if (
                layer["status"] == "SUCCEEDED"
                and int(layer["records_accepted"] or 0) + in_rejects != in_bronze
            ):
                problems.append(
                    f"accepted {layer['records_accepted']} + rejects {in_rejects} != bronze {in_bronze}"
                )
            if problems:
                mismatches.append({"batch_id": batch_id, "problems": problems})
        return Outcome(len(layers), len(mismatches), details={"mismatches": mismatches[:20]})

    return check


def _fingerprint(df: DataFrame, contract: EntityContract, money: str | None) -> dict[str, Any]:
    # Hash of the typed business columns: detects content changes that keep counts and totals intact.
    content = F.xxhash64(*[F.col(c) for c in contract.column_names])
    aggs = [F.count(F.lit(1)).alias("rows"), F.sum(content).alias("fingerprint")]
    if money:
        aggs.append(F.sum(money).alias("amount"))
    row = df.agg(*aggs).first()
    assert row is not None
    return {
        "rows": int(row["rows"]),
        "fingerprint": int(row["fingerprint"] or 0),
        "amount": Decimal(str(row["amount"])) if money and row["amount"] is not None else Decimal("0"),
    }


def _silver_state_check(repo: MetadataRepository, contract: EntityContract) -> Any:
    def check(ctx: QualityContext) -> Outcome:
        batches = [
            {"batch_id": r["batch_id"], "ingestion_date": r["ingestion_date"]}
            for r in repo.layer_rows("silver", contract.name, status="SUCCEEDED")
        ]
        actual_df = ctx.table("silver", contract.name)
        money = MONEY_COLUMN.get(contract.name)
        actual = _fingerprint(actual_df, contract, money)
        if not batches:
            return Outcome(actual["rows"], actual["rows"], observed=actual["rows"], expected=0)
        expected_df = conform(
            bronze_input(ctx.spark, ctx.settings, contract.name, batches),
            contract,
            ctx.settings.business_rules,
            parent_keys(ctx.spark, ctx.settings, contract),
            broadcast=DIMENSIONS,
        ).where(F.size("_reasons") == 0)
        expected = _fingerprint(expected_df, contract, money)
        differences = {
            k: {"silver": str(actual[k]), "derived_from_bronze": str(expected[k])}
            for k in actual
            if actual[k] != expected[k]
        }
        evaluated = max(actual["rows"], expected["rows"])
        violations = abs(actual["rows"] - expected["rows"]) or (1 if differences else 0)
        return Outcome(evaluated, violations, observed=actual["amount"] if money else actual["rows"],
                       expected=expected["amount"] if money else expected["rows"],
                       details={"differences": differences} if differences else None)  # fmt: skip

    return check


def reconciliation_rules(repo: MetadataRepository, entities: list[str]) -> list[DatasetRule]:
    rules = []
    for entity in entities:
        contract = get_contract(entity)
        upper = entity.upper().replace("_", "-")
        rules.append(
            DatasetRule(
                rule_id=f"REC-{upper}-BATCH-COUNTS",
                name=f"{entity} batch counts bronze -> silver",
                description="Per batch: metadata counts equal Delta counts and bronze = accepted + rejected.",
                category=Category.RECONCILIATION,
                layer="silver",
                table_name=f"silver.{entity}",
                severity=Severity.BLOCKING,
                check=_batch_counts_check(repo, entity),
            )
        )
        rules.append(
            DatasetRule(
                rule_id=f"REC-{upper}-SILVER-STATE",
                name=f"{entity} silver state vs bronze",
                description="Silver equals the latest valid version per key re-derived from Bronze "
                "(row count, monetary total and content fingerprint).",
                category=Category.RECONCILIATION,
                layer="silver",
                table_name=f"silver.{entity}",
                severity=Severity.BLOCKING,
                check=_silver_state_check(repo, contract),
            )
        )
    return rules
