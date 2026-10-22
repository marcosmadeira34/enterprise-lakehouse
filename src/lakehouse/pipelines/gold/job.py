"""Gold: rebuild the star schema from a pinned Silver snapshot, audit it, then publish.

Write-Audit-Publish:

1. pin the current version of every Silver input (all reads use that snapshot);
2. overwrite each Gold table (atomic per table; previous versions stay readable by time travel);
3. run the BLOCKING Gold rules against exactly the versions just written;
4. record a publication: PUBLISHED (consumers switch to these versions) or BLOCKED (consumers keep
   the previous publication).

Consumers (the API) read the versions named by the latest PUBLISHED row, never "latest", so a failed
or crashed rebuild cannot expose a half-updated or wrong set of tables.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from lakehouse.common import faults
from lakehouse.common.spark import commit_metadata, ensure_table, latest_version, read_delta, spark_path_for
from lakehouse.observability.logging import log_event
from lakehouse.pipelines.gold import model
from lakehouse.pipelines.runtime import RunHandle
from lakehouse.quality.framework import QualityGateError, blocking_failures, run_rules
from lakehouse.quality.rules import QualityContext, gold_rules

log = logging.getLogger(__name__)

SILVER_INPUTS = ("categories", "products", "customers", "stores", "sales", "sale_items", "inventory")


@dataclass(frozen=True)
class GoldTable:
    name: str
    build: Callable[[dict[str, DataFrame], SparkSession, str], DataFrame]
    partition_by: tuple[str, ...] = ()
    description: str = ""


def _fact(s: dict[str, DataFrame], _spark: SparkSession, run_id: str) -> DataFrame:
    return model.fact_sales_item(s["sales"], s["sale_items"], s["products"], s["customers"], run_id)


GOLD_TABLES: tuple[GoldTable, ...] = (
    GoldTable("dim_date", lambda s, _sp, _r: model.dim_date(s["sales"]), description="Calendar of the sales period."),
    GoldTable("dim_customer", lambda s, sp, _r: model.dim_customer(s["customers"], sp),
              description="Customers without direct identifiers, plus ANONYMOUS/UNKNOWN members."),
    GoldTable("dim_product", lambda s, _sp, _r: model.dim_product(s["products"], s["categories"]),
              description="Products with category hierarchy."),
    GoldTable("dim_store", lambda s, _sp, _r: model.dim_store(s["stores"]), description="Stores."),
    GoldTable("fact_sales_item", _fact, ("sale_month",), "Completed sale/return lines (grain: sale line)."),
    GoldTable("fact_inventory_snapshot", lambda s, _sp, _r: model.fact_inventory_snapshot(s["inventory"]),
              description="Daily stock per store and product."),
)  # fmt: skip


@dataclass
class GoldResult:
    status: str
    publication_id: int
    table_versions: dict[str, int] = field(default_factory=dict)
    source_versions: dict[str, int] = field(default_factory=dict)
    row_counts: dict[str, int] = field(default_factory=dict)
    quality: list[Any] = field(default_factory=list)


def gold_path(settings: Any, table: str) -> str:
    return spark_path_for(settings.table_uri("gold", table))


def _write(df: DataFrame, path: str, table: GoldTable, spark: SparkSession, metadata: str) -> None:
    ensure_table(spark, path, df.schema, partition_by=table.partition_by,
                 properties={"lakehouse.layer": "gold"}, comment=table.description)  # fmt: skip
    writer = df.write.format("delta").mode("overwrite").option("userMetadata", metadata)
    if table.partition_by:
        writer = writer.partitionBy(*table.partition_by)
    writer.save(path)


def run_gold(spark: SparkSession, run: RunHandle) -> GoldResult:
    settings, repo = run.settings, run.repo
    with repo.lock("gold", settings.pipeline.lock_ttl_seconds):
        run.step = "gold:pin_sources"
        source_versions: dict[str, int] = {}
        for entity in SILVER_INPUTS:
            version = latest_version(spark, spark_path_for(settings.table_uri("silver", entity)))
            if version is None:
                raise RuntimeError(f"silver.{entity} does not exist yet: run the Silver step first")
            source_versions[f"silver.{entity}"] = version
        silver = {
            e: read_delta(
                spark, spark_path_for(settings.table_uri("silver", e)), source_versions[f"silver.{e}"]
            )
            for e in SILVER_INPUTS
        }
        metadata = commit_metadata(run_id=run.run_id, source_versions=source_versions)

        # The fact feeds the aggregate: persist it once instead of recomputing the joins.
        fact = _fact(silver, spark, run.run_id).persist()
        built: dict[str, DataFrame] = {}
        try:
            for table in GOLD_TABLES:
                run.step = f"gold:{table.name}"
                df = fact if table.name == "fact_sales_item" else table.build(silver, spark, run.run_id)
                _write(df, gold_path(settings, table.name), table, spark, metadata)
                built[table.name] = df
            run.step = "gold:agg_daily_sales"
            agg_table = GoldTable(
                "agg_daily_sales", lambda *_: fact, description="Daily sales KPIs per store."
            )
            _write(
                model.agg_daily_sales(fact),
                gold_path(settings, "agg_daily_sales"),
                agg_table,
                spark,
                metadata,
            )
        finally:
            fact.unpersist()
        faults.checkpoint("gold.after_tables_written")

        names = [t.name for t in GOLD_TABLES] + ["agg_daily_sales"]
        table_versions = {f"gold.{n}": int(latest_version(spark, gold_path(settings, n)) or 0) for n in names}
        row_counts = {
            f"gold.{n}": int(read_delta(spark, gold_path(settings, n), table_versions[f"gold.{n}"]).count())
            for n in names
        }

        run.step = "gold:audit"
        context = QualityContext(spark, settings, versions={**source_versions, **table_versions})
        results = run_rules(gold_rules(), context)
        repo.save_quality_results([r.to_record(run.run_id) for r in results])
        failures = blocking_failures(results)
        status = "BLOCKED" if failures else "PUBLISHED"
        publication_id = repo.record_publication(
            run.run_id, status, table_versions, source_versions, row_counts, [r.rule_id for r in failures]
        )
        if status == "PUBLISHED":
            repo.upsert_metric_definitions(model.METRICS)
        run.add(written=sum(row_counts.values()))
        log_event(log, "gold_publication", level=logging.INFO if not failures else logging.ERROR,
                  status=status, publication_id=publication_id, table_versions=table_versions,
                  row_counts=row_counts)  # fmt: skip
        if failures:
            raise QualityGateError(failures)
        return GoldResult(status, publication_id, table_versions, source_versions, row_counts, results)
