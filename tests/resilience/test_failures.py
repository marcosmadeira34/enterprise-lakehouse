"""Failure injection, re-execution and recovery scenarios (local Spark + Delta + SQLite).

Each scenario asserts the invariants that matter: no duplicated data, consistent metadata, failures
recorded and investigable, and a clean outcome after the retry.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import functions as F

from lakehouse.common.faults import InjectedFailure
from lakehouse.common.storage import LocalStore
from lakehouse.generator.retail import GeneratorConfig, generate
from lakehouse.metadata.repository import LockNotAcquiredError, MetadataRepository
from lakehouse.quality.framework import QualityGateError
from tests.conftest import load_manifest, make_settings, platform_for

pytestmark = [pytest.mark.spark, pytest.mark.resilience]


def _fresh(tmp_path: Path, spark: Any, **overrides: Any) -> tuple[Any, MetadataRepository, Any]:
    settings = make_settings(tmp_path, **overrides)
    repo = MetadataRepository.from_url(settings.metadata.url)
    return settings, repo, platform_for(settings, repo, spark)


def _count(spark: Any, settings: Any, layer: str, table: str) -> int:
    return int(spark.read.format("delta").load(settings.table_uri(layer, table)).count())


def _write(settings: Any, entity: str, name: str, text: str) -> None:
    path = Path(settings.storage.landing_uri) / entity / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


STORES_CSV = "store_id,store_name,city,state,store_format,opened_date,updated_at\nS001,Loja,Recife,PE,MALL,2020-01-01,2026-07-01T00:00:00\n"


def test_crash_between_bronze_commit_and_metadata_update(
    tmp_path: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, repo, platform = _fresh(tmp_path, spark)
    generate(LocalStore(), settings.storage.landing_uri, GeneratorConfig(scale="tiny"), drop=1)
    platform.ingest(["customers"])
    monkeypatch.setenv("LAKEHOUSE_FAIL_AT", "bronze.after_delta_commit")
    with pytest.raises(InjectedFailure):
        platform.bronze(["customers"])
    committed = _count(spark, settings, "bronze", "customers")
    batch = repo.layer_rows("bronze", "customers")[0]
    assert committed > 0  # data reached Delta...
    assert batch["status"] == "FAILED"  # ...but the operational state did not
    assert {f["status"] for f in repo.list_files("customers")} == {"PENDING"}
    failed_run = repo.list_runs(pipeline="bronze")[0][0]
    assert failed_run["status"] == "FAILED" and failed_run["error_type"] == "InjectedFailure"
    assert repo.get_run(failed_run["run_id"])["errors"]  # type: ignore[index]

    monkeypatch.delenv("LAKEHOUSE_FAIL_AT")
    retry = platform.bronze(["customers"])
    assert retry["batches"][0]["recovered"] is True
    assert _count(spark, settings, "bronze", "customers") == committed  # not appended twice
    assert {f["status"] for f in repo.list_files("customers")} == {"COMMITTED"}
    assert repo.layer_rows("bronze", "customers")[0]["records_read"] == committed


def test_crash_after_silver_merge_then_retry_is_consistent(
    tmp_path: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, repo, platform = _fresh(tmp_path, spark)
    generate(LocalStore(), settings.storage.landing_uri, GeneratorConfig(scale="tiny"), drop=1)
    expected = load_manifest(settings, 1)["expected"]
    platform.ingest(["customers"])
    platform.bronze(["customers"])
    monkeypatch.setenv("LAKEHOUSE_FAIL_AT", "silver.after_merge")
    with pytest.raises(InjectedFailure):
        platform.silver(["customers"])
    assert _count(spark, settings, "silver", "customers") == expected["silver_counts"]["customers"]
    assert repo.layer_rows("silver", "customers")[0]["status"] == "FAILED"

    monkeypatch.delenv("LAKEHOUSE_FAIL_AT")
    platform.silver(["customers"])
    platform.silver(["customers"])  # nothing pending: no-op
    assert _count(spark, settings, "silver", "customers") == expected["silver_counts"]["customers"]
    rejected = expected["rejection_counts"]["customers"]
    assert spark.read.format("delta").load(settings.table_uri("silver", "_rejects")).count() == rejected
    layer = repo.layer_rows("silver", "customers")[0]
    assert layer["status"] == "SUCCEEDED" and layer["attempts"] == 2
    assert platform.reconcile(["customers"])["results"]["not_passed"] == []


def test_lock_held_by_another_process_is_refused_and_recorded(tmp_path: Path, spark: Any) -> None:
    _settings, repo, platform = _fresh(tmp_path, spark)
    assert repo.acquire_lock("silver:sales", "other-worker", 600)
    with pytest.raises(LockNotAcquiredError):
        platform.silver(["sales"])
    assert repo.list_runs(pipeline="silver")[0][0]["error_type"] == "LockNotAcquiredError"


def test_batch_above_reject_threshold_is_blocked_not_published(tmp_path: Path, spark: Any) -> None:
    settings, repo, platform = _fresh(tmp_path, spark, quality={"max_reject_ratio": 0.25})
    _write(settings, "stores", "stores.csv", STORES_CSV)
    header = "sale_id,store_id,customer_id,sale_ts,transaction_type,status,total_amount,currency,original_sale_id,updated_at\n"
    good = "T1,S001,,2026-07-01T10:00:00,SALE,COMPLETED,10.00,BRL,,2026-07-01T10:05:00\n"
    bad = "".join(
        f"T{i},S001,,not-a-date,SALE,COMPLETED,10.00,BRL,,2026-07-01T10:05:00\n" for i in range(2, 5)
    )
    _write(settings, "sales", "sales.csv", header + good + bad)
    platform.ingest(["stores", "sales"])
    platform.bronze(["stores", "sales"])
    platform.silver(["stores"])
    with pytest.raises(QualityGateError, match="SLV-SALES-REJECT-RATIO"):
        platform.silver(["sales"])
    layer = repo.layer_rows("silver", "sales")[0]
    assert layer["status"] == "FAILED" and "blocked" in layer["error_message"]
    assert layer["records_rejected"] == 3 and layer["records_accepted"] == 0
    assert _count(spark, settings, "silver", "sales") == 0  # the valid record was not published either
    assert spark.read.format("delta").load(settings.table_uri("silver", "_rejects")).count() == 3


def test_unreadable_and_incompatible_files_do_not_block_the_batch(tmp_path: Path, spark: Any) -> None:
    settings, repo, platform = _fresh(tmp_path, spark)
    _write(settings, "stores", "a.csv", STORES_CSV)
    _write(settings, "stores", "b_incompatible.csv", "store_name,city\nLoja,Recife\n")
    corrupt = Path(settings.storage.landing_uri) / "inventory" / "broken.parquet"
    corrupt.parent.mkdir(parents=True)
    corrupt.write_bytes(b"PAR1 this is not a parquet file")
    platform.ingest(["stores", "inventory"])
    platform.bronze(["stores", "inventory"])
    statuses = {f["file_name"]: f["status"] for f in repo.list_files()}
    assert statuses == {
        "a.csv": "COMMITTED",
        "b_incompatible.csv": "QUARANTINED",
        "broken.parquet": "QUARANTINED",
    }
    assert _count(spark, settings, "bronze", "stores") == 1
    errors = [e for r in repo.list_runs(pipeline="bronze")[0] for e in repo.get_run(r["run_id"])["errors"]]  # type: ignore[index]
    assert any(e["entity"] == "inventory" for e in errors)  # the unreadable file left an investigable error


# ---------------------------------------------------------------------- scenarios on a full drop-1 lake
@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory, spark: Any) -> dict[str, Any]:
    settings = make_settings(tmp_path_factory.mktemp("resilience"))
    repo = MetadataRepository.from_url(settings.metadata.url)
    platform = platform_for(settings, repo, spark)
    generate(LocalStore(), settings.storage.landing_uri, GeneratorConfig(scale="tiny"), drop=1)
    platform.run_all()
    return {"settings": settings, "repo": repo, "platform": platform, "manifest": load_manifest(settings, 1)}


def test_late_reference_data_is_detected_and_fixed_by_historical_reprocessing(
    lake: dict[str, Any], spark: Any
) -> None:
    settings, platform = lake["settings"], lake["platform"]
    orphans = [r for r in lake["manifest"]["expected"]["rejections"] if r["reason"] == "ORPHAN_PRODUCT"]
    rejects = spark.read.format("delta").load(settings.table_uri("silver", "_rejects"))
    product_ids = sorted(
        row["product_id"]
        for row in rejects.where("rejection_reason = 'ORPHAN_PRODUCT'")
        .select(F.get_json_object("raw_record", "$.product_id").alias("product_id"))
        .distinct()
        .collect()
    )
    assert len(product_ids) == len(orphans)
    fact_before = _count(spark, settings, "gold", "fact_sales_item")
    # The missing products are registered later.
    lines = [json.dumps({"product_id": p, "sku": f"SKU-{p}", "product_name": f"Late {p}", "category_id": "CAT001",
                         "unit_price": 1.0, "unit_cost": 0.5, "is_active": True, "updated_at": "2026-09-10T00:00:00"})
             for p in product_ids]  # fmt: skip
    _write(settings, "products", "products_late.json", "\n".join(lines) + "\n")
    platform.ingest(["products"])
    platform.bronze(["products"])
    platform.silver(["products"])

    # Reconciliation notices that Silver no longer matches what Bronze implies.
    with pytest.raises(QualityGateError, match="REC-SALE-ITEMS-SILVER-STATE"):
        platform.reconcile(["sale_items"])

    summary = platform.reprocess(["sale_items"], since="2000-01-01")
    assert summary["gold"]["status"] == "PUBLISHED"
    assert platform.rejects("sale_items", reason="ORPHAN_PRODUCT") == []
    assert _count(spark, settings, "gold", "fact_sales_item") == fact_before + len(orphans)
    assert platform.reconcile()["results"]["not_passed"] == []


def test_tampering_with_silver_is_detected_and_repaired(lake: dict[str, Any], spark: Any) -> None:
    from delta.tables import DeltaTable

    settings, platform = lake["settings"], lake["platform"]
    sales = DeltaTable.forPath(spark, settings.table_uri("silver", "sales"))
    victim = sales.toDF().where("status = 'COMPLETED'").orderBy("sale_id").first()["sale_id"]
    sales.delete(F.col("sale_id") == victim)  # an out-of-band change nobody recorded
    with pytest.raises(QualityGateError) as excinfo:
        platform.reconcile()
    failed = {f.rule_id for f in excinfo.value.failures}
    assert {"REC-SALES-SILVER-STATE", "REC-GOLD-FACT-COUNT", "REC-GOLD-FACT-AMOUNT"} <= failed

    platform.reprocess(["sales"], full_refresh=True)
    assert platform.reconcile()["results"]["not_passed"] == []


def test_crash_after_gold_write_keeps_previous_publication(
    lake: dict[str, Any], spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, repo, platform = lake["settings"], lake["repo"], lake["platform"]
    published = repo.latest_publication()
    assert published is not None
    monkeypatch.setenv("LAKEHOUSE_FAIL_AT", "gold.after_tables_written")
    with pytest.raises(InjectedFailure):
        platform.gold()
    # Tables have newer versions, but consumers still resolve the previous publication.
    assert repo.latest_publication()["id"] == published["id"]  # type: ignore[index]
    newest = spark.sql(
        f"DESCRIBE HISTORY delta.`{settings.table_uri('gold', 'fact_sales_item')}` LIMIT 1"
    ).first()
    assert newest["version"] > published["table_versions"]["gold.fact_sales_item"]

    monkeypatch.delenv("LAKEHOUSE_FAIL_AT")
    result = platform.gold()
    assert result["status"] == "PUBLISHED" and result["publication_id"] > published["id"]
    assert result["row_counts"] == published["row_counts"]
