"""End-to-end pipeline on local Spark + local Delta + SQLite metadata, checked against the generator's
manifest (the oracle). Drop 1 is the initial load; drop 2 brings updates, late sales, a schema change,
an incompatible file and a re-sent file.

Tests in this module share state and run in file order: drop-1 checks come before the drop-2 fixture.
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import functions as F

from lakehouse.common.storage import LocalStore
from lakehouse.contracts import ENTITY_ORDER
from lakehouse.generator.retail import GeneratorConfig, generate
from lakehouse.metadata.repository import MetadataRepository
from tests.conftest import load_manifest, make_settings, platform_for

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def env(tmp_path_factory: pytest.TempPathFactory, spark: Any) -> dict[str, Any]:
    settings = make_settings(tmp_path_factory.mktemp("pipeline"))
    repo = MetadataRepository.from_url(settings.metadata.url)
    platform = platform_for(settings, repo, spark)
    generate(LocalStore(), settings.storage.landing_uri, GeneratorConfig(scale="tiny"), drop=1)
    first = platform.run_all()
    return {"settings": settings, "repo": repo, "platform": platform, "spark": spark, "first": first,
            "manifest": load_manifest(settings, 1)}  # fmt: skip


def _table(env: dict[str, Any], layer: str, name: str) -> Any:
    return env["spark"].read.format("delta").load(env["settings"].table_uri(layer, name))


def _silver_counts(env: dict[str, Any]) -> dict[str, int]:
    return {e: _table(env, "silver", e).count() for e in ENTITY_ORDER}


def _latest_results(repo: MetadataRepository) -> dict[str, dict[str, Any]]:
    return {r["rule_id"]: r for r in repo.latest_quality_status()}


def _gold_totals(env: dict[str, Any]) -> dict[str, Any]:
    fact = _table(env, "gold", "fact_sales_item")
    agg = _table(env, "gold", "agg_daily_sales").agg(F.sum("orders").alias("orders")).first()
    net = fact.agg(F.sum("net_amount").alias("n")).first()["n"]
    return {"fact_rows": fact.count(), "net_revenue": str(net), "orders": int(agg["orders"])}


# ---------------------------------------------------------------------- drop 1
def test_silver_holds_exactly_the_valid_world(env: dict[str, Any]) -> None:
    assert _silver_counts(env) == env["manifest"]["expected"]["silver_counts"]


def test_every_injected_anomaly_is_rejected_with_its_reason(env: dict[str, Any]) -> None:
    rejects = _table(env, "silver", "_rejects").collect()
    expected = env["manifest"]["expected"]["rejections"]
    assert Counter((r["entity"], r["rejection_reason"]) for r in rejects) == Counter(
        (e["entity"], e["reason"]) for e in expected
    )
    actual_keys = {(r["entity"], r["business_key"], r["rejection_reason"]) for r in rejects}
    for item in expected:
        if item["business_key"] is not None:
            assert (item["entity"], item["business_key"], item["reason"]) in actual_keys
    assert all(r["raw_record"] and r["rejection_reasons"] for r in rejects)


def test_warning_rules_detect_injected_inconsistencies(env: dict[str, Any]) -> None:
    latest = _latest_results(env["repo"])
    for rule_id, violations in env["manifest"]["expected"]["warnings"].items():
        assert latest[rule_id]["violations"] == violations, rule_id
        assert latest[rule_id]["status"] == ("WARNED" if violations else "PASSED"), rule_id


def test_gold_matches_oracle_and_is_published(env: dict[str, Any]) -> None:
    assert _gold_totals(env) == env["manifest"]["expected"]["gold"]
    publication = env["repo"].latest_publication()
    assert publication is not None and publication["status"] == "PUBLISHED"
    assert publication["source_versions"].keys() >= {"silver.sales", "silver.sale_items"}
    customers = _table(env, "gold", "dim_customer")
    assert "email" not in customers.columns and "first_name" not in customers.columns  # no PII in Gold


def test_reconciliation_and_batch_accounting(env: dict[str, Any]) -> None:
    assert env["first"]["reconcile"]["results"]["not_passed"] == []
    for row in env["repo"].layer_rows("silver"):
        assert row["status"] == "SUCCEEDED"
        assert row["records_read"] == row["records_accepted"] + row["records_rejected"]
    bronze_rows = {e: _table(env, "bronze", e).count() for e in ENTITY_ORDER}
    files = sum(f["rows"] for f in env["manifest"]["files"])
    assert sum(bronze_rows.values()) == files  # Bronze keeps every record, duplicates included


def test_rejects_are_queryable_with_reasons(env: dict[str, Any]) -> None:
    rows = env["platform"].rejects("sale_items", reason="ORPHAN_PRODUCT")
    assert rows and all("ORPHAN_PRODUCT" in r["rejection_reasons"] for r in rows)
    assert all(r["_source_file"].endswith(".parquet") for r in rows)


def test_rerun_is_idempotent(env: dict[str, Any]) -> None:
    before = {"silver": _silver_counts(env), "gold": _gold_totals(env),
              "rejects": _table(env, "silver", "_rejects").count(),
              "bronze": {e: _table(env, "bronze", e).count() for e in ENTITY_ORDER}}  # fmt: skip
    again = env["platform"].run_all()
    assert all(e["batch_id"] is None for e in again["ingest"]["entities"])
    assert again["bronze"]["batches"] == [] and again["silver"]["batches"] == []
    after = {"silver": _silver_counts(env), "gold": _gold_totals(env),
             "rejects": _table(env, "silver", "_rejects").count(),
             "bronze": {e: _table(env, "bronze", e).count() for e in ENTITY_ORDER}}  # fmt: skip
    assert after == before
    assert again["reconcile"]["results"]["not_passed"] == []


def test_replaying_a_processed_batch_changes_nothing(env: dict[str, Any]) -> None:
    sales_batch = env["repo"].layer_rows("silver", "sales")[0]["batch_id"]
    before = {r["sale_id"]: r["updated_at"] for r in _table(env, "silver", "sales").collect()}
    env["platform"].reprocess(["sales"], batch_ids=[sales_batch], publish_gold=False)
    after = {r["sale_id"]: r["updated_at"] for r in _table(env, "silver", "sales").collect()}
    assert after == before
    assert _table(env, "silver", "_rejects").where("entity = 'sales'").count() == sum(
        1 for r in env["manifest"]["expected"]["rejections"] if r["entity"] == "sales"
    )


# ---------------------------------------------------------------------- drop 2
@pytest.fixture(scope="module")
def drop2(env: dict[str, Any]) -> dict[str, Any]:
    generate(LocalStore(), env["settings"].storage.landing_uri, GeneratorConfig(scale="tiny"), drop=2)
    result = env["platform"].run_all()
    return {"result": result, "manifest": load_manifest(env["settings"], 2)}


def test_schema_change_is_captured_and_incompatible_file_quarantined(
    env: dict[str, Any], drop2: dict[str, Any]
) -> None:
    events = {
        (e["event_type"], tuple(e["columns"]), e["action"]) for e in env["repo"].schema_events("customers")
    }
    assert ("ADDED_COLUMNS", ("loyalty_tier",), "CAPTURED") in events
    assert ("MISSING_OPTIONAL", ("phone",), "NULL_FILLED") in events
    assert ("MISSING_REQUIRED", ("customer_id",), "QUARANTINED") in events
    files = {f["uri"]: f for f in env["repo"].list_files(limit=1000)}
    for uri in drop2["manifest"]["expected"]["quarantined_files"]:
        assert files[uri]["status"] == "QUARANTINED" and "customer_id" in files[uri]["error_message"]
    for uri in drop2["manifest"]["expected"]["duplicate_files"]:
        assert files[uri]["status"] == "DUPLICATE" and files[uri]["duplicate_of"] is not None
    extra = _table(env, "silver", "customers").where(F.col("_extra_columns").isNotNull()).first()
    assert extra is not None and "loyalty_tier" in extra["_extra_columns"]


def test_updates_applied_and_stale_versions_ignored(env: dict[str, Any], drop2: dict[str, Any]) -> None:
    cities = {r["customer_id"]: r["city"] for r in _table(env, "silver", "customers").collect()}
    for customer_id, city in drop2["manifest"]["expected"]["updated_customers"].items():
        assert cities[customer_id] == city
    for customer_id, city in drop2["manifest"]["expected"]["stale_customers"].items():
        assert cities[customer_id] == city  # the older out-of-order update did not overwrite


def test_drop2_state_and_gold_match_oracle(env: dict[str, Any], drop2: dict[str, Any]) -> None:
    expected = drop2["manifest"]["expected"]
    assert _silver_counts(env) == expected["silver_counts"]
    assert _gold_totals(env) == expected["gold"]
    latest = _latest_results(env["repo"])
    for rule_id, violations in expected["warnings"].items():
        assert latest[rule_id]["violations"] == violations, rule_id
    assert drop2["result"]["reconcile"]["results"]["not_passed"] == []
    assert Decimal(expected["gold"]["net_revenue"]) != Decimal(
        env["manifest"]["expected"]["gold"]["net_revenue"]
    )
