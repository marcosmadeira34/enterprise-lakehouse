"""Integration against the real Docker services: PostgreSQL metadata and MinIO (S3A from Spark,
boto3 for ingestion, delta-rs for the API). Nothing is mocked.

Run with ``make up`` then ``make test-integration`` (loads .env). Each session uses a throw-away
PostgreSQL database and unique S3 prefixes, so it never touches the stack's own data.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from lakehouse.common.storage import S3Store, get_store, join_uri
from lakehouse.config import Settings
from lakehouse.generator.retail import GeneratorConfig, generate
from lakehouse.metadata.repository import MetadataRepository

pytestmark = pytest.mark.integration


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"{name} not set: run `make up` and `make test-integration` (which loads .env)")
    return value


@pytest.fixture(scope="module")
def stack() -> Iterator[dict[str, Any]]:
    from sqlalchemy import create_engine, text

    admin_password = _require("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT", "15432")
    os.environ["AWS_ACCESS_KEY_ID"] = _require("MINIO_ROOT_USER")
    os.environ["AWS_SECRET_ACCESS_KEY"] = _require("MINIO_ROOT_PASSWORD")
    database = f"lakehouse_it_{uuid.uuid4().hex[:8]}"
    admin = create_engine(f"postgresql+psycopg://postgres:{admin_password}@127.0.0.1:{port}/postgres",
                          isolation_level="AUTOCOMMIT")  # fmt: skip
    with admin.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {database}"))
    prefix = f"it-{uuid.uuid4().hex[:8]}"
    settings = Settings.model_validate(
        {
            "storage": {
                "landing_uri": f"s3://landing/{prefix}",
                "lake_uri": f"s3://lakehouse/{prefix}",
                "s3_endpoint_url": f"http://127.0.0.1:{os.getenv('MINIO_API_PORT', '19010')}",
            },
            "metadata": {
                "url": f"postgresql+psycopg://postgres:{admin_password}@127.0.0.1:{port}/{database}"
            },
            "spark": {"master": "local[2]", "shuffle_partitions": 2, "driver_memory": "1g"},
        }
    )
    try:
        yield {"settings": settings, "prefix": prefix}
    finally:
        store = S3Store(settings.storage)
        for uri in (settings.storage.landing_uri, settings.storage.lake_uri):
            store.delete(uri + "/")
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
        admin.dispose()


@pytest.fixture(scope="module")
def pipeline(stack: dict[str, Any]) -> Iterator[dict[str, Any]]:
    from lakehouse.pipelines.runner import Platform

    settings = stack["settings"]
    repo = MetadataRepository.from_url(settings.metadata.url)
    platform = Platform(settings, repo, trigger="test")
    platform.init()
    store = get_store(settings.storage.landing_uri, settings.storage)
    manifest = generate(store, settings.storage.landing_uri, GeneratorConfig(scale="tiny"), drop=1)
    result = platform.run_all()
    try:
        yield {"settings": settings, "repo": repo, "platform": platform, "manifest": manifest, "result": result,
               "store": store}  # fmt: skip
    finally:
        platform.stop()


def test_metadata_lives_in_postgres(pipeline: dict[str, Any]) -> None:
    repo: MetadataRepository = pipeline["repo"]
    assert repo.engine.dialect.name == "postgresql"
    runs, total = repo.list_runs(limit=50)
    assert total >= 6 and {r["pipeline"] for r in runs} >= {
        "ingest",
        "bronze",
        "silver",
        "gold",
        "quality",
        "reconcile",
    }
    assert all(r["status"] == "SUCCEEDED" for r in runs)


def test_lake_on_minio_matches_oracle(pipeline: dict[str, Any]) -> None:
    from pyspark.sql import functions as F

    spark, settings = pipeline["platform"].spark, pipeline["settings"]
    expected = pipeline["manifest"]["expected"]

    def table(layer: str, name: str) -> Any:
        return spark.read.format("delta").load(settings.table_uri(layer, name).replace("s3://", "s3a://"))

    for entity, count in expected["silver_counts"].items():
        assert table("silver", entity).count() == count, entity
    fact = table("gold", "fact_sales_item")
    assert str(fact.agg(F.sum("net_amount")).first()[0]) == expected["gold"]["net_revenue"]
    assert pipeline["result"]["reconcile"]["results"]["not_passed"] == []
    # The Delta log physically lives in MinIO (listed with the raw S3 API: the landing-oriented
    # ObjectStore.list_files deliberately hides "_" paths such as _delta_log).
    bucket, prefix = S3Store.split(join_uri(settings.storage.lake_uri, "bronze", "sales", "_delta_log/"))
    keys = pipeline["store"]._client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    assert any(k["Key"].endswith("00000000000000000001.json") for k in keys)


def test_duplicate_detection_on_s3(pipeline: dict[str, Any]) -> None:
    settings, store, platform = pipeline["settings"], pipeline["store"], pipeline["platform"]
    original = next(f for f in pipeline["manifest"]["files"] if f["entity"] == "stores")
    copy_uri = join_uri(settings.storage.landing_uri, "stores", "stores_resent.csv")
    store.put_bytes(copy_uri, store.read_bytes(original["uri"]))
    result = platform.ingest(["stores"])["entities"][0]
    assert result["duplicates"] == [copy_uri] and result["batch_id"] is None


def test_api_reads_published_gold_from_minio(pipeline: dict[str, Any]) -> None:
    from fastapi.testclient import TestClient

    from lakehouse.api.app import create_app

    client = TestClient(create_app(pipeline["settings"], pipeline["repo"]))
    assert client.get("/health").json()["status"] == "ok"
    total = client.get("/analytics/sales-summary", params={"group_by": "total"}).json()
    assert total["items"][0]["net_revenue"] == pipeline["manifest"]["expected"]["gold"]["net_revenue"]
    assert total["items"][0]["orders"] == pipeline["manifest"]["expected"]["gold"]["orders"]
    runs = client.get("/runs", params={"pipeline": "gold"}).json()
    assert runs["total"] >= 1
    assert (
        client.get("/quality/results", params={"severity": "BLOCKING", "status": "FAILED"}).json()["total"]
        == 0
    )
