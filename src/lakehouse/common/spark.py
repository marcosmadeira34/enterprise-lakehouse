"""Centralized SparkSession construction and Delta table helpers."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Sequence
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from lakehouse.common.storage import is_s3
from lakehouse.config import Settings

# Must match the Hadoop version bundled with PySpark 3.5 (3.3.4); mixing versions breaks S3A.
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def build_spark(settings: Settings, app_suffix: str = "") -> SparkSession:
    from delta import configure_spark_with_delta_pip

    cfg = settings.spark
    builder = (
        SparkSession.builder.master(cfg.master)
        .appName(f"{cfg.app_name}{('-' + app_suffix) if app_suffix else ''}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        # Invalid dates become NULL (and are then rejected explicitly) instead of raising.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.ansi.enabled", "false")
        # Schema evolution is a reviewed decision, never a side effect of a write.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if cfg.ivy_dir:
        builder = builder.config("spark.jars.ivy", cfg.ivy_dir)
    packages: list[str] = []
    if settings.storage.uses_s3:
        packages.append(HADOOP_AWS_PACKAGE)
        endpoint = settings.storage.s3_endpoint_url or ""
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.storage.s3_region)
            .config("spark.hadoop.fs.s3a.path.style.access", str(settings.storage.s3_path_style).lower())
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.access.key", settings.storage.s3_access_key or "")
            .config("spark.hadoop.fs.s3a.secret.key", settings.storage.s3_secret_key or "")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
        )
    for key, value in cfg.extra_conf.items():
        builder = builder.config(key, value)
    # Python workers must use the same interpreter (and virtualenv) as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # The JVM inherits file descriptor 1 and spark-submit/Ivy print to it. Point it at stderr while the
    # gateway starts so stdout stays reserved for the CLI's JSON result.
    sys.stdout.flush()
    saved_stdout = os.dup(1)
    os.dup2(2, 1)
    try:
        spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    finally:
        os.dup2(saved_stdout, 1)
        os.close(saved_stdout)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def table_exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable

    return bool(DeltaTable.isDeltaTable(spark, path))


def ensure_table(
    spark: SparkSession,
    path: str,
    schema: StructType,
    partition_by: Sequence[str] = (),
    properties: dict[str, str] | None = None,
    comment: str | None = None,
) -> None:
    """Create the Delta table with an explicit schema if it does not exist yet.

    Tables are always created up front, so no write can silently infer or widen a schema.
    """
    from delta.tables import DeltaTable

    builder = DeltaTable.createIfNotExists(spark).location(path).addColumns(schema)
    if partition_by:
        builder = builder.partitionedBy(*partition_by)
    for key, value in (properties or {}).items():
        builder = builder.property(key, value)
    if comment:
        builder = builder.comment(comment)
    builder.execute()


def latest_version(spark: SparkSession, path: str) -> int | None:
    from delta.tables import DeltaTable

    if not table_exists(spark, path):
        return None
    row = DeltaTable.forPath(spark, path).history(1).select("version").first()
    return int(row["version"]) if row else None


def read_delta(spark: SparkSession, path: str, version: int | None = None) -> DataFrame:
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def find_commit(spark: SparkSession, path: str, key: str, value: str) -> int | None:
    """Version of the commit whose ``userMetadata`` JSON has ``key == value`` (None if absent)."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if not table_exists(spark, path):
        return None
    history = DeltaTable.forPath(spark, path).history().select("version", "userMetadata")
    candidates = history.where(F.col("userMetadata").contains(value)).collect()
    for row in candidates:
        try:
            meta: dict[str, Any] = json.loads(row["userMetadata"])
        except (TypeError, json.JSONDecodeError):
            continue
        if meta.get(key) == value:
            return int(row["version"])
    return None


def commit_metadata(**fields: Any) -> str:
    return json.dumps(fields, sort_keys=True, default=str)


def spark_path_for(uri: str) -> str:
    from lakehouse.common.storage import to_spark_path

    return to_spark_path(uri) if is_s3(uri) else uri
