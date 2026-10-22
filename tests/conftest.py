from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from lakehouse.config import Settings
from lakehouse.metadata.repository import MetadataRepository

# PySpark converts collected timestamps to the driver's local zone; the platform runs in UTC.
os.environ["TZ"] = "UTC"
time.tzset()


def make_settings(root: Path, **overrides: Any) -> Settings:
    data: dict[str, Any] = {
        "storage": {"landing_uri": str(root / "landing"), "lake_uri": str(root / "lake")},
        "metadata": {"url": f"sqlite:///{root / 'metadata.db'}"},
        "spark": {"master": "local[2]", "shuffle_partitions": 2, "driver_memory": "1g"},
    }
    for section, values in overrides.items():
        data.setdefault(section, {}).update(values)
    return Settings.model_validate(data)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
def repo(settings: Settings) -> MetadataRepository:
    repository = MetadataRepository.from_url(settings.metadata.url)
    repository.init_schema()
    return repository


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    pytest.importorskip("pyspark")
    from lakehouse.common.spark import build_spark

    base = make_settings(tmp_path_factory.mktemp("spark"))
    base.spark.extra_conf.update(
        {
            "spark.databricks.delta.snapshotPartitions": "1",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.default.parallelism": "2",
        }
    )
    session = build_spark(base, "tests")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _no_armed_faults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKEHOUSE_FAIL_AT", raising=False)


def load_manifest(settings: Settings, drop: int) -> dict[str, Any]:
    path = Path(settings.storage.landing_uri) / "_manifests" / f"drop_{drop:03d}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def platform_for(settings: Settings, repo: MetadataRepository, spark: Any) -> Any:
    from lakehouse.pipelines.runner import Platform

    platform = Platform(settings, repo, trigger="test")
    platform._spark = spark  # shared session; Platform.stop() must not stop it
    platform.stop = lambda: None  # type: ignore[method-assign]
    platform.init()
    return platform
