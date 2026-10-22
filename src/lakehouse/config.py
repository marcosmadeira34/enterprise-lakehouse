"""Centralized configuration: YAML defaults overlaid by ``LAKEHOUSE__SECTION__KEY`` environment variables."""

from __future__ import annotations

import json
import os
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

ENV_PREFIX = "LAKEHOUSE__"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "lakehouse.yaml"


class StorageSettings(BaseModel):
    landing_uri: str = "./data/landing"
    lake_uri: str = "./data/lake"
    s3_endpoint_url: str | None = None
    s3_region: str = "us-east-1"
    s3_path_style: bool = True

    @field_validator("landing_uri", "lake_uri")
    @classmethod
    def _normalize_uri(cls, value: str) -> str:
        if value.startswith("s3a://"):
            value = "s3://" + value[len("s3a://") :]
        if value.startswith("s3://"):
            return value.rstrip("/")
        return str(Path(value).expanduser().resolve())

    @property
    def uses_s3(self) -> bool:
        return self.landing_uri.startswith("s3://") or self.lake_uri.startswith("s3://")

    # Credentials are read from the environment only, so they never end up in config files or logs.
    @property
    def s3_access_key(self) -> str | None:
        return os.getenv("AWS_ACCESS_KEY_ID")

    @property
    def s3_secret_key(self) -> str | None:
        return os.getenv("AWS_SECRET_ACCESS_KEY")


class MetadataSettings(BaseModel):
    url: str = "sqlite:///./data/metadata.db"


class SparkSettings(BaseModel):
    master: str = "local[2]"
    app_name: str = "enterprise-lakehouse"
    driver_memory: str = "2g"
    shuffle_partitions: int = Field(default=8, ge=1)
    ivy_dir: str | None = None
    extra_conf: dict[str, str] = Field(default_factory=dict)


class PipelineSettings(BaseModel):
    source_system: str = "retail-pos"
    lock_ttl_seconds: int = Field(default=7200, gt=0)
    stale_run_seconds: int = Field(default=10800, gt=0)


class BusinessRules(BaseModel):
    max_future_days: int = Field(default=1, ge=0)
    min_valid_date: date = date(2015, 1, 1)
    money_tolerance: Decimal = Decimal("0.01")
    allow_negative_stock: bool = True
    allowed_currencies: list[str] = Field(default_factory=lambda: ["BRL"])


class VolumeAnomalySettings(BaseModel):
    lookback_batches: int = Field(default=5, ge=1)
    min_history: int = Field(default=3, ge=1)
    min_ratio: float = Field(default=0.5, gt=0)
    max_ratio: float = Field(default=2.0, gt=0)


class QualitySettings(BaseModel):
    max_reject_ratio: float = Field(default=0.25, ge=0, le=1)
    volume_anomaly: VolumeAnomalySettings = Field(default_factory=VolumeAnomalySettings)


class Settings(BaseModel):
    environment: str = "local"
    storage: StorageSettings = Field(default_factory=StorageSettings)
    metadata: MetadataSettings = Field(default_factory=MetadataSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    business_rules: BusinessRules = Field(default_factory=BusinessRules)
    quality: QualitySettings = Field(default_factory=QualitySettings)

    def table_uri(self, layer: str, table: str) -> str:
        return f"{self.storage.lake_uri}/{layer}/{table}"

    @classmethod
    def load(cls, path: str | Path | None = None, env: dict[str, str] | None = None) -> Settings:
        config_path = Path(path or os.getenv("LAKEHOUSE_CONFIG") or DEFAULT_CONFIG_PATH)
        raw: dict[str, Any] = {}
        if config_path.exists():
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        _apply_env_overrides(raw, os.environ if env is None else env)
        return cls.model_validate(raw)


def _apply_env_overrides(raw: dict[str, Any], env: dict[str, str] | os._Environ[str]) -> None:
    for name, value in env.items():
        if not name.startswith(ENV_PREFIX):
            continue
        keys = [part.lower() for part in name[len(ENV_PREFIX) :].split("__") if part]
        if not keys:
            continue
        node = raw
        for key in keys[:-1]:
            child = node.get(key)
            if not isinstance(child, dict):
                child = {}
                node[key] = child
            node = child
        node[keys[-1]] = _parse_env_value(value)


def _parse_env_value(value: str) -> Any:
    """Accept JSON literals (numbers, booleans, lists, objects); anything else stays a string."""
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value
