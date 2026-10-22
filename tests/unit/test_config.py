from pathlib import Path

import pytest
from pydantic import ValidationError

from lakehouse.config import Settings


def test_env_overrides_nested_keys_and_parses_json_literals(tmp_path: Path) -> None:
    config = tmp_path / "c.yaml"
    config.write_text("spark:\n  shuffle_partitions: 8\nquality:\n  max_reject_ratio: 0.25\n")
    settings = Settings.load(
        config,
        env={
            "LAKEHOUSE__SPARK__SHUFFLE_PARTITIONS": "3",
            "LAKEHOUSE__QUALITY__VOLUME_ANOMALY__MAX_RATIO": "4.5",
            "LAKEHOUSE__STORAGE__LAKE_URI": "s3a://lake/root/",
            "LAKEHOUSE__BUSINESS_RULES__ALLOWED_CURRENCIES": '["BRL", "USD"]',
            "UNRELATED": "x",
        },
    )
    assert settings.spark.shuffle_partitions == 3
    assert settings.quality.max_reject_ratio == 0.25
    assert settings.quality.volume_anomaly.max_ratio == 4.5
    # s3a:// is a Spark detail: configuration is normalized to s3:// without trailing slash.
    assert settings.storage.lake_uri == "s3://lake/root"
    assert settings.table_uri("silver", "sales") == "s3://lake/root/silver/sales"
    assert settings.business_rules.allowed_currencies == ["BRL", "USD"]


def test_local_paths_are_absolute(tmp_path: Path) -> None:
    settings = Settings.load(
        tmp_path / "missing.yaml", env={"LAKEHOUSE__STORAGE__LANDING_URI": "./rel/landing"}
    )
    assert Path(settings.storage.landing_uri).is_absolute()


def test_invalid_values_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        Settings.load(tmp_path / "missing.yaml", env={"LAKEHOUSE__QUALITY__MAX_REJECT_RATIO": "1.5"})


def test_credentials_only_come_from_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key-from-env")
    settings = Settings.load(tmp_path / "missing.yaml", env={})
    assert settings.storage.s3_access_key == "key-from-env"
    assert "key-from-env" not in settings.model_dump_json()


def test_repository_default_config_loads() -> None:
    settings = Settings.load(env={})
    assert settings.metadata.url.startswith("sqlite")
    assert settings.business_rules.allow_negative_stock is True
