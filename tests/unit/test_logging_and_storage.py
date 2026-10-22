import json
import logging
from pathlib import Path

import pytest

from lakehouse.common.faults import InjectedFailure, checkpoint
from lakehouse.common.storage import LocalStore, from_spark_path, sha256_of, to_spark_path
from lakehouse.observability.logging import JsonFormatter, bind, log_event, redact


def test_json_log_carries_context_and_redacts_secrets() -> None:
    logger = logging.getLogger("test.json")
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    with bind(run_id="r1", correlation_id="c1"), bind(entity="sales"):
        log_event(logger, "connected", url="postgresql://lake:s3cr3t@db:5432/x", password="p", rows=3)
    payload = json.loads(JsonFormatter().format(records[0]))
    assert payload["run_id"] == "r1" and payload["entity"] == "sales" and payload["event"] == "connected"
    assert payload["password"] == "***"
    assert "s3cr3t" not in json.dumps(payload) and payload["url"] == "postgresql://lake:***@db:5432/x"
    assert payload["rows"] == 3


def test_redact_nested_structures() -> None:
    assert redact({"aws_secret_access_key": "x", "nested": [{"token": "y", "ok": 1}]}) == {
        "aws_secret_access_key": "***",
        "nested": [{"token": "***", "ok": 1}],
    }


def test_local_store_lists_data_only_and_hashes(tmp_path: Path) -> None:
    store = LocalStore()
    store.put_bytes(str(tmp_path / "e" / "a.csv"), b"abc")
    (tmp_path / "e" / ".hidden").write_text("x")
    (tmp_path / "_manifests").mkdir()
    (tmp_path / "_manifests" / "m.json").write_text("{}")
    files = store.list_files(str(tmp_path))
    assert [f.name for f in files] == ["a.csv"]
    assert (
        sha256_of(store, files[0].uri) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )
    assert not list((tmp_path / "e").glob(".a.csv.tmp"))  # atomic write leaves no temp file


def test_spark_path_conversion() -> None:
    assert to_spark_path("s3://lake/silver/sales") == "s3a://lake/silver/sales"
    assert from_spark_path("s3a://lake/x") == "s3://lake/x"
    assert to_spark_path("/tmp/x") == "/tmp/x"


def test_fault_checkpoints_only_fire_when_armed(monkeypatch: pytest.MonkeyPatch) -> None:
    checkpoint("silver.after_merge")
    monkeypatch.setenv("LAKEHOUSE_FAIL_AT", "silver.after_merge")
    with pytest.raises(InjectedFailure):
        checkpoint("silver.after_merge")
    with pytest.raises(ValueError):
        checkpoint("not.a.checkpoint")
