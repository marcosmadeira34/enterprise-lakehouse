from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import insert, update
from sqlalchemy.exc import IntegrityError

from lakehouse.metadata import models as m
from lakehouse.metadata.repository import LockNotAcquiredError, MetadataRepository, utcnow


def _register(repo: MetadataRepository, uri: str, sha: str, run_id: str = "r1") -> object:
    with repo.transaction() as conn:
        return repo.register_file(conn, "sales", uri, sha, 10, None, run_id)


def test_file_identity_is_content_not_name(repo: MetadataRepository) -> None:
    run = repo.start_run("ingest")
    first = _register(repo, "/l/sales/a.csv", "h1", run)
    again = _register(repo, "/l/sales/a.csv", "h1", run)
    renamed = _register(repo, "/l/sales/copy_of_a.csv", "h1", run)
    changed = _register(repo, "/l/sales/a.csv", "h2", run)
    assert first.status == "PENDING"  # type: ignore[attr-defined]
    assert again.status == "KNOWN"  # type: ignore[attr-defined]
    assert renamed.status == "DUPLICATE"  # type: ignore[attr-defined]
    assert renamed.duplicate_of == first.file_id  # type: ignore[attr-defined]
    assert changed.status == "PENDING"  # type: ignore[attr-defined]  # same name, new content


def test_database_enforces_single_live_registration(repo: MetadataRepository) -> None:
    values = {"entity": "sales", "uri": "/x", "file_name": "x", "content_sha256": "h", "size_bytes": 1,
              "status": "PENDING", "run_id": "r", "first_seen_at": utcnow()}  # fmt: skip
    with repo.transaction() as conn:
        conn.execute(insert(m.ingested_files).values(**values))
    with pytest.raises(IntegrityError), repo.transaction() as conn:
        conn.execute(insert(m.ingested_files).values(**{**values, "uri": "/y", "status": "COMMITTED"}))
    # Non-live states (duplicate, quarantined) may repeat the content.
    with repo.transaction() as conn:
        conn.execute(insert(m.ingested_files).values(**{**values, "uri": "/z", "status": "QUARANTINED"}))


def test_run_lifecycle_records_duration_and_errors(repo: MetadataRepository) -> None:
    run_id = repo.start_run("silver", correlation_id="corr-1", trigger="test", params={"entities": ["sales"]})
    error = RuntimeError("boom")
    repo.record_error(run_id, "silver:sales", error, "trace")
    repo.finish_run(run_id, "FAILED", records_read=10, error=error)
    run = repo.get_run(run_id)
    assert run is not None
    assert run["status"] == "FAILED" and run["correlation_id"] == "corr-1"
    assert run["duration_seconds"] >= 0 and run["error_type"] == "RuntimeError"
    assert run["errors"][0]["step"] == "silver:sales"
    rows, total = repo.list_runs(limit=10, status="FAILED")
    assert total == 1 and rows[0]["run_id"] == run_id


def test_lock_is_exclusive_and_expired_leases_are_taken_over(repo: MetadataRepository) -> None:
    assert repo.acquire_lock("silver:sales", "a", 60)
    assert not repo.acquire_lock("silver:sales", "b", 60)
    with pytest.raises(LockNotAcquiredError), repo.lock("silver:sales", 60, owner="c"):
        pass
    with repo.transaction() as conn:  # simulate a crashed holder whose lease ran out
        conn.execute(update(m.locks).values(expires_at=datetime.now(UTC) - timedelta(seconds=1)))
    assert repo.acquire_lock("silver:sales", "b", 60)
    repo.release_lock("silver:sales", "a")  # not the owner: no effect
    assert not repo.acquire_lock("silver:sales", "d", 60)
    repo.release_lock("silver:sales", "b")
    assert repo.acquire_lock("silver:sales", "d", 60)


def test_pending_batches_follow_layer_state(repo: MetadataRepository) -> None:
    run = repo.start_run("ingest")
    with repo.transaction() as conn:
        f1 = repo.register_file(conn, "sales", "/a", "h1", 1, None, run)
        b1 = repo.create_batch(conn, "sales", "pos", run, [f1.file_id], 1)
        f2 = repo.register_file(conn, "sales", "/b", "h2", 1, None, run)
        b2 = repo.create_batch(conn, "sales", "pos", run, [f2.file_id], 1)
    assert [b["batch_id"] for b in repo.pending_batches("bronze", "sales")] == [
        b1["batch_id"],
        b2["batch_id"],
    ]
    assert repo.pending_batches("silver", "sales") == []
    repo.set_layer_status(b1["batch_id"], "bronze", "RUNNING", run)
    repo.set_layer_status(b1["batch_id"], "bronze", "SUCCEEDED", run, records_read=5)
    repo.set_layer_status(b1["batch_id"], "silver", "PENDING")
    assert [b["batch_id"] for b in repo.pending_batches("bronze", "sales")] == [b2["batch_id"]]
    assert [b["batch_id"] for b in repo.pending_batches("silver", "sales")] == [b1["batch_id"]]
    assert repo.get_layer(b1["batch_id"], "bronze")["attempts"] == 1  # type: ignore[index]
    assert repo.recent_batch_volumes("sales", b2["batch_seq"], 5) == [5]


def test_schema_version_guard(repo: MetadataRepository) -> None:
    repo.init_schema()  # idempotent
    with repo.transaction() as conn:
        conn.execute(insert(m.schema_version).values(version=m.SCHEMA_VERSION + 1, applied_at=utcnow()))
    with pytest.raises(RuntimeError):
        repo.init_schema()
