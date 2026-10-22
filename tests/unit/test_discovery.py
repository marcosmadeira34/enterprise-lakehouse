from pathlib import Path

from lakehouse.config import Settings
from lakehouse.ingestion.discovery import ingest
from lakehouse.metadata.repository import MetadataRepository


def _write(settings: Settings, entity: str, name: str, content: str) -> Path:
    path = Path(settings.storage.landing_uri) / entity / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _run(settings: Settings, repo: MetadataRepository):  # type: ignore[no-untyped-def]
    run_id = repo.start_run("ingest")
    return {r.entity: r for r in ingest(settings, repo, ["stores", "sales"], run_id)}


def test_incremental_arrivals_duplicates_and_changed_content(
    settings: Settings, repo: MetadataRepository
) -> None:
    _write(
        settings, "sales", "a.csv", "sale_id\n1\n1\n"
    )  # repeated records inside a file are not our concern
    _write(settings, "sales", "b.csv", "sale_id\n2\n")
    _write(settings, "sales", "notes.txt", "not data")
    _write(settings, "sales", ".tmp_upload.csv", "partial")
    first = _run(settings, repo)["sales"]
    assert first.new_files == 2 and first.batch_id
    assert first.ignored and first.ignored[0].endswith("notes.txt")

    again = _run(settings, repo)["sales"]
    assert again.new_files == 0 and again.batch_id is None and again.already_known == 2

    _write(settings, "sales", "a_resent.csv", "sale_id\n1\n1\n")
    _write(settings, "sales", "c.csv", "sale_id\n3\n")
    third = _run(settings, repo)["sales"]
    assert third.new_files == 1 and len(third.duplicates) == 1
    # A duplicate is recorded once, not on every run.
    fourth = _run(settings, repo)["sales"]
    assert fourth.duplicates == [] and fourth.new_files == 0

    _write(settings, "sales", "b.csv", "sale_id\n2\n22\n")  # same name, new content -> new file
    fifth = _run(settings, repo)["sales"]
    assert fifth.new_files == 1

    statuses = sorted(f["status"] for f in repo.list_files("sales"))
    assert statuses == ["DUPLICATE", "PENDING", "PENDING", "PENDING", "PENDING"]
    assert len(repo.pending_batches("bronze", "sales")) == 3


def test_entity_without_files_creates_no_batch(settings: Settings, repo: MetadataRepository) -> None:
    result = _run(settings, repo)["stores"]
    assert result.discovered == 0 and result.batch_id is None
