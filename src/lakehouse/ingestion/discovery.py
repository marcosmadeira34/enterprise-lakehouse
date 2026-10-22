"""Landing-zone discovery and batch planning. Pure Python: no Spark is needed to decide *what* to load.

A file's identity is ``(entity, SHA-256 of its content)``:

* the same bytes re-sent under another name are a **duplicate** and are not loaded again;
* a file re-uploaded under the same name with different bytes is a **new** file;
* repeated records inside one file are not this layer's concern: Bronze keeps them all.

``(uri, size, modified time)`` is only used as a cache to avoid re-hashing files already registered.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sqlalchemy import select

from lakehouse.common.storage import ObjectStore, StoredFile, get_store, join_uri, sha256_of
from lakehouse.config import Settings
from lakehouse.contracts import get_contract
from lakehouse.metadata import models as m
from lakehouse.metadata.repository import MetadataRepository, aware
from lakehouse.observability.logging import bind, log_event

log = logging.getLogger(__name__)


@dataclass
class EntityIngestion:
    entity: str
    discovered: int = 0
    new_files: int = 0
    already_known: int = 0
    duplicates: list[str] = field(default_factory=list)
    ignored: list[str] = field(default_factory=list)
    batch_id: str | None = None
    bytes_planned: int = 0


def _cached_known(repo: MetadataRepository, entity: str, files: list[StoredFile]) -> set[str]:
    """URIs whose (size, modified time) match a registration: no need to hash them again."""
    if not files:
        return set()
    f = m.ingested_files
    with repo.engine.connect() as conn:
        rows = conn.execute(
            select(f.c.uri, f.c.size_bytes, f.c.source_modified_at).where(
                f.c.entity == entity, f.c.uri.in_([x.uri for x in files])
            )
        ).all()
    registered = {(r.uri, int(r.size_bytes), aware(r.source_modified_at)) for r in rows}
    return {x.uri for x in files if (x.uri, x.size_bytes, x.modified_at) in registered}


def plan_entity(
    settings: Settings, repo: MetadataRepository, store: ObjectStore, entity: str, run_id: str
) -> EntityIngestion:
    contract = get_contract(entity)
    summary = EntityIngestion(entity)
    extension = f".{contract.source_format.value}"
    listed = store.list_files(join_uri(settings.storage.landing_uri, entity))
    summary.discovered = len(listed)
    candidates = []
    for item in listed:
        if item.name.lower().endswith(extension):
            candidates.append(item)
        else:
            summary.ignored.append(item.uri)
    cached = _cached_known(repo, entity, candidates)
    # Hash outside the database transaction: reading large objects must not hold it open.
    hashed = [(item, sha256_of(store, item.uri)) for item in candidates if item.uri not in cached]
    summary.already_known += len(cached)

    with repo.transaction() as conn:
        pending: list[int] = []
        planned_bytes = 0
        for item, sha in hashed:
            registration = repo.register_file(
                conn, entity, item.uri, sha, item.size_bytes, item.modified_at, run_id
            )
            if registration.status == "PENDING":
                pending.append(registration.file_id)
                planned_bytes += item.size_bytes
            elif registration.status == "DUPLICATE":
                summary.duplicates.append(item.uri)
                log_event(
                    log,
                    "duplicate_file",
                    logging.WARNING,
                    uri=item.uri,
                    duplicate_of=registration.duplicate_of,
                )
            else:
                summary.already_known += 1
        if pending:
            batch = repo.create_batch(
                conn, entity, settings.pipeline.source_system, run_id, pending, planned_bytes
            )
            summary.batch_id = batch["batch_id"]
            summary.new_files = len(pending)
            summary.bytes_planned = planned_bytes
    log_event(
        log,
        "entity_ingestion_planned",
        entity=entity,
        discovered=summary.discovered,
        new_files=summary.new_files,
        already_known=summary.already_known,
        duplicates=len(summary.duplicates),
        ignored=len(summary.ignored),
        batch_id=summary.batch_id,
    )
    return summary


def ingest(
    settings: Settings, repo: MetadataRepository, entities: list[str], run_id: str
) -> list[EntityIngestion]:
    store = get_store(settings.storage.landing_uri, settings.storage)
    results = []
    for entity in entities:
        with bind(entity=entity), repo.lock(f"ingest:{entity}", settings.pipeline.lock_ttl_seconds):
            results.append(plan_entity(settings, repo, store, entity, run_id))
    return results
