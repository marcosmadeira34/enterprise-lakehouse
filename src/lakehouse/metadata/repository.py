"""Operational metadata repository: runs, batches, files, locks, quality results and publications.

Every method opens its own short transaction. Callers never hold a database transaction open while
Spark is working, so a long job cannot keep row locks or a connection hostage.
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, and_, create_engine, delete, event, func, insert, or_, select, update
from sqlalchemy.engine import Connection, CursorResult, RowMapping
from sqlalchemy.exc import IntegrityError

from lakehouse.metadata import models as m


def utcnow() -> datetime:
    return datetime.now(UTC)


def aware(value: datetime | None) -> datetime | None:
    """SQLite returns naive datetimes; everything is stored in UTC, so re-attach the zone."""
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def new_id(prefix: str) -> str:
    return f"{prefix}-{utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


class LockNotAcquiredError(RuntimeError):
    pass


@dataclass(frozen=True)
class FileRegistration:
    file_id: int
    status: str  # PENDING | DUPLICATE | KNOWN
    duplicate_of: int | None = None


def build_engine(url: str) -> Engine:
    if url.startswith("sqlite:///"):
        db_path = url[len("sqlite:///") :]
        if db_path and db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(url, connect_args={"check_same_thread": False, "timeout": 30})

        # pysqlite's implicit transactions break SAVEPOINT; let SQLAlchemy issue BEGIN itself
        # (recipe from the SQLAlchemy SQLite dialect documentation).
        @event.listens_for(engine, "connect")
        def _on_connect(dbapi_connection: Any, _record: Any) -> None:
            dbapi_connection.isolation_level = None

        @event.listens_for(engine, "begin")
        def _on_begin(conn: Connection) -> None:
            conn.exec_driver_sql("BEGIN")

        return engine
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


class MetadataRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @classmethod
    def from_url(cls, url: str) -> MetadataRepository:
        return cls(build_engine(url))

    # ------------------------------------------------------------------ schema
    def init_schema(self) -> None:
        m.metadata.create_all(self.engine)
        with self.engine.begin() as conn:
            current = conn.execute(select(func.max(m.schema_version.c.version))).scalar()
            if current is None:
                conn.execute(insert(m.schema_version).values(version=m.SCHEMA_VERSION, applied_at=utcnow()))
            elif current > m.SCHEMA_VERSION:
                raise RuntimeError(
                    f"database schema v{current} is newer than this code (v{m.SCHEMA_VERSION})"
                )

    def ping(self) -> bool:
        with self.engine.connect() as conn:
            conn.execute(select(1))
        return True

    # ------------------------------------------------------------------ runs
    def start_run(
        self,
        pipeline: str,
        correlation_id: str | None = None,
        trigger: str = "cli",
        params: dict[str, Any] | None = None,
        attempt: int = 1,
    ) -> str:
        run_id = new_id(pipeline)
        with self.engine.begin() as conn:
            conn.execute(
                insert(m.runs).values(
                    run_id=run_id,
                    pipeline=pipeline,
                    status="RUNNING",
                    correlation_id=correlation_id or run_id,
                    attempt=attempt,
                    trigger=trigger,
                    params=params or {},
                    started_at=utcnow(),
                )
            )
        return run_id

    def finish_run(
        self,
        run_id: str,
        status: str,
        records_read: int | None = None,
        records_written: int | None = None,
        records_rejected: int | None = None,
        error: BaseException | None = None,
    ) -> None:
        finished = utcnow()
        with self.engine.begin() as conn:
            started = aware(
                conn.execute(select(m.runs.c.started_at).where(m.runs.c.run_id == run_id)).scalar_one()
            )
            assert started is not None
            conn.execute(
                update(m.runs)
                .where(m.runs.c.run_id == run_id)
                .values(
                    status=status,
                    finished_at=finished,
                    duration_seconds=(finished - started).total_seconds(),
                    records_read=records_read,
                    records_written=records_written,
                    records_rejected=records_rejected,
                    error_type=type(error).__name__ if error else None,
                    error_message=str(error)[:4000] if error else None,
                )
            )

    def record_error(
        self,
        run_id: str,
        step: str,
        error: BaseException,
        traceback_text: str | None = None,
        entity: str | None = None,
        batch_id: str | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(m.run_errors).values(
                    run_id=run_id,
                    step=step,
                    entity=entity,
                    batch_id=batch_id,
                    error_type=type(error).__name__,
                    message=str(error)[:4000],
                    traceback=(traceback_text or "")[-8000:] or None,
                    occurred_at=utcnow(),
                )
            )

    def list_runs(
        self, limit: int = 50, offset: int = 0, pipeline: str | None = None, status: str | None = None
    ) -> tuple[list[dict[str, Any]], int]:
        conditions = []
        if pipeline:
            conditions.append(m.runs.c.pipeline == pipeline)
        if status:
            conditions.append(m.runs.c.status == status)
        where = and_(*conditions) if conditions else None
        query = select(m.runs).order_by(m.runs.c.started_at.desc(), m.runs.c.run_id.desc())
        count_query = select(func.count()).select_from(m.runs)
        if where is not None:
            query, count_query = query.where(where), count_query.where(where)
        with self.engine.connect() as conn:
            rows = conn.execute(query.limit(limit).offset(offset)).mappings().all()
            total = conn.execute(count_query).scalar_one()
        return [_row(r) for r in rows], total

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(select(m.runs).where(m.runs.c.run_id == run_id)).mappings().first()
            if row is None:
                return None
            run = _row(row)
            run["errors"] = [
                _row(r)
                for r in conn.execute(
                    select(m.run_errors).where(m.run_errors.c.run_id == run_id).order_by(m.run_errors.c.id)
                ).mappings()
            ]
            run["batch_layers"] = [
                _row(r)
                for r in conn.execute(
                    select(m.batch_layers, m.batches.c.entity)
                    .join(m.batches, m.batches.c.batch_id == m.batch_layers.c.batch_id)
                    .where(m.batch_layers.c.run_id == run_id)
                    .order_by(m.batches.c.batch_seq, m.batch_layers.c.layer)
                ).mappings()
            ]
            run["batches_created"] = [
                _row(r)
                for r in conn.execute(
                    select(m.batches).where(m.batches.c.run_id == run_id).order_by(m.batches.c.batch_seq)
                ).mappings()
            ]
        return run

    def stale_runs(self, older_than: timedelta) -> list[dict[str, Any]]:
        cutoff = utcnow() - older_than
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(m.runs).where(m.runs.c.status == "RUNNING", m.runs.c.started_at < cutoff)
            ).mappings()
            return [_row(r) for r in rows]

    def mark_run_abandoned(self, run_id: str, reason: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(m.runs)
                .where(m.runs.c.run_id == run_id, m.runs.c.status == "RUNNING")
                .values(status="ABANDONED", finished_at=utcnow(), error_message=reason)
            )

    # ------------------------------------------------------------------ files and batches
    def register_file(
        self,
        conn: Connection,
        entity: str,
        uri: str,
        sha256: str,
        size_bytes: int,
        modified_at: datetime | None,
        run_id: str,
    ) -> FileRegistration:
        """Register a landing file inside the caller's transaction.

        * Same content already registered under the same URI -> KNOWN (nothing recorded again).
        * Same content live under another URI -> DUPLICATE row pointing at the original.
        * Otherwise -> PENDING, to be attached to a batch.
        """
        known = conn.execute(
            select(m.ingested_files.c.file_id).where(
                m.ingested_files.c.entity == entity,
                m.ingested_files.c.uri == uri,
                m.ingested_files.c.content_sha256 == sha256,
            )
        ).first()
        if known is not None:
            return FileRegistration(int(known.file_id), "KNOWN")
        live = self._live_file(conn, entity, sha256)
        if live is None:
            values = self._file_values(entity, uri, sha256, size_bytes, modified_at, run_id, "PENDING")
            try:
                with conn.begin_nested():
                    file_id = _pk(conn.execute(insert(m.ingested_files).values(**values)))
                return FileRegistration(int(file_id), "PENDING")
            except IntegrityError:
                # Lost a race: another process registered the same content first.
                live = self._live_file(conn, entity, sha256)
                if live is None:
                    raise
        values = self._file_values(entity, uri, sha256, size_bytes, modified_at, run_id, "DUPLICATE")
        values["duplicate_of"] = live
        file_id = _pk(conn.execute(insert(m.ingested_files).values(**values)))
        return FileRegistration(int(file_id), "DUPLICATE", duplicate_of=live)

    @staticmethod
    def _live_file(conn: Connection, entity: str, sha256: str) -> int | None:
        row = conn.execute(
            select(m.ingested_files.c.file_id).where(
                m.ingested_files.c.entity == entity,
                m.ingested_files.c.content_sha256 == sha256,
                m.ingested_files.c.status.in_(("PENDING", "COMMITTED")),
            )
        ).first()
        return int(row.file_id) if row else None

    @staticmethod
    def _file_values(
        entity: str, uri: str, sha256: str, size: int, modified: datetime | None, run_id: str, status: str
    ) -> dict[str, Any]:
        return {
            "entity": entity,
            "uri": uri,
            "file_name": uri.rstrip("/").rsplit("/", 1)[-1],
            "content_sha256": sha256,
            "size_bytes": size,
            "source_modified_at": modified,
            "status": status,
            "run_id": run_id,
            "first_seen_at": utcnow(),
        }

    def create_batch(
        self,
        conn: Connection,
        entity: str,
        source_system: str,
        run_id: str,
        file_ids: Sequence[int],
        size: int,
    ) -> dict[str, Any]:
        batch_id = new_id(f"b-{entity}")
        now = utcnow()
        seq = _pk(
            conn.execute(
                insert(m.batches).values(
                    batch_id=batch_id,
                    entity=entity,
                    source_system=source_system,
                    run_id=run_id,
                    ingestion_date=now.date().isoformat(),
                    files_received=len(file_ids),
                    bytes_received=size,
                    created_at=now,
                )
            )
        )
        conn.execute(
            update(m.ingested_files)
            .where(m.ingested_files.c.file_id.in_(list(file_ids)))
            .values(batch_id=batch_id)
        )
        conn.execute(
            insert(m.batch_layers).values(batch_id=batch_id, layer="bronze", status="PENDING", attempts=0)
        )
        return {
            "batch_id": batch_id,
            "batch_seq": int(seq),
            "entity": entity,
            "ingestion_date": now.date().isoformat(),
        }

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        with self.engine.begin() as conn:
            yield conn

    def batch_files(self, batch_id: str) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(m.ingested_files)
                .where(m.ingested_files.c.batch_id == batch_id)
                .order_by(m.ingested_files.c.file_id)
            ).mappings()
            return [_row(r) for r in rows]

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(select(m.batches).where(m.batches.c.batch_id == batch_id)).mappings().first()
            return _row(row) if row else None

    def pending_batches(self, layer: str, entity: str) -> list[dict[str, Any]]:
        """Batches of ``entity`` that still need ``layer`` (never run, failed or interrupted), oldest first.

        Callers hold the entity lock, so a RUNNING status here can only be left over by a crash.
        """
        bl = m.batch_layers
        query = (
            select(m.batches)
            .join(bl, and_(bl.c.batch_id == m.batches.c.batch_id, bl.c.layer == layer))
            .where(m.batches.c.entity == entity, bl.c.status != "SUCCEEDED")
            .order_by(m.batches.c.batch_seq)
        )
        if layer != "bronze":
            bronze = bl.alias("bronze")
            query = query.join(
                bronze, and_(bronze.c.batch_id == m.batches.c.batch_id, bronze.c.layer == "bronze")
            ).where(bronze.c.status == "SUCCEEDED")
        with self.engine.connect() as conn:
            return [_row(r) for r in conn.execute(query).mappings()]

    def batches_for_reprocess(
        self,
        entity: str,
        batch_ids: Sequence[str] | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> list[dict[str, Any]]:
        """Batches eligible for historical reprocessing (Bronze already committed)."""
        bl = m.batch_layers
        query = (
            select(m.batches)
            .join(bl, and_(bl.c.batch_id == m.batches.c.batch_id, bl.c.layer == "bronze"))
            .where(m.batches.c.entity == entity, bl.c.status == "SUCCEEDED")
            .order_by(m.batches.c.batch_seq)
        )
        if batch_ids:
            query = query.where(m.batches.c.batch_id.in_(list(batch_ids)))
        if since:
            query = query.where(m.batches.c.ingestion_date >= since)
        if until:
            query = query.where(m.batches.c.ingestion_date <= until)
        with self.engine.connect() as conn:
            return [_row(r) for r in conn.execute(query).mappings()]

    def set_layer_status(
        self,
        batch_id: str,
        layer: str,
        status: str,
        run_id: str | None = None,
        records_read: int | None = None,
        records_accepted: int | None = None,
        records_rejected: int | None = None,
        delta_version: int | None = None,
        metrics: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> None:
        now = utcnow()
        values: dict[str, Any] = {"status": status, "run_id": run_id}
        if status == "RUNNING":
            values.update(started_at=now, finished_at=None, error_message=None)
        else:
            values.update(finished_at=now)
        for key, value in {
            "records_read": records_read,
            "records_accepted": records_accepted,
            "records_rejected": records_rejected,
            "delta_version": delta_version,
            "metrics": metrics,
            "error_message": error_message[:4000] if error_message else None,
        }.items():
            if value is not None:
                values[key] = value
        bl = m.batch_layers
        with self.engine.begin() as conn:
            result = conn.execute(
                update(bl)
                .where(bl.c.batch_id == batch_id, bl.c.layer == layer)
                .values(**values, attempts=bl.c.attempts + (1 if status == "RUNNING" else 0))
            )
            if result.rowcount == 0:
                conn.execute(
                    insert(bl).values(
                        batch_id=batch_id, layer=layer, attempts=1 if status == "RUNNING" else 0, **values
                    )
                )

    def get_layer(self, batch_id: str, layer: str) -> dict[str, Any] | None:
        bl = m.batch_layers
        with self.engine.connect() as conn:
            row = (
                conn.execute(select(bl).where(bl.c.batch_id == batch_id, bl.c.layer == layer))
                .mappings()
                .first()
            )
            return _row(row) if row else None

    def mark_files_committed(self, batch_id: str, counts_by_sha: dict[str, int]) -> None:
        f = m.ingested_files
        with self.engine.begin() as conn:
            for sha, count in counts_by_sha.items():
                conn.execute(
                    update(f)
                    .where(f.c.batch_id == batch_id, f.c.content_sha256 == sha, f.c.status == "PENDING")
                    .values(status="COMMITTED", records_read=count, committed_at=utcnow())
                )

    def update_file(self, file_id: int, **values: Any) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(m.ingested_files).where(m.ingested_files.c.file_id == file_id).values(**values)
            )

    def list_files(
        self, entity: str | None = None, status: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        f = m.ingested_files
        query = select(f).order_by(f.c.file_id.desc()).limit(limit)
        if entity:
            query = query.where(f.c.entity == entity)
        if status:
            query = query.where(f.c.status == status)
        with self.engine.connect() as conn:
            return [_row(r) for r in conn.execute(query).mappings()]

    def record_schema_event(
        self,
        entity: str,
        file_id: int,
        batch_id: str | None,
        event_type: str,
        columns: list[str],
        action: str,
        contract_version: int,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(m.schema_events).values(
                    entity=entity,
                    file_id=file_id,
                    batch_id=batch_id,
                    event_type=event_type,
                    columns=sorted(columns),
                    action=action,
                    contract_version=contract_version,
                    detected_at=utcnow(),
                )
            )

    def schema_events(self, entity: str | None = None) -> list[dict[str, Any]]:
        query = select(m.schema_events).order_by(m.schema_events.c.id)
        if entity:
            query = query.where(m.schema_events.c.entity == entity)
        with self.engine.connect() as conn:
            return [_row(r) for r in conn.execute(query).mappings()]

    def recent_batch_volumes(self, entity: str, before_seq: int, limit: int) -> list[int]:
        """Bronze record counts of the previous successful batches with data (newest first)."""
        bl = m.batch_layers
        query = (
            select(bl.c.records_read)
            .join(m.batches, m.batches.c.batch_id == bl.c.batch_id)
            .where(
                m.batches.c.entity == entity,
                m.batches.c.batch_seq < before_seq,
                bl.c.layer == "bronze",
                bl.c.status == "SUCCEEDED",
                bl.c.records_read > 0,
            )
            .order_by(m.batches.c.batch_seq.desc())
            .limit(limit)
        )
        with self.engine.connect() as conn:
            return [int(r.records_read) for r in conn.execute(query)]

    def layer_rows(
        self, layer: str, entity: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        bl = m.batch_layers
        query = (
            select(bl, m.batches.c.entity, m.batches.c.batch_seq, m.batches.c.ingestion_date)
            .join(m.batches, m.batches.c.batch_id == bl.c.batch_id)
            .where(bl.c.layer == layer)
            .order_by(m.batches.c.batch_seq)
        )
        if entity:
            query = query.where(m.batches.c.entity == entity)
        if status:
            query = query.where(bl.c.status == status)
        with self.engine.connect() as conn:
            return [_row(r) for r in conn.execute(query).mappings()]

    # ------------------------------------------------------------------ quality
    def save_quality_results(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.engine.begin() as conn:
            conn.execute(
                insert(m.quality_results),
                [dict(r, evaluated_at=r.get("evaluated_at") or utcnow()) for r in rows],
            )

    def list_quality_results(
        self,
        limit: int = 100,
        offset: int = 0,
        run_id: str | None = None,
        batch_id: str | None = None,
        status: str | None = None,
        severity: str | None = None,
        rule_id: str | None = None,
        table_name: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        q = m.quality_results
        conditions = [
            cond
            for cond in (
                q.c.run_id == run_id if run_id else None,
                q.c.batch_id == batch_id if batch_id else None,
                q.c.status == status if status else None,
                q.c.severity == severity if severity else None,
                q.c.rule_id == rule_id if rule_id else None,
                q.c.table_name == table_name if table_name else None,
            )
            if cond is not None
        ]
        query = select(q).order_by(q.c.id.desc())
        count = select(func.count()).select_from(q)
        if conditions:
            query, count = query.where(and_(*conditions)), count.where(and_(*conditions))
        with self.engine.connect() as conn:
            rows = conn.execute(query.limit(limit).offset(offset)).mappings().all()
            total = conn.execute(count).scalar_one()
        return [_row(r) for r in rows], total

    # ------------------------------------------------------------------ gold publications
    def record_publication(
        self,
        run_id: str,
        status: str,
        table_versions: dict[str, int],
        source_versions: dict[str, int],
        row_counts: dict[str, int],
        blocking_failures: list[str],
    ) -> int:
        with self.engine.begin() as conn:
            pub_id = _pk(
                conn.execute(
                    insert(m.gold_publications).values(
                        run_id=run_id,
                        status=status,
                        table_versions=table_versions,
                        source_versions=source_versions,
                        row_counts=row_counts,
                        blocking_failures=blocking_failures,
                        created_at=utcnow(),
                    )
                )
            )
        return int(pub_id)

    def latest_publication(self, status: str = "PUBLISHED") -> dict[str, Any] | None:
        g = m.gold_publications
        with self.engine.connect() as conn:
            row = (
                conn.execute(select(g).where(g.c.status == status).order_by(g.c.id.desc()).limit(1))
                .mappings()
                .first()
            )
            return _row(row) if row else None

    def upsert_metric_definitions(self, definitions: Sequence[dict[str, Any]]) -> None:
        md = m.metric_definitions
        with self.engine.begin() as conn:
            for definition in definitions:
                values = dict(definition, updated_at=utcnow())
                result = conn.execute(
                    update(md).where(md.c.metric_name == definition["metric_name"]).values(**values)
                )
                if result.rowcount == 0:
                    conn.execute(insert(md).values(**values))

    # ------------------------------------------------------------------ locks
    def acquire_lock(self, name: str, owner: str, ttl_seconds: int) -> bool:
        now = utcnow()
        expires = now + timedelta(seconds=ttl_seconds)
        lk = m.locks
        with self.engine.begin() as conn:
            try:
                with conn.begin_nested():
                    conn.execute(
                        insert(lk).values(lock_name=name, owner=owner, acquired_at=now, expires_at=expires)
                    )
                return True
            except IntegrityError:
                pass
            # Take over only an expired lease (the previous holder crashed without releasing it).
            result = conn.execute(
                update(lk)
                .where(lk.c.lock_name == name, or_(lk.c.expires_at < now, lk.c.owner == owner))
                .values(owner=owner, acquired_at=now, expires_at=expires)
            )
            return bool(result.rowcount == 1)

    def release_lock(self, name: str, owner: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(delete(m.locks).where(m.locks.c.lock_name == name, m.locks.c.owner == owner))

    @contextmanager
    def lock(self, name: str, ttl_seconds: int, owner: str | None = None) -> Iterator[str]:
        owner = owner or f"{socket.gethostname()}:{uuid.uuid4().hex[:8]}"
        if not self.acquire_lock(name, owner, ttl_seconds):
            raise LockNotAcquiredError(f"lock {name!r} is held by another process")
        try:
            yield owner
        finally:
            self.release_lock(name, owner)

    # ------------------------------------------------------------------ summaries (API / metrics)
    def run_status_counts(self) -> list[dict[str, Any]]:
        r = m.runs
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(r.c.pipeline, r.c.status, func.count().label("runs")).group_by(
                    r.c.pipeline, r.c.status
                )
            ).mappings()
            return [dict(row) for row in rows]

    def last_runs_by_pipeline(self) -> list[dict[str, Any]]:
        r = m.runs
        latest = (
            select(r.c.pipeline, func.max(r.c.started_at).label("started_at"))
            .group_by(r.c.pipeline)
            .subquery()
        )
        query = select(r).join(
            latest, and_(latest.c.pipeline == r.c.pipeline, latest.c.started_at == r.c.started_at)
        )
        with self.engine.connect() as conn:
            return [_row(row) for row in conn.execute(query).mappings()]

    def layer_totals(self) -> list[dict[str, Any]]:
        bl = m.batch_layers
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(
                    bl.c.layer,
                    m.batches.c.entity,
                    bl.c.status,
                    func.count().label("batches"),
                    func.coalesce(func.sum(bl.c.records_read), 0).label("records_read"),
                    func.coalesce(func.sum(bl.c.records_accepted), 0).label("records_accepted"),
                    func.coalesce(func.sum(bl.c.records_rejected), 0).label("records_rejected"),
                )
                .join(m.batches, m.batches.c.batch_id == bl.c.batch_id)
                .group_by(bl.c.layer, m.batches.c.entity, bl.c.status)
            ).mappings()
            return [dict(row) for row in rows]

    def latest_quality_status(self) -> list[dict[str, Any]]:
        """Most recent result of every rule."""
        q = m.quality_results
        latest = select(q.c.rule_id, func.max(q.c.id).label("id")).group_by(q.c.rule_id).subquery()
        with self.engine.connect() as conn:
            rows = conn.execute(select(q).join(latest, latest.c.id == q.c.id)).mappings()
            return [_row(r) for r in rows]

    def file_status_counts(self) -> list[dict[str, Any]]:
        f = m.ingested_files
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(f.c.entity, f.c.status, func.count().label("files")).group_by(f.c.entity, f.c.status)
            ).mappings()
            return [dict(row) for row in rows]


def _pk(result: CursorResult[Any]) -> int:
    key = result.inserted_primary_key
    if key is None:
        raise RuntimeError("insert did not return a primary key")
    return int(key[0])


def _row(row: RowMapping) -> dict[str, Any]:
    out = dict(row)
    for key, value in out.items():
        if isinstance(value, datetime):
            out[key] = aware(value)
    return out
