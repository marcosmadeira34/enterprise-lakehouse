"""Operational metadata schema (SQLAlchemy Core).

Runs on PostgreSQL in Docker and on SQLite for fast tests; only portable constructs are used. Partial
unique indexes are declared for both dialects so the database itself enforces "one live registration
per file content".
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    text,
)

SCHEMA_VERSION = 1

metadata = MetaData()

# Integer surrogate keys use BIGINT on PostgreSQL; SQLite only auto-increments INTEGER PRIMARY KEY.
BigId = BigInteger().with_variant(Integer(), "sqlite")

schema_version = Table(
    "schema_version",
    metadata,
    Column("version", Integer, primary_key=True),
    Column("applied_at", DateTime(timezone=True), nullable=False),
)

runs = Table(
    "pipeline_runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("pipeline", String(32), nullable=False),  # ingest | bronze | silver | gold | quality | ...
    Column("status", String(16), nullable=False),  # RUNNING | SUCCEEDED | FAILED | ABANDONED
    Column("correlation_id", String(128), nullable=False),  # shared by all steps of one pipeline execution
    Column("attempt", Integer, nullable=False, server_default=text("1")),
    Column("trigger", String(32), nullable=False),  # cli | airflow | benchmark | test
    Column("params", JSON, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("duration_seconds", Float),
    Column("records_read", BigInteger),
    Column("records_written", BigInteger),
    Column("records_rejected", BigInteger),
    Column("error_type", String(128)),
    Column("error_message", Text),
    Index("ix_runs_started", "started_at"),
    Index("ix_runs_correlation", "correlation_id"),
)

run_errors = Table(
    "run_errors",
    metadata,
    Column("id", BigId, primary_key=True, autoincrement=True),
    Column("run_id", String(64), ForeignKey("pipeline_runs.run_id"), nullable=False),
    Column("step", String(128), nullable=False),
    Column("entity", String(64)),
    Column("batch_id", String(64)),
    Column("error_type", String(128), nullable=False),
    Column("message", Text, nullable=False),
    Column("traceback", Text),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Index("ix_run_errors_run", "run_id"),
)

batches = Table(
    "batches",
    metadata,
    # batch_seq gives a total order of arrival used to break ties between record versions.
    Column("batch_seq", BigId, primary_key=True, autoincrement=True),
    Column("batch_id", String(64), nullable=False, unique=True),
    Column("entity", String(64), nullable=False),
    Column("source_system", String(64), nullable=False),
    Column("run_id", String(64), ForeignKey("pipeline_runs.run_id"), nullable=False),
    Column("ingestion_date", String(10), nullable=False),  # partition value in Bronze (yyyy-MM-dd)
    Column("files_received", Integer, nullable=False),
    Column("bytes_received", BigInteger, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("ix_batches_entity", "entity", "batch_seq"),
)

# Status of each batch in each layer. A row per (batch, layer) makes partial progress explicit.
batch_layers = Table(
    "batch_layers",
    metadata,
    Column("batch_id", String(64), ForeignKey("batches.batch_id"), primary_key=True),
    Column("layer", String(16), primary_key=True),  # bronze | silver
    Column("status", String(16), nullable=False),  # PENDING | RUNNING | SUCCEEDED | FAILED
    Column("run_id", String(64)),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("records_read", BigInteger),
    Column("records_accepted", BigInteger),
    Column("records_rejected", BigInteger),
    Column("delta_version", BigInteger),
    Column("metrics", JSON),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("error_message", Text),
    Index("ix_batch_layers_status", "layer", "status"),
)

ingested_files = Table(
    "ingested_files",
    metadata,
    Column("file_id", BigId, primary_key=True, autoincrement=True),
    Column("entity", String(64), nullable=False),
    Column("uri", Text, nullable=False),
    Column("file_name", String(512), nullable=False),
    Column("content_sha256", String(64), nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("source_modified_at", DateTime(timezone=True)),
    # PENDING (assigned to a batch, not yet in Bronze) | COMMITTED | DUPLICATE | QUARANTINED | FAILED
    Column("status", String(16), nullable=False),
    Column("batch_id", String(64), ForeignKey("batches.batch_id")),
    Column("run_id", String(64), nullable=False),
    Column("duplicate_of", BigInteger),
    Column("records_read", BigInteger),
    Column("schema_columns", JSON),
    Column("error_message", Text),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("committed_at", DateTime(timezone=True)),
    # At most one live registration (pending or committed) per content: the database is the arbiter
    # when two ingestion processes race on the same file.
    Index(
        "ux_ingested_files_live_content",
        "entity",
        "content_sha256",
        unique=True,
        postgresql_where=text("status IN ('PENDING', 'COMMITTED')"),
        sqlite_where=text("status IN ('PENDING', 'COMMITTED')"),
    ),
    Index("ix_ingested_files_batch", "batch_id"),
)

schema_events = Table(
    "schema_events",
    metadata,
    Column("id", BigId, primary_key=True, autoincrement=True),
    Column("entity", String(64), nullable=False),
    Column("file_id", BigInteger, nullable=False),
    Column("batch_id", String(64)),
    Column("event_type", String(32), nullable=False),  # ADDED_COLUMNS | MISSING_OPTIONAL | MISSING_REQUIRED
    Column("columns", JSON, nullable=False),
    Column("action", String(32), nullable=False),  # CAPTURED | NULL_FILLED | QUARANTINED
    Column("contract_version", Integer, nullable=False),
    Column("detected_at", DateTime(timezone=True), nullable=False),
)

quality_results = Table(
    "quality_results",
    metadata,
    Column("id", BigId, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("batch_id", String(64)),
    Column("rule_id", String(128), nullable=False),
    Column("rule_name", String(256), nullable=False),
    Column("description", Text, nullable=False),
    Column("category", String(32), nullable=False),  # completeness | validity | ... | reconciliation
    Column("layer", String(16), nullable=False),
    Column("table_name", String(128), nullable=False),
    Column("severity", String(16), nullable=False),  # BLOCKING | WARNING
    Column("records_evaluated", BigInteger, nullable=False),
    Column("violations", BigInteger, nullable=False),
    Column("compliance_pct", Float, nullable=False),
    Column("threshold_pct", Float),
    Column("status", String(16), nullable=False),  # PASSED | WARNED | FAILED
    Column("observed_value", Numeric(24, 4)),
    Column("expected_value", Numeric(24, 4)),
    Column("details", JSON),
    Column("evaluated_at", DateTime(timezone=True), nullable=False),
    Index("ix_quality_rule", "rule_id", "evaluated_at"),
    Index("ix_quality_run", "run_id"),
    Index("ix_quality_batch", "batch_id"),
)

gold_publications = Table(
    "gold_publications",
    metadata,
    Column("id", BigId, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("status", String(16), nullable=False),  # PUBLISHED | BLOCKED
    Column("table_versions", JSON, nullable=False),  # gold table -> Delta version
    Column("source_versions", JSON, nullable=False),  # silver table -> Delta version read
    Column("row_counts", JSON, nullable=False),
    Column("blocking_failures", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Index("ix_gold_publications_status", "status", "id"),
)

metric_definitions = Table(
    "metric_definitions",
    metadata,
    Column("metric_name", String(128), primary_key=True),
    Column("table_name", String(128), nullable=False),
    Column("description", Text, nullable=False),
    Column("formula", Text, nullable=False),
    Column("source_tables", JSON, nullable=False),
    Column("filters", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

locks = Table(
    "processing_locks",
    metadata,
    Column("lock_name", String(128), primary_key=True),
    Column("owner", String(128), nullable=False),
    Column("acquired_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
)

__all__ = [
    "SCHEMA_VERSION",
    "batch_layers",
    "batches",
    "gold_publications",
    "ingested_files",
    "locks",
    "metadata",
    "metric_definitions",
    "quality_results",
    "run_errors",
    "runs",
    "schema_events",
    "schema_version",
]
