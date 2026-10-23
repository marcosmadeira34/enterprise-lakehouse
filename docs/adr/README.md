# Architecture decision records

| # | Decision | Status |
|---|---|---|
| [0001](0001-medallion-delta-lake.md) | Medallion layers on Delta Lake | Accepted |
| [0002](0002-spark-local-in-airflow-worker.md) | Spark in local mode, launched by Airflow as a CLI subprocess | Accepted |
| [0003](0003-file-identity-content-hash.md) | File identity is the content hash | Accepted |
| [0004](0004-consistency-by-recovery.md) | Delta/PostgreSQL consistency by commit tagging and recovery | Accepted |
| [0005](0005-idempotent-silver-merge.md) | Idempotent Silver: guarded MERGE and per-batch reject replacement | Accepted |
| [0006](0006-object-storage-minio.md) | MinIO (frozen Bitnami image) behind the S3 API only | Accepted, with risk |
| [0007](0007-controlled-schema-evolution.md) | Controlled schema evolution | Accepted |
| [0008](0008-gold-write-audit-publish.md) | Gold rebuild with write-audit-publish and version pinning | Accepted |
| [0009](0009-metadata-store.md) | Operational metadata in PostgreSQL via SQLAlchemy Core | Accepted |
| [0010](0010-metrics-from-metadata.md) | Metrics derived from metadata (pull), no Pushgateway | Accepted |
