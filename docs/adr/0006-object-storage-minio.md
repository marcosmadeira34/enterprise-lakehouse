# 0006 — MinIO (frozen Bitnami image) behind the S3 API only

## Context
The brief asks for MinIO. During the environment inspection (2026-10-22): `minio/minio` no longer exists
on Docker Hub ("repository does not exist") and `quay.io/minio/*` answers 401. The only MinIO image that
could be pulled was `bitnamilegacy/minio:2025.7.23`, a frozen build that receives no updates.

## Decision
* Use `bitnamilegacy/minio:2025.7.23` for local development only, bound to `127.0.0.1`.
* All code reaches storage through the S3 API (boto3 for ingestion, S3A for Spark, object_store for
  delta-rs). Nothing depends on MinIO-specific features; buckets are created by the image and again,
  idempotently, by `lakehouse init`.

## Consequences
* Security fixes will not arrive for this image: acceptable for a local lab, not for any shared deployment.
* Swapping the backend (RustFS, SeaweedFS, Ceph RGW or AWS S3) is a compose change plus
  `LAKEHOUSE__STORAGE__S3_ENDPOINT_URL`; no code change.
