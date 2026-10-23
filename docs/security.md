# Security

Scope: a local laboratory. These are the controls that exist, and the limits of the environment.

## Credentials

* No secret is committed. `.env` is git-ignored; `.env.example` contains placeholders only;
  `make env` (`scripts/make_env.py`) generates random values locally with mode `0600`.
* Secrets reach processes only through environment variables (`AWS_*`, database URLs). The YAML
  configuration never holds credentials; S3 keys are read from the environment at use time
  (`StorageSettings.s3_access_key`), so they are not part of `Settings` dumps.
* PostgreSQL has separate roles: `airflow` (Airflow DB), `lakehouse` (owner of the metadata DB) and
  `lakehouse_reader` (read-only, used by the API).
* All published ports bind to `127.0.0.1`.

## Logs

* JSON logs pass through `redact()` (`observability/logging.py`): keys that look like secrets
  (`password`, `secret`, `token`, `access_key`, …) are replaced and credentials in URLs
  (`scheme://user:password@`) are masked. Unit-tested.
* Logs carry identifiers and counts, never record contents. Rejected records are stored in the lake
  (`silver._rejects.raw_record`), not in logs.

## Personal data

* All data is synthetic: generic name lists and the reserved `example.com` e-mail domain.
* The model still treats customer names, e-mail and phone as personal data: they exist in Bronze, Silver
  and the rejects table only; **Gold has no direct identifiers** (asserted in
  `test_gold_matches_oracle_and_is_published`). The API exposes Gold aggregates and operational metadata,
  never Silver.

## Limits of this environment

* MinIO runs a frozen image without security updates (ADR-0006) and the pipelines use the root
  credentials; in any shared environment use a dedicated service account with bucket-scoped policies.
* No TLS between services (internal Docker network); no authentication on the FastAPI service; Airflow
  runs with `SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`. All acceptable only on `localhost`.
* No encryption at rest beyond what the host disk provides.
* Bronze is append-only by table property, which prevents accidental updates through Delta APIs, not
  deliberate deletion of objects in the bucket.

## Hardening path

Object-storage service accounts and bucket policies; TLS and OIDC for Airflow and the API; secrets from
a vault (Docker/K8s secrets) instead of `.env`; column masking or tokenization of PII in Silver;
row-level audit of `_rejects` access.
