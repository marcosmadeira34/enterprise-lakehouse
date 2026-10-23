# Operations

## Starting and stopping

```bash
make env        # creates .env with random local secrets (once)
make up         # build + start: postgres, minio, airflow (api-server, scheduler, dag-processor), api, prometheus, grafana
make ps         # health of every service
make down       # stop, keep data
make reset      # stop and DELETE volumes (metadata, lake, logs)
```

Start-up order is enforced by health checks: PostgreSQL and MinIO healthy → `airflow-init` (DB migration)
and `lakehouse-init` (metadata schema, buckets) complete → Airflow services and API start.

| Service | URL (defaults from `.env.example`) |
|---|---|
| Airflow | http://localhost:18088 |
| API (OpenAPI docs) | http://localhost:18000/docs |
| Grafana | http://localhost:13000 (admin / `GRAFANA_ADMIN_PASSWORD`) |
| Prometheus | http://localhost:19090 |
| MinIO console | http://localhost:19011 (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`) |
| PostgreSQL | localhost:15432 |

## How Airflow runs Spark here

The scheduler container runs tasks (LocalExecutor). Each task executes
`/opt/lakehouse/venv/bin/lakehouse --trigger airflow --correlation-id {{ run_id }} --attempt {{ ti.try_number }} <step>`,
which starts a local Spark JVM inside that container. What the task can reach:

* MinIO at `http://minio:9000` (S3A, path-style) and PostgreSQL at `postgres:5432`, both on the compose
  network `lakehouse`; never `localhost`.
* Java 17 at `/usr/lib/jvm/java-17-openjdk-amd64`; Delta and hadoop-aws jars pre-resolved in
  `/opt/lakehouse/ivy` (no Maven access needed at run time).
* Configuration: `LAKEHOUSE_CONFIG` + `LAKEHOUSE__*` variables defined once in `docker-compose.yml`
  (`x-lakehouse-env`).

`make dag-check` validates DAG imports and structure inside the container.

## Running the pipeline

```bash
# Through Airflow (UI: trigger lakehouse_demo_data, then lakehouse_pipeline), or:
make demo                 # generate drop 1 in MinIO + run-all inside the container
make demo-drop2           # incremental drop (updates, late sales, schema change, duplicate, incompatible file)
make pipeline             # unpause + trigger the lakehouse_pipeline DAG

# Individual steps (inside the container: docker compose exec airflow-scheduler /opt/lakehouse/venv/bin/lakehouse …)
lakehouse ingest [--entity sales]
lakehouse bronze [--entity sales] [--batch-id b-…]
lakehouse silver [--entity sales] [--batch-id b-…]
lakehouse gold
lakehouse quality [--suite silver|gold]
lakehouse reconcile
lakehouse run-all
```

Locally without Docker (local filesystem + SQLite, default `config/lakehouse.yaml`):

```bash
uv sync --all-extras
uv run lakehouse init
uv run lakehouse generate --scale small
uv run lakehouse run-all
```

### Exit codes

| Code | Meaning | Airflow behaviour |
|---|---|---|
| 0 | success | — |
| 1 | error (see `run_errors` and the JSON log) | retried (`retries=2`, exponential back-off) |
| 2 | a BLOCKING quality rule failed | task fails; quality/reconcile tasks have `retries=0` |
| 3 | another process holds the lock | retried |

## Re-execution

Every step can be re-run at any time; see the idempotency table in `docs/architecture.md`. In Airflow,
"Clear" on any task or a new DAG run is safe.

## Historical reprocessing

```bash
# Replay specific batches (upsert semantics; older versions never overwrite newer ones)
lakehouse reprocess --entity sale_items --batch-id b-sale_items-20261022T221530-3f2a9c1d
# Replay everything ingested in a date range
lakehouse reprocess --entity sale_items --since 2026-10-01 --until 2026-10-22
# Rebuild an entity from all Bronze batches (needed when accepted records must disappear)
lakehouse reprocess --entity sales --full-refresh
```

Gold is republished at the end (unless `--skip-gold`); run `lakehouse reconcile` afterwards. The
`lakehouse_reprocess` DAG exposes the same parameters (entities, batch_ids, since, until, full_refresh,
publish_gold) and reconciles at the end. Reprocessing a parent (e.g. products) does not cascade to
children: include them explicitly (`--entity products --entity sale_items`).

## Recovery

```bash
lakehouse recover   # runs RUNNING > stale_run_seconds → ABANDONED; their batch layers → FAILED (retryable)
```

`lakehouse_pipeline` runs `recover` as its first task. Data-level recovery is automatic in the jobs
(see `docs/failure_scenarios.md`).

## Inspecting runs, batches and quality

```bash
lakehouse status                    # last run per pipeline, layer totals, file states, current publication
lakehouse runs --limit 20           # history
lakehouse runs --run-id <run_id>    # one run with errors and batch layers
lakehouse rejects --entity sales --reason MISSING_REQUIRED:store_id
curl -s localhost:18000/runs?status=FAILED | jq
curl -s 'localhost:18000/quality/results?severity=BLOCKING&status=FAILED' | jq
curl -s 'localhost:18000/analytics/sales-summary?group_by=store&start_date=2026-07-01' | jq
```

SQL against the metadata database (`psql -h localhost -p 15432 -U lakehouse lakehouse`):

```sql
-- Runs of one pipeline execution (Airflow run_id = correlation_id)
SELECT pipeline, status, attempt, duration_seconds, records_read, records_written, records_rejected, error_message
FROM pipeline_runs WHERE correlation_id = 'manual__2026-10-22T22:00:00+00:00' ORDER BY started_at;

-- Batch status per layer
SELECT b.entity, b.batch_id, l.layer, l.status, l.attempts, l.records_read, l.records_accepted, l.records_rejected
FROM batches b JOIN batch_layers l USING (batch_id) ORDER BY b.batch_seq, l.layer;

-- Files that did not load, and why
SELECT entity, file_name, status, error_message FROM ingested_files WHERE status IN ('QUARANTINED', 'DUPLICATE');

-- Schema drift
SELECT entity, event_type, columns, action, detected_at FROM schema_events ORDER BY id DESC;

-- Latest result of every rule that is not passing
SELECT DISTINCT ON (rule_id) rule_id, severity, status, violations, records_evaluated, evaluated_at
FROM quality_results ORDER BY rule_id, id DESC;

-- Errors with their step
SELECT r.pipeline, e.step, e.entity, e.batch_id, e.error_type, e.message, e.occurred_at
FROM run_errors e JOIN pipeline_runs r USING (run_id) ORDER BY e.id DESC LIMIT 20;
```

## Observability

* **Logs**: JSON on stderr, one object per event with `run_id`, `correlation_id`, `pipeline`,
  `entity`, `batch_id`, `event` and counts (Airflow task logs show them as-is).
* **Metrics**: `GET /metrics` on the API, scraped by Prometheus every 15 s (`lakehouse_runs_total`,
  `lakehouse_last_run_failed`, `lakehouse_last_run_duration_seconds`, `lakehouse_last_run_records_per_second`,
  `lakehouse_layer_records_total`, `lakehouse_layer_batches`, `lakehouse_files`, `lakehouse_quality_rule_*`,
  `lakehouse_gold_publication_age_seconds`).
* **Alerts**: `infrastructure/prometheus/alerts.yml` (failed pipeline, blocking rule failed, warnings,
  quarantined files, stale Gold, metrics down) — visible in Prometheus' *Alerts* page; no Alertmanager
  receiver is configured in v1. Airflow's `on_failure_callback` logs an `airflow_task_failed` event.
* **Dashboard**: Grafana → *Lakehouse — Operations*.

## Tests

```bash
make test-fast          # unit, no Spark: 46 tests, ~2 s
make test-spark         # Spark unit + local pipeline integration: 32 tests, ~3 min
make test-resilience    # failure injection: 8 tests, ~3 min
make test               # everything without Docker
make up && make test-integration   # against PostgreSQL + MinIO in Docker: 4 tests, ~2.5 min
make lint typecheck
```
