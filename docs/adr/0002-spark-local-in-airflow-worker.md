# 0002 — Spark in local mode, launched by Airflow as a CLI subprocess

## Context
The first version must run on a laptop with Docker Compose, without a cluster manager. Airflow 3 tasks
talk to the API server through the Task SDK; heavy libraries in Airflow's own environment risk
dependency conflicts and slow DAG parsing.

## Decision
* The Airflow image contains Java 17 and a **separate virtualenv** (`/opt/lakehouse/venv`) with PySpark,
  Delta and the `lakehouse` package. Airflow's environment is untouched.
* Each task is a `BashOperator` running `lakehouse <step>`; the CLI starts Spark in `local[N]` mode inside
  the worker (the scheduler container, LocalExecutor). One JVM per task, released when the task ends.
* Delta and hadoop-aws jars are resolved at image build time into an Ivy cache, so tasks need no
  network access to Maven.
* DAG files contain orchestration only and never import PySpark.

## Consequences
* Each task pays ~10 s JVM start-up; acceptable for batch steps.
* Parallelism is bounded by the worker container (`MAX_ACTIVE_TASKS_PER_DAG=2`).
* Moving to a cluster is a configuration change: `LAKEHOUSE__SPARK__MASTER` (e.g. `spark://...` or
  `k8s://...`) plus shipping the same package; jobs do not change.
* The same CLI is what developers and tests run, so orchestration adds no untested code path.
