"""
### Enterprise Lakehouse DAGs

Orchestration only: every task runs the `lakehouse` CLI from its own virtualenv
(`/opt/lakehouse/venv`) as a subprocess, so each Spark job gets its own JVM in local mode inside the
worker, and DAG parsing never imports PySpark. All steps are idempotent, so retries and manual
re-runs never duplicate data.

* `lakehouse_ingestion`, `lakehouse_bronze`, `lakehouse_silver`, `lakehouse_gold`,
  `lakehouse_quality`, `lakehouse_reconciliation`: one layer/step each, manually triggered.
* `lakehouse_pipeline`: the full chain, with one Silver task per entity following foreign keys.
* `lakehouse_reprocess`: parameterized historical reprocessing (batches, date range or full refresh).
* `lakehouse_demo_data`: writes synthetic files to the landing bucket.

CLI exit codes: 0 ok, 2 blocking quality failure, 3 lock busy (retried), 1 error.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

LAKEHOUSE_BIN = os.getenv("LAKEHOUSE_BIN", "/opt/lakehouse/venv/bin/lakehouse")
ENTITIES = ["categories", "products", "customers", "stores", "sales", "sale_items", "payments", "inventory"]
# Silver task dependencies mirror the foreign keys of the data contracts.
SILVER_PARENTS = {
    "products": ["categories"],
    "sales": ["stores", "customers"],
    "sale_items": ["sales", "products"],
    "payments": ["sales"],
    "inventory": ["stores", "products"],
}
START = datetime(2026, 1, 1)
log = logging.getLogger("lakehouse.alerts")


def alert_on_failure(context: dict[str, Any]) -> None:
    """Structured alert line; Prometheus alert rules cover the same failures from the metadata side."""
    ti = context["ti"]
    log.error(json.dumps({"event": "airflow_task_failed", "dag_id": ti.dag_id, "task_id": ti.task_id,
                          "run_id": context.get("run_id"), "try_number": ti.try_number}))  # fmt: skip


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": alert_on_failure,
}

ENTITY_FLAGS = "{% for e in params.entities %} --entity {{ e }}{% endfor %}"
BATCH_FLAGS = "{% for b in params.batch_ids %} --batch-id {{ b }}{% endfor %}"


def cli(task_id: str, args: str, **kwargs: Any) -> BashOperator:
    command = (
        f"{LAKEHOUSE_BIN} --trigger airflow --correlation-id '{{{{ run_id }}}}' "
        f"--attempt {{{{ ti.try_number }}}} {args}"
    )
    return BashOperator(task_id=task_id, bash_command=command, **kwargs)


def entity_params(with_batches: bool = False) -> dict[str, Param]:
    params = {
        "entities": Param(ENTITIES, type="array", items={"type": "string", "enum": ENTITIES},
                          description="Entities to process (dependency order is applied by the CLI)."),
    }  # fmt: skip
    if with_batches:
        params["batch_ids"] = Param([], type="array", items={"type": "string"},
                                    description="Restrict to these batch ids (empty = all pending).")  # fmt: skip
    return params


def layer_dag(dag_id: str, doc: str, args: str, params: dict[str, Param] | None = None) -> DAG:
    with DAG(
        dag_id=dag_id,
        schedule=None,
        start_date=START,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        params=params or {},
        doc_md=doc,
        tags=["lakehouse", "layer"],
    ) as dag:
        cli(dag_id.removeprefix("lakehouse_"), args)
    return dag


ingestion = layer_dag("lakehouse_ingestion", "Discover landing files, detect duplicates by content hash, plan batches.",
                      f"ingest{ENTITY_FLAGS}", entity_params())  # fmt: skip
bronze = layer_dag("lakehouse_bronze", "Load planned batches into Bronze (one Delta commit per batch).",
                   f"bronze{ENTITY_FLAGS}{BATCH_FLAGS}", entity_params(True))  # fmt: skip
silver = layer_dag("lakehouse_silver", "Conform pending Bronze batches into Silver (idempotent MERGE).",
                   f"silver{ENTITY_FLAGS}{BATCH_FLAGS}", entity_params(True))  # fmt: skip
gold = layer_dag("lakehouse_gold", "Rebuild, audit and publish Gold (write-audit-publish).", "gold")
quality = layer_dag(
    "lakehouse_quality", "Silver and Gold quality suites; fails on BLOCKING rules.", "quality"
)
reconciliation = layer_dag("lakehouse_reconciliation", "Cross-layer count and financial reconciliation.",
                           f"reconcile{ENTITY_FLAGS}", entity_params())  # fmt: skip


with DAG(
    dag_id="lakehouse_pipeline",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,  # one pipeline at a time; entity locks protect against overlap anyway
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["lakehouse", "pipeline"],
) as pipeline:
    recover = cli("recover", "recover", retries=0)
    ingest = cli("ingest", "ingest")
    bronze_all = cli("bronze", "bronze")
    silver_tasks = {e: cli(f"silver_{e}", f"silver --entity {e}") for e in ENTITIES}
    gold_task = cli("gold", "gold")
    quality_task = cli("quality", "quality", retries=0)  # a failed rule will not pass on retry
    reconcile_task = cli("reconcile", "reconcile", retries=0)

    recover >> ingest >> bronze_all
    for entity, task in silver_tasks.items():
        parents = SILVER_PARENTS.get(entity, [])
        if parents:
            [silver_tasks[p] for p in parents] >> task
        else:
            bronze_all >> task
    list(silver_tasks.values()) >> gold_task >> quality_task >> reconcile_task


with DAG(
    dag_id="lakehouse_reprocess",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    params={
        "entities": Param(["sales"], type="array", minItems=1, items={"type": "string", "enum": ENTITIES}),
        "batch_ids": Param([], type="array", items={"type": "string"}, description="Batches to replay"),
        "since": Param(None, type=["null", "string"], format="date", description="Ingestion date from"),
        "until": Param(None, type=["null", "string"], format="date", description="Ingestion date to"),
        "full_refresh": Param(
            False, type="boolean", description="Rebuild the entities from all Bronze batches"
        ),
        "publish_gold": Param(True, type="boolean"),
    },
    doc_md="""
Historical reprocessing from Bronze (immutable). Replays selected batches through Silver — safe
because the MERGE never lets older versions overwrite newer ones — or rebuilds entities entirely
(`full_refresh`), then republishes Gold and reconciles.
""",
    tags=["lakehouse", "reprocess"],
) as reprocess:
    flags = (
        f"{ENTITY_FLAGS}{BATCH_FLAGS}"
        "{% if params.since %} --since {{ params.since }}{% endif %}"
        "{% if params.until %} --until {{ params.until }}{% endif %}"
        "{% if params.full_refresh %} --full-refresh{% endif %}"
        "{% if not params.publish_gold %} --skip-gold{% endif %}"
    )
    cli("reprocess", f"reprocess{flags}") >> cli("reconcile", "reconcile")


with DAG(
    dag_id="lakehouse_demo_data",
    schedule=None,
    start_date=START,
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    params={
        "drop": Param(
            1, type="integer", enum=[1, 2], description="1 = initial load, 2 = incremental changes"
        ),
        "scale": Param("small", type="string", enum=["tiny", "small", "medium", "large"]),
        "seed": Param(42, type="integer"),
        "error_rate": Param(0.01, type="number", minimum=0, maximum=0.5),
    },
    doc_md="Write a deterministic synthetic drop to the landing bucket (see docs/data_dictionary.md).",
    tags=["lakehouse", "demo"],
) as demo:
    cli("generate", "generate --drop {{ params.drop }} --scale {{ params.scale }} --seed {{ params.seed }} "
                    "--error-rate {{ params.error_rate }}")  # fmt: skip
