"""DAG integrity check, run inside the Airflow container (`make dag-check`).

Fails on import errors and verifies the structure the platform relies on: the expected DAGs exist,
every task calls the lakehouse CLI, and Silver tasks follow the foreign-key order.
"""

from __future__ import annotations

import sys

try:  # Airflow >= 3.1
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # pragma: no cover
    from airflow.models.dagbag import DagBag  # type: ignore[no-redef]

EXPECTED = {
    "lakehouse_ingestion", "lakehouse_bronze", "lakehouse_silver", "lakehouse_gold", "lakehouse_quality",
    "lakehouse_reconciliation", "lakehouse_pipeline", "lakehouse_reprocess", "lakehouse_demo_data",
}  # fmt: skip


def main() -> int:
    bag = DagBag(dag_folder="/opt/airflow/dags")  # examples are off via AIRFLOW__CORE__LOAD_EXAMPLES
    problems = [f"import error in {path}: {err}" for path, err in bag.import_errors.items()]
    missing = EXPECTED - set(bag.dag_ids)
    problems += [f"missing DAG {d}" for d in sorted(missing)]
    for dag_id in EXPECTED - missing:
        for task in bag.get_dag(dag_id).tasks:
            if "/opt/lakehouse/venv/bin/lakehouse" not in getattr(task, "bash_command", ""):
                problems.append(f"{dag_id}.{task.task_id} does not call the lakehouse CLI")
    if "lakehouse_pipeline" not in missing:
        dag = bag.get_dag("lakehouse_pipeline")
        upstream = {t.task_id: set(t.upstream_task_ids) for t in dag.tasks}
        checks = {
            "silver_sale_items": {"silver_sales", "silver_products"},
            "silver_sales": {"silver_stores", "silver_customers"},
            "silver_products": {"silver_categories"},
            "gold": {
                f"silver_{e}"
                for e in (
                    "categories",
                    "products",
                    "customers",
                    "stores",
                    "sales",
                    "sale_items",
                    "payments",
                    "inventory",
                )
            },
            "reconcile": {"quality"},
        }
        for task_id, expected in checks.items():
            if upstream.get(task_id) != expected:
                problems.append(
                    f"lakehouse_pipeline.{task_id} upstream {upstream.get(task_id)} != {expected}"
                )
    for problem in problems:
        print("FAIL", problem)
    print(f"checked {len(bag.dag_ids)} DAGs: {'OK' if not problems else f'{len(problems)} problem(s)'}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
