"""Platform facade: every pipeline step as one tracked run. Used by the CLI (and therefore Airflow),
the benchmark and the tests, so all of them exercise the same code path."""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import timedelta
from typing import Any

from lakehouse.common.storage import get_store
from lakehouse.config import Settings
from lakehouse.contracts import resolve_entities
from lakehouse.metadata.repository import MetadataRepository, utcnow
from lakehouse.observability.logging import log_event
from lakehouse.pipelines.runtime import tracked_run

log = logging.getLogger(__name__)


class Platform:
    def __init__(
        self,
        settings: Settings,
        repo: MetadataRepository | None = None,
        correlation_id: str | None = None,
        trigger: str = "cli",
        attempt: int = 1,
    ) -> None:
        self.settings = settings
        self.repo = repo or MetadataRepository.from_url(settings.metadata.url)
        self.correlation_id = correlation_id
        self.trigger = trigger
        self.attempt = attempt
        self._spark: Any = None

    # ------------------------------------------------------------------ lifecycle
    @property
    def spark(self) -> Any:
        if self._spark is None:
            from lakehouse.common.spark import build_spark

            self._spark = build_spark(self.settings)
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    def _run(self, pipeline: str, params: dict[str, Any]) -> Any:
        return tracked_run(
            self.settings, self.repo, pipeline, self.correlation_id, self.trigger, params, self.attempt
        )

    def init(self) -> dict[str, Any]:
        self.repo.init_schema()
        for uri in (self.settings.storage.landing_uri, self.settings.storage.lake_uri):
            get_store(uri, self.settings.storage).ensure_container(uri)
        return {"metadata": "ready", "landing_uri": self.settings.storage.landing_uri,
                "lake_uri": self.settings.storage.lake_uri}  # fmt: skip

    # ------------------------------------------------------------------ steps
    def ingest(self, entities: list[str] | None = None) -> dict[str, Any]:
        from lakehouse.ingestion.discovery import ingest

        selected = resolve_entities(entities)
        with self._run("ingest", {"entities": selected}) as run:
            results = ingest(self.settings, self.repo, selected, run.run_id)
            run.summary = {"run_id": run.run_id, "entities": [asdict(r) for r in results]}
            return run.summary

    def bronze(self, entities: list[str] | None = None, batch_ids: list[str] | None = None) -> dict[str, Any]:
        from lakehouse.pipelines.bronze.job import run_bronze

        selected = resolve_entities(entities)
        with self._run("bronze", {"entities": selected, "batch_ids": batch_ids}) as run:
            results = run_bronze(self.spark, run, selected, batch_ids)
            return {"run_id": run.run_id, "batches": [
                {k: v for k, v in asdict(r).items() if k != "quality"} for r in results]}  # fmt: skip

    def silver(self, entities: list[str] | None = None, batch_ids: list[str] | None = None) -> dict[str, Any]:
        from lakehouse.pipelines.silver.job import run_silver

        selected = resolve_entities(entities)
        with self._run("silver", {"entities": selected, "batch_ids": batch_ids}) as run:
            results = run_silver(self.spark, run, selected, batch_ids)
            return {"run_id": run.run_id, "batches": [asdict(r) for r in results]}

    def gold(self) -> dict[str, Any]:
        from lakehouse.pipelines.gold.job import run_gold

        with self._run("gold", {}) as run:
            result = run_gold(self.spark, run)
            return {"run_id": run.run_id, "status": result.status, "publication_id": result.publication_id,
                    "table_versions": result.table_versions, "row_counts": result.row_counts}  # fmt: skip

    def quality(self, suites: tuple[str, ...] = ("silver", "gold")) -> dict[str, Any]:
        from lakehouse.quality.suite import run_quality

        with self._run("quality", {"suites": list(suites)}) as run:
            results = run_quality(self.spark, run, suites)
            return {"run_id": run.run_id, "results": _summarize(results)}

    def reconcile(self, entities: list[str] | None = None) -> dict[str, Any]:
        from lakehouse.quality.suite import run_reconciliation

        selected = resolve_entities(entities)
        with self._run("reconcile", {"entities": selected}) as run:
            results = run_reconciliation(self.spark, run, selected)
            return {"run_id": run.run_id, "results": _summarize(results)}

    def reprocess(
        self,
        entities: list[str],
        batch_ids: list[str] | None = None,
        since: str | None = None,
        until: str | None = None,
        full_refresh: bool = False,
        publish_gold: bool = True,
    ) -> dict[str, Any]:
        """Historical reprocessing: replay Silver for selected Bronze batches (or rebuild it) and,
        by default, republish Gold. Bronze is never rewritten: it is the immutable source of replay."""
        from lakehouse.pipelines.silver.job import reset_for_reprocess, run_silver

        selected = resolve_entities(entities)
        params = {"entities": selected, "batch_ids": batch_ids, "since": since, "until": until,
                  "full_refresh": full_refresh}  # fmt: skip
        with self._run("reprocess", params) as run:
            scheduled: dict[str, list[str]] = {}
            for entity in selected:
                ids = batch_ids
                if not full_refresh and (since or until):
                    ids = [
                        b["batch_id"]
                        for b in self.repo.batches_for_reprocess(entity, batch_ids, since, until)
                    ]
                if not full_refresh and ids is not None and not ids:
                    scheduled[entity] = []
                    continue
                scheduled[entity] = reset_for_reprocess(self.spark, run, entity, ids, full_refresh)
            replay_ids = sorted({b for ids in scheduled.values() for b in ids})
            results = run_silver(self.spark, run, selected, replay_ids or None) if replay_ids else []
            summary: dict[str, Any] = {"run_id": run.run_id, "scheduled": scheduled,
                                       "batches": [asdict(r) for r in results]}  # fmt: skip
        if publish_gold and replay_ids:
            summary["gold"] = self.gold()
        return summary

    def run_all(self, entities: list[str] | None = None) -> dict[str, Any]:
        """Full pipeline in dependency order. Every step is idempotent, so re-running is safe."""
        return {
            "ingest": self.ingest(entities),
            "bronze": self.bronze(entities),
            "silver": self.silver(entities),
            "gold": self.gold(),
            "quality": self.quality(),
            "reconcile": self.reconcile(entities),
        }

    def recover(self) -> dict[str, Any]:
        """Close runs left RUNNING by crashed processes and make their batches retryable.

        Data-level recovery is in the jobs themselves (Bronze looks for an existing commit, Silver is
        idempotent), so this only repairs the operational state.
        """
        older = timedelta(seconds=self.settings.pipeline.stale_run_seconds)
        abandoned = []
        for run in self.repo.stale_runs(older):
            self.repo.mark_run_abandoned(run["run_id"], f"no heartbeat for more than {older}")
            abandoned.append(run["run_id"])
        interrupted = []
        cutoff = utcnow() - older
        for layer in ("bronze", "silver"):
            for row in self.repo.layer_rows(layer, status="RUNNING"):
                if row["started_at"] is None or row["started_at"] < cutoff:
                    self.repo.set_layer_status(row["batch_id"], layer, "FAILED", row["run_id"],
                                               error_message="interrupted (recovered by lakehouse recover)")  # fmt: skip
                    interrupted.append({"batch_id": row["batch_id"], "layer": layer})
        log_event(
            log, "recovery_completed", abandoned_runs=len(abandoned), interrupted_batches=len(interrupted)
        )
        return {"abandoned_runs": abandoned, "interrupted_batches": interrupted}

    def status(self) -> dict[str, Any]:
        return {
            "last_runs": self.repo.last_runs_by_pipeline(),
            "layers": self.repo.layer_totals(),
            "files": self.repo.file_status_counts(),
            "publication": self.repo.latest_publication(),
        }

    def rejects(self, entity: str, reason: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        from pyspark.sql import functions as F

        from lakehouse.common.spark import read_delta, spark_path_for, table_exists

        path = spark_path_for(self.settings.table_uri("silver", "_rejects"))
        if not table_exists(self.spark, path):
            return []
        df = read_delta(self.spark, path).where(F.col("entity") == entity)
        if reason:
            df = df.where(F.array_contains("rejection_reasons", reason))
        return [
            r.asDict(recursive=True) for r in df.orderBy("_batch_seq", "business_key").limit(limit).collect()
        ]


def _summarize(results: list[Any]) -> dict[str, Any]:
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r.status.value] = by_status.get(r.status.value, 0) + 1
    return {
        "by_status": by_status,
        "not_passed": [
            {
                "rule_id": r.rule_id,
                "status": r.status.value,
                "severity": r.severity.value,
                "violations": r.violations,
                "evaluated": r.records_evaluated,
            }
            for r in results
            if r.status.value != "PASSED"
        ],
    }
