"""Prometheus exposition derived from the metadata repository.

Batch jobs are short-lived, so instead of every job pushing metrics, the API exposes ``/metrics``
computed from the operational metadata on each scrape: the database is the single source of truth and
nothing is lost when a job dies before reporting.
"""

from __future__ import annotations

from collections.abc import Iterator

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.exposition import CONTENT_TYPE_LATEST

from lakehouse.metadata.repository import MetadataRepository, utcnow

STATUS_CODE = {"PASSED": 0, "WARNED": 1, "FAILED": 2, "ERROR": 3}


class MetadataCollector:
    def __init__(self, repo: MetadataRepository) -> None:
        self.repo = repo

    def collect(self) -> Iterator[Metric]:
        runs = CounterMetricFamily(
            "lakehouse_runs", "Pipeline runs by final status.", labels=["pipeline", "status"]
        )
        for row in self.repo.run_status_counts():
            runs.add_metric([row["pipeline"], row["status"]], row["runs"])
        yield runs

        duration = GaugeMetricFamily(
            "lakehouse_last_run_duration_seconds", "Duration of the last run.", labels=["pipeline"]
        )
        finished = GaugeMetricFamily("lakehouse_last_run_finished_timestamp_seconds", "End time of the last run.",
                                     labels=["pipeline"])  # fmt: skip
        failed = GaugeMetricFamily("lakehouse_last_run_failed", "1 if the last run of the pipeline failed.",
                                   labels=["pipeline"])  # fmt: skip
        throughput = GaugeMetricFamily("lakehouse_last_run_records_per_second", "Records read per second, last run.",
                                       labels=["pipeline"])  # fmt: skip
        for run in self.repo.last_runs_by_pipeline():
            labels = [run["pipeline"]]
            failed.add_metric(labels, 1.0 if run["status"] in {"FAILED", "ABANDONED"} else 0.0)
            if run["duration_seconds"] is not None:
                duration.add_metric(labels, float(run["duration_seconds"]))
                if run["records_read"] and run["duration_seconds"] > 0:
                    throughput.add_metric(labels, run["records_read"] / run["duration_seconds"])
            if run["finished_at"] is not None:
                finished.add_metric(labels, run["finished_at"].timestamp())
        yield from (duration, finished, failed, throughput)

        records = CounterMetricFamily("lakehouse_layer_records", "Records per layer and entity.",
                                      labels=["layer", "entity", "kind"])  # fmt: skip
        batches = GaugeMetricFamily("lakehouse_layer_batches", "Batches per layer, entity and status.",
                                    labels=["layer", "entity", "status"])  # fmt: skip
        totals: dict[tuple[str, str, str], float] = {}
        for row in self.repo.layer_totals():
            batches.add_metric([row["layer"], row["entity"], row["status"]], row["batches"])
            for kind in ("read", "accepted", "rejected"):
                key = (row["layer"], row["entity"], kind)
                totals[key] = totals.get(key, 0) + float(row[f"records_{kind}"])
        for key, value in totals.items():
            records.add_metric(list(key), value)
        yield from (records, batches)

        files = GaugeMetricFamily(
            "lakehouse_files", "Landing files by entity and status.", labels=["entity", "status"]
        )
        for row in self.repo.file_status_counts():
            files.add_metric([row["entity"], row["status"]], row["files"])
        yield files

        status = GaugeMetricFamily("lakehouse_quality_rule_status", "Last result: 0 passed, 1 warned, 2 failed, 3 error.",
                                   labels=["rule_id", "severity", "table"])  # fmt: skip
        violations = GaugeMetricFamily("lakehouse_quality_rule_violations", "Violations in the last evaluation.",
                                       labels=["rule_id", "severity", "table"])  # fmt: skip
        compliance = GaugeMetricFamily("lakehouse_quality_rule_compliance_pct", "Compliance % in the last evaluation.",
                                       labels=["rule_id", "severity", "table"])  # fmt: skip
        for rule in self.repo.latest_quality_status():
            labels = [rule["rule_id"], rule["severity"], rule["table_name"]]
            status.add_metric(labels, STATUS_CODE[rule["status"]])
            violations.add_metric(labels, float(rule["violations"]))
            compliance.add_metric(labels, float(rule["compliance_pct"]))
        yield from (status, violations, compliance)

        publication = self.repo.latest_publication()
        age = GaugeMetricFamily(
            "lakehouse_gold_publication_age_seconds", "Age of the published Gold version."
        )
        if publication is not None:
            age.add_metric([], (utcnow() - publication["created_at"]).total_seconds())
        yield age


def render_metrics(repo: MetadataRepository) -> tuple[bytes, str]:
    registry = CollectorRegistry(auto_describe=False)
    registry.register(MetadataCollector(repo))
    return generate_latest(registry), CONTENT_TYPE_LATEST
