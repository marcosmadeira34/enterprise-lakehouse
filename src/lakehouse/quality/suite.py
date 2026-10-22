"""Entry points for the standalone quality and reconciliation steps."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from lakehouse.contracts import ENTITY_ORDER
from lakehouse.observability.logging import log_event
from lakehouse.pipelines.runtime import RunHandle
from lakehouse.quality.framework import (
    DatasetRule,
    QualityGateError,
    RuleResult,
    blocking_failures,
    run_rules,
)
from lakehouse.quality.reconciliation import reconciliation_rules
from lakehouse.quality.rules import QualityContext, gold_rules, silver_rules

log = logging.getLogger(__name__)


def _table_parts(table_name: str) -> tuple[str, str]:
    layer, table = table_name.split(".", 1)
    return layer, table


def _available(context: QualityContext, rules: list[DatasetRule]) -> list[DatasetRule]:
    selected = []
    for rule in rules:
        layer, table = _table_parts(rule.table_name)
        if context.exists(layer, table):
            selected.append(rule)
        else:
            log_event(
                log,
                "quality_rule_skipped",
                logging.WARNING,
                rule_id=rule.rule_id,
                reason=f"{rule.table_name} missing",
            )
    return selected


def run_quality(
    spark: SparkSession, run: RunHandle, suites: tuple[str, ...] = ("silver", "gold")
) -> list[RuleResult]:
    results: list[RuleResult] = []
    if "silver" in suites:
        run.step = "quality:silver"
        context = QualityContext(spark, run.settings)
        results += run_rules(_available(context, silver_rules()), context)
    if "gold" in suites:
        run.step = "quality:gold"
        publication = run.repo.latest_publication()
        if publication is None:
            log_event(log, "quality_gold_skipped", logging.WARNING, reason="no published Gold version")
        else:
            versions = {**publication["source_versions"], **publication["table_versions"]}
            results += run_rules(gold_rules(), QualityContext(spark, run.settings, versions=versions))
    run.repo.save_quality_results([r.to_record(run.run_id) for r in results])
    failures = blocking_failures(results)
    if failures:
        raise QualityGateError(failures)
    return results


def run_reconciliation(
    spark: SparkSession, run: RunHandle, entities: list[str] | None = None
) -> list[RuleResult]:
    """Reconcile layers using the *latest* versions, so drift after publication is also detected."""
    run.step = "reconcile"
    context = QualityContext(spark, run.settings)
    selected = [
        e
        for e in (entities or list(ENTITY_ORDER))
        if context.exists("silver", e) and context.exists("bronze", e)
    ]
    rules = reconciliation_rules(run.repo, selected)
    if context.exists("gold", "fact_sales_item"):
        rules += [r for r in gold_rules() if r.rule_id.startswith("REC-")]
    results = run_rules(rules, context)
    run.repo.save_quality_results([r.to_record(run.run_id) for r in results])
    failures = blocking_failures(results)
    if failures:
        raise QualityGateError(failures)
    return results
