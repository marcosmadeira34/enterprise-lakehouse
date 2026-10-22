"""Reusable data quality framework.

Two kinds of rules share one result model:

* **Row rules** decide, record by record, whether a record is rejected in Silver (see
  :mod:`lakehouse.pipelines.silver.transform`). Their per-batch counts are reported as results.
* **Dataset rules** evaluate a whole table or the relation between tables (uniqueness, consistency,
  reconciliation, volume). A BLOCKING failure stops the pipeline step; a WARNING is recorded only.

Status logic (independent of Spark so it can be unit tested):

* no violations -> PASSED
* violations within ``threshold_pct`` -> WARNED
* violations above the threshold (or any violation without threshold) -> FAILED if BLOCKING, else WARNED
* the check itself crashed -> ERROR (treated as a failure for BLOCKING rules)
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

from lakehouse.observability.logging import log_event

log = logging.getLogger(__name__)


class Severity(StrEnum):
    BLOCKING = "BLOCKING"
    WARNING = "WARNING"


class Status(StrEnum):
    PASSED = "PASSED"
    WARNED = "WARNED"
    FAILED = "FAILED"
    ERROR = "ERROR"


class Category(StrEnum):
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    INTEGRITY = "referential_integrity"
    CONSISTENCY = "consistency"
    RECONCILIATION = "reconciliation"
    VOLUME = "volume"


@dataclass(frozen=True)
class Outcome:
    """What a check measured."""

    evaluated: int
    violations: int
    observed: Decimal | float | int | None = None
    expected: Decimal | float | int | None = None
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class RuleResult:
    rule_id: str
    rule_name: str
    description: str
    category: str
    layer: str
    table_name: str
    severity: Severity
    records_evaluated: int
    violations: int
    threshold_pct: float | None = None
    observed_value: Decimal | float | int | None = None
    expected_value: Decimal | float | int | None = None
    details: dict[str, Any] | None = None
    batch_id: str | None = None
    errored: bool = False

    @property
    def violation_pct(self) -> float:
        return 0.0 if self.records_evaluated == 0 else 100.0 * self.violations / self.records_evaluated

    @property
    def compliance_pct(self) -> float:
        return round(100.0 - self.violation_pct, 4)

    @property
    def status(self) -> Status:
        if self.errored:
            return Status.ERROR
        if self.violations == 0:
            return Status.PASSED
        if self.threshold_pct is not None and self.violation_pct <= self.threshold_pct:
            return Status.WARNED
        return Status.FAILED if self.severity is Severity.BLOCKING else Status.WARNED

    @property
    def is_blocking_failure(self) -> bool:
        return self.severity is Severity.BLOCKING and self.status in {Status.FAILED, Status.ERROR}

    def to_record(self, run_id: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "batch_id": self.batch_id,
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "description": self.description,
            "category": self.category,
            "layer": self.layer,
            "table_name": self.table_name,
            "severity": self.severity.value,
            "records_evaluated": int(self.records_evaluated),
            "violations": int(self.violations),
            "compliance_pct": self.compliance_pct,
            "threshold_pct": self.threshold_pct,
            "status": self.status.value,
            "observed_value": _num(self.observed_value),
            "expected_value": _num(self.expected_value),
            "details": self.details,
        }


def _num(value: Decimal | float | int | None) -> Decimal | None:
    return None if value is None else Decimal(str(value))


@dataclass(frozen=True)
class DatasetRule:
    rule_id: str
    name: str
    description: str
    category: Category
    layer: str
    table_name: str
    severity: Severity
    check: Callable[[Any], Outcome] = field(compare=False)
    threshold_pct: float | None = None

    def evaluate(self, context: Any, batch_id: str | None = None) -> RuleResult:
        try:
            outcome = self.check(context)
            errored = False
        except Exception as exc:  # a crashing check must surface, never pass silently
            log_event(log, "quality_rule_error", logging.ERROR, rule_id=self.rule_id, error=str(exc))
            outcome = Outcome(0, 0, details={"error": f"{type(exc).__name__}: {exc}",
                                             "traceback": traceback.format_exc()[-2000:]})  # fmt: skip
            errored = True
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.name,
            description=self.description,
            category=self.category.value,
            layer=self.layer,
            table_name=self.table_name,
            severity=self.severity,
            records_evaluated=outcome.evaluated,
            violations=outcome.violations,
            threshold_pct=self.threshold_pct,
            observed_value=outcome.observed,
            expected_value=outcome.expected,
            details=outcome.details,
            batch_id=batch_id,
            errored=errored,
        )


class QualityGateError(RuntimeError):
    """Raised when at least one BLOCKING rule failed."""

    def __init__(self, failures: Sequence[RuleResult]) -> None:
        self.failures = list(failures)
        ids = ", ".join(f"{r.rule_id}({r.violations}/{r.records_evaluated})" for r in self.failures)
        super().__init__(f"blocking quality rules failed: {ids}")


def run_rules(rules: Sequence[DatasetRule], context: Any, batch_id: str | None = None) -> list[RuleResult]:
    results = []
    for rule in rules:
        result = rule.evaluate(context, batch_id)
        level = logging.WARNING if result.status is not Status.PASSED else logging.INFO
        log_event(
            log,
            "quality_rule_evaluated",
            level,
            rule_id=result.rule_id,
            status=result.status.value,
            severity=result.severity.value,
            evaluated=result.records_evaluated,
            violations=result.violations,
        )
        results.append(result)
    return results


def blocking_failures(results: Sequence[RuleResult]) -> list[RuleResult]:
    return [r for r in results if r.is_blocking_failure]


def volume_outcome(
    current: int, history: Sequence[int], min_history: int, min_ratio: float, max_ratio: float
) -> Outcome:
    """Compare a batch volume with the mean of previous batches of the same entity.

    With too little history the check abstains (0 evaluated) instead of guessing.
    """
    if len(history) < min_history:
        return Outcome(
            0, 0, observed=current, details={"reason": "insufficient_history", "history": list(history)}
        )
    mean = sum(history) / len(history)
    ratio = current / mean if mean else float("inf")
    anomalous = not (min_ratio <= ratio <= max_ratio)
    return Outcome(
        evaluated=1,
        violations=int(anomalous),
        observed=current,
        expected=round(mean, 2),
        details={
            "ratio": round(ratio, 4),
            "min_ratio": min_ratio,
            "max_ratio": max_ratio,
            "history": list(history),
        },
    )
