import pytest

from lakehouse.quality.framework import (
    Category,
    DatasetRule,
    Outcome,
    QualityGateError,
    RuleResult,
    Severity,
    Status,
    blocking_failures,
    volume_outcome,
)


def _result(
    severity: Severity, evaluated: int, violations: int, threshold: float | None = None
) -> RuleResult:
    return RuleResult("R", "r", "d", "validity", "silver", "t", severity, evaluated, violations, threshold)


@pytest.mark.parametrize(
    ("severity", "violations", "threshold", "status"),
    [
        (Severity.BLOCKING, 0, None, Status.PASSED),
        (Severity.BLOCKING, 1, None, Status.FAILED),
        (Severity.WARNING, 1, None, Status.WARNED),
        (Severity.BLOCKING, 10, 10.0, Status.WARNED),  # 10% within a 10% threshold
        (Severity.BLOCKING, 11, 10.0, Status.FAILED),
        (Severity.WARNING, 50, 10.0, Status.WARNED),
    ],
)
def test_status_matrix(severity: Severity, violations: int, threshold: float | None, status: Status) -> None:
    assert _result(severity, 100, violations, threshold).status is status


def test_compliance_and_record_shape() -> None:
    result = _result(Severity.WARNING, 200, 3)
    assert result.compliance_pct == 98.5
    record = result.to_record("run-1")
    assert record["status"] == "WARNED" and record["compliance_pct"] == 98.5 and record["run_id"] == "run-1"
    assert _result(Severity.WARNING, 0, 0).compliance_pct == 100.0


def test_crashing_check_is_an_error_not_a_pass() -> None:
    def broken(_ctx: object) -> Outcome:
        raise ValueError("table missing")

    rule = DatasetRule("X", "x", "d", Category.CONSISTENCY, "gold", "gold.t", Severity.BLOCKING, broken)
    result = rule.evaluate(None)
    assert result.status is Status.ERROR and result.is_blocking_failure
    assert "table missing" in result.details["error"]  # type: ignore[index]
    with pytest.raises(QualityGateError, match="X"):
        raise QualityGateError(blocking_failures([result]))


def test_warning_rule_never_blocks() -> None:
    rule = DatasetRule(
        "W", "w", "d", Category.VALIDITY, "silver", "s.t", Severity.WARNING, lambda _: Outcome(10, 10)
    )
    assert blocking_failures([rule.evaluate(None)]) == []


@pytest.mark.parametrize(
    ("current", "history", "violations", "evaluated"),
    [
        (100, [100, 110, 90], 0, 1),
        (20, [100, 110, 90], 1, 1),  # sharp drop: possibly a truncated delivery
        (500, [100, 110, 90], 1, 1),  # spike: possibly a re-sent backlog
        (5, [100], 0, 0),  # not enough history: abstain rather than guess
    ],
)
def test_volume_anomaly(current: int, history: list[int], violations: int, evaluated: int) -> None:
    outcome = volume_outcome(current, history, min_history=3, min_ratio=0.5, max_ratio=2.0)
    assert (outcome.violations, outcome.evaluated) == (violations, evaluated)
