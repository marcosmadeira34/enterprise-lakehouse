"""API tests against a real SQLite metadata store and a real Delta table written with delta-rs."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from lakehouse.api.app import create_app
from lakehouse.config import Settings
from lakehouse.metadata.repository import MetadataRepository
from lakehouse.quality.framework import RuleResult, Severity

deltalake = pytest.importorskip("deltalake")

MONEY = pa.decimal128(18, 2)
SCHEMA = pa.schema(
    [("sale_date", pa.date32()), ("date_key", pa.int32()), ("store_id", pa.string()), ("orders", pa.int64()),
     ("returns", pa.int64()), ("items_sold", pa.int64()), ("gross_revenue", MONEY), ("discount_total", MONEY),
     ("returns_amount", MONEY), ("net_revenue", MONEY), ("avg_ticket", MONEY), ("gross_margin", MONEY)]
)  # fmt: skip


def _row(day: int, store: str, orders: int, net: str, returns: str = "0.00") -> dict[str, Any]:
    d = Decimal
    return {"sale_date": date(2026, 7, day), "date_key": 20260700 + day, "store_id": store, "orders": orders,
            "returns": 1 if d(returns) else 0, "items_sold": orders * 2, "gross_revenue": d(net) - d(returns),
            "discount_total": d("0.00"), "returns_amount": d(returns), "net_revenue": d(net),
            "avg_ticket": None, "gross_margin": d(net) / 2}  # fmt: skip


@pytest.fixture
def client(settings: Settings, repo: MetadataRepository) -> TestClient:
    path = settings.table_uri("gold", "agg_daily_sales")
    rows = [_row(1, "S001", 2, "100.00"), _row(1, "S002", 1, "50.00"), _row(2, "S001", 4, "180.00", "-20.00")]
    deltalake.write_deltalake(path, pa.Table.from_pylist(rows, SCHEMA))  # version 0: published
    deltalake.write_deltalake(
        path, pa.Table.from_pylist([_row(3, "S009", 9, "999.00")], SCHEMA), mode="append"
    )
    repo.record_publication("gold-run", "PUBLISHED", {"gold.agg_daily_sales": 0}, {"silver.sales": 3},
                            {"gold.agg_daily_sales": 3}, [])  # fmt: skip
    repo.record_publication(
        "gold-run-2", "BLOCKED", {"gold.agg_daily_sales": 1}, {}, {}, ["REC-GOLD-FACT-AMOUNT"]
    )

    for i in range(3):
        run_id = repo.start_run("silver", correlation_id="corr")
        repo.finish_run(
            run_id, "SUCCEEDED" if i else "FAILED", records_read=10 * i, error=None if i else ValueError("x")
        )
    result = RuleResult(
        "SLV-X", "x", "d", "validity", "silver", "silver.sales", Severity.WARNING, 10, 1, batch_id="b1"
    )
    repo.save_quality_results([result.to_record("q-run")])
    return TestClient(create_app(settings, repo))


def test_health(client: TestClient) -> None:
    body = client.get("/health").json()
    assert body == {"status": "ok", "checks": {"metadata": "ok", "gold_publication": 1}}


def test_runs_pagination_filters_and_detail(client: TestClient) -> None:
    page = client.get("/runs", params={"limit": 2, "offset": 0}).json()
    assert page["total"] == 3 and len(page["items"]) == 2 and page["limit"] == 2
    assert client.get("/runs", params={"limit": 2, "offset": 2}).json()["items"][0]
    failed = client.get("/runs", params={"status": "FAILED"}).json()
    assert failed["total"] == 1 and failed["items"][0]["error_type"] == "ValueError"
    run_id = failed["items"][0]["run_id"]
    detail = client.get(f"/runs/{run_id}").json()
    assert detail["run_id"] == run_id and detail["errors"] == [] and "batch_layers" in detail
    assert client.get("/runs/missing").status_code == 404


def test_parameter_validation(client: TestClient) -> None:
    assert client.get("/runs", params={"limit": 0}).status_code == 422
    assert client.get("/runs", params={"limit": 201}).status_code == 422
    assert client.get("/runs", params={"status": "BOGUS"}).status_code == 422
    bad_range = client.get(
        "/analytics/sales-summary", params={"start_date": "2026-07-03", "end_date": "2026-07-01"}
    )
    assert bad_range.status_code == 422
    assert client.get("/analytics/sales-summary", params={"group_by": "hour"}).status_code == 422


def test_quality_results(client: TestClient) -> None:
    page = client.get("/quality/results", params={"severity": "WARNING", "batch_id": "b1"}).json()
    assert page["total"] == 1 and page["items"][0]["status"] == "WARNED"
    assert client.get("/quality/results", params={"status": "FAILED"}).json()["total"] == 0


def test_sales_summary_serves_the_published_version_only(client: TestClient) -> None:
    body = client.get("/analytics/sales-summary", params={"group_by": "total"}).json()
    assert body["source"]["delta_version"] == 0 and body["source"]["publication_id"] == 1
    total = body["items"][0]
    # Version 1 (S009, not published) is invisible.
    assert total["net_revenue"] == "330.00" and total["orders"] == 7
    assert total["avg_ticket"] == "50.00"  # (330 - (-20)) / 7: returns do not dilute the ticket

    by_store = client.get("/analytics/sales-summary", params={"group_by": "store"}).json()["items"]
    assert [(r["store_id"], r["net_revenue"]) for r in by_store] == [("S001", "280.00"), ("S002", "50.00")]

    day = client.get("/analytics/sales-summary",
                     params={"start_date": "2026-07-02", "store_id": "s001", "group_by": "day"}).json()  # fmt: skip
    assert day["total"] == 1 and day["items"][0]["returns_amount"] == "-20.00"
    page = client.get(
        "/analytics/sales-summary", params={"group_by": "day_store", "limit": 1, "offset": 1}
    ).json()
    assert page["total"] == 3 and len(page["items"]) == 1


def test_sales_summary_without_publication(settings: Settings, repo: MetadataRepository) -> None:
    client = TestClient(create_app(settings, repo))
    assert client.get("/analytics/sales-summary").status_code == 503
    assert client.get("/health").json()["status"] == "degraded"


def test_metrics_exposition(client: TestClient) -> None:
    text = client.get("/metrics").text
    assert 'lakehouse_runs_total{pipeline="silver",status="SUCCEEDED"} 2.0' in text
    assert (
        'lakehouse_quality_rule_status{rule_id="SLV-X",severity="WARNING",table="silver.sales"} 1.0' in text
    )
    assert "lakehouse_gold_publication_age_seconds" in text
    assert "lakehouse_last_run_records_per_second" in text
