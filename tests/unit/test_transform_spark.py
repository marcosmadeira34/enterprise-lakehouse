"""Silver/Gold transformation logic on small in-memory DataFrames (local Spark, no I/O)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

from lakehouse.config import BusinessRules
from lakehouse.contracts import get_contract
from lakehouse.pipelines.bronze.job import evaluate_schema

pytestmark = pytest.mark.spark


def _one(spark: Any, expr: Any, value: str | None) -> Any:
    from pyspark.sql import functions as F

    return spark.createDataFrame([(value,)], "v string").select(expr(F.col("v")).alias("out")).first()["out"]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("12.50", Decimal("12.50")),
        (" R$ 7.1 ", Decimal("7.10")),
        ("-3.00", Decimal("-3.00")),
        ("1.005", Decimal("1.01")),  # HALF_UP to cents
        ("1,234.00", None),  # thousands separator is ambiguous across locales: invalid
        ("abc", None),
        ("", None),
    ],
)
def test_parse_money(spark: Any, raw: str, expected: Decimal | None) -> None:
    from lakehouse.pipelines.silver.transform import parse_money

    assert _one(spark, parse_money, raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-07-01T10:15:00", datetime(2026, 7, 1, 10, 15)),
        ("2026-07-01 10:15:00.500000", datetime(2026, 7, 1, 10, 15, 0, 500000)),
        ("2026-07-01T10:15:00Z", datetime(2026, 7, 1, 10, 15)),
        ("2026-07-01T10:15:00-03:00", datetime(2026, 7, 1, 13, 15)),  # normalized to UTC
        ("01/07/2026 10:15:00", datetime(2026, 7, 1, 10, 15)),  # legacy POS layout (dd/MM/yyyy)
        ("2026-02-30T10:00:00", None),  # impossible date
        ("not-a-date", None),
    ],
)
def test_parse_timestamp(spark: Any, raw: str, expected: datetime | None) -> None:
    from lakehouse.pipelines.silver.transform import parse_timestamp

    assert _one(spark, parse_timestamp, raw) == expected


def test_parse_scalars(spark: Any) -> None:
    from lakehouse.pipelines.silver.transform import normalize_id, parse_bool, parse_date, parse_int

    assert _one(spark, parse_int, "3.7") is None  # a cast would silently truncate to 3
    assert _one(spark, parse_int, " 42 ") == 42
    assert _one(spark, parse_date, "2024-13-45") is None
    assert _one(spark, parse_date, "31/01/2024") == date(2024, 1, 31)
    assert _one(spark, parse_bool, "Sim") is True and _one(spark, parse_bool, "talvez") is None
    assert _one(spark, normalize_id, "  ab12 ") == "AB12"
    assert _one(spark, normalize_id, "   ") is None


def _bronze_sales(spark: Any, rows: list[dict[str, Any]]) -> Any:
    from pyspark.sql import functions as F

    from lakehouse.pipelines.bronze.job import bronze_schema

    contract = get_contract("sales")
    schema = bronze_schema(contract)
    base = {"currency": "BRL", "status": "COMPLETED", "transaction_type": "SALE", "store_id": "S001",
            "updated_at": "2026-07-01T12:00:00", "sale_ts": "2026-07-01T10:00:00", "total_amount": "10.00"}  # fmt: skip
    data = []
    for i, row in enumerate(rows):
        values = {**base, **row}
        data.append({**{f.name: values.get(f.name) for f in schema.fields},
                     "_batch_id": values.get("_batch_id", "b1"), "_batch_seq": values.get("_batch_seq", 1),
                     "_source_file": "f.csv", "_record_hash": f"h{i}"})  # fmt: skip
    df = spark.createDataFrame(data, schema)
    return df.withColumn("_ingestion_date", F.lit("2026-07-01").cast("date"))


def _reasons(df: Any) -> dict[str, list[str]]:
    return {
        r["sale_id"] or r["_business_key"]: r["_reasons"]
        for r in df.select("sale_id", "_business_key", "_reasons").collect()
    }


def test_sales_validation_reasons(spark: Any) -> None:
    from lakehouse.pipelines.silver.transform import conform

    rows = [
        {"sale_id": "T1"},
        {"sale_id": "T2", "total_amount": "-5.00"},  # negative SALE: invalid
        {
            "sale_id": "T3",
            "transaction_type": "RETURN",
            "total_amount": "-5.00",
            "original_sale_id": "T1",
        },  # valid
        {"sale_id": "T4", "transaction_type": "RETURN", "total_amount": "-5.00"},  # return without original
        {"sale_id": "T5", "store_id": " "},  # blank = missing
        {"sale_id": "T6", "sale_ts": "2099-01-01T00:00:00"},
        {"sale_id": "T7", "status": "PENDING"},
        {"sale_id": "T8", "store_id": "S999"},  # orphan store
        {"sale_id": "T9", "currency": "USD"},
    ]
    stores = spark.createDataFrame([("S001",)], "store_id string")
    out = _reasons(
        conform(_bronze_sales(spark, rows), get_contract("sales"), BusinessRules(), {"stores": stores})
    )
    assert out["T1"] == [] and out["T3"] == []
    assert out["T2"] == ["NEGATIVE_AMOUNT_FOR_SALE"]
    assert out["T4"] == ["RETURN_WITHOUT_ORIGINAL"]
    assert out["T5"] == ["MISSING_REQUIRED:store_id"]
    assert out["T6"] == ["FUTURE_DATE:sale_ts"]
    assert out["T7"] == ["INVALID_DOMAIN:status"]
    assert out["T8"] == ["ORPHAN_STORE"]
    assert out["T9"] == ["INVALID_CURRENCY"]


def test_dedup_keeps_latest_version_and_breaks_ties_by_batch(spark: Any) -> None:
    from lakehouse.pipelines.silver.transform import accepted_rows, conform

    rows = [
        {"sale_id": "T1", "updated_at": "2026-07-01T12:00:00", "total_amount": "1.00", "_batch_seq": 1},
        {"sale_id": "T1", "updated_at": "2026-07-02T12:00:00", "total_amount": "2.00", "_batch_seq": 1},
        {
            "sale_id": "T2",
            "updated_at": "2026-07-01T12:00:00",
            "total_amount": "3.00",
            "_batch_seq": 1,
            "_batch_id": "b1",
        },
        {
            "sale_id": "T2",
            "updated_at": "2026-07-01T12:00:00",
            "total_amount": "4.00",
            "_batch_seq": 2,
            "_batch_id": "b2",
        },
    ]
    conformed = conform(_bronze_sales(spark, rows), get_contract("sales"), BusinessRules(), {"stores": None})
    # No stores table yet: every non-null store is an orphan, so check dedup on reasons instead.
    reasons = sorted(tuple(r["_reasons"]) for r in conformed.collect())
    assert reasons.count(("ORPHAN_STORE",)) == 4
    stores = spark.createDataFrame([("S001",)], "store_id string")
    conformed = conform(
        _bronze_sales(spark, rows), get_contract("sales"), BusinessRules(), {"stores": stores}
    )
    accepted = {
        r["sale_id"]: r["total_amount"]
        for r in accepted_rows(conformed, get_contract("sales"), "run").collect()
    }
    assert accepted == {"T1": Decimal("2.00"), "T2": Decimal("4.00")}
    dupes = [r for r in conformed.collect() if r["_reasons"] == ["DUPLICATE_BUSINESS_KEY"]]
    assert len(dupes) == 2


def test_negative_stock_policy_is_configurable(spark: Any) -> None:
    from lakehouse.pipelines.silver.transform import domain_rules

    assert domain_rules("inventory", BusinessRules(allow_negative_stock=True)) == []
    assert [r.reason for r, _ in domain_rules("inventory", BusinessRules(allow_negative_stock=False))] == [
        "NEGATIVE_STOCK"
    ]


def test_schema_policy() -> None:
    contract = get_contract("customers")
    decision = evaluate_schema(
        contract, ["Customer_ID", "email", "signup_date", "updated_at", "loyalty_tier", "_corrupt_record"]
    )
    assert decision.compatible
    assert decision.extra == ("loyalty_tier",)
    assert "phone" in decision.missing_optional
    broken = evaluate_schema(contract, ["email", "signup_date", "updated_at"])
    assert not broken.compatible and broken.missing_required == ("customer_id",)


def test_gold_daily_aggregate(spark: Any) -> None:
    from lakehouse.pipelines.gold.model import agg_daily_sales

    schema = ("sale_id string, sale_date date, date_key int, store_id string, transaction_type string, quantity int, "
              "gross_amount decimal(18,2), discount_amount decimal(18,2), net_amount decimal(18,2), "
              "cost_amount decimal(18,2)")  # fmt: skip
    d = date(2026, 7, 1)
    fact = spark.createDataFrame(
        [
            (
                "T1",
                d,
                20260701,
                "S1",
                "SALE",
                2,
                Decimal("20.00"),
                Decimal("2.00"),
                Decimal("18.00"),
                Decimal("10.00"),
            ),
            (
                "T1",
                d,
                20260701,
                "S1",
                "SALE",
                1,
                Decimal("5.00"),
                Decimal("0.00"),
                Decimal("5.00"),
                Decimal("3.00"),
            ),
            (
                "T2",
                d,
                20260701,
                "S1",
                "SALE",
                1,
                Decimal("7.00"),
                Decimal("0.00"),
                Decimal("7.00"),
                Decimal("4.00"),
            ),
            (
                "R1",
                d,
                20260701,
                "S1",
                "RETURN",
                -1,
                Decimal("-5.00"),
                Decimal("0.00"),
                Decimal("-5.00"),
                Decimal("-3.00"),
            ),
        ],
        schema,
    )
    row = agg_daily_sales(fact).first().asDict()
    assert row["orders"] == 2 and row["returns"] == 1 and row["items_sold"] == 4
    assert row["gross_revenue"] == Decimal("32.00") and row["discount_total"] == Decimal("2.00")
    assert row["returns_amount"] == Decimal("-5.00") and row["net_revenue"] == Decimal("25.00")
    assert row["avg_ticket"] == Decimal("15.00")  # (18 + 5 + 7) / 2 orders; returns excluded
    assert row["gross_margin"] == Decimal("11.00")  # 25 - (10 + 3 + 4 - 3)
