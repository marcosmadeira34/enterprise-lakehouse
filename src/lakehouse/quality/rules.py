"""Dataset rule catalog for Silver and Gold, evaluated with Spark aggregations (no driver-side scans)."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.common.spark import read_delta, spark_path_for, table_exists
from lakehouse.config import Settings
from lakehouse.contracts import ENTITIES, EntityContract
from lakehouse.quality.framework import Category, DatasetRule, Outcome, Severity

B, W = Severity.BLOCKING, Severity.WARNING


@dataclass
class QualityContext:
    spark: SparkSession
    settings: Settings
    # "layer.table" -> Delta version to read (pinned snapshot); absent means latest.
    versions: dict[str, int] = field(default_factory=dict)

    def path(self, layer: str, table: str) -> str:
        return spark_path_for(self.settings.table_uri(layer, table))

    def exists(self, layer: str, table: str) -> bool:
        return table_exists(self.spark, self.path(layer, table))

    def table(self, layer: str, table: str) -> DataFrame:
        return read_delta(self.spark, self.path(layer, table), self.versions.get(f"{layer}.{table}"))

    @property
    def tolerance(self) -> Decimal:
        return self.settings.business_rules.money_tolerance


def _count(df: DataFrame) -> int:
    return int(df.count())


def unique_key(layer: str, table: str, keys: tuple[str, ...]) -> DatasetRule:
    def check(ctx: QualityContext) -> Outcome:
        df = ctx.table(layer, table)
        row = (
            df.groupBy(*keys)
            .count()
            .agg(
                F.coalesce(F.sum("count"), F.lit(0)).alias("rows"),
                F.coalesce(F.sum(F.when(F.col("count") > 1, F.col("count") - 1)), F.lit(0)).alias("excess"),
            )
            .first()
        )
        assert row is not None
        return Outcome(int(row["rows"]), int(row["excess"]))

    return DatasetRule(
        rule_id=f"{_prefix(layer)}-{table.upper().replace('_', '-')}-UNIQUE-KEY",
        name=f"{table} unique {'/'.join(keys)}",
        description=f"Each {list(keys)} appears once in {layer}.{table}; violations count the extra rows.",
        category=Category.UNIQUENESS,
        layer=layer,
        table_name=f"{layer}.{table}",
        severity=B,
        check=check,
    )


def not_null(layer: str, table: str, columns: tuple[str, ...]) -> DatasetRule:
    def check(ctx: QualityContext) -> Outcome:
        df = ctx.table(layer, table)
        any_null = F.lit(False)
        for c in columns:
            any_null = any_null | F.col(c).isNull()
        row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(any_null.cast("long")).alias("v")).first()
        assert row is not None
        return Outcome(int(row["n"]), int(row["v"] or 0))

    return DatasetRule(
        rule_id=f"{_prefix(layer)}-{table.upper().replace('_', '-')}-NOT-NULL",
        name=f"{table} mandatory columns",
        description=f"Columns {list(columns)} are never NULL in {layer}.{table}.",
        category=Category.COMPLETENESS,
        layer=layer,
        table_name=f"{layer}.{table}",
        severity=B,
        check=check,
    )


def _prefix(layer: str) -> str:
    return {"silver": "SLV", "gold": "GLD", "bronze": "BRZ"}[layer]


def _silver_entity_rules(contract: EntityContract) -> list[DatasetRule]:
    return [
        unique_key("silver", contract.name, contract.business_key),
        not_null("silver", contract.name, contract.required_columns),
    ]


# ---------------------------------------------------------------------- silver cross-record rules
def _sales_ri_customer(ctx: QualityContext) -> Outcome:
    sales = ctx.table("silver", "sales").where(F.col("customer_id").isNotNull())
    customers = ctx.table("silver", "customers").select("customer_id")
    total = _count(ctx.table("silver", "sales"))
    return Outcome(total, _count(sales.join(customers, "customer_id", "left_anti")))


def _sales_total_mismatch(ctx: QualityContext) -> Outcome:
    sales = (
        ctx.table("silver", "sales").where(F.col("status") == "COMPLETED").select("sale_id", "total_amount")
    )
    lines = ctx.table("silver", "sale_items").groupBy("sale_id").agg(F.sum("line_total").alias("lines_total"))
    joined = sales.join(lines, "sale_id", "inner")
    diff = F.abs(F.col("total_amount") - F.col("lines_total")) > F.lit(ctx.tolerance)
    row = joined.agg(F.count(F.lit(1)).alias("n"), F.sum(diff.cast("long")).alias("v")).first()
    assert row is not None
    return Outcome(int(row["n"]), int(row["v"] or 0))


def _payments_total_mismatch(ctx: QualityContext) -> Outcome:
    sales = (
        ctx.table("silver", "sales").where(F.col("status") == "COMPLETED").select("sale_id", "total_amount")
    )
    paid = (
        ctx.table("silver", "payments")
        .where(F.col("status").isin("APPROVED", "REFUNDED"))
        .groupBy("sale_id")
        .agg(F.sum("amount").alias("paid"))
    )
    joined = sales.join(paid, "sale_id", "left")
    diff = F.abs(F.col("total_amount") - F.coalesce("paid", F.lit(0))) > F.lit(ctx.tolerance)
    row = joined.agg(F.count(F.lit(1)).alias("n"), F.sum(diff.cast("long")).alias("v")).first()
    assert row is not None
    return Outcome(int(row["n"]), int(row["v"] or 0))


def _line_total(ctx: QualityContext) -> Outcome:
    items = ctx.table("silver", "sale_items")
    expected = F.col("quantity") * F.col("unit_price") - F.signum("quantity") * F.coalesce(
        "discount_amount", F.lit(0)
    )
    diff = F.abs(F.col("line_total") - expected) > F.lit(ctx.tolerance)
    row = items.agg(F.count(F.lit(1)).alias("n"), F.sum(diff.cast("long")).alias("v")).first()
    assert row is not None
    return Outcome(int(row["n"]), int(row["v"] or 0))


def _dup_email(ctx: QualityContext) -> Outcome:
    customers = ctx.table("silver", "customers")
    shared = (
        customers.where(F.col("email").isNotNull())
        .groupBy("email")
        .agg(F.countDistinct("customer_id").alias("ids"))
        .where(F.col("ids") > 1)
    )
    row = shared.agg(F.coalesce(F.sum("ids"), F.lit(0)).alias("v")).first()
    assert row is not None
    return Outcome(_count(customers), int(row["v"]))


def _where_count(table: str, condition: Any) -> Any:
    def check(ctx: QualityContext) -> Outcome:
        df = ctx.table("silver", table)
        row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(condition().cast("long")).alias("v")).first()
        assert row is not None
        return Outcome(int(row["n"]), int(row["v"] or 0))

    return check


def _return_original(ctx: QualityContext) -> Outcome:
    sales = ctx.table("silver", "sales")
    returns = sales.where(F.col("transaction_type") == "RETURN")
    originals = sales.select(F.col("sale_id").alias("original_sale_id"))
    return Outcome(_count(returns), _count(returns.join(originals, "original_sale_id", "left_anti")))


def silver_rules() -> list[DatasetRule]:
    rules = [r for contract in ENTITIES.values() for r in _silver_entity_rules(contract)]

    def rule(
        rule_id: str, name: str, description: str, category: Category, table: str, check: Any
    ) -> DatasetRule:
        return DatasetRule(rule_id, name, description, category, "silver", f"silver.{table}", W, check)

    rules += [
        rule("SLV-SALES-RI-CUSTOMER", "sales with unregistered customer",
             "Sales whose customer_id is not in silver.customers (accepted by policy, reported).",
             Category.INTEGRITY, "sales", _sales_ri_customer),
        rule("SLV-SALES-TOTAL-MISMATCH", "sale total vs lines",
             "Completed sales whose total_amount differs from the sum of line_total by more than the tolerance.",
             Category.CONSISTENCY, "sales", _sales_total_mismatch),
        rule("SLV-PAYMENTS-TOTAL-MISMATCH", "payments vs sale total",
             "Completed sales whose APPROVED/REFUNDED payments do not add up to total_amount.",
             Category.CONSISTENCY, "payments", _payments_total_mismatch),
        rule("SLV-SALE-ITEMS-LINE-TOTAL", "line total arithmetic",
             "line_total must equal quantity * unit_price - sign(quantity) * discount_amount.",
             Category.CONSISTENCY, "sale_items", _line_total),
        rule("SLV-CUSTOMERS-DUP-EMAIL", "customers sharing an e-mail",
             "Distinct customer ids sharing one e-mail: probable duplicate registrations.",
             Category.UNIQUENESS, "customers", _dup_email),
        rule("SLV-INVENTORY-NEG-STOCK", "negative stock (backorder)",
             "Snapshots with negative quantity_on_hand; allowed as backorders, monitored.",
             Category.VALIDITY, "inventory", _where_count("inventory", lambda: F.col("quantity_on_hand") < 0)),
        rule("SLV-PRODUCTS-NEG-MARGIN", "products priced below cost",
             "Products whose unit_cost exceeds unit_price (clearance or pricing error).",
             Category.VALIDITY, "products", _where_count("products", lambda: F.col("unit_cost") > F.col("unit_price"))),
        rule("SLV-SALES-RETURN-ORIGINAL", "returns without original sale",
             "RETURN transactions whose original_sale_id is not in silver.sales.",
             Category.INTEGRITY, "sales", _return_original),
    ]  # fmt: skip
    return rules


# ---------------------------------------------------------------------- gold rules (publication gate)
def _silver_fact_basis(ctx: QualityContext) -> DataFrame:
    sales = (
        ctx.table("silver", "sales")
        .where(F.col("status") == "COMPLETED")
        .select("sale_id", "transaction_type")
    )
    return ctx.table("silver", "sale_items").join(sales, "sale_id", "inner")


def _fact_vs_silver_count(ctx: QualityContext) -> Outcome:
    expected = _count(_silver_fact_basis(ctx))
    observed = _count(ctx.table("gold", "fact_sales_item"))
    return Outcome(1, int(observed != expected), observed, expected)


def _sum(df: DataFrame, column: str) -> Decimal:
    row = df.agg(F.sum(column).alias("s")).first()
    return Decimal(str(row["s"])) if row and row["s"] is not None else Decimal("0")


def _fact_vs_silver_amount(ctx: QualityContext) -> Outcome:
    expected = _sum(_silver_fact_basis(ctx), "line_total")
    observed = _sum(ctx.table("gold", "fact_sales_item"), "net_amount")
    return Outcome(1, int(abs(observed - expected) > ctx.tolerance), observed, expected)


def _agg_vs_fact_amount(ctx: QualityContext) -> Outcome:
    expected = _sum(ctx.table("gold", "fact_sales_item"), "net_amount")
    observed = _sum(ctx.table("gold", "agg_daily_sales"), "net_revenue")
    return Outcome(1, int(abs(observed - expected) > ctx.tolerance), observed, expected)


def _agg_vs_silver_orders(ctx: QualityContext) -> Outcome:
    basis = _silver_fact_basis(ctx).where(F.col("transaction_type") == "SALE")
    expected = int(basis.select("sale_id").distinct().count())
    row = ctx.table("gold", "agg_daily_sales").agg(F.sum("orders").alias("s")).first()
    observed = int(row["s"] or 0) if row else 0
    return Outcome(1, int(observed != expected), observed, expected)


def _fact_ri(dim: str, fact_col: str, dim_col: str) -> Any:
    def check(ctx: QualityContext) -> Outcome:
        fact = ctx.table("gold", "fact_sales_item")
        keys = ctx.table("gold", dim).select(F.col(dim_col).alias(fact_col))
        return Outcome(_count(fact), _count(fact.join(keys, fact_col, "left_anti")))

    return check


def gold_rules() -> list[DatasetRule]:
    def rule(
        rule_id: str, name: str, description: str, category: Category, table: str, check: Any
    ) -> DatasetRule:
        return DatasetRule(rule_id, name, description, category, "gold", f"gold.{table}", B, check)

    rules = [
        unique_key("gold", "fact_sales_item", ("sale_id", "line_number")),
        unique_key("gold", "agg_daily_sales", ("sale_date", "store_id")),
        unique_key("gold", "dim_customer", ("customer_key",)),
        unique_key("gold", "dim_product", ("product_id",)),
        not_null(
            "gold",
            "fact_sales_item",
            ("sale_id", "line_number", "date_key", "store_id", "customer_key", "product_id", "net_amount"),
        ),
    ]
    rules += [
        rule(f"GLD-FACT-RI-{dim.upper().replace('_', '-')}", f"fact -> {dim}",
             f"Every fact_sales_item.{fc} exists in {dim}.{dc}.", Category.INTEGRITY, "fact_sales_item",
             _fact_ri(dim, fc, dc))
        for dim, fc, dc in (("dim_product", "product_id", "product_id"), ("dim_store", "store_id", "store_id"),
                            ("dim_customer", "customer_key", "customer_key"), ("dim_date", "date_key", "date_key"))
    ]  # fmt: skip
    rules += [
        rule("REC-GOLD-FACT-COUNT", "fact rows vs silver",
             "fact_sales_item rows = silver sale_items of COMPLETED sales.",
             Category.RECONCILIATION, "fact_sales_item", _fact_vs_silver_count),
        rule("REC-GOLD-FACT-AMOUNT", "fact net amount vs silver",
             "SUM(fact.net_amount) = SUM(silver.sale_items.line_total) of COMPLETED sales (within tolerance).",
             Category.RECONCILIATION, "fact_sales_item", _fact_vs_silver_amount),
        rule("REC-GOLD-AGG-AMOUNT", "daily aggregate vs fact",
             "SUM(agg_daily_sales.net_revenue) = SUM(fact_sales_item.net_amount).",
             Category.RECONCILIATION, "agg_daily_sales", _agg_vs_fact_amount),
        rule("REC-GOLD-AGG-ORDERS", "daily orders vs silver",
             "SUM(agg_daily_sales.orders) = distinct COMPLETED SALE transactions with lines in Silver.",
             Category.RECONCILIATION, "agg_daily_sales", _agg_vs_silver_orders),
    ]  # fmt: skip
    return rules
