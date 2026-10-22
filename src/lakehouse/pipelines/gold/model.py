"""Gold business model: star schema for retail sales analytics.

Business rules that belong to consumption live here (and only here):

* only COMPLETED transactions reach the fact; returns stay as negative lines so net revenue is
  ``SUM(net_amount)`` without special cases; CANCELLED sales are excluded;
* customers are resolved to a dimension key: ``ANONYMOUS`` for sales without customer, ``UNKNOWN``
  for a customer id not (yet) registered — the fact never loses revenue for a missing dimension row;
* Gold carries no direct personal identifiers (names, e-mail, phone stay in Silver).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.pipelines.silver.transform import MONEY_TYPE

ANONYMOUS = "ANONYMOUS"
UNKNOWN = "UNKNOWN"

# Indicator catalog: published to metadata so every number can be traced to its formula and source.
METRICS: list[dict[str, object]] = [
    {"metric_name": "net_revenue", "table_name": "gold.agg_daily_sales",
     "description": "Revenue after discounts and returns.",
     "formula": "SUM(fact_sales_item.net_amount)", "source_tables": ["silver.sales", "silver.sale_items"],
     "filters": "sales.status = 'COMPLETED' (SALE and RETURN lines)"},
    {"metric_name": "gross_revenue", "table_name": "gold.agg_daily_sales",
     "description": "List value of units sold, before discounts.",
     "formula": "SUM(quantity * unit_price) over SALE lines", "source_tables": ["silver.sale_items"],
     "filters": "transaction_type = 'SALE' AND status = 'COMPLETED'"},
    {"metric_name": "discount_total", "table_name": "gold.agg_daily_sales",
     "description": "Discounts granted on sales.", "formula": "SUM(discount_amount) over SALE lines",
     "source_tables": ["silver.sale_items"], "filters": "transaction_type = 'SALE' AND status = 'COMPLETED'"},
    {"metric_name": "returns_amount", "table_name": "gold.agg_daily_sales",
     "description": "Value refunded on returns (negative).", "formula": "SUM(net_amount) over RETURN lines",
     "source_tables": ["silver.sale_items"], "filters": "transaction_type = 'RETURN' AND status = 'COMPLETED'"},
    {"metric_name": "orders", "table_name": "gold.agg_daily_sales",
     "description": "Number of completed sales.", "formula": "COUNT(DISTINCT sale_id) over SALE transactions",
     "source_tables": ["silver.sales"], "filters": "transaction_type = 'SALE' AND status = 'COMPLETED'"},
    {"metric_name": "avg_ticket", "table_name": "gold.agg_daily_sales",
     "description": "Average net value of a completed sale (returns excluded).",
     "formula": "SUM(net_amount of SALE lines) / orders", "source_tables": ["silver.sale_items", "silver.sales"],
     "filters": "transaction_type = 'SALE' AND status = 'COMPLETED'"},
    {"metric_name": "gross_margin", "table_name": "gold.agg_daily_sales",
     "description": "Net revenue minus cost of goods at current unit cost.",
     "formula": "SUM(net_amount) - SUM(quantity * unit_cost)", "source_tables": ["silver.sale_items", "silver.products"],
     "filters": "status = 'COMPLETED'; cost uses the current product cost (SCD type 1)"},
]  # fmt: skip


def dim_date(sales: DataFrame) -> DataFrame:
    bounds = sales.agg(F.min("sale_date").alias("lo"), F.max("sale_date").alias("hi"))
    days = bounds.select(F.explode(F.sequence("lo", "hi", F.expr("INTERVAL 1 DAY"))).alias("date"))
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),  # 1 = Sunday
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def dim_customer(customers: DataFrame, spark: SparkSession) -> DataFrame:
    registered = customers.select(
        F.col("customer_id").alias("customer_key"),
        F.lit(True).alias("is_registered"),
        "city",
        "state",
        "signup_date",
        F.year("signup_date").alias("signup_year"),
    )
    members = spark.createDataFrame(
        [(ANONYMOUS, False, None, None, None, None), (UNKNOWN, False, None, None, None, None)],
        registered.schema,
    )
    return registered.unionByName(members)


def dim_product(products: DataFrame, categories: DataFrame) -> DataFrame:
    cat = categories.select("category_id", "category_name", "parent_category_id")
    parent = categories.select(
        F.col("category_id").alias("parent_category_id"), F.col("category_name").alias("parent_category_name")
    )
    return (
        products.join(F.broadcast(cat), "category_id", "left")
        .join(F.broadcast(parent), "parent_category_id", "left")
        .select(
            "product_id",
            "sku",
            "product_name",
            "category_id",
            "category_name",
            "parent_category_id",
            "parent_category_name",
            "unit_price",
            "unit_cost",
            F.coalesce("is_active", F.lit(True)).alias("is_active"),
        )
    )


def dim_store(stores: DataFrame) -> DataFrame:
    return stores.select("store_id", "store_name", "city", "state", "store_format", "opened_date")


def fact_sales_item(
    sales: DataFrame, items: DataFrame, products: DataFrame, customers: DataFrame, run_id: str
) -> DataFrame:
    completed = sales.where(F.col("status") == "COMPLETED").select(
        "sale_id", "sale_ts", "sale_date", "sale_month", "store_id", "customer_id", "transaction_type"
    )
    known = customers.select(F.col("customer_id").alias("_known_customer"))
    cost = products.select("product_id", "category_id", "unit_cost")
    line = items.select("sale_id", "line_number", "product_id", "quantity", "unit_price", "discount_amount",
                        "line_total")  # fmt: skip
    joined = (
        line.join(completed, "sale_id", "inner")
        .join(F.broadcast(cost), "product_id", "left")
        .join(F.broadcast(known), F.col("customer_id") == F.col("_known_customer"), "left")
    )
    money = MONEY_TYPE
    return joined.select(
        "sale_id",
        "line_number",
        "sale_ts",
        "sale_date",
        "sale_month",
        F.date_format("sale_date", "yyyyMMdd").cast("int").alias("date_key"),
        "store_id",
        F.when(F.col("customer_id").isNull(), F.lit(ANONYMOUS))
        .when(F.col("_known_customer").isNull(), F.lit(UNKNOWN))
        .otherwise(F.col("customer_id"))
        .alias("customer_key"),
        F.col("customer_id").alias("source_customer_id"),
        "product_id",
        "category_id",
        "transaction_type",
        "quantity",
        "unit_price",
        F.coalesce("discount_amount", F.lit(0).cast(money)).alias("discount_amount"),
        (F.col("quantity") * F.col("unit_price")).cast(money).alias("gross_amount"),
        F.col("line_total").alias("net_amount"),
        (F.col("quantity") * F.coalesce("unit_cost", F.lit(0))).cast(money).alias("cost_amount"),
        F.lit(run_id).alias("_gold_run_id"),
    )


def agg_daily_sales(fact: DataFrame) -> DataFrame:
    is_sale = F.col("transaction_type") == "SALE"
    is_return = F.col("transaction_type") == "RETURN"
    money = MONEY_TYPE
    zero = F.lit(0).cast(money)
    agg = fact.groupBy("sale_date", "date_key", "store_id").agg(
        F.countDistinct(F.when(is_sale, F.col("sale_id"))).alias("orders"),
        F.countDistinct(F.when(is_return, F.col("sale_id"))).alias("returns"),
        F.coalesce(F.sum(F.when(is_sale, F.col("quantity"))), F.lit(0)).cast("long").alias("items_sold"),
        F.coalesce(F.sum(F.when(is_sale, F.col("gross_amount"))), zero).cast(money).alias("gross_revenue"),
        F.coalesce(F.sum(F.when(is_sale, F.col("discount_amount"))), zero)
        .cast(money)
        .alias("discount_total"),
        F.coalesce(F.sum(F.when(is_return, F.col("net_amount"))), zero).cast(money).alias("returns_amount"),
        F.sum("net_amount").cast(money).alias("net_revenue"),
        F.coalesce(F.sum(F.when(is_sale, F.col("net_amount"))), zero).alias("_sales_net"),
        F.sum("cost_amount").cast(money).alias("cost_amount"),
    )
    return agg.select(
        "sale_date",
        "date_key",
        "store_id",
        "orders",
        "returns",
        "items_sold",
        "gross_revenue",
        "discount_total",
        "returns_amount",
        "net_revenue",
        F.when(F.col("orders") > 0, F.round(F.col("_sales_net") / F.col("orders"), 2))
        .cast(money)
        .alias("avg_ticket"),
        (F.col("net_revenue") - F.col("cost_amount")).cast(money).alias("gross_margin"),
    )


def fact_inventory_snapshot(inventory: DataFrame) -> DataFrame:
    return inventory.select(
        "store_id",
        "product_id",
        "snapshot_date",
        F.date_format("snapshot_date", "yyyyMMdd").cast("int").alias("date_key"),
        "quantity_on_hand",
        "reorder_point",
        (F.col("quantity_on_hand") < 0).alias("is_backorder"),
        (F.col("quantity_on_hand") <= F.coalesce("reorder_point", F.lit(0))).alias("below_reorder_point"),
    )
