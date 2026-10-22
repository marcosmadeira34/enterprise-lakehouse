"""Silver conformance: typing, normalization, row-level validation, referential integrity and dedup.

Everything here is a pure DataFrame -> DataFrame function built from native Spark expressions (no
Python UDFs), so it is unit-testable and the optimizer sees the whole plan.

Each record ends with a ``_reasons`` array. Empty means valid; otherwise the codes say why the record
is rejected, in a stable order (malformed record, column checks, domain rules, foreign keys, duplicate).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.config import BusinessRules
from lakehouse.contracts import ColumnType, EntityContract, FkPolicy

MONEY_TYPE = DecimalType(18, 2)
REASON_MALFORMED = "MALFORMED_RECORD"
REASON_DUPLICATE = "DUPLICATE_BUSINESS_KEY"

# Accepted source layouts. Optional sections cover fractional seconds and UTC offsets ("Z", "-03:00").
TIMESTAMP_FORMATS = (
    "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][XXX]",
    "yyyy-MM-dd HH:mm:ss[.SSSSSS][XXX]",
    "dd/MM/yyyy HH:mm:ss",
)
DATE_FORMATS = ("yyyy-MM-dd", "dd/MM/yyyy")
TRUE_VALUES = ("true", "1", "yes", "y", "sim", "s")
FALSE_VALUES = ("false", "0", "no", "n", "nao", "não")

SPARK_TYPES: dict[ColumnType, DataType] = {
    ColumnType.STRING: StringType(),
    ColumnType.ID: StringType(),
    ColumnType.CODE: StringType(),
    ColumnType.EMAIL: StringType(),
    ColumnType.INT: IntegerType(),
    ColumnType.MONEY: MONEY_TYPE,
    ColumnType.DATE: DateType(),
    ColumnType.TIMESTAMP: TimestampType(),
    ColumnType.BOOLEAN: BooleanType(),
}

# Columns derived during conformance, per entity.
DERIVED: dict[str, list[tuple[str, DataType, Callable[[], Column]]]] = {
    "sales": [
        ("sale_date", DateType(), lambda: F.to_date("sale_ts")),
        ("sale_month", StringType(), lambda: F.date_format("sale_ts", "yyyy-MM")),
    ],
}

SILVER_LINEAGE = [
    StructField("_extra_columns", StringType()),
    StructField("_record_hash", StringType()),
    StructField("_batch_id", StringType()),
    StructField("_batch_seq", LongType()),
    StructField("_source_file", StringType()),
    StructField("_silver_run_id", StringType()),
    StructField("_silver_processed_at", TimestampType()),
]

REJECTS_SCHEMA = StructType(
    [
        StructField("entity", StringType()),
        StructField("business_key", StringType()),
        StructField("rejection_reason", StringType()),
        StructField("rejection_reasons", ArrayType(StringType())),
        StructField("raw_record", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_batch_seq", LongType()),
        StructField("_source_file", StringType()),
        StructField("_record_hash", StringType()),
        StructField("_run_id", StringType()),
        StructField("rejected_at", TimestampType()),
    ]
)


# ---------------------------------------------------------------------- normalization primitives
def clean_text(c: Column) -> Column:
    """Trim; empty strings become NULL (a blank field is a missing value)."""
    trimmed = F.trim(c)
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)


def normalize_id(c: Column) -> Column:
    return F.upper(clean_text(c))


def parse_int(c: Column) -> Column:
    s = clean_text(c)
    # Spark would truncate "3.7" to 3 on cast; only integral literals are accepted.
    return F.when(s.rlike(r"^[+-]?\d{1,9}$"), s.cast(IntegerType()))


def parse_money(c: Column) -> Column:
    s = F.regexp_replace(clean_text(c), r"^(R\$|\$)", "")
    s = F.regexp_replace(s, r"\s+", "")
    # Decimal point is the contract; thousands separators are not accepted (ambiguous across locales).
    return F.when(s.rlike(r"^[+-]?\d{1,16}(\.\d+)?$"), s.cast(MONEY_TYPE))


def parse_timestamp(c: Column) -> Column:
    s = clean_text(c)
    return F.coalesce(*[F.try_to_timestamp(s, F.lit(fmt)) for fmt in TIMESTAMP_FORMATS])


def parse_date(c: Column) -> Column:
    s = clean_text(c)
    return F.coalesce(*[F.to_date(F.try_to_timestamp(s, F.lit(fmt))) for fmt in DATE_FORMATS])


def parse_bool(c: Column) -> Column:
    s = F.lower(clean_text(c))
    return F.when(s.isin(*TRUE_VALUES), F.lit(True)).when(s.isin(*FALSE_VALUES), F.lit(False))


def typed(column_type: ColumnType, c: Column) -> Column:
    match column_type:
        case ColumnType.ID | ColumnType.CODE:
            return normalize_id(c)
        case ColumnType.EMAIL:
            return F.lower(clean_text(c))
        case ColumnType.INT:
            return parse_int(c)
        case ColumnType.MONEY:
            return parse_money(c)
        case ColumnType.DATE:
            return parse_date(c)
        case ColumnType.TIMESTAMP:
            return parse_timestamp(c)
        case ColumnType.BOOLEAN:
            return parse_bool(c)
        case _:
            return clean_text(c)


# ---------------------------------------------------------------------- rule catalog
@dataclass(frozen=True)
class RowRule:
    reason: str
    description: str
    category: str


def _reason_rule_id(entity: str, reason: str) -> str:
    return f"SLV-{entity.upper()}-{reason.replace(':', '-').replace('_', '-')}"


def domain_rules(entity: str, rules: BusinessRules) -> list[tuple[RowRule, Callable[[], Column]]]:
    """Business validity rules evaluated on typed columns. Negative values are judged by meaning:
    a RETURN is negative by definition, a REFUNDED payment too; a negative unit price never is."""
    money_rule = "validity"
    catalog: dict[str, list[tuple[RowRule, Callable[[], Column]]]] = {
        "products": [
            (RowRule("NEGATIVE_PRICE", "Product list price must not be negative.", money_rule),
             lambda: F.col("unit_price") < 0),
            (RowRule("NEGATIVE_COST", "Product unit cost must not be negative.", money_rule),
             lambda: F.col("unit_cost") < 0),
        ],
        "sales": [
            (RowRule("NEGATIVE_AMOUNT_FOR_SALE", "A SALE total must not be negative.", money_rule),
             lambda: (F.col("transaction_type") == "SALE") & (F.col("total_amount") < 0)),
            (RowRule("POSITIVE_AMOUNT_FOR_RETURN", "A RETURN total must not be positive.", money_rule),
             lambda: (F.col("transaction_type") == "RETURN") & (F.col("total_amount") > 0)),
            (RowRule("RETURN_WITHOUT_ORIGINAL", "A RETURN must reference the original sale.", "completeness"),
             lambda: (F.col("transaction_type") == "RETURN") & F.col("original_sale_id").isNull()),
            (RowRule("INVALID_CURRENCY", f"Currency must be one of {rules.allowed_currencies}.", "validity"),
             lambda: ~F.col("currency").isin(*rules.allowed_currencies)),
        ],
        "sale_items": [
            (RowRule("NEGATIVE_UNIT_PRICE", "Unit price charged must not be negative.", money_rule),
             lambda: F.col("unit_price") < 0),
            (RowRule("NEGATIVE_DISCOUNT", "Discount must not be negative.", money_rule),
             lambda: F.col("discount_amount") < 0),
            (RowRule("ZERO_QUANTITY", "Quantity must not be zero (negative only on returns).", "validity"),
             lambda: F.col("quantity") == 0),
        ],
        "payments": [
            (RowRule("NEGATIVE_AMOUNT_FOR_PAYMENT", "Only REFUNDED payments may be negative.", money_rule),
             lambda: (F.col("amount") < 0) & (F.col("status") != "REFUNDED")),
            (RowRule("POSITIVE_REFUND", "A REFUNDED payment must not be positive.", money_rule),
             lambda: (F.col("amount") > 0) & (F.col("status") == "REFUNDED")),
        ],
        "inventory": []
        if rules.allow_negative_stock
        else [
            (RowRule("NEGATIVE_STOCK", "Stock on hand must not be negative.", "validity"),
             lambda: F.col("quantity_on_hand") < 0),
        ],
    }  # fmt: skip
    return catalog.get(entity, [])


def row_rule_catalog(contract: EntityContract, rules: BusinessRules) -> dict[str, tuple[str, RowRule]]:
    """reason code -> (rule id, rule) for every row-level rule that applies to the entity."""
    out: list[RowRule] = [RowRule(REASON_MALFORMED, "Record could not be parsed (wrong field count or JSON).",
                                  "validity")]  # fmt: skip
    for col in contract.columns:
        if col.required:
            out.append(RowRule(f"MISSING_REQUIRED:{col.name}", f"{col.name} is mandatory.", "completeness"))
        if col.type not in (ColumnType.STRING, ColumnType.ID, ColumnType.CODE, ColumnType.EMAIL):
            out.append(RowRule(f"INVALID_FORMAT:{col.name}", f"{col.name} must be a valid {col.type.value}.",
                               "validity"))  # fmt: skip
        if col.allowed_values:
            out.append(RowRule(f"INVALID_DOMAIN:{col.name}", f"{col.name} must be one of {list(col.allowed_values)}.",
                               "validity"))  # fmt: skip
        if col.type in (ColumnType.DATE, ColumnType.TIMESTAMP):
            out.append(
                RowRule(
                    f"FUTURE_DATE:{col.name}",
                    f"{col.name} must not be later than now + {rules.max_future_days} day(s).",
                    "validity",
                )
            )
            out.append(RowRule(f"DATE_OUT_OF_RANGE:{col.name}",
                               f"{col.name} must not be earlier than {rules.min_valid_date}.", "validity"))  # fmt: skip
    out.extend(rule for rule, _ in domain_rules(contract.name, rules))
    for fk in contract.foreign_keys:
        if fk.policy is FkPolicy.REJECT:
            out.append(
                RowRule(
                    fk.reason_code, f"{fk.column} must exist in silver.{fk.parent}.", "referential_integrity"
                )
            )
    out.append(RowRule(REASON_DUPLICATE, f"Only the latest version per {list(contract.business_key)} is kept.",
                       "uniqueness"))  # fmt: skip
    return {r.reason: (_reason_rule_id(contract.name, r.reason), r) for r in out}


# ---------------------------------------------------------------------- conformance
def _array_of(conditions: list[tuple[Column, str]]) -> Column:
    if not conditions:
        return F.array().cast(ArrayType(StringType()))
    return F.array(*[F.when(cond, F.lit(reason)) for cond, reason in conditions])


def type_and_validate(df: DataFrame, contract: EntityContract, rules: BusinessRules) -> DataFrame:
    """Typed business columns plus ``_reasons`` from parsing and domain rules."""
    now_limit = F.current_timestamp() + F.expr(f"INTERVAL {int(rules.max_future_days)} DAYS")
    min_date = F.lit(rules.min_valid_date.isoformat()).cast("date")
    column_checks: list[tuple[Column, str]] = [(F.col("_corrupt_record").isNotNull(), REASON_MALFORMED)]
    typed_cols = []
    for col in contract.columns:
        raw = clean_text(F.col(col.name))
        value = typed(col.type, F.col(col.name))
        typed_cols.append(value.alias(col.name))
        if col.required:
            column_checks.append((raw.isNull(), f"MISSING_REQUIRED:{col.name}"))
        column_checks.append((raw.isNotNull() & value.isNull(), f"INVALID_FORMAT:{col.name}"))
        if col.allowed_values:
            column_checks.append(
                (value.isNotNull() & ~value.isin(*col.allowed_values), f"INVALID_DOMAIN:{col.name}")
            )
        if col.type is ColumnType.TIMESTAMP:
            column_checks.append((value > now_limit, f"FUTURE_DATE:{col.name}"))
            column_checks.append((F.to_date(value) < min_date, f"DATE_OUT_OF_RANGE:{col.name}"))
        elif col.type is ColumnType.DATE:
            column_checks.append((value > F.to_date(now_limit), f"FUTURE_DATE:{col.name}"))
            column_checks.append((value < min_date, f"DATE_OUT_OF_RANGE:{col.name}"))

    raw_record = F.to_json(
        F.struct(*[F.col(n) for n in contract.column_names], "_extra_columns", "_corrupt_record")
    )
    business_key = F.concat_ws("|", *[normalize_id(F.col(k)) for k in contract.business_key])
    lineage = ["_extra_columns", "_record_hash", "_batch_id", "_batch_seq", "_source_file"]
    staged = df.select(
        *typed_cols,
        *lineage,
        raw_record.alias("_raw_record"),
        business_key.alias("_business_key"),
        _array_of(column_checks).alias("_col_reasons"),
    )
    domain = [(cond(), rule.reason) for rule, cond in domain_rules(contract.name, rules)]
    return staged.withColumn("_reasons", F.array_compact(F.concat("_col_reasons", _array_of(domain)))).drop(
        "_col_reasons"
    )


def check_foreign_keys(
    df: DataFrame, contract: EntityContract, parent_keys: dict[str, DataFrame | None], broadcast: set[str]
) -> DataFrame:
    """Append ORPHAN_* reasons for REJECT-policy foreign keys whose value is absent from the parent.

    ``parent_keys`` maps parent entity -> DataFrame with a single key column (None: no parent table).
    NULL child values are not orphans; a missing mandatory value is already reported as such.
    """
    for i, fk in enumerate(contract.foreign_keys):
        if fk.policy is not FkPolicy.REJECT:
            continue
        flag = f"_fk_missing_{i}"
        keys = parent_keys.get(fk.parent)
        if keys is None:
            df = df.withColumn(flag, F.col(fk.column).isNotNull())
        else:
            marker = f"_fk_found_{i}"
            right = (
                keys.select(F.col(fk.parent_column).alias(f"_fk_key_{i}"))
                .distinct()
                .withColumn(marker, F.lit(True))
            )
            if fk.parent in broadcast:
                right = F.broadcast(right)
            df = (
                df.join(right, df[fk.column] == right[f"_fk_key_{i}"], "left")
                .withColumn(flag, F.col(fk.column).isNotNull() & F.col(marker).isNull())
                .drop(f"_fk_key_{i}", marker)
            )
        df = df.withColumn(
            "_reasons",
            F.when(F.col(flag), F.concat("_reasons", F.array(F.lit(fk.reason_code)))).otherwise(
                F.col("_reasons")
            ),
        ).drop(flag)
    return df


def deduplicate(df: DataFrame, contract: EntityContract) -> DataFrame:
    """Among valid records, keep the latest version of each business key; the others are rejected.

    Order: version column, then batch arrival order, then source file and record hash so the choice is
    deterministic even for exact duplicates.
    """
    valid = F.size("_reasons") == 0
    window = Window.partitionBy(valid, *contract.business_key).orderBy(
        F.col(contract.version_column).desc_nulls_last(),
        F.col("_batch_seq").desc(),
        F.col("_source_file").desc(),
        F.col("_record_hash").desc(),
    )
    ranked = df.withColumn("_rn", F.row_number().over(window))
    return ranked.withColumn(
        "_reasons",
        F.when(valid & (F.col("_rn") > 1), F.array(F.lit(REASON_DUPLICATE))).otherwise(F.col("_reasons")),
    ).drop("_rn")


def with_derived(df: DataFrame, contract: EntityContract) -> DataFrame:
    for name, _dtype, expr in DERIVED.get(contract.name, []):
        df = df.withColumn(name, expr())
    return df


def conform(
    bronze: DataFrame,
    contract: EntityContract,
    rules: BusinessRules,
    parent_keys: dict[str, DataFrame | None],
    broadcast: set[str] | None = None,
) -> DataFrame:
    df = type_and_validate(bronze, contract, rules)
    df = check_foreign_keys(df, contract, parent_keys, broadcast or set())
    df = deduplicate(df, contract)
    return with_derived(df, contract)


def silver_schema(contract: EntityContract) -> StructType:
    fields = [StructField(c.name, SPARK_TYPES[c.type]) for c in contract.columns]
    fields += [StructField(name, dtype) for name, dtype, _ in DERIVED.get(contract.name, [])]
    return StructType(fields + SILVER_LINEAGE)


def accepted_rows(conformed: DataFrame, contract: EntityContract, run_id: str) -> DataFrame:
    schema = silver_schema(contract)
    projected = (
        conformed.where(F.size("_reasons") == 0)
        .withColumn("_silver_run_id", F.lit(run_id))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    return projected.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def rejected_rows(conformed: DataFrame, contract: EntityContract, run_id: str) -> DataFrame:
    return conformed.where(F.size("_reasons") > 0).select(
        F.lit(contract.name).alias("entity"),
        F.col("_business_key").alias("business_key"),
        F.element_at("_reasons", 1).alias("rejection_reason"),
        F.col("_reasons").alias("rejection_reasons"),
        F.col("_raw_record").alias("raw_record"),
        "_batch_id",
        "_batch_seq",
        "_source_file",
        "_record_hash",
        F.lit(run_id).alias("_run_id"),
        F.current_timestamp().alias("rejected_at"),
    )


MONEY_COLUMN = {"sales": "total_amount", "sale_items": "line_total", "payments": "amount"}


def max_future(rules: BusinessRules) -> timedelta:
    return timedelta(days=rules.max_future_days)
