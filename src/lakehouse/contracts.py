"""Data contracts of the retail domain.

A contract is the single source of truth for an entity: source format, columns and their types,
which columns are mandatory, the business key, foreign keys and how Silver is laid out. Bronze, Silver,
the synthetic generator and the data dictionary all derive from these definitions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class SourceFormat(StrEnum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"


class ColumnType(StrEnum):
    STRING = "string"
    ID = "id"  # trimmed, upper-cased identifier
    INT = "int"
    MONEY = "money"  # decimal(18,2)
    DATE = "date"
    TIMESTAMP = "timestamp"
    BOOLEAN = "boolean"
    EMAIL = "email"  # trimmed, lower-cased
    CODE = "code"  # trimmed, upper-cased domain code (e.g. SALE / RETURN)


class FkPolicy(StrEnum):
    REJECT = "reject"  # the record goes to the rejects table
    WARN = "warn"  # the record is accepted; a quality rule reports the gap


@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType
    required: bool = False
    description: str = ""
    allowed_values: tuple[str, ...] = ()


@dataclass(frozen=True)
class ForeignKey:
    column: str
    parent: str
    parent_column: str
    policy: FkPolicy = FkPolicy.REJECT

    @property
    def reason_code(self) -> str:
        return f"ORPHAN_{_SINGULAR.get(self.parent, self.parent).upper()}"


_SINGULAR = {
    "categories": "category",
    "products": "product",
    "customers": "customer",
    "stores": "store",
    "sales": "sale",
}


@dataclass(frozen=True)
class EntityContract:
    name: str
    description: str
    source_format: SourceFormat
    columns: tuple[Column, ...]
    business_key: tuple[str, ...]
    version: int = 1
    foreign_keys: tuple[ForeignKey, ...] = ()
    # Column used to order competing versions of the same business key (latest wins).
    version_column: str = "updated_at"
    # Silver partition column derived during conformance (None = unpartitioned).
    silver_partition: str | None = None
    tags: tuple[str, ...] = field(default=())

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns if c.required)

    def column(self, name: str) -> Column:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(f"{self.name} has no column {name!r}")


def _c(
    name: str,
    type_: ColumnType,
    required: bool = False,
    description: str = "",
    allowed: tuple[str, ...] = (),
) -> Column:
    return Column(name, type_, required, description, allowed)


T = ColumnType

CATEGORIES = EntityContract(
    name="categories",
    description="Product categories (hierarchy of one optional parent level).",
    source_format=SourceFormat.CSV,
    columns=(
        _c("category_id", T.ID, True, "Category business key."),
        _c("category_name", T.STRING, True, "Display name."),
        _c("parent_category_id", T.ID, False, "Optional parent category."),
        _c("updated_at", T.TIMESTAMP, True, "Last change at the source (UTC)."),
    ),
    business_key=("category_id",),
)

PRODUCTS = EntityContract(
    name="products",
    description="Product catalog with list price and unit cost.",
    source_format=SourceFormat.JSON,
    columns=(
        _c("product_id", T.ID, True, "Product business key."),
        _c("sku", T.ID, True, "Stock keeping unit."),
        _c("product_name", T.STRING, True, "Display name."),
        _c("category_id", T.ID, True, "Category of the product."),
        _c("unit_price", T.MONEY, True, "Current list price (BRL)."),
        _c("unit_cost", T.MONEY, False, "Current unit cost (BRL)."),
        _c("is_active", T.BOOLEAN, False, "Whether the product is sold."),
        _c("updated_at", T.TIMESTAMP, True, "Last change at the source (UTC)."),
    ),
    business_key=("product_id",),
    foreign_keys=(ForeignKey("category_id", "categories", "category_id"),),
)

CUSTOMERS = EntityContract(
    name="customers",
    description="Registered customers. Contains personal data (synthetic) restricted to Silver.",
    source_format=SourceFormat.CSV,
    columns=(
        _c("customer_id", T.ID, True, "Customer business key."),
        _c("first_name", T.STRING, False, "Personal data (synthetic)."),
        _c("last_name", T.STRING, False, "Personal data (synthetic)."),
        _c("email", T.EMAIL, False, "Personal data (synthetic)."),
        _c("phone", T.STRING, False, "Personal data (synthetic); optional, may be absent from newer files."),
        _c("city", T.STRING, False, "City of residence."),
        _c("state", T.CODE, False, "State code (UF)."),
        _c("signup_date", T.DATE, True, "Registration date."),
        _c("updated_at", T.TIMESTAMP, True, "Last change at the source (UTC)."),
    ),
    business_key=("customer_id",),
)

STORES = EntityContract(
    name="stores",
    description="Physical and online stores.",
    source_format=SourceFormat.CSV,
    columns=(
        _c("store_id", T.ID, True, "Store business key."),
        _c("store_name", T.STRING, True, "Display name."),
        _c("city", T.STRING, True, "City."),
        _c("state", T.CODE, True, "State code (UF)."),
        _c("store_format", T.CODE, False, "Store format.", ("STREET", "MALL", "ONLINE")),
        _c("opened_date", T.DATE, False, "Opening date."),
        _c("updated_at", T.TIMESTAMP, True, "Last change at the source (UTC)."),
    ),
    business_key=("store_id",),
)

SALES = EntityContract(
    name="sales",
    description="Sale and return headers. Returns carry negative totals by definition.",
    source_format=SourceFormat.CSV,
    columns=(
        _c("sale_id", T.ID, True, "Sale business key."),
        _c("store_id", T.ID, True, "Store where the transaction happened."),
        _c("customer_id", T.ID, False, "Registered customer; null for anonymous sales."),
        _c("sale_ts", T.TIMESTAMP, True, "Transaction timestamp (UTC)."),
        _c("transaction_type", T.CODE, True, "SALE or RETURN.", ("SALE", "RETURN")),
        _c("status", T.CODE, True, "COMPLETED or CANCELLED.", ("COMPLETED", "CANCELLED")),
        _c("total_amount", T.MONEY, True, "Net total; negative for returns."),
        _c("currency", T.CODE, True, "ISO currency code."),
        _c("original_sale_id", T.ID, False, "Sale being returned (returns only)."),
        _c("updated_at", T.TIMESTAMP, True, "Last change at the source (UTC)."),
    ),
    business_key=("sale_id",),
    foreign_keys=(
        ForeignKey("store_id", "stores", "store_id"),
        # A customer can be registered after the sale reaches the lake: accepted and reported.
        ForeignKey("customer_id", "customers", "customer_id", FkPolicy.WARN),
    ),
    silver_partition="sale_month",
)

SALE_ITEMS = EntityContract(
    name="sale_items",
    description="Sale lines. Quantity is negative on return lines.",
    source_format=SourceFormat.PARQUET,
    columns=(
        _c("sale_id", T.ID, True, "Parent sale."),
        _c("line_number", T.INT, True, "Line number within the sale."),
        _c("product_id", T.ID, True, "Product sold."),
        _c("quantity", T.INT, True, "Units; negative on return lines, never zero."),
        _c("unit_price", T.MONEY, True, "Unit price charged; never negative."),
        _c("discount_amount", T.MONEY, False, "Line discount; never negative."),
        _c("line_total", T.MONEY, True, "quantity * unit_price - sign(quantity) * discount."),
        _c("updated_at", T.TIMESTAMP, True, "Last change at the source (UTC)."),
    ),
    business_key=("sale_id", "line_number"),
    foreign_keys=(
        ForeignKey("sale_id", "sales", "sale_id"),
        ForeignKey("product_id", "products", "product_id"),
    ),
)

PAYMENTS = EntityContract(
    name="payments",
    description="Payments and refunds of sales.",
    source_format=SourceFormat.JSON,
    columns=(
        _c("payment_id", T.ID, True, "Payment business key."),
        _c("sale_id", T.ID, True, "Sale paid (or refunded)."),
        _c(
            "payment_method",
            T.CODE,
            True,
            "Payment method.",
            ("CREDIT_CARD", "DEBIT_CARD", "PIX", "CASH"),
        ),
        _c("amount", T.MONEY, True, "Amount; negative only for REFUNDED payments."),
        _c("status", T.CODE, True, "Payment status.", ("APPROVED", "DECLINED", "REFUNDED")),
        _c("paid_ts", T.TIMESTAMP, True, "Payment timestamp (UTC)."),
        _c("updated_at", T.TIMESTAMP, True, "Last change at the source (UTC)."),
    ),
    business_key=("payment_id",),
    foreign_keys=(ForeignKey("sale_id", "sales", "sale_id"),),
)

INVENTORY = EntityContract(
    name="inventory",
    description="Daily stock snapshot per store and product.",
    source_format=SourceFormat.PARQUET,
    columns=(
        _c("store_id", T.ID, True, "Store."),
        _c("product_id", T.ID, True, "Product."),
        _c("snapshot_date", T.DATE, True, "Snapshot date."),
        _c("quantity_on_hand", T.INT, True, "Units on hand; negative means backorder."),
        _c("reorder_point", T.INT, False, "Replenishment threshold."),
        _c("updated_at", T.TIMESTAMP, True, "Last change at the source (UTC)."),
    ),
    business_key=("store_id", "product_id", "snapshot_date"),
    foreign_keys=(
        ForeignKey("store_id", "stores", "store_id"),
        ForeignKey("product_id", "products", "product_id"),
    ),
)

# Processing order respects foreign keys: parents are conformed before children.
ENTITIES: dict[str, EntityContract] = {
    c.name: c for c in (CATEGORIES, PRODUCTS, CUSTOMERS, STORES, SALES, SALE_ITEMS, PAYMENTS, INVENTORY)
}
ENTITY_ORDER: tuple[str, ...] = tuple(ENTITIES)


def get_contract(entity: str) -> EntityContract:
    try:
        return ENTITIES[entity]
    except KeyError as exc:
        raise ValueError(f"unknown entity {entity!r}; expected one of {sorted(ENTITIES)}") from exc


def resolve_entities(entities: list[str] | None) -> list[str]:
    """Validate a user selection and return it in dependency order."""
    if not entities:
        return list(ENTITY_ORDER)
    for name in entities:
        get_contract(name)
    return [name for name in ENTITY_ORDER if name in entities]
