"""Deterministic synthetic retail data with controlled anomalies.

Design: the generator first builds a *world* of valid records from the seed. Anomalies are emitted as
extra rows that are never part of the world (a malformed sale and its lines, an orphan line, an
exact duplicate...). Warning-level anomalies (financial mismatch, unknown customer, negative stock)
are valid records and do live in the world. The manifest therefore knows exactly what the platform
must accept, reject and report, and the expectations are computed from the world, not hard-coded.

No real personal data is produced: names come from short generic lists and e-mails use the
reserved ``example.com`` domain.

Drop 1 is the initial load. Drop 2 rebuilds the drop-1 world from the same seed and emits incremental
changes: updates, late (historical) sales, a schema change, an incompatible file and a re-sent file.
"""

from __future__ import annotations

import csv
import io
import json
import random
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from lakehouse.common.storage import ObjectStore, join_uri
from lakehouse.contracts import ENTITIES

CENT = Decimal("0.01")

SCALES: dict[str, dict[str, int]] = {
    "tiny": {
        "customers": 40,
        "products": 20,
        "categories": 5,
        "stores": 3,
        "sales": 150,
        "inventory_days": 2,
    },
    "small": {
        "customers": 300,
        "products": 80,
        "categories": 10,
        "stores": 6,
        "sales": 1500,
        "inventory_days": 5,
    },
    "medium": {
        "customers": 5000,
        "products": 1000,
        "categories": 25,
        "stores": 30,
        "sales": 50000,
        "inventory_days": 7,
    },
    "large": {
        "customers": 50000,
        "products": 5000,
        "categories": 40,
        "stores": 100,
        "sales": 500000,
        "inventory_days": 7,
    },
}

DROP1_SCENARIOS = (
    "duplicate_customers",
    "orphan_products",
    "invalid_dates",
    "negative_amounts",
    "incomplete_records",
    "malformed_rows",
    "financial_mismatch",
    "orphan_customers",
    "negative_stock",
    "negative_margin",
    "legacy_date_format",
)
DROP2_SCENARIOS = (
    "updates",
    "stale_update",
    "new_sales",
    "late_batch",
    "schema_change",
    "incompatible_schema",
    "repeated_file",
)
ALL_SCENARIOS = DROP1_SCENARIOS + DROP2_SCENARIOS

# Inventory snapshots cover at most this many products per store to keep volumes proportional.
MAX_INVENTORY_PRODUCTS = 200

FIRST_NAMES = ("Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela", "Hugo", "Iara", "Joao")
LAST_NAMES = ("Silva", "Souza", "Costa", "Lima", "Rocha", "Alves", "Pereira", "Gomes", "Ribeiro", "Martins")
CITIES = (
    ("Sao Paulo", "SP"),
    ("Campinas", "SP"),
    ("Rio de Janeiro", "RJ"),
    ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"),
    ("Porto Alegre", "RS"),
    ("Recife", "PE"),
    ("Salvador", "BA"),
)
CATEGORY_NAMES = (
    "Bebidas",
    "Mercearia",
    "Limpeza",
    "Higiene",
    "Hortifruti",
    "Padaria",
    "Frios",
    "Eletronicos",
    "Casa",
    "Pet",
)
PRODUCT_NOUNS = ("Cafe", "Arroz", "Sabao", "Shampoo", "Banana", "Pao", "Queijo", "Fone", "Copo", "Racao")
PRODUCT_ADJ = ("Premium", "Classico", "Leve", "Integral", "Natural", "Max", "Mini", "Eco")
PAYMENT_METHODS = ("CREDIT_CARD", "DEBIT_CARD", "PIX", "CASH")
STORE_FORMATS = ("STREET", "MALL", "ONLINE")


def money(value: Decimal | float | int) -> Decimal:
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass
class GeneratorConfig:
    seed: int = 42
    scale: str = "small"
    files_per_entity: int = 2
    error_rate: float = 0.01
    scenarios: tuple[str, ...] = ALL_SCENARIOS
    # Zipf-like exponent of the store distribution (0 = uniform). Used by benchmarks to create skew.
    store_skew: float = 0.0
    start_date: date = date(2026, 7, 1)
    days: int = 60
    overrides: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.scale not in SCALES:
            raise ValueError(f"unknown scale {self.scale!r}; expected one of {sorted(SCALES)}")
        unknown = set(self.scenarios) - set(ALL_SCENARIOS)
        if unknown:
            raise ValueError(f"unknown scenarios: {sorted(unknown)}")
        if not 0 <= self.error_rate <= 0.5:
            raise ValueError("error_rate must be between 0 and 0.5")
        if self.files_per_entity < 1:
            raise ValueError("files_per_entity must be >= 1")

    def size(self, key: str) -> int:
        return self.overrides.get(key, SCALES[self.scale][key])

    def has(self, scenario: str) -> bool:
        return scenario in self.scenarios

    def anomalies(self, base: int) -> int:
        """Number of anomalous records for a scenario: proportional to volume, at least one."""
        if self.error_rate == 0:
            return 0
        return max(1, round(base * self.error_rate))

    @property
    def window_end(self) -> datetime:
        return datetime.combine(self.start_date + timedelta(days=self.days), datetime.min.time())


@dataclass
class World:
    """Valid source state, keyed by business key. Values are typed Python objects."""

    categories: dict[str, dict[str, Any]] = field(default_factory=dict)
    products: dict[str, dict[str, Any]] = field(default_factory=dict)
    customers: dict[str, dict[str, Any]] = field(default_factory=dict)
    stores: dict[str, dict[str, Any]] = field(default_factory=dict)
    sales: dict[str, dict[str, Any]] = field(default_factory=dict)
    sale_items: dict[str, dict[str, Any]] = field(default_factory=dict)
    payments: dict[str, dict[str, Any]] = field(default_factory=dict)
    inventory: dict[str, dict[str, Any]] = field(default_factory=dict)

    def table(self, entity: str) -> dict[str, dict[str, Any]]:
        table: dict[str, dict[str, Any]] = getattr(self, entity)
        return table


def business_key(entity: str, row: dict[str, Any]) -> str:
    return "|".join(str(row[k]) for k in ENTITIES[entity].business_key)


@dataclass
class Drop:
    """Rows to write for one drop, per entity and file part, plus raw extras."""

    rows: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    rejections: list[dict[str, Any]] = field(default_factory=list)
    # Raw text lines appended to the first CSV part (malformed records).
    raw_lines: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    def reject(self, entity: str, row: dict[str, Any] | None, reason: str) -> None:
        key = business_key(entity, row) if row is not None else None
        self.rejections.append({"entity": entity, "business_key": key, "reason": reason})


class RetailGenerator:
    def __init__(self, config: GeneratorConfig) -> None:
        self.cfg = config
        self.rng = random.Random(config.seed)
        self.world = World()
        self._sale_seq = 0
        self._customer_seq = 0
        self._product_seq = 0
        self._payment_seq = 0
        self._extra_line_seq = 0
        # Indexes so generation stays linear in volume.
        self._items_by_sale: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._payments_by_sale: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._customer_ids: list[str] = []
        self._product_ids: list[str] = []
        self._store_ids: list[str] = []
        self._store_weights: list[float] = []
        self._dims_updated_at = config.window_end + timedelta(hours=1)

    # ------------------------------------------------------------------ world (valid data)
    def build_world(self) -> None:
        self._build_categories()
        self._build_products()
        self._build_customers()
        self._build_stores()
        self._build_sales()
        self._build_inventory()

    def _build_categories(self) -> None:
        count = self.cfg.size("categories")
        top_level = max(1, min(3, count))
        for i in range(1, count + 1):
            cid = f"CAT{i:03d}"
            name = CATEGORY_NAMES[(i - 1) % len(CATEGORY_NAMES)]
            if i > len(CATEGORY_NAMES):
                name = f"{name} {i}"
            parent = None if i <= top_level else f"CAT{self.rng.randint(1, top_level):03d}"
            self.world.categories[cid] = {
                "category_id": cid,
                "category_name": name,
                "parent_category_id": parent,
                "updated_at": self._dims_updated_at,
            }

    def _new_product(self, updated_at: datetime) -> dict[str, Any]:
        self._product_seq += 1
        pid = f"P{self._product_seq:06d}"
        price = money(self.rng.uniform(2, 400))
        return {
            "product_id": pid,
            "sku": f"SKU-{self._product_seq:07d}",
            "product_name": f"{self.rng.choice(PRODUCT_NOUNS)} {self.rng.choice(PRODUCT_ADJ)} {self._product_seq}",
            "category_id": self.rng.choice(list(self.world.categories)),
            "unit_price": price,
            "unit_cost": money(price * Decimal(str(round(self.rng.uniform(0.4, 0.8), 2)))),
            "is_active": self.rng.random() > 0.05,
            "updated_at": updated_at,
        }

    def _build_products(self) -> None:
        for _ in range(self.cfg.size("products")):
            product = self._new_product(self._dims_updated_at)
            self.world.products[product["product_id"]] = product
        if self.cfg.has("negative_margin"):
            # Clearance items sold below cost: valid, but reported as a warning.
            for pid in self.rng.sample(
                list(self.world.products), self.cfg.anomalies(len(self.world.products))
            ):
                product = self.world.products[pid]
                product["unit_cost"] = money(product["unit_price"] * Decimal("1.15"))

    def _new_customer(self, updated_at: datetime) -> dict[str, Any]:
        self._customer_seq += 1
        first, last = self.rng.choice(FIRST_NAMES), self.rng.choice(LAST_NAMES)
        city, state = self.rng.choice(CITIES)
        signup = date(2020, 1, 1) + timedelta(days=self.rng.randint(0, 2000))
        return {
            "customer_id": f"C{self._customer_seq:07d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}.{self._customer_seq}@example.com",
            "phone": f"+55 11 9{self.rng.randint(1000, 9999)}-{self.rng.randint(1000, 9999)}",
            "city": city,
            "state": state,
            "signup_date": min(signup, self.cfg.start_date),
            "updated_at": updated_at,
        }

    def _build_customers(self) -> None:
        for _ in range(self.cfg.size("customers")):
            customer = self._new_customer(self._dims_updated_at)
            self.world.customers[customer["customer_id"]] = customer
        if self.cfg.has("duplicate_customers"):
            # Same person registered twice under different ids: both records are valid at row level;
            # the shared e-mail is reported as a warning (entity resolution is a business decision).
            ids = list(self.world.customers)
            for cid in self.rng.sample(ids, self.cfg.anomalies(len(ids))):
                twin = self._new_customer(self._dims_updated_at)
                original = self.world.customers[cid]
                twin.update({k: original[k] for k in ("first_name", "last_name", "email", "city", "state")})
                self.world.customers[twin["customer_id"]] = twin

    def _build_stores(self) -> None:
        for i in range(1, self.cfg.size("stores") + 1):
            city, state = CITIES[(i - 1) % len(CITIES)]
            sid = f"S{i:03d}"
            self.world.stores[sid] = {
                "store_id": sid,
                "store_name": f"Loja {city} {i}",
                "city": city,
                "state": state,
                "store_format": STORE_FORMATS[(i - 1) % len(STORE_FORMATS)],
                "opened_date": date(2015, 1, 1) + timedelta(days=30 * i),
                "updated_at": self._dims_updated_at,
            }

    def _random_ts(self, start: date | None = None, days: int | None = None) -> datetime:
        start = start or self.cfg.start_date
        span = (days or self.cfg.days) * 86400
        return datetime.combine(start, datetime.min.time()) + timedelta(seconds=self.rng.randint(0, span - 1))

    def _next_sale_id(self) -> str:
        self._sale_seq += 1
        return f"T{self._sale_seq:09d}"

    def _next_payment_id(self) -> str:
        self._payment_seq += 1
        return f"PAY{self._payment_seq:09d}"

    def _make_sale(
        self, sale_ts: datetime | None = None, updated_lag: timedelta = timedelta(minutes=5)
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        """A complete, valid SALE with its lines and payments (not yet added to the world)."""
        sale_id = self._next_sale_id()
        ts = sale_ts or self._random_ts()
        store_id = self.rng.choices(self._store_ids, weights=self._store_weights)[0]
        customer_id = self.rng.choice(self._customer_ids) if self.rng.random() < 0.85 else None
        status = "COMPLETED" if self.rng.random() > 0.03 else "CANCELLED"
        updated_at = ts + updated_lag
        items = []
        for line in range(1, self.rng.randint(1, 4) + 1):
            product = self.world.products[self.rng.choice(self._product_ids)]
            qty = self.rng.randint(1, 5)
            price = product["unit_price"]
            gross = money(price * qty)
            discount = money(gross * Decimal(str(self.rng.choice((0, 0, 0, 0.05, 0.1)))))
            items.append(
                {
                    "sale_id": sale_id,
                    "line_number": line,
                    "product_id": product["product_id"],
                    "quantity": qty,
                    "unit_price": price,
                    "discount_amount": discount,
                    "line_total": gross - discount,
                    "updated_at": updated_at,
                }
            )
        total = sum((i["line_total"] for i in items), Decimal("0.00"))
        sale = {
            "sale_id": sale_id,
            "store_id": store_id,
            "customer_id": customer_id,
            "sale_ts": ts,
            "transaction_type": "SALE",
            "status": status,
            "total_amount": total,
            "currency": "BRL",
            "original_sale_id": None,
            "updated_at": updated_at,
        }
        return sale, items, self._make_payments(sale)

    def _make_payments(self, sale: dict[str, Any]) -> list[dict[str, Any]]:
        total: Decimal = sale["total_amount"]
        paid_ts = sale["sale_ts"] + timedelta(seconds=30)
        base = {"sale_id": sale["sale_id"], "paid_ts": paid_ts, "updated_at": sale["updated_at"]}
        if sale["status"] == "CANCELLED":
            return [{**base, "payment_id": self._next_payment_id(), "payment_method": "CREDIT_CARD",
                     "amount": total, "status": "DECLINED"}]  # fmt: skip
        if sale["transaction_type"] == "RETURN":
            return [{**base, "payment_id": self._next_payment_id(), "payment_method": "PIX",
                     "amount": total, "status": "REFUNDED"}]  # fmt: skip
        if total > Decimal("50") and self.rng.random() < 0.3:
            first = money(total * Decimal("0.5"))
            parts = [first, total - first]
        else:
            parts = [total]
        return [
            {
                **base,
                "payment_id": self._next_payment_id(),
                "payment_method": self.rng.choice(PAYMENT_METHODS),
                "amount": amount,
                "status": "APPROVED",
            }
            for amount in parts
        ]

    def _make_return(self, original: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        sale_id = self._next_sale_id()
        ts = original["sale_ts"] + timedelta(days=self.rng.randint(1, 5))
        updated_at = ts + timedelta(minutes=5)
        source_lines = self._items_by_sale[original["sale_id"]]
        returned = self.rng.sample(source_lines, self.rng.randint(1, len(source_lines)))
        items = []
        for line, src in enumerate(returned, start=1):
            qty = -self.rng.randint(1, src["quantity"])
            gross = money(src["unit_price"] * qty)
            discount = money(src["discount_amount"] * Decimal(-qty) / Decimal(src["quantity"]))
            # Refund is net of the discount originally granted: line_total = q*p - sign(q)*d.
            items.append(
                {
                    "sale_id": sale_id,
                    "line_number": line,
                    "product_id": src["product_id"],
                    "quantity": qty,
                    "unit_price": src["unit_price"],
                    "discount_amount": discount,
                    "line_total": gross + discount,
                    "updated_at": updated_at,
                }
            )
        sale = {
            "sale_id": sale_id,
            "store_id": original["store_id"],
            "customer_id": original["customer_id"],
            "sale_ts": ts,
            "transaction_type": "RETURN",
            "status": "COMPLETED",
            "total_amount": sum((i["line_total"] for i in items), Decimal("0.00")),
            "currency": "BRL",
            "original_sale_id": original["sale_id"],
            "updated_at": updated_at,
        }
        return sale, items

    def _add_sale(
        self, sale: dict[str, Any], items: list[dict[str, Any]], payments: list[dict[str, Any]]
    ) -> None:
        self.world.sales[sale["sale_id"]] = sale
        for item in items:
            self.world.sale_items[business_key("sale_items", item)] = item
        for payment in payments:
            self.world.payments[payment["payment_id"]] = payment
        self._items_by_sale[sale["sale_id"]] = items
        self._payments_by_sale[sale["sale_id"]] = payments

    def _build_sales(self) -> None:
        self._customer_ids = list(self.world.customers)
        self._product_ids = list(self.world.products)
        self._store_ids = list(self.world.stores)
        self._store_weights = [
            1.0 / ((rank + 1) ** self.cfg.store_skew) for rank in range(len(self._store_ids))
        ]
        for _ in range(self.cfg.size("sales")):
            self._add_sale(*self._make_sale())
        completed = [s for s in self.world.sales.values() if s["status"] == "COMPLETED"]
        for original in self.rng.sample(completed, max(1, len(completed) // 25)):
            sale, items = self._make_return(original)
            self._add_sale(sale, items, self._make_payments(sale))
        plain_sales = [s for s in self.world.sales.values() if s["transaction_type"] == "SALE"]
        completed_sales = [s for s in plain_sales if s["status"] == "COMPLETED"]
        if self.cfg.has("financial_mismatch"):
            # Header total disagrees with its lines (payments follow the header): accepted, then warned.
            for sale in self.rng.sample(completed_sales, self.cfg.anomalies(len(completed_sales))):
                sale["total_amount"] += Decimal("10.00")
                self._payments_by_sale[sale["sale_id"]][-1]["amount"] += Decimal("10.00")
        if self.cfg.has("orphan_customers"):
            for i, sale in enumerate(self.rng.sample(plain_sales, self.cfg.anomalies(len(plain_sales))), 1):
                sale["customer_id"] = f"C9{i:06d}"

    def _build_inventory(self) -> None:
        products = list(self.world.products)[:MAX_INVENTORY_PRODUCTS]
        days = self.cfg.size("inventory_days")
        last_day = self.cfg.start_date + timedelta(days=self.cfg.days - 1)
        for store_id in self.world.stores:
            for offset in range(days):
                snapshot = last_day - timedelta(days=offset)
                for pid in products:
                    row = {
                        "store_id": store_id,
                        "product_id": pid,
                        "snapshot_date": snapshot,
                        "quantity_on_hand": self.rng.randint(0, 200),
                        "reorder_point": self.rng.randint(5, 30),
                        "updated_at": datetime.combine(snapshot, datetime.min.time()) + timedelta(hours=23),
                    }
                    self.world.inventory[business_key("inventory", row)] = row
        if self.cfg.has("negative_stock"):
            keys = list(self.world.inventory)
            for key in self.rng.sample(keys, self.cfg.anomalies(len(keys))):
                self.world.inventory[key]["quantity_on_hand"] = -self.rng.randint(1, 10)

    # ------------------------------------------------------------------ drop 1
    def drop1(self) -> Drop:
        drop = Drop()
        for entity in ENTITIES:
            drop.rows[entity] = [dict(row) for row in self.world.table(entity).values()]
        cfg = self.cfg
        sales_rows = drop.rows["sales"]

        if cfg.has("legacy_date_format"):
            # One store runs a legacy POS that emits dd/MM/yyyy timestamps: valid, normalized in Silver.
            legacy_store = next(iter(self.world.stores))
            for row in sales_rows:
                if row["store_id"] == legacy_store:
                    row["_format"] = "legacy"

        if cfg.has("duplicate_customers"):
            customers = drop.rows["customers"]
            for idx in sorted(
                self.rng.sample(range(len(customers)), cfg.anomalies(len(customers))), reverse=True
            ):
                duplicate = dict(customers[idx])
                customers.insert(idx + 1, duplicate)  # same file, adjacent: a legitimate Bronze duplicate
                drop.reject("customers", duplicate, "DUPLICATE_BUSINESS_KEY")

        if cfg.has("invalid_dates"):
            for bad_ts in self._pick_bad_timestamps(cfg.anomalies(len(sales_rows))):
                self._reject_whole_sale(drop, {"sale_ts": bad_ts[0]}, bad_ts[1])
            for _ in range(cfg.anomalies(len(drop.rows["customers"]))):
                customer = self._new_customer(self._dims_updated_at)
                customer["signup_date"] = "2024-13-45"
                drop.rows["customers"].append(customer)
                drop.reject("customers", customer, "INVALID_FORMAT:signup_date")

        if cfg.has("negative_amounts"):
            for _ in range(cfg.anomalies(len(sales_rows))):
                self._reject_whole_sale(drop, {"total_amount": Decimal("-25.00")}, "NEGATIVE_AMOUNT_FOR_SALE")
            for sale_id in self._sample_sale_ids(cfg.anomalies(len(drop.rows["sale_items"]))):
                line = self._extra_line(sale_id)
                line["unit_price"] = Decimal("-9.90")
                line["line_total"] = money(line["unit_price"] * line["quantity"])
                drop.rows["sale_items"].append(line)
                drop.reject("sale_items", line, "NEGATIVE_UNIT_PRICE")
            for sale_id in self._sample_sale_ids(cfg.anomalies(len(drop.rows["payments"]))):
                sale = self.world.sales[sale_id]
                payment = {
                    "payment_id": self._next_payment_id(),
                    "sale_id": sale_id,
                    "payment_method": "CASH",
                    "amount": Decimal("-5.00"),
                    "status": "APPROVED",
                    "paid_ts": sale["sale_ts"] + timedelta(minutes=1),
                    "updated_at": sale["updated_at"],
                }
                drop.rows["payments"].append(payment)
                drop.reject("payments", payment, "NEGATIVE_AMOUNT_FOR_PAYMENT")

        if cfg.has("orphan_products"):
            for i, sale_id in enumerate(
                self._sample_sale_ids(cfg.anomalies(len(drop.rows["sale_items"]))), 1
            ):
                line = self._extra_line(sale_id)
                line["product_id"] = f"P9{i:05d}"
                drop.rows["sale_items"].append(line)
                drop.reject("sale_items", line, "ORPHAN_PRODUCT")

        if cfg.has("incomplete_records"):
            for _ in range(cfg.anomalies(len(sales_rows))):
                self._reject_whole_sale(drop, {"store_id": None}, "MISSING_REQUIRED:store_id")
            for _ in range(cfg.anomalies(len(drop.rows["products"]))):
                product = self._new_product(self._dims_updated_at)
                product["product_name"] = None
                drop.rows["products"].append(product)
                drop.reject("products", product, "MISSING_REQUIRED:product_name")

        if cfg.has("malformed_rows"):
            for i in range(cfg.anomalies(len(sales_rows))):
                drop.raw_lines["sales"].append(f"T8{i:08d},BROKEN-ROW")
                drop.reject("sales", None, "MALFORMED_RECORD")
        return drop

    def _pick_bad_timestamps(self, count: int) -> list[tuple[str, str]]:
        options = [
            ("2026-02-30T10:00:00", "INVALID_FORMAT:sale_ts"),  # impossible calendar date
            ("not-a-date", "INVALID_FORMAT:sale_ts"),
            ("2099-01-01T00:00:00", "FUTURE_DATE:sale_ts"),
            ("2001-01-01T00:00:00", "DATE_OUT_OF_RANGE:sale_ts"),
        ]
        return [options[i % len(options)] for i in range(count)]

    def _reject_whole_sale(self, drop: Drop, overrides: dict[str, Any], reason: str) -> None:
        """An invalid sale header: the header is rejected and its lines/payments become orphans."""
        sale, items, payments = self._make_sale()
        sale.update(overrides)
        drop.rows["sales"].append(sale)
        drop.reject("sales", sale, reason)
        for item in items:
            drop.rows["sale_items"].append(item)
            drop.reject("sale_items", item, "ORPHAN_SALE")
        for payment in payments:
            drop.rows["payments"].append(payment)
            drop.reject("payments", payment, "ORPHAN_SALE")

    def _sample_sale_ids(self, count: int) -> list[str]:
        candidates = [s["sale_id"] for s in self.world.sales.values() if s["transaction_type"] == "SALE"]
        return self.rng.sample(candidates, min(count, len(candidates)))

    def _extra_line(self, sale_id: str) -> dict[str, Any]:
        """A line appended to a valid sale; it is rejected, so the sale total intentionally excludes it."""
        product = self.world.products[self.rng.choice(self._product_ids)]
        qty = 1
        self._extra_line_seq += 1
        return {
            "sale_id": sale_id,
            # Far above any real line number and unique, so it never collides with a valid line.
            "line_number": 1000 + self._extra_line_seq,
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": product["unit_price"],
            "discount_amount": Decimal("0.00"),
            "line_total": product["unit_price"],
            "updated_at": self.world.sales[sale_id]["updated_at"],
        }

    # ------------------------------------------------------------------ drop 2 (incremental)
    def drop2(self) -> tuple[Drop, dict[str, Any]]:
        cfg = self.cfg
        self.rng = random.Random(cfg.seed * 1000 + 2)
        drop = Drop()
        extras: dict[str, Any] = {
            "updated_customers": {},
            "stale_customers": {},
            "incompatible_customers": [],
        }
        drop2_ts = cfg.window_end + timedelta(days=10)

        if cfg.has("updates"):
            ids = list(self.world.customers)
            for cid in self.rng.sample(ids, max(1, len(ids) // 20)):
                customer = self.world.customers[cid]
                city, state = self.rng.choice([c for c in CITIES if c[0] != customer["city"]])
                customer.update({"city": city, "state": state, "updated_at": drop2_ts})
                if cfg.has("schema_change"):
                    customer["phone"] = None  # the new file layout no longer carries phone numbers
                drop.rows["customers"].append(dict(customer))
                extras["updated_customers"][cid] = city
            for pid in self.rng.sample(list(self.world.products), max(1, len(self.world.products) // 10)):
                product = self.world.products[pid]
                product["unit_price"] = money(product["unit_price"] * Decimal("1.10"))
                product["updated_at"] = drop2_ts
                drop.rows["products"].append(dict(product))
            returned = {s["original_sale_id"] for s in self.world.sales.values() if s["original_sale_id"]}
            completed = [
                s
                for s in self.world.sales.values()
                if s["status"] == "COMPLETED"
                and s["transaction_type"] == "SALE"
                and s["sale_id"] not in returned
            ]
            for sale in self.rng.sample(completed, max(1, len(completed) // 50)):
                sale["status"] = "CANCELLED"
                sale["updated_at"] = drop2_ts
                drop.rows["sales"].append(dict(sale))

        if cfg.has("stale_update"):
            # An out-of-order update older than what Silver already holds: must not overwrite it.
            untouched = [c for c in self.world.customers if c not in extras["updated_customers"]]
            for cid in self.rng.sample(untouched, max(1, len(untouched) // 50)):
                stale = dict(self.world.customers[cid])
                stale["city"] = "Cidade Desatualizada"
                stale["updated_at"] = stale["updated_at"] - timedelta(days=30)
                if cfg.has("schema_change"):
                    stale["phone"] = None
                drop.rows["customers"].append(stale)
                extras["stale_customers"][cid] = self.world.customers[cid]["city"]

        if cfg.has("schema_change"):
            for _ in range(max(1, cfg.size("customers") // 50)):
                customer = self._new_customer(drop2_ts)
                customer["phone"] = None
                self.world.customers[customer["customer_id"]] = customer
                drop.rows["customers"].append(dict(customer))

        if cfg.has("new_sales"):
            count = max(1, cfg.size("sales") // 5)
            late = round(count * 0.3) if cfg.has("late_batch") else 0
            for i in range(count):
                if i < late:
                    # Late batch: transactions from the first week of the window arriving weeks later.
                    sale, items, payments = self._make_sale(
                        self._random_ts(cfg.start_date, 7), updated_lag=timedelta(days=self.cfg.days + 5)
                    )
                else:
                    sale, items, payments = self._make_sale(
                        self._random_ts(cfg.start_date + timedelta(days=cfg.days), 5)
                    )
                self._add_sale(sale, items, payments)
                drop.rows["sales"].append(dict(sale))
                drop.rows["sale_items"].extend(dict(i) for i in items)
                drop.rows["payments"].extend(dict(p) for p in payments)

        if cfg.has("incompatible_schema"):
            for _ in range(3):
                extras["incompatible_customers"].append(self._new_customer(drop2_ts))
        return drop, extras


# ---------------------------------------------------------------------- serialization
def _fmt(value: Any, legacy: bool = False) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y %H:%M:%S") if legacy else value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    return str(value)


def to_csv(rows: list[dict[str, Any]], columns: list[str], raw_lines: list[str] | None = None) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        legacy = row.get("_format") == "legacy"
        writer.writerow([_fmt(row.get(c), legacy) for c in columns])
    for line in raw_lines or []:
        buffer.write(line + "\n")
    return buffer.getvalue().encode("utf-8")


def to_json_lines(rows: list[dict[str, Any]], columns: list[str]) -> bytes:
    def convert(value: Any, column: str) -> Any:
        if isinstance(value, Decimal):
            return float(value)  # JSON numbers; 2-decimal values round-trip exactly through repr
        if isinstance(value, datetime):
            # Payments arrive with an explicit UTC designator, exercising offset-aware parsing.
            suffix = "Z" if column == "paid_ts" else ""
            return value.strftime("%Y-%m-%dT%H:%M:%S") + suffix
        if isinstance(value, date):
            return value.isoformat()
        return value

    lines = [json.dumps({c: convert(row.get(c), c) for c in columns}, ensure_ascii=False) for row in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def to_parquet(rows: list[dict[str, Any]], entity: str) -> bytes:
    contract = ENTITIES[entity]
    arrow_types = {"int": pa.int32(), "money": pa.decimal128(18, 2), "date": pa.date32(),
                   "timestamp": pa.timestamp("us"), "boolean": pa.bool_()}  # fmt: skip
    fields = [pa.field(c.name, arrow_types.get(c.type.value, pa.string())) for c in contract.columns]
    schema = pa.schema(fields)
    table = pa.Table.from_pylist(
        [{c.name: row.get(c.name) for c in contract.columns} for row in rows], schema
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def _split(rows: list[dict[str, Any]], parts: int) -> list[list[dict[str, Any]]]:
    if not rows:
        return []
    parts = max(1, min(parts, len(rows)))
    size = -(-len(rows) // parts)
    return [rows[i : i + size] for i in range(0, len(rows), size)]


# ---------------------------------------------------------------------- expectations (test oracle)
def expectations(world: World, rejections: list[dict[str, Any]], tolerance: Decimal = CENT) -> dict[str, Any]:
    sales = world.sales
    items_by_sale: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in world.sale_items.values():
        items_by_sale[item["sale_id"]].append(item)
    paid: dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
    for payment in world.payments.values():
        if payment["status"] in {"APPROVED", "REFUNDED"}:
            paid[payment["sale_id"]] += payment["amount"]
    completed = [s for s in sales.values() if s["status"] == "COMPLETED"]
    total_mismatch = sum(
        1
        for s in completed
        if items_by_sale[s["sale_id"]]
        and abs(s["total_amount"] - sum(i["line_total"] for i in items_by_sale[s["sale_id"]])) > tolerance
    )
    payment_mismatch = sum(1 for s in completed if abs(paid[s["sale_id"]] - s["total_amount"]) > tolerance)
    emails = Counter(c["email"] for c in world.customers.values())
    fact_items = [i for s in completed for i in items_by_sale[s["sale_id"]]]
    return {
        "silver_counts": {entity: len(world.table(entity)) for entity in ENTITIES},
        "rejections": rejections,
        "rejection_counts": dict(Counter(r["entity"] for r in rejections)),
        "warnings": {
            "SLV-SALES-TOTAL-MISMATCH": total_mismatch,
            "SLV-PAYMENTS-TOTAL-MISMATCH": payment_mismatch,
            "SLV-SALES-RI-CUSTOMER": sum(
                1 for s in sales.values() if s["customer_id"] and s["customer_id"] not in world.customers
            ),
            "SLV-CUSTOMERS-DUP-EMAIL": sum(1 for c in world.customers.values() if emails[c["email"]] > 1),
            "SLV-INVENTORY-NEG-STOCK": sum(1 for r in world.inventory.values() if r["quantity_on_hand"] < 0),
            "SLV-PRODUCTS-NEG-MARGIN": sum(
                1
                for p in world.products.values()
                if p["unit_cost"] is not None and p["unit_cost"] > p["unit_price"]
            ),
        },
        "gold": {
            "fact_rows": len(fact_items),
            "net_revenue": str(sum((i["line_total"] for i in fact_items), Decimal("0.00"))),
            "orders": sum(1 for s in completed if s["transaction_type"] == "SALE"),
        },
    }


# ---------------------------------------------------------------------- entry point
def generate(store: ObjectStore, landing_uri: str, config: GeneratorConfig, drop: int = 1) -> dict[str, Any]:
    """Write one drop of synthetic files to ``landing_uri`` and return its manifest.

    The manifest is also written to ``<landing>/_manifests/drop_NNN.json`` (ignored by ingestion).
    """
    if drop not in (1, 2):
        raise ValueError("drop must be 1 (initial load) or 2 (incremental changes)")
    generator = RetailGenerator(config)
    generator.build_world()
    first = generator.drop1()
    files: list[dict[str, Any]] = []

    def write(entity: str, name: str, data: bytes, rows: int, kind: str = "data") -> str:
        uri = join_uri(landing_uri, entity, name)
        store.put_bytes(uri, data)
        files.append({"entity": entity, "uri": uri, "rows": rows, "kind": kind})
        return uri

    def write_entity(
        entity: str, rows: list[dict[str, Any]], tag: str, raw: list[str] | None = None
    ) -> list[bytes]:
        contract = ENTITIES[entity]
        columns = list(contract.column_names)
        payloads = []
        for part, chunk in enumerate(_split(rows, config.files_per_entity), start=1):
            extra = raw if part == 1 else None
            fmt = contract.source_format.value
            if fmt == "csv":
                data = to_csv(chunk, columns, extra)
            elif fmt == "json":
                data = to_json_lines(chunk, columns)
            else:
                data = to_parquet(chunk, entity)
            write(entity, f"{entity}_{tag}_p{part:03d}.{fmt}", data, len(chunk) + len(extra or []))
            payloads.append(data)
        return payloads

    if drop == 1:
        for entity in ENTITIES:
            write_entity(entity, first.rows[entity], "d001", first.raw_lines.get(entity))
        expected = expectations(generator.world, first.rejections, config_tolerance())
        expected["duplicate_files"] = []
        expected["quarantined_files"] = []
    else:
        second, extras = generator.drop2()
        sales_d001 = None
        for entity in ENTITIES:
            rows = second.rows.get(entity, [])
            if entity == "customers" and config.has("schema_change") and rows:
                # Reordered columns, `phone` removed, `loyalty_tier` added.
                columns = ["updated_at", "customer_id", "email", "last_name", "first_name", "city", "state",
                           "signup_date", "loyalty_tier"]  # fmt: skip
                for row in rows:
                    row["loyalty_tier"] = generator.rng.choice(("GOLD", "SILVER", "BRONZE"))
                write(entity, "customers_d002_v2_p001.csv", to_csv(rows, columns), len(rows))
            elif rows:
                write_entity(entity, rows, "d002")
        if config.has("incompatible_schema"):
            columns = ["first_name", "last_name", "email", "city", "state", "signup_date", "updated_at"]
            write("customers", "customers_d002_legacy.csv",
                  to_csv(extras["incompatible_customers"], columns), 3, kind="incompatible")  # fmt: skip
        if config.has("repeated_file"):
            sales_d001 = to_csv(
                _split(first.rows["sales"], config.files_per_entity)[0],
                list(ENTITIES["sales"].column_names),
                first.raw_lines.get("sales"),
            )
            write("sales", "sales_d002_resend.csv", sales_d001, -1, kind="duplicate")
        expected = expectations(generator.world, [], config_tolerance())
        expected["duplicate_files"] = [f["uri"] for f in files if f["kind"] == "duplicate"]
        expected["quarantined_files"] = [f["uri"] for f in files if f["kind"] == "incompatible"]
        expected["updated_customers"] = extras["updated_customers"]
        expected["stale_customers"] = extras["stale_customers"]

    manifest = {
        "drop": drop,
        "seed": config.seed,
        "scale": config.scale,
        "sizes": {k: config.size(k) for k in SCALES[config.scale]},
        "files_per_entity": config.files_per_entity,
        "error_rate": config.error_rate,
        "store_skew": config.store_skew,
        "scenarios": list(config.scenarios),
        "files": files,
        "expected": expected,
    }
    store.put_bytes(
        join_uri(landing_uri, "_manifests", f"drop_{drop:03d}.json"),
        json.dumps(manifest, indent=2, default=str).encode("utf-8"),
    )
    return manifest


def config_tolerance() -> Decimal:
    return CENT
