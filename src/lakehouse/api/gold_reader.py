"""Read-only access to *published* Gold versions through delta-rs (no JVM, no Spark job per request)."""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from lakehouse.common.storage import is_s3
from lakehouse.config import Settings


class GoldReader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _storage_options(self) -> dict[str, str] | None:
        storage = self.settings.storage
        if not is_s3(storage.lake_uri):
            return None
        options = {
            "AWS_REGION": storage.s3_region,
            "AWS_ACCESS_KEY_ID": storage.s3_access_key or "",
            "AWS_SECRET_ACCESS_KEY": storage.s3_secret_key or "",
        }
        if storage.s3_endpoint_url:
            options["AWS_ENDPOINT_URL"] = storage.s3_endpoint_url
            options["AWS_ALLOW_HTTP"] = str(storage.s3_endpoint_url.startswith("http://")).lower()
        if storage.s3_path_style:
            options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false"
        return options

    def table(self, name: str, version: int) -> Any:
        return _open(self.settings.table_uri("gold", name), version, _freeze(self._storage_options()))

    def daily_sales(
        self, version: int, start: date | None = None, end: date | None = None, store_id: str | None = None
    ) -> pa.Table:
        expression = None
        for condition in (
            pc.field("sale_date") >= pa.scalar(start) if start else None,
            pc.field("sale_date") <= pa.scalar(end) if end else None,
            pc.field("store_id") == pa.scalar(store_id) if store_id else None,
        ):
            if condition is not None:
                expression = condition if expression is None else expression & condition
        return self.table("agg_daily_sales", version).to_pyarrow_table(filters=expression)


def _freeze(options: dict[str, str] | None) -> tuple[tuple[str, str], ...] | None:
    return tuple(sorted(options.items())) if options else None


@lru_cache(maxsize=32)
def _open(uri: str, version: int, options: tuple[tuple[str, str], ...] | None) -> Any:
    # A published version is immutable, so caching the opened snapshot is safe.
    from deltalake import DeltaTable

    return DeltaTable(uri, version=version, storage_options=dict(options) if options else None)


MONEY_FIELDS = ("gross_revenue", "discount_total", "returns_amount", "net_revenue", "gross_margin")
COUNT_FIELDS = ("orders", "returns", "items_sold")


def summarize(table: pa.Table, group_by: str) -> list[dict[str, Any]]:
    """Roll daily/store rows up to the requested grain; ratios are recomputed, never averaged."""
    keys = {"day": ["sale_date"], "store": ["store_id"], "day_store": ["sale_date", "store_id"], "total": []}[
        group_by
    ]
    aggregations = [(f, "sum") for f in (*COUNT_FIELDS, *MONEY_FIELDS)]
    if keys:
        grouped = table.group_by(keys).aggregate(aggregations)
        rows = grouped.rename_columns([c.removesuffix("_sum") for c in grouped.column_names]).to_pylist()
    else:
        rows = [{f: pc.sum(table[f]).as_py() if table.num_rows else None for f, _ in aggregations}]
    for row in rows:
        for f in COUNT_FIELDS:
            row[f] = int(row[f] or 0)
        for f in MONEY_FIELDS:
            row[f] = row[f] if row[f] is not None else 0
        sales_net = row["net_revenue"] - row["returns_amount"]
        row["avg_ticket"] = round(sales_net / row["orders"], 2) if row["orders"] else None
    rows.sort(key=lambda r: tuple(str(r[k]) for k in keys))
    return rows
