"""Query API. Decoupled from processing: operational history comes from the metadata repository,
analytics from the latest *published* Gold versions. No Spark job runs inside a request.

No ``from __future__ import annotations`` here: FastAPI must evaluate the dependency annotations,
which are defined inside the app factory.
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from lakehouse.api.gold_reader import GoldReader, summarize
from lakehouse.config import Settings
from lakehouse.metadata.repository import MetadataRepository
from lakehouse.observability.logging import configure_logging, log_event

log = logging.getLogger("lakehouse.api")

Limit = Annotated[int, Query(ge=1, le=200, description="page size")]
Offset = Annotated[int, Query(ge=0, description="rows to skip")]


class Page(BaseModel):
    items: list[dict[str, Any]]
    total: int
    limit: int
    offset: int


def create_app(
    settings: Settings | None = None, repo: MetadataRepository | None = None, gold: GoldReader | None = None
) -> FastAPI:
    settings = settings or Settings.load()
    repo = repo or MetadataRepository.from_url(settings.metadata.url)
    gold = gold or GoldReader(settings)
    app = FastAPI(
        title="Enterprise Lakehouse API",
        version="0.1.0",
        description="Run history, quality results and published Gold analytics.",
    )

    def get_repo() -> MetadataRepository:
        return repo

    Repo = Annotated[MetadataRepository, Depends(get_repo)]  # noqa: N806 - a type alias

    @app.exception_handler(Exception)
    async def _unhandled(_request: Request, exc: Exception) -> JSONResponse:
        log_event(log, "api_error", logging.ERROR, error_type=type(exc).__name__, error=str(exc))
        return JSONResponse(status_code=500, content={"detail": "internal error"})

    @app.get("/health")
    def health(repo: Repo) -> JSONResponse:
        checks: dict[str, Any] = {}
        try:
            repo.ping()
            checks["metadata"] = "ok"
        except Exception as exc:
            checks["metadata"] = f"error: {type(exc).__name__}"
            return JSONResponse(status_code=503, content={"status": "unavailable", "checks": checks})
        publication = repo.latest_publication()
        checks["gold_publication"] = publication["id"] if publication else None
        status = "ok" if publication else "degraded"  # serving history works; analytics not yet
        return JSONResponse(status_code=200, content={"status": status, "checks": checks})

    @app.get("/runs", response_model=Page)
    def list_runs(
        repo: Repo,
        limit: Limit = 50,
        offset: Offset = 0,
        pipeline: str | None = None,
        status: Literal["RUNNING", "SUCCEEDED", "FAILED", "ABANDONED"] | None = None,
    ) -> Page:
        items, total = repo.list_runs(limit, offset, pipeline, status)
        return Page(items=items, total=total, limit=limit, offset=offset)

    @app.get("/runs/{run_id}")
    def get_run(run_id: str, repo: Repo) -> dict[str, Any]:
        run = repo.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"run {run_id!r} not found")
        return run

    @app.get("/quality/results", response_model=Page)
    def quality_results(
        repo: Repo,
        limit: Limit = 100,
        offset: Offset = 0,
        run_id: str | None = None,
        batch_id: str | None = None,
        rule_id: str | None = None,
        table_name: str | None = None,
        status: Literal["PASSED", "WARNED", "FAILED", "ERROR"] | None = None,
        severity: Literal["BLOCKING", "WARNING"] | None = None,
    ) -> Page:
        items, total = repo.list_quality_results(
            limit, offset, run_id, batch_id, status, severity, rule_id, table_name
        )
        return Page(items=items, total=total, limit=limit, offset=offset)

    @app.get("/analytics/sales-summary")
    def sales_summary(
        repo: Repo,
        start_date: date | None = None,
        end_date: date | None = None,
        store_id: str | None = None,
        group_by: Literal["day", "store", "day_store", "total"] = "day",
        limit: Limit = 100,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        if start_date and end_date and start_date > end_date:
            raise HTTPException(status_code=422, detail="start_date must not be after end_date")
        publication = repo.latest_publication()
        if publication is None:
            raise HTTPException(status_code=503, detail="no published Gold version yet")
        version = int(publication["table_versions"]["gold.agg_daily_sales"])
        rows = summarize(
            gold.daily_sales(version, start_date, end_date, store_id.upper() if store_id else None), group_by
        )
        return {
            "items": [_jsonable(r) for r in rows[offset : offset + limit]],
            "total": len(rows),
            "limit": limit,
            "offset": offset,
            "group_by": group_by,
            "source": {
                "table": "gold.agg_daily_sales",
                "delta_version": version,
                "publication_id": publication["id"],
                "published_at": publication["created_at"],
                "metric_definitions": "net_revenue = SUM(net_amount) of COMPLETED lines; avg_ticket = "
                "(net_revenue - returns_amount) / orders",
            },
        }

    @app.get("/metrics", include_in_schema=False)
    def metrics(repo: Repo) -> Response:
        from lakehouse.observability.metrics import render_metrics

        body, content_type = render_metrics(repo)
        return Response(content=body, media_type=content_type)

    return app


def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
    return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in row.items()}


def main() -> None:  # pragma: no cover - process entry point
    import uvicorn

    configure_logging()
    uvicorn.run(create_app(), host="0.0.0.0", port=8000, log_config=None)
