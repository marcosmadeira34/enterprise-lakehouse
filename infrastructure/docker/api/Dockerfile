# syntax=docker/dockerfile:1
# Query API: FastAPI + delta-rs. No JVM and no Spark: requests never start processing jobs.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    TZ=UTC \
    LAKEHOUSE_CONFIG=/opt/lakehouse/config/lakehouse.yaml

WORKDIR /opt/lakehouse
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-install-project --no-dev --extra api
COPY src ./src
COPY config ./config
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --extra api --no-editable

RUN useradd --system --uid 10001 lakehouse
USER lakehouse
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "lakehouse.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
