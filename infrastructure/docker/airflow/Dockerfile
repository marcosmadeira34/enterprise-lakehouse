# syntax=docker/dockerfile:1
# Airflow 3 + the lakehouse processing runtime (Java 17, PySpark, Delta) in an isolated virtualenv.
#
# Tasks run `/opt/lakehouse/venv/bin/lakehouse ...` as a subprocess (one JVM per task, Spark in local
# mode inside the worker). The lakehouse venv never shares packages with Airflow's own environment.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    TZ=UTC

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/lakehouse/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    # Base interpreter of the official image (Airflow itself lives in a venv at /home/airflow/.local).
    UV_PYTHON=/usr/python/bin/python3.12

WORKDIR /opt/lakehouse/app
# Dependencies first (cached layer), then the project itself.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-install-project --no-dev --extra spark
COPY src ./src
COPY config ./config
COPY scripts/prefetch_spark_jars.py ./scripts/
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --extra spark --no-editable

# Resolve Delta and hadoop-aws jars at build time: tasks then start without network access to Maven.
ENV LAKEHOUSE_CONFIG=/opt/lakehouse/app/config/lakehouse.yaml \
    LAKEHOUSE__SPARK__IVY_DIR=/opt/lakehouse/ivy \
    PYSPARK_PYTHON=/opt/lakehouse/venv/bin/python
# Only the Ivy directory must be writable at run time (Ivy writes resolution reports); the venv stays
# root-owned and read-only, and chown-ing it would duplicate ~600 MB into a new layer.
RUN /opt/lakehouse/venv/bin/python scripts/prefetch_spark_jars.py \
    && chown -R airflow:0 /opt/lakehouse/ivy

USER airflow
WORKDIR /opt/airflow
