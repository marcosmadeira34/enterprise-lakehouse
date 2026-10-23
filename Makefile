# Standard commands. Run `make help` for the list.
SHELL := /bin/bash
COMPOSE := docker compose
UV := uv
LAKEHOUSE_DOCKER := $(COMPOSE) exec -T airflow-scheduler /opt/lakehouse/venv/bin/lakehouse

.DEFAULT_GOAL := help
.PHONY: help env install lint typecheck test test-fast test-spark test-resilience test-integration test-all \
        up down restart reset logs ps dag-check demo pipeline demo-drop2 status benchmark clean

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-18s %s\n", $$1, $$2}'

env: ## Create .env with random local secrets (never overwrites)
	python3 scripts/make_env.py

install: ## Create the local virtualenv with every extra (Java 17 required for Spark)
	$(UV) sync --all-extras

lint: ## Ruff lint + format check
	$(UV) run ruff check src tests dags scripts
	$(UV) run ruff format --check src tests dags scripts

typecheck: ## mypy on the package
	$(UV) run mypy

test-fast: ## Unit tests without Spark (seconds)
	$(UV) run pytest tests/unit -m "not spark"

test-spark: ## Unit + local pipeline tests with Spark (no Docker)
	$(UV) run pytest tests/unit tests/integration -m "spark and not integration"

test-resilience: ## Failure-injection and re-execution scenarios (Spark, no Docker)
	$(UV) run pytest tests/resilience

test: ## Everything that does not need Docker services
	$(UV) run pytest -m "not integration"

test-integration: ## Tests against the Docker services (PostgreSQL + MinIO); run `make up` first
	set -a && source .env && set +a && $(UV) run pytest -m integration tests/integration -o addopts="-ra"

test-all: test test-integration ## All test levels

up: env ## Build and start the stack
	$(COMPOSE) up -d --build
	@echo "Airflow  http://localhost:$${AIRFLOW_PORT:-18088}   API http://localhost:$${API_PORT:-18000}/docs"
	@echo "Grafana  http://localhost:$${GRAFANA_PORT:-13000}   MinIO console http://localhost:$${MINIO_CONSOLE_PORT:-19011}"

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

restart: ## Restart all services
	$(COMPOSE) restart

reset: ## Stop the stack and DELETE all volumes (metadata, lake, logs)
	$(COMPOSE) down -v

logs: ## Follow logs
	$(COMPOSE) logs -f --tail=100

ps: ## Service status
	$(COMPOSE) ps

dag-check: ## Validate DAG imports and structure inside the Airflow container
	$(COMPOSE) exec -T airflow-scheduler python - < scripts/check_dags.py

demo: ## Generate drop 1 in MinIO and run the full pipeline inside the Airflow container
	$(LAKEHOUSE_DOCKER) generate --drop 1 --scale small
	$(LAKEHOUSE_DOCKER) run-all

demo-drop2: ## Generate the incremental drop and run the pipeline again
	$(LAKEHOUSE_DOCKER) generate --drop 2 --scale small
	$(LAKEHOUSE_DOCKER) run-all

pipeline: ## Trigger the lakehouse_pipeline DAG through Airflow
	$(COMPOSE) exec -T airflow-scheduler airflow dags unpause lakehouse_pipeline
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger lakehouse_pipeline

status: ## Layer totals, file states, last runs (inside Docker)
	$(LAKEHOUSE_DOCKER) status

benchmark: ## Local benchmark (see docs/benchmark.md for parameters)
	$(UV) run python scripts/benchmark.py --scale small

clean: ## Remove local data and caches
	rm -rf data .pytest_cache .mypy_cache .ruff_cache
