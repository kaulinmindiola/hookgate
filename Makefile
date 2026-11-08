.PHONY: install up down stack logs migrate run worker simulate demo format lint types test test-integration chaos smoke check

SCENARIO ?= chaos
SEED ?= 7

install:
	uv sync

up:
	docker compose up -d --wait postgres redis rabbitmq

down:
	docker compose down

stack:
	docker compose up --build -d

logs:
	docker compose logs -f api worker beat

migrate:
	uv run alembic upgrade head

run:
	uv run uvicorn hookgate.api.main:create_app --factory --host 127.0.0.1 --port 8000

worker:
	uv run celery -A hookgate.worker worker --pool threads --concurrency 4 --loglevel INFO

simulate:
	uv run python -m simulator run --scenario $(SCENARIO) --seed $(SEED)

demo:
	docker compose --profile demo run --rm simulator run --scenario $(SCENARIO) --seed $(SEED)

format:
	uv run ruff format .
	uv run ruff check --fix .
	uv run ruff format .

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run python scripts/check_domain_purity.py

types:
	uv run mypy

test:
	uv run pytest -m "not chaos and not integration" --cov=hookgate.domain --cov-report=term-missing --cov-fail-under=80

test-integration:
	uv run pytest -m integration

chaos:
	uv run pytest -m chaos -v

smoke:
	docker compose up --build -d
	docker compose --profile demo run --rm simulator run --scenario chaos --seed 7

check: lint types test test-integration