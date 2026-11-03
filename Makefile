.PHONY: install up down migrate run format lint types test test-integration chaos check

install:
	uv sync

up:
	docker compose up -d --wait postgres redis rabbitmq

down:
	docker compose down

migrate:
	uv run alembic upgrade head

run:
	uv run uvicorn hookgate.api.main:create_app --factory --host 127.0.0.1 --port 8000

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
	@echo "No chaos tests yet (they arrive in phase F11)."

check: lint types test test-integration