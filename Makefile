.PHONY: install format lint types test chaos check

install:
	uv sync

format:
	uv run ruff check --fix .
	uv run ruff format .

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run python scripts/check_domain_purity.py

types:
	uv run mypy

test:
	uv run pytest -m "not chaos" --cov=hookgate.domain --cov-report=term-missing --cov-fail-under=80

chaos:
	@echo "No chaos tests yet (they arrive in phase F11)."

check: lint types test