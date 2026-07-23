.PHONY: install run test lint format typecheck check clean

install:
	uv sync --dev

run:
	uv run intelipump-fdc

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy src

check: lint typecheck test
