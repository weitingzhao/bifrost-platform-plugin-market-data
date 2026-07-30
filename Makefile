.PHONY: install-dev test lint db-init db-init-dry verify-market-data

install-dev:
	pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check src tests scripts

db-init:
	python scripts/init_schema.py

db-init-dry:
	python scripts/init_schema.py --dry-run

verify-market-data:
	bash scripts/verify-market-data.sh
