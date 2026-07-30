.PHONY: install-dev test lint db-init

install-dev:
	pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check src tests

db-init:
	@echo "db-init stub — implemented in P1 (schema/ddl.py + scripts)"
	@exit 1
