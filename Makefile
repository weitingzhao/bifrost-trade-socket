.PHONY: install install-dev test test-ib lint clean

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest -m 'not ib'

test-ib:
	pytest -m ib

lint:
	ruff check src/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
