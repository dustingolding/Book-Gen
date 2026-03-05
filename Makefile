.PHONY: install lint test run-flow init-db

install:
	pip install -r requirements.txt

lint:
	ruff check app tests


test:
	pytest -q

init-db:
	python -m app.cli init-db

run-flow:
	python -m app.cli run-flow --run-date $$(date -u +%F)
