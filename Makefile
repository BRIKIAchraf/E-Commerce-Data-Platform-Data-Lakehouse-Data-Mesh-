# Makefile for E-commerce Data Platform automation

.PHONY: up down restart build dbt-run dbt-test spark-submit test clean help

help:
	@echo "E-commerce Data Platform Makefile Commands:"
	@echo "  make up            - Launch all services via docker-compose"
	@echo "  make down          - Stop and clean up all docker-compose services"
	@echo "  make build         - Build custom docker images (Spark, Flink, API)"
	@echo "  make dbt-run       - Execute dbt transformations on Snowflake"
	@echo "  make dbt-test      - Run dbt quality tests"
	@echo "  make test          - Run PyTest unit/integration tests"
	@echo "  make clean         - Clean up cached files and temporary directories"

up:
	docker-compose up -d

down:
	docker-compose down -v

restart: down up

build:
	docker build -t custom-spark -f infra/docker/Dockerfile.spark .
	docker build -t custom-flink -f infra/docker/Dockerfile.flink .
	docker build -t custom-api -f infra/docker/Dockerfile.api .

dbt-run:
	cd warehouse/dbt && dbt run --profiles-dir .

dbt-test:
	cd warehouse/dbt && dbt test --profiles-dir .

test:
	pytest tests/ -v

clean:
	rm -rf .pytest_cache/
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
