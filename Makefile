SHELL := /bin/bash
.DEFAULT_GOAL := help

MVNW := ./mvnw
MAVEN_FLAGS ?= --batch-mode --no-transfer-progress
IMAGE_NAME ?= transaction-outbox-service:local
APP_PORT ?= 8080
LOAD_REQUESTS ?= 100
LOAD_CONCURRENCY ?= 10
LOAD_MIN_THROUGHPUT ?= 1
LOAD_MAX_P95_MS ?= 3000
CHROME_BIN ?= $(shell command -v google-chrome 2>/dev/null || command -v chromium 2>/dev/null || command -v chromium-browser 2>/dev/null || { test -x "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" && echo "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"; })

.PHONY: help build run stop reset logs clean unit-test integration-test test lint docs-lint coverage migration-preflight traffic load-test check

help: ## Show the available local workflows.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the executable JAR and local application image.
	$(MVNW) $(MAVEN_FLAGS) clean package -DskipUnitTests=true
	docker build --tag $(IMAGE_NAME) .

run: ## Build and start the complete application, PostgreSQL, and Kafka stack.
	docker compose up --detach --build --wait

stop: ## Stop the local stack while retaining PostgreSQL and Kafka data.
	docker compose down

reset: ## Stop the local stack and delete its PostgreSQL and Kafka volumes.
	docker compose down --volumes

logs: ## Follow application logs from the local stack.
	docker compose logs --follow app

clean: ## Remove Maven build output.
	$(MVNW) $(MAVEN_FLAGS) clean

unit-test: ## Run fast unit tests only.
	$(MVNW) $(MAVEN_FLAGS) test -DskipIntegrationTests=true

integration-test: ## Run PostgreSQL/Kafka Testcontainers integration tests only.
	$(MVNW) $(MAVEN_FLAGS) verify -DskipUnitTests=true -Djacoco.skip=true

test: ## Run all unit and integration tests.
	$(MVNW) $(MAVEN_FLAGS) clean verify

lint: ## Run Java source checks.
	$(MVNW) $(MAVEN_FLAGS) checkstyle:check
	python3 -m py_compile scripts/traffic_simulator.py
	python3 -m py_compile scripts/validate_docs.py

docs-lint: ## Reproduce Markdown, link, traceability, and Mermaid checks.
	PUPPETEER_SKIP_DOWNLOAD=true npm ci --no-audit --no-fund
	PUPPETEER_EXECUTABLE_PATH="$(CHROME_BIN)" npm run docs:check

coverage: ## Run all tests and enforce the JaCoCo coverage threshold.
	$(MVNW) $(MAVEN_FLAGS) clean verify

migration-preflight: ## Read-only V2 identity/currency worklist before applying V3.
	docker compose up --detach --wait postgres
	docker compose exec -T postgres psql -X -U payments -d payments < scripts/sql/preflight_v3_transaction_identity.sql

traffic: run ## Send mixed success/failure traffic and verify database/Kafka delivery.
	python3 scripts/traffic_simulator.py --mode smoke --base-url http://localhost:$(APP_PORT)

load-test: run ## Run the configurable concurrent local load scenario.
	python3 scripts/traffic_simulator.py --mode load --base-url http://localhost:$(APP_PORT) --requests $(LOAD_REQUESTS) --concurrency $(LOAD_CONCURRENCY) --min-throughput $(LOAD_MIN_THROUGHPUT) --max-p95-ms $(LOAD_MAX_P95_MS)

check: lint docs-lint test build ## Run repository-static checks, tests, and build gates.
