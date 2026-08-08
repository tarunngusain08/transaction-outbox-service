SHELL := /bin/bash
.DEFAULT_GOAL := help

MVNW := ./mvnw
MAVEN_FLAGS ?= --batch-mode --no-transfer-progress
IMAGE_NAME ?= transaction-outbox-service:local
LOAD_REQUESTS ?= 100
LOAD_CONCURRENCY ?= 10

.PHONY: help build run stop logs clean unit-test integration-test test lint coverage traffic load-test check

help: ## Show the available local workflows.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the executable JAR and local application image.
	$(MVNW) $(MAVEN_FLAGS) clean package -DskipUnitTests=true
	docker build --tag $(IMAGE_NAME) .

run: ## Build and start the complete application, PostgreSQL, and Kafka stack.
	docker compose up --detach --build --wait

stop: ## Stop the local stack while retaining PostgreSQL data.
	docker compose down

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

coverage: ## Run all tests and enforce the JaCoCo coverage threshold.
	$(MVNW) $(MAVEN_FLAGS) clean verify

traffic: run ## Send mixed success/failure traffic and verify database/Kafka delivery.
	python3 scripts/traffic_simulator.py --mode smoke

load-test: run ## Run the configurable concurrent local load scenario.
	python3 scripts/traffic_simulator.py --mode load --requests $(LOAD_REQUESTS) --concurrency $(LOAD_CONCURRENCY)

check: lint test build ## Run every local quality gate used before review.
