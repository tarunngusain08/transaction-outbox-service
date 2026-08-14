SHELL := /bin/bash
.DEFAULT_GOAL := help

MVNW := ./mvnw
MAVEN_FLAGS ?= --batch-mode --no-transfer-progress
IMAGE_NAME ?= transaction-outbox-service:local
APP_PORT ?= 8080

.PHONY: help build run stop reset clean unit-test integration-test test lint traffic check

help: ## Show available commands.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the executable JAR and local container image.
	$(MVNW) $(MAVEN_FLAGS) clean package -DskipUnitTests=true
	docker build --tag $(IMAGE_NAME) .

run: ## Start the application, PostgreSQL, and Kafka.
	docker compose up --detach --build --wait

stop: ## Stop the local stack and keep its data volumes.
	docker compose down

reset: ## Stop the stack and remove disposable local data volumes.
	docker compose down --volumes

clean: ## Remove Maven build output.
	$(MVNW) $(MAVEN_FLAGS) clean

unit-test: ## Run focused unit tests.
	$(MVNW) $(MAVEN_FLAGS) clean test -DskipIntegrationTests=true

integration-test: ## Run the PostgreSQL/Kafka integration test.
	$(MVNW) $(MAVEN_FLAGS) clean verify -DskipUnitTests=true -Djacoco.skip=true

test: ## Run all tests and enforce coverage.
	$(MVNW) $(MAVEN_FLAGS) clean verify

lint: ## Run Java style checks and validate the traffic script.
	$(MVNW) $(MAVEN_FLAGS) checkstyle:check
	python3 -m py_compile scripts/traffic_simulator.py

traffic: run ## Send success/failure traffic and confirm DB/Kafka delivery.
	python3 scripts/traffic_simulator.py --base-url http://localhost:$(APP_PORT)

check: lint test build ## Run the complete local quality gate.
