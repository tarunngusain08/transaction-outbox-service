# Transaction Outbox Service

A small Spring Boot service that stores payment records in PostgreSQL, publishes
`TRANSACTION_CREATED` events to Kafka, and normalizes a legacy JSON format into
the create-transaction schema.

## Design

`POST /api/v1/transactions` saves a transaction and a `PENDING` outbox row in one
PostgreSQL transaction. A scheduled publisher sends the stored JSON event to the
`payments.transactions.created` Kafka topic and marks the row `PUBLISHED` after
Kafka acknowledges it. Failed sends remain `PENDING` and are retried with a
short backoff.

The outbox keeps the database write and event intent atomic. Kafka delivery is
at least once, so a real consumer should deduplicate using `eventId`.

See [architecture](docs/architecture.md), [use cases](docs/use-cases.md), and
[sequence diagrams](docs/sequence-diagrams.md) for the compact design model.

## Prerequisites

- Java 21+
- Docker with Docker Compose
- GNU Make
- Python 3, only for the traffic smoke test

## Build, run, and test

Each workflow has one command:

```bash
make build             # Build the executable JAR and container image
make run               # Start Spring Boot, PostgreSQL, and Kafka
make test              # Run unit + integration tests and coverage checks
make unit-test         # Unit tests only
make integration-test  # PostgreSQL/Kafka Testcontainers test only
make lint              # Checkstyle and Python syntax
make traffic           # Mixed HTTP traffic plus DB/outbox/Kafka verification
make stop              # Stop containers and retain local volumes
```

The Flyway history was intentionally squashed for this take-home project. If a
local volume came from an older revision, recreate disposable local data before
running the service:

```bash
make reset
make run
```

## Create a transaction

Amounts use integer minor units. For INR, `150000` means INR 1,500.00.

```bash
curl -i -X POST http://localhost:8080/api/v1/transactions \
  -H 'Content-Type: application/json' \
  --data @examples/create-transaction.json
```

A valid request returns `201 Created`. Reusing an existing
`externalReference` returns `409 Conflict`.

## Normalize a legacy record

```bash
curl -sS -X POST http://localhost:8080/api/v1/transactions/normalize \
  -H 'Content-Type: application/json' \
  --data @examples/legacy-transaction.json
```

The response uses the create endpoint's schema. It converts decimal major units
to integer minor units, maps `DR`/`CR`, flattens payer/payee accounts, converts
the legacy timestamp to UTC, and moves IFSC codes and remarks into `metadata`.
Normalization itself does not write to PostgreSQL or Kafka.

## Inspect the local pipeline

```bash
# PostgreSQL transactions and delivery state
docker compose exec postgres psql -U payments -d payments -c \
  "SELECT t.external_reference, o.status, o.retry_count FROM transactions t JOIN outbox_events o ON o.aggregate_id = t.id ORDER BY t.created_at DESC;"

# Kafka event values
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic payments.transactions.created \
  --from-beginning
```

## Scope

This exercise records transaction data and emits creation events. It does not
authorize payments, move money, maintain a ledger, settle funds, reconcile
accounts, authenticate callers, or provide production high availability.
