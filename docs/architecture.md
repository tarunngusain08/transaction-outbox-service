# Architecture notes

## Purpose and boundaries

This is a transaction-ingestion and event-publication exercise. It accepts a
canonical transaction, stores it in PostgreSQL, and eventually publishes a
transaction-created event to Kafka. It also adapts a legacy bank-style input to
the canonical create request. It does not authorize, move, settle, reconcile,
refund, or account for money.

The [README](../README.md) is the reviewer entry point. This document records
durable system boundaries; detailed field rules live in
[ADR 0001](decisions/0001-v1-ingestion-contract.md), versioning in
[ADR 0002](decisions/0002-api-event-versioning.md), and supported migration
operations in the [V3](migrations/v3-identity-migration.md) and
[V7](migrations/v7-event-quarantine.md) runbooks.

## Current boundaries and ownership

| Boundary | Current behavior |
| --- | --- |
| HTTP API | Active routes are `POST /api/v2/transactions`, `GET /api/v2/transactions/{transactionId}`, and `POST /api/v2/transactions/normalize`; V1 routes are explicitly retired with `410 Gone` |
| Transaction record | The client supplies canonical business fields; the service owns transaction ID, initial transaction `PENDING` status, receipt time, and event identity |
| Money and identity | V2 accepts positive integer INR paise and uses `(sourceSystem, externalReference)` as its idempotency key |
| Normalization | Side-effect-free, deterministic conversion of legacy decimal rupees, DR/CR, nested accounts, IFSC/remarks, and Asia/Kolkata time into a create-compatible request |
| Database | PostgreSQL is the source of truth for transactions and durable event intent; Flyway owns schema evolution |
| Messaging | Kafka topic `payments.transactions.created`; transaction UUID is the message key; producer emits versioned `TRANSACTION_CREATED` envelopes |

Transaction `PENDING` means the ingestion record has its initial status; it is
not payment-processing state. Outbox `PUBLISHED` means Kafka acknowledged the
event; it is not a payment-success signal.

## Consistency and delivery

`TransactionService.create` uses a PostgreSQL insert-if-absent operation on the
case-sensitive idempotency key. The winning request writes the canonical
transaction and one `PENDING` outbox event in the same database transaction; no
Kafka call occurs on the request thread. An equal durable request fingerprint
returns the original record as a replay, while a different body for the same key
returns `409 Conflict`.

The scheduled poller handles one event at a time within a bounded batch:

1. A short transaction claims one due `PENDING` row or expired `PROCESSING`
   lease using `FOR UPDATE SKIP LOCKED` and a claim token.
2. With no database lock open, the worker sends the stored payload to Kafka and
   waits for acknowledgement.
3. A second short transaction verifies the claim token and records `PUBLISHED`,
   or schedules capped exponential-backoff retry after ordinary failure.

Kafka acknowledgement followed by a failed database finalize, or lease recovery
after interruption, can result in a duplicate Kafka record. Delivery is
therefore **at least once**: consumers must validate the versioned envelope and
deduplicate `eventId`.

The local Compose topic has three partitions and replication factor one. The
transaction UUID key gives records for one transaction partition affinity; there
is no global transaction order. These are local-development settings, not a
production topology or durability claim.

## Version and migration boundaries

V2 is the active HTTP and event contract. V1 input is never silently interpreted
as V2. New V2 events carry `schemaVersion=2`; version-aware consumers must not
infer schema from the topic name. Historical published V1 records may remain on
the topic.

V1-V6 migrations are preserved byte-for-byte. V7 quarantines historical
unpublished rows so the V2 publisher cannot accidentally publish an incompatible
payload. Quarantine is containment, not delivery recovery; historical delivery
remains unresolved until a future reviewed compatibility workflow exists. The
V7 runbook is the only operator procedure for that supported path.

The V7 database constraint only checks numeric `payload.schemaVersion=2` for a
publishable row. It is a discriminator, not full event-schema validation.

## Observability and retained data

`GET /actuator/outbox` reports `PENDING`, `PROCESSING`, and `QUARANTINED` counts
and their oldest timestamps in one aggregate query. Basic `/actuator/health` is
separate and intentionally does not treat an outbox backlog as ingest API
unavailability.

Published rows are retained as local operational history. This keeps audit and
diagnostic evidence for the exercise but creates unbounded storage growth; no
retention policy, archive, or capacity conclusion is implemented. Planned
retention is clearly marked as future work in the
[use-case model](use-cases.md#planned-next-phase-use-cases).

## Verification and limits

Unit tests cover canonicalization and outbox decisions. Testcontainers tests
cross the HTTP, PostgreSQL, Flyway, scheduler, and Kafka boundaries. The traffic
simulator correlates HTTP responses, transaction rows, fingerprints, outbox
payloads, Kafka keys, event IDs, and event bodies. A retained-outbox query-plan
test protects the current indexed delivery summary query. Local load runs are
diagnostic acceptance evidence only, not capacity, SLO, HA, or DR evidence.

Current non-goals include authentication, source identity verification,
tokenization/encryption policy, operator replay controls, retention/archiving,
telemetry/alerting backends, schema registry, rolling migration, and complete
payment-lifecycle capabilities. Each is planned only where named as such in the
[use-case model](use-cases.md#planned-next-phase-use-cases) and
[diagram catalog](sequence-diagrams.md#planned-next-phase-sequences).
