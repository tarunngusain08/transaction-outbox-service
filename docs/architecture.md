# Architecture notes

## Documentation map

- [V2 versioning decision](decisions/0002-api-event-versioning.md) — governing
  API/event versions, V1 retirement, and historical-payload quarantine.
- [Ingestion contract decision](decisions/0001-v1-ingestion-contract.md) —
  money, identity, ownership, idempotency, metadata, migration, normalization,
  and outbox rules adopted by V2.
- [Use-case model](use-cases.md) — actors, triggers, outcomes, status, and
  implemented-versus-planned scope.
- [Sequence-diagram catalog](sequence-diagrams.md) — runtime ordering, state
  transitions, alternate flows, engineering workflows, and next-phase designs.

## Consistency boundary

`TransactionService.create` is the business transaction boundary. It uses a
PostgreSQL `INSERT ... ON CONFLICT DO NOTHING RETURNING id` decision for the
case-sensitive `(sourceSystem, externalReference)` key. The winner writes the
canonical transaction and its `TRANSACTION_CREATED` outbox event in one
PostgreSQL transaction. No Kafka call occurs on the request thread.

Every new transaction stores a versioned SHA-256 fingerprint of all canonical
client-owned fields. A loser reads the committed winner: an equal durable
fingerprint returns the original transaction as an idempotent replay; a
different or absent fingerprint returns `409 Conflict`. Consequently,
concurrent requests cannot both perform a preliminary read and then race to
create duplicate state, and historical rows are never guessed equivalent.

This avoids the classic dual-write failure where a transaction commits but no
durable event record accompanies it. A serialization or database failure rolls
back the transaction insert as well as any staged outbox insert. Kafka delivery
is deliberately asynchronous and is not part of this atomic boundary.

## API and event compatibility

The active routes are `/api/v2/transactions` and
`/api/v2/transactions/normalize`. The strict request contract requires
`sourceSystem`, reserves ID/status/receipt time for the server, and is not
backward compatible with the earlier V1 prototype. V1 routes therefore return
an explicit `410 Gone` and successor link without binding the request or
touching durable state.

New `TRANSACTION_CREATED` records use `schemaVersion=2` on the existing
`payments.transactions.created` topic. V2 adds the producer envelope and the
source/receipt fields required by the strict API. Consumers must branch on the
version rather than infer a schema from topic name.

## Delivery semantics

The scheduled poller processes at most `batchSize` events per run in three
phases for each event:

1. Immediately before one send, a short `REQUIRES_NEW` claim transaction selects
   one due `PENDING` row or expired `PROCESSING` lease with
   `FOR UPDATE SKIP LOCKED`, assigns a unique claim token, changes status to
   `PROCESSING`, and commits. Later events are not pre-claimed.
2. With no database transaction or row lock open, the worker publishes the
   stored JSON using the transaction ID as Kafka key and waits for acknowledgement.
   Kafka producer `max.block.ms` and the future wait both have explicit bounds.
3. A short `REQUIRES_NEW` finalize transaction locks the one row, verifies the
   claim token is still current, and records `PUBLISHED` or a scheduled retry.

If Kafka accepts the record but the database update subsequently fails, the
lease eventually expires and the event is retried. A consumer can also observe
the Kafka record before the `PUBLISHED` database commit completes. Delivery is
therefore **at least once**; consumers must use `eventId` as an idempotency key.

Ordinary failures increment `retry_count`, retain a bounded root error, and use
exponential backoff (1, 2, 4, ... seconds, capped at five minutes) indefinitely.
They never strand a V2 event in a terminal application state. An interrupted
sender preserves the thread interrupt and leaves its event `PROCESSING` without
consuming a retry; lease expiry returns ownership to a worker.

Migration V6 treats every pre-V2 unpublished `PENDING`, `PROCESSING`, or
`FAILED` row as unverified. It preserves the stored payload, retry evidence, and
prior status; clears any stale claim; and moves the row to `QUARANTINED` with a
reason and timestamp. The claim query cannot select that status. This prevents
an old payload from being advertised as V2 or silently transformed. Published
historical rows remain published. Controlled compatibility review/replay is a
planned operator workflow, not an automatic migration action.

A V6 database constraint also requires `payload.schemaVersion` to be `2` while
a row is `PENDING` or `PROCESSING`. Therefore an old writer cannot create fresh
claimable V1 work after V6, and clearing quarantine fields is insufficient to
release a legacy payload.

## Concurrency

`FOR UPDATE SKIP LOCKED` gives concurrent pollers disjoint individual claims
without waiting. A claim lease recovers work after a process exits between claim
and finalize. Guarded claim-token checks stop a stale worker from finalizing a
lease now owned by another worker. Startup requires the lease to exceed Kafka
producer `max.block.ms` plus the acknowledgement wait plus a safety margin. A
lease can still expire after an unobservable broker acknowledgement or process
pause, so `eventId` deduplication remains mandatory.

## Normalization boundary

The normalization endpoint is side-effect-free: it neither writes to the
database nor emits an event. It is deterministic and omits server-owned ID,
status, and receipt time. Its output is the exact request shape accepted by the
create endpoint and can be submitted unchanged. It:

- converts positive plain-decimal INR rupees to exact integer paise without
  rounding or exponent notation;
- maps `DR`/`CR` into canonical enums;
- treats the source timestamp as `Asia/Kolkata` and returns an ISO-8601 instant;
- flattens payer/payee accounts;
- preserves source-only fields in canonicalized `metadata`; and
- applies the same identifier, metadata, and timestamp-domain rules as create.

## Verification strategy

Fast unit tests isolate normalization, transaction creation, and outbox retry
logic. A separate Testcontainers suite exercises the HTTP boundary, Flyway
migrations, PostgreSQL constraints and persistence, scheduled outbox delivery,
and Kafka consumption together. A compatibility integration test migrates a
realistic V1 envelope, starts the production scheduler against Kafka, proves the
old event stays quarantined, and proves a new schema-V2 event is delivered.

The repository-level traffic simulator covers the packaged Compose topology. It
sends expected successes and failures under sequential or concurrent load, then
submits successful normalized output unchanged to creation and correlates its
unique run prefix across the full HTTP response, `transactions` row and durable
fingerprint, published `outbox_events` row and stored payload, and consumed Kafka
record. It checks every canonical field, timestamp relationship, Kafka key,
envelope, and single creation identity while allowing repeats of that same event
identity. Configurable local throughput and p95 bounds make load mode a bounded
acceptance scenario, not a capacity claim.

`GET /actuator/outbox` reports pending, processing, and quarantined counts plus
the oldest publishable and quarantined ages without changing ordinary
`/actuator/health`. It enables separate delivery and compatibility-review alarm
signals, but this repository does not configure an alert backend.

## Current production-readiness limits

- `sourceSystem` is client-supplied. Authentication/authorization must derive or
  verify source identity before it can be trusted for tenant/payment identity.
- Flyway runs synchronously at startup. The documented upgrade procedure stops
  writers and pollers; no expand/contract, rolling, or zero-downtime migration
  path is implemented.
- The local load thresholds are functional acceptance bounds only. They do not
  establish capacity, production SLOs, degraded-dependency behavior, HA, or DR.
- The reference-canonicalization table rejects ordinary row-level insert,
  update, and delete operations. A database owner can still disable/drop the
  trigger or table, so it is row-mutation protected rather than immutable or
  tamper-evident audit storage.

## Production follow-ups

The planned use cases below are specified in the
[use-case model](use-cases.md#planned-next-phase-use-cases) and corresponding
[sequence diagrams](sequence-diagrams.md#planned-next-phase-sequences). None is
implemented yet.

- UC-P01: authenticate and authorize API requests.
- UC-P02: enforce deterministic request rate limits.
- UC-P03: encrypt/tokenize account identifiers and redact sensitive logs.
- UC-P04: add an authenticated, auditable way to expedite a persistently failing
  event after an operator resolves its underlying dependency.
- UC-P05: archive/partition old `PUBLISHED` rows under a retention policy.
- UC-P06: export correlated telemetry and provide production SLO dashboards and
  alerts.
- UC-P07: enforce schema-registry compatibility before event changes deploy.
- UC-P08: review and transform quarantined legacy payloads through an
  authenticated, auditable compatibility workflow.
