# Architecture notes

## Documentation map

- [V2 versioning decision](decisions/0002-api-event-versioning.md) — governing
  API/event versions, V1 retirement, and historical-payload quarantine.
- [Forward-only migration decision](decisions/0003-forward-only-flyway-upgrades.md)
  — immutable applied migrations, release gating, and V6-to-V7 upgrade support.
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
an explicit `410 Gone` and operation-specific successor link without binding
the request or touching durable state.

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

Applied migrations V1-V6 remain byte-for-byte identical to artifact `1d9d97c`.
The forward-only V7 migration treats every pre-V2 unpublished `PENDING` or
`PROCESSING` row as unverified; V6 has already converted historical `FAILED`
rows to `PENDING`. V7 preserves the stored payload, retry/error evidence, and
post-V6 state; clears any stale claim; and moves the row to `QUARANTINED` with a
reason and timestamp. The claim query cannot select that status. Published
historical rows remain published.

This is containment, not historical delivery recovery. Production release
requires the pre-V7 unpublished count to be zero, either initially or after the
compatible old publisher drains it. Otherwise release remains blocked until the
planned authenticated compatibility/replacement workflow resolves every row.

A V7 database constraint requires the JSON value at `payload.schemaVersion` to
be numeric `2` while a row is `PENDING` or `PROCESSING`. This is only a
publishability discriminator: a body such as `{"schemaVersion":2}` passes that
one check and is not thereby a conforming V2 envelope. Ordinary application
writes are serialized to the complete contract; consumers must still validate
the complete body.

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
logic and pin SHA-256 digests for every already-applied V1-V6 migration. A
separate Testcontainers suite exercises the HTTP boundary, Flyway migrations,
PostgreSQL constraints and persistence, scheduled outbox delivery, and Kafka
consumption together. A compatibility integration test creates a populated
canonical V6 database, applies V7, starts the production scheduler against
Kafka, proves the old event stays quarantined, and proves a new schema-V2 event
is delivered. The exact V7 preflight script is also run against an empty
canonical V6 database, a V6 database with an unresolved event, and an already
upgraded V7 database to prove its process exit fails closed. It also changes a
recorded V3 checksum and proves the lineage check rejects it.

The repository-level traffic simulator covers the packaged Compose topology. It
sends expected successes and failures under sequential or concurrent load, then
submits successful normalized output unchanged to creation and correlates its
unique run prefix across the full HTTP response, `transactions` row and durable
fingerprint, published `outbox_events` row and stored payload, and consumed Kafka
record. It checks every canonical field, timestamp relationship, Kafka key,
envelope, and single creation identity while allowing repeats of that same event
identity. Configurable local throughput and p95 bounds make load mode a bounded
acceptance scenario, not a capacity claim.

`GET /actuator/outbox` reads pending, processing, and quarantined counts plus
both oldest timestamps in one PostgreSQL aggregate statement. Publishable age
uses `created_at`; quarantine age uses `quarantined_at`. Its outer predicate
uses explicit status-equality branches so PostgreSQL can combine the three
partial indexes. An integration fixture with 200,000 retained `PUBLISHED` rows
and 100 rows in each active state requires a `BitmapOr` plan and rejects an
outbox sequential scan. This is a regression bound, not a latency or capacity
claim. The endpoint does not change ordinary `/actuator/health`, and this
repository does not configure an alert backend.

## Current production-readiness limits

- `sourceSystem` is client-supplied. Authentication/authorization must derive or
  verify source identity before it can be trusted for tenant/payment identity.
- Flyway runs synchronously at startup. The documented upgrade procedure stops
  writers and pollers; no expand/contract, rolling, or zero-downtime migration
  path is implemented.
- A nonzero historical-unpublished/quarantine count means historical event
  delivery is unresolved. V7 prevents unsafe publication but does not satisfy
  the transaction-to-event promise for those rows.
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
