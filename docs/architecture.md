# Architecture notes

## Documentation map

- [Use-case model](use-cases.md) — actors, triggers, outcomes, status, and
  implemented-versus-planned scope.
- [Sequence-diagram catalog](sequence-diagrams.md) — runtime ordering, state
  transitions, alternate flows, engineering workflows, and next-phase designs.

## Consistency boundary

`TransactionService.create` is the business transaction boundary. It writes the
canonical transaction and its `TRANSACTION_CREATED` outbox event through JPA in
one PostgreSQL transaction. No Kafka call occurs on the request thread.

This avoids the classic dual-write failure where a transaction commits but no
durable event record accompanies it. A serialization or database failure rolls
back the transaction insert as well as any staged outbox insert. Kafka delivery
is deliberately asynchronous and is not part of this atomic boundary.

## Delivery semantics

The scheduled poller processes small batches in three phases:

1. A short `REQUIRES_NEW` claim transaction selects due `PENDING` rows or
   expired `PROCESSING` leases with `FOR UPDATE SKIP LOCKED`, assigns each a
   unique claim token, changes status to `PROCESSING`, and commits.
2. With no database transaction or row lock open, the worker publishes the
   stored JSON using the transaction ID as Kafka key and waits for acknowledgement.
   Kafka producer `max.block.ms` and the future wait both have explicit bounds.
3. A short `REQUIRES_NEW` finalize transaction locks the one row, verifies the
   claim token is still current, and records `PUBLISHED`, a scheduled retry, or
   terminal `FAILED`.

If Kafka accepts the record but the database update subsequently fails, the
lease eventually expires and the event is retried. A consumer can also observe
the Kafka record before the `PUBLISHED` database commit completes. Delivery is
therefore **at least once**; consumers must use `eventId` as an idempotency key.

Failures use exponential backoff (1, 2, 4, ... seconds, capped at five minutes).
With the default eight-attempt budget, scheduled waits reach 64 seconds before
the eighth failure becomes terminal. Larger retry budgets reach the five-minute
formula cap. A `FAILED` row remains queryable for operator review.

## Concurrency

`FOR UPDATE SKIP LOCKED` gives concurrent pollers disjoint claim batches without
waiting on another poller's selected rows. A claim lease recovers work after a
process exits between claim and finalize. Guarded claim-token checks stop a stale
worker from finalizing a lease now owned by another worker. A lease expiring
during a very slow but successful publish can still produce a duplicate, which
is why `eventId` deduplication remains mandatory.

## Normalization boundary

The normalization endpoint is side-effect-free: it neither writes to the
database nor emits an event. It is not deterministic because each response gets
a new UUID. Its output is the same canonical shape accepted by the create
endpoint. It:

- converts a decimal major-unit string to exact ISO-currency minor units;
- maps `DR`/`CR` into canonical enums;
- treats the source timestamp as `Asia/Kolkata` and returns an ISO-8601 instant;
- flattens payer/payee accounts; and
- preserves source-only fields in `metadata`.

## Verification strategy

Fast unit tests isolate normalization, transaction creation, and outbox retry
logic. A separate Testcontainers suite exercises the HTTP boundary, Flyway
migrations, PostgreSQL constraints and persistence, scheduled outbox delivery,
and Kafka consumption together.

The repository-level traffic simulator covers the packaged Compose topology. It
sends expected successes and failures under sequential or concurrent load, then
correlates its unique run prefix across `transactions`, published
`outbox_events`, and consumed Kafka messages. It checks the Kafka key, envelope,
single creation identity, and complete canonical body while allowing repeats of
that same event identity. Configurable local throughput and p95 bounds make load
mode a bounded acceptance scenario, not a capacity claim.

`GET /actuator/outbox` reports pending, processing, and failed counts plus oldest
unpublished age without changing ordinary `/actuator/health`. It enables a
separate delivery alarm signal, but this repository does not configure an alert
backend.

## Production follow-ups

The planned use cases below are specified in the
[use-case model](use-cases.md#planned-next-phase-use-cases) and corresponding
[sequence diagrams](sequence-diagrams.md#planned-next-phase-sequences). None is
implemented yet.

- UC-P01: authenticate and authorize API requests.
- UC-P02: enforce deterministic request rate limits.
- UC-P03: encrypt/tokenize account identifiers and redact sensitive logs.
- UC-P04: add an auditable operator replay path for `FAILED` events.
- UC-P05: archive/partition old `PUBLISHED` rows under a retention policy.
- UC-P06: export correlated telemetry and provide production SLO dashboards and
  alerts.
- UC-P07: enforce schema-registry compatibility before event changes deploy.
