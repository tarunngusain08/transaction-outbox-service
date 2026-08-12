# Architecture notes

## Consistency boundary

`TransactionService.create` is the business transaction boundary. It writes the
canonical transaction and its `TRANSACTION_CREATED` outbox event through JPA in
one PostgreSQL transaction. No Kafka call occurs on the request thread.

This avoids the classic dual-write failure where a transaction commits but its
event is lost. A serialization or database failure rolls back both inserts.

## Delivery semantics

The scheduled poller selects eligible `PENDING` event IDs in small batches. Each
event is then processed in its own `REQUIRES_NEW` transaction:

1. Lock the row and re-check that it is still eligible.
2. Publish the stored JSON payload using the transaction ID as the Kafka key.
3. Wait for the broker acknowledgement.
4. Mark the row `PUBLISHED` and retain it for audit.

If Kafka accepts the record but the database update subsequently fails, the
event is retried. Delivery is therefore **at least once**. Consumers must use
`eventId` as an idempotency key.

Failures use exponential backoff (1, 2, 4, ... seconds, capped at five minutes).
After the configured maximum, the row becomes `FAILED` and remains queryable for
operator review.

## Concurrency

`PESSIMISTIC_WRITE` serializes delivery of a selected event. A second service
instance may discover the same candidate ID, but after it obtains the lock it
re-checks the status and skips an event already published by another instance.
This design favors clarity for the exercise. At high volume, use claim tokens or
`FOR UPDATE SKIP LOCKED` batch claiming to reduce lock duration.

## Normalization boundary

The normalization endpoint is deliberately pure: it neither writes to the
database nor emits an event. Its output is the same canonical shape accepted by
the create endpoint. It:

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
`outbox_events`, and consumed Kafka messages. This verifies the complete data
path without treating expected 4xx responses as test failures.

## Production follow-ups

- Partition or archive old `PUBLISHED` rows according to audit-retention policy.
- Add an operator-controlled replay path for `FAILED` events.
- Encrypt or tokenize account identifiers and apply field-level log redaction.
- Add authentication, authorization, rate limiting, and request tracing.
- Add schema-registry compatibility checks for Kafka events.
- Add broker/database telemetry and production SLO dashboards.
