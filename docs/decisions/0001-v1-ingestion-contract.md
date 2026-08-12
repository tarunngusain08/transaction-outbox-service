# ADR 0001: Original ingestion, identity, and delivery contracts

- Status: superseded by [ADR 0002](0002-api-event-versioning.md)
- Date: 2026-08-10
- Scope: the original proposed V1 hardening rules, adopted as V2 by ADR 0002

This document preserves the reasoning that selected the strict contract. The
prototype already had an incompatible V1 shape, so ADR 0002 corrects the public
API/event version labels. The rules below govern the active V2 contract; V1 is
retired rather than silently reinterpreted.

## Context

The original prototype left several correctness-sensitive choices implicit.
That made database upgrades, idempotent replay, normalization, and outbox
recovery depend on Java or PostgreSQL defaults rather than one product contract.
This decision fixes those contracts before the corresponding implementation is
hardened.

This service records synthetic payment instructions and emits creation events.
It does not authorize, move, settle, reconcile, refund, or account for money.

## Decision

### Money

- The active V2 contract supports only `INR`.
- INR has minor-unit exponent `2`.
- Canonical `amount` is a positive signed-64-bit integer count of paise.
- Legacy `txn_amount` is a positive plain-decimal rupee value. It must convert
  exactly to paise without rounding and must fit in a signed 64-bit integer.
- Legacy exponent notation is rejected. Literal decimal scale is a feed-format
  detail: `1500`, `1500.0`, `1500.00`, and `1500.000` represent the same exact
  amount; `1500.001` is rejected.
- The canonical endpoint accepts only the uppercase literal `INR`. The legacy
  adapter accepts ASCII case variants and emits uppercase `INR`.

### Source-scoped identity and identifiers

The idempotency key is `(sourceSystem, externalReference)`.

- `sourceSystem` is 1-32 uppercase ASCII characters matching
  `[A-Z][A-Z0-9_]{0,31}`.
- `externalReference` is 1-100 case-sensitive ASCII characters matching
  `[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}`.
- Source and destination account identifiers are 1-64 case-sensitive ASCII
  characters matching `[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}`.
- Canonical requests are rejected if an identifier contains leading or trailing
  whitespace, Unicode lookalikes, unsupported punctuation, or a case variant
  that is not the intended identity. The service does not silently trim or
  Unicode-normalize canonical identifiers.
- The legacy adapter may remove feed padding before applying these canonical
  rules. It emits `sourceSystem=LEGACY_BANK_FEED`.

These deliberately narrow alphabets make byte identity, API validation, SQL
uniqueness, and fingerprint comparison agree.

### Field and timestamp ownership

- `transactionId` is a server-generated UUID and is not accepted in a create
  request.
- `status` is server-owned, starts as `PENDING`, and is not accepted in a create
  request. V2 has no status-transition endpoint.
- `createdAt` is the source-owned business occurrence time. It is optional; when
  omitted, the server uses `receivedAt`.
- `receivedAt` is the server-owned ingestion time and is never accepted from a
  client.
- All persisted, returned, fingerprinted, and emitted timestamps are UTC
  instants canonicalized by truncating to PostgreSQL microsecond precision.
- A supplied `createdAt` must be no earlier than the Unix epoch and no more than
  five minutes ahead of `receivedAt`.

For replay comparison, an omitted source timestamp remains an explicit `null`
in the request fingerprint even though the stored `createdAt` defaults to
`receivedAt`. A later request that supplies a timestamp is therefore not the
same request merely because it happens to equal the stored default.

### Atomic idempotency and request fingerprint

Creation uses one PostgreSQL transaction and a database-native insert-if-absent
operation on `(sourceSystem, externalReference)`.

- The winner inserts the transaction and one outbox row and receives
  `201 Created`.
- A concurrent or later request with the same key waits for the winning database
  decision. If its durable fingerprint matches, it receives the original record
  with `200 OK` and no new outbox row.
- A request with the same key and a different fingerprint receives
  `409 Conflict` and changes no durable state.
- Historical rows without a fingerprint cannot be guessed equivalent. They
  require manual reconciliation and conflicting replay is rejected.

Fingerprint version 1 is SHA-256 over a deterministic representation of every
client-controlled canonical field: source system, external reference, amount,
currency, type, source account, destination account, channel, the nullable
requested `createdAt`, and metadata. It excludes server-owned ID, status,
resolved default timestamp, receipt time, and event fields. Record field order
is fixed, metadata object keys are recursively sorted, array order is preserved,
enum names are uppercase, and timestamps use the microsecond-canonical instant.
The fingerprint and its version are stored with the transaction.

### JSON and metadata

- Duplicate member names are rejected anywhere in an incoming JSON document.
- Unknown canonical or legacy top-level fields are rejected.
- Metadata supports objects, arrays, strings, booleans, `null`, and signed
  64-bit integers only.
- Decimal, floating-point, and exponent-form metadata numbers are rejected even
  when mathematically integral. Integers of different parser widths are
  canonicalized to signed 64-bit values before fingerprinting and persistence.
- Metadata object keys are nonblank and at most 128 UTF-8 bytes. String values
  are at most 1,024 UTF-8 bytes, nesting depth is at most five, and the
  canonical serialized metadata is at most 16 KiB.

### Normalization

Normalization is side-effect-free and deterministic. The same accepted legacy
document always produces the same create-compatible JSON document. Its output:

- contains every field accepted by create and no server-owned field;
- uses `sourceSystem=LEGACY_BANK_FEED`;
- converts exact decimal rupees to integer paise;
- maps legacy transaction types, channels, accounts, IFSC values, remarks, and
  the Asia/Kolkata source timestamp into the canonical contract; and
- passes the same bean-validation and metadata-canonicalization rules as a
  direct create request.

The normalization response can therefore be submitted unchanged to the create
endpoint.

### Historical reference migration

Before the replacement V3 migration changes any row, operators run the supplied
read-only preflight. The worklist is grouped by source and trimmed canonical
reference and includes every original row identity.

- If two or more rows map to one canonical key, migration aborts before any data
  mutation.
- Every colliding row remains in place. Automation never merges, deletes,
  renames, moves, or chooses a winner.
- Resolution requires an approved source namespace, alias/supersession record,
  or data correction with its own audit evidence.
- Only non-colliding padded references are canonicalized. Their old and new
  values are recorded in a row-mutation-protected migration audit table in the
  same transaction as the update. The trigger is not owner-proof,
  tamper-evident, or equivalent to immutable audit storage.
- Existing unsupported currency or identifier data is also reported for manual
  reconciliation rather than silently coerced.

The reviewed V3 has not been published, so it is replaced before release. If
that checksum has been applied to any persistent environment, operators must not
rewrite it casually: they must follow the controlled forward/repair runbook and
retain evidence of the applied state.

### Outbox recovery and ownership

- Kafka delivery remains at least once; consumers deduplicate by `eventId`.
- Events are claimed immediately before their individual send. Lease duration
  must exceed the configured Kafka blocking bound, acknowledgement wait, and a
  safety margin.
- A normal delivery failure returns the row to `PENDING`, increments its failure
  count, and schedules capped exponential backoff. V2 does not
  automatically strand events in terminal `FAILED`; it retries indefinitely.
- An interrupted sender leaves the row `PROCESSING` without consuming a retry.
  Lease expiry makes ownership recoverable by another poller.
- The service-owning team owns the outbox until `PUBLISHED`. Operators monitor
  unpublished age and retry count, investigate persistent failures, and use a
  separately approved repair procedure rather than an unauthenticated replay
  endpoint.
- Published rows are retained for this exercise. Alert routing, authenticated
  repair tooling, archival, and partitioning remain explicit production
  follow-ups.

## Characterization and regression matrix

Executable tests introduced with each remediation must cover at least:

1. populated V2-to-V3 upgrade with a non-colliding padded reference and an audit
   mapping;
2. collision preflight/migration abort with every original row unchanged;
3. INR/paise boundary and exact legacy conversion cases;
4. ASCII identity, case sensitivity, microsecond timestamp behavior, duplicate
   JSON members, and metadata integer canonicalization;
5. barrier-synchronized identical and conflicting concurrent creates;
6. deterministic normalization followed by an unchanged create request;
7. just-in-time claims, interruption recovery, and indefinite scheduled retry;
8. full HTTP, transaction row, outbox row/payload, Kafka key, event ID, and event
   body correlation.

## Consequences

The V2 API is narrower and intentionally breaking relative to V1: callers must
provide `sourceSystem` and may no longer provide ID or status. PostgreSQL-specific
insert-on-conflict behavior is an accepted implementation dependency. Legacy
rows without fingerprints or rows that violate the new identity/money policy
require explicit operator work; the service will not manufacture equivalence to
make an upgrade pass.
