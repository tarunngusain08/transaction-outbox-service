# ADR 0002: Explicit API V2, event schema V2, and legacy quarantine

- Status: accepted
- Date: 2026-08-10
- Scope: public REST routes, transaction-created events, and pre-V2 outbox rows
- Supersedes: ADR 0001's V1 version label, but adopts its strict contract rules

## Context

The original V1 prototype accepted caller-owned `transactionId` and `status`,
did not require `sourceSystem`, and returned/emitted a smaller transaction
shape. Later hardening changed those ownership and identity rules while the
route remained `/api/v1` and the event still advertised `schemaVersion=1`.
Those are breaking changes, not a compatible V1 extension.

The database may also contain unpublished payloads serialized by the old code.
Changing only the new producer version would let the current poller publish an
old payload on a topic whose current producer contract is V2. Blindly upgrading
the JSON is unsafe because source identity, receipt time, and prior broker
observability cannot be inferred reliably.

## Decision

1. The strict ingestion/read/normalization routes are under
   `/api/v2/transactions`.
2. The retired `/api/v1/transactions` routes return `410 Gone`, a stable problem
   type, and an operation-specific successor link: create maps to the V2
   collection, normalize retains `/normalize`, and GET-by-ID retains the same
   transaction ID. The service does not bind a V1 body, create state, or guess a
   V2 request.
3. Newly created `TRANSACTION_CREATED` envelopes advertise `schemaVersion=2`
   and retain the `producer` field. The topic remains
   `payments.transactions.created`; consumers must branch on and validate the
   explicit version.
4. Applied migrations V1-V6 retain their bytes and checksums. Forward-only
   Flyway V7 moves every unpublished row remaining after V6 to `QUARANTINED`.
   It preserves payload and diagnostics, records a quarantine reason/time and
   previous post-V6 status, and clears stale claim ownership.
5. The ordinary claim query accepts only due `PENDING` and expired `PROCESSING`
   rows. It cannot publish `QUARANTINED` rows. A database constraint requires
   the `payload.schemaVersion` JSON value to equal numeric `2` for every
   `PENDING` or `PROCESSING` row. This is a discriminator check, not validation
   of the complete V2 body.
6. V7 leaves historical `PUBLISHED` rows unchanged. At-least-once delivery means
   their prior observability cannot safely be rewritten.
7. Releasing a quarantined payload requires a future authenticated,
   authorization-checked compatibility workflow. It must validate or explicitly
   transform the old body, retain the original evidence, record actor/reason,
   and define event-identity semantics before creating any replacement V2 event.
8. Production release requires a zero pre-V7 historical-unpublished count. If
   the compatible old producer cannot drain a nonzero count, V7 may contain the
   rows but release remains incomplete until the workflow in item 7 resolves
   them.

## Compatibility matrix

| Boundary | V1 | V2 |
|---|---|---|
| Create request | Caller could supply ID/status; no source namespace | Caller supplies `sourceSystem`; ID/status/receipt time are server-owned |
| REST route | Retired; always `410 Gone` | `/api/v2/transactions` |
| Event envelope | Historical shape; producer/source/receipt fields may be absent | `schemaVersion=2`, producer, event identity/time, canonical V2 transaction |
| Existing unpublished row at V7 | Quarantined without payload mutation; release remains blocked | Not applicable; new rows are created after V7 |
| Publisher eligibility | Never inferred from payload | Only V2-created `PENDING`/`PROCESSING` rows are claimable |

## Consequences

Clients must deliberately migrate to V2. Consumers can distinguish the new
schema without guessing from deployment time. Historical delivery is paused
rather than lost, merged, deleted, or mislabeled; operators receive visibility
but no unsafe unauthenticated release switch.

The same topic now contains any already-published historical V1 records and new
V2 records, so version-aware consumers remain mandatory. `sourceSystem` is
still self-declared until authentication derives or verifies it. The Flyway
procedure is stop-the-world for writers/pollers and does not claim rolling or
zero-downtime upgrade safety.
