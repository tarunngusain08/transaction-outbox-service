# V7 historical event-quarantine runbook

V7 is the forward-only boundary between historical event payloads and
`TRANSACTION_CREATED` schema version 2. Quarantine prevents incompatible
publication; it does not complete delivery of the historical transaction.

## Supported migration lineage

Migrations V1 through V6 are restored byte-for-byte from artifact commit
`1d9d97c` and must never be edited again. A unit test pins their SHA-256
digests. Integration tests create a populated V6 database with that exact
chain, apply V7, start the real publisher, and verify Kafka behavior.

This supports both a clean installation and a database that already recorded
the canonical V1-V6 checksums. Never use `flyway repair` to make different SQL
look equivalent.

The short-lived, unpushed branch version that rewrote V3 and used quarantine as
V6 is not a supported release lineage. Reset a disposable local database that
ran it with `make reset`. If such a database contains non-disposable data, stop:
capture its schema, checksums, and backup, then design and test a bespoke
forward migration. Do not delete, merge, or silently select historical records.

## Mandatory release gate

1. Stop every application writer and outbox poller that uses the database.
2. Take a restorable PostgreSQL snapshot and capture `flyway_schema_history`.
3. Against the stopped pre-V7 database, run:

   ```bash
   make migration-v7-preflight
   ```

4. Retain the event-level worklist as deployment evidence. The command returns
   zero only when `flyway_schema_history` is an exact multiset match for the
   six expected V1-V6 `version`/`type`/`script`/`checksum`/`success` tuples and
   the reported `unresolved_historical_unpublished_events` count is zero.
   A changed checksum, missing or duplicate migration, failed migration,
   version-null repeatable migration, extra versioned migration,
   already-applied V7 migration, or every nonzero event count makes the process
   exit nonzero.

If the count is nonzero, either drain the rows with the compatible pre-V2
publisher before upgrading, or delay release until UC-P08 provides a reviewed
compatibility/replacement workflow. The executable gate does not approve V7 in
that state. A separately authorized emergency decision may still apply V7 only
as fail-safe containment, but it strands those rows in `QUARANTINED`; the
release remains operationally incomplete until every row is resolved.

Do not rerun this pre-V7 command as a post-migration check: it deliberately
rejects a database whose history includes V7, even if quarantine makes the old
unpublished predicate empty. Use the post-migration queries below instead.

This service runs Flyway synchronously at startup. The procedure requires
stopped writers and pollers; it is not a rolling or zero-downtime plan.

`V7PreflightIT` copies and executes this exact SQL file inside PostgreSQL 17. It
asserts success for an empty canonical V6 database and failure for a changed
recorded checksum, a deliberately replaced V2 row plus duplicate V1 and
version-null repeatable rows, one unresolved historical event, and an
already-applied V7 migration.

## Migration behavior

The preserved V6 migration first converts any `FAILED` row to `PENDING` and
makes it due. V7 then:

- adds quarantine time, reason, and prior-status columns;
- moves every remaining historical `PENDING` or `PROCESSING` row to
  `QUARANTINED`;
- preserves payload, event identity, retry/error evidence, and the post-V6
  next-attempt value;
- clears claim ownership so no stale worker appears to own the row;
- installs state constraints and a partial quarantine-time index;
- requires a numeric JSON `schemaVersion` discriminator equal to `2` before a
  row can be `PENDING` or `PROCESSING`; and
- updates the audit trigger name/message through V7 instead of changing V3.

The discriminator constraint is deliberately narrow. It rejects a missing,
string-valued, or non-2 discriminator, but it does **not** validate the complete
V2 envelope. Application serialization and consumer contract validation remain
responsible for full schema conformance.

Historical `PUBLISHED` rows are unchanged. Rows created by the V2 application
after V7 start in `PENDING` and follow the normal claim/retry path.

## Post-migration verification

Run these checks before starting traffic:

```sql
SELECT status, quarantined_from_status, COUNT(*)
FROM outbox_events
GROUP BY status, quarantined_from_status
ORDER BY status, quarantined_from_status;

SELECT id, quarantine_reason, quarantined_at
FROM outbox_events
WHERE status = 'QUARANTINED'
  AND (quarantined_at IS NULL OR quarantine_reason IS NULL);
```

The second query must return no rows. `GET /actuator/outbox` reads all counts
and oldest timestamps in one SQL statement. Its publishable age starts at
`created_at`; its quarantine age starts at `quarantined_at`.

Any nonzero quarantine count is a separate release/operations signal. It is not
a recoverable publisher backlog and must not be described as delivered.

## Recovery policy

There is deliberately no SQL snippet or public API that flips a quarantined row
back to `PENDING`. Status-only mutation violates the delivery-state constraint;
even clearing all quarantine fields still fails the numeric V2-discriminator
constraint while the historical payload has no numeric `schemaVersion: 2`.

Until UC-P08 is implemented, preserve and investigate the evidence. A future
workflow must authenticate and authorize the operator, parse the actual legacy
schema, fully validate or explicitly transform it to V2, preserve the original,
record actor/reason and causal identity, and define event-ID/deduplication
semantics before creating a replacement.
