# V6 historical event-quarantine runbook

V6 is the boundary between historical event payloads and
`TRANSACTION_CREATED` schema version 2. It never assumes an unpublished payload
is compatible merely because its database status is retryable.

## Before migration

1. Stop every application writer and outbox poller that uses the database.
2. Take a restorable PostgreSQL snapshot and capture `flyway_schema_history`.
3. Inventory unpublished rows and save the result as deployment evidence:

   ```sql
   SELECT id, status, retry_count, last_error, created_at, next_attempt_at,
          payload ->> 'schemaVersion' AS declared_schema_version
   FROM outbox_events
   WHERE status IN ('PENDING', 'PROCESSING', 'FAILED')
   ORDER BY created_at, id;
   ```

4. Confirm that the application being deployed emits event schema V2 and uses
   only the V2 REST routes.

This prototype runs Flyway synchronously at application startup. The procedure
requires stopped writers/pollers; it is not a rolling or zero-downtime plan.

## Migration behavior

Within the Flyway transaction, V6:

- adds quarantine time, reason, and prior-status columns;
- changes every existing unpublished `PENDING`, `PROCESSING`, or `FAILED` row
  to `QUARANTINED`;
- preserves payload, retry/error evidence, timestamps, and event identity;
- clears claim ownership so no stale worker appears to own the row; and
- installs constraints that separate publishable and quarantined state and
  require `payload.schemaVersion=2` for `PENDING`/`PROCESSING` rows.

Historical `PUBLISHED` rows are not changed. Rows created by the V2 application
after V6 start in `PENDING` and follow the normal claim/retry path.

## After migration

Verify before starting traffic:

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

The second query must return no rows. After the V2 service starts, verify
`/actuator/outbox`: quarantined count/age are a separate operational signal and
must never be counted as a publishable backlog.

## Recovery policy

There is deliberately no SQL snippet or public API that flips a quarantined row
back to `PENDING`. Status-only mutation violates the delivery-state constraint;
even clearing all quarantine fields still violates the publishable-schema
constraint while the historical payload is unchanged.
Until UC-P08 is implemented, retain and investigate the evidence manually; do
not mutate, delete, or merge it.

A future release workflow must authenticate and authorize the operator, parse
the actual historical schema, validate or explicitly transform it to V2,
preserve the original row, record actor/reason and causal identity, and create a
replacement event only under a reviewed event-id/deduplication policy.

## If an older V6 checksum was applied

Do not rewrite migration history or use `flyway repair` to make different SQL
look equivalent. Capture the applied checksum and schema, restore the exact
migration used by that environment, and create a reviewed forward-only
migration from the observed state. Exercise it on a restored copy first.
