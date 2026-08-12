# V3 identity migration runbook

V3 replaces the unpublished prototype migration with a fail-before-mutation
upgrade. It introduces source-scoped references, server receipt time, durable
request-fingerprint columns, and the V2 INR/identifier constraints.

## Before migration

1. Stop application writers and take a restorable PostgreSQL snapshot.
2. Record the application commit and the complete `flyway_schema_history` row
   set.
3. Against the retained V2 database, run:

   ```bash
   make migration-preflight
   ```

4. Save the output as deployment evidence. The collision count must be zero and
   the contract-violation query must return no rows.
5. Review every `CANONICALIZE_WITH_AUDIT` item. V3 will make exactly that trim
   and record its old/new values.

The preflight opens a repeatable-read, read-only transaction. It does not update,
move, rename, merge, or delete a payment row.

## Collision handling

Any `COLLISION_ABORT_MANUAL_RECONCILIATION` item blocks the deployment. Preserve
all listed records in `transactions`. An authorized domain owner must resolve
the identity through a source namespace, an alias/supersession record, or an
approved correction with separate audit evidence. Automation must not choose a
winner.

V3 repeats the blocking checks before its first mutation, so a stale or skipped
preflight still fails safely. Flyway executes this PostgreSQL migration in one
transaction; later failure also rolls back its audit insert and canonicalization.

## After migration

Verify:

```sql
SELECT *
FROM transaction_reference_canonicalization_audit
ORDER BY canonicalized_at, transaction_id;

SELECT source_system, external_reference, COUNT(*)
FROM transactions
GROUP BY source_system, external_reference
HAVING COUNT(*) > 1;
```

The second query must return no rows. The audit table rejects ordinary row-level
inserts, updates, and deletes after migration. This is row-mutation protection,
not immutability: a sufficiently privileged database owner can disable or drop
the trigger/table. Production audit requirements need restricted ownership,
separate append-only storage, and tamper-evidence.

Historical transactions have `sourceSystem=DIRECT_API`,
`receivedAt=<migration time>`, and a null request fingerprint. The service does
not assume that a later replay is equivalent to such a row; reconciliation is
manual.

## If the old V3 was already applied

Do not overwrite the migration file and run `flyway repair` merely to silence a
checksum mismatch. That would make migration history claim that different SQL
ran.

1. Halt the deployment and retain a database snapshot.
2. Capture the old V3 checksum, installed timestamp, application artifact, and
   actual constraints from PostgreSQL.
3. Restore the exact old V3 file for that environment's current release.
4. Create and review a new forward-only migration that performs the preflight,
   safe data mapping, and schema transition from the observed state.
5. Exercise that path on a restored copy before production rollout.

`flyway repair` is appropriate only when review proves the applied and resolved
migrations are semantically identical. The old and replacement V3 are not, so a
forward migration is required.
