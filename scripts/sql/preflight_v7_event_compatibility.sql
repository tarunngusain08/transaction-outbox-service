\set ON_ERROR_STOP on

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

\echo 'V7 canonical Flyway lineage check'
DO $preflight$
DECLARE
    matching_migrations INTEGER;
    recorded_migrations INTEGER;
BEGIN
    IF TO_REGCLASS('flyway_schema_history') IS NULL THEN
        RAISE EXCEPTION
            'V7 preflight requires a canonical Flyway V6 database; schema history is missing';
    END IF;

    WITH expected(version, script, checksum) AS (
        VALUES
            ('1', 'V1__create_transactions.sql', -1701863425),
            ('2', 'V2__create_outbox_events.sql', 98626752),
            ('3', 'V3__harden_transaction_invariants.sql', -915971206),
            ('4', 'V4__add_outbox_claim_leases.sql', 1046310496),
            ('5', 'V5__optimize_outbox_claim_indexes.sql', -1676750279),
            ('6', 'V6__recover_terminal_outbox_events.sql', 1886286009)
    )
    SELECT COUNT(*)
    INTO matching_migrations
    FROM expected
    JOIN flyway_schema_history AS actual
      ON actual.version = expected.version
     AND actual.type = 'SQL'
     AND actual.script = expected.script
     AND actual.checksum = expected.checksum
     AND actual.success;

    SELECT COUNT(*)
    INTO recorded_migrations
    FROM flyway_schema_history
    WHERE version IS NOT NULL;

    IF matching_migrations <> 6 OR recorded_migrations <> 6 THEN
        RAISE EXCEPTION
            'V7 preflight requires the exact canonical Flyway V1-V6 state; found a missing, changed, failed, extra, or already-applied migration'
            USING HINT =
                'Do not use flyway repair. Follow docs/migrations/v7-event-quarantine.md.';
    END IF;
END
$preflight$;

\echo 'V7 unpublished historical event inventory'
SELECT
    status,
    COALESCE(payload ->> 'schemaVersion', '<missing>') AS declared_schema_version,
    COUNT(*) AS event_count,
    MIN(created_at) AS oldest_created_at,
    MAX(retry_count) AS maximum_retry_count
FROM outbox_events
WHERE status = 'PENDING'
   OR status = 'PROCESSING'
   OR status = 'FAILED'
GROUP BY status, COALESCE(payload ->> 'schemaVersion', '<missing>')
ORDER BY status, declared_schema_version;

\echo 'V7 release gate (must be zero before declaring historical delivery complete)'
SELECT COUNT(*) AS unresolved_historical_unpublished_events
FROM outbox_events
WHERE status = 'PENDING'
   OR status = 'PROCESSING'
   OR status = 'FAILED';

\echo 'V7 event-level operator worklist'
SELECT
    id,
    aggregate_id,
    status,
    retry_count,
    last_error,
    created_at,
    next_attempt_at,
    payload ->> 'schemaVersion' AS declared_schema_version
FROM outbox_events
WHERE status = 'PENDING'
   OR status = 'PROCESSING'
   OR status = 'FAILED'
ORDER BY created_at, id;

DO $preflight$
DECLARE
    unresolved_events BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO unresolved_events
    FROM outbox_events
    WHERE status = 'PENDING'
       OR status = 'PROCESSING'
       OR status = 'FAILED';

    IF unresolved_events <> 0 THEN
        RAISE EXCEPTION
            'V7 release gate failed: % unresolved historical unpublished event(s)',
            unresolved_events
            USING HINT =
                'Drain every row with the compatible pre-V2 publisher, then rerun the gate.';
    END IF;
END
$preflight$;

COMMIT;
