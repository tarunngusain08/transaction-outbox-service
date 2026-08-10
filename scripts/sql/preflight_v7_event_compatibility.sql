\set ON_ERROR_STOP on

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

\echo 'V7 unpublished historical event inventory'
SELECT
    status,
    COALESCE(payload ->> 'schemaVersion', '<missing>') AS declared_schema_version,
    COUNT(*) AS event_count,
    MIN(created_at) AS oldest_created_at,
    MAX(retry_count) AS maximum_retry_count
FROM outbox_events
WHERE status IN ('PENDING', 'PROCESSING', 'FAILED')
GROUP BY status, COALESCE(payload ->> 'schemaVersion', '<missing>')
ORDER BY status, declared_schema_version;

\echo 'V7 release gate (must be zero before declaring historical delivery complete)'
SELECT COUNT(*) AS unresolved_historical_unpublished_events
FROM outbox_events
WHERE status IN ('PENDING', 'PROCESSING', 'FAILED');

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
WHERE status IN ('PENDING', 'PROCESSING', 'FAILED')
ORDER BY created_at, id;

COMMIT;
