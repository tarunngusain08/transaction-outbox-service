\set ON_ERROR_STOP on

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

\echo 'V3 canonical-reference worklist'
WITH canonicalized AS (
    SELECT
        id,
        'DIRECT_API'::TEXT AS source_system,
        external_reference AS old_reference,
        BTRIM(external_reference) AS canonical_reference
    FROM transactions
), grouped AS (
    SELECT
        source_system,
        canonical_reference,
        COUNT(*) AS row_count,
        BOOL_OR(old_reference <> canonical_reference) AS has_padding,
        JSONB_AGG(
            JSONB_BUILD_OBJECT(
                'transactionId', id,
                'oldReference', old_reference
            )
            ORDER BY id::TEXT
        ) AS records
    FROM canonicalized
    GROUP BY source_system, canonical_reference
)
SELECT
    source_system,
    canonical_reference,
    row_count,
    CASE
        WHEN row_count > 1 THEN 'COLLISION_ABORT_MANUAL_RECONCILIATION'
        WHEN has_padding THEN 'CANONICALIZE_WITH_AUDIT'
        ELSE 'NO_CHANGE'
    END AS required_action,
    records
FROM grouped
WHERE row_count > 1 OR has_padding
ORDER BY source_system, canonical_reference;

\echo 'V3 collision count (must be zero)'
SELECT COUNT(*) AS canonical_collision_groups
FROM (
    SELECT BTRIM(external_reference)
    FROM transactions
    GROUP BY BTRIM(external_reference)
    HAVING COUNT(*) > 1
) AS collisions;

\echo 'V1 contract violations (must return no rows)'
SELECT
    id AS transaction_id,
    external_reference,
    currency,
    source_account,
    destination_account,
    channel,
    created_at,
    CASE
        WHEN BTRIM(external_reference)
                 !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$'
            THEN 'INVALID_CANONICAL_REFERENCE'
        WHEN currency <> 'INR'
            THEN 'UNSUPPORTED_CURRENCY'
        WHEN source_account !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$'
            THEN 'INVALID_SOURCE_ACCOUNT'
        WHEN destination_account !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$'
            THEN 'INVALID_DESTINATION_ACCOUNT'
        WHEN channel NOT IN ('UPI', 'CARD', 'NEFT', 'IMPS', 'RTGS', 'BANK_TRANSFER', 'OTHER')
            THEN 'UNSUPPORTED_CHANNEL'
        WHEN created_at < TIMESTAMPTZ '1970-01-01 00:00:00+00'
            THEN 'TIMESTAMP_BEFORE_EPOCH'
        WHEN created_at > CURRENT_TIMESTAMP + INTERVAL '5 minutes'
            THEN 'TIMESTAMP_TOO_FAR_IN_FUTURE'
    END AS violation
FROM transactions
WHERE BTRIM(external_reference)
          !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$'
   OR currency <> 'INR'
   OR source_account !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$'
   OR destination_account !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$'
   OR channel NOT IN ('UPI', 'CARD', 'NEFT', 'IMPS', 'RTGS', 'BANK_TRANSFER', 'OTHER')
   OR created_at < TIMESTAMPTZ '1970-01-01 00:00:00+00'
   OR created_at > CURRENT_TIMESTAMP + INTERVAL '5 minutes'
ORDER BY id;

COMMIT;
