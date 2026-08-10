-- Quarantine is a compatibility boundary, not a retry-state recovery.
ALTER TABLE outbox_events
    DROP CONSTRAINT chk_outbox_delivery_state,
    DROP CONSTRAINT chk_outbox_status;

ALTER TABLE outbox_events
    ADD COLUMN quarantined_at TIMESTAMPTZ,
    ADD COLUMN quarantine_reason VARCHAR(1000),
    ADD COLUMN quarantined_from_status VARCHAR(12);

-- V6 is the first release that emits event schema version 2. Every unpublished
-- row present before this migration contains an older or otherwise unverified
-- payload. Preserve it byte-for-byte for an explicit compatibility review; do
-- not let the normal publisher send it under the version-2 topic contract.
UPDATE outbox_events
SET quarantined_from_status = status,
    status = 'QUARANTINED',
    quarantined_at = CURRENT_TIMESTAMP,
    quarantine_reason =
        'Historical unpublished payload predates event schema version 2; compatibility review and controlled replay are required',
    claim_token = NULL,
    claimed_at = NULL
WHERE status IN ('PENDING', 'PROCESSING', 'FAILED');

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'QUARANTINED')) NOT VALID,
    ADD CONSTRAINT chk_outbox_publishable_schema_v2
        CHECK (
            status IN ('PUBLISHED', 'QUARANTINED')
            OR COALESCE(payload -> 'schemaVersion' = '2'::JSONB, FALSE)
        ) NOT VALID,
    ADD CONSTRAINT chk_outbox_delivery_state
        CHECK (
            (
                status = 'PUBLISHED'
                AND published_at IS NOT NULL
                AND claim_token IS NULL
                AND claimed_at IS NULL
                AND quarantined_at IS NULL
                AND quarantine_reason IS NULL
                AND quarantined_from_status IS NULL
            )
            OR (
                status = 'PROCESSING'
                AND published_at IS NULL
                AND claim_token IS NOT NULL
                AND claimed_at IS NOT NULL
                AND quarantined_at IS NULL
                AND quarantine_reason IS NULL
                AND quarantined_from_status IS NULL
            )
            OR (
                status = 'PENDING'
                AND published_at IS NULL
                AND claim_token IS NULL
                AND claimed_at IS NULL
                AND quarantined_at IS NULL
                AND quarantine_reason IS NULL
                AND quarantined_from_status IS NULL
            )
            OR (
                status = 'QUARANTINED'
                AND published_at IS NULL
                AND claim_token IS NULL
                AND claimed_at IS NULL
                AND quarantined_at IS NOT NULL
                AND quarantine_reason IS NOT NULL
                AND quarantined_from_status IN ('PENDING', 'PROCESSING', 'FAILED')
            )
        ) NOT VALID;

ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_status;
ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_publishable_schema_v2;
ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_delivery_state;
