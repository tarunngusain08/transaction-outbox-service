-- V1 through V6 are immutable because earlier artifacts may already have
-- recorded their checksums. V7 is the forward-only V2 event-contract boundary.
ALTER TABLE outbox_events
    DROP CONSTRAINT chk_outbox_delivery_state,
    DROP CONSTRAINT chk_outbox_status;

ALTER TABLE outbox_events
    ADD COLUMN quarantined_at TIMESTAMPTZ,
    ADD COLUMN quarantine_reason VARCHAR(1000),
    ADD COLUMN quarantined_from_status VARCHAR(12);

-- The application and every old publisher must be stopped while Flyway runs.
-- V6 converted terminal FAILED rows to PENDING, so V7 quarantines the complete
-- unpublished set as it exists after the previously released migration chain.
UPDATE outbox_events
SET quarantined_from_status = status,
    status = 'QUARANTINED',
    quarantined_at = CURRENT_TIMESTAMP,
    quarantine_reason =
        'Historical unpublished payload predates event schema version 2; compatibility review and controlled replay are required',
    claim_token = NULL,
    claimed_at = NULL
WHERE status IN ('PENDING', 'PROCESSING');

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
                AND quarantined_from_status IN ('PENDING', 'PROCESSING')
            )
        ) NOT VALID;

ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_status;
ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_publishable_schema_v2;
ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_delivery_state;

CREATE INDEX idx_outbox_quarantined_at
    ON outbox_events (quarantined_at)
    WHERE status = 'QUARANTINED';

-- Correct the operator-facing name and message without changing applied V3.
ALTER FUNCTION reject_reference_audit_mutation()
    RENAME TO reject_reference_audit_row_mutation;

ALTER TRIGGER trg_reference_audit_immutable
    ON transaction_reference_canonicalization_audit
    RENAME TO trg_reference_audit_row_mutation;

CREATE OR REPLACE FUNCTION reject_reference_audit_row_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'transaction reference canonicalization audit is row-mutation protected';
END
$$;
