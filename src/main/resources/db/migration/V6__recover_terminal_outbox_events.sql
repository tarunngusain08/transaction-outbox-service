-- Preserve retry_count and last_error as operational evidence while making every
-- unpublished event service-owned and deliverable again.
UPDATE outbox_events
SET status = 'PENDING',
    next_attempt_at = CURRENT_TIMESTAMP
WHERE status = 'FAILED';

ALTER TABLE outbox_events
    DROP CONSTRAINT chk_outbox_delivery_state,
    DROP CONSTRAINT chk_outbox_status;

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED')) NOT VALID,
    ADD CONSTRAINT chk_outbox_delivery_state
        CHECK (
            (
                status = 'PUBLISHED'
                AND published_at IS NOT NULL
                AND claim_token IS NULL
                AND claimed_at IS NULL
            )
            OR (
                status = 'PROCESSING'
                AND published_at IS NULL
                AND claim_token IS NOT NULL
                AND claimed_at IS NOT NULL
            )
            OR (
                status = 'PENDING'
                AND published_at IS NULL
                AND claim_token IS NULL
                AND claimed_at IS NULL
            )
        ) NOT VALID;

ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_status;
ALTER TABLE outbox_events VALIDATE CONSTRAINT chk_outbox_delivery_state;
