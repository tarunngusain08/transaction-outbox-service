ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_status_check;

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED'));

ALTER TABLE outbox_events
    ADD COLUMN claim_token UUID,
    ADD COLUMN claimed_at TIMESTAMPTZ;

ALTER TABLE outbox_events
    DROP CONSTRAINT chk_outbox_publication_state;

ALTER TABLE outbox_events
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
                status IN ('PENDING', 'FAILED')
                AND published_at IS NULL
                AND claim_token IS NULL
                AND claimed_at IS NULL
            )
        );

DROP INDEX idx_outbox_pending;

CREATE INDEX idx_outbox_claimable
    ON outbox_events (next_attempt_at, claimed_at, created_at)
    WHERE status IN ('PENDING', 'PROCESSING');
